# ecom — a rate-limited microservices e-commerce backend

Four Go services behind an API gateway that enforces a **distributed** rate
limit in Redis, with a choreographed saga coordinating checkout across service
boundaries.

Every performance number below is measured by a test or a load run in this
repository. None are estimated, and each is reproducible with a command given
here.

```
                    ┌──────────────┐
   client ─────────▶│ nginx :8000  │
                    └──────┬───────┘
                  ┌────────┴────────┐
            ┌─────▼─────┐     ┌─────▼─────┐
            │ gateway-1 │     │ gateway-2 │      one shared rate limit,
            └─────┬─────┘     └─────┬─────┘      not one per instance
                  └────────┬────────┘
        ┌──────────┬───────┴───┬──────────┐
   ┌────▼───┐ ┌────▼────┐ ┌────▼───┐ ┌────▼────┐
   │  auth  │ │ catalog │ │ orders │ │ payment │
   └────┬───┘ └────┬────┘ └────┬───┘ └────┬────┘
        └──────────┴─────┬─────┴──────────┘
              ┌──────────┼──────────┐
         ┌────▼───┐ ┌────▼────┐ ┌───▼───┐
         │ redis  │ │ postgres│ │ kafka │
         └────────┘ └─────────┘ └───────┘
```

## What is actually interesting here

**The rate limit holds across instances, not per instance.** Two gateway
processes sharing one Redis. 500 requests through the load balancer against a
limit of 100:

```
allowed: 100    denied (429): 400
```

Per-instance limiting would have admitted ~200. Exactly 100 were admitted, in
two separate runs. The enforcement is a Lua script evaluated inside Redis, so
check-and-decrement is atomic. An earlier non-atomic GET-then-INCR prototype
admitted **1000 of 1000** against the same limit.

**Checkout was measured under load, not guessed at.** Ramping to 200 req/s:

```
14,256 orders created   0 failures   0.00% HTTP error rate   188.8 req/s
p50 530ms      p95 985ms      p99 1010ms
```

**The load test found a real bottleneck.** Orders are created at ~95/s but
confirmed at ~1/s. Kafka consumer lag isolates it to one consumer:

```
payment-service / order.created      lag 28022
orders-service  / payment.succeeded  lag 0
```

The payment consumer is a strictly sequential fetch/handle/commit loop with a
synchronous database write per message — the cost of the at-least-once
guarantee. It is documented rather than quietly fixed, with three remedies
ranked by cost, in [ADR 0002](docs/adr/0002-saga-choreography.md).

## Quick start

Requires Docker and Go 1.25+.

```sh
cp .env.example .env
export JWT_SECRET="$(openssl rand -hex 32)"
docker compose -f deploy/docker-compose.full.yml up -d --build
```

Eleven containers: nginx, two gateways, four services, Redis, Postgres, Kafka,
Prometheus. Two ports reach the host — **8000** for the load balancer and
**9090** for Prometheus. Redis, Postgres, Kafka and the services themselves are
reachable only inside the Docker network, so k6 drives the stack exactly as a
client would.

```sh
# sign up (public route, rate limited by IP)
curl -sX POST http://localhost:8000/auth/signup \
  -H 'Content-Type: application/json' \
  -d '{"email":"you@example.com","password":"correct-horse-battery-staple"}'

# the response carries a JWT; protected routes are keyed by user
TOKEN=...
curl -s http://localhost:8000/catalog/products -H "Authorization: Bearer $TOKEN"
```

Tear down with `docker compose -f deploy/docker-compose.full.yml down -v`.

## Running the tests

The integration suites need live backends. They **skip** rather than fail when
none are reachable, so `go test ./...` stays honest on a machine without Docker
— which means a skipped test looks exactly like a passing one. CI sets the
variables and then fails the build if anything skipped.

```sh
docker compose -f deploy/docker-compose.yml up -d   # publishes 6379/5432/9092

export RATELIMITER_TEST_REDIS_ADDR=127.0.0.1:6379
export KAFKA_TEST_BROKERS=127.0.0.1:9092
export AUTH_TEST_DATABASE_URL='postgres://ecom:ecom@127.0.0.1:5432/ecom_auth?sslmode=disable'
export CATALOG_TEST_DATABASE_URL='postgres://ecom:ecom@127.0.0.1:5432/ecom_catalog?sslmode=disable'
export ORDERS_TEST_DATABASE_URL='postgres://ecom:ecom@127.0.0.1:5432/ecom_orders?sslmode=disable'
export PAYMENT_TEST_DATABASE_URL='postgres://ecom:ecom@127.0.0.1:5432/ecom_payment?sslmode=disable'

for m in pkg ratelimiter gateway services/auth services/catalog services/orders services/payment; do
  (cd "$m" && go test -race -count=1 ./...)
done
```

`-race` is not optional: the properties this project claims — exactly-N through
the limiter, no overselling, exactly one charge — are all concurrency
properties, and a passing assertion can hide a data race.

Run only one compose file at a time. They both publish Prometheus on 9090, so
the second to start fails — and more importantly the dev stack is what exposes
5432/6379/9092 to the host, which is exactly what the full stack withholds. A
test run against the full stack silently skips every integration test.

## Reproducing the load tests

```sh
export JWT_SECRET="$(openssl rand -hex 32)"
docker compose -f deploy/docker-compose.full.yml up -d --build

# (a) the limit holds across both gateway instances
docker run --rm --network ecom-full_default \
  -v "$PWD/deploy/k6:/scripts:ro" -v "$PWD/deploy/k6/results:/results" \
  -e BASE_URL=http://loadbalancer:80 -e RATE_LIMIT=100 -e OUT_DIR=/results \
  grafana/k6:latest run /scripts/ratelimit.js

# (b) checkout latency and throughput
docker run --rm --network ecom-full_default \
  -v "$PWD/deploy/k6:/scripts:ro" -v "$PWD/deploy/k6/results:/results" \
  -e BASE_URL=http://loadbalancer:80 -e OUT_DIR=/results \
  grafana/k6:latest run /scripts/checkout.js
```

For (b), raise `RATE_LIMIT` first (`RATE_LIMIT=1000000 docker compose ... up -d
gateway-1 gateway-2`), or the limiter — the subject of test (a) — becomes the
bottleneck in test (b) and you measure it instead of the checkout path.

Full evidence: [`deploy/testdata/phase7-acceptance.txt`](deploy/testdata/phase7-acceptance.txt).

k6's raw output embeds whole request bodies, so a load run leaves live JWTs for
the synthetic test users in `deploy/k6/results/`. `scripts/redact-k6-results.sh`
strips them; install it as a pre-commit hook so a re-run cannot quietly stage a
token:

```sh
ln -s ../../scripts/redact-k6-results.sh .git/hooks/pre-commit
```

## Summary

> Built a microservices e-commerce backend (Go, Redis, Kafka, Postgres, Docker)
> with a custom Redis-backed rate limiter that enforces one shared limit across
> horizontally-scaled API gateway instances: 500 concurrent requests against a
> limit of 100 admitted **exactly 100**, where per-instance limiting would have
> admitted ~200. Enforcement is a Lua script evaluated inside Redis, making
> check-and-decrement atomic — a non-atomic GET-then-INCR prototype admitted
> 1000 of 1000 under the same load. Implemented the order-payment flow as a
> choreographed saga with idempotent consumers and compensating transactions,
> where idempotency is enforced by a unique index proven load-bearing: with it
> dropped, 8 concurrent deliveries of one event charged the customer 3 times.

Sustained throughput was **188.8 req/s** with **14,256 orders created and zero
failures**; end-to-end checkout latency was p50 530ms / p95 985ms / p99 1010ms.

Those latency numbers deserve their context rather than a headline. They were
measured at 200 req/s offered load against the whole stack on a single laptop
Docker VM (8 CPUs, 4 GiB), so they describe the system *at saturation*, not the
cost of a request. The split makes that plain — browsing a product is 2.2ms p95
while creating an order is 984.5ms p95, because order creation prices from the
catalog and reserves stock synchronously. It is a queueing number, and a
different machine would produce a different one.

The rate-limiter result is the durable claim: exactly-100-of-500 is a property
of the design, reproduced across three separate runs, and does not depend on the
hardware it was measured on.

## Design decisions

Recorded as ADRs, each with the alternatives that were real candidates and what
would make the decision wrong later:

- **[0001 — token bucket as the gateway default](docs/adr/0001-rate-limiter-algorithm.md).**
  Decided on measured Redis memory: 176 B flat per key for token bucket versus
  499,168 B and growing for the sliding window log at 5,000 requests. Includes
  measured window-boundary behaviour for all three algorithms.
- **[0002 — choreographed saga, not an orchestrator](docs/adr/0002-saga-choreography.md).**
  Covers compensation ordering, at-least-once delivery, the measured throughput
  ceiling, and the missing dead-letter topic.

## Layout

```
gateway/       API gateway: correlation ID → JWT → rate limit → proxy
ratelimiter/   three algorithms, in-memory and Redis/Lua
pkg/           correlation, logging, dbx, events, httpx, metrics
services/      auth, catalog, orders, payment
deploy/        compose topologies, Dockerfile, k6 scripts, evidence
docs/adr/      architecture decision records
```

A Go workspace of seven modules. Each service owns its own database and
migrations; no service reads another's tables.

The gateway middleware order is deliberate — correlation ID first so even
rejected requests are traceable, then JWT, then the rate limiter, so
authenticated callers are keyed by user rather than by IP.

## Operational notes

**Observability.** Every service exposes `/metrics` with RED metrics (rate,
errors, duration), labelled by route pattern rather than raw path to keep
cardinality bounded. One correlation ID reconstructs a request's whole lifecycle
across all four services, and it survives the hop into Kafka and back out.

**Health checks** deliberately do not verify dependencies. `/healthz` answers
"can this process serve requests", so a Redis outage cannot make every gateway
instance look unhealthy and get pulled from rotation — turning a degradation
into a total outage. The tradeoff is that a load balancer may route to a service
whose backend is down.

**Secrets.** `JWT_SECRET` has no default anywhere; the gateway and auth both
refuse to start without it, and auth enforces a 32-byte minimum — verified by
starting them without one. The `ecom:ecom` Postgres credential in the compose
files is a local development convenience: that container publishes no port in
the full stack, so it is unreachable from the host.

## Known gaps

- **No dead-letter topic.** A handler that fails permanently is retried
  immediately and forever. A malformed message *is* handled — committed past and
  logged as data loss — but a well-formed message whose handler always fails is
  not. This is the largest gap in the saga.
- **Payment consumer throughput**, above: ~1 order/s. Running one consumer per
  partition is a 3× gain with no code change.
- **The payment processor is a stub.** `PAYMENT_FAIL_AMOUNT_CENTS` makes failure
  deterministic so the compensating path is testable on demand.

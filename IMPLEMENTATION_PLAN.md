# Implementation plan — rate-limited microservices e-commerce platform

## 0. What this is

A small e-commerce backend (auth, catalog, orders, payment) fronted by a
horizontally-scaled API gateway that embeds a **custom, Redis-backed
distributed rate limiter**. Orders and payment coordinate through an event
bus using the saga pattern instead of a distributed transaction.

This document is the single source of truth for scope, architecture, and
phase sequencing. An IDE coding agent working on this repo should read this
file in full before writing any code. See `AGENTS.md` for how the agent
should operate while executing this plan.

## 1. Assumptions (confirm or override before Phase 0)

1. **Language: Go**, one language across gateway + all services. Chosen for
   goroutine-based concurrency (directly relevant to the rate limiter and
   the saga consumers) and because it reads as a deliberate systems choice
   in an interview, not a default.
2. **Redis** for shared rate-limit state (atomic ops via Lua scripts).
3. **PostgreSQL**, one database per service — no shared tables, no
   cross-service joins.
4. **Kafka** for the event bus (`segmentio/kafka-go`). RabbitMQ is an
   acceptable substitute if you'd rather avoid Kafka's ops overhead; the
   saga logic is identical either way, only the client library changes.
5. **Hand-rolled gateway** (Go + `chi` router) rather than Kong/Envoy — the
   point of this project is to build and explain the middleware yourself,
   not configure someone else's YAML.
6. **k6** for load testing, **Prometheus + Grafana** (optional but
   recommended) for metrics during load tests.
7. JWT auth (`golang-jwt/jwt`), HS256 for simplicity, validated once at the
   gateway and passed downstream as a trusted header.

If any of these don't match your actual constraints (team's existing
stack, a language you're trying to showcase instead, etc.), change them
here first — don't let the agent silently pick something else mid-build.

## 2. Repo layout

```
.
├── AGENTS.md
├── IMPLEMENTATION_PLAN.md
├── docs/
│   └── adr/                      # one file per architectural decision
├── ratelimiter/                  # standalone module, phase 1-2
│   ├── algorithms/               # token bucket, sliding window log, sliding window counter
│   ├── redis/                    # Lua scripts + Go client wrapper
│   └── ratelimiter_test.go
├── gateway/                      # phase 3
│   ├── middleware/                # rate limit middleware, auth middleware, correlation-id middleware
│   └── router/
├── services/
│   ├── auth/
│   ├── catalog/
│   ├── orders/                   # saga initiator
│   └── payment/                  # saga participant
├── deploy/
│   ├── docker-compose.yml
│   └── k6/                       # load test scripts + saved results
└── .github/workflows/            # CI, phase 8+
```

## 3. Architecture summary

```
Client → Load balancer → Gateway (×2, rate limiter + JWT check)
                              │
                    ┌─────────┴─────────┐
                    │  shared Redis      │  (rate-limit state)
                    └─────────┬─────────┘
                    allowed   │   denied
                    ┌─────────┴─────────┐
                    │                   │
              Microservices        429 response
        (auth, catalog, orders, payment)
                    │
        Orders ⇄ Event bus (Kafka) ⇄ Payment   (saga)
```

Full diagrams (rate limiter, microservices, and the combined view) were
generated earlier in this conversation — treat those as the visual
reference for this plan.

## 4. Phase-by-phase plan

Each phase = one thin vertical slice. Do not start phase *n+1* until phase
*n*'s acceptance criteria pass. This mirrors `incremental-implementation`
and `planning-and-task-breakdown` — see `AGENTS.md` for when to invoke each.

### Phase 0 — Spec lock & scaffold
- **Tasks:** confirm assumptions in §1, scaffold repo layout in §2, empty
  `go.mod` per module, `docker-compose.yml` skeleton with just Redis and
  Postgres services running.
- **Acceptance criteria:** `docker compose up` starts Redis + Postgres with
  no errors. Repo structure matches §2.

### Phase 1 — Rate limiter core (in-memory, no Redis yet)
- **Tasks:** implement three algorithms behind one interface:
  - Token bucket (burst-tolerant, O(1) state per key)
  - Sliding window log (exact, O(n) state per key)
  - Sliding window counter (approximate, O(1) state, weighted blend of
    current + previous fixed window)
- **Acceptance criteria:** unit tests cover — request at exactly the limit,
  request one over the limit, burst absorption (token bucket only), window
  boundary edge case (a burst that straddles two fixed windows should
  **not** double the effective limit for the sliding window counter).
  Table-driven tests, no Redis dependency yet.

### Phase 2 — Redis-backed, atomic
- **Tasks:** port each algorithm's state into Redis. Implement each as a
  single **Lua script** (`EVAL`) so the check-and-increment is atomic —
  never a `GET` then `INCR` as two round trips. Add TTL/`PEXPIRE` so keys
  don't grow unbounded.
- **Acceptance criteria:** a concurrency test that fires N goroutines at
  the same key simultaneously must allow **exactly** the configured limit
  through, not limit±race-condition-slop. This is the test that proves the
  atomicity claim — keep the raw output, you'll cite it later.

### Phase 3 — Gateway + middleware
- **Tasks:** `chi`-based gateway; middleware chain = correlation-id →
  JWT validation → rate limiter → route to service. Return `429` with
  `Retry-After` and `X-RateLimit-{Limit,Remaining,Reset}` headers on every
  response, allowed or not.
- **Acceptance criteria:** integration test hitting the gateway directly
  (services can be stubs at this point) confirms headers are present and
  correct on both allowed and denied paths.

### Phase 4 — Microservices (CRUD layer)
- **Tasks:** auth (signup/login, issues JWT), catalog (products,
  inventory), orders (create/get order), payment (mocked charge via
  Stripe test mode or a local stub). Each service: its own Postgres
  schema, its own migrations, no shared code beyond a small internal
  `pkg/` for correlation-id propagation and structured logging.
- **Acceptance criteria:** each service has integration tests against a
  real (test-container or docker-compose) Postgres instance — not mocks
  for the DB layer. Gateway can route to all four through Phase 3's
  middleware chain end-to-end.

### Phase 5 — Saga (orders ⇄ payment)
- **Tasks:** choreography-based saga. Orders creates a `pending` order,
  publishes `OrderCreated`. Payment consumes it, attempts a charge,
  publishes `PaymentSucceeded` or `PaymentFailed`. Orders consumes the
  result: confirms the order, or runs the compensating action (release
  the inventory reservation in catalog, mark order `cancelled`).
  Idempotency keys on the payment consumer so a redelivered event can't
  double-charge.
- **Acceptance criteria:** a test that publishes `OrderCreated` twice with
  the same order ID must result in exactly one charge attempt. A test that
  forces `PaymentFailed` must leave the system in a fully compensated
  state (no orphaned inventory reservation, order marked `cancelled`, not
  stuck `pending`).

### Phase 6 — Observability
- **Tasks:** correlation ID generated at the gateway, propagated through
  every service call and every Kafka message header. Structured (JSON)
  logs everywhere. Prometheus metrics: request rate, error rate, duration
  (RED metrics) per service, plus rate-limiter-specific metrics (allowed
  vs denied counts, current bucket/window state sampling).
- **Acceptance criteria:** given only the correlation ID from one order
  request, you can grep/query logs across all four services and see the
  full lifecycle of that one request. This is the "distributed debugging"
  capability worth demonstrating.

### Phase 7 — Integration + load test
- **Tasks:** full `docker compose up` — load balancer, 2 gateway
  instances, Redis, 4 services + Postgres, Kafka. Run k6 scripts that (a)
  hammer a single endpoint past its rate limit and confirm the limit holds
  **across both gateway instances combined**, not per-instance, and (b)
  run a realistic checkout flow at increasing concurrency to find
  throughput and p50/p95/p99 latency.
- **Acceptance criteria:** numbers captured and saved in
  `deploy/k6/results/`. This is where your resume numbers come from — do
  not estimate them, run the test.

### Phase 8 — Documentation
- **Tasks:** write ADRs (see `docs/adr/`) for the two decisions worth
  defending in an interview:
  1. Token bucket vs. sliding window counter — why you chose the default,
     when you'd switch.
  2. Choreography vs. orchestration for the saga — why choreography here,
     when a central orchestrator (e.g., Temporal) would be worth the extra
     moving part.
- **Acceptance criteria:** each ADR states the decision, the alternatives
  considered, and the concrete trade-off (not "it's simpler" — say *what*
  it trades away).

### Phase 9 — Ship
- **Tasks:** README with architecture diagram, setup instructions,
  captured load-test numbers, and the resume bullet. CI pipeline running
  tests on every push (GitHub Actions). Final pass for secrets left in
  code, `.env.example` committed instead of real `.env`.
- **Acceptance criteria:** a stranger can clone the repo, run one command,
  and have the whole stack up with a working example request in under 5
  minutes.

## 5. Known hard problems — slow down here

These are the places where a fast, confident-looking answer is usually
wrong. Treat them as required stops, not optional polish:

- **Check-then-act races** in the rate limiter (Phase 2) — must be a
  single atomic Redis operation, never two round trips.
- **At-least-once delivery** from Kafka (Phase 5) — every consumer must be
  idempotent; "it probably won't get redelivered" is not a design.
- **Partial saga failure ordering** (Phase 5) — what happens if the
  compensating action itself fails? Decide and document this, don't leave
  it as a silent gap.
- **Clock skew** in the token bucket's refill calculation — using
  wall-clock time across distributed callers can drift; Redis's own time
  (`TIME` command) inside the Lua script avoids trusting each caller's
  clock.

## 6. Testing strategy

- **Unit tests**: algorithm logic (Phase 1), pure functions, no I/O.
- **Integration tests**: real Redis/Postgres/Kafka via docker-compose or
  testcontainers — not mocked. A rate limiter test against a mocked Redis
  proves nothing about the atomicity claim.
- **Load tests**: k6, run against the fully integrated stack (Phase 7),
  not against a single service in isolation — the whole point is proving
  the limit holds across horizontally-scaled instances.

## 7. Resume bullet (fill in once Phase 7 numbers exist)

> Designed and built a microservices e-commerce platform (Go, Kafka,
> Redis, Docker) with a custom Redis-backed rate limiter enforcing limits
> consistently across horizontally-scaled API gateway instances at
> [X] req/sec with p99 latency under [Y]ms; implemented the order-payment
> saga with idempotent consumers and compensating transactions for
> failure handling.
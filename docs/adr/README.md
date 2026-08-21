# Architecture Decision Records

Each ADR records a decision that was not obvious, the alternatives that were
real candidates, and what would make the decision wrong later.

Where an ADR states a number, that number is measured by a test or a load run in
this repository and is reproducible. None are estimated.

| ADR | Decision | Status |
|---|---|---|
| [0001](0001-rate-limiter-algorithm.md) | Token bucket as the gateway's default rate-limiting algorithm | Accepted |
| [0002](0002-saga-choreography.md) | Choreographed saga for checkout, not an orchestrator | Accepted, with a known gap |

## Reproducing the measurements

Both ADRs depend on a live Redis, Postgres and Kafka:

```sh
docker compose -f deploy/docker-compose.yml up -d
```

ADR 0001, memory per algorithm and window-boundary behaviour:

```sh
cd ratelimiter
RATELIMITER_TEST_REDIS_ADDR=127.0.0.1:6379 \
  go test ./redis/ -run TestMemoryFootprintPerAlgorithm -v -count=1
go test ./algorithms/ -run TestBoundaryBurstComparison -v -count=1
```

`MEMTEST_REQUESTS` varies the traffic driven into one key (default 5000), which
is what shows the log's O(n) growth against the other two staying flat.

ADR 0002, the concurrency case the unique index exists for:

```sh
cd services/payment
PAYMENT_TEST_DATABASE_URL='postgres://ecom:ecom@127.0.0.1:5432/ecom_payment?sslmode=disable' \
  go test -run TestConcurrentDeliveryChargesOnce -v -count=1 ./...
```

To see it fail as it should, drop the index first:

```sh
docker exec deploy-postgres-1 psql -U ecom -d ecom_payment \
  -c 'DROP INDEX IF EXISTS payments_order_idx;'
```

The saga throughput numbers come from the Phase 7 load run; see
`deploy/testdata/phase7-acceptance.txt` and `deploy/k6/`.

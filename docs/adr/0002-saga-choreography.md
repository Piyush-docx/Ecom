# ADR 0002: Choreographed saga for checkout, not an orchestrator

- **Status:** Accepted, with a known gap (see "Known gaps")
- **Date:** 2026-08-21
- **Context phase:** Phase 5 (implementation), Phase 7 (measurement)

## Context

Checkout spans three services and cannot be one database transaction:

1. `orders` creates an order and reserves stock in `catalog`
2. `payment` charges the customer
3. the order is confirmed, or the reservation is released and the order cancelled

Each step is a local transaction in a different database. The distributed
failure — payment declines after stock is reserved — must be compensated rather
than rolled back.

Two shapes were available:

- **Orchestration:** a coordinator service owns the workflow, calls each
  participant, and drives compensation.
- **Choreography:** each service reacts to events; no service owns the workflow.

## Decision

**Choreography.** Three topics, two participants, no coordinator:

```
orders  --OrderCreated-->      payment
payment --PaymentSucceeded-->  orders   (commit reservation, confirm order)
payment --PaymentFailed-->     orders   (release reservation, cancel order)
```

Wiring is in `services/payment/main.go` and `services/orders/main.go`; the
handlers are `services/orders/saga.go` and `services/payment/saga.go`.

## Rationale

**Why choreography here.** With two participants and one branch point, an
orchestrator is a third service to deploy, monitor, and keep available for a
workflow with exactly one decision in it. The coordination logic would not be
simpler for being centralised — it would be the same two handlers, plus a
network hop and a new single point of failure.

**Why the industry default argument does not apply yet.** Choreography is
usually criticised for becoming untraceable as participants multiply: with N
services reacting to each other, no single place describes the workflow. That
criticism is correct and it is the reason to revisit this (see "When to
revisit"). At N=2 the entire workflow is three topics and fits in the diagram
above.

**What makes it traceable today.** Phase 6 gives every request a correlation ID
that survives the hop into Kafka and back out: `pkg/events` carries it in the
envelope, and `Consumer.handle` rebuilds the logging context from it. One ID
reconstructs the whole lifecycle across all four services, which is the
mitigation for choreography's main weakness. `pkg/logging/logging_test.go`
asserts exactly this.

## Consequences

### Compensation ordering is deliberate

On `PaymentFailed`, `orders` releases every stock hold **before** marking the
order cancelled. If a release fails, the handler returns an error, the offset is
not committed, the event is redelivered, and the release is retried — safe
because catalog's release is idempotent. The order stays `pending` until every
release succeeds.

The reverse order is worse in a specific way: a failed release after the order
is already terminal leaves stock held for an order that no longer exists, and
nothing in the system would ever revisit it. That inventory is silently
unsellable. Keeping the order pending keeps the discrepancy visible and the
retry running.

### At-least-once delivery, so every handler is idempotent

`Consumer.Run` fetches without committing, runs the handler, and commits only on
success. A crash between fetch and commit redelivers the message. The
alternative (`ReadMessage`, which auto-commits) would drop a message whose
handler crashed — and a dropped `OrderCreated` means a customer never charged
for an order holding real stock.

Idempotency is enforced in two layers, and only one of them actually closes the
race. `HandleOrderCreated` first looks for an existing payment by `order_id`;
behind it, a unique index on `payments (order_id)` makes the guarantee hold when
two deliveries arrive at once.

The application check alone is not sufficient, and this was verified rather than
assumed. With the unique index dropped, `TestConcurrentDeliveryChargesOnce`
fires 8 simultaneous deliveries of one `OrderCreated`:

```
index dropped:  8 concurrent deliveries produced 3 charge attempts   <- customer charged 3x
index restored: 8 concurrent deliveries produced exactly 1 charge
```

Both deliveries look, both find nothing, and both charge — exactly the failure
the migration's comment predicts. Note that the *sequential* duplicate tests
still pass with the index dropped, because the application check handles that
case; only concurrency exposes it. That is why the index is not redundant with
the lookup.

Fourteen saga tests now cover duplicate delivery, compensation, and late events,
including `TestManyDuplicateDeliveriesChargeOnce`,
`TestLateEventCannotFlipASettledOrder`, and the concurrent case above.

### Throughput is bounded by the slowest consumer — measured

Phase 7 measured the cost of this design. Under load the system created orders
at ~95/s and confirmed them at **~1/s**:

```
orders:   pending=28022  confirmed=512
payments: succeeded=437
```

Kafka consumer lag isolates it to one consumer:

```
payment-service / order.created      lag 28022  (9257 + 9485 + 9280)
orders-service  / payment.succeeded  lag 0
```

`orders` keeps up completely; `payment` does not. The cause is `Consumer.Run` in
`pkg/events/kafka.go`: a strictly sequential fetch/handle/commit loop with a
synchronous database write per message. The topic has 3 partitions but one
consumer instance drains them one message at a time.

This is the cost of the at-least-once guarantee, not a defect in it — the offset
must not be committed before the handler succeeds. Nothing is lost, and the
backlog drains cleanly once load stops. What was not previously quantified is
how expensive it is.

**This is not caused by choreography.** An orchestrator making synchronous calls
in the same sequential loop would have the same ceiling. It is recorded here
because Phase 7 measured it while measuring the saga, and because the remedies
below interact with the ordering guarantees this ADR depends on.

Remedies, in increasing order of cost:

1. **Run 3 payment consumer instances**, one per partition. The partition count
   already allows this with no code change — a 3x gain.
2. **Handle messages concurrently within one consumer**, committing the lowest
   contiguous offset. Higher gain; needs care, because committing out of order
   would break the redelivery guarantee.
3. **Batch the database writes.** Largest gain, most invasive.

Option 1 is the obvious first move and is deliberately not applied here: it
changes Phase 5's runtime behaviour and belongs with a change that can be
measured on its own.

## Known gaps

**No dead-letter topic.** A handler that fails permanently is retried
immediately and indefinitely. `Consumer.Run` logs and continues rather than
returning, so one poisonous message cannot kill the consumer — but it also never
stops being retried. A malformed message *is* handled (committed past, logged
loudly as data loss, in `Consumer.handle`); a well-formed message whose handler
always fails is not.

The production answer is a dead-letter topic plus an alert after N attempts.
This is the single largest gap in the saga implementation and is called out in
`pkg/events/kafka.go` at the point where it would be added.

**Consequence today:** a permanently failing catalog leaves orders stuck
`pending` with stock held, retried forever. An operator can see this — pending
orders and a growing retry log — but nothing escalates on its own.

## When to revisit

Move to **orchestration** when any of these becomes true:

- **A third participant joins the workflow** (shipping, fraud, loyalty). Each
  new participant adds edges to an implicit graph no single file describes, and
  that is where choreography stops paying.
- **The workflow needs a branch that no single service can decide** — anything
  requiring knowledge of two participants' state at once.
- **Someone needs to ask "where is order X in the workflow?"** and the answer
  requires reading logs from three services. An orchestrator has that state in
  one table by construction.

The first is the likeliest trigger and worth treating as a rewrite signal rather
than something to grow into gradually.

## Related

- ADR 0001 covers the rate limiter, which sits in front of this flow.
- `deploy/testdata/phase7-acceptance.txt` has the full measurement context.

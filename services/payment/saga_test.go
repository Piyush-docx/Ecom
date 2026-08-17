package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"pkg/events"
)

// Saga tests against a real Kafka and a real Postgres.
//
// Neither is mocked, for the same reason the plan gives elsewhere: the
// "exactly one charge" guarantee lives in a unique index, and the redelivery
// behaviour being tested is a property of Kafka's at-least-once semantics. A
// fake broker that delivers each message once would test the opposite of what
// matters.
//
//	docker compose -f deploy/docker-compose.yml up -d

func testBrokers() []string {
	if v := os.Getenv("KAFKA_TEST_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:9092"}
}

// newSagaFixture returns a payment saga consumer wired to real Kafka.
func newSagaFixture(t *testing.T, gateway Gateway) (*SagaConsumer, *events.Publisher) {
	t.Helper()

	api, _ := newTestAPI(t, gateway)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The saga publishes its outcomes to the real saga topics, so those are the
	// ones that must exist. Creating per-test topics here would leave the code
	// under test publishing to topics nothing created, which is a property of
	// the fixture rather than of the system.
	//
	// Sharing topics across tests is safe because each test uses a fresh order
	// ID and asserts on database state rather than on what it reads back from
	// the broker.
	brokers := testBrokers()
	if err := events.EnsureTopics(ctx, brokers, events.SagaTopics, 1, 1); err != nil {
		t.Skipf("no Kafka at %v (%v) — start one with: docker compose -f deploy/docker-compose.yml up -d kafka", brokers, err)
	}
	if err := events.WaitForTopics(ctx, brokers, events.SagaTopics, 20*time.Second); err != nil {
		t.Fatalf("topics not ready: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	publisher := events.NewPublisher(brokers, logger)
	t.Cleanup(func() { _ = publisher.Close() })

	// The consumer publishes outcomes to the real topic names, so a test that
	// wants to observe them subscribes there. Overriding is not worth the
	// indirection: what matters here is the charge, which the database records.
	saga := NewSagaConsumer(api.store, gateway, publisher, nil, logger)

	return saga, publisher
}

// orderCreatedEnvelope builds an OrderCreated event.
func orderCreatedEnvelope(t *testing.T, eventID, orderID string, amount int64) *events.Envelope {
	t.Helper()
	env, err := events.NewEnvelope(eventID, events.TypeOrderCreated, "test-correlation-"+orderID,
		events.OrderCreated{
			OrderID:     orderID,
			UserID:      testUser,
			AmountCents: amount,
			Currency:    "USD",
			Items:       []events.OrderItem{{ProductID: newUUID(), Quantity: 1}},
		})
	if err != nil {
		t.Fatalf("building envelope: %v", err)
	}
	return env
}

// TestDuplicateOrderCreatedChargesExactlyOnce is the first Phase 5 acceptance
// criterion, verbatim from IMPLEMENTATION_PLAN.md:
//
//	"a test that publishes OrderCreated twice with the same order ID must
//	 result in exactly one charge attempt."
//
// The handler is invoked directly with two deliveries of the same event, which
// is exactly what Kafka's at-least-once delivery produces. The guarantee comes
// from the unique index on payments.order_id, not from any check in the
// handler — a check alone would lose the race between two concurrent
// deliveries.
func TestDuplicateOrderCreatedChargesExactlyOnce(t *testing.T) {
	saga, _ := newSagaFixture(t, StubGateway{})
	_, pool := newTestAPI(t, StubGateway{})

	orderID := newUUID()
	env := orderCreatedEnvelope(t, "evt-"+orderID, orderID, 4999)

	ctx := context.Background()

	// Deliver the same event twice, as a redelivery would.
	if err := saga.HandleOrderCreated(ctx, env); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	if err := saga.HandleOrderCreated(ctx, env); err != nil {
		t.Fatalf("second delivery: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM payments WHERE order_id = $1`, orderID).Scan(&count); err != nil {
		t.Fatalf("counting charges: %v", err)
	}

	if count != 1 {
		t.Errorf("two deliveries of one OrderCreated produced %d charge attempts, want exactly 1 — "+
			"the customer was charged twice", count)
	} else {
		t.Logf("two deliveries of OrderCreated for order %s produced exactly 1 charge", orderID[:8])
	}
}

// TestManyDuplicateDeliveriesChargeOnce hardens the same guarantee against a
// broker that redelivers repeatedly, which happens when a consumer keeps
// failing to commit its offset.
func TestManyDuplicateDeliveriesChargeOnce(t *testing.T) {
	saga, _ := newSagaFixture(t, StubGateway{})
	_, pool := newTestAPI(t, StubGateway{})

	orderID := newUUID()
	env := orderCreatedEnvelope(t, "evt-"+orderID, orderID, 1234)

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := saga.HandleOrderCreated(ctx, env); err != nil {
			t.Fatalf("delivery %d: %v", i+1, err)
		}
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM payments WHERE order_id = $1`, orderID).Scan(&count); err != nil {
		t.Fatalf("counting charges: %v", err)
	}
	if count != 1 {
		t.Errorf("10 deliveries produced %d charges, want exactly 1", count)
	}
}

// TestChargeCountingGateway confirms the idempotency holds at the payment
// processor, not merely in the database.
//
// The distinction matters: a handler that called the processor and only then
// discovered the duplicate would already have moved the customer's money. This
// counts actual Charge invocations.
func TestChargeCountingGateway(t *testing.T) {
	counting := &countingGateway{}
	saga, _ := newSagaFixture(t, counting)

	orderID := newUUID()
	env := orderCreatedEnvelope(t, "evt-"+orderID, orderID, 2500)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := saga.HandleOrderCreated(ctx, env); err != nil {
			t.Fatalf("delivery %d: %v", i+1, err)
		}
	}

	if got := counting.calls(); got != 1 {
		t.Errorf("the payment processor was called %d times for one order, want exactly 1 — "+
			"the customer's card was charged more than once", got)
	} else {
		t.Log("5 deliveries of OrderCreated resulted in exactly 1 call to the payment processor")
	}
}

// TestFailedChargeIsAlsoIdempotent confirms a declined order is not retried
// into a later success. Retrying could charge a customer whose order has
// already been cancelled by the compensating path.
func TestFailedChargeIsAlsoIdempotent(t *testing.T) {
	const declineAmount = 66600
	counting := &countingGateway{failAmount: declineAmount}
	saga, _ := newSagaFixture(t, counting)
	_, pool := newTestAPI(t, counting)

	orderID := newUUID()
	env := orderCreatedEnvelope(t, "evt-"+orderID, orderID, declineAmount)

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := saga.HandleOrderCreated(ctx, env); err != nil {
			t.Fatalf("delivery %d: %v", i+1, err)
		}
	}

	if got := counting.calls(); got != 1 {
		t.Errorf("the processor was called %d times for a declined order, want 1", got)
	}

	var status string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM payments WHERE order_id = $1`, orderID).Scan(&status); err != nil {
		t.Fatalf("loading payment: %v", err)
	}
	if status != StatusFailed {
		t.Errorf("payment status = %q, want %q", status, StatusFailed)
	}
}

// TestMalformedEventIsSkipped confirms an undecodable payload does not block
// the partition forever. It can never succeed on redelivery, so the handler
// must report success and let the offset advance.
func TestMalformedEventIsSkipped(t *testing.T) {
	saga, _ := newSagaFixture(t, StubGateway{})

	// An envelope whose declared type does not match its payload.
	env, err := events.NewEnvelope("evt-bad", events.TypeOrderCreated, "corr",
		map[string]any{"unexpected": "shape", "order_id": 12345})
	if err != nil {
		t.Fatalf("building envelope: %v", err)
	}

	if err := saga.HandleOrderCreated(context.Background(), env); err != nil {
		t.Errorf("a malformed event returned %v, want nil so the offset advances — "+
			"otherwise it is redelivered forever and blocks the partition", err)
	}
}

// TestEventWithNoOrderIDIsSkipped covers the same reasoning for a
// structurally-valid payload that is semantically unusable.
func TestEventWithNoOrderIDIsSkipped(t *testing.T) {
	saga, _ := newSagaFixture(t, StubGateway{})

	env, err := events.NewEnvelope("evt-empty", events.TypeOrderCreated, "corr",
		events.OrderCreated{UserID: testUser, AmountCents: 100, Currency: "USD"})
	if err != nil {
		t.Fatalf("building envelope: %v", err)
	}

	if err := saga.HandleOrderCreated(context.Background(), env); err != nil {
		t.Errorf("an event with no order id returned %v, want nil", err)
	}
}

// countingGateway records how many times a charge was actually attempted.
//
// Guarded by a mutex because concurrent deliveries exercise it from several
// goroutines, and an unsynchronised counter would both miscount and trip the
// race detector.
type countingGateway struct {
	failAmount int64

	mu sync.Mutex
	n  int
}

func (g *countingGateway) Charge(_ context.Context, _ string, amountCents int64, _ string) error {
	g.mu.Lock()
	g.n++
	g.mu.Unlock()

	if g.failAmount != 0 && amountCents == g.failAmount {
		return fmt.Errorf("card declined")
	}
	return nil
}

func (g *countingGateway) calls() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.n
}

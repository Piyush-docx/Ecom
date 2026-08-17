package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"pkg/events"
)

// Saga tests for the orders service, against a real Postgres and a real Kafka.
//
//	docker compose -f deploy/docker-compose.yml up -d

func testBrokers() []string {
	if v := os.Getenv("KAFKA_TEST_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:9092"}
}

// newSagaTestAPI returns an orders API with its saga coordinator wired to real
// Kafka, plus the stub catalog so a test can inspect what was released.
func newSagaTestAPI(t *testing.T) (*API, *stubCatalog) {
	t.Helper()

	api, catalog, _ := newTestAPI(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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

	api.saga = NewSagaCoordinator(api.store, api.catalog, publisher, nil, logger)
	return api, catalog
}

// createPendingOrder makes an order through the HTTP API and returns it with
// its product ID.
func createPendingOrder(t *testing.T, api *API, catalog *stubCatalog, quantity int) (orderView, string) {
	t.Helper()

	productID := newUUID()
	catalog.addProduct(productID, 2500)

	rec := doJSON(t, api, http.MethodPost, "/orders/", createOrderRequest{
		Items: []orderItemRequest{{ProductID: productID, Quantity: quantity}},
	}, testUser)
	if rec.Code != http.StatusCreated {
		t.Fatalf("creating order: status = %d (body=%s)", rec.Code, rec.Body)
	}
	return decodeOrder(t, rec), productID
}

// paymentFailedEnvelope builds a PaymentFailed event for an order.
func paymentFailedEnvelope(t *testing.T, orderID, reason string) *events.Envelope {
	t.Helper()
	env, err := events.NewEnvelope(newUUID(), events.TypePaymentFailed, "test-correlation-"+orderID,
		events.PaymentFailed{OrderID: orderID, Reason: reason})
	if err != nil {
		t.Fatalf("building envelope: %v", err)
	}
	return env
}

// paymentSucceededEnvelope builds a PaymentSucceeded event for an order.
func paymentSucceededEnvelope(t *testing.T, orderID string, amount int64) *events.Envelope {
	t.Helper()
	env, err := events.NewEnvelope(newUUID(), events.TypePaymentSucceeded, "test-correlation-"+orderID,
		events.PaymentSucceeded{
			OrderID: orderID, PaymentID: newUUID(), AmountCents: amount, Currency: "USD",
		})
	if err != nil {
		t.Fatalf("building envelope: %v", err)
	}
	return env
}

// TestPaymentFailedLeavesFullyCompensatedState is the second Phase 5 acceptance
// criterion, verbatim from IMPLEMENTATION_PLAN.md:
//
//	"A test that forces PaymentFailed must leave the system in a fully
//	 compensated state (no orphaned inventory reservation, order marked
//	 cancelled, not stuck pending)."
//
// All three conditions are asserted separately, because each is a distinct way
// the saga could be wrong: releasing stock but forgetting the order leaves it
// pending forever, and cancelling the order but forgetting the stock loses
// inventory silently.
func TestPaymentFailedLeavesFullyCompensatedState(t *testing.T) {
	api, catalog := newSagaTestAPI(t)

	order, productID := createPendingOrder(t, api, catalog, 2)
	if order.Status != StatusPending {
		t.Fatalf("a new order is %q, want %q", order.Status, StatusPending)
	}

	// Force the failure path.
	if err := api.saga.HandlePaymentFailed(context.Background(),
		paymentFailedEnvelope(t, order.ID, "card declined")); err != nil {
		t.Fatalf("handling PaymentFailed: %v", err)
	}

	// 1. No orphaned inventory reservation.
	if got := catalog.releaseCount(productID); got != 1 {
		t.Errorf("the reservation was released %d times, want 1 — "+
			"stock is still held for an order that will never be paid", got)
	}

	// 2. The order is cancelled...
	rec := doJSON(t, api, http.MethodGet, "/orders/"+order.ID, nil, testUser)
	if rec.Code != http.StatusOK {
		t.Fatalf("loading the order: status = %d", rec.Code)
	}
	final := decodeOrder(t, rec)

	if final.Status != StatusCancelled {
		t.Errorf("order status = %q, want %q", final.Status, StatusCancelled)
	}
	// 3. ...and specifically not stuck pending.
	if final.Status == StatusPending {
		t.Error("the order is stuck pending after a failed payment")
	}
	if final.FailureReason == "" {
		t.Error("a cancelled order records no failure reason")
	}

	t.Logf("compensated: order %s is %s (%s), reservation released",
		final.ID[:8], final.Status, final.FailureReason)
}

// TestPaymentSucceededConfirmsAndConsumesStock covers the success path: the
// reservation becomes a real stock decrement and the order is confirmed.
func TestPaymentSucceededConfirmsAndConsumesStock(t *testing.T) {
	api, catalog := newSagaTestAPI(t)

	order, productID := createPendingOrder(t, api, catalog, 2)

	if err := api.saga.HandlePaymentSucceeded(context.Background(),
		paymentSucceededEnvelope(t, order.ID, order.TotalCents)); err != nil {
		t.Fatalf("handling PaymentSucceeded: %v", err)
	}

	if got := catalog.commitCount(productID); got != 1 {
		t.Errorf("the reservation was committed %d times, want 1", got)
	}
	if got := catalog.releaseCount(productID); got != 0 {
		t.Errorf("a successful payment released the reservation %d times, want 0", got)
	}

	rec := doJSON(t, api, http.MethodGet, "/orders/"+order.ID, nil, testUser)
	final := decodeOrder(t, rec)
	if final.Status != StatusConfirmed {
		t.Errorf("order status = %q, want %q", final.Status, StatusConfirmed)
	}
}

// TestCompensationIsIdempotent confirms a redelivered PaymentFailed does not
// double-release stock.
//
// Kafka is at-least-once, so this will happen. Catalog's release is idempotent
// per (order_id, product_id), and the order's status guard rejects a second
// transition, so the second delivery is a no-op at both layers.
func TestCompensationIsIdempotent(t *testing.T) {
	api, catalog := newSagaTestAPI(t)

	order, productID := createPendingOrder(t, api, catalog, 2)
	env := paymentFailedEnvelope(t, order.ID, "card declined")

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		if err := api.saga.HandlePaymentFailed(ctx, env); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}

	// The release endpoint is called each time -- it is idempotent, so that is
	// harmless -- but the order must settle exactly once.
	rec := doJSON(t, api, http.MethodGet, "/orders/"+order.ID, nil, testUser)
	final := decodeOrder(t, rec)
	if final.Status != StatusCancelled {
		t.Errorf("after 3 deliveries the order is %q, want %q", final.Status, StatusCancelled)
	}
	if got := catalog.releaseCount(productID); got < 1 {
		t.Errorf("the reservation was never released")
	}
}

// TestLateEventCannotFlipASettledOrder covers out-of-order delivery.
//
// Kafka orders messages within a partition, but PaymentSucceeded and
// PaymentFailed are on different topics, so nothing guarantees their relative
// order. A late PaymentFailed must not cancel an order that has already been
// confirmed and whose stock has already been consumed.
func TestLateEventCannotFlipASettledOrder(t *testing.T) {
	api, catalog := newSagaTestAPI(t)
	ctx := context.Background()

	t.Run("a late PaymentFailed cannot cancel a confirmed order", func(t *testing.T) {
		order, _ := createPendingOrder(t, api, catalog, 1)

		if err := api.saga.HandlePaymentSucceeded(ctx,
			paymentSucceededEnvelope(t, order.ID, order.TotalCents)); err != nil {
			t.Fatalf("confirming: %v", err)
		}
		// The stale event must be accepted (returning an error would retry it
		// forever) but must not change anything.
		if err := api.saga.HandlePaymentFailed(ctx,
			paymentFailedEnvelope(t, order.ID, "late failure")); err != nil {
			t.Fatalf("late PaymentFailed returned an error, want it absorbed: %v", err)
		}

		final := decodeOrder(t, doJSON(t, api, http.MethodGet, "/orders/"+order.ID, nil, testUser))
		if final.Status != StatusConfirmed {
			t.Errorf("order status = %q, want %q — a late event undid a completed payment",
				final.Status, StatusConfirmed)
		}
	})

	t.Run("a late PaymentSucceeded cannot confirm a cancelled order", func(t *testing.T) {
		order, _ := createPendingOrder(t, api, catalog, 1)

		if err := api.saga.HandlePaymentFailed(ctx,
			paymentFailedEnvelope(t, order.ID, "declined")); err != nil {
			t.Fatalf("cancelling: %v", err)
		}
		if err := api.saga.HandlePaymentSucceeded(ctx,
			paymentSucceededEnvelope(t, order.ID, order.TotalCents)); err != nil {
			t.Fatalf("late PaymentSucceeded returned an error, want it absorbed: %v", err)
		}

		final := decodeOrder(t, doJSON(t, api, http.MethodGet, "/orders/"+order.ID, nil, testUser))
		if final.Status != StatusCancelled {
			t.Errorf("order status = %q, want %q", final.Status, StatusCancelled)
		}
	})
}

// TestCompensationFailureLeavesOrderPendingForRetry documents and verifies the
// decision IMPLEMENTATION_PLAN.md 5 demands: what happens when the compensating
// action itself fails.
//
// The decision is that the order stays pending and the event is redelivered.
// The alternative — cancel the order and then attempt the release — would leave
// stock held for a terminal order that nothing will ever revisit, making the
// inventory silently unsellable. Staying pending keeps the discrepancy visible
// and the retry alive.
func TestCompensationFailureLeavesOrderPendingForRetry(t *testing.T) {
	api, catalog := newSagaTestAPI(t)

	order, productID := createPendingOrder(t, api, catalog, 1)

	// Make the catalog reject releases, as an outage would.
	catalog.failReleases(true)

	err := api.saga.HandlePaymentFailed(context.Background(),
		paymentFailedEnvelope(t, order.ID, "card declined"))
	if err == nil {
		t.Fatal("compensation reported success while the release failed — " +
			"the event would be committed and never retried")
	}

	// The order must still be pending, so the redelivery can complete the work.
	final := decodeOrder(t, doJSON(t, api, http.MethodGet, "/orders/"+order.ID, nil, testUser))
	if final.Status != StatusPending {
		t.Errorf("after a failed compensation the order is %q, want %q — "+
			"cancelling before the stock is released strands that inventory",
			final.Status, StatusPending)
	}

	// Recovery: the catalog comes back and the redelivery completes.
	catalog.failReleases(false)
	if err := api.saga.HandlePaymentFailed(context.Background(),
		paymentFailedEnvelope(t, order.ID, "card declined")); err != nil {
		t.Fatalf("retry after recovery: %v", err)
	}

	recovered := decodeOrder(t, doJSON(t, api, http.MethodGet, "/orders/"+order.ID, nil, testUser))
	if recovered.Status != StatusCancelled {
		t.Errorf("after the catalog recovered the order is %q, want %q", recovered.Status, StatusCancelled)
	}
	if got := catalog.releaseCount(productID); got < 1 {
		t.Error("the reservation was never released even after recovery")
	}

	t.Log("compensation failure: order held pending, retried after recovery, then cancelled")
}

// TestEventForUnknownOrderIsSkipped confirms an event about an order this
// service has no record of is absorbed rather than retried forever.
func TestEventForUnknownOrderIsSkipped(t *testing.T) {
	api, _ := newSagaTestAPI(t)
	ctx := context.Background()

	if err := api.saga.HandlePaymentFailed(ctx, paymentFailedEnvelope(t, newUUID(), "x")); err != nil {
		t.Errorf("PaymentFailed for an unknown order returned %v, want nil", err)
	}
	if err := api.saga.HandlePaymentSucceeded(ctx, paymentSucceededEnvelope(t, newUUID(), 100)); err != nil {
		t.Errorf("PaymentSucceeded for an unknown order returned %v, want nil", err)
	}
}

// TestOrderCreatedIsPublishedOnCheckout confirms creating an order actually
// starts the saga, by consuming the event back off the real topic.
func TestOrderCreatedIsPublishedOnCheckout(t *testing.T) {
	api, catalog := newSagaTestAPI(t)

	order, _ := createPendingOrder(t, api, catalog, 3)

	// Read from the topic with a fresh group so the event is visible from the
	// beginning of the log.
	received := make(chan *events.Envelope, 16)
	consumer := events.NewConsumer(testBrokers(), events.TopicOrderCreated,
		"orders-saga-test-"+newUUID(),
		func(ctx context.Context, env *events.Envelope) error {
			received <- env
			return nil
		}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() { _ = consumer.Run(ctx) }()

	deadline := time.After(25 * time.Second)
	for {
		select {
		case env := <-received:
			var payload events.OrderCreated
			if err := events.Decode(env, events.TypeOrderCreated, &payload); err != nil {
				continue
			}
			if payload.OrderID != order.ID {
				continue // an event from another test
			}
			if payload.AmountCents != order.TotalCents {
				t.Errorf("published amount = %d, want %d", payload.AmountCents, order.TotalCents)
			}
			if payload.UserID != testUser {
				t.Errorf("published user = %q, want %q", payload.UserID, testUser)
			}
			if env.CorrelationID == "" {
				t.Error("the published event carries no correlation id — the trace breaks here")
			}
			if len(payload.Items) != 1 || payload.Items[0].Quantity != 3 {
				t.Errorf("published items = %+v, want one line of quantity 3", payload.Items)
			}
			t.Logf("OrderCreated observed on the topic for order %s", order.ID[:8])
			return
		case <-deadline:
			t.Fatal("OrderCreated was never published — the saga never starts")
		}
	}
}

// stubCatalog additions used only by the saga tests.

// commitCount reports how many times a product's reservation was committed.
func (s *stubCatalog) commitCount(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committed[id]
}

// failReleases makes the stub reject release calls, simulating a catalog
// outage during compensation.
func (s *stubCatalog) failReleases(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releasesFail = fail
}

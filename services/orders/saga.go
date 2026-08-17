package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"pkg/correlation"
	"pkg/events"
	"pkg/metrics"
)

// SagaCoordinator is orders' side of the choreographed saga.
//
// Orders is the initiator: it publishes OrderCreated, then reacts to whatever
// payment reports. There is no central orchestrator — each service only reacts
// to events, which is the choreography/orchestration tradeoff documented in the
// Phase 8 ADR.
//
//	PaymentSucceeded -> commit the inventory reservation, confirm the order
//	PaymentFailed    -> release the inventory reservation, cancel the order
type SagaCoordinator struct {
	store     *Store
	catalog   *CatalogClient
	publisher *events.Publisher
	metrics   *metrics.Metrics
	logger    *slog.Logger
}

// NewSagaCoordinator wires orders' event handling.
func NewSagaCoordinator(store *Store, catalog *CatalogClient, publisher *events.Publisher, m *metrics.Metrics, logger *slog.Logger) *SagaCoordinator {
	return &SagaCoordinator{store: store, catalog: catalog, publisher: publisher, metrics: m, logger: logger}
}

// record is a nil-safe metrics helper, since tests construct the coordinator
// without metrics.
func (s *SagaCoordinator) record(fn func(*metrics.Metrics)) {
	if s.metrics != nil {
		fn(s.metrics)
	}
}

// PublishOrderCreated announces a new pending order, starting the saga.
//
// Called after the order row exists and its stock is reserved. That ordering is
// deliberate: publishing first would let payment charge for an order that the
// database then failed to record.
func (s *SagaCoordinator) PublishOrderCreated(ctx context.Context, order *Order) error {
	items := make([]events.OrderItem, 0, len(order.Items))
	for _, it := range order.Items {
		items = append(items, events.OrderItem{ProductID: it.ProductID, Quantity: it.Quantity})
	}

	env, err := events.NewEnvelope(newUUID(), events.TypeOrderCreated, correlation.FromContext(ctx),
		events.OrderCreated{
			OrderID:     order.ID,
			UserID:      order.UserID,
			AmountCents: order.TotalCents,
			Currency:    order.Currency,
			Items:       items,
		})
	if err != nil {
		return fmt.Errorf("building OrderCreated: %w", err)
	}

	if err := s.publisher.Publish(ctx, events.TopicOrderCreated, order.ID, env); err != nil {
		return err
	}
	s.record(func(m *metrics.Metrics) {
		m.RecordEventPublished(events.TopicOrderCreated, events.TypeOrderCreated)
	})
	return nil
}

// HandlePaymentSucceeded confirms an order and consumes its reserved stock.
//
// Order of operations matters. The inventory commit happens first, then the
// status change: if the process dies between them the event is redelivered and
// both steps repeat harmlessly, whereas confirming first would leave a
// confirmed order whose stock was never actually consumed.
func (s *SagaCoordinator) HandlePaymentSucceeded(ctx context.Context, env *events.Envelope) error {
	var payload events.PaymentSucceeded
	if err := events.Decode(env, events.TypePaymentSucceeded, &payload); err != nil {
		s.logger.ErrorContext(ctx, "undecodable PaymentSucceeded, skipping",
			"error", err, "event_id", env.ID)
		return nil
	}

	order, err := s.store.OrderByID(ctx, payload.OrderID)
	if err != nil {
		if errors.Is(err, ErrOrderNotFound) {
			// Nothing this service can do about an order it has no record of,
			// and redelivering forever would block the partition.
			s.logger.ErrorContext(ctx, "PaymentSucceeded for an unknown order, skipping",
				"order_id", payload.OrderID)
			return nil
		}
		return fmt.Errorf("loading order: %w", err)
	}

	// Commit each reservation, turning the hold into a real stock decrement.
	// Catalog's commit is idempotent per (order_id, product_id), so a
	// redelivered event does not consume stock twice.
	for _, item := range order.Items {
		if err := s.catalog.Commit(ctx, order.ID, item.ProductID); err != nil {
			// Leave the offset uncommitted so this is retried. The order stays
			// pending in the meantime, which is the safe state: a confirmed
			// order whose stock was never consumed would oversell later.
			return fmt.Errorf("committing reservation for product %s: %w", item.ProductID, err)
		}
	}

	updated, err := s.store.UpdateStatus(ctx, order.ID, StatusConfirmed, "")
	if err != nil {
		if errors.Is(err, ErrInvalidTransition) {
			// Already terminal. Either this event was redelivered, or a
			// PaymentFailed got here first. The status guard in UpdateStatus is
			// what prevents a late event from flipping a settled order, so this
			// is the guard working rather than a failure.
			s.logger.WarnContext(ctx, "PaymentSucceeded for an order that is no longer pending",
				"order_id", order.ID, "error", err)
			return nil
		}
		return fmt.Errorf("confirming order: %w", err)
	}

	s.logger.InfoContext(ctx, "order confirmed by saga",
		"order_id", updated.ID, "payment_id", payload.PaymentID)
	s.record(func(m *metrics.Metrics) {
		m.RecordEventConsumed(events.TopicPaymentSucceeded, events.TypePaymentSucceeded, "success")
		m.RecordSagaOutcome("confirmed")
	})
	return nil
}

// HandlePaymentFailed compensates: it releases the inventory reservation and
// cancels the order.
//
// This is the compensating transaction the plan requires, and its acceptance
// criterion is that a forced PaymentFailed leaves no orphaned reservation and
// no order stuck pending.
//
// # What happens if compensation itself fails
//
// IMPLEMENTATION_PLAN.md 5 asks for this to be decided rather than left as a
// silent gap. The decision:
//
// If releasing a reservation fails, the handler returns an error. The offset is
// not committed, so the event is redelivered and the release is retried — safe
// because catalog's release is idempotent. The order deliberately stays
// `pending` until every release has succeeded.
//
// The alternative — cancel the order first, then try to release — is worse in a
// specific way: a failed release would leave stock held for an order that no
// longer exists, and nothing in the system would ever revisit it, because the
// order is terminal. That inventory would be silently unsellable. Keeping the
// order pending means the discrepancy stays visible and the retry keeps
// happening.
//
// The cost of this choice is that a permanently failing catalog leaves orders
// stuck pending and their stock held, retried indefinitely. That is the right
// tradeoff at this scale — an operator can see pending orders and a growing
// retry log — but it is not a complete answer: a production system needs a
// dead-letter topic and an alert after N attempts, which the Phase 8 ADR
// records as the known gap.
func (s *SagaCoordinator) HandlePaymentFailed(ctx context.Context, env *events.Envelope) error {
	var payload events.PaymentFailed
	if err := events.Decode(env, events.TypePaymentFailed, &payload); err != nil {
		s.logger.ErrorContext(ctx, "undecodable PaymentFailed, skipping",
			"error", err, "event_id", env.ID)
		return nil
	}

	order, err := s.store.OrderByID(ctx, payload.OrderID)
	if err != nil {
		if errors.Is(err, ErrOrderNotFound) {
			s.logger.ErrorContext(ctx, "PaymentFailed for an unknown order, skipping",
				"order_id", payload.OrderID)
			return nil
		}
		return fmt.Errorf("loading order: %w", err)
	}

	// Release every hold before touching the order's status, for the reason
	// given above.
	for _, item := range order.Items {
		if err := s.catalog.Release(ctx, order.ID, item.ProductID); err != nil {
			s.logger.ErrorContext(ctx, "compensation failed, order stays pending for retry",
				"error", err, "order_id", order.ID, "product_id", item.ProductID)
			s.record(func(m *metrics.Metrics) {
				m.RecordEventConsumed(events.TopicPaymentFailed, events.TypePaymentFailed, "retry")
			})
			return fmt.Errorf("releasing reservation for product %s: %w", item.ProductID, err)
		}
	}

	reason := payload.Reason
	if reason == "" {
		reason = "payment failed"
	}

	updated, err := s.store.UpdateStatus(ctx, order.ID, StatusCancelled, reason)
	if err != nil {
		if errors.Is(err, ErrInvalidTransition) {
			s.logger.WarnContext(ctx, "PaymentFailed for an order that is no longer pending",
				"order_id", order.ID, "error", err)
			return nil
		}
		return fmt.Errorf("cancelling order: %w", err)
	}

	s.logger.InfoContext(ctx, "order cancelled and inventory released by saga",
		"order_id", updated.ID, "reason", reason)
	s.record(func(m *metrics.Metrics) {
		m.RecordEventConsumed(events.TopicPaymentFailed, events.TypePaymentFailed, "success")
		m.RecordSagaOutcome("compensated")
	})
	return nil
}

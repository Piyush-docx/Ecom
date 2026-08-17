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

// SagaConsumer is payment's side of the choreographed saga: it consumes
// OrderCreated, attempts a charge exactly once, and publishes the outcome.
//
// Payment is a saga participant, not the coordinator. It never decides what
// happens to an order — it only reports whether money moved, and orders decides
// the rest.
type SagaConsumer struct {
	store     *Store
	gateway   Gateway
	publisher *events.Publisher
	metrics   *metrics.Metrics
	logger    *slog.Logger
}

// NewSagaConsumer wires payment's event handling.
func NewSagaConsumer(store *Store, gateway Gateway, publisher *events.Publisher, m *metrics.Metrics, logger *slog.Logger) *SagaConsumer {
	return &SagaConsumer{store: store, gateway: gateway, publisher: publisher, metrics: m, logger: logger}
}

// record is a nil-safe metrics helper, since tests construct the consumer
// without metrics.
func (s *SagaConsumer) record(fn func(*metrics.Metrics)) {
	if s.metrics != nil {
		fn(s.metrics)
	}
}

// HandleOrderCreated charges for an order and publishes the result.
//
// This is the handler the Phase 5 acceptance criterion targets: publishing
// OrderCreated twice with the same order ID must result in exactly one charge
// attempt. Kafka is at-least-once, so redelivery is a certainty rather than an
// edge case, and the guarantee comes from the unique index on payments.order_id
// rather than from any check in this function.
//
// Returning an error leaves the offset uncommitted, so the message is
// redelivered — which is why every path here is safe to repeat.
func (s *SagaConsumer) HandleOrderCreated(ctx context.Context, env *events.Envelope) error {
	var order events.OrderCreated
	if err := events.Decode(env, events.TypeOrderCreated, &order); err != nil {
		// A payload that cannot be decoded will never decode on redelivery.
		// Returning nil commits past it rather than blocking the partition
		// forever; the consumer logs it as data loss.
		s.logger.ErrorContext(ctx, "undecodable OrderCreated, skipping",
			"error", err, "event_id", env.ID)
		return nil
	}

	if order.OrderID == "" {
		s.logger.ErrorContext(ctx, "OrderCreated has no order id, skipping", "event_id", env.ID)
		return nil
	}

	// If this order was already settled, republish the same outcome rather than
	// charging again. A redelivery must produce the same result for orders,
	// which may itself have missed the original result event.
	if existing, err := s.store.PaymentByOrderID(ctx, order.OrderID); err == nil {
		s.logger.InfoContext(ctx, "order already has a charge, republishing outcome",
			"order_id", order.OrderID, "status", existing.Status)
		return s.publishOutcome(ctx, existing)
	} else if !errors.Is(err, ErrPaymentNotFound) {
		// A genuine database failure. Returning the error leaves the offset
		// uncommitted so the event is retried rather than silently dropped.
		return fmt.Errorf("looking up existing payment: %w", err)
	}

	status := StatusSucceeded
	failureReason := ""
	if err := s.gateway.Charge(ctx, order.OrderID, order.AmountCents, order.Currency); err != nil {
		status = StatusFailed
		failureReason = err.Error()
	}

	payment, err := s.store.RecordCharge(ctx, Payment{
		ID: newUUID(), OrderID: order.OrderID, UserID: order.UserID,
		AmountCents: order.AmountCents, Currency: order.Currency,
		Status: status, FailureReason: failureReason,
	})
	if err != nil {
		if errors.Is(err, ErrAlreadyCharged) {
			// Another delivery of this event won the race between the lookup
			// above and this insert. The unique index is what makes "exactly
			// one charge" true; this branch is that guarantee being observed.
			existing, lookupErr := s.store.PaymentByOrderID(ctx, order.OrderID)
			if lookupErr != nil {
				return fmt.Errorf("loading the winning charge: %w", lookupErr)
			}
			s.logger.InfoContext(ctx, "concurrent delivery lost the race, republishing the winner",
				"order_id", order.OrderID, "status", existing.Status)
			return s.publishOutcome(ctx, existing)
		}
		return fmt.Errorf("recording charge: %w", err)
	}

	s.logger.InfoContext(ctx, "charge attempted via saga",
		"order_id", payment.OrderID, "status", payment.Status, "amount_cents", payment.AmountCents)
	s.record(func(m *metrics.Metrics) {
		m.RecordEventConsumed(events.TopicOrderCreated, events.TypeOrderCreated, "success")
	})

	return s.publishOutcome(ctx, payment)
}

// publishOutcome emits PaymentSucceeded or PaymentFailed for a settled payment.
//
// A publish failure is returned, so the offset is not committed and the event
// is redelivered. That redelivery finds the payment already recorded and
// republishes without charging again — the charge and the announcement are
// separately idempotent, which is what makes retrying safe.
func (s *SagaConsumer) publishOutcome(ctx context.Context, payment *Payment) error {
	correlationID := correlation.FromContext(ctx)

	if payment.Status == StatusSucceeded {
		env, err := events.NewEnvelope(newUUID(), events.TypePaymentSucceeded, correlationID,
			events.PaymentSucceeded{
				OrderID:     payment.OrderID,
				PaymentID:   payment.ID,
				AmountCents: payment.AmountCents,
				Currency:    payment.Currency,
			})
		if err != nil {
			return fmt.Errorf("building PaymentSucceeded: %w", err)
		}
		// Keyed by order ID so every event about one order shares a partition
		// and stays ordered.
		if err := s.publisher.Publish(ctx, events.TopicPaymentSucceeded, payment.OrderID, env); err != nil {
			return err
		}
		s.record(func(m *metrics.Metrics) {
			m.RecordEventPublished(events.TopicPaymentSucceeded, events.TypePaymentSucceeded)
		})
		return nil
	}

	env, err := events.NewEnvelope(newUUID(), events.TypePaymentFailed, correlationID,
		events.PaymentFailed{
			OrderID: payment.OrderID,
			Reason:  payment.FailureReason,
		})
	if err != nil {
		return fmt.Errorf("building PaymentFailed: %w", err)
	}
	if err := s.publisher.Publish(ctx, events.TopicPaymentFailed, payment.OrderID, env); err != nil {
		return err
	}
	s.record(func(m *metrics.Metrics) {
		m.RecordEventPublished(events.TopicPaymentFailed, events.TypePaymentFailed)
	})
	return nil
}

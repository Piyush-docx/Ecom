// Package events defines the saga's message contract: the topics, the event
// payloads, and the envelope every message is wrapped in.
//
// The saga is choreographed rather than orchestrated (see the Phase 8 ADR):
// there is no central coordinator, only services reacting to each other's
// events.
//
//	orders  -- OrderCreated -->  payment
//	payment -- PaymentSucceeded | PaymentFailed --> orders
//
// Orders then either commits the inventory reservation and confirms the order,
// or releases the reservation and cancels it.
//
// # Why an envelope
//
// Every message carries an ID, a type, a timestamp and a correlation ID
// alongside its payload. The correlation ID is the load-bearing part: Phase 6
// requires reconstructing one request's path across all four services, and an
// event that drops it breaks the chain at exactly the point where a synchronous
// stack trace would have been unavailable anyway.
package events

import (
	"encoding/json"
	"fmt"
	"time"
)

// Topics. One per event type rather than a single shared topic, so a consumer
// subscribes only to what it handles and a malformed payload of one type cannot
// stall the delivery of another.
const (
	TopicOrderCreated     = "order.created"
	TopicPaymentSucceeded = "payment.succeeded"
	TopicPaymentFailed    = "payment.failed"
)

// Event types, carried in the envelope so a consumer can reject a payload that
// arrived on an unexpected topic instead of silently misinterpreting it.
const (
	TypeOrderCreated     = "OrderCreated"
	TypePaymentSucceeded = "PaymentSucceeded"
	TypePaymentFailed    = "PaymentFailed"
)

// Envelope wraps every event.
type Envelope struct {
	// ID uniquely identifies this message. Distinct from the aggregate ID: two
	// deliveries of the same event share an ID, whereas two genuinely separate
	// events about one order do not.
	ID string `json:"id"`

	// Type is one of the Type* constants.
	Type string `json:"type"`

	// CorrelationID ties this event to the HTTP request that ultimately caused
	// it, across every service that handles it.
	CorrelationID string `json:"correlation_id"`

	// OccurredAt is when the event happened, set by the producer.
	//
	// Consumers must not use this for ordering decisions: producers' clocks
	// differ, and IMPLEMENTATION_PLAN.md 5 flags clock skew as a known hazard.
	// Ordering comes from Kafka partitioning, and correctness comes from the
	// idempotency and state guards, never from comparing timestamps.
	OccurredAt time.Time `json:"occurred_at"`

	// Payload is the type-specific body.
	Payload json.RawMessage `json:"payload"`
}

// OrderCreated is published by orders once a pending order exists and its
// inventory is reserved.
//
// It carries everything payment needs to charge, so payment never has to call
// back into orders. A consumer that must query its producer to handle an event
// has a synchronous dependency wearing an asynchronous disguise.
type OrderCreated struct {
	OrderID     string `json:"order_id"`
	UserID      string `json:"user_id"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`

	// Items lets orders compensate without re-reading its own database, and
	// tells any future consumer what was bought.
	Items []OrderItem `json:"items"`
}

// OrderItem is one line of an order.
type OrderItem struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

// PaymentSucceeded is published when a charge is accepted.
type PaymentSucceeded struct {
	OrderID     string `json:"order_id"`
	PaymentID   string `json:"payment_id"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
}

// PaymentFailed is published when a charge is declined or cannot be attempted.
//
// This is the event that triggers compensation: orders releases the inventory
// reservation and cancels the order.
type PaymentFailed struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

// NewEnvelope wraps a payload for publication.
func NewEnvelope(id, eventType, correlationID string, payload any) (*Envelope, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encoding %s payload: %w", eventType, err)
	}
	return &Envelope{
		ID:            id,
		Type:          eventType,
		CorrelationID: correlationID,
		OccurredAt:    time.Now().UTC(),
		Payload:       body,
	}, nil
}

// Decode unmarshals an envelope's payload into v, checking the type first.
//
// The type check is not ceremony. Without it, a PaymentFailed body decoded as
// PaymentSucceeded would silently produce a zero-valued struct, and the saga
// would confirm an order whose payment was declined.
func Decode[T any](e *Envelope, expectedType string, v *T) error {
	if e.Type != expectedType {
		return fmt.Errorf("expected event type %s, got %s", expectedType, e.Type)
	}
	if err := json.Unmarshal(e.Payload, v); err != nil {
		return fmt.Errorf("decoding %s payload: %w", expectedType, err)
	}
	return nil
}

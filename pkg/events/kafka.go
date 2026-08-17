package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"pkg/correlation"
)

// correlationHeader carries the correlation ID as a Kafka message header, in
// addition to the envelope field.
//
// Both, deliberately: the header lets infrastructure and tooling read the trace
// without parsing the body, while the envelope field survives if a message is
// ever republished, archived, or replayed from a store that drops headers.
const correlationHeader = "correlation-id"

// Publisher writes events to Kafka.
type Publisher struct {
	writer *kafka.Writer
	logger *slog.Logger
}

// NewPublisher returns a Publisher for the given brokers.
func NewPublisher(brokers []string, logger *slog.Logger) *Publisher {
	return &Publisher{
		writer: &kafka.Writer{
			Addr: kafka.TCP(brokers...),
			// The topic is set per message, since one publisher emits several
			// event types.
			Balancer: &kafka.Hash{},
			// RequireAll: the write is acknowledged only once every in-sync
			// replica has it. RequireOne would let an acknowledged event vanish
			// with the leader, and an OrderCreated that disappears leaves an
			// order pending forever with its stock held.
			RequiredAcks: kafka.RequireAll,
			// Synchronous writes. Async would return before the broker has the
			// message, so a publish "succeeding" would not mean the saga can
			// proceed -- the caller could commit its own state against an event
			// that was never stored.
			Async:                  false,
			AllowAutoTopicCreation: true,
		},
		logger: logger,
	}
}

// Publish sends an event to a topic, keyed for ordering.
//
// The key must be the order ID. Kafka guarantees ordering only within a
// partition, and the Hash balancer maps a key to a partition, so keying by
// order ID puts every event about one order on one partition in order. Keying
// by anything else -- or not at all -- would let PaymentSucceeded overtake the
// OrderCreated it answers.
func (p *Publisher) Publish(ctx context.Context, topic, key string, env *Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("encoding envelope: %w", err)
	}

	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: body,
		Headers: []kafka.Header{
			{Key: correlationHeader, Value: []byte(env.CorrelationID)},
		},
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publishing to %s: %w", topic, err)
	}

	p.logger.InfoContext(ctx, "event published",
		"topic", topic, "type", env.Type, "key", key, "event_id", env.ID)
	return nil
}

// Close flushes and shuts down the writer.
func (p *Publisher) Close() error { return p.writer.Close() }

// Handler processes one event.
//
// Returning an error means the message was not handled and its offset is not
// committed, so it will be redelivered. Handlers must therefore be idempotent:
// a handler that half-succeeded and then failed will see the same message
// again.
type Handler func(ctx context.Context, env *Envelope) error

// Consumer reads events from one topic and dispatches them to a Handler.
type Consumer struct {
	reader  *kafka.Reader
	handler Handler
	logger  *slog.Logger
	topic   string
}

// NewConsumer returns a Consumer subscribed to topic as part of groupID.
//
// The group ID matters for correctness, not just bookkeeping: all instances of
// one service must share a group so each message is handled once across the
// fleet. Two services consuming the same topic use different groups, so both
// see every message.
func NewConsumer(brokers []string, topic, groupID string, handler Handler, logger *slog.Logger) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 1,
			MaxBytes: 10e6,
			// Start from the beginning for a new group, so an event published
			// before a consumer first started is still processed rather than
			// silently skipped.
			StartOffset: kafka.FirstOffset,
			MaxWait:     500 * time.Millisecond,
		}),
		handler: handler,
		logger:  logger,
		topic:   topic,
	}
}

// Run consumes until ctx is cancelled.
//
// This is the at-least-once loop: FetchMessage reads without committing, the
// handler runs, and only then is the offset committed. A crash between fetch
// and commit redelivers the message, which is why every handler is idempotent.
// The alternative -- ReadMessage, which commits automatically -- would drop a
// message whose handler crashed, and a dropped OrderCreated means a customer
// who is never charged for an order holding real stock.
func (c *Consumer) Run(ctx context.Context) error {
	defer func() {
		if err := c.reader.Close(); err != nil {
			c.logger.Error("closing consumer", "error", err, "topic", c.topic)
		}
	}()

	c.logger.InfoContext(ctx, "consumer started", "topic", c.topic)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				c.logger.InfoContext(ctx, "consumer stopping", "topic", c.topic)
				return nil
			}
			return fmt.Errorf("fetching from %s: %w", c.topic, err)
		}

		if err := c.handle(ctx, msg); err != nil {
			// Not committed, so this message is redelivered. Logging and
			// continuing rather than returning keeps one poisonous message from
			// killing the consumer, but note the message will be retried
			// immediately and indefinitely -- a dead-letter topic is the
			// production answer, and is called out in the Phase 8 ADR.
			c.logger.ErrorContext(ctx, "handling event failed, will be redelivered",
				"error", err, "topic", c.topic, "offset", msg.Offset)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			// The handler succeeded but the offset did not commit, so the
			// message is redelivered and handled again. Idempotency is what
			// makes that safe.
			c.logger.ErrorContext(ctx, "committing offset failed",
				"error", err, "topic", c.topic, "offset", msg.Offset)
		}
	}
}

// handle decodes a message and runs the handler with a correlation-carrying
// context.
func (c *Consumer) handle(ctx context.Context, msg kafka.Message) error {
	var env Envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		// A malformed message can never succeed on redelivery, so committing
		// past it is the only way to make progress. It is logged loudly because
		// this is data loss, however unhandleable the data was.
		c.logger.ErrorContext(ctx, "skipping malformed message",
			"error", err, "topic", c.topic, "offset", msg.Offset)
		if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
			return fmt.Errorf("committing past malformed message: %w", commitErr)
		}
		return nil
	}

	// Rebuild the correlation context so every log line the handler writes is
	// findable under the ID of the request that started all this.
	if env.CorrelationID != "" {
		ctx = correlation.NewContext(ctx, env.CorrelationID)
	}

	c.logger.InfoContext(ctx, "event received",
		"topic", c.topic, "type", env.Type, "event_id", env.ID)

	return c.handler(ctx, &env)
}

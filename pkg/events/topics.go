package events

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// SagaTopics are the topics the saga uses.
var SagaTopics = []string{
	TopicOrderCreated,
	TopicPaymentSucceeded,
	TopicPaymentFailed,
}

// DefaultPartitions is the partition count for saga topics.
//
// Ordering is guaranteed only within a partition, and events are keyed by order
// ID, so every event about one order lands on one partition and stays ordered
// regardless of this number. More partitions buy parallelism across different
// orders; the count is fixed here so it is a deliberate choice rather than
// whatever the broker default happens to be.
const DefaultPartitions = 3

// EnsureTopics creates the saga's topics if they do not exist.
//
// Auto-creation is enabled on the broker, but relying on it is a race: the
// first produce to an unknown topic triggers creation and then fails with
// "Unknown Topic Or Partition" while the metadata propagates, so the very first
// event of a cold start is lost unless the producer retries. Creating topics up
// front removes that race, and lets the partition count be chosen rather than
// inherited.
//
// It is idempotent -- an already-existing topic is not an error -- so every
// service can call it at startup without coordinating.
func EnsureTopics(ctx context.Context, brokers []string, topics []string, partitions, replicationFactor int) error {
	if len(brokers) == 0 {
		return errors.New("no brokers configured")
	}

	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dialing kafka at %s: %w", brokers[0], err)
	}
	defer conn.Close()

	// Topic creation must go through the cluster controller; any other broker
	// rejects the request.
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("locating kafka controller: %w", err)
	}

	controllerConn, err := kafka.DialContext(ctx, "tcp",
		net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dialing kafka controller: %w", err)
	}
	defer controllerConn.Close()

	configs := make([]kafka.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		configs = append(configs, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: replicationFactor,
		})
	}

	if err := controllerConn.CreateTopics(configs...); err != nil {
		return fmt.Errorf("creating topics: %w", err)
	}
	return nil
}

// WaitForTopics blocks until every topic is visible in cluster metadata.
//
// CreateTopics returns as soon as the request is accepted, not once the topic
// is usable, so producing immediately afterwards can still hit "Unknown Topic
// Or Partition". This closes that window.
func WaitForTopics(ctx context.Context, brokers []string, topics []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	want := make(map[string]bool, len(topics))
	for _, t := range topics {
		want[t] = true
	}

	var lastErr error
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}

		partitions, err := conn.ReadPartitions()
		conn.Close()
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}

		found := make(map[string]bool, len(topics))
		for _, p := range partitions {
			if want[p.Topic] {
				found[p.Topic] = true
			}
		}
		if len(found) == len(want) {
			return nil
		}

		lastErr = fmt.Errorf("only %d of %d topics visible", len(found), len(want))
		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("topics not ready after %s: %w", timeout, lastErr)
}

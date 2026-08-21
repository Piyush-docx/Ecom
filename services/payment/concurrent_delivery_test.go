package main

import (
	"context"
	"sync"
	"testing"
)

// TestConcurrentDeliveryChargesOnce is the case the unique index exists for.
// The application-level "have I seen this order?" check in HandleOrderCreated
// cannot close it on its own: two concurrent deliveries both look, both find
// nothing, and both charge.
func TestConcurrentDeliveryChargesOnce(t *testing.T) {
	saga, _ := newSagaFixture(t, StubGateway{})
	_, pool := newTestAPI(t, StubGateway{})

	orderID := newUUID()
	env := orderCreatedEnvelope(t, "evt-"+orderID, orderID, 4999)
	ctx := context.Background()

	const n = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = saga.HandleOrderCreated(ctx, env)
		}()
	}
	close(start)
	wg.Wait()

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM payments WHERE order_id = $1`, orderID).Scan(&count); err != nil {
		t.Fatalf("counting charges: %v", err)
	}
	if count != 1 {
		t.Errorf("%d concurrent deliveries produced %d charge attempts, want exactly 1 — the customer was charged %d times", n, count, count)
	} else {
		t.Logf("%d concurrent deliveries produced exactly 1 charge", n)
	}
}

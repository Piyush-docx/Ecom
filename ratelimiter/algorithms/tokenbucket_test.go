package algorithms_test

import (
	"testing"
	"time"

	"ratelimiter"
	"ratelimiter/algorithms"
)

// TestTokenBucketAbsorbsBurst is the burst-absorption acceptance criterion
// from IMPLEMENTATION_PLAN.md Phase 1. It is what distinguishes the token
// bucket from the window algorithms: a key that has been idle long enough to
// refill may spend its entire capacity in one instant.
func TestTokenBucketAbsorbsBurst(t *testing.T) {
	const (
		capacity = 10
		interval = time.Second
	)

	clk := ratelimiter.NewManualClock(epoch)
	tb, err := algorithms.NewTokenBucket(capacity, interval, clk)
	if err != nil {
		t.Fatalf("constructing token bucket: %v", err)
	}

	// A fresh key starts full, so the whole capacity is spendable with zero
	// time elapsed between requests.
	for i := 1; i <= capacity; i++ {
		if res := allow(t, tb, "burst"); !res.Allowed {
			t.Fatalf("burst request %d of %d: got denied, want allowed", i, capacity)
		}
	}
	if res := allow(t, tb, "burst"); res.Allowed {
		t.Fatal("request past an emptied bucket: got allowed, want denied")
	}

	// After a full interval the bucket has refilled completely, so the same
	// full burst is available again.
	clk.Advance(interval)
	for i := 1; i <= capacity; i++ {
		if res := allow(t, tb, "burst"); !res.Allowed {
			t.Fatalf("second burst request %d of %d: got denied, want allowed — bucket did not refill", i, capacity)
		}
	}
}

// TestTokenBucketRefillsGradually confirms the bucket refills continuously at
// the configured rate rather than all at once at an interval boundary. This is
// the property that makes it smooth out traffic between bursts.
func TestTokenBucketRefillsGradually(t *testing.T) {
	const (
		capacity = 10
		interval = time.Second // 10 tokens/sec => 1 token per 100ms
	)

	clk := ratelimiter.NewManualClock(epoch)
	tb, err := algorithms.NewTokenBucket(capacity, interval, clk)
	if err != nil {
		t.Fatalf("constructing token bucket: %v", err)
	}

	for i := 0; i < capacity; i++ {
		allow(t, tb, "user-1")
	}
	if res := allow(t, tb, "user-1"); res.Allowed {
		t.Fatal("expected an empty bucket")
	}

	// Not quite one token's worth of time: still denied.
	clk.Advance(90 * time.Millisecond)
	if res := allow(t, tb, "user-1"); res.Allowed {
		t.Error("after 90ms (0.9 tokens): got allowed, want denied")
	}

	// Crossing the 100ms mark accrues a whole token: exactly one more request.
	clk.Advance(20 * time.Millisecond) // 110ms total
	if res := allow(t, tb, "user-1"); !res.Allowed {
		t.Error("after 110ms (1.1 tokens): got denied, want allowed")
	}
	if res := allow(t, tb, "user-1"); res.Allowed {
		t.Error("second request after 110ms: got allowed, want denied — only one token had accrued")
	}
}

// TestTokenBucketDoesNotOverfill confirms idle time beyond the refill interval
// does not accumulate tokens past capacity. Without the cap, a key idle for an
// hour would be able to burst far past its configured ceiling.
func TestTokenBucketDoesNotOverfill(t *testing.T) {
	const capacity = 5

	clk := ratelimiter.NewManualClock(epoch)
	tb, err := algorithms.NewTokenBucket(capacity, time.Second, clk)
	if err != nil {
		t.Fatalf("constructing token bucket: %v", err)
	}

	// Touch the key so it has state, then idle far longer than the interval.
	allow(t, tb, "idle")
	clk.Advance(time.Hour)

	allowed := 0
	for i := 0; i < capacity*3; i++ {
		if res := allow(t, tb, "idle"); res.Allowed {
			allowed++
		}
	}
	if allowed != capacity {
		t.Errorf("after an hour idle, allowed = %d, want exactly %d (capacity cap)", allowed, capacity)
	}
}

// TestTokenBucketBackwardsClockDoesNotDrainTokens covers the clock-skew hazard
// in IMPLEMENTATION_PLAN.md §5 from the other direction: a backwards clock jump
// must not *remove* tokens a key has already earned.
//
// The shared backwards-clock test cannot catch this, because it leaves the
// bucket empty — draining an empty bucket is unobservable. Here the bucket is
// deliberately left partly full so a negative refill would be visible.
func TestTokenBucketBackwardsClockDoesNotDrainTokens(t *testing.T) {
	const (
		capacity = 10
		interval = time.Second
	)

	clk := ratelimiter.NewManualClock(epoch)
	tb, err := algorithms.NewTokenBucket(capacity, interval, clk)
	if err != nil {
		t.Fatalf("constructing token bucket: %v", err)
	}

	// Spend 4 of 10, leaving 6 tokens and an established lastSeen.
	const spent = 4
	for i := 0; i < spent; i++ {
		allow(t, tb, "user-1")
	}

	// Jump the clock backwards by several times the refill interval. A refill
	// that trusts a negative elapsed time would subtract tokens here.
	clk.Advance(-5 * interval)

	remaining := 0
	for i := 0; i < capacity; i++ {
		if res := allow(t, tb, "user-1"); res.Allowed {
			remaining++
		}
	}
	if want := capacity - spent; remaining != want {
		t.Errorf("after a backwards clock jump, allowed = %d, want %d — "+
			"the jump must neither grant nor drain tokens", remaining, want)
	}
}

// TestTokenBucketRetryAfterIsAccurate confirms that waiting exactly the
// advertised RetryAfter is sufficient for the next request to succeed. A
// RetryAfter that is too short would send clients into a denied retry loop.
func TestTokenBucketRetryAfterIsAccurate(t *testing.T) {
	const capacity = 4

	clk := ratelimiter.NewManualClock(epoch)
	tb, err := algorithms.NewTokenBucket(capacity, time.Second, clk)
	if err != nil {
		t.Fatalf("constructing token bucket: %v", err)
	}

	for i := 0; i < capacity; i++ {
		allow(t, tb, "user-1")
	}

	res := allow(t, tb, "user-1")
	if res.Allowed {
		t.Fatal("expected the bucket to be empty")
	}
	if res.RetryAfter <= 0 {
		t.Fatalf("RetryAfter = %s, want positive", res.RetryAfter)
	}

	clk.Advance(res.RetryAfter)
	if res := allow(t, tb, "user-1"); !res.Allowed {
		t.Errorf("after waiting the advertised RetryAfter: got denied, want allowed")
	}
}

// TestTokenBucketResetAfter confirms ResetAfter reports the time to a fully
// replenished bucket, which is longer than the time to a single token.
func TestTokenBucketResetAfter(t *testing.T) {
	const (
		capacity = 10
		interval = time.Second
	)

	clk := ratelimiter.NewManualClock(epoch)
	tb, err := algorithms.NewTokenBucket(capacity, interval, clk)
	if err != nil {
		t.Fatalf("constructing token bucket: %v", err)
	}

	// A brand new key is full, so nothing is pending replenishment.
	res := allow(t, tb, "fresh")
	if want := interval / capacity; res.ResetAfter != want {
		t.Errorf("after one request: ResetAfter = %s, want %s (one token's worth)", res.ResetAfter, want)
	}

	// Drain it; a fully empty bucket takes the whole interval to refill.
	for i := 1; i < capacity; i++ {
		res = allow(t, tb, "fresh")
	}
	if res.ResetAfter != interval {
		t.Errorf("with an emptied bucket: ResetAfter = %s, want %s", res.ResetAfter, interval)
	}
}

// Package algorithms holds the in-memory rate-limiting algorithms behind
// ratelimiter.Limiter. Each keeps its own per-key state guarded by a mutex;
// none of them talk to the network. Phase 2 reimplements the same three
// algorithms against Redis.
package algorithms

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"ratelimiter"
)

// TokenBucket allows bursts up to a fixed capacity, refilling continuously at
// a steady rate. State per key is O(1): a token count and a timestamp.
//
// Its defining property is burst tolerance — a key idle long enough to refill
// can spend the whole bucket at once, then is throttled to the refill rate.
// That is the right default for user-facing API traffic, which is bursty by
// nature. It is the wrong choice when a hard ceiling per window must never be
// exceeded; use SlidingWindowLog there.
type TokenBucket struct {
	capacity   int
	refillRate float64 // tokens per second
	clock      ratelimiter.Clock

	mu      sync.Mutex
	buckets map[string]*bucketState
}

type bucketState struct {
	tokens   float64
	lastSeen time.Time
}

// NewTokenBucket returns a token bucket allowing capacity requests in a burst,
// refilling at capacity tokens per interval.
//
// For example capacity=100, interval=time.Minute refills at 100/60 tokens per
// second, so a key spending its full burst waits ~0.6s before one more request
// is permitted, and ~60s to regain the full burst.
//
// If clock is nil, ratelimiter.SystemClock is used. It returns an error for a
// non-positive capacity or interval, both of which would otherwise produce a
// limiter that denies (or divides by zero) rather than limits.
func NewTokenBucket(capacity int, interval time.Duration, clock ratelimiter.Clock) (*TokenBucket, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("token bucket: capacity must be positive, got %d", capacity)
	}
	if interval <= 0 {
		return nil, fmt.Errorf("token bucket: interval must be positive, got %s", interval)
	}
	if clock == nil {
		clock = ratelimiter.SystemClock{}
	}
	return &TokenBucket{
		capacity:   capacity,
		refillRate: float64(capacity) / interval.Seconds(),
		clock:      clock,
		buckets:    make(map[string]*bucketState),
	}, nil
}

// Allow consumes one token from key's bucket if any remain.
func (tb *TokenBucket) Allow(_ context.Context, key string) (ratelimiter.Result, error) {
	now := tb.clock.Now()

	tb.mu.Lock()
	defer tb.mu.Unlock()

	b, ok := tb.buckets[key]
	if !ok {
		// A previously unseen key starts full, so a fresh client gets its
		// whole burst allowance rather than being throttled into a cold start.
		b = &bucketState{tokens: float64(tb.capacity), lastSeen: now}
		tb.buckets[key] = b
	} else {
		tb.refill(b, now)
	}

	res := ratelimiter.Result{Limit: tb.capacity}

	if b.tokens >= 1 {
		b.tokens--
		res.Allowed = true
		res.Remaining = int(b.tokens)
	} else {
		// Denied: report when the bucket accrues its next whole token.
		res.RetryAfter = tb.durationFor(1 - b.tokens)
	}

	res.ResetAfter = tb.durationFor(float64(tb.capacity) - b.tokens)
	return res, nil
}

// refill credits tokens for the time elapsed since the bucket was last
// touched, capped at capacity. The caller must hold tb.mu.
func (tb *TokenBucket) refill(b *bucketState, now time.Time) {
	elapsed := now.Sub(b.lastSeen)
	// A clock that jumps backwards (NTP correction, or a caller whose wall
	// clock skews behind — plan §5) must not drain the bucket. Treat any
	// non-positive elapsed time as no elapsed time.
	if elapsed > 0 {
		b.tokens = math.Min(float64(tb.capacity), b.tokens+elapsed.Seconds()*tb.refillRate)
	}
	b.lastSeen = now
}

// durationFor returns how long it takes to accrue tokens at the refill rate.
func (tb *TokenBucket) durationFor(tokens float64) time.Duration {
	if tokens <= 0 {
		return 0
	}
	return time.Duration(tokens / tb.refillRate * float64(time.Second))
}

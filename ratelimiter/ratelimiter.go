// Package ratelimiter defines the interface shared by every rate-limiting
// algorithm in this project, plus the clock abstraction they depend on.
//
// Phase 1 implementations (see algorithms/) keep all state in memory. Phase 2
// ports that state into Redis behind the same interface, so callers — the
// gateway middleware in particular — never learn which backend is in use.
package ratelimiter

import (
	"context"
	"time"
)

// Limiter decides whether a single request against key is allowed.
//
// Implementations must be safe for concurrent use by multiple goroutines.
//
// The ctx parameter is unused by the in-memory implementations but is part of
// the interface from the start: the Redis-backed implementations in Phase 2
// issue network calls that must honor cancellation and deadlines.
type Limiter interface {
	Allow(ctx context.Context, key string) (Result, error)
}

// Result reports the outcome of one Allow call.
//
// The fields map directly onto the response headers the gateway sets in
// Phase 3, which is why they are carried even when a request is allowed:
//
//	X-RateLimit-Limit     <- Limit
//	X-RateLimit-Remaining <- Remaining
//	X-RateLimit-Reset     <- ResetAfter
//	Retry-After           <- RetryAfter (denied responses only)
type Result struct {
	// Allowed reports whether the request may proceed.
	Allowed bool

	// Limit is the configured ceiling for the key, echoed back so callers
	// don't need their own copy of the configuration.
	Limit int

	// Remaining is the number of further requests that would be allowed if
	// they arrived at the same instant as this one. It is zero on denial and
	// never negative.
	Remaining int

	// ResetAfter is how long until the key returns to full capacity: an empty
	// bucket for the token bucket, or an expired window for the window-based
	// algorithms. It is zero when the key is already at full capacity.
	ResetAfter time.Duration

	// RetryAfter is how long the caller should wait before retrying. It is
	// meaningful only when Allowed is false, and is zero otherwise.
	//
	// It differs from ResetAfter: RetryAfter is when *one* request would be
	// permitted again, while ResetAfter is when the key is fully replenished.
	RetryAfter time.Duration
}

// Clock supplies the current time to an algorithm.
//
// This is an interface rather than a direct time.Now call for two reasons.
// Tests need deterministic control over window boundaries — the sliding
// window counter's boundary behavior is impossible to assert reliably against
// a real clock. And IMPLEMENTATION_PLAN.md §5 identifies clock skew across
// distributed callers as a known hard problem: in Phase 2 this abstraction is
// what lets the Redis implementations read Redis's own TIME inside the Lua
// script instead of trusting each gateway instance's wall clock.
type Clock interface {
	Now() time.Time
}

// SystemClock reads the host's wall clock. It is the default for production
// use in Phase 1.
type SystemClock struct{}

// Now returns the current local time.
func (SystemClock) Now() time.Time { return time.Now() }

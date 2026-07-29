package algorithms

import (
	"context"
	"fmt"
	"sync"
	"time"

	"ratelimiter"
)

// SlidingWindowLog records the timestamp of every allowed request and counts
// how many fall inside the trailing window. It is exact: at no instant can
// more than limit requests in the preceding window have been allowed, with no
// boundary artifacts of any kind.
//
// The cost of that exactness is O(n) memory per key, where n is the limit —
// it stores one timestamp per allowed request. A limit of 10,000 requests per
// minute means 10,000 timestamps retained per key. Prefer SlidingWindowCounter
// when the limit is large and an approximation is acceptable; prefer this when
// the ceiling is a hard contractual guarantee.
type SlidingWindowLog struct {
	limit  int
	window time.Duration
	clock  ratelimiter.Clock

	mu   sync.Mutex
	logs map[string][]time.Time
}

// NewSlidingWindowLog returns a limiter permitting limit requests in any
// trailing window.
//
// If clock is nil, ratelimiter.SystemClock is used.
func NewSlidingWindowLog(limit int, window time.Duration, clock ratelimiter.Clock) (*SlidingWindowLog, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("sliding window log: limit must be positive, got %d", limit)
	}
	if window <= 0 {
		return nil, fmt.Errorf("sliding window log: window must be positive, got %s", window)
	}
	if clock == nil {
		clock = ratelimiter.SystemClock{}
	}
	return &SlidingWindowLog{
		limit:  limit,
		window: window,
		clock:  clock,
		logs:   make(map[string][]time.Time),
	}, nil
}

// Allow records a request against key if fewer than limit requests fall within
// the trailing window.
func (s *SlidingWindowLog) Allow(_ context.Context, key string) (ratelimiter.Result, error) {
	now := s.clock.Now()
	cutoff := now.Add(-s.window)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Drop timestamps that have aged out of the window. Entries are appended
	// in clock order, so the survivors are a suffix of the slice — but a
	// backwards clock jump could break that ordering, so find the boundary by
	// search rather than assuming a sorted slice.
	entries := s.logs[key]
	kept := entries[:0]
	for _, ts := range entries {
		if ts.After(cutoff) {
			kept = append(kept, ts)
		}
	}
	entries = kept

	res := ratelimiter.Result{Limit: s.limit}

	if len(entries) < s.limit {
		entries = append(entries, now)
		res.Allowed = true
		res.Remaining = s.limit - len(entries)
	} else {
		// Denied: the oldest in-window request is the one whose expiry frees a
		// slot, so that is when a retry can succeed.
		res.RetryAfter = s.clamp(oldest(entries).Add(s.window).Sub(now))
	}

	if len(entries) > 0 {
		// The key is fully replenished once the newest entry ages out.
		res.ResetAfter = s.clamp(newest(entries).Add(s.window).Sub(now))
	}

	s.logs[key] = entries
	return res, nil
}

// clamp bounds a reported duration to [0, window].
//
// An entry recorded under a clock that has since jumped backwards sits in the
// future relative to now, which would otherwise yield a duration longer than
// the window itself — an impossible value for the X-RateLimit-Reset and
// Retry-After headers the gateway derives from these fields in Phase 3.
func (s *SlidingWindowLog) clamp(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	if d > s.window {
		return s.window
	}
	return d
}

// oldest returns the earliest timestamp in a non-empty slice.
func oldest(ts []time.Time) time.Time {
	m := ts[0]
	for _, t := range ts[1:] {
		if t.Before(m) {
			m = t
		}
	}
	return m
}

// newest returns the latest timestamp in a non-empty slice.
func newest(ts []time.Time) time.Time {
	m := ts[0]
	for _, t := range ts[1:] {
		if t.After(m) {
			m = t
		}
	}
	return m
}

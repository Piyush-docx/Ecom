package algorithms

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"ratelimiter"
)

// SlidingWindowCounter approximates a sliding window using two fixed-window
// counters: the current one and its predecessor. It keeps O(1) state per key —
// two integers and a window index — while avoiding the boundary flaw of a
// plain fixed-window counter.
//
// A plain fixed window permits a burst of `limit` at the very end of one
// window and another `limit` at the start of the next, letting 2*limit through
// in an instant that straddles the boundary. This algorithm closes that hole
// by charging a decaying fraction of the previous window's count against the
// current one.
//
// # The estimate
//
// With f as the fraction of the current fixed window already elapsed, the
// estimated count over the trailing window is:
//
//	estimate = prev*(1-f) + curr
//
// At f=0 the previous window is counted in full; at f=1 it has decayed out
// entirely. The estimate assumes the previous window's requests were spread
// uniformly across it, which is what makes this an approximation: a previous
// window whose requests were all clustered at its start is over-counted (the
// limiter is stricter than reality), and one clustered at its end is
// under-counted (slightly more permissive than a true sliding window).
// Cloudflare's published analysis of this tradeoff put the error under 1% at
// production traffic volumes, which is why it is the common default at scale.
//
// Prefer SlidingWindowLog when the ceiling must be exact.
type SlidingWindowCounter struct {
	limit  int
	window time.Duration
	clock  ratelimiter.Clock

	mu      sync.Mutex
	windows map[string]*counterState
}

type counterState struct {
	// windowStart is the start instant of the current fixed window.
	windowStart time.Time
	curr        int
	prev        int
}

// NewSlidingWindowCounter returns a limiter approximating limit requests per
// trailing window.
//
// If clock is nil, ratelimiter.SystemClock is used.
func NewSlidingWindowCounter(limit int, window time.Duration, clock ratelimiter.Clock) (*SlidingWindowCounter, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("sliding window counter: limit must be positive, got %d", limit)
	}
	if window <= 0 {
		return nil, fmt.Errorf("sliding window counter: window must be positive, got %s", window)
	}
	if clock == nil {
		clock = ratelimiter.SystemClock{}
	}
	return &SlidingWindowCounter{
		limit:   limit,
		window:  window,
		clock:   clock,
		windows: make(map[string]*counterState),
	}, nil
}

// Allow charges one request against key if the weighted estimate permits it.
func (s *SlidingWindowCounter) Allow(_ context.Context, key string) (ratelimiter.Result, error) {
	now := s.clock.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	st, ok := s.windows[key]
	if !ok {
		st = &counterState{windowStart: now}
		s.windows[key] = st
	}
	s.roll(st, now)

	elapsed := now.Sub(st.windowStart)
	// Fraction of the current fixed window already elapsed, in [0,1).
	f := float64(elapsed) / float64(s.window)
	estimate := float64(st.prev)*(1-f) + float64(st.curr)

	res := ratelimiter.Result{Limit: s.limit}

	// Compare against limit using the estimate *before* this request is
	// counted: the request is admitted only if it fits under the ceiling.
	if estimate+1 <= float64(s.limit) {
		st.curr++
		res.Allowed = true
		if rem := float64(s.limit) - (estimate + 1); rem > 0 {
			res.Remaining = int(rem)
		}
	} else {
		res.RetryAfter = s.retryAfter(st, elapsed)
	}

	// The key is fully clear once the current window ends and the previous
	// window's contribution has fully decayed.
	res.ResetAfter = s.window - elapsed
	return res, nil
}

// roll advances the key's fixed windows to cover now. The caller must hold
// s.mu.
func (s *SlidingWindowCounter) roll(st *counterState, now time.Time) {
	elapsed := now.Sub(st.windowStart)
	switch {
	case elapsed < 0:
		// Clock moved backwards (plan §5). Re-anchor rather than computing a
		// negative window index, which would corrupt the weighting.
		st.windowStart = now
	case elapsed < s.window:
		// Still inside the current window; nothing to roll.
	case elapsed < 2*s.window:
		// Advanced exactly one window: today's count becomes yesterday's.
		st.prev = st.curr
		st.curr = 0
		st.windowStart = st.windowStart.Add(s.window)
	default:
		// Two or more windows of silence: both counters have aged out. Anchor
		// the new window to the boundary grid rather than to now, so window
		// starts stay aligned across a key's lifetime.
		skipped := elapsed / s.window
		st.prev = 0
		st.curr = 0
		st.windowStart = st.windowStart.Add(skipped * s.window)
	}
}

// retryAfter estimates when the decaying previous-window contribution will
// have fallen far enough for one request to fit. The caller must hold s.mu.
func (s *SlidingWindowCounter) retryAfter(st *counterState, elapsed time.Duration) time.Duration {
	// Time until the current fixed window ends.
	remainder := s.window - elapsed

	// With the previous window empty, relief comes only after the rollover —
	// but not *at* it. At the boundary this window's count becomes the
	// previous one, which at f=0 still counts in full, so the estimate is
	// unchanged. It decays as curr*(1-f), and one slot opens once
	//   curr*(1-f) + 1 <= limit,
	// i.e. f >= (curr-limit+1)/curr. Advertise the boundary plus that much of
	// the next window, rounded up so a client honoring the value exactly is
	// admitted rather than denied a fraction of a tick early.
	if st.prev == 0 {
		if st.curr == 0 {
			// No traffic in either window; nothing to wait for.
			return remainder
		}
		f := float64(st.curr-s.limit+1) / float64(st.curr)
		if f < 0 {
			f = 0
		}
		return remainder + time.Duration(math.Ceil(f*float64(s.window)))
	}

	// Solve for the elapsed fraction f at which the estimate drops to limit-1:
	//   prev*(1-f) + curr = limit - 1
	// Any f beyond the end of the current window is capped at the rollover.
	target := float64(s.limit) - 1 - float64(st.curr)
	if target < 0 {
		// curr alone already exceeds the ceiling; wait for the rollover.
		return remainder
	}
	f := 1 - target/float64(st.prev)
	if f >= 1 {
		return remainder
	}

	// Round the threshold up. Truncating toward zero here would advertise a
	// wait a fraction short of the moment the slot actually opens, so a client
	// honoring Retry-After to the nanosecond would be denied again and retry
	// in a loop.
	wait := time.Duration(math.Ceil(f*float64(s.window))) - elapsed
	if wait <= 0 {
		// The threshold is already met at this instant but the request still
		// did not fit due to rounding; retry on the next tick rather than
		// reporting a zero wait that would invite an immediate hot retry.
		return time.Millisecond
	}
	if wait > remainder {
		return remainder
	}
	return wait
}

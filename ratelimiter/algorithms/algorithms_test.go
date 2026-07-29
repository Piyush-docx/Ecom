package algorithms_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"ratelimiter"
	"ratelimiter/algorithms"
)

// epoch is an arbitrary fixed instant. Tests anchor the manual clock here so
// no assertion depends on the wall clock.
var epoch = time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)

// constructor builds one limiter under test at the given limit and window.
type constructor struct {
	name string
	new  func(limit int, window time.Duration, clk ratelimiter.Clock) (ratelimiter.Limiter, error)
}

// all returns every algorithm, so the shared behaviors below run against each.
func all() []constructor {
	return []constructor{
		{
			name: "TokenBucket",
			new: func(limit int, window time.Duration, clk ratelimiter.Clock) (ratelimiter.Limiter, error) {
				return algorithms.NewTokenBucket(limit, window, clk)
			},
		},
		{
			name: "SlidingWindowLog",
			new: func(limit int, window time.Duration, clk ratelimiter.Clock) (ratelimiter.Limiter, error) {
				return algorithms.NewSlidingWindowLog(limit, window, clk)
			},
		},
		{
			name: "SlidingWindowCounter",
			new: func(limit int, window time.Duration, clk ratelimiter.Clock) (ratelimiter.Limiter, error) {
				return algorithms.NewSlidingWindowCounter(limit, window, clk)
			},
		},
	}
}

// allow issues one request and fails the test on an unexpected error.
func allow(t *testing.T, l ratelimiter.Limiter, key string) ratelimiter.Result {
	t.Helper()
	res, err := l.Allow(context.Background(), key)
	if err != nil {
		t.Fatalf("Allow(%q) returned unexpected error: %v", key, err)
	}
	return res
}

// TestAtAndOverLimit covers the two headline acceptance criteria from
// IMPLEMENTATION_PLAN.md Phase 1: a request at exactly the limit is allowed,
// and the next one over the limit is denied. Every algorithm must agree here —
// they differ in how they age state out, not in where the ceiling sits.
func TestAtAndOverLimit(t *testing.T) {
	const (
		limit  = 5
		window = time.Minute
	)

	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			clk := ratelimiter.NewManualClock(epoch)
			l, err := c.new(limit, window, clk)
			if err != nil {
				t.Fatalf("constructing limiter: %v", err)
			}

			// Requests 1..limit must all be allowed, with no time passing, so
			// no algorithm can replenish mid-test.
			for i := 1; i <= limit; i++ {
				res := allow(t, l, "user-1")
				if !res.Allowed {
					t.Fatalf("request %d of %d: got denied, want allowed", i, limit)
				}
				if want := limit - i; res.Remaining != want {
					t.Errorf("request %d: Remaining = %d, want %d", i, res.Remaining, want)
				}
				if res.Limit != limit {
					t.Errorf("request %d: Limit = %d, want %d", i, res.Limit, limit)
				}
				if res.RetryAfter != 0 {
					t.Errorf("request %d: RetryAfter = %s on an allowed request, want 0", i, res.RetryAfter)
				}
			}

			// One over the limit must be denied.
			res := allow(t, l, "user-1")
			if res.Allowed {
				t.Fatalf("request %d of %d: got allowed, want denied", limit+1, limit)
			}
			if res.Remaining != 0 {
				t.Errorf("denied request: Remaining = %d, want 0", res.Remaining)
			}
			if res.RetryAfter <= 0 {
				t.Errorf("denied request: RetryAfter = %s, want a positive duration", res.RetryAfter)
			}
			// The sliding window counter can legitimately need the rest of the
			// current window plus part of the next before a slot opens, so the
			// bound is two windows rather than one.
			if res.RetryAfter > 2*window {
				t.Errorf("denied request: RetryAfter = %s, want no more than %s", res.RetryAfter, 2*window)
			}

			// The property that actually matters for the Retry-After header in
			// Phase 3: a client that waits exactly as long as it was told must
			// then be admitted. An under-estimate here puts clients into a
			// denied-retry loop.
			waited := res.RetryAfter
			clk.Advance(waited)
			if retried := allow(t, l, "user-1"); !retried.Allowed {
				t.Errorf("after waiting the advertised RetryAfter of %s: got denied, want allowed", waited)
			}
		})
	}
}

// TestKeysAreIndependent confirms one key's exhaustion does not affect another.
// Without this, a single noisy client would throttle everyone — the limiter
// would be global rather than per-key.
func TestKeysAreIndependent(t *testing.T) {
	const limit = 3

	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			clk := ratelimiter.NewManualClock(epoch)
			l, err := c.new(limit, time.Minute, clk)
			if err != nil {
				t.Fatalf("constructing limiter: %v", err)
			}

			for i := 0; i < limit; i++ {
				if res := allow(t, l, "noisy"); !res.Allowed {
					t.Fatalf("noisy request %d: got denied, want allowed", i+1)
				}
			}
			if res := allow(t, l, "noisy"); res.Allowed {
				t.Fatal("noisy over limit: got allowed, want denied")
			}

			// A different key must still have its full allowance.
			for i := 0; i < limit; i++ {
				if res := allow(t, l, "quiet"); !res.Allowed {
					t.Fatalf("quiet request %d: got denied, want allowed — keys are not independent", i+1)
				}
			}
		})
	}
}

// TestFullWindowElapsedReplenishes confirms that after a full window of
// silence, a key regains its entire allowance under every algorithm.
func TestFullWindowElapsedReplenishes(t *testing.T) {
	const (
		limit  = 4
		window = time.Minute
	)

	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			clk := ratelimiter.NewManualClock(epoch)
			l, err := c.new(limit, window, clk)
			if err != nil {
				t.Fatalf("constructing limiter: %v", err)
			}

			for i := 0; i < limit; i++ {
				allow(t, l, "user-1")
			}
			if res := allow(t, l, "user-1"); res.Allowed {
				t.Fatal("expected the key to be exhausted before advancing the clock")
			}

			// Advance well past the window so every algorithm's state has aged
			// out: the bucket refills, the log entries expire, and the counter
			// rolls both of its windows.
			clk.Advance(2 * window)

			for i := 0; i < limit; i++ {
				if res := allow(t, l, "user-1"); !res.Allowed {
					t.Fatalf("after a full window elapsed, request %d: got denied, want allowed", i+1)
				}
			}
		})
	}
}

// TestConstructorRejectsInvalidConfig checks that a misconfigured limiter fails
// loudly at construction rather than silently denying every request (limit <= 0)
// or dividing by a zero interval.
func TestConstructorRejectsInvalidConfig(t *testing.T) {
	cases := []struct {
		name   string
		limit  int
		window time.Duration
	}{
		{"zero limit", 0, time.Minute},
		{"negative limit", -1, time.Minute},
		{"zero window", 10, 0},
		{"negative window", 10, -time.Second},
	}

	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					if _, err := c.new(tc.limit, tc.window, nil); err == nil {
						t.Errorf("new(limit=%d, window=%s) returned no error, want one", tc.limit, tc.window)
					}
				})
			}
		})
	}
}

// TestNilClockDefaultsToSystemClock confirms a nil clock is a usable default
// rather than a nil-pointer panic on the first request.
func TestNilClockDefaultsToSystemClock(t *testing.T) {
	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			l, err := c.new(1, time.Minute, nil)
			if err != nil {
				t.Fatalf("constructing limiter: %v", err)
			}
			if res := allow(t, l, "user-1"); !res.Allowed {
				t.Error("first request with a nil clock: got denied, want allowed")
			}
		})
	}
}

// TestConcurrentAllowIsExact fires goroutines at a single key simultaneously
// and requires that exactly `limit` get through.
//
// Phase 2 repeats this test against Redis, where it is the proof of the
// atomicity claim. Here it proves the in-memory mutex discipline is correct —
// a check-then-act split across the lock would show up as a count above the
// limit under -race.
func TestConcurrentAllowIsExact(t *testing.T) {
	const (
		limit    = 50
		requests = 500
	)

	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			clk := ratelimiter.NewManualClock(epoch)
			l, err := c.new(limit, time.Minute, clk)
			if err != nil {
				t.Fatalf("constructing limiter: %v", err)
			}

			var (
				wg      sync.WaitGroup
				mu      sync.Mutex
				allowed int
			)
			// The clock never advances here, so no replenishment can occur and
			// the expected count is exactly the limit.
			for i := 0; i < requests; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					res, err := l.Allow(context.Background(), "hot-key")
					if err != nil {
						return
					}
					if res.Allowed {
						mu.Lock()
						allowed++
						mu.Unlock()
					}
				}()
			}
			wg.Wait()

			if allowed != limit {
				t.Errorf("concurrent requests allowed = %d, want exactly %d", allowed, limit)
			}
		})
	}
}

// TestBackwardsClockDoesNotGrantExtraAllowance guards the clock-skew hazard
// called out in IMPLEMENTATION_PLAN.md §5. A clock that jumps backwards must
// never hand a key more allowance than it had.
func TestBackwardsClockDoesNotGrantExtraAllowance(t *testing.T) {
	const (
		limit  = 3
		window = time.Minute
	)

	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			clk := ratelimiter.NewManualClock(epoch)
			l, err := c.new(limit, window, clk)
			if err != nil {
				t.Fatalf("constructing limiter: %v", err)
			}

			for i := 0; i < limit; i++ {
				allow(t, l, "user-1")
			}

			// Jump the clock backwards, then confirm the key is still spent.
			clk.Advance(-30 * time.Second)
			if res := allow(t, l, "user-1"); res.Allowed {
				t.Error("after a backwards clock jump: got allowed, want denied")
			}
		})
	}
}

// TestResultLimitAlwaysEchoed confirms Limit is populated on every response,
// allowed or denied. Phase 3 sets X-RateLimit-Limit from this field on every
// response, so a zero here would surface as a wrong header in production.
func TestResultLimitAlwaysEchoed(t *testing.T) {
	const limit = 2

	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			clk := ratelimiter.NewManualClock(epoch)
			l, err := c.new(limit, time.Minute, clk)
			if err != nil {
				t.Fatalf("constructing limiter: %v", err)
			}

			for i := 0; i < limit+2; i++ {
				res := allow(t, l, "user-1")
				if res.Limit != limit {
					t.Errorf("request %d: Limit = %d, want %d", i+1, res.Limit, limit)
				}
				if res.Remaining < 0 {
					t.Errorf("request %d: Remaining = %d, want non-negative", i+1, res.Remaining)
				}
			}
		})
	}
}

// TestLimiterInterfaceSatisfied is a compile-time check that each algorithm
// still satisfies ratelimiter.Limiter.
func TestLimiterInterfaceSatisfied(t *testing.T) {
	for _, c := range all() {
		l, err := c.new(1, time.Minute, nil)
		if err != nil {
			t.Fatalf("%s: constructing limiter: %v", c.name, err)
		}
		var _ ratelimiter.Limiter = l
	}
}

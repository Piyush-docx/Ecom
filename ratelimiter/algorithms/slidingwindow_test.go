package algorithms_test

import (
	"testing"
	"time"

	"ratelimiter"
	"ratelimiter/algorithms"
)

// TestSlidingWindowCounterBoundaryDoesNotDoubleLimit is the window-boundary
// acceptance criterion from IMPLEMENTATION_PLAN.md Phase 1, and the case
// AGENTS.md flags as the easiest to get subtly wrong.
//
// The failure mode being excluded is the plain fixed-window counter's: spend
// the full limit at the end of one window, then the full limit again just
// after the boundary, and 2*limit requests land within a span shorter than one
// window. The weighted previous-window term must prevent that.
func TestSlidingWindowCounterBoundaryDoesNotDoubleLimit(t *testing.T) {
	const (
		limit  = 10
		window = time.Minute
	)

	clk := ratelimiter.NewManualClock(epoch)
	swc, err := algorithms.NewSlidingWindowCounter(limit, window, clk)
	if err != nil {
		t.Fatalf("constructing sliding window counter: %v", err)
	}

	// The key's fixed window is anchored at its first request, so establish
	// that anchor at epoch before doing anything else. Without this, the burst
	// below would itself anchor the window and the test would never cross a
	// boundary at all.
	if res := allow(t, swc, "user-1"); !res.Allowed {
		t.Fatalf("anchoring request: got denied, want allowed")
	}

	// Spend the rest of the limit in the last moment of that first window.
	clk.Advance(window - time.Second)
	for i := 2; i <= limit; i++ {
		if res := allow(t, swc, "user-1"); !res.Allowed {
			t.Fatalf("first-window request %d of %d: got denied, want allowed", i, limit)
		}
	}

	// Step just past the boundary into the next fixed window, so prev=limit
	// and the current window's counter is freshly zeroed. A naive fixed window
	// resets here and would allow another full `limit`.
	clk.Advance(2 * time.Second)

	allowedAfterBoundary := 0
	for i := 0; i < limit; i++ {
		if res := allow(t, swc, "user-1"); res.Allowed {
			allowedAfterBoundary++
		}
	}

	// Only ~1/60th of the previous window has decayed, so almost the entire
	// previous count still counts against the ceiling. A couple of requests
	// slipping through is the algorithm's known approximation error; anything
	// close to a second full burst is the bug this test exists to catch.
	if allowedAfterBoundary > 2 {
		t.Errorf("allowed %d requests immediately after the window boundary, want at most 2 — "+
			"the previous window's count is not being weighted against the current one",
			allowedAfterBoundary)
	}

	// The headline guarantee: the total across the boundary stays near the
	// limit, nowhere near 2*limit.
	total := limit + allowedAfterBoundary
	if total >= 2*limit {
		t.Errorf("total allowed across the boundary = %d, want well under %d (2x the limit)", total, 2*limit)
	}
}

// TestSlidingWindowCounterDecaysPreviousWindow confirms the previous window's
// contribution shrinks as the current window progresses, rather than dropping
// all at once. Halfway through the next window, roughly half the previous
// window's allowance should have freed up.
func TestSlidingWindowCounterDecaysPreviousWindow(t *testing.T) {
	const (
		limit  = 10
		window = time.Minute
	)

	clk := ratelimiter.NewManualClock(epoch)
	swc, err := algorithms.NewSlidingWindowCounter(limit, window, clk)
	if err != nil {
		t.Fatalf("constructing sliding window counter: %v", err)
	}

	// Fill the first window completely.
	for i := 0; i < limit; i++ {
		allow(t, swc, "user-1")
	}

	// Move to the midpoint of the second window: the previous window is
	// weighted at 1-0.5 = 0.5, so estimate = 10*0.5 = 5, leaving room for 5.
	clk.Advance(window + window/2)

	allowed := 0
	for i := 0; i < limit; i++ {
		if res := allow(t, swc, "user-1"); res.Allowed {
			allowed++
		}
	}

	if allowed < 4 || allowed > 6 {
		t.Errorf("at the midpoint of the next window, allowed = %d, want ~5 "+
			"(half the previous window's count should have decayed)", allowed)
	}
}

// TestSlidingWindowCounterSteadyStateHoldsLimit confirms that sustained
// traffic across many windows never exceeds the configured rate on average.
func TestSlidingWindowCounterSteadyStateHoldsLimit(t *testing.T) {
	const (
		limit  = 10
		window = time.Minute
		cycles = 10
	)

	clk := ratelimiter.NewManualClock(epoch)
	swc, err := algorithms.NewSlidingWindowCounter(limit, window, clk)
	if err != nil {
		t.Fatalf("constructing sliding window counter: %v", err)
	}

	totalAllowed := 0
	// Hammer well past the limit in every window for several windows.
	for c := 0; c < cycles; c++ {
		for i := 0; i < limit*3; i++ {
			if res := allow(t, swc, "user-1"); res.Allowed {
				totalAllowed++
			}
		}
		clk.Advance(window)
	}

	// Over `cycles` windows the ceiling is cycles*limit. Sustained saturation
	// should stay at or under that, never above.
	if max := cycles * limit; totalAllowed > max {
		t.Errorf("over %d saturated windows, allowed = %d, want no more than %d", cycles, totalAllowed, max)
	}
}

// TestSlidingWindowCounterRetryAfter exercises the RetryAfter paths for a
// denied request. Phase 3 puts this value straight into the Retry-After
// header, so it must be positive, never exceed the window, and — most
// importantly — actually be long enough that a client honoring it succeeds.
func TestSlidingWindowCounterRetryAfter(t *testing.T) {
	const (
		limit  = 10
		window = time.Minute
	)

	t.Run("denied within the first window", func(t *testing.T) {
		// No previous window exists yet (prev == 0), so the only relief is the
		// rollover into the next window.
		clk := ratelimiter.NewManualClock(epoch)
		swc, err := algorithms.NewSlidingWindowCounter(limit, window, clk)
		if err != nil {
			t.Fatalf("constructing sliding window counter: %v", err)
		}

		for i := 0; i < limit; i++ {
			allow(t, swc, "user-1")
		}
		res := allow(t, swc, "user-1")
		if res.Allowed {
			t.Fatal("expected the key to be exhausted")
		}
		// A denial early in a window may have to wait out the rest of that
		// window plus part of the next, so the bound is two windows.
		if res.RetryAfter <= 0 || res.RetryAfter > 2*window {
			t.Errorf("RetryAfter = %s, want positive and no more than %s", res.RetryAfter, 2*window)
		}

		clk.Advance(res.RetryAfter)
		if res := allow(t, swc, "user-1"); !res.Allowed {
			t.Error("after waiting the advertised RetryAfter: got denied, want allowed")
		}
	})

	t.Run("denied with a saturated previous window", func(t *testing.T) {
		// prev is non-zero here, so RetryAfter solves for the moment the
		// decaying previous-window term frees a slot.
		clk := ratelimiter.NewManualClock(epoch)
		swc, err := algorithms.NewSlidingWindowCounter(limit, window, clk)
		if err != nil {
			t.Fatalf("constructing sliding window counter: %v", err)
		}

		for i := 0; i < limit; i++ {
			allow(t, swc, "user-1")
		}
		// Roll into the next window: prev = limit, curr = 0.
		clk.Advance(window)

		res := allow(t, swc, "user-1")
		if res.Allowed {
			t.Fatal("expected denial immediately after the rollover with a full previous window")
		}
		if res.RetryAfter <= 0 || res.RetryAfter > window {
			t.Fatalf("RetryAfter = %s, want positive and no more than %s", res.RetryAfter, window)
		}

		clk.Advance(res.RetryAfter)
		if res := allow(t, swc, "user-1"); !res.Allowed {
			t.Error("after waiting the advertised RetryAfter: got denied, want allowed")
		}
	})

	t.Run("denied with the current window already at the ceiling", func(t *testing.T) {
		// prev is non-zero and curr alone has reached the limit, so no amount
		// of decay within this window helps: RetryAfter must be the rollover.
		clk := ratelimiter.NewManualClock(epoch)
		swc, err := algorithms.NewSlidingWindowCounter(limit, window, clk)
		if err != nil {
			t.Fatalf("constructing sliding window counter: %v", err)
		}

		// Put a small count in the first window so prev is non-zero later.
		allow(t, swc, "user-1")
		clk.Advance(window)

		// Fill the current window to the ceiling.
		allowed := 0
		for i := 0; i < limit*2; i++ {
			if res := allow(t, swc, "user-1"); res.Allowed {
				allowed++
			}
		}
		if allowed == 0 {
			t.Fatal("expected some requests to be allowed in the second window")
		}

		res := allow(t, swc, "user-1")
		if res.Allowed {
			t.Fatal("expected the key to be exhausted")
		}
		if res.RetryAfter <= 0 || res.RetryAfter > 2*window {
			t.Errorf("RetryAfter = %s, want positive and no more than %s", res.RetryAfter, 2*window)
		}

		clk.Advance(res.RetryAfter)
		if res := allow(t, swc, "user-1"); !res.Allowed {
			t.Error("after waiting the advertised RetryAfter: got denied, want allowed")
		}
	})
}

// TestSlidingWindowCounterLongIdleResetsBothWindows covers the multi-window
// skip path: after two or more windows of silence both counters have aged out
// and the key starts clean.
func TestSlidingWindowCounterLongIdleResetsBothWindows(t *testing.T) {
	const (
		limit  = 5
		window = time.Minute
	)

	clk := ratelimiter.NewManualClock(epoch)
	swc, err := algorithms.NewSlidingWindowCounter(limit, window, clk)
	if err != nil {
		t.Fatalf("constructing sliding window counter: %v", err)
	}

	for i := 0; i < limit; i++ {
		allow(t, swc, "user-1")
	}

	// Skip several whole windows at once.
	clk.Advance(5 * window)

	allowed := 0
	for i := 0; i < limit; i++ {
		if res := allow(t, swc, "user-1"); res.Allowed {
			allowed++
		}
	}
	if allowed != limit {
		t.Errorf("after a long idle, allowed = %d, want the full %d", allowed, limit)
	}
}

// TestSlidingWindowLogIsExactAcrossBoundary confirms the log has no boundary
// artifact whatsoever — unlike the counter, it admits no approximation, so
// exactly `limit` requests may occur in any trailing window.
func TestSlidingWindowLogIsExactAcrossBoundary(t *testing.T) {
	const (
		limit  = 5
		window = time.Minute
	)

	clk := ratelimiter.NewManualClock(epoch)
	swl, err := algorithms.NewSlidingWindowLog(limit, window, clk)
	if err != nil {
		t.Fatalf("constructing sliding window log: %v", err)
	}

	// Spend the full limit at the end of a nominal window.
	clk.Advance(window - time.Second)
	for i := 1; i <= limit; i++ {
		if res := allow(t, swl, "user-1"); !res.Allowed {
			t.Fatalf("request %d of %d: got denied, want allowed", i, limit)
		}
	}

	// Just past the nominal boundary, every one of those requests is still
	// inside the trailing window, so nothing may be admitted.
	clk.Advance(2 * time.Second)
	for i := 0; i < limit; i++ {
		if res := allow(t, swl, "user-1"); res.Allowed {
			t.Fatalf("request %d just past the boundary: got allowed, want denied — "+
				"the log must be exact, with no boundary reset", i+1)
		}
	}
}

// TestSlidingWindowLogExpiresEntriesIndividually confirms entries age out one
// at a time, freeing exactly one slot each, rather than the whole window
// clearing at once.
func TestSlidingWindowLogExpiresEntriesIndividually(t *testing.T) {
	const (
		limit  = 3
		window = time.Minute
	)

	clk := ratelimiter.NewManualClock(epoch)
	swl, err := algorithms.NewSlidingWindowLog(limit, window, clk)
	if err != nil {
		t.Fatalf("constructing sliding window log: %v", err)
	}

	// Three requests, spaced ten seconds apart.
	for i := 0; i < limit; i++ {
		if res := allow(t, swl, "user-1"); !res.Allowed {
			t.Fatalf("setup request %d: got denied, want allowed", i+1)
		}
		clk.Advance(10 * time.Second)
	}

	// 20s after the last one, all three are still in the window.
	clk.Advance(10 * time.Second)
	if res := allow(t, swl, "user-1"); res.Allowed {
		t.Fatal("with three entries still in the window: got allowed, want denied")
	}

	// Advance until only the first entry has aged out: exactly one slot frees.
	// The first entry was at epoch; move just past epoch+window.
	clk.Set(epoch.Add(window + time.Second))
	if res := allow(t, swl, "user-1"); !res.Allowed {
		t.Error("after the oldest entry expired: got denied, want allowed")
	}
	if res := allow(t, swl, "user-1"); res.Allowed {
		t.Error("a second request after only one entry expired: got allowed, want denied")
	}
}

// TestSlidingWindowLogRetryAfterIsAccurate confirms that waiting the advertised
// RetryAfter is enough for the next request to be admitted.
func TestSlidingWindowLogRetryAfterIsAccurate(t *testing.T) {
	const (
		limit  = 3
		window = time.Minute
	)

	clk := ratelimiter.NewManualClock(epoch)
	swl, err := algorithms.NewSlidingWindowLog(limit, window, clk)
	if err != nil {
		t.Fatalf("constructing sliding window log: %v", err)
	}

	for i := 0; i < limit; i++ {
		allow(t, swl, "user-1")
		clk.Advance(time.Second)
	}

	res := allow(t, swl, "user-1")
	if res.Allowed {
		t.Fatal("expected the key to be exhausted")
	}
	if res.RetryAfter <= 0 {
		t.Fatalf("RetryAfter = %s, want positive", res.RetryAfter)
	}

	clk.Advance(res.RetryAfter)
	if res := allow(t, swl, "user-1"); !res.Allowed {
		t.Error("after waiting the advertised RetryAfter: got denied, want allowed")
	}
}

// TestSlidingWindowLogHandlesOutOfOrderTimestamps covers the case where a
// backwards clock jump leaves the log's entries unsorted. The expiry scan and
// the oldest/newest lookups must not assume ordering, or RetryAfter would be
// computed from the wrong entry.
func TestSlidingWindowLogHandlesOutOfOrderTimestamps(t *testing.T) {
	const (
		limit  = 3
		window = time.Minute
	)

	clk := ratelimiter.NewManualClock(epoch)
	swl, err := algorithms.NewSlidingWindowLog(limit, window, clk)
	if err != nil {
		t.Fatalf("constructing sliding window log: %v", err)
	}

	// Record one entry late in the window, then jump the clock backwards and
	// record another, so the stored timestamps are out of order.
	clk.Advance(30 * time.Second)
	allow(t, swl, "user-1")
	clk.Advance(-20 * time.Second)
	allow(t, swl, "user-1")
	allow(t, swl, "user-1")

	res := allow(t, swl, "user-1")
	if res.Allowed {
		t.Fatal("expected the key to be exhausted")
	}
	if res.RetryAfter <= 0 || res.RetryAfter > window {
		t.Errorf("RetryAfter = %s, want positive and no more than %s", res.RetryAfter, window)
	}
	if res.ResetAfter <= 0 || res.ResetAfter > window {
		t.Errorf("ResetAfter = %s, want positive and no more than %s", res.ResetAfter, window)
	}
}

// TestSlidingWindowLogMemoryIsBounded confirms the log does not retain
// timestamps for requests that have aged out. This is the algorithm's main
// operational risk — unbounded growth on a hot key — so the pruning path is
// worth asserting directly.
func TestSlidingWindowLogMemoryIsBounded(t *testing.T) {
	const (
		limit  = 5
		window = time.Minute
	)

	clk := ratelimiter.NewManualClock(epoch)
	swl, err := algorithms.NewSlidingWindowLog(limit, window, clk)
	if err != nil {
		t.Fatalf("constructing sliding window log: %v", err)
	}

	// Sustained traffic across many windows. If expired entries were retained,
	// the key's slice would grow without bound; instead it must stay capped at
	// the limit, which we observe indirectly through Remaining staying sane.
	for cycle := 0; cycle < 20; cycle++ {
		for i := 0; i < limit*2; i++ {
			res := allow(t, swl, "hot")
			if res.Remaining > limit {
				t.Fatalf("cycle %d: Remaining = %d exceeds the limit %d", cycle, res.Remaining, limit)
			}
		}
		clk.Advance(window)
	}

	// After a full window of silence the key must be fully replenished.
	clk.Advance(window)
	res := allow(t, swl, "hot")
	if !res.Allowed {
		t.Error("after a quiet window: got denied, want allowed")
	}
	if want := limit - 1; res.Remaining != want {
		t.Errorf("after a quiet window: Remaining = %d, want %d — stale entries were not pruned", res.Remaining, want)
	}
}

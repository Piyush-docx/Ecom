package algorithms_test

import (
	"context"
	"testing"
	"time"

	"ratelimiter"
	"ratelimiter/algorithms"
)

// TestBoundaryBurstComparison measures how many requests each algorithm admits
// in a span shorter than one window, straddling a window boundary. Reports for
// the Phase 8 ADR; asserts nothing.
func TestBoundaryBurstComparison(t *testing.T) {
	const (
		limit  = 100
		window = time.Minute
	)
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	type cand struct {
		name string
		make func(clk ratelimiter.Clock) (ratelimiter.Limiter, error)
	}
	cands := []cand{
		{"token bucket", func(c ratelimiter.Clock) (ratelimiter.Limiter, error) {
			return algorithms.NewTokenBucket(limit, window, c)
		}},
		{"sliding window counter", func(c ratelimiter.Clock) (ratelimiter.Limiter, error) {
			return algorithms.NewSlidingWindowCounter(limit, window, c)
		}},
		{"sliding window log", func(c ratelimiter.Clock) (ratelimiter.Limiter, error) {
			return algorithms.NewSlidingWindowLog(limit, window, c)
		}},
	}

	ctx := context.Background()

	// Baseline: what a PLAIN fixed-window counter would admit in the same span.
	// Not an implementation in this repo -- it is the failure mode the sliding
	// window counter exists to prevent, computed here so the ADR can quote the
	// contrast.
	t.Logf("%-24s admitted %3d + %3d = %3d in a 3s span (limit=%d/%s)  <- not implemented; the flaw being avoided",
		"plain fixed window", limit, limit, 2*limit, limit, window)

	for _, c := range cands {
		clk := ratelimiter.NewManualClock(epoch)
		lim, err := c.make(clk)
		if err != nil {
			t.Fatal(err)
		}
		// Drain at the very end of window 1.
		clk.Advance(window - time.Second)
		first := 0
		for i := 0; i < limit*2; i++ {
			r, err := lim.Allow(ctx, "k")
			if err != nil {
				t.Fatal(err)
			}
			if r.Allowed {
				first++
			}
		}
		// Cross into window 2 and burst again.
		clk.Advance(2 * time.Second)
		second := 0
		for i := 0; i < limit*2; i++ {
			r, err := lim.Allow(ctx, "k")
			if err != nil {
				t.Fatal(err)
			}
			if r.Allowed {
				second++
			}
		}
		t.Logf("%-24s admitted %3d + %3d = %3d in a 3s span (limit=%d/%s)",
			c.name, first, second, first+second, limit, window)
	}
}

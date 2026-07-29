package ratelimiter

import (
	"testing"
	"time"
)

func TestSystemClockAdvances(t *testing.T) {
	c := SystemClock{}
	before := c.Now()
	if before.IsZero() {
		t.Fatal("SystemClock.Now returned the zero time")
	}
	if after := c.Now(); after.Before(before) {
		t.Errorf("SystemClock.Now went backwards: %s then %s", before, after)
	}
}

func TestManualClockOnlyMovesWhenTold(t *testing.T) {
	start := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)
	c := NewManualClock(start)

	if got := c.Now(); !got.Equal(start) {
		t.Errorf("Now() = %s, want %s", got, start)
	}
	// Reading the clock must not advance it.
	if got := c.Now(); !got.Equal(start) {
		t.Errorf("Now() after a second read = %s, want %s", got, start)
	}

	c.Advance(90 * time.Second)
	if want := start.Add(90 * time.Second); !c.Now().Equal(want) {
		t.Errorf("after Advance: Now() = %s, want %s", c.Now(), want)
	}

	// Advancing by a negative duration models a backwards clock jump.
	c.Advance(-30 * time.Second)
	if want := start.Add(60 * time.Second); !c.Now().Equal(want) {
		t.Errorf("after a negative Advance: Now() = %s, want %s", c.Now(), want)
	}

	c.Set(start)
	if got := c.Now(); !got.Equal(start) {
		t.Errorf("after Set: Now() = %s, want %s", got, start)
	}
}

// TestManualClockIsConcurrencySafe backs the concurrency tests in the
// algorithms package, which read this clock from many goroutines at once.
func TestManualClockIsConcurrencySafe(t *testing.T) {
	c := NewManualClock(time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC))
	done := make(chan struct{})

	for i := 0; i < 50; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				_ = c.Now()
				c.Advance(time.Millisecond)
			}
		}()
	}
	for i := 0; i < 50; i++ {
		<-done
	}
}

package redis_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	rlredis "ratelimiter/redis"
)

// TestMemoryFootprintPerAlgorithm measures what each algorithm actually costs
// in Redis for one key, so the Phase 8 ADR quotes a measurement rather than an
// assertion. It is not an assertion about correctness and has no fixed
// expectations: it reports.
func TestMemoryFootprintPerAlgorithm(t *testing.T) {
	rdb := newClient(t)
	prefix := uniquePrefix(t)
	ctx := context.Background()

	const limit = 10000
	const window = time.Minute
	requests := 5000
	if v := os.Getenv("MEMTEST_REQUESTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("MEMTEST_REQUESTS=%q: %v", v, err)
		}
		requests = n
	}

	tb, err := rlredis.NewTokenBucket(rdb, prefix+":tb", limit, window)
	if err != nil {
		t.Fatal(err)
	}
	swc, err := rlredis.NewSlidingWindowCounter(rdb, prefix+":swc", limit, window)
	if err != nil {
		t.Fatal(err)
	}
	swl, err := rlredis.NewSlidingWindowLog(rdb, prefix+":swl", limit, window)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < requests; i++ {
		if _, err := tb.Allow(ctx, "user-1"); err != nil {
			t.Fatal(err)
		}
		if _, err := swc.Allow(ctx, "user-1"); err != nil {
			t.Fatal(err)
		}
		if _, err := swl.Allow(ctx, "user-1"); err != nil {
			t.Fatal(err)
		}
	}

	for _, c := range []struct{ label, prefix string }{
		{"token bucket", prefix + ":tb"},
		{"sliding window counter", prefix + ":swc"},
		{"sliding window log", prefix + ":swl"},
	} {
		keys, err := rdb.Keys(ctx, c.prefix+"*").Result()
		if err != nil || len(keys) == 0 {
			t.Fatalf("no keys for %s: %v", c.label, err)
		}
		for _, k := range keys {
			typ, _ := rdb.Type(ctx, k).Result()
			res, err := rdb.Do(ctx, "MEMORY", "USAGE", k).Result()
			if err != nil {
				t.Fatalf("MEMORY USAGE %s: %v", k, err)
			}
			bytes, _ := res.(int64)
			t.Logf("%-24s key=%-28s type=%-6s bytes=%d (after %d allowed requests, limit=%d)",
				c.label, k, typ, bytes, requests, limit)
			if err := rdb.Del(ctx, k).Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

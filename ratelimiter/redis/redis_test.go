package redis_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"ratelimiter"
	rlredis "ratelimiter/redis"
)

// These are integration tests against a real Redis. IMPLEMENTATION_PLAN.md 6 is
// explicit that a rate limiter test against a mocked Redis proves nothing about
// the atomicity claim, so there are no fakes here.
//
// Start one with:
//
//	docker compose -f deploy/docker-compose.yml up -d redis
//
// Override the address with RATELIMITER_TEST_REDIS_ADDR.

func redisAddr() string {
	if addr := os.Getenv("RATELIMITER_TEST_REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

// newClient connects to Redis, skipping the test if none is reachable.
//
// Skipping rather than failing keeps `go test ./...` honest on a machine
// without Redis: the suite reports these as skipped instead of pretending the
// atomicity guarantee was verified.
func newClient(t *testing.T) *goredis.Client {
	t.Helper()

	client := goredis.NewClient(&goredis.Options{Addr: redisAddr()})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Skipf("no Redis at %s (%v) — start one with: docker compose -f deploy/docker-compose.yml up -d redis", redisAddr(), err)
	}

	t.Cleanup(func() { client.Close() })
	return client
}

// uniquePrefix returns a key namespace unique to one test, so tests cannot
// interfere with each other and need no flushing of a shared database.
func uniquePrefix(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("rltest:%s:%d", t.Name(), time.Now().UnixNano())
}

// constructor builds one Redis-backed limiter under test.
type constructor struct {
	name string
	new  func(c rlredis.Scripter, prefix string, limit int, window time.Duration) (ratelimiter.Limiter, error)
}

func all() []constructor {
	return []constructor{
		{
			name: "TokenBucket",
			new: func(c rlredis.Scripter, prefix string, limit int, window time.Duration) (ratelimiter.Limiter, error) {
				return rlredis.NewTokenBucket(c, prefix, limit, window)
			},
		},
		{
			name: "SlidingWindowLog",
			new: func(c rlredis.Scripter, prefix string, limit int, window time.Duration) (ratelimiter.Limiter, error) {
				return rlredis.NewSlidingWindowLog(c, prefix, limit, window)
			},
		},
		{
			name: "SlidingWindowCounter",
			new: func(c rlredis.Scripter, prefix string, limit int, window time.Duration) (ratelimiter.Limiter, error) {
				return rlredis.NewSlidingWindowCounter(c, prefix, limit, window)
			},
		},
	}
}

func allow(t *testing.T, l ratelimiter.Limiter, key string) ratelimiter.Result {
	t.Helper()
	res, err := l.Allow(context.Background(), key)
	if err != nil {
		t.Fatalf("Allow(%q) returned unexpected error: %v", key, err)
	}
	return res
}

// TestConcurrentAllowIsExact is the Phase 2 acceptance criterion.
//
// N goroutines fire at the same key simultaneously; exactly the configured
// limit must be allowed through. Not limit±race-condition-slop — exactly.
//
// This is the test that proves the atomicity claim. A check-then-act split into
// a GET and a separate INCR would let concurrent callers read the same count
// and each conclude they were under the limit, admitting more than the limit.
// Because each algorithm is a single Lua script, and Redis blocks all other
// activity for a script's runtime, the read-modify-write cannot interleave.
func TestConcurrentAllowIsExact(t *testing.T) {
	client := newClient(t)

	const (
		limit    = 100
		requests = 1000
		window   = time.Minute
	)

	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			l, err := c.new(client, uniquePrefix(t), limit, window)
			if err != nil {
				t.Fatalf("constructing limiter: %v", err)
			}

			var (
				wg      sync.WaitGroup
				mu      sync.Mutex
				allowed int
				errs    []error
			)

			// Release every goroutine at once to maximize contention on the key.
			start := make(chan struct{})
			for i := 0; i < requests; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					res, err := l.Allow(context.Background(), "hot-key")
					mu.Lock()
					defer mu.Unlock()
					if err != nil {
						errs = append(errs, err)
						return
					}
					if res.Allowed {
						allowed++
					}
				}()
			}
			close(start)
			wg.Wait()

			if len(errs) > 0 {
				t.Fatalf("%d of %d requests errored, first: %v", len(errs), requests, errs[0])
			}
			if allowed != limit {
				t.Errorf("%d concurrent requests against one key allowed %d, want exactly %d — "+
					"a count above the limit means the check-and-increment is not atomic",
					requests, allowed, limit)
			} else {
				t.Logf("%d concurrent requests against one key: exactly %d allowed, %d denied",
					requests, allowed, requests-allowed)
			}
		})
	}
}

// TestAtAndOverLimit confirms the Redis implementations agree with the Phase 1
// in-memory ones on where the ceiling sits.
func TestAtAndOverLimit(t *testing.T) {
	client := newClient(t)

	const (
		limit  = 5
		window = time.Minute
	)

	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			l, err := c.new(client, uniquePrefix(t), limit, window)
			if err != nil {
				t.Fatalf("constructing limiter: %v", err)
			}

			for i := 1; i <= limit; i++ {
				res := allow(t, l, "user-1")
				if !res.Allowed {
					t.Fatalf("request %d of %d: got denied, want allowed", i, limit)
				}
				if res.Limit != limit {
					t.Errorf("request %d: Limit = %d, want %d", i, res.Limit, limit)
				}
				if want := limit - i; res.Remaining != want {
					t.Errorf("request %d: Remaining = %d, want %d", i, res.Remaining, want)
				}
			}

			res := allow(t, l, "user-1")
			if res.Allowed {
				t.Fatalf("request %d of %d: got allowed, want denied", limit+1, limit)
			}
			if res.Remaining != 0 {
				t.Errorf("denied request: Remaining = %d, want 0", res.Remaining)
			}
			if res.RetryAfter <= 0 {
				t.Errorf("denied request: RetryAfter = %s, want positive", res.RetryAfter)
			}
		})
	}
}

// TestKeysAreIndependent confirms one key's exhaustion leaves another alone.
func TestKeysAreIndependent(t *testing.T) {
	client := newClient(t)

	const limit = 3

	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			l, err := c.new(client, uniquePrefix(t), limit, time.Minute)
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

			for i := 0; i < limit; i++ {
				if res := allow(t, l, "quiet"); !res.Allowed {
					t.Fatalf("quiet request %d: got denied, want allowed — keys are not independent", i+1)
				}
			}
		})
	}
}

// TestSharedStateAcrossInstances is the property the whole project exists to
// demonstrate: two independently constructed limiters, standing in for two
// horizontally-scaled gateway instances, must enforce one combined limit rather
// than one limit each.
//
// Phase 7 proves this again at load across two real gateway processes; this is
// the unit-level version of that claim.
func TestSharedStateAcrossInstances(t *testing.T) {
	client := newClient(t)

	const (
		limit  = 10
		window = time.Minute
	)

	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			prefix := uniquePrefix(t)

			// Two limiters, same prefix and config — as if two gateway
			// processes were sharing one Redis.
			a, err := c.new(client, prefix, limit, window)
			if err != nil {
				t.Fatalf("constructing limiter A: %v", err)
			}
			b, err := c.new(client, prefix, limit, window)
			if err != nil {
				t.Fatalf("constructing limiter B: %v", err)
			}

			allowed := 0
			// Alternate between them so neither can be the sole accountant.
			for i := 0; i < limit*2; i++ {
				l := a
				if i%2 == 1 {
					l = b
				}
				if res := allow(t, l, "shared-user"); res.Allowed {
					allowed++
				}
			}

			if allowed != limit {
				t.Errorf("across two limiter instances, allowed = %d, want exactly %d — "+
					"the limit is being enforced per instance rather than globally",
					allowed, limit)
			}
		})
	}
}

// TestKeysExpire confirms every algorithm sets a TTL, so Redis memory cannot
// grow without bound as new keys are seen. A limiter that never expires state
// is an outage waiting for enough distinct clients.
func TestKeysExpire(t *testing.T) {
	client := newClient(t)

	const (
		limit  = 5
		window = 2 * time.Second
	)

	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			prefix := uniquePrefix(t)
			l, err := c.new(client, prefix, limit, window)
			if err != nil {
				t.Fatalf("constructing limiter: %v", err)
			}

			allow(t, l, "expiring-key")

			ctx := context.Background()
			keys, err := client.Keys(ctx, prefix+"*").Result()
			if err != nil {
				t.Fatalf("listing keys: %v", err)
			}
			if len(keys) == 0 {
				t.Fatal("no keys written to Redis")
			}

			for _, k := range keys {
				ttl, err := client.PTTL(ctx, k).Result()
				if err != nil {
					t.Fatalf("PTTL(%s): %v", k, err)
				}
				// -1 means the key exists with no expiry set, which is the
				// unbounded-growth bug this test guards against.
				if ttl < 0 {
					t.Errorf("key %s has TTL %s, want a positive expiry — "+
						"state must not accumulate forever", k, ttl)
				}
			}
		})
	}
}

// TestWindowElapsesAndReplenishes confirms state actually ages out in Redis:
// after a full window of silence, a key regains its allowance.
//
// This uses a short real window and a real sleep. There is no manual clock
// here, unlike the Phase 1 tests — the scripts deliberately read Redis's own
// TIME so that no caller can influence the limit, which means time can only be
// advanced by waiting.
func TestWindowElapsesAndReplenishes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: this test sleeps for a real window")
	}

	client := newClient(t)

	const (
		limit  = 3
		window = 1500 * time.Millisecond
	)

	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			l, err := c.new(client, uniquePrefix(t), limit, window)
			if err != nil {
				t.Fatalf("constructing limiter: %v", err)
			}

			for i := 0; i < limit; i++ {
				if res := allow(t, l, "user-1"); !res.Allowed {
					t.Fatalf("setup request %d: got denied, want allowed", i+1)
				}
			}
			if res := allow(t, l, "user-1"); res.Allowed {
				t.Fatal("expected the key to be exhausted before waiting")
			}

			// Wait out two full windows so every algorithm's state has aged
			// out: the bucket refills, log entries expire, and the counter
			// rolls both of its windows.
			time.Sleep(2 * window)

			allowed := 0
			for i := 0; i < limit; i++ {
				if res := allow(t, l, "user-1"); res.Allowed {
					allowed++
				}
			}
			if allowed != limit {
				t.Errorf("after waiting out the window, allowed = %d, want the full %d — "+
					"expired state is not being released", allowed, limit)
			}
		})
	}
}

// TestSlidingWindowLogExpiresEntriesIndividually confirms the log frees slots
// one at a time as individual entries age out, rather than relying on the whole
// key expiring.
//
// The distinction matters: a key under continuous traffic has its TTL refreshed
// on every request and so never expires. If per-entry expiry were missing, such
// a key would be permanently exhausted after its first `limit` requests. Only a
// test that keeps touching the key while time passes can tell the two apart.
func TestSlidingWindowLogExpiresEntriesIndividually(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: this test sleeps across a real window")
	}

	client := newClient(t)

	const (
		limit  = 3
		window = time.Second
	)

	l, err := rlredis.NewSlidingWindowLog(client, uniquePrefix(t), limit, window)
	if err != nil {
		t.Fatalf("constructing sliding window log: %v", err)
	}

	// Spend the whole allowance up front.
	for i := 0; i < limit; i++ {
		if res := allow(t, l, "user-1"); !res.Allowed {
			t.Fatalf("setup request %d: got denied, want allowed", i+1)
		}
	}

	// Poll steadily for slightly longer than one window. Each denied poll
	// refreshes the key's TTL, so the key itself never expires — only the
	// individual entries can age out. By the end, all three original entries
	// have left the trailing window and requests must be admitted again.
	deadline := time.Now().Add(window + 500*time.Millisecond)
	admitted := 0
	for time.Now().Before(deadline) {
		if res := allow(t, l, "user-1"); res.Allowed {
			admitted++
		}
		time.Sleep(50 * time.Millisecond)
	}

	if admitted == 0 {
		t.Error("no request was admitted while polling across a full window — " +
			"entries are not aging out of the sorted set individually")
	}
}

// TestRetryAfterIsHonored confirms that a client waiting exactly as long as it
// was told is then admitted. Phase 3 puts this value into the Retry-After
// header, so an under-estimate would put clients into a denied-retry loop.
//
// The equivalent in-memory test caught a real float-truncation bug in Phase 1;
// this is the same property checked against the Lua implementations.
func TestRetryAfterIsHonored(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: this test sleeps for the advertised retry window")
	}

	client := newClient(t)

	const (
		limit  = 3
		window = time.Second
	)

	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			l, err := c.new(client, uniquePrefix(t), limit, window)
			if err != nil {
				t.Fatalf("constructing limiter: %v", err)
			}

			for i := 0; i < limit; i++ {
				allow(t, l, "user-1")
			}

			res := allow(t, l, "user-1")
			if res.Allowed {
				t.Fatal("expected the key to be exhausted")
			}
			if res.RetryAfter <= 0 {
				t.Fatalf("RetryAfter = %s, want positive", res.RetryAfter)
			}

			// A small margin covers the round-trip and scheduling jitter
			// between reading RetryAfter and issuing the retry.
			time.Sleep(res.RetryAfter + 50*time.Millisecond)

			if retried := allow(t, l, "user-1"); !retried.Allowed {
				t.Errorf("after waiting the advertised RetryAfter of %s: got denied, want allowed", res.RetryAfter)
			}
		})
	}
}

// TestConstructorRejectsInvalidConfig checks that a misconfigured limiter fails
// at construction rather than at the first request.
func TestConstructorRejectsInvalidConfig(t *testing.T) {
	client := newClient(t)

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
					if _, err := c.new(client, "rltest", tc.limit, tc.window); err == nil {
						t.Errorf("new(limit=%d, window=%s) returned no error, want one", tc.limit, tc.window)
					}
				})
			}
			t.Run("nil client", func(t *testing.T) {
				if _, err := c.new(nil, "rltest", 10, time.Minute); err == nil {
					t.Error("new(client=nil) returned no error, want one")
				}
			})
		})
	}
}

// TestScriptCacheFlushIsRecoverable confirms a limiter keeps working after
// SCRIPT FLUSH. The Redis docs call the script cache "always volatile" — it can
// be cleared by a restart, a failover, or an explicit flush — so a client that
// only ever issued EVALSHA would start failing with NOSCRIPT until restarted.
func TestScriptCacheFlushIsRecoverable(t *testing.T) {
	client := newClient(t)

	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			l, err := c.new(client, uniquePrefix(t), 10, time.Minute)
			if err != nil {
				t.Fatalf("constructing limiter: %v", err)
			}

			if res := allow(t, l, "user-1"); !res.Allowed {
				t.Fatal("first request: got denied, want allowed")
			}

			if err := client.ScriptFlush(context.Background()).Err(); err != nil {
				t.Fatalf("SCRIPT FLUSH: %v", err)
			}

			if res := allow(t, l, "user-1"); !res.Allowed {
				t.Error("after SCRIPT FLUSH: got denied, want allowed — the NOSCRIPT fallback did not reload the script")
			}
		})
	}
}

// TestContextCancellationIsHonored confirms a cancelled context aborts the call
// rather than blocking. The gateway in Phase 3 puts a deadline on every request.
func TestContextCancellationIsHonored(t *testing.T) {
	client := newClient(t)

	for _, c := range all() {
		t.Run(c.name, func(t *testing.T) {
			l, err := c.new(client, uniquePrefix(t), 10, time.Minute)
			if err != nil {
				t.Fatalf("constructing limiter: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			if _, err := l.Allow(ctx, "user-1"); err == nil {
				t.Error("Allow with a cancelled context returned no error, want one")
			}
		})
	}
}

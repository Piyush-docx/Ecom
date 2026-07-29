package redis

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"ratelimiter"
)

// TokenBucket is the Redis-backed token bucket. State per key is a hash holding
// a fractional token count and the timestamp it was last updated.
type TokenBucket struct {
	client   Scripter
	script   *goredis.Script
	prefix   string
	capacity int
	rate     float64 // tokens per second
}

// NewTokenBucket returns a token bucket allowing capacity requests in a burst,
// refilling at capacity tokens per interval.
//
// keyPrefix namespaces this limiter's keys in Redis so several limiters can
// share one instance without colliding.
func NewTokenBucket(client Scripter, keyPrefix string, capacity int, interval time.Duration) (*TokenBucket, error) {
	if client == nil {
		return nil, fmt.Errorf("token bucket: client must not be nil")
	}
	if capacity <= 0 {
		return nil, fmt.Errorf("token bucket: capacity must be positive, got %d", capacity)
	}
	if interval <= 0 {
		return nil, fmt.Errorf("token bucket: interval must be positive, got %s", interval)
	}
	return &TokenBucket{
		client:   client,
		script:   goredis.NewScript(tokenBucketScript),
		prefix:   keyPrefix,
		capacity: capacity,
		rate:     float64(capacity) / interval.Seconds(),
	}, nil
}

// Allow consumes one token from key's bucket if any remain.
func (tb *TokenBucket) Allow(ctx context.Context, key string) (ratelimiter.Result, error) {
	return runScript(ctx, tb.script, tb.client, tb.prefix+":tb:"+key, tb.capacity,
		tb.capacity,
		strconv.FormatFloat(tb.rate, 'f', -1, 64),
		1,
	)
}

// SlidingWindowLog is the Redis-backed sliding window log. State per key is a
// sorted set holding one member per allowed request, scored by timestamp.
//
// It is exact, at O(n) memory per key where n is the limit.
type SlidingWindowLog struct {
	client Scripter
	script *goredis.Script
	prefix string
	limit  int
	window time.Duration

	// seq disambiguates two requests that land on the same Redis timestamp.
	// Sorted set members must be unique or the second would overwrite the
	// first, silently under-counting the window.
	seq atomic.Uint64
}

// NewSlidingWindowLog returns a limiter permitting limit requests in any
// trailing window.
func NewSlidingWindowLog(client Scripter, keyPrefix string, limit int, window time.Duration) (*SlidingWindowLog, error) {
	if client == nil {
		return nil, fmt.Errorf("sliding window log: client must not be nil")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("sliding window log: limit must be positive, got %d", limit)
	}
	if window <= 0 {
		return nil, fmt.Errorf("sliding window log: window must be positive, got %s", window)
	}
	return &SlidingWindowLog{
		client: client,
		script: goredis.NewScript(slidingWindowLogScript),
		prefix: keyPrefix,
		limit:  limit,
		window: window,
	}, nil
}

// Allow records a request against key if fewer than limit requests fall within
// the trailing window.
func (s *SlidingWindowLog) Allow(ctx context.Context, key string) (ratelimiter.Result, error) {
	// The member id must be unique across every caller sharing this Redis, not
	// merely within this process, so it combines a per-process counter with the
	// key and a nanosecond timestamp. This value is never used for timing
	// decisions — only Redis's own clock is — so a caller's skewed wall clock
	// cannot affect the limit.
	member := strconv.FormatInt(time.Now().UnixNano(), 36) + "-" +
		strconv.FormatUint(s.seq.Add(1), 36)

	return runScript(ctx, s.script, s.client, s.prefix+":swl:"+key, s.limit,
		s.limit,
		s.window.Milliseconds(),
		member,
	)
}

// SlidingWindowCounter is the Redis-backed sliding window counter. State per
// key is a hash holding the current and previous fixed-window counts.
//
// Windows are anchored to a global grid rather than to a key's first request,
// so every caller computes the same window index for a given key.
type SlidingWindowCounter struct {
	client Scripter
	script *goredis.Script
	prefix string
	limit  int
	window time.Duration
}

// NewSlidingWindowCounter returns a limiter approximating limit requests per
// trailing window.
func NewSlidingWindowCounter(client Scripter, keyPrefix string, limit int, window time.Duration) (*SlidingWindowCounter, error) {
	if client == nil {
		return nil, fmt.Errorf("sliding window counter: client must not be nil")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("sliding window counter: limit must be positive, got %d", limit)
	}
	if window <= 0 {
		return nil, fmt.Errorf("sliding window counter: window must be positive, got %s", window)
	}
	return &SlidingWindowCounter{
		client: client,
		script: goredis.NewScript(slidingWindowCounterScript),
		prefix: keyPrefix,
		limit:  limit,
		window: window,
	}, nil
}

// Allow charges one request against key if the weighted estimate permits it.
func (s *SlidingWindowCounter) Allow(ctx context.Context, key string) (ratelimiter.Result, error) {
	return runScript(ctx, s.script, s.client, s.prefix+":swc:"+key, s.limit,
		s.limit,
		s.window.Milliseconds(),
	)
}

// Compile-time confirmation that each type satisfies the shared interface, so
// the gateway in Phase 3 can hold any of them behind ratelimiter.Limiter.
var (
	_ ratelimiter.Limiter = (*TokenBucket)(nil)
	_ ratelimiter.Limiter = (*SlidingWindowLog)(nil)
	_ ratelimiter.Limiter = (*SlidingWindowCounter)(nil)
)

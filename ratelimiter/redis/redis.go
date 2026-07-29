// Package redis implements the ratelimiter.Limiter algorithms against a shared
// Redis instance, so a limit holds across horizontally-scaled callers rather
// than per-process.
//
// # Atomicity
//
// Every algorithm is a single Lua script evaluated by Redis. The Redis docs
// state it plainly: "Redis guarantees the script's atomic execution. While
// executing the script, all server activities are blocked during its entire
// runtime." The read-modify-write inside a script therefore cannot interleave
// with another caller, which is what AGENTS.md 2.2 requires — a GET followed by
// an INCR as two round trips would be a race regardless of how it behaves under
// light load.
//
// # Clock
//
// Scripts read Redis's own TIME rather than accepting a timestamp from the
// caller. Every instance shares that one clock, so the limit is immune to skew
// between callers (IMPLEMENTATION_PLAN.md 5). This is why these types take no
// ratelimiter.Clock: the authoritative clock lives in Redis.
package redis

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"ratelimiter"
)

//go:embed scripts/tokenbucket.lua
var tokenBucketScript string

//go:embed scripts/slidingwindowlog.lua
var slidingWindowLogScript string

//go:embed scripts/slidingwindowcounter.lua
var slidingWindowCounterScript string

// Scripter is the subset of the go-redis client these limiters need. Taking an
// interface rather than a concrete *goredis.Client lets callers pass a plain
// client, a cluster client, or a wrapper without changing this package.
type Scripter interface {
	goredis.Scripter
}

// runScript evaluates a script and decodes its reply.
//
// goredis.Script.Run issues EVALSHA first and transparently falls back to EVAL
// on a NOSCRIPT error, which is the execution pattern the Redis docs prescribe:
// the script cache is volatile and may be flushed by a restart or failover at
// any time, so an application must be able to reload on demand.
func runScript(ctx context.Context, s *goredis.Script, c Scripter, key string, limit int, args ...any) (ratelimiter.Result, error) {
	raw, err := s.Run(ctx, c, []string{key}, args...).Result()
	if err != nil {
		return ratelimiter.Result{}, fmt.Errorf("rate limiter script: %w", err)
	}
	return decode(raw, limit)
}

// decode converts a script's reply into a Result.
//
// Scripts return every field as a string. Lua has one numeric type and Redis
// converts a Lua number to an integer reply by removing the decimal part, so a
// fractional token count or millisecond duration returned as a number would be
// silently truncated. Returning strings and parsing here preserves them.
func decode(raw any, limit int) (ratelimiter.Result, error) {
	values, ok := raw.([]any)
	if !ok {
		return ratelimiter.Result{}, fmt.Errorf("rate limiter script: expected an array reply, got %T", raw)
	}
	if len(values) != 4 {
		return ratelimiter.Result{}, fmt.Errorf("rate limiter script: expected 4 reply elements, got %d", len(values))
	}

	nums := make([]float64, 4)
	for i, v := range values {
		s, ok := v.(string)
		if !ok {
			return ratelimiter.Result{}, fmt.Errorf("rate limiter script: reply element %d is %T, want string", i, v)
		}
		n, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return ratelimiter.Result{}, fmt.Errorf("rate limiter script: reply element %d (%q) is not numeric: %w", i, s, err)
		}
		nums[i] = n
	}

	remaining := int(nums[1])
	if remaining < 0 {
		remaining = 0
	}

	return ratelimiter.Result{
		Allowed:    nums[0] == 1,
		Limit:      limit,
		Remaining:  remaining,
		ResetAfter: millis(nums[2]),
		RetryAfter: millis(nums[3]),
	}, nil
}

// millis converts a possibly-fractional millisecond count into a Duration,
// clamping negatives to zero.
func millis(ms float64) time.Duration {
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms * float64(time.Millisecond))
}

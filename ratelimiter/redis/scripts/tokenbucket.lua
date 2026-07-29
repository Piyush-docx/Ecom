-- Token bucket, evaluated atomically inside Redis.
--
-- Redis guarantees a script's atomic execution: all server activity is blocked
-- for its entire runtime, so the read-modify-write below cannot interleave with
-- another caller. That is what makes this a single atomic operation rather than
-- the GET-then-INCR race AGENTS.md 2.2 forbids.
--
-- KEYS[1]  bucket key
-- ARGV[1]  capacity           (tokens)
-- ARGV[2]  refill rate        (tokens per second, may be fractional)
-- ARGV[3]  requested tokens   (always 1 today; kept explicit for clarity)
--
-- Returns { allowed, remaining, reset_after_ms, retry_after_ms }, every element
-- a string. Lua numbers convert to RESP integers with the decimal part removed,
-- so fractional values must cross the boundary as strings or they silently
-- truncate.

local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

-- Read Redis's own clock rather than trusting the caller's. Every gateway
-- instance shares this one clock, so bucket refill is immune to skew between
-- them (IMPLEMENTATION_PLAN.md 5). TIME returns { seconds, microseconds }.
local time = redis.call('TIME')
local now = tonumber(time[1]) + (tonumber(time[2]) / 1000000)

local state = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local last = tonumber(state[2])

if tokens == nil then
  -- Unseen key starts full, so a fresh client gets its whole burst allowance.
  tokens = capacity
  last = now
end

-- Credit tokens for elapsed time, capped at capacity. A clock that moves
-- backwards must not drain the bucket, so a negative delta contributes nothing.
local elapsed = now - last
if elapsed > 0 then
  tokens = math.min(capacity, tokens + (elapsed * rate))
end

local allowed = 0
local retry_after = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
else
  retry_after = (requested - tokens) / rate
end

redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', tostring(now))

-- Expire the key once it would have refilled completely; until then its state
-- still matters. Without this, every key ever seen would persist forever.
local reset_after = (capacity - tokens) / rate
local ttl = math.ceil(reset_after * 1000) + 1000
redis.call('PEXPIRE', key, ttl)

return {
  tostring(allowed),
  tostring(math.floor(tokens)),
  tostring(reset_after * 1000),
  tostring(retry_after * 1000),
}

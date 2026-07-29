-- Sliding window log, evaluated atomically inside Redis.
--
-- Backed by a sorted set: one member per allowed request, scored by the
-- microsecond timestamp at which it was admitted. Expiry is a ZREMRANGEBYSCORE
-- of everything older than the trailing window, so the count is exact with no
-- boundary artifacts.
--
-- The cost is O(n) memory per key, n being the limit. A large limit means a
-- large sorted set per key; prefer the counter when that matters.
--
-- KEYS[1]  sorted set key
-- ARGV[1]  limit
-- ARGV[2]  window in milliseconds
-- ARGV[3]  a unique member id, since two requests can share a timestamp and a
--          sorted set would otherwise collapse them into one member
--
-- Returns { allowed, remaining, reset_after_ms, retry_after_ms } as strings.

local key    = KEYS[1]
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local member = ARGV[3]

-- Redis's clock, shared by every gateway instance (IMPLEMENTATION_PLAN.md 5).
local time = redis.call('TIME')
local now = (tonumber(time[1]) * 1000) + (tonumber(time[2]) / 1000)

-- Drop entries that have aged out of the trailing window before counting.
local cutoff = now - window
redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)

local count = redis.call('ZCARD', key)

local allowed = 0
local retry_after = 0
if count < limit then
  redis.call('ZADD', key, now, member)
  allowed = 1
  count = count + 1
else
  -- The oldest surviving entry is the one whose expiry frees the next slot.
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  if oldest[2] ~= nil then
    retry_after = (tonumber(oldest[2]) + window) - now
    if retry_after < 0 then retry_after = 0 end
  end
end

-- The key is fully clear once its newest entry ages out.
local reset_after = window
local newest = redis.call('ZRANGE', key, -1, -1, 'WITHSCORES')
if newest[2] ~= nil then
  reset_after = (tonumber(newest[2]) + window) - now
  if reset_after < 0 then reset_after = 0 end
end

-- Bound the key's lifetime: once the newest entry expires there is nothing
-- left to remember.
redis.call('PEXPIRE', key, math.ceil(reset_after) + 1000)

local remaining = limit - count
if remaining < 0 then remaining = 0 end

return {
  tostring(allowed),
  tostring(remaining),
  tostring(reset_after),
  tostring(retry_after),
}

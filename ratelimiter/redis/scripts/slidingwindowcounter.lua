-- Sliding window counter, evaluated atomically inside Redis.
--
-- Two fixed-window counters (current and previous) held in one hash, with the
-- previous window's count weighted by how much of the trailing window still
-- overlaps it:
--
--   estimate = prev*(1-f) + curr
--
-- where f is the fraction of the current fixed window elapsed. This is O(1)
-- state per key while avoiding the plain fixed window's boundary flaw, where a
-- burst straddling the boundary lets 2*limit through.
--
-- Unlike the in-memory implementation, windows are anchored to a global grid
-- (floor(now/window)) rather than to a key's first request. Every gateway
-- instance therefore computes the same window index for the same key, which is
-- what lets the limit hold across horizontally-scaled instances.
--
-- KEYS[1]  hash key
-- ARGV[1]  limit
-- ARGV[2]  window in milliseconds
--
-- Returns { allowed, remaining, reset_after_ms, retry_after_ms } as strings.

local key    = KEYS[1]
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

-- Redis's clock, shared by every gateway instance (IMPLEMENTATION_PLAN.md 5).
local time = redis.call('TIME')
local now = (tonumber(time[1]) * 1000) + (tonumber(time[2]) / 1000)

local current_window = math.floor(now / window)
local elapsed = now - (current_window * window)
local f = elapsed / window

local state = redis.call('HMGET', key, 'window', 'curr', 'prev')
local stored_window = tonumber(state[1])
local curr = tonumber(state[2]) or 0
local prev = tonumber(state[3]) or 0

if stored_window == nil then
  curr = 0
  prev = 0
elseif stored_window < current_window - 1 then
  -- Two or more windows of silence: both counters have aged out.
  curr = 0
  prev = 0
elseif stored_window == current_window - 1 then
  -- Exactly one window elapsed: this window's count becomes the previous one.
  prev = curr
  curr = 0
end
-- stored_window == current_window means we are still inside it; keep both.

local estimate = (prev * (1 - f)) + curr

local allowed = 0
local retry_after = 0
local remaining = 0

if estimate + 1 <= limit then
  curr = curr + 1
  allowed = 1
  local rem = limit - (estimate + 1)
  if rem > 0 then remaining = math.floor(rem) end
else
  -- Time until this fixed window ends.
  local remainder = window - elapsed
  if prev == 0 then
    -- Only the rollover can help, and not at the instant of it: at the
    -- boundary curr becomes prev and at f=0 still counts in full. It then
    -- decays as curr*(1-f), so a slot opens once curr*(1-f) + 1 <= limit.
    if curr == 0 then
      retry_after = remainder
    else
      local target_f = (curr - limit + 1) / curr
      if target_f < 0 then target_f = 0 end
      retry_after = remainder + math.ceil(target_f * window)
    end
  else
    -- Solve prev*(1-f) + curr = limit - 1 for f, capping at the rollover.
    local target = limit - 1 - curr
    if target < 0 then
      retry_after = remainder
    else
      local threshold_f = 1 - (target / prev)
      if threshold_f >= 1 then
        retry_after = remainder
      else
        -- Round up so a client honoring the value exactly is admitted rather
        -- than denied a fraction of a tick early.
        local wait = math.ceil(threshold_f * window) - elapsed
        if wait <= 0 then
          retry_after = 1
        elseif wait > remainder then
          retry_after = remainder
        else
          retry_after = wait
        end
      end
    end
  end
end

redis.call('HSET', key, 'window', current_window, 'curr', curr, 'prev', prev)

-- Keep the key alive long enough for the current window's count to still be
-- weighted as the previous window during the next one, then let it expire.
redis.call('PEXPIRE', key, math.ceil(window * 2) + 1000)

return {
  tostring(allowed),
  tostring(remaining),
  tostring(window - elapsed),
  tostring(retry_after),
}

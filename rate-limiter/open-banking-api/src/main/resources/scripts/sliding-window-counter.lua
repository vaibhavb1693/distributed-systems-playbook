-- Sliding Window Counter — weighted average of the current and previous fixed windows.
-- Good balance of accuracy (no boundary-burst problem) and memory (2 counters, not N timestamps).
-- KEYS[1] = rl:sliding-counter:{partnerId}:{currentBucket}
-- KEYS[2] = rl:sliding-counter:{partnerId}:{previousBucket}
-- ARGV[1] = limit
-- ARGV[2] = windowSeconds
-- ARGV[3] = elapsedFraction (0..1, how far into the current window "now" is; computed in Java)
-- returns { allowed(0/1), limit, remaining, resetSeconds }

local currentKey = KEYS[1]
local previousKey = KEYS[2]
local limit = tonumber(ARGV[1])
local windowSeconds = tonumber(ARGV[2])
local elapsedFraction = tonumber(ARGV[3])

local previousCount = tonumber(redis.call('GET', previousKey) or '0')
local currentCount = tonumber(redis.call('GET', currentKey) or '0')

local weightedCount = (previousCount * (1 - elapsedFraction)) + currentCount

if weightedCount < limit then
    local newCount = redis.call('INCR', currentKey)
    if newCount == 1 then
        redis.call('EXPIRE', currentKey, windowSeconds * 2)
    end
    local remaining = math.floor(limit - weightedCount - 1)
    if remaining < 0 then remaining = 0 end
    return {1, limit, remaining, windowSeconds}
else
    return {0, limit, 0, windowSeconds}
end

-- Leaky Bucket — smoothest output, no burst tolerance. Modeled as a virtual queue level
-- that leaks (drains) at a constant rate; a request is admitted only if there's room.
-- KEYS[1] = rl:leaky-bucket:{partnerId}
-- ARGV[1] = capacity
-- ARGV[2] = leakRatePerSecond
-- ARGV[3] = nowMillis
-- returns { allowed(0/1), capacity, roomRemaining, resetSeconds(always 1 - continuous leak) }

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local leakRatePerSecond = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local bucket = redis.call('HMGET', key, 'level', 'lastLeak')
local level = tonumber(bucket[1])
local lastLeak = tonumber(bucket[2])

if level == nil then
    level = 0
    lastLeak = now
end

local elapsedSeconds = (now - lastLeak) / 1000
if elapsedSeconds < 0 then elapsedSeconds = 0 end

level = math.max(0, level - (elapsedSeconds * leakRatePerSecond))

local allowed = 0
if level < capacity then
    level = level + 1
    allowed = 1
end

redis.call('HMSET', key, 'level', level, 'lastLeak', now)
redis.call('EXPIRE', key, 3600)

local remaining = math.floor(capacity - level)
if remaining < 0 then remaining = 0 end

return {allowed, capacity, remaining, 1}

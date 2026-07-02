-- Sliding Window Log — exact but memory-heavy (every request timestamp stored in a ZSET).
-- KEYS[1] = rl:sliding-log:{partnerId}
-- ARGV[1] = nowMillis
-- ARGV[2] = windowMillis
-- ARGV[3] = limit
-- ARGV[4] = unique member suffix (client-generated, avoids same-millisecond collisions)
-- returns { allowed(0/1), limit, remaining, resetSeconds }

local key = KEYS[1]
local now = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local suffix = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, 0, now - windowMs)
local count = redis.call('ZCARD', key)

if count < limit then
    redis.call('ZADD', key, now, now .. '-' .. suffix)
    redis.call('PEXPIRE', key, windowMs)
    return {1, limit, limit - count - 1, math.ceil(windowMs / 1000)}
else
    return {0, limit, 0, math.ceil(windowMs / 1000)}
end

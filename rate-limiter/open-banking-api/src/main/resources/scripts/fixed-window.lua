-- Fixed Window Counter
-- KEYS[1] = rl:fixed:{partnerId}:{windowBucket}  (bucket computed in Java, baked into the key)
-- ARGV[1] = limit
-- ARGV[2] = windowSeconds
-- returns { allowed(0/1), limit, remaining, resetSeconds }

local key = KEYS[1]
local limit = tonumber(ARGV[1])
local windowSeconds = tonumber(ARGV[2])

local count = redis.call('INCR', key)
if count == 1 then
    redis.call('EXPIRE', key, windowSeconds)
end

local ttl = redis.call('TTL', key)
if ttl < 0 then
    ttl = windowSeconds
end

if count > limit then
    return {0, limit, 0, ttl}
else
    return {1, limit, limit - count, ttl}
end

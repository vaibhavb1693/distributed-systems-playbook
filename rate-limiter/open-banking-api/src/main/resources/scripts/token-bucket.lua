-- Token Bucket — burst-friendly; the bucket holds up to `capacity` tokens and refills at
-- `refillRatePerSecond`. A request costs 1 token; if none available, reject.
-- KEYS[1] = rl:token-bucket:{partnerId}
-- ARGV[1] = capacity
-- ARGV[2] = refillRatePerSecond
-- ARGV[3] = nowMillis
-- returns { allowed(0/1), capacity, tokensRemaining, resetSeconds(always 1 - continuous refill, no fixed window) }

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refillRatePerSecond = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local bucket = redis.call('HMGET', key, 'tokens', 'lastRefill')
local tokens = tonumber(bucket[1])
local lastRefill = tonumber(bucket[2])

if tokens == nil then
    tokens = capacity
    lastRefill = now
end

local elapsedSeconds = (now - lastRefill) / 1000
if elapsedSeconds < 0 then elapsedSeconds = 0 end

tokens = math.min(capacity, tokens + (elapsedSeconds * refillRatePerSecond))

local allowed = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'lastRefill', now)
redis.call('EXPIRE', key, 3600)

return {allowed, capacity, math.floor(tokens), 1}

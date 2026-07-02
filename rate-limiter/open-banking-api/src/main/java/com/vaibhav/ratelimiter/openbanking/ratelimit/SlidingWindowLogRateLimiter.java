package com.vaibhav.ratelimiter.openbanking.ratelimit;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * Exact — every request timestamp is stored in a Redis sorted set, so the window boundary
 * problem fixed-window has doesn't exist here. Trade-off: memory grows with request volume
 * (one ZSET entry per request within the window), not just partner count. Best fit for
 * low-volume, high-stakes endpoints (billing/audit APIs per the tier table) where exactness
 * matters more than memory.
 */
@Component("slidingWindowLog")
public class SlidingWindowLogRateLimiter implements RateLimiter {

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<List> script;

    public SlidingWindowLogRateLimiter(StringRedisTemplate redisTemplate,
                                        @Qualifier("slidingWindowLogScript") RedisScript<List> script) {
        this.redisTemplate = redisTemplate;
        this.script = script;
    }

    @Override
    public RateLimitResult tryConsume(String partnerId, RateLimitConfig config) {
        String key = "rl:sliding-log:" + partnerId;
        long nowMillis = System.currentTimeMillis();
        long windowMillis = config.windowSeconds() * 1000;

        @SuppressWarnings("unchecked")
        List<Long> result = redisTemplate.execute(script, List.of(key),
                String.valueOf(nowMillis), String.valueOf(windowMillis), String.valueOf(config.limit()),
                UUID.randomUUID().toString());

        return new RateLimitResult(result.get(0) == 1, result.get(1), result.get(2), result.get(3));
    }
}

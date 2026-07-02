package com.vaibhav.ratelimiter.openbanking.ratelimit;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Smoothest output, zero burst tolerance — modeled as a virtual queue that leaks (drains)
 * at a constant rate; a request is admitted only if there's room in the queue right now.
 * Unlike token bucket, an idle client does NOT accumulate any allowance to spend later.
 * Best fit for shaping traffic into a downstream system that genuinely can't handle bursts.
 */
@Component("leakyBucket")
public class LeakyBucketRateLimiter implements RateLimiter {

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<List> script;

    public LeakyBucketRateLimiter(StringRedisTemplate redisTemplate,
                                   @Qualifier("leakyBucketScript") RedisScript<List> script) {
        this.redisTemplate = redisTemplate;
        this.script = script;
    }

    @Override
    public RateLimitResult tryConsume(String partnerId, RateLimitConfig config) {
        String key = "rl:leaky-bucket:" + partnerId;
        long nowMillis = System.currentTimeMillis();

        @SuppressWarnings("unchecked")
        List<Long> result = redisTemplate.execute(script, List.of(key),
                String.valueOf(config.effectiveCapacity()), String.valueOf(config.ratePerSecond()),
                String.valueOf(nowMillis));

        return new RateLimitResult(result.get(0) == 1, result.get(1), result.get(2), result.get(3));
    }
}

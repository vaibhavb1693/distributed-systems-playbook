package com.vaibhav.ratelimiter.openbanking.ratelimit;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Simplest algorithm — one counter per (partner, window bucket), reset via TTL. Cheap, but
 * a client can send 2x the limit across a window boundary (limit at the end of window N,
 * then limit again right at the start of window N+1).
 */
@Component("fixedWindow")
public class FixedWindowRateLimiter implements RateLimiter {

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<List> script;

    public FixedWindowRateLimiter(StringRedisTemplate redisTemplate,
                                   @Qualifier("fixedWindowScript") RedisScript<List> script) {
        this.redisTemplate = redisTemplate;
        this.script = script;
    }

    @Override
    public RateLimitResult tryConsume(String partnerId, RateLimitConfig config) {
        long nowSeconds = Instant.now().getEpochSecond();
        long bucket = nowSeconds / config.windowSeconds();
        String key = "rl:fixed:" + partnerId + ":" + bucket;

        @SuppressWarnings("unchecked")
        List<Long> result = redisTemplate.execute(script, List.of(key),
                String.valueOf(config.limit()), String.valueOf(config.windowSeconds()));

        return new RateLimitResult(result.get(0) == 1, result.get(1), result.get(2), result.get(3));
    }
}

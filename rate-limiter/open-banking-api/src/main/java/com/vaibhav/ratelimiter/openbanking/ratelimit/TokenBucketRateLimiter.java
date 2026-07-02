package com.vaibhav.ratelimiter.openbanking.ratelimit;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Burst-friendly — the bucket holds up to `effectiveCapacity()` tokens (can exceed the
 * steady-state limit for Enterprise's burst allowance) and refills continuously at
 * limit/windowSeconds tokens/sec. A client that's been idle can burst up to full capacity,
 * then is throttled to the steady rate. Best fit for SDKs and Enterprise tier traffic,
 * which is naturally bursty (batch jobs) rather than smooth.
 */
@Component("tokenBucket")
public class TokenBucketRateLimiter implements RateLimiter {

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<List> script;

    public TokenBucketRateLimiter(StringRedisTemplate redisTemplate,
                                   @Qualifier("tokenBucketScript") RedisScript<List> script) {
        this.redisTemplate = redisTemplate;
        this.script = script;
    }

    @Override
    public RateLimitResult tryConsume(String partnerId, RateLimitConfig config) {
        String key = "rl:token-bucket:" + partnerId;
        long nowMillis = System.currentTimeMillis();

        @SuppressWarnings("unchecked")
        List<Long> result = redisTemplate.execute(script, List.of(key),
                String.valueOf(config.effectiveCapacity()), String.valueOf(config.ratePerSecond()),
                String.valueOf(nowMillis));

        return new RateLimitResult(result.get(0) == 1, result.get(1), result.get(2), result.get(3));
    }
}

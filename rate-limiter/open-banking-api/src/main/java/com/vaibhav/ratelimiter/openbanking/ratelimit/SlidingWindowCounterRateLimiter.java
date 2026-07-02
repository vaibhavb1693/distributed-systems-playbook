package com.vaibhav.ratelimiter.openbanking.ratelimit;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Weighted combination of the current and previous fixed-window counters — approximates a
 * true sliding window without storing every timestamp. Good balance of accuracy (no hard
 * boundary-burst problem) and memory (2 counters per partner, not N timestamps); this is
 * the default recommended algorithm for the Pro tier.
 */
@Component("slidingWindowCounter")
public class SlidingWindowCounterRateLimiter implements RateLimiter {

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<List> script;

    public SlidingWindowCounterRateLimiter(StringRedisTemplate redisTemplate,
                                            @Qualifier("slidingWindowCounterScript") RedisScript<List> script) {
        this.redisTemplate = redisTemplate;
        this.script = script;
    }

    @Override
    public RateLimitResult tryConsume(String partnerId, RateLimitConfig config) {
        long nowSeconds = Instant.now().getEpochSecond();
        long windowSeconds = config.windowSeconds();
        long currentBucket = nowSeconds / windowSeconds;
        long previousBucket = currentBucket - 1;
        double elapsedFraction = (double) (nowSeconds % windowSeconds) / windowSeconds;

        String currentKey = "rl:sliding-counter:" + partnerId + ":" + currentBucket;
        String previousKey = "rl:sliding-counter:" + partnerId + ":" + previousBucket;

        @SuppressWarnings("unchecked")
        List<Long> result = redisTemplate.execute(script, List.of(currentKey, previousKey),
                String.valueOf(config.limit()), String.valueOf(windowSeconds), String.valueOf(elapsedFraction));

        return new RateLimitResult(result.get(0) == 1, result.get(1), result.get(2), result.get(3));
    }
}

package com.vaibhav.ratelimiter.openbanking.ratelimit;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

/**
 * Maps the URL algorithm segment (e.g. "token-bucket" in /ob/token-bucket/accounts) to the
 * matching RateLimiter bean — deliberately an explicit map here rather than relying on
 * Spring bean-name conventions, so the URL vocabulary and the wiring are both visible in
 * one place.
 */
@Component
public class RateLimiterRegistry {

    private final Map<String, RateLimiter> limitersByAlgorithm;

    public RateLimiterRegistry(
            @Qualifier("fixedWindow") RateLimiter fixedWindow,
            @Qualifier("slidingWindowLog") RateLimiter slidingWindowLog,
            @Qualifier("slidingWindowCounter") RateLimiter slidingWindowCounter,
            @Qualifier("tokenBucket") RateLimiter tokenBucket,
            @Qualifier("leakyBucket") RateLimiter leakyBucket) {
        this.limitersByAlgorithm = Map.of(
                "fixed", fixedWindow,
                "sliding-log", slidingWindowLog,
                "sliding-counter", slidingWindowCounter,
                "token-bucket", tokenBucket,
                "leaky-bucket", leakyBucket
        );
    }

    public Optional<RateLimiter> find(String algorithm) {
        return Optional.ofNullable(limitersByAlgorithm.get(algorithm));
    }

    public boolean isValidAlgorithm(String algorithm) {
        return limitersByAlgorithm.containsKey(algorithm);
    }
}

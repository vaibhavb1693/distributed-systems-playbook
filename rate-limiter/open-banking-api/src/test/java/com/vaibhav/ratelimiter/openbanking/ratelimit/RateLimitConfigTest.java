package com.vaibhav.ratelimiter.openbanking.ratelimit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class RateLimitConfigTest {

    @Test
    void effectiveCapacityFallsBackToLimitWhenBurstNotSet() {
        RateLimitConfig config = new RateLimitConfig(60, 60, 0);
        assertThat(config.effectiveCapacity()).isEqualTo(60);
    }

    @Test
    void effectiveCapacityUsesBurstWhenSet() {
        RateLimitConfig config = new RateLimitConfig(10_000, 60, 10_500);
        assertThat(config.effectiveCapacity()).isEqualTo(10_500);
    }

    @Test
    void ratePerSecondDividesLimitByWindow() {
        RateLimitConfig config = new RateLimitConfig(1000, 60, 0);
        assertThat(config.ratePerSecond()).isCloseTo(16.666, within(0.001));
    }
}

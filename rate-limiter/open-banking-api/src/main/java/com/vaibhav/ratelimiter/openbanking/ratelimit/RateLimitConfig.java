package com.vaibhav.ratelimiter.openbanking.ratelimit;

/**
 * One config shape serves all 5 algorithms so a partner's tier limit is directly
 * comparable across algorithms — that's the point of exposing every algorithm as its own
 * endpoint against the same underlying limit.
 *
 * Fixed/sliding-log/sliding-counter use limit + windowSeconds directly. Token bucket and
 * leaky bucket derive capacity from burstCapacity (falling back to limit if unset) and a
 * refill/leak rate of limit/windowSeconds per second.
 */
public record RateLimitConfig(long limit, long windowSeconds, long burstCapacity) {

    public long effectiveCapacity() {
        return burstCapacity > 0 ? burstCapacity : limit;
    }

    public double ratePerSecond() {
        return (double) limit / windowSeconds;
    }
}

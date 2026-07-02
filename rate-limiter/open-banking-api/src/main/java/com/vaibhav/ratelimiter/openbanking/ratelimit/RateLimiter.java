package com.vaibhav.ratelimiter.openbanking.ratelimit;

/**
 * Strategy interface — one implementation per algorithm, all backed by a single atomic
 * Redis Lua script (no read-modify-write race between the check and the update).
 */
public interface RateLimiter {
    RateLimitResult tryConsume(String partnerId, RateLimitConfig config);
}

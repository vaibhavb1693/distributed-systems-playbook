package com.vaibhav.ratelimiter.openbanking.ratelimit;

public record RateLimitResult(boolean allowed, long limit, long remaining, long resetSeconds) {}

package com.vaibhav.ratelimiter.openbanking.partner;

import com.vaibhav.ratelimiter.openbanking.config.RateLimiterProperties;
import com.vaibhav.ratelimiter.openbanking.ratelimit.RateLimitConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * No partner onboarding DB in this demo — a fixed set of demo partners is configured in
 * application.yml. An unregistered partnerId defaults to FREE, the most restrictive tier,
 * which is the safe default for an unknown caller.
 */
@Component
@RequiredArgsConstructor
public class PartnerRegistry {

    private final RateLimiterProperties properties;

    public PartnerTier tierFor(String partnerId) {
        return properties.getPartners().getOrDefault(partnerId, PartnerTier.FREE);
    }

    public RateLimitConfig configFor(PartnerTier tier) {
        RateLimitConfig config = properties.getTiers().get(tier);
        if (config == null) {
            throw new IllegalStateException("No rate limit config configured for tier: " + tier);
        }
        return config;
    }
}

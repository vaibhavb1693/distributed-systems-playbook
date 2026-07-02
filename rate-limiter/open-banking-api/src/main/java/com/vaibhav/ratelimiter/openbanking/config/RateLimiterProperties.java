package com.vaibhav.ratelimiter.openbanking.config;

import com.vaibhav.ratelimiter.openbanking.partner.PartnerTier;
import com.vaibhav.ratelimiter.openbanking.ratelimit.RateLimitConfig;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "rate-limiter")
@Getter
@Setter
public class RateLimiterProperties {

    /** Per-tier limit, applied identically regardless of which algorithm endpoint is hit. */
    private Map<PartnerTier, RateLimitConfig> tiers = new HashMap<>();

    /** Demo partner registry: partnerId -> tier. Unregistered partners default to FREE. */
    private Map<String, PartnerTier> partners = new HashMap<>();
}

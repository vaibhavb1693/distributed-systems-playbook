package com.vaibhav.ratelimiter.openbanking.partner;

import com.vaibhav.ratelimiter.openbanking.config.RateLimiterProperties;
import com.vaibhav.ratelimiter.openbanking.ratelimit.RateLimitConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartnerRegistryTest {

    private PartnerRegistry partnerRegistry;

    @BeforeEach
    void setUp() {
        RateLimiterProperties properties = new RateLimiterProperties();
        properties.setPartners(Map.of("partner-pro-1", PartnerTier.PRO));
        properties.setTiers(Map.of(
                PartnerTier.FREE, new RateLimitConfig(60, 60, 60),
                PartnerTier.PRO, new RateLimitConfig(1000, 60, 1000)
        ));
        partnerRegistry = new PartnerRegistry(properties);
    }

    @Test
    void shouldReturnConfiguredTierForKnownPartner() {
        assertThat(partnerRegistry.tierFor("partner-pro-1")).isEqualTo(PartnerTier.PRO);
    }

    @Test
    void shouldDefaultUnknownPartnerToFree() {
        assertThat(partnerRegistry.tierFor("some-unregistered-partner")).isEqualTo(PartnerTier.FREE);
    }

    @Test
    void shouldReturnConfigForKnownTier() {
        RateLimitConfig config = partnerRegistry.configFor(PartnerTier.PRO);
        assertThat(config.limit()).isEqualTo(1000);
    }

    @Test
    void shouldThrowForTierWithNoConfiguredLimits() {
        assertThatThrownBy(() -> partnerRegistry.configFor(PartnerTier.ENTERPRISE))
                .isInstanceOf(IllegalStateException.class);
    }
}

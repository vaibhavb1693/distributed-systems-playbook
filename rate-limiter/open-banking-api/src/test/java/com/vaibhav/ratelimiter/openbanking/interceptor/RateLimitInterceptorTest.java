package com.vaibhav.ratelimiter.openbanking.interceptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.ratelimiter.openbanking.partner.PartnerRegistry;
import com.vaibhav.ratelimiter.openbanking.partner.PartnerTier;
import com.vaibhav.ratelimiter.openbanking.ratelimit.RateLimitConfig;
import com.vaibhav.ratelimiter.openbanking.ratelimit.RateLimitResult;
import com.vaibhav.ratelimiter.openbanking.ratelimit.RateLimiter;
import com.vaibhav.ratelimiter.openbanking.ratelimit.RateLimiterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RateLimitInterceptorTest {

    @Mock
    private RateLimiterRegistry rateLimiterRegistry;

    @Mock
    private PartnerRegistry partnerRegistry;

    @Mock
    private RateLimiter rateLimiter;

    private SimpleMeterRegistry meterRegistry;
    private RateLimitInterceptor interceptor;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        interceptor = new RateLimitInterceptor(rateLimiterRegistry, partnerRegistry, new ObjectMapper(), meterRegistry);
    }

    @Test
    void shouldRejectRequestMissingPartnerHeader() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/ob/fixed/accounts");
        MockHttpServletResponse response = new MockHttpServletResponse();

        boolean result = interceptor.preHandle(request, response, new Object());

        assertThat(result).isFalse();
        assertThat(response.getStatus()).isEqualTo(400);
        assertThat(response.getContentAsString()).contains("MISSING_PARTNER_ID");
    }

    @Test
    void shouldRejectUnknownAlgorithm() throws Exception {
        when(rateLimiterRegistry.isValidAlgorithm("not-a-real-algorithm")).thenReturn(false);

        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/ob/not-a-real-algorithm/accounts");
        request.addHeader("X-Partner-Id", "partner-free-1");
        MockHttpServletResponse response = new MockHttpServletResponse();

        boolean result = interceptor.preHandle(request, response, new Object());

        assertThat(result).isFalse();
        assertThat(response.getStatus()).isEqualTo(400);
        assertThat(response.getContentAsString()).contains("UNKNOWN_ALGORITHM");
    }

    @Test
    void shouldAllowRequestAndSetRateLimitHeaders() throws Exception {
        RateLimitConfig config = new RateLimitConfig(60, 60, 60);
        when(rateLimiterRegistry.isValidAlgorithm("fixed")).thenReturn(true);
        when(rateLimiterRegistry.find("fixed")).thenReturn(java.util.Optional.of(rateLimiter));
        when(partnerRegistry.tierFor("partner-free-1")).thenReturn(PartnerTier.FREE);
        when(partnerRegistry.configFor(PartnerTier.FREE)).thenReturn(config);
        when(rateLimiter.tryConsume("partner-free-1", config)).thenReturn(new RateLimitResult(true, 60, 59, 60));

        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/ob/fixed/accounts");
        request.addHeader("X-Partner-Id", "partner-free-1");
        MockHttpServletResponse response = new MockHttpServletResponse();

        boolean result = interceptor.preHandle(request, response, new Object());

        assertThat(result).isTrue();
        assertThat(response.getHeader("X-RateLimit-Limit")).isEqualTo("60");
        assertThat(response.getHeader("X-RateLimit-Remaining")).isEqualTo("59");
        assertThat(response.getHeader("X-RateLimit-Reset")).isNotNull();
        assertThat(response.getHeader("Retry-After")).isNull();

        assertThat(meterRegistry.find("ratelimit.requests")
                .tag("algorithm", "fixed").tag("tier", "FREE").tag("outcome", "allowed")
                .counter().count()).isEqualTo(1.0);
    }

    @Test
    void shouldRejectAndSetRetryAfterWhenLimitExceeded() throws Exception {
        RateLimitConfig config = new RateLimitConfig(60, 60, 60);
        when(rateLimiterRegistry.isValidAlgorithm("fixed")).thenReturn(true);
        when(rateLimiterRegistry.find("fixed")).thenReturn(java.util.Optional.of(rateLimiter));
        when(partnerRegistry.tierFor("partner-free-1")).thenReturn(PartnerTier.FREE);
        when(partnerRegistry.configFor(PartnerTier.FREE)).thenReturn(config);
        when(rateLimiter.tryConsume("partner-free-1", config)).thenReturn(new RateLimitResult(false, 60, 0, 42));

        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/ob/fixed/accounts");
        request.addHeader("X-Partner-Id", "partner-free-1");
        MockHttpServletResponse response = new MockHttpServletResponse();

        boolean result = interceptor.preHandle(request, response, new Object());

        assertThat(result).isFalse();
        assertThat(response.getStatus()).isEqualTo(429);
        assertThat(response.getHeader("Retry-After")).isEqualTo("42");
        assertThat(response.getContentAsString()).contains("RATE_LIMIT_EXCEEDED");

        assertThat(meterRegistry.find("ratelimit.requests")
                .tag("algorithm", "fixed").tag("tier", "FREE").tag("outcome", "rejected")
                .counter().count()).isEqualTo(1.0);
    }
}

package com.vaibhav.ratelimiter.openbanking.interceptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.ratelimiter.openbanking.dto.RateLimitErrorResponse;
import com.vaibhav.ratelimiter.openbanking.partner.PartnerRegistry;
import com.vaibhav.ratelimiter.openbanking.partner.PartnerTier;
import com.vaibhav.ratelimiter.openbanking.ratelimit.RateLimitConfig;
import com.vaibhav.ratelimiter.openbanking.ratelimit.RateLimitResult;
import com.vaibhav.ratelimiter.openbanking.ratelimit.RateLimiter;
import com.vaibhav.ratelimiter.openbanking.ratelimit.RateLimiterRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.io.IOException;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * All rate limiting happens here, before the request ever reaches a controller — the
 * controller itself has no knowledge of limits, algorithms, or partners. The algorithm is
 * determined entirely by the URL prefix (/ob/{algorithm}/...), so any partner can hit any
 * algorithm's endpoint against their same tier limit — that's what makes side-by-side
 * comparison possible later.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RateLimitInterceptor implements HandlerInterceptor {

    private static final String PARTNER_HEADER = "X-Partner-Id";
    private static final Pattern ALGORITHM_PATTERN = Pattern.compile("^/ob/([a-z-]+)/.*$");

    private final RateLimiterRegistry rateLimiterRegistry;
    private final PartnerRegistry partnerRegistry;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws IOException {
        String partnerId = request.getHeader(PARTNER_HEADER);
        if (partnerId == null || partnerId.isBlank()) {
            writeError(response, HttpStatus.BAD_REQUEST, "MISSING_PARTNER_ID", PARTNER_HEADER + " header is required");
            return false;
        }

        String algorithm = extractAlgorithm(request.getRequestURI());
        if (algorithm == null || !rateLimiterRegistry.isValidAlgorithm(algorithm)) {
            writeError(response, HttpStatus.BAD_REQUEST, "UNKNOWN_ALGORITHM", "Unknown rate limiting algorithm in path");
            return false;
        }

        PartnerTier tier = partnerRegistry.tierFor(partnerId);
        RateLimitConfig config = partnerRegistry.configFor(tier);
        RateLimiter rateLimiter = rateLimiterRegistry.find(algorithm).orElseThrow();

        RateLimitResult result = rateLimiter.tryConsume(partnerId, config);

        response.setHeader("X-RateLimit-Limit", String.valueOf(result.limit()));
        response.setHeader("X-RateLimit-Remaining", String.valueOf(Math.max(0, result.remaining())));
        response.setHeader("X-RateLimit-Reset", String.valueOf(Instant.now().getEpochSecond() + result.resetSeconds()));

        String outcome = result.allowed() ? "allowed" : "rejected";
        meterRegistry.counter("ratelimit.requests", "algorithm", algorithm, "tier", tier.name(), "outcome", outcome).increment();

        if (!result.allowed()) {
            response.setHeader("Retry-After", String.valueOf(result.resetSeconds()));
            writeError(response, HttpStatus.TOO_MANY_REQUESTS, "RATE_LIMIT_EXCEEDED",
                    "Rate limit exceeded for algorithm=" + algorithm + " tier=" + tier);
            return false;
        }

        return true;
    }

    private String extractAlgorithm(String uri) {
        Matcher matcher = ALGORITHM_PATTERN.matcher(uri);
        return matcher.matches() ? matcher.group(1) : null;
    }

    private void writeError(HttpServletResponse response, HttpStatus status, String error, String message) throws IOException {
        response.setStatus(status.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getWriter().write(objectMapper.writeValueAsString(new RateLimitErrorResponse(error, message)));
    }
}

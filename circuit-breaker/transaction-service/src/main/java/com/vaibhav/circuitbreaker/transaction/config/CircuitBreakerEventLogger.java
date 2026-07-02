package com.vaibhav.circuitbreaker.transaction.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Logs every circuit breaker lifecycle event as a single structured JSON line — state
 * transitions (CLOSED/OPEN/HALF_OPEN), rejected calls while OPEN, and call outcomes.
 * This is the log-based complement to the Micrometer metrics Resilience4j already emits
 * automatically (resilience4j.circuitbreaker.state / .calls) once observability is wired
 * up for this pattern.
 */
@Component
@Slf4j
public class CircuitBreakerEventLogger {

    private static final String CIRCUIT_BREAKER_NAME = "fraudCircuitBreaker";

    public CircuitBreakerEventLogger(CircuitBreakerRegistry registry, ObjectMapper objectMapper) {
        registry.circuitBreaker(CIRCUIT_BREAKER_NAME).getEventPublisher()
                .onStateTransition(event -> logEvent(objectMapper, "STATE_TRANSITION", Map.of(
                        "from", event.getStateTransition().getFromState().name(),
                        "to", event.getStateTransition().getToState().name()
                )))
                .onCallNotPermitted(event -> logEvent(objectMapper, "CALL_NOT_PERMITTED", Map.of()))
                .onError(event -> logEvent(objectMapper, "ERROR", Map.of(
                        "durationMs", event.getElapsedDuration().toMillis(),
                        "errorMessage", String.valueOf(event.getThrowable().getMessage())
                )))
                .onSlowCallRateExceeded(event -> logEvent(objectMapper, "SLOW_CALL_RATE_EXCEEDED", Map.of(
                        "slowCallRate", event.getSlowCallRate()
                )))
                .onFailureRateExceeded(event -> logEvent(objectMapper, "FAILURE_RATE_EXCEEDED", Map.of(
                        "failureRate", event.getFailureRate()
                )))
                .onSuccess(event -> logEvent(objectMapper, "SUCCESS", Map.of(
                        "durationMs", event.getElapsedDuration().toMillis()
                )));
    }

    private void logEvent(ObjectMapper objectMapper, String eventType, Map<String, Object> extra) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("circuitBreaker", CIRCUIT_BREAKER_NAME);
        payload.put("eventType", eventType);
        payload.put("timestamp", Instant.now().toString());
        payload.putAll(extra);
        try {
            log.info(objectMapper.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize circuit breaker event of type {}", eventType, e);
        }
    }
}

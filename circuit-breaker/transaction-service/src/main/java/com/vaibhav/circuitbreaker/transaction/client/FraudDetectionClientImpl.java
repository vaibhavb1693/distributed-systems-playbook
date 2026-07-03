package com.vaibhav.circuitbreaker.transaction.client;

import com.vaibhav.circuitbreaker.transaction.domain.RiskLevel;
import com.vaibhav.circuitbreaker.transaction.dto.FraudScoreRequest;
import com.vaibhav.circuitbreaker.transaction.dto.FraudScoreResponse;
import com.vaibhav.circuitbreaker.transaction.dto.TransactionRequest;
import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

/**
 * All three annotations target the same Resilience4j instance name ("fraudCircuitBreaker")
 * so their config lives together under one key in application.yml. Composition order is
 * Resilience4j's documented default: Retry(outer) -> CircuitBreaker -> Bulkhead(inner,
 * closest to the actual call). A CallNotPermittedException (circuit OPEN) is cheap to
 * retry — no network round-trip — so Retry wrapping CircuitBreaker doesn't waste real work.
 *
 * fallbackMethod MUST live on the outermost annotation (@Retry), not @CircuitBreaker.
 * Confirmed by live testing: a fallback attached to an inner annotation's aspect resolves
 * the call to a normal return (the fallback's result) before the exception ever propagates
 * to an outer annotation — so a fallback on @CircuitBreaker silently swallows every failure
 * before @Retry ever sees anything to retry (Retry's own metrics showed 100%
 * "successful_without_retry" even while genuinely failing calls were happening). Moving the
 * fallback to @Retry lets each individual attempt reach CircuitBreaker's failure-rate
 * tracking normally, retries actually happen up to maxAttempts, and only the fully-exhausted
 * failure (or an immediate CallNotPermittedException when the circuit is already OPEN, which
 * isn't in retryExceptions so it skips straight past retry) invokes the fallback.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class FraudDetectionClientImpl implements FraudDetectionClient {

    private final RestTemplate restTemplate;

    @Value("${fraud.service.url}")
    private String fraudServiceUrl;

    @Override
    @Retry(name = "fraudCircuitBreaker", fallbackMethod = "fallbackAssessRisk")
    @CircuitBreaker(name = "fraudCircuitBreaker")
    @Bulkhead(name = "fraudCircuitBreaker")
    public RiskAssessment assessRisk(TransactionRequest request) {
        FraudScoreRequest scoreRequest = new FraudScoreRequest(request.payerId(), request.payeeId(), request.amount());
        FraudScoreResponse response = restTemplate.postForObject(
                fraudServiceUrl + "/api/fraud/score", scoreRequest, FraudScoreResponse.class);
        return new RiskAssessment(response.riskLevel(), false);
    }

    // Signature must match assessRisk's args + a trailing Throwable — Resilience4j resolves
    // this by reflection. Fail-open, not fail-closed: blocking every transaction during a
    // fraud service outage is worse than letting them through flagged for manual review.
    // Package-private (not private) so it's directly unit-testable without reflection.
    RiskAssessment fallbackAssessRisk(TransactionRequest request, Throwable t) {
        log.warn("Fraud service unavailable for payerId={} payeeId={}, falling back to MEDIUM risk with review flag. Reason: {}",
                request.payerId(), request.payeeId(), t.toString());
        return new RiskAssessment(RiskLevel.MEDIUM, true);
    }
}

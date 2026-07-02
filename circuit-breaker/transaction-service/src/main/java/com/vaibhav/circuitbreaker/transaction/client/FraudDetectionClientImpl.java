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
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class FraudDetectionClientImpl implements FraudDetectionClient {

    private final RestTemplate restTemplate;

    @Value("${fraud.service.url}")
    private String fraudServiceUrl;

    @Override
    @Retry(name = "fraudCircuitBreaker")
    @CircuitBreaker(name = "fraudCircuitBreaker", fallbackMethod = "fallbackAssessRisk")
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

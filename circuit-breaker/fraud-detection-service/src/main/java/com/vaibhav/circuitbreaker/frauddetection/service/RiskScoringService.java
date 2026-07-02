package com.vaibhav.circuitbreaker.frauddetection.service;

import com.vaibhav.circuitbreaker.frauddetection.domain.RiskLevel;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * Deterministic, amount-based scoring — enough to make the demo reproducible without
 * needing an actual ML model. Real fraud scoring is out of scope for this pattern demo;
 * the point is the resilience behavior around calling this service, not the score itself.
 */
@Service
public class RiskScoringService {

    private static final BigDecimal HIGH_RISK_THRESHOLD = BigDecimal.valueOf(100_000);
    private static final BigDecimal MEDIUM_RISK_THRESHOLD = BigDecimal.valueOf(10_000);

    public RiskLevel score(BigDecimal amount) {
        if (amount.compareTo(HIGH_RISK_THRESHOLD) > 0) {
            return RiskLevel.HIGH;
        }
        if (amount.compareTo(MEDIUM_RISK_THRESHOLD) > 0) {
            return RiskLevel.MEDIUM;
        }
        return RiskLevel.LOW;
    }
}

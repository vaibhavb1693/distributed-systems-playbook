package com.vaibhav.circuitbreaker.transaction.client;

import com.vaibhav.circuitbreaker.transaction.domain.RiskLevel;
import com.vaibhav.circuitbreaker.transaction.dto.TransactionRequest;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.ResourceAccessException;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class FraudDetectionClientImplTest {

    private final FraudDetectionClientImpl client = new FraudDetectionClientImpl(null);

    @Test
    void fallbackShouldReturnMediumRiskFlaggedForReview() {
        TransactionRequest request = new TransactionRequest("payer-1", "payee-1", BigDecimal.valueOf(25_000));

        RiskAssessment assessment = client.fallbackAssessRisk(request, new ResourceAccessException("connection refused"));

        assertThat(assessment.riskLevel()).isEqualTo(RiskLevel.MEDIUM);
        assertThat(assessment.flaggedForReview()).isTrue();
    }
}

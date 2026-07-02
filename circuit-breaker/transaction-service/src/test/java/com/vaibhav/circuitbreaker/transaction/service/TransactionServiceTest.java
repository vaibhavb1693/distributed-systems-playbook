package com.vaibhav.circuitbreaker.transaction.service;

import com.vaibhav.circuitbreaker.transaction.client.FraudDetectionClient;
import com.vaibhav.circuitbreaker.transaction.client.RiskAssessment;
import com.vaibhav.circuitbreaker.transaction.domain.RiskLevel;
import com.vaibhav.circuitbreaker.transaction.domain.TransactionStatus;
import com.vaibhav.circuitbreaker.transaction.dto.TransactionRequest;
import com.vaibhav.circuitbreaker.transaction.dto.TransactionResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransactionServiceTest {

    @Mock
    private FraudDetectionClient fraudDetectionClient;

    @InjectMocks
    private TransactionService transactionService;

    private final TransactionRequest request = new TransactionRequest("payer-1", "payee-1", BigDecimal.valueOf(50_000));

    @Test
    void shouldApproveLowRiskTransaction() {
        when(fraudDetectionClient.assessRisk(request)).thenReturn(new RiskAssessment(RiskLevel.LOW, false));

        TransactionResponse response = transactionService.processTransaction(request);

        assertThat(response.status()).isEqualTo(TransactionStatus.APPROVED);
        assertThat(response.riskLevel()).isEqualTo(RiskLevel.LOW);
        assertThat(response.flaggedForReview()).isFalse();
    }

    @Test
    void shouldApproveMediumRiskTransaction() {
        when(fraudDetectionClient.assessRisk(request)).thenReturn(new RiskAssessment(RiskLevel.MEDIUM, false));

        TransactionResponse response = transactionService.processTransaction(request);

        assertThat(response.status()).isEqualTo(TransactionStatus.APPROVED);
    }

    @Test
    void shouldBlockHighRiskTransaction() {
        when(fraudDetectionClient.assessRisk(request)).thenReturn(new RiskAssessment(RiskLevel.HIGH, false));

        TransactionResponse response = transactionService.processTransaction(request);

        assertThat(response.status()).isEqualTo(TransactionStatus.BLOCKED);
    }

    @Test
    void shouldApproveAndFlagWhenFraudServiceFellBackDuringOutage() {
        // Simulates the circuit-open / fallback path: MEDIUM + flaggedForReview=true.
        // Fail-open by design — see RiskAssessment's javadoc.
        when(fraudDetectionClient.assessRisk(request)).thenReturn(new RiskAssessment(RiskLevel.MEDIUM, true));

        TransactionResponse response = transactionService.processTransaction(request);

        assertThat(response.status()).isEqualTo(TransactionStatus.APPROVED);
        assertThat(response.flaggedForReview()).isTrue();
    }
}

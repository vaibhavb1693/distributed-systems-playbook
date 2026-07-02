package com.vaibhav.circuitbreaker.transaction.service;

import com.vaibhav.circuitbreaker.transaction.client.FraudDetectionClient;
import com.vaibhav.circuitbreaker.transaction.client.RiskAssessment;
import com.vaibhav.circuitbreaker.transaction.domain.RiskLevel;
import com.vaibhav.circuitbreaker.transaction.domain.TransactionStatus;
import com.vaibhav.circuitbreaker.transaction.dto.TransactionRequest;
import com.vaibhav.circuitbreaker.transaction.dto.TransactionResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionService {

    private final FraudDetectionClient fraudDetectionClient;

    public TransactionResponse processTransaction(TransactionRequest request) {
        RiskAssessment assessment = fraudDetectionClient.assessRisk(request);

        TransactionStatus status = assessment.riskLevel() == RiskLevel.HIGH
                ? TransactionStatus.BLOCKED
                : TransactionStatus.APPROVED;

        TransactionResponse response = new TransactionResponse(
                UUID.randomUUID(), request.amount(), assessment.riskLevel(), assessment.flaggedForReview(), status);

        log.info("Transaction processed: payerId={} amount={} riskLevel={} flaggedForReview={} status={}",
                request.payerId(), request.amount(), response.riskLevel(), response.flaggedForReview(), response.status());

        return response;
    }
}

package com.vaibhav.circuitbreaker.transaction.client;

import com.vaibhav.circuitbreaker.transaction.dto.TransactionRequest;

public interface FraudDetectionClient {
    RiskAssessment assessRisk(TransactionRequest request);
}

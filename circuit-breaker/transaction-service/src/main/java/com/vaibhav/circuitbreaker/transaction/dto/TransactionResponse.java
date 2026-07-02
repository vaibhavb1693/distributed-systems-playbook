package com.vaibhav.circuitbreaker.transaction.dto;

import com.vaibhav.circuitbreaker.transaction.domain.RiskLevel;
import com.vaibhav.circuitbreaker.transaction.domain.TransactionStatus;

import java.math.BigDecimal;
import java.util.UUID;

public record TransactionResponse(
        UUID transactionId,
        BigDecimal amount,
        RiskLevel riskLevel,
        boolean flaggedForReview,
        TransactionStatus status
) {}

package com.vaibhav.idempotent.loanprocessing.dto;

public record CreditScoreReceivedEvent(
        String eventId,
        String loanId,
        int creditScore,
        String receivedAt
) {}

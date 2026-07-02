package com.vaibhav.idempotent.loanprocessing.dto;

public record LoanDecisionMadeEvent(
        String loanId,
        String decision,
        String decidedAt
) {}

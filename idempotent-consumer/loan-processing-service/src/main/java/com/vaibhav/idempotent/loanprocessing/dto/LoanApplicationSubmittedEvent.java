package com.vaibhav.idempotent.loanprocessing.dto;

import java.math.BigDecimal;

public record LoanApplicationSubmittedEvent(
        String eventId,
        String loanId,
        String applicantName,
        BigDecimal amount,
        String submittedAt
) {}

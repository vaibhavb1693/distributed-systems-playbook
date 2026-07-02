package com.vaibhav.idempotent.loanprocessing.dto;

public record KycDocumentVerifiedEvent(
        String loanId,
        long expectedVersion,
        String verifiedAt
) {}

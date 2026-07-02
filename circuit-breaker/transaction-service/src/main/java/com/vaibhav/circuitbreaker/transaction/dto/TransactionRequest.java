package com.vaibhav.circuitbreaker.transaction.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record TransactionRequest(
        @NotBlank String payerId,
        @NotBlank String payeeId,
        @NotNull @Positive BigDecimal amount
) {}

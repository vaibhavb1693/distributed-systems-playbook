package com.vaibhav.idempotent.loanapplication.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record SubmitApplicationRequest(
        @NotBlank String applicantName,
        @NotNull @Positive BigDecimal amount
) {}

package com.vaibhav.idempotent.loanapplication.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public record CreditScoreRequest(@Min(300) @Max(850) int creditScore) {}

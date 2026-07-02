package com.vaibhav.idempotent.loanapplication.dto;

import jakarta.validation.constraints.Pattern;

public record DecisionRequest(@Pattern(regexp = "APPROVED|REJECTED") String decision) {}

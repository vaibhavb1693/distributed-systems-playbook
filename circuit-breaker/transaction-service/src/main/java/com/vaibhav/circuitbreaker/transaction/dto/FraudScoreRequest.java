package com.vaibhav.circuitbreaker.transaction.dto;

import java.math.BigDecimal;

public record FraudScoreRequest(String payerId, String payeeId, BigDecimal amount) {}

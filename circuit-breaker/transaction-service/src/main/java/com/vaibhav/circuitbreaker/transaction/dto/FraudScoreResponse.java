package com.vaibhav.circuitbreaker.transaction.dto;

import com.vaibhav.circuitbreaker.transaction.domain.RiskLevel;

public record FraudScoreResponse(RiskLevel riskLevel) {}

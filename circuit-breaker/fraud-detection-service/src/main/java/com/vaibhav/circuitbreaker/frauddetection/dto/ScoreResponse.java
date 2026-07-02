package com.vaibhav.circuitbreaker.frauddetection.dto;

import com.vaibhav.circuitbreaker.frauddetection.domain.RiskLevel;

public record ScoreResponse(RiskLevel riskLevel) {}

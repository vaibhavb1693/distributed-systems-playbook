package com.vaibhav.circuitbreaker.transaction.client;

import com.vaibhav.circuitbreaker.transaction.domain.RiskLevel;

/**
 * flaggedForReview is true only when this came from the fallback (fraud service was
 * unavailable/degraded) — it's what distinguishes "the fraud service said MEDIUM" from
 * "we don't actually know, so we degraded to MEDIUM and a human should look at this."
 */
public record RiskAssessment(RiskLevel riskLevel, boolean flaggedForReview) {}

package com.vaibhav.circuitbreaker.frauddetection.service;

import com.vaibhav.circuitbreaker.frauddetection.domain.RiskLevel;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class RiskScoringServiceTest {

    private final RiskScoringService riskScoringService = new RiskScoringService();

    @Test
    void shouldScoreLowForSmallAmount() {
        assertThat(riskScoringService.score(BigDecimal.valueOf(500))).isEqualTo(RiskLevel.LOW);
    }

    @Test
    void shouldScoreLowAtMediumThresholdBoundary() {
        assertThat(riskScoringService.score(BigDecimal.valueOf(10_000))).isEqualTo(RiskLevel.LOW);
    }

    @Test
    void shouldScoreMediumJustAboveThreshold() {
        assertThat(riskScoringService.score(BigDecimal.valueOf(10_001))).isEqualTo(RiskLevel.MEDIUM);
    }

    @Test
    void shouldScoreMediumAtHighThresholdBoundary() {
        assertThat(riskScoringService.score(BigDecimal.valueOf(100_000))).isEqualTo(RiskLevel.MEDIUM);
    }

    @Test
    void shouldScoreHighJustAboveThreshold() {
        assertThat(riskScoringService.score(BigDecimal.valueOf(100_001))).isEqualTo(RiskLevel.HIGH);
    }

    @Test
    void shouldScoreHighForLargeAmount() {
        assertThat(riskScoringService.score(BigDecimal.valueOf(5_000_000))).isEqualTo(RiskLevel.HIGH);
    }
}

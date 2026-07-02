package com.vaibhav.dlq.kycprocessor.dto;

public record DlqStatsResponse(
        long totalReceived,
        long transientCount,
        long permanentCount,
        double replaySuccessRate,
        long pendingManualReviewCount
) {}

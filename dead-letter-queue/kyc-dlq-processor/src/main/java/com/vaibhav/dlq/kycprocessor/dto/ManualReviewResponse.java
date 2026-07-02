package com.vaibhav.dlq.kycprocessor.dto;

import com.vaibhav.dlq.kycprocessor.domain.KycManualReview;

import java.time.LocalDateTime;
import java.util.UUID;

public record ManualReviewResponse(
        UUID id,
        String documentId,
        String userId,
        String failureReason,
        String status,
        LocalDateTime failedAt,
        String resolutionNote,
        String resolvedBy,
        LocalDateTime resolvedAt
) {
    public static ManualReviewResponse from(KycManualReview review) {
        return new ManualReviewResponse(
                review.getId(), review.getDocumentId(), review.getUserId(), review.getFailureReason(),
                review.getStatus(), review.getFailedAt(), review.getResolutionNote(),
                review.getResolvedBy(), review.getResolvedAt());
    }
}

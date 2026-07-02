package com.vaibhav.dlq.kycprocessor.domain;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "kyc_manual_review")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KycManualReview {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "document_id", nullable = false)
    private String documentId;

    @Column(name = "user_id", nullable = false)
    private String userId;

    @Column(name = "failure_reason", nullable = false)
    private String failureReason;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(nullable = false)
    @Builder.Default
    private String status = "PENDING_REVIEW";

    @Column(name = "failed_at", nullable = false)
    private LocalDateTime failedAt;

    @Column(name = "resolution_note", columnDefinition = "text")
    private String resolutionNote;

    @Column(name = "resolved_by")
    private String resolvedBy;

    @Column(name = "resolved_at")
    private LocalDateTime resolvedAt;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
}

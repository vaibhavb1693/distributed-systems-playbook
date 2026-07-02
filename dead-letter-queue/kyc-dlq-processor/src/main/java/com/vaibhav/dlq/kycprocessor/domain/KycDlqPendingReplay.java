package com.vaibhav.dlq.kycprocessor.domain;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "kyc_dlq_pending_replay")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KycDlqPendingReplay {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "document_id", nullable = false)
    private String documentId;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(name = "replay_at", nullable = false)
    private LocalDateTime replayAt;

    @Column(nullable = false)
    @Builder.Default
    private String status = "PENDING";

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
}

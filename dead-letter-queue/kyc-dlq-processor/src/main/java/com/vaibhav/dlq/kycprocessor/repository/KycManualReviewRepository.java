package com.vaibhav.dlq.kycprocessor.repository;

import com.vaibhav.dlq.kycprocessor.domain.KycManualReview;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface KycManualReviewRepository extends JpaRepository<KycManualReview, UUID> {

    long countByStatus(String status);

    Page<KycManualReview> findByStatus(String status, Pageable pageable);

    Page<KycManualReview> findByStatusAndFailureReason(String status, String failureReason, Pageable pageable);

    Optional<KycManualReview> findByDocumentId(String documentId);
}

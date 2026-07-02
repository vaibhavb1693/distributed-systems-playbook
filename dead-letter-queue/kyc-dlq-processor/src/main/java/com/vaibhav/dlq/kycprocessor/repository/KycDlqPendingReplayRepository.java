package com.vaibhav.dlq.kycprocessor.repository;

import com.vaibhav.dlq.kycprocessor.domain.KycDlqPendingReplay;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface KycDlqPendingReplayRepository extends JpaRepository<KycDlqPendingReplay, UUID> {

    List<KycDlqPendingReplay> findByStatusAndReplayAtLessThanEqualOrderByReplayAtAsc(
            String status, LocalDateTime now, Pageable pageable);

    long countByStatus(String status);

    Optional<KycDlqPendingReplay> findByDocumentIdAndStatus(String documentId, String status);
}

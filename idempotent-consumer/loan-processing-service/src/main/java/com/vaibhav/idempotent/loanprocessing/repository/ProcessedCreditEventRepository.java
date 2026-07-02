package com.vaibhav.idempotent.loanprocessing.repository;

import com.vaibhav.idempotent.loanprocessing.domain.ProcessedCreditEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.UUID;

@Repository
public interface ProcessedCreditEventRepository extends JpaRepository<ProcessedCreditEvent, UUID> {

    // Strategy 2: DB unique constraint dedup. A native INSERT (not JpaRepository.save())
    // is required here — save() on an entity with a manually-assigned @Id performs a
    // merge (SELECT-then-INSERT-or-UPDATE), which would silently succeed on a duplicate
    // eventId instead of raising the DataIntegrityViolationException this strategy relies
    // on to detect duplicates.
    @Modifying
    @Query(value = "INSERT INTO processed_credit_events (event_id, loan_id, processed_at) VALUES (:eventId, :loanId, :processedAt)",
            nativeQuery = true)
    void insertOrThrow(@Param("eventId") UUID eventId, @Param("loanId") UUID loanId, @Param("processedAt") LocalDateTime processedAt);
}

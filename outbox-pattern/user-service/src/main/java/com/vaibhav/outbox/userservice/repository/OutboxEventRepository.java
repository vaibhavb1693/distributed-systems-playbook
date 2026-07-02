package com.vaibhav.outbox.userservice.repository;

import com.vaibhav.outbox.userservice.domain.OutboxEvent;
import com.vaibhav.outbox.userservice.domain.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    // FOR UPDATE SKIP LOCKED prevents multiple poller instances from grabbing the same rows.
    // SKIP LOCKED (vs FOR UPDATE NOWAIT) means this instance skips rows locked by another
    // instance rather than throwing an error — correct behaviour for concurrent pollers.
    @Query(value = """
            SELECT * FROM outbox_events
            WHERE status = 'PENDING'
            ORDER BY created_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEvent> findPendingEventsWithLock(@Param("limit") int limit);

    // Events stuck in PROCESSING (app crashed mid-publish) are reset so they get retried.
    @Modifying
    @Query("""
            UPDATE OutboxEvent o SET o.status = 'PENDING'
            WHERE o.status = 'PROCESSING'
            AND o.updatedAt < :threshold
            """)
    int resetStuckProcessingEvents(@Param("threshold") LocalDateTime threshold);

    @Modifying
    @Query("""
            UPDATE OutboxEvent o
            SET o.status = :status, o.publishedAt = :publishedAt
            WHERE o.id = :id
            """)
    void updateStatusById(
            @Param("id") UUID id,
            @Param("status") String status,
            @Param("publishedAt") LocalDateTime publishedAt
    );

    @Modifying
    @Query("UPDATE OutboxEvent o SET o.retryCount = o.retryCount + 1 WHERE o.id = :id")
    void incrementRetryCount(@Param("id") UUID id);

    long countByStatus(OutboxStatus status);
}

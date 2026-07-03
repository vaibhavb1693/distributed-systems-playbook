package com.vaibhav.idempotent.loanprocessing.service;

import com.vaibhav.idempotent.loanprocessing.repository.ProcessedCreditEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Isolated in its own REQUIRES_NEW transaction, and in its own Spring bean (not a method on
 * CreditScoreReceivedConsumer — same self-invocation-proxy-bypass reasoning as outbox-pattern's
 * OutboxEventService).
 *
 * Why REQUIRES_NEW is required here, confirmed by live testing: when the native INSERT hits
 * the unique-constraint violation, Hibernate marks the CURRENT physical transaction
 * rollback-only internally — this happens regardless of whether the caller catches the
 * resulting DataIntegrityViolationException. If the insert ran in the same transaction as the
 * @KafkaListener method (participating, not REQUIRES_NEW), catching the exception and
 * returning normally would still throw UnexpectedRollbackException on commit, since Spring
 * refuses to silently commit a transaction it already knows is rollback-only. Running the
 * insert attempt in its own REQUIRES_NEW transaction means only THIS transaction gets marked
 * rollback-only and cleanly rolled back when the exception propagates out of this method — the
 * caller's own (separate, untouched) transaction is free to catch the rethrown exception and
 * continue normally.
 */
@Service
@RequiredArgsConstructor
public class ProcessedCreditEventInsertService {

    private final ProcessedCreditEventRepository processedCreditEventRepository;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void insertOrThrow(UUID eventId, UUID loanId, LocalDateTime processedAt) {
        processedCreditEventRepository.insertOrThrow(eventId, loanId, processedAt);
    }
}

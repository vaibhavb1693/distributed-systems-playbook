package com.vaibhav.outbox.userservice.service;

import com.vaibhav.outbox.userservice.domain.OutboxEvent;
import com.vaibhav.outbox.userservice.domain.OutboxStatus;
import com.vaibhav.outbox.userservice.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxEventService {

    private final OutboxEventRepository outboxEventRepository;

    // Fetches PENDING events and immediately marks them PROCESSING in the same transaction.
    // FOR UPDATE SKIP LOCKED in the query prevents other poller instances from claiming
    // the same batch. Committing this transaction releases the row lock.
    @Transactional
    public List<OutboxEvent> claimPendingEvents(int batchSize) {
        resetStuckEvents();

        List<OutboxEvent> pending = outboxEventRepository.findPendingEventsWithLock(batchSize);
        if (pending.isEmpty()) {
            return pending;
        }

        pending.forEach(e -> e.setStatus(OutboxStatus.PROCESSING));
        return outboxEventRepository.saveAll(pending);
    }

    // Events stuck in PROCESSING for more than 5 minutes indicate a crash mid-publish.
    // Resetting them to PENDING ensures they get retried on the next poll cycle.
    private void resetStuckEvents() {
        LocalDateTime threshold = LocalDateTime.now().minusMinutes(5);
        int reset = outboxEventRepository.resetStuckProcessingEvents(threshold);
        if (reset > 0) {
            log.warn("Reset {} stuck PROCESSING events older than 5 minutes", reset);
        }
    }

    @Transactional
    public void markPublished(UUID eventId) {
        outboxEventRepository.updateStatusById(
                eventId,
                OutboxStatus.PUBLISHED.name(),
                LocalDateTime.now()
        );
    }

    @Transactional
    public void markPendingForRetry(UUID eventId) {
        outboxEventRepository.incrementRetryCount(eventId);
        outboxEventRepository.updateStatusById(eventId, OutboxStatus.PENDING.name(), null);
    }

    @Transactional
    public void markFailed(UUID eventId) {
        outboxEventRepository.updateStatusById(eventId, OutboxStatus.FAILED.name(), null);
    }
}

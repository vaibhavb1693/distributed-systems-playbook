package com.vaibhav.dlq.kycprocessor.consumer;

import com.vaibhav.dlq.kycprocessor.config.DlqProperties;
import com.vaibhav.dlq.kycprocessor.domain.KycDlqPendingReplay;
import com.vaibhav.dlq.kycprocessor.repository.KycDlqPendingReplayRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Mirrors outbox-pattern's OutboxPoller: poll a Postgres table for due work, publish to
 * Kafka, mark done. Durable across restarts, unlike an in-memory scheduled-task approach —
 * a pending replay that was scheduled 4 minutes before a pod restart still fires on schedule
 * afterward, it just gets picked up by the next poll cycle instead of a timer that died with
 * the old process.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KycDlqReplayPoller {

    private static final String ORIGINAL_TOPIC = "finflow.kyc.document.uploaded";

    private final KycDlqPendingReplayRepository pendingReplayRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final DlqProperties dlqProperties;

    @Scheduled(fixedDelayString = "${kyc.dlq.replay-poll-interval-ms:30000}")
    public void pollAndReplay() {
        List<KycDlqPendingReplay> due = pendingReplayRepository.findByStatusAndReplayAtLessThanEqualOrderByReplayAtAsc(
                "PENDING", LocalDateTime.now(), PageRequest.of(0, dlqProperties.getReplayBatchSize()));

        if (due.isEmpty()) {
            return;
        }

        log.debug("Found {} DLQ items due for auto-replay", due.size());
        for (KycDlqPendingReplay item : due) {
            replayOne(item);
        }
    }

    private void replayOne(KycDlqPendingReplay item) {
        meterRegistry.counter("kyc.dlq.replay.attempted").increment();
        try {
            kafkaTemplate.send(ORIGINAL_TOPIC, item.getDocumentId(), item.getPayload()).get(5, TimeUnit.SECONDS);
            item.setStatus("REPLAYED");
            meterRegistry.counter("kyc.dlq.replay.success").increment();
            log.info("Auto-replayed transient DLQ message: documentId={}", item.getDocumentId());
        } catch (Exception e) {
            item.setStatus("FAILED");
            meterRegistry.counter("kyc.dlq.replay.failed").increment();
            log.error("Failed to auto-replay documentId={}: {}", item.getDocumentId(), e.getMessage());
        }
        pendingReplayRepository.save(item);
    }
}

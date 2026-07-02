package com.vaibhav.dlq.kycprocessor.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.dlq.kycprocessor.config.DlqProperties;
import com.vaibhav.dlq.kycprocessor.domain.KycDlqPendingReplay;
import com.vaibhav.dlq.kycprocessor.domain.KycManualReview;
import com.vaibhav.dlq.kycprocessor.repository.KycDlqPendingReplayRepository;
import com.vaibhav.dlq.kycprocessor.repository.KycManualReviewRepository;
import com.vaibhav.dlq.kycprocessor.service.ManualReviewAlertService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

/**
 * Re-classifies every DLQ arrival using the X-Failure-Type header the producer (kyc-service)
 * attached — this consumer doesn't re-run any pipeline logic, it just routes based on what
 * the producer already determined.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KycDlqConsumer {

    private final KycManualReviewRepository manualReviewRepository;
    private final KycDlqPendingReplayRepository pendingReplayRepository;
    private final ManualReviewAlertService alertService;
    private final MeterRegistry meterRegistry;
    private final DlqProperties dlqProperties;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "finflow.kyc.document.dlq",
            groupId = "kyc-dlq-processor",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onDlqMessage(ConsumerRecord<String, String> record) {
        String failureType = header(record, "X-Failure-Type");
        String failureReason = header(record, "X-Failure-Reason");
        String documentId = header(record, "X-Document-Id");
        String payload = record.value();

        meterRegistry.counter("kyc.dlq.messages.received", "failure_type", String.valueOf(failureType)).increment();

        if ("TRANSIENT".equals(failureType)) {
            scheduleAutoReplay(documentId, payload);
        } else {
            routeToManualReview(documentId, failureReason, payload);
        }
    }

    private void scheduleAutoReplay(String documentId, String payload) {
        LocalDateTime replayAt = LocalDateTime.now().plusMinutes(dlqProperties.getReplayCooldownMinutes());
        pendingReplayRepository.save(KycDlqPendingReplay.builder()
                .documentId(documentId)
                .payload(payload)
                .replayAt(replayAt)
                .status("PENDING")
                .build());
        log.info("Scheduled auto-replay: documentId={} replayAt={}", documentId, replayAt);
    }

    private void routeToManualReview(String documentId, String failureReason, String payload) {
        manualReviewRepository.save(KycManualReview.builder()
                .documentId(documentId)
                .userId(extractUserId(payload))
                .failureReason(failureReason)
                .payload(payload)
                .status("PENDING_REVIEW")
                .failedAt(LocalDateTime.now())
                .build());
        log.error("Routed to manual review: documentId={} reason={}", documentId, failureReason);
        alertService.checkAndAlertIfThresholdExceeded();
    }

    private String extractUserId(String payload) {
        try {
            JsonNode node = objectMapper.readTree(payload);
            return node.has("userId") ? node.get("userId").asText() : "UNKNOWN";
        } catch (Exception e) {
            return "UNKNOWN";
        }
    }

    private String header(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}

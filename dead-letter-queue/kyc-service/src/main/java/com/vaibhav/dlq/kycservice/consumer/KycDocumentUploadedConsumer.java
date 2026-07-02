package com.vaibhav.dlq.kycservice.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.dlq.kycservice.domain.FailureType;
import com.vaibhav.dlq.kycservice.dto.KycDocumentUploadedEvent;
import com.vaibhav.dlq.kycservice.exception.PermanentFailureException;
import com.vaibhav.dlq.kycservice.exception.TransientFailureException;
import com.vaibhav.dlq.kycservice.service.KycPipelineService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.function.LongConsumer;

/**
 * Retries TRANSIENT failures synchronously (blocking the listener thread, not scheduling
 * async work) so that if this process crashes mid-retry, the message hasn't been acked yet
 * (ack-mode=RECORD acks after the listener method returns) and Kafka redelivers it —
 * simpler durability story than an async retry scheduler, at the cost of blocking this
 * partition's processing during the backoff window. Acceptable trade-off for a demo-scale
 * pipeline; a high-throughput real system would offload retries via Spring Kafka's
 * @RetryableTopic / a separate retry topic instead.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KycDocumentUploadedConsumer {

    private static final int MAX_RETRIES = 3;
    private static final long[] BACKOFF_MS = {1000, 2000, 4000};
    private static final String ORIGINAL_TOPIC = "finflow.kyc.document.uploaded";
    private static final String DLQ_TOPIC = "finflow.kyc.document.dlq";

    private final KycPipelineService kycPipelineService;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final LongConsumer backoffSleeper;

    @KafkaListener(
            topics = ORIGINAL_TOPIC,
            groupId = "kyc-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onDocumentUploaded(ConsumerRecord<String, String> record) throws Exception {
        KycDocumentUploadedEvent event = objectMapper.readValue(record.value(), KycDocumentUploadedEvent.class);
        processWithRetries(event);
    }

    void processWithRetries(KycDocumentUploadedEvent event) {
        int attempt = 0;
        while (true) {
            try {
                kycPipelineService.process(event);
                meterRegistry.counter("kyc.pipeline.processed", "outcome", "success").increment();
                return;
            } catch (TransientFailureException e) {
                if (attempt >= MAX_RETRIES) {
                    publishToDlq(event, FailureType.TRANSIENT, e.getReasonCode(), e.getMessage(), attempt);
                    meterRegistry.counter("kyc.pipeline.processed", "outcome", "transient_failure").increment();
                    return;
                }
                long backoff = BACKOFF_MS[attempt];
                log.warn("Transient KYC failure (attempt {}/{}), retrying in {}ms: documentId={} reason={}",
                        attempt + 1, MAX_RETRIES, backoff, event.documentId(), e.getReasonCode());
                backoffSleeper.accept(backoff);
                attempt++;
            } catch (PermanentFailureException e) {
                publishToDlq(event, FailureType.PERMANENT, e.getReasonCode(), e.getMessage(), attempt);
                meterRegistry.counter("kyc.pipeline.processed", "outcome", "permanent_failure").increment();
                return;
            }
        }
    }

    private void publishToDlq(KycDocumentUploadedEvent event, FailureType type, String reasonCode, String message, int retryCount) {
        try {
            String json = objectMapper.writeValueAsString(event);
            ProducerRecord<String, String> record = new ProducerRecord<>(DLQ_TOPIC, event.documentId(), json);
            record.headers().add("X-Failure-Reason", reasonCode.getBytes(StandardCharsets.UTF_8));
            record.headers().add("X-Failure-Type", type.name().getBytes(StandardCharsets.UTF_8));
            record.headers().add("X-Retry-Count", String.valueOf(retryCount).getBytes(StandardCharsets.UTF_8));
            record.headers().add("X-Original-Topic", ORIGINAL_TOPIC.getBytes(StandardCharsets.UTF_8));
            record.headers().add("X-Failed-At", Instant.now().toString().getBytes(StandardCharsets.UTF_8));
            record.headers().add("X-Document-Id", event.documentId().getBytes(StandardCharsets.UTF_8));

            kafkaTemplate.send(record);
            log.error("Routed to DLQ: documentId={} failureType={} reason={} message={} retryCount={}",
                    event.documentId(), type, reasonCode, message, retryCount);
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish document to DLQ", e);
        }
    }
}

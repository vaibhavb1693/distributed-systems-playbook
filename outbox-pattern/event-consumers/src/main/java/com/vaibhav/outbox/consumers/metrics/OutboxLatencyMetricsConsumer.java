package com.vaibhav.outbox.consumers.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.outbox.consumers.model.UserEvent;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Records end-to-end outbox latency (event created in Postgres -> received here), tagged
 * by publishing mode (polling|cdc) and topic. Runs in its own consumer group so it gets an
 * independent copy of every message — recording the metric inside NotificationConsumer or
 * AuditConsumer instead would double-count every event since both already consume all
 * three topics.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxLatencyMetricsConsumer {

    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    @KafkaListener(
            topics = {
                    "finflow.user.registered",
                    "finflow.user.profile.updated",
                    "finflow.user.kyc.updated"
            },
            groupId = "finflow-metrics-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onUserEvent(ConsumerRecord<String, String> record,
                             @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        UserEvent event = deserialize(record.value());

        String createdAtRaw = event.get("eventCreatedAtMs");
        String mode = event.get("mode");
        if (createdAtRaw.isBlank() || mode.isBlank()) {
            log.debug("Skipping latency metric: missing eventCreatedAtMs/mode on topic={}", topic);
            return;
        }

        try {
            long createdAtMs = Long.parseLong(createdAtRaw);
            long latencyMs = Math.max(0, System.currentTimeMillis() - createdAtMs);
            meterRegistry.timer("outbox.event.latency", "mode", mode, "topic", topic)
                    .record(Duration.ofMillis(latencyMs));
        } catch (NumberFormatException e) {
            log.debug("Skipping latency metric: unparsable eventCreatedAtMs={}", createdAtRaw);
        }
    }

    private UserEvent deserialize(String json) {
        try {
            return objectMapper.readValue(json, UserEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize user event: " + json, e);
        }
    }
}

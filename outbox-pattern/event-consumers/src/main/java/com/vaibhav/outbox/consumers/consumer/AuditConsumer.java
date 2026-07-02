package com.vaibhav.outbox.consumers.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.outbox.consumers.model.UserEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Simulates a compliance audit log consumer.
 * In production this would write immutable records to an append-only audit store.
 * Regulators require a complete, tamper-proof history of all user lifecycle events.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class AuditConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = {
                    "finflow.user.registered",
                    "finflow.user.profile.updated",
                    "finflow.user.kyc.updated"
            },
            groupId = "finflow-audit-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onUserEvent(ConsumerRecord<String, String> record) {
        UserEvent event = deserialize(record.value());

        log.info("""
                [AUDIT] Immutable record written →
                  topic     : {}
                  partition : {}
                  offset    : {}
                  userId    : {}
                  kafkaKey  : {}
                  receivedAt: {}
                  payload   : {}
                """,
                record.topic(),
                record.partition(),
                record.offset(),
                event.getUserId(),
                record.key(),
                Instant.ofEpochMilli(record.timestamp()),
                record.value()
        );
    }

    private UserEvent deserialize(String json) {
        try {
            return objectMapper.readValue(json, UserEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize audit event: " + json, e);
        }
    }
}

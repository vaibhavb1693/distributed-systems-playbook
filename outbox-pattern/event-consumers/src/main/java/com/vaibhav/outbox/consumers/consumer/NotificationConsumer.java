package com.vaibhav.outbox.consumers.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.outbox.consumers.model.UserEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "finflow.user.registered",
            groupId = "finflow-notification-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onUserRegistered(ConsumerRecord<String, String> record,
                                 @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        UserEvent event = deserialize(record.value());
        log.info("[NOTIFICATION] Welcome email queued for userId={}, email={}",
                event.getUserId(), event.get("email"));
        log.info("[NOTIFICATION] SMS queued: 'Your FinFlow account is ready' → {}", event.get("phone"));
    }

    @KafkaListener(
            topics = "finflow.user.kyc.updated",
            groupId = "finflow-notification-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onKycStatusUpdated(ConsumerRecord<String, String> record) {
        UserEvent event = deserialize(record.value());
        String newStatus = event.get("newStatus");
        log.info("[NOTIFICATION] KYC status notification queued for userId={} — new status: {}",
                event.getUserId(), newStatus);

        if ("VERIFIED".equals(newStatus)) {
            log.info("[NOTIFICATION] SMS queued: 'KYC verified! You are now fully onboarded on FinFlow.'");
        } else if ("REJECTED".equals(newStatus)) {
            log.info("[NOTIFICATION] Email queued: 'KYC verification failed. Please re-submit your documents.'");
        }
    }

    @KafkaListener(
            topics = "finflow.user.profile.updated",
            groupId = "finflow-notification-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onProfileUpdated(ConsumerRecord<String, String> record) {
        UserEvent event = deserialize(record.value());
        log.info("[NOTIFICATION] Profile update confirmation email queued for userId={}", event.getUserId());
    }

    private UserEvent deserialize(String json) {
        try {
            return objectMapper.readValue(json, UserEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize user event: " + json, e);
        }
    }
}

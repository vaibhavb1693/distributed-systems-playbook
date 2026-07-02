package com.vaibhav.outbox.consumers.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.outbox.consumers.model.UserEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class NotificationConsumerTest {

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private NotificationConsumer notificationConsumer;

    @Test
    void shouldProcessUserRegisteredEventWithoutError() {
        String payload = """
                {
                    "userId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                    "name": "Vaibhav Bhatt",
                    "email": "vaibhav@finflow.com",
                    "phone": "+91-9876543210",
                    "kycStatus": "PENDING",
                    "registeredAt": "2026-07-02T10:00:00"
                }
                """;

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "finflow.user.registered", 0, 0L,
                "a1b2c3d4-e5f6-7890-abcd-ef1234567890", payload
        );

        assertDoesNotThrow(() ->
                notificationConsumer.onUserRegistered(record, "finflow.user.registered")
        );
    }

    @Test
    void shouldProcessKycVerifiedEventWithoutError() {
        String payload = """
                {
                    "userId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                    "previousStatus": "SUBMITTED",
                    "newStatus": "VERIFIED",
                    "updatedAt": "2026-07-02T11:00:00"
                }
                """;

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "finflow.user.kyc.updated", 0, 1L,
                "a1b2c3d4-e5f6-7890-abcd-ef1234567890", payload
        );

        assertDoesNotThrow(() -> notificationConsumer.onKycStatusUpdated(record));
    }

    @Test
    void shouldThrowOnMalformedJson() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "finflow.user.registered", 0, 0L,
                "some-key", "not-valid-json{"
        );

        assertThrows(RuntimeException.class, () ->
                notificationConsumer.onUserRegistered(record, "finflow.user.registered")
        );
    }
}

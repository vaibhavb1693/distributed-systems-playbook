package com.vaibhav.outbox.consumers.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class OutboxLatencyMetricsConsumerTest {

    private SimpleMeterRegistry meterRegistry;
    private OutboxLatencyMetricsConsumer consumer;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        consumer = new OutboxLatencyMetricsConsumer(new ObjectMapper(), meterRegistry);
    }

    @Test
    void shouldRecordLatencyForWellFormedEvent() {
        long createdAtMs = System.currentTimeMillis() - 42;
        String payload = """
                {
                    "userId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                    "eventCreatedAtMs": "%d",
                    "mode": "polling"
                }
                """.formatted(createdAtMs);

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "finflow.user.registered", 0, 0L,
                "a1b2c3d4-e5f6-7890-abcd-ef1234567890", payload
        );

        consumer.onUserEvent(record, "finflow.user.registered");

        Timer timer = meterRegistry.find("outbox.event.latency")
                .tag("mode", "polling")
                .tag("topic", "finflow.user.registered")
                .timer();

        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1);
    }

    @Test
    void shouldSkipSilentlyWhenFieldsMissing() {
        String payload = """
                {
                    "userId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
                }
                """;

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "finflow.user.registered", 0, 0L,
                "a1b2c3d4-e5f6-7890-abcd-ef1234567890", payload
        );

        assertDoesNotThrow(() -> consumer.onUserEvent(record, "finflow.user.registered"));
        assertThat(meterRegistry.find("outbox.event.latency").timer()).isNull();
    }
}

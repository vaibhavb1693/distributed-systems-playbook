package com.vaibhav.idempotent.loanprocessing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.idempotent.loanprocessing.domain.LoanApplication;
import com.vaibhav.idempotent.loanprocessing.repository.LoanApplicationRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LoanApplicationSubmittedConsumerTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private LoanApplicationRepository loanApplicationRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private SimpleMeterRegistry meterRegistry;
    private LoanApplicationSubmittedConsumer consumer;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        consumer = new LoanApplicationSubmittedConsumer(redisTemplate, loanApplicationRepository, objectMapper, meterRegistry);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    void shouldProcessFirstDelivery() throws Exception {
        when(valueOperations.setIfAbsent(anyString(), eq("1"), any(Duration.class))).thenReturn(true);

        consumer.onApplicationSubmitted(buildRecord(UUID.randomUUID(), UUID.randomUUID()));

        verify(loanApplicationRepository).save(any(LoanApplication.class));
        assertThat(outcomeCount("processed")).isEqualTo(1.0);
        assertThat(outcomeCount("duplicate")).isEqualTo(0.0);
    }

    @Test
    void shouldSkipDuplicateDelivery() throws Exception {
        when(valueOperations.setIfAbsent(anyString(), eq("1"), any(Duration.class))).thenReturn(false);

        consumer.onApplicationSubmitted(buildRecord(UUID.randomUUID(), UUID.randomUUID()));

        verify(loanApplicationRepository, never()).save(any());
        assertThat(outcomeCount("duplicate")).isEqualTo(1.0);
        assertThat(outcomeCount("processed")).isEqualTo(0.0);
    }

    private double outcomeCount(String outcome) {
        var counter = meterRegistry.find("idempotent.consumer.events")
                .tag("strategy", "redis-ttl")
                .tag("topic", "finflow.loan.application.submitted")
                .tag("outcome", outcome)
                .counter();
        return counter == null ? 0.0 : counter.count();
    }

    private ConsumerRecord<String, String> buildRecord(UUID eventId, UUID loanId) {
        String payload = """
                {
                    "eventId": "%s",
                    "loanId": "%s",
                    "applicantName": "Vaibhav Bhatt",
                    "amount": 500000,
                    "submittedAt": "2026-07-02T10:00:00"
                }
                """.formatted(eventId, loanId);
        return new ConsumerRecord<>("finflow.loan.application.submitted", 0, 0L, loanId.toString(), payload);
    }
}

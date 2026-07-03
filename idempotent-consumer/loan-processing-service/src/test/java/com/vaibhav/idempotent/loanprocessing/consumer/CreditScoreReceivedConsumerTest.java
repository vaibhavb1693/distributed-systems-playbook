package com.vaibhav.idempotent.loanprocessing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.idempotent.loanprocessing.repository.LoanApplicationRepository;
import com.vaibhav.idempotent.loanprocessing.service.ProcessedCreditEventInsertService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CreditScoreReceivedConsumerTest {

    @Mock
    private ProcessedCreditEventInsertService processedCreditEventInsertService;

    @Mock
    private LoanApplicationRepository loanApplicationRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private SimpleMeterRegistry meterRegistry;
    private CreditScoreReceivedConsumer consumer;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        consumer = new CreditScoreReceivedConsumer(processedCreditEventInsertService, loanApplicationRepository, objectMapper, meterRegistry);
    }

    @Test
    void shouldProcessFirstDelivery() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID loanId = UUID.randomUUID();

        consumer.onCreditScoreReceived(buildRecord(eventId, loanId, 720));

        verify(processedCreditEventInsertService).insertOrThrow(eq(eventId), eq(loanId), any(LocalDateTime.class));
        verify(loanApplicationRepository).updateCreditScore(loanId, 720);
        assertThat(outcomeCount("processed")).isEqualTo(1.0);
    }

    @Test
    void shouldSkipDuplicateDeliveryOnConstraintViolation() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID loanId = UUID.randomUUID();
        doThrow(new DataIntegrityViolationException("duplicate key"))
                .when(processedCreditEventInsertService).insertOrThrow(eq(eventId), eq(loanId), any(LocalDateTime.class));

        consumer.onCreditScoreReceived(buildRecord(eventId, loanId, 720));

        verify(loanApplicationRepository, never()).updateCreditScore(any(), anyInt());
        assertThat(outcomeCount("duplicate")).isEqualTo(1.0);
        assertThat(outcomeCount("processed")).isEqualTo(0.0);
    }

    private double outcomeCount(String outcome) {
        var counter = meterRegistry.find("idempotent.consumer.events")
                .tag("strategy", "db-unique-constraint")
                .tag("topic", "finflow.credit.score.received")
                .tag("outcome", outcome)
                .counter();
        return counter == null ? 0.0 : counter.count();
    }

    private ConsumerRecord<String, String> buildRecord(UUID eventId, UUID loanId, int creditScore) {
        String payload = """
                {
                    "eventId": "%s",
                    "loanId": "%s",
                    "creditScore": %d,
                    "receivedAt": "2026-07-02T10:00:00"
                }
                """.formatted(eventId, loanId, creditScore);
        return new ConsumerRecord<>("finflow.credit.score.received", 0, 0L, loanId.toString(), payload);
    }
}

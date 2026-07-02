package com.vaibhav.idempotent.loanprocessing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.idempotent.loanprocessing.repository.LoanDecisionRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class LoanDecisionMadeConsumerTest {

    @Mock
    private LoanDecisionRepository loanDecisionRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private SimpleMeterRegistry meterRegistry;
    private LoanDecisionMadeConsumer consumer;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        consumer = new LoanDecisionMadeConsumer(loanDecisionRepository, objectMapper, meterRegistry);
    }

    @Test
    void shouldUpsertOnFirstDelivery() throws Exception {
        UUID loanId = UUID.randomUUID();

        consumer.onLoanDecisionMade(buildRecord(loanId, "APPROVED"));

        verify(loanDecisionRepository).upsert(eq(loanId), eq("APPROVED"), any(LocalDateTime.class));
        assertThat(processedCount()).isEqualTo(1.0);
    }

    @Test
    void shouldUpsertAgainOnRedeliveryWithoutError() throws Exception {
        // Natural idempotency: applying the same decision N times is safe and expected to
        // just call upsert again each time — there's no dedup gate to prove here, only that
        // repeated delivery doesn't throw or corrupt state.
        UUID loanId = UUID.randomUUID();

        consumer.onLoanDecisionMade(buildRecord(loanId, "APPROVED"));
        consumer.onLoanDecisionMade(buildRecord(loanId, "APPROVED"));
        consumer.onLoanDecisionMade(buildRecord(loanId, "APPROVED"));

        verify(loanDecisionRepository, times(3)).upsert(eq(loanId), eq("APPROVED"), any(LocalDateTime.class));
        assertThat(processedCount()).isEqualTo(3.0);
    }

    private double processedCount() {
        var counter = meterRegistry.find("idempotent.consumer.events")
                .tag("strategy", "upsert")
                .tag("topic", "finflow.loan.decision.made")
                .tag("outcome", "processed")
                .counter();
        return counter == null ? 0.0 : counter.count();
    }

    private ConsumerRecord<String, String> buildRecord(UUID loanId, String decision) {
        String payload = """
                {
                    "loanId": "%s",
                    "decision": "%s",
                    "decidedAt": "2026-07-02T10:00:00"
                }
                """.formatted(loanId, decision);
        return new ConsumerRecord<>("finflow.loan.decision.made", 0, 0L, loanId.toString(), payload);
    }
}

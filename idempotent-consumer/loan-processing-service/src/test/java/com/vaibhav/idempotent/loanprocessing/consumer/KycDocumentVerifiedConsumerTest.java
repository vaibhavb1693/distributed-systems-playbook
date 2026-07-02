package com.vaibhav.idempotent.loanprocessing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.idempotent.loanprocessing.repository.LoanApplicationRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KycDocumentVerifiedConsumerTest {

    @Mock
    private LoanApplicationRepository loanApplicationRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private SimpleMeterRegistry meterRegistry;
    private KycDocumentVerifiedConsumer consumer;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        consumer = new KycDocumentVerifiedConsumer(loanApplicationRepository, objectMapper, meterRegistry);
    }

    @Test
    void shouldProcessWhenVersionMatches() throws Exception {
        UUID loanId = UUID.randomUUID();
        when(loanApplicationRepository.markKycVerifiedIfVersionMatches(loanId, 0L)).thenReturn(1);

        consumer.onKycDocumentVerified(buildRecord(loanId, 0));

        assertThat(outcomeCount("processed")).isEqualTo(1.0);
        assertThat(outcomeCount("duplicate")).isEqualTo(0.0);
    }

    @Test
    void shouldSuppressStaleOrDuplicateWhenVersionMismatches() throws Exception {
        UUID loanId = UUID.randomUUID();
        // Version already advanced by a prior (first) delivery of this same event.
        when(loanApplicationRepository.markKycVerifiedIfVersionMatches(loanId, 0L)).thenReturn(0);

        consumer.onKycDocumentVerified(buildRecord(loanId, 0));

        assertThat(outcomeCount("duplicate")).isEqualTo(1.0);
        assertThat(outcomeCount("processed")).isEqualTo(0.0);
    }

    private double outcomeCount(String outcome) {
        var counter = meterRegistry.find("idempotent.consumer.events")
                .tag("strategy", "optimistic-lock")
                .tag("topic", "finflow.kyc.document.verified")
                .tag("outcome", outcome)
                .counter();
        return counter == null ? 0.0 : counter.count();
    }

    private ConsumerRecord<String, String> buildRecord(UUID loanId, long expectedVersion) {
        String payload = """
                {
                    "loanId": "%s",
                    "expectedVersion": %d,
                    "verifiedAt": "2026-07-02T10:00:00"
                }
                """.formatted(loanId, expectedVersion);
        return new ConsumerRecord<>("finflow.kyc.document.verified", 0, 0L, loanId.toString(), payload);
    }
}

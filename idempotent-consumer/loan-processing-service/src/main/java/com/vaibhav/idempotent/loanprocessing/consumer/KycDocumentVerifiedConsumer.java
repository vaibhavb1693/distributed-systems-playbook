package com.vaibhav.idempotent.loanprocessing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.idempotent.loanprocessing.dto.KycDocumentVerifiedEvent;
import com.vaibhav.idempotent.loanprocessing.repository.LoanApplicationRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Strategy 3: optimistic locking / version check. The event carries the version the
 * producer last observed; the UPDATE only applies if that still matches the current row.
 * Zero rows updated means either a duplicate redelivery (version already advanced by the
 * first delivery) or a genuinely stale/out-of-order event — either way, skip rather than
 * overwrite newer state with older data.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KycDocumentVerifiedConsumer {

    private static final String STRATEGY = "optimistic-lock";

    private final LoanApplicationRepository loanApplicationRepository;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    @KafkaListener(
            topics = "finflow.kyc.document.verified",
            groupId = "loan-processing-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void onKycDocumentVerified(ConsumerRecord<String, String> record) throws Exception {
        KycDocumentVerifiedEvent event = objectMapper.readValue(record.value(), KycDocumentVerifiedEvent.class);
        UUID loanId = UUID.fromString(event.loanId());

        int updated = loanApplicationRepository.markKycVerifiedIfVersionMatches(loanId, event.expectedVersion());

        if (updated == 0) {
            log.warn("Stale or duplicate KYC event suppressed (version mismatch): loanId={} expectedVersion={}",
                    loanId, event.expectedVersion());
            recordMetric("duplicate");
            return;
        }

        log.info("KYC verification processed: loanId={} expectedVersion={}", loanId, event.expectedVersion());
        recordMetric("processed");
    }

    private void recordMetric(String outcome) {
        meterRegistry.counter("idempotent.consumer.events",
                "strategy", STRATEGY,
                "topic", "finflow.kyc.document.verified",
                "outcome", outcome).increment();
    }
}

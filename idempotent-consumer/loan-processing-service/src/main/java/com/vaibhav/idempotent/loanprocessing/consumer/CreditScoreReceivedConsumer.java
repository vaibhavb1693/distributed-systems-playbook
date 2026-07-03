package com.vaibhav.idempotent.loanprocessing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.idempotent.loanprocessing.dto.CreditScoreReceivedEvent;
import com.vaibhav.idempotent.loanprocessing.repository.LoanApplicationRepository;
import com.vaibhav.idempotent.loanprocessing.service.ProcessedCreditEventInsertService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Strategy 2: DB unique constraint dedup. The INSERT into processed_credit_events is the
 * dedup gate — a duplicate eventId raises DataIntegrityViolationException on the PK, which
 * we catch and skip. This also doubles as a permanent, queryable audit trail of every
 * credit score event ever processed (unlike Strategy 1's TTL-bounded Redis keys).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class CreditScoreReceivedConsumer {

    private static final String STRATEGY = "db-unique-constraint";

    private final ProcessedCreditEventInsertService processedCreditEventInsertService;
    private final LoanApplicationRepository loanApplicationRepository;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    @KafkaListener(
            topics = "finflow.credit.score.received",
            groupId = "loan-processing-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void onCreditScoreReceived(ConsumerRecord<String, String> record) throws Exception {
        CreditScoreReceivedEvent event = objectMapper.readValue(record.value(), CreditScoreReceivedEvent.class);
        UUID eventId = UUID.fromString(event.eventId());
        UUID loanId = UUID.fromString(event.loanId());

        try {
            // REQUIRES_NEW, its own physical transaction — see ProcessedCreditEventInsertService's
            // javadoc for why that's required (not just a style preference) to avoid
            // UnexpectedRollbackException when this catch block returns normally.
            processedCreditEventInsertService.insertOrThrow(eventId, loanId, LocalDateTime.now());
        } catch (DataIntegrityViolationException e) {
            log.info("Duplicate suppressed (DB unique constraint): eventId={} loanId={}", eventId, loanId);
            recordMetric("duplicate");
            return;
        }

        loanApplicationRepository.updateCreditScore(loanId, event.creditScore());
        log.info("Credit score processed: loanId={} creditScore={}", loanId, event.creditScore());
        recordMetric("processed");
    }

    private void recordMetric(String outcome) {
        meterRegistry.counter("idempotent.consumer.events",
                "strategy", STRATEGY,
                "topic", "finflow.credit.score.received",
                "outcome", outcome).increment();
    }
}

package com.vaibhav.idempotent.loanprocessing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.idempotent.loanprocessing.dto.LoanDecisionMadeEvent;
import com.vaibhav.idempotent.loanprocessing.repository.LoanDecisionRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Strategy 4: natural idempotency via upsert. A decision is a terminal, convergent state —
 * applying the same (or even a corrected) decision any number of times just leaves
 * loan_decisions in the same end state. No dedup bookkeeping needed, which is the whole
 * point: this strategy only works because the operation is naturally idempotent, not
 * because we detected and skipped a duplicate.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class LoanDecisionMadeConsumer {

    private static final String STRATEGY = "upsert";

    private final LoanDecisionRepository loanDecisionRepository;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    @KafkaListener(
            topics = "finflow.loan.decision.made",
            groupId = "loan-processing-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onLoanDecisionMade(ConsumerRecord<String, String> record) throws Exception {
        LoanDecisionMadeEvent event = objectMapper.readValue(record.value(), LoanDecisionMadeEvent.class);
        UUID loanId = UUID.fromString(event.loanId());

        loanDecisionRepository.upsert(loanId, event.decision(), LocalDateTime.now());

        log.info("Decision upserted (naturally idempotent): loanId={} decision={}", loanId, event.decision());
        meterRegistry.counter("idempotent.consumer.events",
                "strategy", STRATEGY,
                "topic", "finflow.loan.decision.made",
                "outcome", "processed").increment();
    }
}

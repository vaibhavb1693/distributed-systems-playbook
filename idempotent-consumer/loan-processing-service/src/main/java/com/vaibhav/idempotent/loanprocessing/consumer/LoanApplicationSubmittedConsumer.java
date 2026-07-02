package com.vaibhav.idempotent.loanprocessing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.idempotent.loanprocessing.domain.LoanApplication;
import com.vaibhav.idempotent.loanprocessing.dto.LoanApplicationSubmittedEvent;
import com.vaibhav.idempotent.loanprocessing.repository.LoanApplicationRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Strategy 1: Redis TTL dedup. SET NX EX is a single atomic Redis command — the first
 * delivery wins the key and proceeds; every redelivery within the 24h window finds the
 * key already set and is skipped. Best for high-frequency events where a bounded dedup
 * window is acceptable and DB write overhead per-event isn't.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class LoanApplicationSubmittedConsumer {

    private static final Duration DEDUP_TTL = Duration.ofHours(24);
    private static final String STRATEGY = "redis-ttl";

    private final StringRedisTemplate redisTemplate;
    private final LoanApplicationRepository loanApplicationRepository;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    @KafkaListener(
            topics = "finflow.loan.application.submitted",
            groupId = "loan-processing-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onApplicationSubmitted(ConsumerRecord<String, String> record) throws Exception {
        LoanApplicationSubmittedEvent event = objectMapper.readValue(record.value(), LoanApplicationSubmittedEvent.class);
        String dedupKey = "dedup:loan-application-submitted:" + event.eventId();

        Boolean firstDelivery = redisTemplate.opsForValue().setIfAbsent(dedupKey, "1", DEDUP_TTL);
        if (!Boolean.TRUE.equals(firstDelivery)) {
            log.info("Duplicate suppressed (Redis TTL dedup): eventId={} loanId={}", event.eventId(), event.loanId());
            recordMetric("duplicate");
            return;
        }

        LoanApplication application = LoanApplication.builder()
                .id(java.util.UUID.fromString(event.loanId()))
                .applicantName(event.applicantName())
                .amount(event.amount())
                .build();
        loanApplicationRepository.save(application);

        log.info("Loan application processed: loanId={} applicantName={}", event.loanId(), event.applicantName());
        recordMetric("processed");
    }

    private void recordMetric(String outcome) {
        meterRegistry.counter("idempotent.consumer.events",
                "strategy", STRATEGY,
                "topic", "finflow.loan.application.submitted",
                "outcome", outcome).increment();
    }
}

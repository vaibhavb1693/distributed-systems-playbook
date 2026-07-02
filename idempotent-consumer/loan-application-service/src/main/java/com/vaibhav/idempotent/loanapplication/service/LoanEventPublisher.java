package com.vaibhav.idempotent.loanapplication.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Publishes the 4 loan lifecycle events straight to Kafka — no outbox here, since this
 * pattern is about consumer-side dedup, not producer-side delivery guarantees (that's
 * outbox-pattern's job).
 *
 * Every publish method accepts `times` so a demo/load-test can deliberately redeliver the
 * *same* event (same eventId / same expectedVersion / same natural key) to prove the
 * downstream consumer's dedup strategy actually works — that's the whole point of this
 * service existing as a producer at all.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class LoanEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void publishApplicationSubmitted(UUID loanId, String applicantName, BigDecimal amount, int times) {
        UUID eventId = UUID.randomUUID();
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("eventId", eventId.toString());
        payload.put("loanId", loanId.toString());
        payload.put("applicantName", applicantName);
        payload.put("amount", amount);
        payload.put("submittedAt", LocalDateTime.now().toString());
        publish("finflow.loan.application.submitted", loanId.toString(), payload, times);
    }

    public void publishKycDocumentVerified(UUID loanId, long expectedVersion, int times) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("loanId", loanId.toString());
        payload.put("expectedVersion", expectedVersion);
        payload.put("verifiedAt", LocalDateTime.now().toString());
        publish("finflow.kyc.document.verified", loanId.toString(), payload, times);
    }

    public void publishCreditScoreReceived(UUID loanId, int creditScore, int times) {
        UUID eventId = UUID.randomUUID();
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("eventId", eventId.toString());
        payload.put("loanId", loanId.toString());
        payload.put("creditScore", creditScore);
        payload.put("receivedAt", LocalDateTime.now().toString());
        publish("finflow.credit.score.received", loanId.toString(), payload, times);
    }

    public void publishLoanDecisionMade(UUID loanId, String decision, int times) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("loanId", loanId.toString());
        payload.put("decision", decision);
        payload.put("decidedAt", LocalDateTime.now().toString());
        publish("finflow.loan.decision.made", loanId.toString(), payload, times);
    }

    private void publish(String topic, String key, Map<String, Object> payload, int times) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            for (int i = 0; i < Math.max(1, times); i++) {
                kafkaTemplate.send(topic, key, json);
            }
            log.info("Published to topic={} key={} times={}", topic, key, Math.max(1, times));
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish event to topic " + topic, e);
        }
    }
}

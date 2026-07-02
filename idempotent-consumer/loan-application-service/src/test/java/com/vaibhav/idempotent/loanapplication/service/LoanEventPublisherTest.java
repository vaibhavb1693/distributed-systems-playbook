package com.vaibhav.idempotent.loanapplication.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LoanEventPublisherTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void shouldPublishOnceByDefault() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture((SendResult<String, String>) null));
        LoanEventPublisher publisher = new LoanEventPublisher(kafkaTemplate, objectMapper);

        publisher.publishApplicationSubmitted(UUID.randomUUID(), "Vaibhav Bhatt", BigDecimal.valueOf(500_000), 1);

        verify(kafkaTemplate, times(1)).send(
                org.mockito.ArgumentMatchers.eq("finflow.loan.application.submitted"), anyString(), anyString());
    }

    @Test
    void shouldRepublishIdenticalPayloadWhenTimesGreaterThanOne() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture((SendResult<String, String>) null));
        LoanEventPublisher publisher = new LoanEventPublisher(kafkaTemplate, objectMapper);

        publisher.publishApplicationSubmitted(UUID.randomUUID(), "Vaibhav Bhatt", BigDecimal.valueOf(500_000), 3);

        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(3)).send(anyString(), anyString(), payloadCaptor.capture());

        List<String> payloads = payloadCaptor.getAllValues();
        // Same eventId across all 3 sends — that's what lets the consumer's dedup strategy
        // recognize these as redeliveries of one logical event, not 3 distinct events.
        assertThat(payloads).hasSize(3);
        assertThat(payloads.get(0)).isEqualTo(payloads.get(1)).isEqualTo(payloads.get(2));
    }
}

package com.vaibhav.dlq.kycservice.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.dlq.kycservice.dto.KycDocumentUploadedEvent;
import com.vaibhav.dlq.kycservice.service.KycPipelineService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KycDocumentUploadedConsumerTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private final KycPipelineService pipeline = new KycPipelineService();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private SimpleMeterRegistry meterRegistry;
    private List<Long> sleptFor;
    private KycDocumentUploadedConsumer consumer;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        sleptFor = new ArrayList<>();
        LongConsumer noOpSleeper = sleptFor::add; // records the backoff instead of actually waiting
        consumer = new KycDocumentUploadedConsumer(pipeline, objectMapper, kafkaTemplate, meterRegistry, noOpSleeper);
    }

    @Test
    void shouldRecordSuccessAndNeverPublishToDlq() {
        consumer.processWithRetries(event("NONE"));

        verify(kafkaTemplate, never()).send(org.mockito.ArgumentMatchers.any(ProducerRecord.class));
        assertThat(sleptFor).isEmpty();
        assertThat(count("success")).isEqualTo(1.0);
    }

    @Test
    void shouldRetryThreeTimesThenPublishTransientFailureToDlq() {
        consumer.processWithRetries(event("VENDOR_TIMEOUT"));

        assertThat(sleptFor).containsExactly(1000L, 2000L, 4000L);
        assertThat(count("transient_failure")).isEqualTo(1.0);

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> dlqRecord = captor.getValue();

        assertThat(dlqRecord.topic()).isEqualTo("finflow.kyc.document.dlq");
        assertThat(header(dlqRecord, "X-Failure-Type")).isEqualTo("TRANSIENT");
        assertThat(header(dlqRecord, "X-Failure-Reason")).isEqualTo("VENDOR_TIMEOUT");
        assertThat(header(dlqRecord, "X-Retry-Count")).isEqualTo("3");
        assertThat(header(dlqRecord, "X-Original-Topic")).isEqualTo("finflow.kyc.document.uploaded");
        assertThat(header(dlqRecord, "X-Document-Id")).isEqualTo("doc-1");
    }

    @Test
    void shouldPublishPermanentFailureToDlqImmediatelyWithoutRetrying() {
        consumer.processWithRetries(event("CORRUPTED"));

        assertThat(sleptFor).isEmpty();
        assertThat(count("permanent_failure")).isEqualTo(1.0);

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> dlqRecord = captor.getValue();

        assertThat(header(dlqRecord, "X-Failure-Type")).isEqualTo("PERMANENT");
        assertThat(header(dlqRecord, "X-Failure-Reason")).isEqualTo("CORRUPTED_DOCUMENT");
        assertThat(header(dlqRecord, "X-Retry-Count")).isEqualTo("0");
    }

    private double count(String outcome) {
        var counter = meterRegistry.find("kyc.pipeline.processed").tag("outcome", outcome).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private String header(ProducerRecord<String, String> record, String key) {
        var header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private KycDocumentUploadedEvent event(String simulate) {
        return new KycDocumentUploadedEvent("doc-1", "user-1", "PASSPORT", simulate, "2026-07-02T10:00:00");
    }
}

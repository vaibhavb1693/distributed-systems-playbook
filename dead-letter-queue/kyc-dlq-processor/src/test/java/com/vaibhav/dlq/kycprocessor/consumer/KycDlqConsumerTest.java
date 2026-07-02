package com.vaibhav.dlq.kycprocessor.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.dlq.kycprocessor.config.DlqProperties;
import com.vaibhav.dlq.kycprocessor.domain.KycDlqPendingReplay;
import com.vaibhav.dlq.kycprocessor.domain.KycManualReview;
import com.vaibhav.dlq.kycprocessor.repository.KycDlqPendingReplayRepository;
import com.vaibhav.dlq.kycprocessor.repository.KycManualReviewRepository;
import com.vaibhav.dlq.kycprocessor.service.ManualReviewAlertService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KycDlqConsumerTest {

    @Mock
    private KycManualReviewRepository manualReviewRepository;

    @Mock
    private KycDlqPendingReplayRepository pendingReplayRepository;

    @Mock
    private ManualReviewAlertService alertService;

    private SimpleMeterRegistry meterRegistry;
    private KycDlqConsumer consumer;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        DlqProperties properties = new DlqProperties();
        properties.setReplayCooldownMinutes(5);
        consumer = new KycDlqConsumer(manualReviewRepository, pendingReplayRepository, alertService,
                meterRegistry, properties, new ObjectMapper());
    }

    @Test
    void shouldScheduleAutoReplayForTransientFailure() {
        ConsumerRecord<String, String> record = buildRecord("TRANSIENT", "VENDOR_TIMEOUT", "doc-1",
                "{\"documentId\":\"doc-1\",\"userId\":\"user-1\"}");

        consumer.onDlqMessage(record);

        ArgumentCaptor<KycDlqPendingReplay> captor = ArgumentCaptor.forClass(KycDlqPendingReplay.class);
        verify(pendingReplayRepository).save(captor.capture());
        assertThat(captor.getValue().getDocumentId()).isEqualTo("doc-1");
        assertThat(captor.getValue().getStatus()).isEqualTo("PENDING");
        assertThat(captor.getValue().getReplayAt()).isAfter(LocalDateTime.now().plusMinutes(4));

        verifyNoInteractions(manualReviewRepository, alertService);
        assertThat(receivedCount("TRANSIENT")).isEqualTo(1.0);
    }

    @Test
    void shouldRouteToManualReviewForPermanentFailureAndCheckAlert() {
        ConsumerRecord<String, String> record = buildRecord("PERMANENT", "CORRUPTED_DOCUMENT", "doc-2",
                "{\"documentId\":\"doc-2\",\"userId\":\"user-2\"}");

        consumer.onDlqMessage(record);

        ArgumentCaptor<KycManualReview> captor = ArgumentCaptor.forClass(KycManualReview.class);
        verify(manualReviewRepository).save(captor.capture());
        assertThat(captor.getValue().getDocumentId()).isEqualTo("doc-2");
        assertThat(captor.getValue().getUserId()).isEqualTo("user-2");
        assertThat(captor.getValue().getFailureReason()).isEqualTo("CORRUPTED_DOCUMENT");
        assertThat(captor.getValue().getStatus()).isEqualTo("PENDING_REVIEW");

        verify(alertService).checkAndAlertIfThresholdExceeded();
        verifyNoInteractions(pendingReplayRepository);
        assertThat(receivedCount("PERMANENT")).isEqualTo(1.0);
    }

    private double receivedCount(String failureType) {
        var counter = meterRegistry.find("kyc.dlq.messages.received").tag("failure_type", failureType).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private ConsumerRecord<String, String> buildRecord(String failureType, String failureReason, String documentId, String payload) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("finflow.kyc.document.dlq", 0, 0L, documentId, payload);
        record.headers().add(new RecordHeader("X-Failure-Type", failureType.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("X-Failure-Reason", failureReason.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("X-Document-Id", documentId.getBytes(StandardCharsets.UTF_8)));
        return record;
    }
}

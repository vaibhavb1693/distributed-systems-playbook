package com.vaibhav.dlq.kycprocessor.consumer;

import com.vaibhav.dlq.kycprocessor.config.DlqProperties;
import com.vaibhav.dlq.kycprocessor.domain.KycDlqPendingReplay;
import com.vaibhav.dlq.kycprocessor.repository.KycDlqPendingReplayRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KycDlqReplayPollerTest {

    @Mock
    private KycDlqPendingReplayRepository pendingReplayRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private SimpleMeterRegistry meterRegistry;
    private KycDlqReplayPoller poller;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        DlqProperties properties = new DlqProperties();
        properties.setReplayBatchSize(20);
        poller = new KycDlqReplayPoller(pendingReplayRepository, kafkaTemplate, meterRegistry, properties);
    }

    @Test
    void shouldDoNothingWhenNoneDue() {
        when(pendingReplayRepository.findByStatusAndReplayAtLessThanEqualOrderByReplayAtAsc(
                eq("PENDING"), any(LocalDateTime.class), any(Pageable.class))).thenReturn(List.of());

        poller.pollAndReplay();

        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void shouldMarkReplayedOnSuccessfulSend() {
        KycDlqPendingReplay item = KycDlqPendingReplay.builder()
                .id(UUID.randomUUID()).documentId("doc-1").payload("{}").replayAt(LocalDateTime.now()).status("PENDING")
                .build();
        when(pendingReplayRepository.findByStatusAndReplayAtLessThanEqualOrderByReplayAtAsc(
                eq("PENDING"), any(LocalDateTime.class), any(Pageable.class))).thenReturn(List.of(item));
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture((SendResult<String, String>) null));

        poller.pollAndReplay();

        ArgumentCaptor<KycDlqPendingReplay> captor = ArgumentCaptor.forClass(KycDlqPendingReplay.class);
        verify(pendingReplayRepository).save(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo("REPLAYED");
        assertThat(meterRegistry.find("kyc.dlq.replay.success").counter().count()).isEqualTo(1.0);
    }

    @Test
    void shouldMarkFailedWhenSendThrows() {
        KycDlqPendingReplay item = KycDlqPendingReplay.builder()
                .id(UUID.randomUUID()).documentId("doc-2").payload("{}").replayAt(LocalDateTime.now()).status("PENDING")
                .build();
        when(pendingReplayRepository.findByStatusAndReplayAtLessThanEqualOrderByReplayAtAsc(
                eq("PENDING"), any(LocalDateTime.class), any(Pageable.class))).thenReturn(List.of(item));
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Kafka unavailable")));

        poller.pollAndReplay();

        ArgumentCaptor<KycDlqPendingReplay> captor = ArgumentCaptor.forClass(KycDlqPendingReplay.class);
        verify(pendingReplayRepository).save(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo("FAILED");
        assertThat(meterRegistry.find("kyc.dlq.replay.failed").counter().count()).isEqualTo(1.0);
    }
}

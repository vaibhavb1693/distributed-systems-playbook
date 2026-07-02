package com.vaibhav.dlq.kycprocessor.service;

import com.vaibhav.dlq.kycprocessor.domain.KycDlqPendingReplay;
import com.vaibhav.dlq.kycprocessor.domain.KycManualReview;
import com.vaibhav.dlq.kycprocessor.dto.DlqStatsResponse;
import com.vaibhav.dlq.kycprocessor.repository.KycDlqPendingReplayRepository;
import com.vaibhav.dlq.kycprocessor.repository.KycManualReviewRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DlqReplayServiceTest {

    @Mock
    private KycManualReviewRepository manualReviewRepository;

    @Mock
    private KycDlqPendingReplayRepository pendingReplayRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private SimpleMeterRegistry meterRegistry;
    private DlqReplayService service;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        service = new DlqReplayService(manualReviewRepository, pendingReplayRepository, kafkaTemplate, meterRegistry);
    }

    @Test
    void shouldReplayManualReviewDocumentWhenFound() {
        KycManualReview review = KycManualReview.builder().id(UUID.randomUUID()).documentId("doc-1").payload("{}").status("PENDING_REVIEW").build();
        when(manualReviewRepository.findByDocumentId("doc-1")).thenReturn(Optional.of(review));

        boolean result = service.replayDocument("doc-1");

        assertThat(result).isTrue();
        verify(kafkaTemplate).send("finflow.kyc.document.uploaded", "doc-1", "{}");
        assertThat(review.getStatus()).isEqualTo("RESOLVED");
        verify(manualReviewRepository).save(review);
    }

    @Test
    void shouldReplayPendingDocumentWhenNotInManualReview() {
        when(manualReviewRepository.findByDocumentId("doc-2")).thenReturn(Optional.empty());
        KycDlqPendingReplay pending = KycDlqPendingReplay.builder().id(UUID.randomUUID()).documentId("doc-2").payload("{}").status("PENDING").build();
        when(pendingReplayRepository.findByDocumentIdAndStatus("doc-2", "PENDING")).thenReturn(Optional.of(pending));

        boolean result = service.replayDocument("doc-2");

        assertThat(result).isTrue();
        assertThat(pending.getStatus()).isEqualTo("REPLAYED");
        verify(pendingReplayRepository).save(pending);
    }

    @Test
    void shouldReturnFalseWhenDocumentNotFoundAnywhere() {
        when(manualReviewRepository.findByDocumentId("doc-3")).thenReturn(Optional.empty());
        when(pendingReplayRepository.findByDocumentIdAndStatus("doc-3", "PENDING")).thenReturn(Optional.empty());

        boolean result = service.replayDocument("doc-3");

        assertThat(result).isFalse();
        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void shouldBulkReplayTransientBypassingCooldown() {
        KycDlqPendingReplay item = KycDlqPendingReplay.builder().id(UUID.randomUUID()).documentId("doc-4").payload("{}").status("PENDING").build();
        when(pendingReplayRepository.findByStatusAndReplayAtLessThanEqualOrderByReplayAtAsc(
                eq("PENDING"), any(LocalDateTime.class), any(Pageable.class))).thenReturn(List.of(item));

        int count = service.replayBulk("TRANSIENT");

        assertThat(count).isEqualTo(1);
        assertThat(item.getStatus()).isEqualTo("REPLAYED");
    }

    @Test
    void shouldBulkReplayPermanentManualReviews() {
        KycManualReview review = KycManualReview.builder().id(UUID.randomUUID()).documentId("doc-5").payload("{}").status("PENDING_REVIEW").build();
        when(manualReviewRepository.findByStatus(eq("PENDING_REVIEW"), any(Pageable.class)))
                .thenReturn(new PageImpl<>(List.of(review)));

        int count = service.replayBulk("PERMANENT");

        assertThat(count).isEqualTo(1);
        assertThat(review.getStatus()).isEqualTo("RESOLVED");
    }

    @Test
    void shouldResolveManualReview() {
        UUID id = UUID.randomUUID();
        KycManualReview review = KycManualReview.builder().id(id).status("PENDING_REVIEW").build();
        when(manualReviewRepository.findById(id)).thenReturn(Optional.of(review));

        service.resolve(id, "Fixed by vendor", "ops-team");

        assertThat(review.getStatus()).isEqualTo("RESOLVED");
        assertThat(review.getResolutionNote()).isEqualTo("Fixed by vendor");
        assertThat(review.getResolvedBy()).isEqualTo("ops-team");
        verify(manualReviewRepository).save(review);
    }

    @Test
    void shouldThrowWhenResolvingUnknownId() {
        UUID id = UUID.randomUUID();
        when(manualReviewRepository.findById(id)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.resolve(id, "note", "someone"))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void shouldComputeStats() {
        when(pendingReplayRepository.count()).thenReturn(10L);
        when(manualReviewRepository.count()).thenReturn(5L);
        when(pendingReplayRepository.countByStatus("REPLAYED")).thenReturn(8L);
        when(pendingReplayRepository.countByStatus("FAILED")).thenReturn(2L);
        when(manualReviewRepository.countByStatus("PENDING_REVIEW")).thenReturn(3L);

        DlqStatsResponse stats = service.stats();

        assertThat(stats.totalReceived()).isEqualTo(15L);
        assertThat(stats.transientCount()).isEqualTo(10L);
        assertThat(stats.permanentCount()).isEqualTo(5L);
        assertThat(stats.replaySuccessRate()).isEqualTo(0.8);
        assertThat(stats.pendingManualReviewCount()).isEqualTo(3L);
    }
}

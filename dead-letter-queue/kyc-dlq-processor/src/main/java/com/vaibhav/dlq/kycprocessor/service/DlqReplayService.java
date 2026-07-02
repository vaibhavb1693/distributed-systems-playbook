package com.vaibhav.dlq.kycprocessor.service;

import com.vaibhav.dlq.kycprocessor.domain.KycDlqPendingReplay;
import com.vaibhav.dlq.kycprocessor.domain.KycManualReview;
import com.vaibhav.dlq.kycprocessor.dto.DlqStatsResponse;
import com.vaibhav.dlq.kycprocessor.repository.KycDlqPendingReplayRepository;
import com.vaibhav.dlq.kycprocessor.repository.KycManualReviewRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class DlqReplayService {

    private static final String ORIGINAL_TOPIC = "finflow.kyc.document.uploaded";

    private final KycManualReviewRepository manualReviewRepository;
    private final KycDlqPendingReplayRepository pendingReplayRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MeterRegistry meterRegistry;

    /** Checks manual review first, then pending-transient-replay. Returns false if neither has this documentId. */
    public boolean replayDocument(String documentId) {
        Optional<KycManualReview> review = manualReviewRepository.findByDocumentId(documentId);
        if (review.isPresent()) {
            replayManualReview(review.get(), "Manually replayed via API");
            return true;
        }

        Optional<KycDlqPendingReplay> pending = pendingReplayRepository.findByDocumentIdAndStatus(documentId, "PENDING");
        if (pending.isPresent()) {
            replayPending(pending.get());
            return true;
        }

        return false;
    }

    /** Bulk replay bypasses the cooldown timer for TRANSIENT items — an explicit ops action, not the automatic poller. */
    public int replayBulk(String failureType) {
        if ("PERMANENT".equalsIgnoreCase(failureType)) {
            List<KycManualReview> reviews = manualReviewRepository.findByStatus("PENDING_REVIEW", Pageable.unpaged()).getContent();
            reviews.forEach(r -> replayManualReview(r, "Bulk replayed via API"));
            return reviews.size();
        }

        List<KycDlqPendingReplay> pendingItems = pendingReplayRepository.findByStatusAndReplayAtLessThanEqualOrderByReplayAtAsc(
                "PENDING", LocalDateTime.now().plusYears(100), Pageable.unpaged());
        pendingItems.forEach(this::replayPending);
        return pendingItems.size();
    }

    public void resolve(UUID id, String resolutionNote, String resolvedBy) {
        KycManualReview review = manualReviewRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("No manual review record found: " + id));
        review.setStatus("RESOLVED");
        review.setResolutionNote(resolutionNote);
        review.setResolvedBy(resolvedBy);
        review.setResolvedAt(LocalDateTime.now());
        manualReviewRepository.save(review);
    }

    public DlqStatsResponse stats() {
        long transientCount = pendingReplayRepository.count();
        long permanentCount = manualReviewRepository.count();
        long replayed = pendingReplayRepository.countByStatus("REPLAYED");
        long failed = pendingReplayRepository.countByStatus("FAILED");
        double successRate = (replayed + failed) == 0 ? 0.0 : (double) replayed / (replayed + failed);
        long pendingManualReview = manualReviewRepository.countByStatus("PENDING_REVIEW");

        return new DlqStatsResponse(transientCount + permanentCount, transientCount, permanentCount, successRate, pendingManualReview);
    }

    private void replayManualReview(KycManualReview review, String resolutionNote) {
        kafkaTemplate.send(ORIGINAL_TOPIC, review.getDocumentId(), review.getPayload());
        review.setStatus("RESOLVED");
        review.setResolutionNote(resolutionNote);
        review.setResolvedAt(LocalDateTime.now());
        manualReviewRepository.save(review);
        recordReplaySuccess();
        log.info("Replayed manual-review document via API: documentId={}", review.getDocumentId());
    }

    private void replayPending(KycDlqPendingReplay item) {
        kafkaTemplate.send(ORIGINAL_TOPIC, item.getDocumentId(), item.getPayload());
        item.setStatus("REPLAYED");
        pendingReplayRepository.save(item);
        recordReplaySuccess();
        log.info("Replayed pending-transient document via API: documentId={}", item.getDocumentId());
    }

    private void recordReplaySuccess() {
        meterRegistry.counter("kyc.dlq.replay.attempted").increment();
        meterRegistry.counter("kyc.dlq.replay.success").increment();
    }
}

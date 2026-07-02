package com.vaibhav.dlq.kycprocessor.controller;

import com.vaibhav.dlq.kycprocessor.dto.BulkReplayResponse;
import com.vaibhav.dlq.kycprocessor.dto.DlqStatsResponse;
import com.vaibhav.dlq.kycprocessor.dto.ManualReviewResponse;
import com.vaibhav.dlq.kycprocessor.dto.ResolveRequest;
import com.vaibhav.dlq.kycprocessor.repository.KycManualReviewRepository;
import com.vaibhav.dlq.kycprocessor.service.DlqReplayService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.NoSuchElementException;
import java.util.UUID;

@RestController
@RequestMapping("/dlq/kyc")
@RequiredArgsConstructor
public class KycDlqController {

    private final DlqReplayService dlqReplayService;
    private final KycManualReviewRepository manualReviewRepository;

    @PostMapping("/replay/{documentId}")
    public ResponseEntity<Void> replayOne(@PathVariable String documentId) {
        boolean replayed = dlqReplayService.replayDocument(documentId);
        return replayed ? ResponseEntity.ok().build() : ResponseEntity.notFound().build();
    }

    @PostMapping("/replay/bulk")
    public ResponseEntity<BulkReplayResponse> replayBulk(
            @RequestParam(defaultValue = "TRANSIENT") String failureType) {
        int count = dlqReplayService.replayBulk(failureType);
        return ResponseEntity.ok(new BulkReplayResponse(count));
    }

    @GetMapping("/messages")
    public ResponseEntity<Page<ManualReviewResponse>> listMessages(
            @RequestParam(defaultValue = "PENDING_REVIEW") String status,
            @RequestParam(required = false) String failureReason,
            Pageable pageable) {
        Page<ManualReviewResponse> page = (failureReason != null)
                ? manualReviewRepository.findByStatusAndFailureReason(status, failureReason, pageable).map(ManualReviewResponse::from)
                : manualReviewRepository.findByStatus(status, pageable).map(ManualReviewResponse::from);
        return ResponseEntity.ok(page);
    }

    @PatchMapping("/messages/{id}/resolve")
    public ResponseEntity<Void> resolve(@PathVariable UUID id, @Valid @RequestBody ResolveRequest request) {
        try {
            dlqReplayService.resolve(id, request.resolutionNote(), request.resolvedBy());
            return ResponseEntity.ok().build();
        } catch (NoSuchElementException e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/stats")
    public ResponseEntity<DlqStatsResponse> stats() {
        return ResponseEntity.ok(dlqReplayService.stats());
    }
}

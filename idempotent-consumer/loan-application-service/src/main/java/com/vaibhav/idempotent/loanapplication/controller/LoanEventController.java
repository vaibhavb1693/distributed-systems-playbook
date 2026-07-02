package com.vaibhav.idempotent.loanapplication.controller;

import com.vaibhav.idempotent.loanapplication.dto.CreditScoreRequest;
import com.vaibhav.idempotent.loanapplication.dto.DecisionRequest;
import com.vaibhav.idempotent.loanapplication.dto.SubmitApplicationRequest;
import com.vaibhav.idempotent.loanapplication.service.LoanEventPublisher;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

/**
 * `times` (default 1) on every endpoint deliberately republishes the identical event —
 * same eventId for the Redis/DB-dedup strategies, same expectedVersion for the
 * optimistic-lock strategy — so a caller (curl, Gatling later) can demo redelivery without
 * needing any Kafka-level replay tooling.
 */
@RestController
@RequestMapping("/api/loans")
@RequiredArgsConstructor
public class LoanEventController {

    private final LoanEventPublisher loanEventPublisher;

    @PostMapping("/{loanId}/submit-application")
    public ResponseEntity<Void> submitApplication(
            @PathVariable UUID loanId,
            @Valid @RequestBody SubmitApplicationRequest request,
            @RequestParam(defaultValue = "1") int times) {
        loanEventPublisher.publishApplicationSubmitted(loanId, request.applicantName(), request.amount(), times);
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/{loanId}/kyc-verified")
    public ResponseEntity<Void> kycVerified(
            @PathVariable UUID loanId,
            @RequestParam(defaultValue = "0") long expectedVersion,
            @RequestParam(defaultValue = "1") int times) {
        loanEventPublisher.publishKycDocumentVerified(loanId, expectedVersion, times);
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/{loanId}/credit-score")
    public ResponseEntity<Void> creditScore(
            @PathVariable UUID loanId,
            @Valid @RequestBody CreditScoreRequest request,
            @RequestParam(defaultValue = "1") int times) {
        loanEventPublisher.publishCreditScoreReceived(loanId, request.creditScore(), times);
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/{loanId}/decision")
    public ResponseEntity<Void> decision(
            @PathVariable UUID loanId,
            @Valid @RequestBody DecisionRequest request,
            @RequestParam(defaultValue = "1") int times) {
        loanEventPublisher.publishLoanDecisionMade(loanId, request.decision(), times);
        return ResponseEntity.accepted().build();
    }
}

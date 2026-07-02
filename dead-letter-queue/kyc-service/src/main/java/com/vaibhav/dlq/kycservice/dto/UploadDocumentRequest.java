package com.vaibhav.dlq.kycservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * `simulate` is the failure-injection lever for this pattern (mirrors fraud-detection-service's
 * /admin/degrade in circuit-breaker, scoped per-document instead of globally since KYC
 * failures are naturally a property of one document, not the whole service being down).
 * NONE means the pipeline completes successfully.
 */
public record UploadDocumentRequest(
        @NotBlank String userId,
        @NotBlank String documentType,
        @Pattern(regexp = "NONE|VENDOR_TIMEOUT|DB_ERROR|CORRUPTED|UNSUPPORTED_FORMAT|FRAUD_FLAG|OCR_FAILURE")
        String simulate
) {}

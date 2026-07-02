package com.vaibhav.dlq.kycservice.service;

import com.vaibhav.dlq.kycservice.dto.KycDocumentUploadedEvent;
import com.vaibhav.dlq.kycservice.exception.PermanentFailureException;
import com.vaibhav.dlq.kycservice.exception.TransientFailureException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Simulates the pipeline described in the pattern: validate format -> OCR extraction ->
 * vendor verification -> store result -> update KYC status. No real OCR/vendor/DB calls —
 * `simulate` on the uploaded event deterministically drives which step "fails" and how, so
 * the demo is reproducible. A real implementation would replace this method's body with
 * the actual pipeline steps; the failure classification/retry/DLQ machinery around it
 * wouldn't change.
 */
@Service
@Slf4j
public class KycPipelineService {

    public void process(KycDocumentUploadedEvent event) {
        switch (event.simulate()) {
            case "VENDOR_TIMEOUT" ->
                    throw new TransientFailureException("VENDOR_TIMEOUT", "Vendor verification API timed out");
            case "DB_ERROR" ->
                    throw new TransientFailureException("DB_CONNECTION_ERROR", "Failed to persist verification result");
            case "CORRUPTED" ->
                    throw new PermanentFailureException("CORRUPTED_DOCUMENT", "Document failed format validation");
            case "UNSUPPORTED_FORMAT" ->
                    throw new PermanentFailureException("UNSUPPORTED_FORMAT", "MIME type not supported");
            case "OCR_FAILURE" ->
                    throw new PermanentFailureException("OCR_EXTRACTION_FAILED", "OCR could not extract required fields");
            case "FRAUD_FLAG" ->
                    throw new PermanentFailureException("VENDOR_FRAUD_FLAG", "Vendor flagged document as fraudulent");
            default -> log.info("KYC pipeline completed: documentId={} userId={}", event.documentId(), event.userId());
        }
    }
}

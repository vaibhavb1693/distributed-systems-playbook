package com.vaibhav.dlq.kycservice.service;

import com.vaibhav.dlq.kycservice.dto.KycDocumentUploadedEvent;
import com.vaibhav.dlq.kycservice.exception.PermanentFailureException;
import com.vaibhav.dlq.kycservice.exception.TransientFailureException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KycPipelineServiceTest {

    private final KycPipelineService pipeline = new KycPipelineService();

    @Test
    void shouldCompleteSuccessfullyWhenSimulateIsNone() {
        assertThatCode(() -> pipeline.process(event("NONE"))).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"VENDOR_TIMEOUT", "DB_ERROR"})
    void shouldClassifyAsTransient(String simulate) {
        assertThatThrownBy(() -> pipeline.process(event(simulate)))
                .isInstanceOf(TransientFailureException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"CORRUPTED", "UNSUPPORTED_FORMAT", "OCR_FAILURE", "FRAUD_FLAG"})
    void shouldClassifyAsPermanent(String simulate) {
        assertThatThrownBy(() -> pipeline.process(event(simulate)))
                .isInstanceOf(PermanentFailureException.class);
    }

    private KycDocumentUploadedEvent event(String simulate) {
        return new KycDocumentUploadedEvent("doc-1", "user-1", "PASSPORT", simulate, "2026-07-02T10:00:00");
    }
}

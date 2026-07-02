package com.vaibhav.dlq.kycprocessor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.dlq.kycprocessor.config.DlqProperties;
import com.vaibhav.dlq.kycprocessor.repository.KycManualReviewRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ManualReviewAlertServiceTest {

    @Mock
    private KycManualReviewRepository manualReviewRepository;

    private ManualReviewAlertService alertService;

    @BeforeEach
    void setUp() {
        DlqProperties properties = new DlqProperties();
        properties.setManualReviewAlertThreshold(10);
        alertService = new ManualReviewAlertService(manualReviewRepository, new ObjectMapper(), properties);
    }

    @Test
    void shouldNotThrowWhenBelowThreshold() {
        when(manualReviewRepository.countByStatus("PENDING_REVIEW")).thenReturn(3L);

        assertThatCode(() -> alertService.checkAndAlertIfThresholdExceeded()).doesNotThrowAnyException();
    }

    @Test
    void shouldNotThrowWhenAboveThreshold() {
        when(manualReviewRepository.countByStatus("PENDING_REVIEW")).thenReturn(11L);

        assertThatCode(() -> alertService.checkAndAlertIfThresholdExceeded()).doesNotThrowAnyException();
    }
}

package com.vaibhav.dlq.kycprocessor.metrics;

import com.vaibhav.dlq.kycprocessor.repository.KycManualReviewRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class DlqMetrics {

    private final KycManualReviewRepository manualReviewRepository;
    private final MeterRegistry meterRegistry;

    @PostConstruct
    void registerGauge() {
        Gauge.builder("kyc.dlq.manual_review.pending", manualReviewRepository,
                        repo -> repo.countByStatus("PENDING_REVIEW"))
                .description("Documents awaiting manual review — alert fires above the configured threshold")
                .register(meterRegistry);
    }
}

package com.vaibhav.dlq.kycprocessor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.dlq.kycprocessor.config.DlqProperties;
import com.vaibhav.dlq.kycprocessor.repository.KycManualReviewRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simulates a PagerDuty/Slack webhook by logging a structured JSON alert payload — real
 * integration would replace the log.error call with an actual HTTP POST to the webhook URL.
 * Checked synchronously right after each new manual-review row is inserted (threshold-
 * crossing, not a scheduled poll) so the alert fires as soon as the backlog actually crosses
 * the line, not up to a poll-interval late.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ManualReviewAlertService {

    private final KycManualReviewRepository manualReviewRepository;
    private final ObjectMapper objectMapper;
    private final DlqProperties dlqProperties;

    public void checkAndAlertIfThresholdExceeded() {
        long pendingCount = manualReviewRepository.countByStatus("PENDING_REVIEW");
        if (pendingCount <= dlqProperties.getManualReviewAlertThreshold()) {
            return;
        }

        Map<String, Object> alert = new LinkedHashMap<>();
        alert.put("alert", "KYC_MANUAL_REVIEW_BACKLOG");
        alert.put("severity", "HIGH");
        alert.put("pendingCount", pendingCount);
        alert.put("threshold", dlqProperties.getManualReviewAlertThreshold());
        alert.put("message", "KYC manual review backlog exceeds threshold — a PagerDuty/Slack webhook would fire here");
        alert.put("timestamp", Instant.now().toString());

        try {
            log.error(objectMapper.writeValueAsString(alert));
        } catch (Exception e) {
            log.error("Failed to serialize manual review backlog alert", e);
        }
    }
}

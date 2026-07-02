package com.vaibhav.dlq.kycprocessor.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "kyc.dlq")
@Getter
@Setter
public class DlqProperties {
    private int replayCooldownMinutes = 5;
    private long replayPollIntervalMs = 30_000;
    private int replayBatchSize = 20;
    private int manualReviewAlertThreshold = 10;
}

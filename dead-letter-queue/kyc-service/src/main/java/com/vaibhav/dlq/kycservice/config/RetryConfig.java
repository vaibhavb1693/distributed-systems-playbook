package com.vaibhav.dlq.kycservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.LongConsumer;

@Configuration
public class RetryConfig {

    /**
     * Extracted as an injectable bean (not a raw Thread.sleep call in the consumer) so
     * unit tests can swap in a no-op and cover the 3-attempt backoff sequence without
     * actually waiting 1s+2s+4s per test.
     */
    @Bean
    public LongConsumer backoffSleeper() {
        return millis -> {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted during retry backoff", e);
            }
        };
    }
}

package com.vaibhav.circuitbreaker.transaction.config;

import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

@Configuration
public class RestTemplateConfig {

    // Read timeout is deliberately well above the fraud service's default degrade
    // latency (5s) so a "slow" degrade surfaces to Resilience4j's slow-call detection
    // (slowCallDurationThreshold=2s) rather than being cut short as a read-timeout
    // exception first.
    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
        return builder
                .setConnectTimeout(Duration.ofSeconds(2))
                .setReadTimeout(Duration.ofSeconds(10))
                .build();
    }
}

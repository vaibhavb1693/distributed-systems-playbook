package com.vaibhav.outbox.userservice.metrics;

import com.vaibhav.outbox.userservice.domain.OutboxStatus;
import com.vaibhav.outbox.userservice.repository.OutboxEventRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Exposes the size of the PENDING outbox backlog as a gauge.
 *
 * Gated by the same condition as OutboxPoller: under the CDC profile, Debezium is the
 * only writer/reader of outbox status, so this bean (and the metric) simply doesn't
 * exist there — a naive ungated gauge would otherwise show a misleading ever-growing
 * backlog under CDC, since nothing ever transitions those rows out of PENDING.
 */
@Component
@ConditionalOnProperty(name = "outbox.poller.enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class OutboxBacklogMetrics {

    private final OutboxEventRepository outboxEventRepository;
    private final MeterRegistry meterRegistry;

    @PostConstruct
    void registerGauge() {
        Gauge.builder("outbox.backlog.size", outboxEventRepository,
                        repo -> repo.countByStatus(OutboxStatus.PENDING))
                .description("Number of PENDING outbox events awaiting publish (polling mode only)")
                .register(meterRegistry);
    }
}

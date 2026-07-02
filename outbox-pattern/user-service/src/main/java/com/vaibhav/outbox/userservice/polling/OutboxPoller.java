package com.vaibhav.outbox.userservice.polling;

import com.vaibhav.outbox.userservice.domain.OutboxEvent;
import com.vaibhav.outbox.userservice.service.OutboxEventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Approach A: Polling-based Outbox Publisher.
 *
 * Disabled when outbox.poller.enabled=false (i.e., when running in CDC mode
 * where Debezium watches the WAL and publishes events directly — no polling needed).
 *
 * Flow per poll cycle:
 *  1. Claim a batch of PENDING events (marks them PROCESSING in one transaction).
 *  2. Publish each to Kafka synchronously with a 5s timeout.
 *  3. Mark PUBLISHED on success, reset to PENDING on failure (retried next cycle).
 *
 * The two-phase claim (PENDING → PROCESSING → PUBLISHED) is safe for clustered deployments:
 * FOR UPDATE SKIP LOCKED in the claim query prevents concurrent pollers from double-publishing.
 * Events stuck in PROCESSING (crash between step 1 and 3) are auto-reset after 5 minutes.
 */
@Component
@ConditionalOnProperty(name = "outbox.poller.enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
@Slf4j
public class OutboxPoller {

    private final OutboxEventService outboxEventService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${outbox.poller.batch-size:10}")
    private int batchSize;

    @Value("${outbox.poller.publish-timeout-seconds:5}")
    private int publishTimeoutSeconds;

    @Value("${outbox.poller.max-retry-count:5}")
    private int maxRetryCount;

    @Scheduled(fixedDelayString = "${outbox.poller.interval-ms:500}")
    public void pollAndPublish() {
        List<OutboxEvent> events = outboxEventService.claimPendingEvents(batchSize);

        if (events.isEmpty()) {
            return;
        }

        log.debug("Claimed {} outbox events for publishing", events.size());

        for (OutboxEvent event : events) {
            publishEvent(event);
        }
    }

    private void publishEvent(OutboxEvent event) {
        if (event.getRetryCount() >= maxRetryCount) {
            log.error("Event {} exceeded max retry count ({}), marking FAILED", event.getId(), maxRetryCount);
            outboxEventService.markFailed(event.getId());
            return;
        }

        try {
            kafkaTemplate
                    .send(event.getTopicName(), event.getAggregateId(), event.getPayload())
                    .get(publishTimeoutSeconds, TimeUnit.SECONDS);

            outboxEventService.markPublished(event.getId());
            log.info("Published event id={} type={} to topic={}",
                    event.getId(), event.getEventType(), event.getTopicName());

        } catch (Exception e) {
            log.error("Failed to publish event id={} type={}, will retry. Error: {}",
                    event.getId(), event.getEventType(), e.getMessage());
            outboxEventService.markPendingForRetry(event.getId());
        }
    }
}

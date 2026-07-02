package com.vaibhav.outbox.userservice.polling;

import com.vaibhav.outbox.userservice.domain.EventType;
import com.vaibhav.outbox.userservice.domain.OutboxEvent;
import com.vaibhav.outbox.userservice.domain.OutboxStatus;
import com.vaibhav.outbox.userservice.service.OutboxEventService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxPollerTest {

    @Mock
    private OutboxEventService outboxEventService;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @InjectMocks
    private OutboxPoller outboxPoller;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(outboxPoller, "batchSize", 10);
        ReflectionTestUtils.setField(outboxPoller, "publishTimeoutSeconds", 5);
        ReflectionTestUtils.setField(outboxPoller, "maxRetryCount", 5);
    }

    @Test
    void shouldPublishPendingEventAndMarkAsPublished() {
        UUID eventId = UUID.randomUUID();
        String aggregateId = UUID.randomUUID().toString();
        OutboxEvent event = buildEvent(eventId, aggregateId, EventType.USER_REGISTERED, 0);

        when(outboxEventService.claimPendingEvents(10)).thenReturn(List.of(event));
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        outboxPoller.pollAndPublish();

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(topicCaptor.capture(), keyCaptor.capture(), eq(event.getPayload()));

        assertThat(topicCaptor.getValue()).isEqualTo("finflow.user.registered");
        assertThat(keyCaptor.getValue()).isEqualTo(aggregateId);
        verify(outboxEventService).markPublished(eventId);
    }

    @Test
    void shouldResetToPendingWhenKafkaPublishFails() {
        UUID eventId = UUID.randomUUID();
        OutboxEvent event = buildEvent(eventId, UUID.randomUUID().toString(), EventType.USER_REGISTERED, 0);

        when(outboxEventService.claimPendingEvents(10)).thenReturn(List.of(event));
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Kafka unavailable")));

        outboxPoller.pollAndPublish();

        verify(outboxEventService).markPendingForRetry(eventId);
        verify(outboxEventService, never()).markPublished(any());
    }

    @Test
    void shouldMarkFailedWhenMaxRetryCountExceeded() {
        UUID eventId = UUID.randomUUID();
        OutboxEvent event = buildEvent(eventId, UUID.randomUUID().toString(), EventType.KYC_STATUS_UPDATED, 5);

        when(outboxEventService.claimPendingEvents(10)).thenReturn(List.of(event));

        outboxPoller.pollAndPublish();

        verify(outboxEventService).markFailed(eventId);
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void shouldDoNothingWhenNoPendingEvents() {
        when(outboxEventService.claimPendingEvents(10)).thenReturn(List.of());

        outboxPoller.pollAndPublish();

        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void shouldPublishAllEventsInBatch() {
        List<OutboxEvent> events = List.of(
                buildEvent(UUID.randomUUID(), UUID.randomUUID().toString(), EventType.USER_REGISTERED, 0),
                buildEvent(UUID.randomUUID(), UUID.randomUUID().toString(), EventType.PROFILE_UPDATED, 0),
                buildEvent(UUID.randomUUID(), UUID.randomUUID().toString(), EventType.KYC_STATUS_UPDATED, 0)
        );

        when(outboxEventService.claimPendingEvents(10)).thenReturn(events);
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        outboxPoller.pollAndPublish();

        verify(kafkaTemplate, times(3)).send(anyString(), anyString(), anyString());
        verify(outboxEventService, times(3)).markPublished(any());
    }

    private OutboxEvent buildEvent(UUID id, String aggregateId, EventType type, int retryCount) {
        OutboxEvent event = new OutboxEvent();
        event.setId(id);
        event.setAggregateType("USER");
        event.setAggregateId(aggregateId);
        event.setEventType(type);
        event.setTopicName(type.getTopic());
        event.setPayload("{\"userId\":\"" + aggregateId + "\"}");
        event.setStatus(OutboxStatus.PROCESSING);
        event.setRetryCount(retryCount);
        return event;
    }
}

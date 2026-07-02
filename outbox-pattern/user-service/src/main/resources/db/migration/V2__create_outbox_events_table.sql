CREATE TABLE outbox_events (
    id              UUID PRIMARY KEY    DEFAULT gen_random_uuid(),
    aggregate_type  VARCHAR(100)        NOT NULL,
    aggregate_id    VARCHAR(255)        NOT NULL,
    event_type      VARCHAR(100)        NOT NULL,
    -- topic_name is the Kafka topic to publish to.
    -- Approach A (polling): OutboxPoller reads this field to route the message.
    -- Approach B (CDC):     Debezium outbox event router uses this field for topic routing.
    topic_name      VARCHAR(255)        NOT NULL,
    payload         TEXT                NOT NULL,
    status          VARCHAR(20)         NOT NULL DEFAULT 'PENDING',
    retry_count     INT                 NOT NULL DEFAULT 0,
    created_at      TIMESTAMP           NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP           NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMP
);

-- Critical for polling performance: poller queries WHERE status = 'PENDING' ORDER BY created_at.
CREATE INDEX idx_outbox_events_status_created ON outbox_events (status, created_at);

-- Supports aggregate-level event history queries.
CREATE INDEX idx_outbox_events_aggregate ON outbox_events (aggregate_type, aggregate_id);

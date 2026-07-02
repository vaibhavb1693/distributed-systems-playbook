-- TRANSIENT failures land here for auto-replay after a cooldown. Durable (Postgres-backed)
-- rather than an in-memory scheduled task, so a pending replay survives a service restart —
-- the same rationale as outbox-pattern's OutboxPoller, applied to DLQ replay.
CREATE TABLE kyc_dlq_pending_replay (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id   VARCHAR(255) NOT NULL,
    payload       TEXT NOT NULL,
    replay_at     TIMESTAMP NOT NULL,
    status        VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_kyc_dlq_pending_replay_status_replay_at ON kyc_dlq_pending_replay (status, replay_at);
CREATE INDEX idx_kyc_dlq_pending_replay_document_id ON kyc_dlq_pending_replay (document_id);

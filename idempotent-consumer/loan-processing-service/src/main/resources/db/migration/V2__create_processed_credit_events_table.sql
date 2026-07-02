-- Strategy 2: DB unique constraint dedup. event_id as PK means a duplicate delivery's
-- INSERT raises a DataIntegrityViolationException that the consumer catches and skips —
-- and it doubles as a permanent audit trail of every credit score event processed.
CREATE TABLE processed_credit_events (
    event_id     UUID PRIMARY KEY,
    loan_id      UUID NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_processed_credit_events_loan_id ON processed_credit_events (loan_id);

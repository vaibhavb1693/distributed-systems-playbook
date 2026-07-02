-- Strategy 4: natural idempotency via upsert. loan_id as PK means applying the same
-- decision event any number of times converges to identical state — no dedup bookkeeping
-- needed at all, which is the whole point of this strategy.
CREATE TABLE loan_decisions (
    loan_id    UUID PRIMARY KEY,
    decision   VARCHAR(20) NOT NULL,
    decided_at TIMESTAMP NOT NULL DEFAULT NOW()
);

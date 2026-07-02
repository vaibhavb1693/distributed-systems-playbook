-- id is the loan's natural identifier (the UUID the producer generates client-side), not
-- a generated PK — every event in the lifecycle references the same loanId.
CREATE TABLE loan_applications (
    id              UUID PRIMARY KEY,
    applicant_name  VARCHAR(255) NOT NULL,
    amount          NUMERIC(15, 2) NOT NULL,
    kyc_verified    BOOLEAN NOT NULL DEFAULT FALSE,
    credit_score    INT,
    -- Used by the optimistic-locking dedup strategy (Strategy 3): the KYC consumer does a
    -- direct WHERE id=? AND version=? update rather than JPA's standard load-then-save
    -- optimistic locking flow, so it can detect a stale/duplicate event without loading
    -- the entity first.
    version         BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

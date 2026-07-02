-- PERMANENT failures land here for a human to look at. payload is kept so a resolved-and-
-- fixed document can still be replayed via the Replay API without re-uploading.
CREATE TABLE kyc_manual_review (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id     VARCHAR(255) NOT NULL,
    user_id         VARCHAR(255) NOT NULL,
    failure_reason  VARCHAR(255) NOT NULL,
    payload         TEXT NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING_REVIEW',
    failed_at       TIMESTAMP NOT NULL,
    resolution_note TEXT,
    resolved_by     VARCHAR(255),
    resolved_at     TIMESTAMP,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_kyc_manual_review_status ON kyc_manual_review (status);
CREATE INDEX idx_kyc_manual_review_document_id ON kyc_manual_review (document_id);

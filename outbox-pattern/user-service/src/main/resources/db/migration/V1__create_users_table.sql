CREATE TABLE users (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(255)        NOT NULL,
    email       VARCHAR(255)        NOT NULL UNIQUE,
    phone       VARCHAR(20),
    kyc_status  VARCHAR(20)         NOT NULL DEFAULT 'PENDING',
    version     BIGINT              NOT NULL DEFAULT 0,
    created_at  TIMESTAMP           NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP           NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_users_email ON users (email);

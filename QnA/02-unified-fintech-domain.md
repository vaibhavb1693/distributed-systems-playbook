# Q: How do all 5 patterns fit into one unified domain?

> Decision made after 01-implementation-ideas.md. Mixed domains across patterns would dilute the
> "playbook" narrative. We unified under a fintech platform so the top-level README tells one
> coherent story: one platform, five hard distributed systems problems, five production solutions.
>
> This file supersedes the use cases in 01-implementation-ideas.md.
> All implementation details from that file are preserved here with domain adjustments applied.

---

## The Platform: FinFlow — A Fintech Lending & Payments Platform

FinFlow is a fictional but realistic fintech platform that handles user onboarding, loan applications,
payment transactions, open banking API access for third-party partners, and KYC (Know Your Customer)
document verification. Each of the 5 patterns solves a real, high-stakes problem within this platform.

**Why fintech:**
- Correctness is non-negotiable — money, compliance, and regulation make every failure consequential
- Naturally justifies every pattern: outbox for audit trails, circuit breaker for fraud scoring SLAs,
  idempotency for payment dedup, rate limiting for partner API fairness, DLQ for KYC compliance
- All 5 patterns feel necessary, not contrived

---

## Platform Services Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        FinFlow Platform                          │
│                                                                  │
│  user-service          → outbox-pattern                          │
│  transaction-service   → circuit-breaker (calls fraud-service)   │
│  loan-processing-service → idempotent-consumer                   │
│  open-banking-api      → rate-limiter-redis                      │
│  kyc-service           → dead-letter-queue                       │
└─────────────────────────────────────────────────────────────────┘
```

---

## 1. Outbox Pattern — User Onboarding Service

### Use Case
When a user registers on FinFlow or updates their profile/KYC status, the `user-service` must write
to Postgres AND reliably publish events to downstream consumers — without dual-write risk:
- `notification-service`: sends welcome email / KYC status SMS
- `audit-service`: writes immutable compliance audit log (regulatory requirement)
- `analytics-service`: tracks onboarding funnel

A crash between the DB write and Kafka publish cannot leave audit logs incomplete or welcome emails
unsent. In fintech, an incomplete audit trail is a compliance violation, not just a bug.

### Why This Use Case
- Registration is low-frequency → CDC is overkill, polling is fine
- Profile/KYC updates are higher-frequency → polling starts adding DB load, CDC shines
- Audit log correctness is a hard requirement — naturally motivates exactly-once publishing
- Two distinct event frequencies in the same service justify implementing both approaches

### Two Approaches — Implemented Side by Side

**Approach A: Polling-based Outbox**
- `user-service` writes user record + `outbox` row in a single DB transaction
- `OutboxPoller` (`@Scheduled`, every 500ms) queries `WHERE status = 'PENDING'`, publishes to Kafka,
  marks `PUBLISHED`
- Simple infra, zero extra dependencies, easy to reason about and debug
- Trade-off: polling interval is the floor on event latency; under high load, polling adds DB read
  pressure; missed poll windows delay downstream consumers

**Approach B: Debezium CDC (Change Data Capture)**
- Same transactional write to `outbox` table — no application change
- Debezium connector watches Postgres WAL; every `INSERT` into `outbox` streams to Kafka via
  Kafka Connect — no application polling, no DB queries
- Near real-time (sub-100ms); zero DB load from polling
- Trade-off: heavier infra (Debezium + Kafka Connect in Docker Compose); schema changes to `outbox`
  table require connector reconfiguration; harder to operate locally

### What to Implement
- `user-service`: `POST /users/register`, `PUT /users/{id}/profile`, `PUT /users/{id}/kyc-status`
- `outbox` table: `id (UUID)`, `aggregate_type`, `aggregate_id`, `event_type`, `payload (JSONB)`,
  `status (PENDING|PUBLISHED|FAILED)`, `created_at`, `published_at`
- Approach A: `OutboxPoller` with `@Transactional` read + Kafka publish + status update
- Approach B: `debezium-connector.json` config + Kafka Connect container in Docker Compose
- Simulated downstream listeners: `NotificationConsumer`, `AuditConsumer` (lightweight, same app)
- README: side-by-side polling vs CDC comparison table, failure scenarios, when to use each

---

## 2. Circuit Breaker — Transaction & Fraud Detection

### Use Case
`transaction-service` receives incoming payment transactions on FinFlow. Before processing any
transaction, it synchronously calls `fraud-detection-service` to get a risk score (`LOW`, `MEDIUM`,
`HIGH`). If risk is `HIGH`, the transaction is blocked.

The `fraud-detection-service` is a latency-sensitive, critical dependency. Under load it can slow
down or fail. Without a circuit breaker:
- Threads pile up waiting for a slow fraud service → transaction-service exhausts its thread pool
- Every transaction hangs → cascading failure across the platform

With a circuit breaker: after N failures/slow calls, the circuit opens, fraud calls fast-fail, and
a fallback risk score is used (`MEDIUM` with a `FLAGGED_FOR_REVIEW` marker) — fail-open, not
fail-closed, because blocking all transactions during a fraud service outage is worse than letting
them through with a review flag.

### Why This Use Case
- Both services built by us → we fully control failure simulation
- The fail-open vs fail-closed decision is a real, consequential engineering call — documented in README
- Rich enough to exercise every Resilience4j feature meaningfully

### All Resilience4j Features Implemented

| Feature | Config | What It Demonstrates |
|---|---|---|
| State machine | CLOSED → OPEN → HALF-OPEN | Core circuit breaker lifecycle |
| Failure rate threshold | 50% in last 10 calls | Opens on error rate |
| Slow call rate threshold | 50% calls > 2s in last 10 | Opens on latency, not just errors |
| Wait duration in OPEN | 30s | How long before attempting recovery |
| Permitted calls in HALF-OPEN | 3 | Probe calls before fully closing |
| Fallback | `MEDIUM` risk + `FLAGGED_FOR_REVIEW` | Fail-open with degraded behavior |
| Bulkhead (semaphore) | Max 10 concurrent calls | Prevents thread exhaustion |
| Retry | 2 retries before circuit counts failure | Retry layered under circuit breaker |
| Health indicator | `/actuator/health/fraudCircuitBreaker` | Ops visibility |
| Circuit breaker events | Logged as structured JSON | State transition observability |

### Failure Simulation
`fraud-detection-service` exposes a `/admin/degrade?mode=` endpoint (disabled in prod profile):
- `mode=latency`: adds configurable sleep (default 5s) → triggers slow-call threshold
- `mode=error`: returns HTTP 500 on all `/score` requests → triggers failure rate threshold
- `mode=down`: disables the endpoint entirely → connection refused path
- `mode=reset`: restores normal behavior

README includes a step-by-step demo script:
1. Start both services
2. Hit `/transactions` — see fraud score in response
3. Hit `/admin/degrade?mode=error` on fraud service
4. Watch circuit transition CLOSED → OPEN in logs
5. Hit `/transactions` — see fallback response with `FLAGGED_FOR_REVIEW`
6. Wait 30s → circuit goes HALF-OPEN → probes → CLOSED

---

## 3. Idempotent Consumer — Loan Application Processing

### Use Case
`loan-processing-service` consumes Kafka events across the loan lifecycle on FinFlow:
- `loan.application.submitted`
- `kyc.document.verified`
- `credit.score.received`
- `loan.decision.made`

Kafka's at-least-once delivery means any event can arrive more than once (consumer rebalance,
broker retry, network blip). Applying duplicates corrupts loan state — a double credit score
update changes the applicant's score, a duplicate decision letter is a compliance violation,
a double disbursement is a financial loss.

### Why This Use Case
- Financial domain: duplicate processing is a regulatory and financial risk, not just a bug
- Four distinct event types with naturally different idempotency needs — each strategy is the
  right fit for its event, not forced
- Demonstrates why "just use Redis for everything" is wrong

### Four Strategies — One Per Event Type

**Strategy 1: Redis TTL Dedup** → `loan.application.submitted`
- On consume: `SET rl:dedup:{eventId} 1 NX EX 86400` — if key exists, skip
- Processes application creation only once within a 24-hour dedup window
- Best for: high-frequency events, short dedup window is acceptable, lowest DB overhead
- Trade-off: Redis unavailability breaks dedup; TTL expiry closes the window (redelivery after
  24h would reprocess)

**Strategy 2: DB Unique Constraint** → `credit.score.received`
- On consume: insert into `processed_credit_events(event_id UUID PK, loan_id, processed_at)`
- Duplicate delivery → `DataIntegrityViolationException` on `event_id` PK → caught, logged, skipped
- Provides permanent audit trail of every credit score event processed (compliance value)
- Best for: long-lived dedup requirement, audit trail needed, low-to-medium frequency
- Trade-off: DB write on every event; table grows unbounded (needs pruning job for old records)

**Strategy 3: Optimistic Locking / Version Check** → `kyc.document.verified`
- `LoanApplication` entity has a `version (BIGINT)` field managed by `@Version`
- Event payload carries `expectedVersion`; service updates `WHERE id=? AND version=?`
- 0 rows updated → stale or duplicate event → skip with warning log
- Also prevents out-of-order processing: an older event cannot overwrite a newer state
- Best for: state transition events where ordering matters, not just deduplication
- Trade-off: producer must propagate version in event; tighter producer-consumer contract

**Strategy 4: Natural Idempotency / Upsert** → `loan.decision.made`
- Decision is a terminal state: `APPROVED` or `REJECTED` with a decision timestamp
- Service executes: `INSERT INTO loan_decisions ... ON CONFLICT (loan_id) DO UPDATE SET decision=...`
- Applying it 10 times produces identical state — no side effect
- Best for: terminal/convergent state events; simplest implementation when semantics allow
- Trade-off: only valid when the operation is naturally convergent; cannot use for additive
  operations (e.g., incrementing a balance)

---

## 4. Redis Rate Limiter — Open Banking API

### Use Case
FinFlow exposes an Open Banking API consumed by licensed third-party partners (fintech apps,
aggregators, payment initiators). Partners are tiered: `FREE`, `PRO`, `ENTERPRISE`. Rate limiting
serves two purposes:
1. **Fairness**: prevent one partner from starving others on shared infrastructure
2. **Revenue protection**: Free tier limits enforce upgrade pressure; Enterprise gets burst allowance

Different partners have legitimately different traffic shapes — a batch reconciliation job sends
bursts; a real-time balance checker sends smooth traffic. No single algorithm fits all.

### Why This Use Case
- Multi-tenancy makes rate limiting a revenue and SLA concern, not just abuse prevention
- Naturally justifies different algorithms per tier/use case — not arbitrary
- Exposes the real engineering question every platform team faces: which algorithm, and why?

### All 5 Algorithms Implemented

| Algorithm | Endpoint Prefix | Tier Fit | Key Trade-off |
|---|---|---|---|
| Fixed Window Counter | `/ob/fixed/` | Free tier | Simple but burst at window boundary (2x rate for 1s) |
| Sliding Window Log | `/ob/sliding-log/` | Billing/audit APIs | Exact but high Redis memory (every timestamp stored) |
| Sliding Window Counter | `/ob/sliding-counter/` | Pro tier default | Good balance of accuracy and memory |
| Token Bucket | `/ob/token-bucket/` | Enterprise, SDKs | Burst-friendly; bucket refills at steady rate |
| Leaky Bucket | `/ob/leaky-bucket/` | Traffic shaping to downstream | Smoothest output; no burst tolerance |

### Implementation Details
- Common `RateLimiter` interface — strategy pattern; algorithm selected per route via config
- Redis key namespace: `rl:{algorithm}:{partnerId}` — per-partner, per-algorithm isolation
- All Redis operations via Lua scripts — atomic read-modify-write, no TOCTOU race conditions
- Tier limits via `application.yml`:
  - `FREE`: 60 req/min, no burst
  - `PRO`: 1000 req/min, sliding counter
  - `ENTERPRISE`: 10000 req/min, token bucket with burst=500
- Standard response headers on every request:
  - `X-RateLimit-Limit`: configured limit for this partner/tier
  - `X-RateLimit-Remaining`: requests left in current window
  - `X-RateLimit-Reset`: epoch seconds when window resets
  - `Retry-After`: included on `429 Too Many Requests` only
- Load test script (curl loop or k6) in README demonstrating each algorithm's behavior under burst:
  - Fixed window: shows the boundary burst problem
  - Token bucket: shows burst absorption then smooth throttle
  - Leaky bucket: shows flat rejection of bursts

---

## 5. Dead Letter Queue — KYC Document Verification Pipeline

### Use Case
FinFlow requires KYC (Know Your Customer) verification for all users. When a user uploads identity
documents, a `kyc.document.uploaded` event is published. The `kyc-service` consumes this event and
runs a pipeline: validate document format → OCR extraction → vendor verification API call → store
result to Postgres → update user KYC status.

Failures are varied and inevitable:
- **Transient**: vendor API timeout, Postgres connection blip, downstream 503
- **Permanent**: corrupted document, unsupported file format, OCR extraction failed, vendor
  rejected document as fraudulent

Every failure must be traceable and recoverable. Silently dropping a KYC event means a user is
stuck unverified indefinitely — a support and compliance problem.

### Why This Use Case
- KYC is a legal requirement: failed events cannot be silently dropped
- Rich failure taxonomy (transient vs permanent) makes DLQ processing logic non-trivial
- Replay and manual review flows are a genuine operational necessity, not just nice-to-have
- Fits perfectly into the FinFlow fintech narrative

### Full DLQ Lifecycle — All Aspects Implemented

**At the Consumer (`kyc-service`)**
- Failure classification before routing:
  - `TRANSIENT`: vendor API timeout, DB connection error, HTTP 503 from vendor
  - `PERMANENT`: unsupported MIME type, document too small/corrupted, vendor fraud flag, OCR failure
- Retry policy for `TRANSIENT` only: 3 attempts with exponential backoff (1s → 2s → 4s)
- After exhausting retries (or immediately for `PERMANENT`): publish to `kyc.document.dlq` topic
  with enriched Kafka headers:
  - `X-Failure-Reason`: human-readable error (e.g., `VENDOR_TIMEOUT`, `UNSUPPORTED_FORMAT`)
  - `X-Failure-Type`: `TRANSIENT` | `PERMANENT`
  - `X-Retry-Count`: number of attempts made
  - `X-Original-Topic`: `kyc.document.uploaded`
  - `X-Failed-At`: ISO-8601 timestamp
  - `X-Document-Id`: for correlation

**DLQ Processor Service (`kyc-dlq-processor`)**
- Separate Spring Boot service consuming `kyc.document.dlq`
- Re-classifies on arrival using `X-Failure-Type` header:
  - `TRANSIENT`: schedules auto-replay after a 5-minute cooldown (scheduled task republishes to
    original topic)
  - `PERMANENT`: inserts into `kyc_manual_review` Postgres table with `status=PENDING_REVIEW`,
    `failure_reason`, `document_id`, `user_id`, `failed_at`
- Alerting hook: when `kyc_manual_review` has >10 `PENDING_REVIEW` rows, fires a structured
  alert log (simulates PagerDuty/Slack webhook payload with message content)

**Replay API (on `kyc-dlq-processor`)**
- `POST /dlq/kyc/replay/{documentId}` — re-publishes specific message to `kyc.document.uploaded`
- `POST /dlq/kyc/replay/bulk?failureType=TRANSIENT` — bulk replay all transient failures
- `GET /dlq/kyc/messages?status=PENDING_REVIEW` — list messages awaiting manual action
  (paginated, filterable by `failureReason`)
- `PATCH /dlq/kyc/messages/{id}/resolve` — mark manually resolved with `resolutionNote` and
  `resolvedBy`
- `GET /dlq/kyc/stats` — summary: total received, by failure type, replay success rate,
  pending manual review count

**Metrics (Micrometer + Actuator)**
- `kyc.dlq.messages.received` tagged by `failure_type`
- `kyc.dlq.replay.attempted` / `kyc.dlq.replay.success` / `kyc.dlq.replay.failed`
- `kyc.dlq.manual_review.pending` — gauge; this is the alerting trigger

---

## Platform Narrative for Top-Level README

```
A user signs up on FinFlow
  └─► user-service saves profile + publishes via Outbox (reliable, no dual-write risk)
        └─► audit-service logs the event (compliance)
        └─► notification-service sends welcome email

The user applies for a loan
  └─► loan events consumed by loan-processing-service with Idempotent Consumer
        (4 strategies across the lifecycle — no duplicate processing)

The user makes a payment
  └─► transaction-service calls fraud-detection-service via Circuit Breaker
        (resilient to fraud service outages — fail-open with review flag)

Third-party partners access FinFlow data via Open Banking API
  └─► rate-limiter-redis enforces per-partner, per-tier limits
        (5 algorithms — right tool for each traffic shape)

The user uploads KYC documents
  └─► kyc-service processes via pipeline; failures handled via Dead Letter Queue
        (retry, replay, manual review — nothing silently dropped)
```

This is the arc the top-level README will follow.

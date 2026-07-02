# Q: What are the production-like use cases and implementation scope for each pattern?

> Decided before writing any code for distributed-systems-playbook. Use this as the implementation north star for all 5 patterns.

---

## 1. Outbox Pattern — User Account & Profile Service

### Use Case
When a user registers or updates their profile, the service must write to Postgres AND reliably publish events to downstream consumers (email service, analytics, audit log) — without dual-write risk. A crash between the DB write and Kafka publish must never leave the system inconsistent.

### Why This Use Case
- Avoids the ecommerce domain (already covered in kafka-saga-patterns)
- High-stakes correctness requirement: a welcome email must fire exactly once, audit log must be complete
- Naturally motivates both approaches — registration is low-frequency (CDC overkill), profile updates are higher-frequency (polling starts to hurt)

### Two Approaches — Implemented Side by Side

**Approach A: Polling-based Outbox**
- A transactional write saves the domain change + an `outbox` row in the same DB transaction
- A `@Scheduled` poller queries `outbox` table for `PENDING` events, publishes to Kafka, marks `PUBLISHED`
- Simple infra, no extra dependencies, easy to reason about
- Trade-off: polling interval = minimum latency; polling under load adds DB pressure

**Approach B: Debezium CDC (Change Data Capture)**
- Same transactional write to `outbox` table
- Debezium watches the Postgres WAL (Write-Ahead Log) and streams row inserts directly into Kafka
- No application-side polling; near real-time; zero DB query overhead
- Trade-off: heavier infra (Debezium + Kafka Connect), harder to operate, schema change sensitivity

### What to Implement
- `user-service`: registration + profile update endpoints
- `outbox` table with `id`, `aggregate_type`, `aggregate_id`, `event_type`, `payload`, `status`, `created_at`
- Approach A: `OutboxPoller` scheduled task with `@Transactional` write + poll loop
- Approach B: Debezium connector config + Kafka Connect in Docker Compose
- Downstream consumers simulated: `email-consumer`, `audit-consumer` (lightweight listeners in same app or separate)
- README: polling vs CDC comparison table, when to use each, failure scenarios

---

## 2. Circuit Breaker — Fraud Detection Pipeline

### Use Case
A `transaction-service` (upstream) receives incoming financial transactions and synchronously calls a `fraud-detection-service` (downstream) to get a risk score before allowing the transaction. The fraud service is a third-party-style dependency — it can go slow, throw errors, or go completely dark.

### Why This Use Case
- Both services are built by us — we control the failure simulation
- High-stakes fallback decision: fail-open (allow transaction with default score) vs fail-closed (reject) — this is a real engineering judgment call
- The fraud service has a `/test/degrade` endpoint to simulate latency, errors, and outage on demand

### All Resilience4j Features Implemented

| Feature | Implementation |
|---|---|
| State machine | CLOSED → OPEN → HALF-OPEN with configurable thresholds |
| Failure rate threshold | Opens circuit after N% failures in sliding window |
| Slow call rate threshold | Also opens on slow calls, not just errors |
| Wait duration | Configurable time circuit stays OPEN before trying HALF-OPEN |
| Fallback | Returns cached/default risk score (fail-open with `LOW_RISK` flag) |
| Bulkhead | Limits concurrent calls to fraud service |
| Retry | Retry 2x before circuit counts the failure |
| Health indicator | Circuit state exposed via `/actuator/health` |
| Events | Circuit state transitions logged as structured events |

### Failure Simulation
- `fraud-detection-service` has a `/admin/degrade?mode=latency|error|down` endpoint
- `latency` mode: adds 5s sleep to all responses (triggers slow-call threshold)
- `error` mode: returns 500s (triggers failure rate threshold)
- `down` mode: shuts down the endpoint entirely (connection refused path)
- Demo script in README showing each scenario and circuit response

---

## 3. Idempotent Consumer — Loan Application Processing

### Use Case
A `loan-processing-service` consumes Kafka events across the loan lifecycle: `application.submitted`, `document.verified`, `credit.score.received`, `loan.decision.made`. Due to Kafka's at-least-once delivery guarantee, any event can arrive more than once. Applying them twice corrupts loan state — a double credit score update or duplicate decision is a compliance violation.

### Why This Use Case
- Financial domain: correctness is non-negotiable, not just a nice-to-have
- Four distinct event types with naturally different idempotency strategies — not forced, each strategy is the right fit for its event
- Rich enough to show why one-size-fits-all (just use Redis) is wrong

### Four Strategies — One Per Event Type

**Strategy 1: Redis TTL Dedup** → `application.submitted`
- On consume, check if `eventId` exists in Redis (`SET NX EX 86400`)
- If key exists: skip (duplicate). If not: process and set key.
- Best for: high-frequency events, short dedup window acceptable, lowest DB load
- Trade-off: Redis down = dedup broken; TTL expiry = window closes

**Strategy 2: DB Unique Constraint** → `credit.score.received`
- Insert event record into `processed_events(event_id, aggregate_id, processed_at)` with `UNIQUE(event_id)`
- On duplicate: `DataIntegrityViolationException` caught → skip processing
- Best for: long-lived dedup requirement, audit trail needed
- Trade-off: DB write on every event; dead rows accumulate (needs pruning job)

**Strategy 3: Optimistic Locking / Version Check** → `document.verified`
- Each `LoanApplication` entity has a `version` field
- Event carries expected version; service updates only if `WHERE id=? AND version=?` matches (0 rows updated = stale/duplicate)
- Best for: state transition events where order matters; prevents out-of-order processing too
- Trade-off: version must be propagated in the event; more complex producer contract

**Strategy 4: Natural Idempotency / Upsert** → `loan.decision.made`
- Decision is `APPROVED` or `REJECTED` — a final terminal state
- Service does an upsert: `INSERT ... ON CONFLICT (application_id) DO UPDATE SET decision=...`
- Applying it 10 times has zero side effect — result is always the same
- Best for: terminal/convergent state events; simplest implementation when semantics allow
- Trade-off: only works when the operation is naturally idempotent; can't use for additive operations

---

## 4. Redis Rate Limiter — Multi-Tenant SaaS Content API

### Use Case
A `content-api` serves multiple tenants (Free, Pro, Enterprise tiers) with different rate limits. The goal: prevent any single tenant from starving others, allow legitimate bursts for Pro/Enterprise, and shape traffic smoothly for downstream services.

### Why This Use Case
- Multi-tenancy makes rate limiting a revenue/fairness concern, not just abuse prevention
- Different tenant tiers naturally justify different algorithms (Free = strict, Enterprise = burst-friendly)
- Exposes the real engineering question: which algorithm for which scenario?

### All 5 Algorithms Implemented

| Algorithm | Endpoint Prefix | Best For | Key Trade-off |
|---|---|---|---|
| Fixed Window Counter | `/api/fixed/` | Simplest use cases | Burst at window boundary (2x rate for 1s) |
| Sliding Window Log | `/api/sliding-log/` | Exact rate limiting, billing APIs | High Redis memory (stores every timestamp) |
| Sliding Window Counter | `/api/sliding-counter/` | Balance of accuracy + memory | Slight approximation at window boundary |
| Token Bucket | `/api/token-bucket/` | Burst-friendly (API SDKs, uploads) | Burst capacity can be abused if misconfigured |
| Leaky Bucket | `/api/leaky-bucket/` | Traffic shaping, smooth downstream load | No burst allowed; unfair under legitimate spikes |

### Implementation Details
- Each algorithm behind a common `RateLimiter` interface — strategy pattern
- Per-tenant, per-algorithm Redis key namespace: `rl:{algorithm}:{tenantId}`
- Lua scripts for atomic Redis operations (prevents race conditions)
- Response headers on every request: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`, `Retry-After` (on 429)
- Tier config via `application.yml`: Free=60/min, Pro=600/min, Enterprise=6000/min
- Load test script (`k6` or simple curl loop) in README to demonstrate each algorithm's behavior under burst

---

## 5. Dead Letter Queue — Document Ingestion Pipeline

### Use Case
A `document-ingestion-service` consumes `document.uploaded` events and processes documents: validate format, extract metadata, virus-scan, store to object store. Failures are inevitable and varied — malformed files, unsupported formats, downstream storage timeouts, schema validation errors. A blocked consumer or silent drop is unacceptable; every failure must be traceable and recoverable.

### Why This Use Case
- Rich failure taxonomy (transient vs permanent) makes the DLQ processing logic non-trivial
- The replay and manual review flows make it a complete system, not just error routing
- Document processing is universally relatable (not domain-specific)

### Full DLQ Lifecycle Implemented

**At the Consumer (`document-ingestion-service`)**
- Classify failure before routing: `TRANSIENT` (DB timeout, storage 503) vs `PERMANENT` (malformed JSON, unsupported MIME type, virus detected)
- Retry policy: 3 attempts with exponential backoff (1s → 2s → 4s) for transient failures only
- After exhausting retries: publish to `document.ingestion.dlq` topic with enriched headers:
  - `X-Failure-Reason`: human-readable error class
  - `X-Failure-Type`: `TRANSIENT` | `PERMANENT`
  - `X-Retry-Count`: how many times attempted
  - `X-Original-Topic`: where it came from
  - `X-Failed-At`: ISO timestamp

**DLQ Processor Service (`dlq-processor-service`)**
- Separate Spring Boot service consuming `document.ingestion.dlq`
- Re-classifies using `X-Failure-Type` header
- `TRANSIENT`: schedules auto-replay after a cooldown window (uses a delay queue or scheduled task)
- `PERMANENT`: inserts into `manual_review_queue` Postgres table with `status=PENDING_REVIEW`
- Alerting hook: when DLQ consumer lag exceeds threshold, fires a structured alert log (simulates webhook to PagerDuty/Slack)

**Replay API**
- `POST /dlq/replay/{messageId}`: re-publishes a specific message back to the original topic
- `POST /dlq/replay/bulk?failureType=TRANSIENT`: bulk replay by failure class
- `GET /dlq/messages?status=PENDING_REVIEW`: list messages awaiting manual action
- `PATCH /dlq/messages/{id}/resolve`: mark a message as manually resolved (with resolution note)

**Metrics (via Actuator + Micrometer)**
- `dlq.messages.received` (by failure type)
- `dlq.replay.success` / `dlq.replay.failed`
- `dlq.manual_review.pending` gauge

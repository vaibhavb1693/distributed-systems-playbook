# Idempotent Consumer Pattern — FinFlow Loan Application Processing

**Problem:** Kafka's at-least-once delivery means any event can arrive more than once — a
consumer rebalance, a broker retry, a network blip, or a redelivered offset after a crash before
commit. In a loan lifecycle, applying the same event twice is a real financial/compliance risk:
a duplicate credit score update corrupts the applicant's score, a duplicate decision letter is a
compliance violation, a duplicate disbursement is a financial loss.

**Why this way:** There is no single "right" dedup strategy — the correct one depends on the
event's frequency, whether an audit trail is needed, and whether the operation is naturally
convergent. `loan-processing-service` consumes 4 event types across the loan lifecycle, each with
a genuinely different idempotency need, and implements the strategy that actually fits — not one
strategy forced onto everything.

**How to run:**
```bash
docker-compose up -d --build
```

---

## How It Works

```
┌──────────────────────────┐
│  loan-application-service │   publishes 4 event types (times=N to demo redelivery)
└─────────────┬─────────────┘
              │
    ┌─────────┴──────────────────────────────────────────────────┐
    ▼                  ▼                    ▼                     ▼
finflow.loan.      finflow.kyc.        finflow.credit.       finflow.loan.
application.        document.           score.                decision.
submitted            verified            received              made
    │                  │                    │                     │
    ▼                  ▼                    ▼                     ▼
Strategy 1:         Strategy 3:          Strategy 2:           Strategy 4:
Redis TTL           Optimistic Lock      DB Unique Constraint  Natural Idempotency
(SET NX EX)         (version compare)    (PK violation)        (upsert)
    │                  │                    │                     │
    └──────────────────┴────────────────────┴─────────────────────┘
                                    ▼
                        loan-processing-service
                     (Postgres: loan_applications,
                      processed_credit_events, loan_decisions
                      + Redis: dedup keys)
```

---

## Four Strategies — One Per Event Type

| Strategy | Event | Mechanism | Best For | Trade-off |
|---|---|---|---|---|
| **1. Redis TTL Dedup** | `finflow.loan.application.submitted` | `SET dedup:{eventId} 1 NX EX 86400` — atomic; the first delivery wins the key, redeliveries within 24h find it already set | High-frequency events, bounded dedup window is acceptable, lowest DB overhead | Redis unavailability breaks dedup; TTL expiry closes the window (redelivery after 24h would reprocess) |
| **2. DB Unique Constraint** | `finflow.credit.score.received` | Native `INSERT INTO processed_credit_events (event_id, ...)` — duplicate `event_id` raises `DataIntegrityViolationException`, caught and skipped | Long-lived dedup + audit trail requirement | DB write on every event; table grows unbounded (needs a pruning job in real prod) |
| **3. Optimistic Locking / Version Check** | `finflow.kyc.document.verified` | `UPDATE loan_applications SET ... , version = version + 1 WHERE id=? AND version=?` — 0 rows updated means stale/duplicate | State-transition events where ordering matters, not just dedup — also rejects out-of-order events, not only duplicates | Producer must know and propagate the current version; tighter producer-consumer contract |
| **4. Natural Idempotency / Upsert** | `finflow.loan.decision.made` | `INSERT ... ON CONFLICT (loan_id) DO UPDATE` — applying it N times converges to identical state | Terminal/convergent state events; simplest implementation when semantics allow | Only valid when the operation is naturally convergent — cannot use for additive operations like incrementing a balance |

**Why native queries, not `JpaRepository.save()`:** Strategy 2's dedup gate is the constraint
violation itself. `save()` on an entity with a manually-assigned `@Id` performs a *merge* (a
SELECT to check existence, then INSERT or UPDATE) — it would silently succeed on a duplicate
`eventId` instead of raising the exception this strategy depends on. `ProcessedCreditEventRepository.insertOrThrow()`
runs a raw `INSERT` via `@Modifying @Query(nativeQuery = true)` specifically to guarantee that.

**Why a direct `WHERE id=? AND version=?` update, not JPA's built-in `@Version` flow:** JPA's
standard optimistic locking requires loading the entity first, then throws `OptimisticLockException`
on a stale save — which the caller has to catch. A single compare-and-swap UPDATE returning a row
count is simpler to reason about in a Kafka consumer, where "stale event, skip and move on" is a
normal, expected outcome rather than an exceptional one.

---

## Demo Script

```bash
# Strategy 1: Redis TTL — same eventId sent 3 times, only the first is processed
curl -X POST "http://localhost:8101/api/loans/$(uuidgen)/submit-application?times=3" \
  -H "Content-Type: application/json" \
  -d '{"applicantName": "Vaibhav Bhatt", "amount": 500000}'
# loan-processing-service logs: 1x "processed", 2x "Duplicate suppressed (Redis TTL dedup)"

# Strategy 3: Optimistic lock — same expectedVersion sent 3 times, only the first applies
LOAN_ID=$(uuidgen)
curl -X POST "http://localhost:8101/api/loans/$LOAN_ID/submit-application" \
  -H "Content-Type: application/json" -d '{"applicantName":"Vaibhav","amount":500000}'
curl -X POST "http://localhost:8101/api/loans/$LOAN_ID/kyc-verified?expectedVersion=0&times=3"
# logs: 1x "processed", 2x "Stale or duplicate KYC event suppressed (version mismatch)"

# Strategy 2: DB unique constraint — same eventId sent 3 times, only the first is processed
curl -X POST "http://localhost:8101/api/loans/$LOAN_ID/credit-score?times=3" \
  -H "Content-Type: application/json" -d '{"creditScore": 720}'
# logs: 1x "Credit score processed", 2x "Duplicate suppressed (DB unique constraint)"

# Strategy 4: Upsert — same decision sent 3 times, all 3 "succeed" (that's the point)
curl -X POST "http://localhost:8101/api/loans/$LOAN_ID/decision?times=3" \
  -H "Content-Type: application/json" -d '{"decision": "APPROVED"}'
# logs: 3x "Decision upserted (naturally idempotent)" — loan_decisions ends up identical either way
```

Check the resulting state:
```bash
docker exec -it finflow-idempotent-postgres psql -U finflow -d finflow \
  -c "SELECT id, applicant_name, kyc_verified, credit_score, version FROM loan_applications;"
docker exec -it finflow-idempotent-postgres psql -U finflow -d finflow \
  -c "SELECT * FROM loan_decisions;"
```

---

## When to Use the Idempotent Consumer Pattern

✅ Your messaging system guarantees at-least-once delivery (Kafka, SQS standard queues, etc.)
✅ Reprocessing an event would have a real side effect (financial, compliance, notification spam)
✅ You can identify what makes an event unique — an `eventId`, a natural key, or a version

## When NOT to Use It

❌ The consumer's operation is already naturally idempotent for *every* event type — Strategy 4's
  upsert may be all you need, and adding Redis/DB dedup bookkeeping on top is pure overhead
❌ Exactly-once semantics are required end-to-end — this pattern gives you effectively-once at the
  consumer; producer-side guarantees still need something like the outbox pattern in this repo
❌ Events are cheap and side-effect-free to reprocess (e.g., idempotent by nature, low value) —
  the dedup machinery isn't worth the complexity

---

## Project Structure

```
idempotent-consumer/
├── docker-compose.yml
├── loan-application-service/          # Spring Boot — publishes the 4 lifecycle events
│   └── src/main/java/com/vaibhav/idempotent/loanapplication/
│       ├── controller/                 # LoanEventController (times= param demos redelivery)
│       ├── service/                    # LoanEventPublisher
│       └── config/                     # Kafka topic declarations
└── loan-processing-service/           # Spring Boot — the idempotent consumer
    ├── src/main/java/com/vaibhav/idempotent/loanprocessing/
    │   ├── domain/                     # LoanApplication, ProcessedCreditEvent, LoanDecision
    │   ├── repository/                 # Native @Modifying queries per strategy
    │   ├── consumer/                   # One consumer class per strategy
    │   └── dto/                        # Typed event records (one per topic)
    └── src/main/resources/
        └── db/migration/               # Flyway: loan_applications, processed_credit_events, loan_decisions
```

---

## API Reference

All 4 endpoints accept `times` (default 1) to deliberately redeliver the identical event —
same `eventId` or `expectedVersion` each time — for demoing dedup without needing Kafka-level
replay tooling.

```bash
# 1. Submit a loan application (Strategy 1: Redis TTL dedup)
curl -X POST http://localhost:8101/api/loans/{loanId}/submit-application \
  -H "Content-Type: application/json" \
  -d '{"applicantName": "Vaibhav Bhatt", "amount": 500000}'

# 2. KYC document verified (Strategy 3: optimistic lock)
curl -X POST "http://localhost:8101/api/loans/{loanId}/kyc-verified?expectedVersion=0"

# 3. Credit score received (Strategy 2: DB unique constraint)
curl -X POST http://localhost:8101/api/loans/{loanId}/credit-score \
  -H "Content-Type: application/json" -d '{"creditScore": 720}'

# 4. Loan decision made (Strategy 4: natural idempotency / upsert)
curl -X POST http://localhost:8101/api/loans/{loanId}/decision \
  -H "Content-Type: application/json" -d '{"decision": "APPROVED"}'
```

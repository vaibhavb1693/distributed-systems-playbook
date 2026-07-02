# Dead Letter Queue Pattern — FinFlow KYC Document Verification

**Problem:** `kyc-service` runs a pipeline on every uploaded document — validate format, OCR
extraction, vendor verification, store result, update KYC status. Failures are inevitable and
varied: a vendor API timeout is transient (retry and it'll probably work), a corrupted document
is permanent (retrying does nothing). KYC is a legal requirement — silently dropping a failed
event leaves a user stuck unverified indefinitely, which is a support and compliance problem, not
just a bug.

**Why this way:** Classify every failure as TRANSIENT or PERMANENT at the point it happens.
Retry TRANSIENT failures with backoff; route anything that exhausts retries (or is PERMANENT
immediately) to a dead letter topic with enough context to act on later — without a human, for
transient issues that resolve themselves, and with one, for everything else.

**How to run:**
```bash
docker-compose up -d --build
```

---

## Full DLQ Lifecycle

```
kyc-service                                          kyc-dlq-processor
────────────                                         ─────────────────

POST /api/kyc/documents/upload
         │
         ▼
finflow.kyc.document.uploaded
         │
         ▼
KycPipelineService.process()
         │
    ┌────┴─────┐
    ▼          ▼
 success   failure classified
    │       ┌────┴─────┐
    │       ▼           ▼
    │  TRANSIENT     PERMANENT
    │  retry 3x          │
    │  (1s,2s,4s)         │
    │       │             │
    │  still failing      │
    │       └──────┬──────┘
    │              ▼
    │   finflow.kyc.document.dlq
    │   (+ X-Failure-Type/-Reason/-Retry-Count/
    │      -Original-Topic/-Failed-At/-Document-Id)
    │              │
    │              ▼
    │      KycDlqConsumer (re-classifies via X-Failure-Type header)
    │        ┌─────┴──────┐
    │        ▼             ▼
    │   TRANSIENT      PERMANENT
    │   kyc_dlq_        kyc_manual_review
    │   pending_replay  (status=PENDING_REVIEW)
    │   (replay_at =         │
    │    failedAt+5min)      ▼
    │        │        alert if >10 pending
    │        ▼        (structured log, simulates PagerDuty/Slack)
    │   KycDlqReplayPoller
    │   (@Scheduled, polls
    │    for due rows,
    │    republishes to
    │    finflow.kyc.document.uploaded)
    │        │
    └────────┴──── back to kyc-service, naturally re-enters the pipeline
```

---

## At the Consumer (`kyc-service`)

Failure classification happens once, in `KycPipelineService` — the two custom exceptions
(`TransientFailureException`, `PermanentFailureException`) carry a `reasonCode` that flows all
the way through to the DLQ headers:

| Failure | Type | Reason Code |
|---|---|---|
| Vendor verification API timeout | TRANSIENT | `VENDOR_TIMEOUT` |
| DB connection error persisting result | TRANSIENT | `DB_CONNECTION_ERROR` |
| Document fails format validation | PERMANENT | `CORRUPTED_DOCUMENT` |
| Unsupported MIME type | PERMANENT | `UNSUPPORTED_FORMAT` |
| OCR extraction failed | PERMANENT | `OCR_EXTRACTION_FAILED` |
| Vendor flagged as fraudulent | PERMANENT | `VENDOR_FRAUD_FLAG` |

`KycDocumentUploadedConsumer` retries TRANSIENT failures **synchronously** (blocking the listener
thread through `Thread.sleep` between attempts, not scheduling async work) — 3 attempts, 1s → 2s
→ 4s backoff. If the process crashes mid-retry, the message hasn't been acked yet (`ack-mode:
RECORD` acks after the listener method returns), so Kafka redelivers it and the retry sequence
starts over. Simpler durability story than an async retry scheduler, at the cost of blocking that
partition's processing during backoff — a deliberate trade-off documented here, not hidden.

On final failure, `finflow.kyc.document.dlq` gets the message with 6 headers:
`X-Failure-Reason`, `X-Failure-Type`, `X-Retry-Count`, `X-Original-Topic`, `X-Failed-At`,
`X-Document-Id`.

---

## DLQ Processor (`kyc-dlq-processor`)

Re-classifies purely from the `X-Failure-Type` header — no pipeline logic re-runs here.

- **TRANSIENT** → inserted into `kyc_dlq_pending_replay` with `replay_at = now + 5min`.
  `KycDlqReplayPoller` (`@Scheduled`, mirrors `outbox-pattern`'s `OutboxPoller`) polls for due
  rows and republishes them to `finflow.kyc.document.uploaded` — durable across restarts, since
  the schedule lives in Postgres, not an in-memory timer.
- **PERMANENT** → inserted into `kyc_manual_review` with `status=PENDING_REVIEW`. Immediately
  checks whether the pending count now exceeds the alert threshold (10 by default) and, if so,
  logs a structured JSON alert payload (`ManualReviewAlertService`) simulating a PagerDuty/Slack
  webhook.

---

## Replay API (`kyc-dlq-processor`, `/dlq/kyc`)

```bash
# Replay a specific document (checks manual review first, then pending-transient-replay)
curl -X POST http://localhost:8122/dlq/kyc/replay/{documentId}

# Bulk replay — TRANSIENT bypasses the 5-min cooldown (explicit ops action);
# PERMANENT replays every PENDING_REVIEW row
curl -X POST "http://localhost:8122/dlq/kyc/replay/bulk?failureType=TRANSIENT"

# List messages awaiting manual review (paginated, filterable)
curl "http://localhost:8122/dlq/kyc/messages?status=PENDING_REVIEW&failureReason=CORRUPTED_DOCUMENT"

# Mark manually resolved
curl -X PATCH http://localhost:8122/dlq/kyc/messages/{id}/resolve \
  -H "Content-Type: application/json" \
  -d '{"resolutionNote": "User re-uploaded a valid document", "resolvedBy": "ops-team"}'

# Summary stats
curl http://localhost:8122/dlq/kyc/stats
```

---

## Metrics (Micrometer + Actuator)

- `kyc.pipeline.processed` (kyc-service) — tagged `outcome=success|transient_failure|permanent_failure`
- `kyc.dlq.messages.received` (kyc-dlq-processor) — tagged `failure_type`
- `kyc.dlq.replay.attempted` / `.success` / `.failed`
- `kyc.dlq.manual_review.pending` — gauge; this is the alerting trigger

---

## Demo Script

```bash
# Happy path — no failure
curl -X POST http://localhost:8121/api/kyc/documents/upload \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-1", "documentType": "PASSPORT", "simulate": "NONE"}'

# Transient failure — watch kyc-service logs retry 3x (1s,2s,4s) then route to DLQ,
# then watch kyc-dlq-processor auto-replay it ~5 minutes later
curl -X POST http://localhost:8121/api/kyc/documents/upload \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-2", "documentType": "PASSPORT", "simulate": "VENDOR_TIMEOUT"}'

# Permanent failure — routed to DLQ immediately, no retries, lands in manual review
curl -X POST http://localhost:8121/api/kyc/documents/upload \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-3", "documentType": "PASSPORT", "simulate": "CORRUPTED"}'

# Check it landed in manual review
curl "http://localhost:8122/dlq/kyc/messages?status=PENDING_REVIEW"

# Trigger the alert threshold — upload 11 permanently-failing documents
for i in $(seq 1 11); do
  curl -s -X POST http://localhost:8121/api/kyc/documents/upload \
    -H "Content-Type: application/json" \
    -d "{\"userId\": \"user-alert-$i\", \"documentType\": \"PASSPORT\", \"simulate\": \"FRAUD_FLAG\"}"
done
# watch kyc-dlq-processor logs for the structured KYC_MANUAL_REVIEW_BACKLOG alert
```

---

## When to Use the Dead Letter Queue Pattern

✅ Failures are inevitable and have a real taxonomy — some retryable, some not
✅ Silently dropping a failed event is unacceptable (compliance, user-facing state, financial)
✅ You need visibility into what's failing and why, not just that something failed

## When NOT to Use It

❌ Every failure is retryable and eventually succeeds — a simple retry loop without DLQ routing
  is enough
❌ Failed events are safe to drop — logging and moving on may be all that's needed
❌ You need synchronous failure feedback to the caller — DLQ is inherently async; if the caller
  needs to know *now* whether something failed, this isn't the right layer for that signal

---

## Project Structure

```
dead-letter-queue/
├── docker-compose.yml
├── kyc-service/                        # Spring Boot — pipeline + classification + retry + DLQ producer
│   └── src/main/java/com/vaibhav/dlq/kycservice/
│       ├── controller/                  # DocumentUploadController (simulate= failure injection)
│       ├── service/                     # KycPipelineService, KycEventPublisher
│       ├── consumer/                    # KycDocumentUploadedConsumer — retry + DLQ routing
│       └── exception/                   # TransientFailureException, PermanentFailureException
└── kyc-dlq-processor/                  # Spring Boot — DLQ consumer + replay + manual review
    ├── src/main/java/com/vaibhav/dlq/kycprocessor/
    │   ├── domain/                      # KycManualReview, KycDlqPendingReplay
    │   ├── consumer/                    # KycDlqConsumer, KycDlqReplayPoller
    │   ├── service/                     # DlqReplayService, ManualReviewAlertService
    │   ├── controller/                  # KycDlqController — the Replay API
    │   └── metrics/                     # DlqMetrics — manual_review.pending gauge
    └── src/main/resources/db/migration/ # Flyway: kyc_manual_review, kyc_dlq_pending_replay
```

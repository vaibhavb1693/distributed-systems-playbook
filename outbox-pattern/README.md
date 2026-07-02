# Outbox Pattern — FinFlow User Onboarding Service

**Problem:** A service needs to write to a database AND publish an event to Kafka atomically.
Without the outbox, a crash between the two leaves your system inconsistent — the DB reflects a
state that no downstream consumer ever heard about.

**Why this way:** Store the event in an `outbox_events` table inside the same DB transaction as
your domain write. A separate process then reliably publishes it to Kafka. You get atomicity from
the DB (not Kafka), which is where you already have a transaction boundary.

**How to run (2 commands):**
```bash
# Approach A — Polling-based outbox
docker-compose --profile polling up

# Approach B — Debezium CDC (Kafka Connect watches the Postgres WAL)
docker-compose --profile cdc up
```
Both approaches publish to the same Kafka topics and are consumed by the same `event-consumers`
service. Switch between them by changing the profile — no code changes needed.

---

## The Problem in Detail

When a user registers on FinFlow, three things must happen:
1. Save the user to Postgres
2. Send a welcome email (notification-service)
3. Write an immutable audit record (audit-service, regulatory requirement)

The naive approach is to write to Postgres, then call Kafka directly:

```
// Dangerous: not atomic
userRepository.save(user);           // ✅ succeeds
kafkaTemplate.send("user.events");   // 💥 app crashes here
// audit-service never hears about this user — compliance violation
```

The outbox pattern eliminates this race by making the Kafka publish a consequence of the DB write,
not a separate operation.

---

## How It Works

```
┌─────────────────────────────────────────────────────────────────────┐
│                         user-service                                │
│                                                                     │
│  POST /api/users/register                                           │
│         │                                                           │
│         ▼                                                           │
│  ┌─────────────────────────────────────────────┐                   │
│  │         Single DB Transaction               │                   │
│  │                                             │                   │
│  │   INSERT INTO users (...)                   │                   │
│  │   INSERT INTO outbox_events (              │                   │
│  │     event_type  = 'USER_REGISTERED',        │                   │
│  │     topic_name  = 'finflow.user.registered',│                   │
│  │     payload     = '{...}',                  │                   │
│  │     status      = 'PENDING'                 │                   │
│  │   )                                         │                   │
│  │                                             │                   │
│  │   COMMIT ──────────────────────────────────►│ atomicity         │
│  └─────────────────────────────────────────────┘  guaranteed       │
│                                                                     │
│  [Approach A]              [Approach B]                             │
│  OutboxPoller              Debezium                                 │
│  @Scheduled/500ms          watches WAL                              │
│       │                        │                                    │
└───────┼────────────────────────┼────────────────────────────────────┘
        │                        │
        ▼                        ▼
   ┌─────────────────────────────────┐
   │            Kafka                │
   │  finflow.user.registered        │
   │  finflow.user.profile.updated   │
   │  finflow.user.kyc.updated       │
   └──────────┬──────────────────────┘
              │
    ┌─────────┴──────────┐
    ▼                    ▼
notification-service  audit-service
(welcome email/SMS)   (compliance log)
```

---

## Approach A: Polling-based Outbox

The `OutboxPoller` runs on a 500ms fixed-delay schedule inside `user-service`.

**Poll cycle:**
```
1. SELECT * FROM outbox_events WHERE status = 'PENDING'
   ORDER BY created_at ASC LIMIT 10
   FOR UPDATE SKIP LOCKED          ← safe for clustered deployments

2. UPDATE status = 'PROCESSING'    ← claim the batch, commit transaction
                                     (lock released; other instances skip these rows)

3. kafkaTemplate.send(...)         ← publish each event synchronously (5s timeout)

4a. Success → UPDATE status = 'PUBLISHED', published_at = NOW()
4b. Failure → UPDATE status = 'PENDING', retry_count++
              (retried next cycle; failed after 5 attempts → FAILED)
```

**Stuck-event recovery:** Events stuck in `PROCESSING` for > 5 minutes (app crash between steps 2
and 4) are automatically reset to `PENDING` at the start of each poll cycle.

**Outbox event states:**
```
PENDING → PROCESSING → PUBLISHED
                    ↘ PENDING (retry)
                    ↘ FAILED  (max retries exceeded)
```

**Key config (`application.yml`):**
```yaml
outbox:
  poller:
    enabled: true          # set to false in CDC profile
    interval-ms: 500       # poll every 500ms
    batch-size: 10         # events per cycle
    publish-timeout-seconds: 5
    max-retry-count: 5
```

---

## Approach B: Debezium CDC (Change Data Capture)

Debezium connects to Postgres as a replication client and tails the Write-Ahead Log (WAL).
Every `INSERT` into `outbox_events` is captured at the WAL level and streamed to Kafka via
Kafka Connect — no polling, no DB query overhead.

```
Postgres WAL
    │
    ▼
Debezium PostgresConnector
    │  (reads replication slot: finflow_outbox_slot)
    ▼
Outbox Event Router SMT
    │  routes by 'topic_name' column → finflow.user.registered
    ▼
Kafka topic
```

The `OutboxPoller` bean is conditionally disabled in the `cdc` Spring profile:
```yaml
# application-cdc.yml
outbox:
  poller:
    enabled: false
```

The Debezium connector is registered automatically by the `init-connector` service in
`docker-compose.yml` once Kafka Connect is healthy — no manual steps.

**Key connector settings (`debezium/connector-config.json`):**
```json
"transforms.outbox.route.by.field": "topic_name",
"transforms.outbox.route.topic.replacement": "${routedByValue}",
"transforms.outbox.expand.json.payload": "true"
```
The `topic_name` column in `outbox_events` stores the exact Kafka topic
(`finflow.user.registered`). Debezium routes directly to it — no mapping logic needed.

---

## Polling vs CDC — Side-by-Side Comparison

| | Polling (Approach A) | CDC / Debezium (Approach B) |
|---|---|---|
| **How it works** | App queries DB on a timer | DB streams WAL changes to Kafka Connect |
| **Latency** | Bounded by poll interval (500ms default) | Near real-time (< 100ms typical) |
| **DB load** | Adds read queries on every poll cycle | Zero app-side query overhead |
| **Infrastructure** | None — just the app + DB + Kafka | Kafka Connect + Debezium connector |
| **Operational complexity** | Low — easy to debug, no extra moving parts | Higher — connector config, replication slots, schema sensitivity |
| **Status tracking** | PENDING → PROCESSING → PUBLISHED in DB | Not tracked by Debezium (WAL is source of truth) |
| **Clustered safety** | `FOR UPDATE SKIP LOCKED` prevents double-publish | Debezium is single-writer by design |
| **Schema change impact** | No impact | Outbox table schema changes need connector reconfiguration |
| **Best for** | Low-to-medium throughput; simple infra; easier ops | High throughput; latency-sensitive; already using Kafka Connect |

---

## Failure Scenarios

| Scenario | What happens |
|---|---|
| App crashes after DB commit, before Kafka publish | Event stays `PENDING` → published on next poll (Approach A) or by Debezium on restart (Approach B) |
| Kafka is down | Approach A: publish fails → event stays `PENDING`, retried. Approach B: Debezium buffers in Connect offsets, publishes when Kafka recovers |
| App crashes mid-batch (PROCESSING state) | Stuck-event cleanup resets to `PENDING` after 5 minutes |
| Duplicate event delivered to consumer | Consumers must be idempotent (see `idempotent-consumer` pattern in this repo) |
| Debezium replication slot fills up (B only) | Postgres WAL grows until slot is consumed — monitor `pg_replication_slots` lag |

---

## When to Use the Outbox Pattern

✅ You need guaranteed event delivery without distributed transactions (2PC)
✅ Your service owns a relational DB and Kafka — no external event store
✅ You can tolerate at-least-once delivery (consumers must handle duplicates)
✅ Audit/compliance requires every state change to produce a traceable event

## When NOT to Use It

❌ You need exactly-once delivery end-to-end — the outbox gives at-least-once; you'd need Kafka transactions + idempotent consumers for exactly-once
❌ You're already using event sourcing — your event log IS your state; no separate outbox needed
❌ You need sub-10ms event latency — even Debezium CDC has ~50–100ms overhead; use Kafka Streams or in-process publishing instead
❌ Your DB doesn't support transactions — outbox relies on atomicity; doesn't work with DynamoDB, Cassandra, etc.

---

## Project Structure

Shared Prometheus + Grafana infra lives one level up, at `../observability/` — it's reused
across all patterns in this repo, not just this one.

```
outbox-pattern/
├── docker-compose.yml              # Full stack — polling and CDC profiles
├── debezium/
│   └── connector-config.json       # Debezium outbox event router config
├── user-service/                   # Spring Boot — domain writes + outbox events
│   ├── src/main/java/com/vaibhav/outbox/userservice/
│   │   ├── domain/                 # User, OutboxEvent, enums
│   │   ├── repository/             # JPA repos with FOR UPDATE SKIP LOCKED query
│   │   ├── service/                # UserService (transactional writes), OutboxEventService
│   │   ├── polling/                # OutboxPoller — Approach A
│   │   ├── metrics/                # OutboxBacklogMetrics — polling backlog gauge
│   │   ├── controller/             # REST endpoints
│   │   └── config/                 # Kafka topic declarations
│   └── src/main/resources/
│       ├── application.yml         # Default config (polling enabled)
│       ├── application-cdc.yml     # CDC profile (polling disabled)
│       └── db/migration/           # Flyway migrations (users + outbox_events tables)
├── event-consumers/                # Spring Boot — notification + audit consumers
│   └── src/main/java/com/vaibhav/outbox/consumers/
│       ├── consumer/               # NotificationConsumer, AuditConsumer
│       ├── metrics/                # OutboxLatencyMetricsConsumer — polling-vs-CDC latency
│       └── model/                  # UserEvent (flexible payload model)
└── load-tests/                     # Gatling (Java DSL) load simulation
    └── src/test/java/com/vaibhav/outbox/loadtest/
        └── OutboxRegistrationSimulation.java
```

---

## API Reference

```bash
# Register a new user (triggers USER_REGISTERED outbox event)
curl -X POST http://localhost:8081/api/users/register \
  -H "Content-Type: application/json" \
  -d '{"name": "Vaibhav Bhatt", "email": "vaibhav@finflow.com", "phone": "+91-9876543210"}'

# Update profile (triggers PROFILE_UPDATED outbox event)
curl -X PUT http://localhost:8081/api/users/{userId}/profile \
  -H "Content-Type: application/json" \
  -d '{"name": "Vaibhav B", "phone": "+91-9000000000"}'

# Update KYC status (triggers KYC_STATUS_UPDATED outbox event)
curl -X PUT http://localhost:8081/api/users/{userId}/kyc-status \
  -H "Content-Type: application/json" \
  -d '{"status": "VERIFIED"}'
```

Watch the `event-consumers` logs to see notification and audit events being processed in real time.

---

## Observability — Comparing Polling vs CDC Latency

The comparison table above is a claim. This section makes it measurable: every outbox event is
tagged with how it was published, so polling and CDC can be graphed side by side under identical
load, on a shared Prometheus + Grafana stack (`../observability/`, reused by every pattern in this
repo).

**What's measured and how:** `UserService.createOutboxEvent()` injects two fields into the
payload JSON before it's ever written to `outbox_events.payload` — `eventCreatedAtMs` (epoch
millis) and `mode` (`polling` or `cdc`, from the `outbox.mode` property). Because the poller
sends `payload` verbatim and Debezium's `expand.json.payload` flattens the same JSON's top-level
keys into the outgoing Kafka message, both approaches carry these two fields identically — no
Debezium config branching needed. A dedicated `OutboxLatencyMetricsConsumer` in `event-consumers`
(its own Kafka consumer group, `finflow-metrics-service`, so it doesn't compete with
`NotificationConsumer`/`AuditConsumer`) reads them on receipt and records a Micrometer `Timer`
(`outbox.event.latency`, tagged `mode` + `topic`) — the event-created-in-Postgres to
event-received-by-a-consumer latency, for both approaches, on the same metric.
`OutboxBacklogMetrics` in `user-service` additionally exposes `outbox.backlog.size` (PENDING row
count) — polling-only, gated behind the same condition as `OutboxPoller`, since CDC never
transitions outbox row status and an ungated gauge would just show a misleading, ever-growing
number.

**Run it:**
```bash
# 1. Bring up the shared observability stack FIRST — it creates the network the pattern
#    stack's app containers join.
cd ../observability && docker-compose up -d

# 2. Bring up the pattern stack (polling or cdc — pick one, they share host port 8081/8082)
cd ../outbox-pattern && docker-compose --profile polling up -d --build

# 3. Open Grafana: http://localhost:3000 (admin/admin) — "Outbox Pattern — Polling vs CDC"
#    dashboard is auto-provisioned, no manual setup.

# 4. Generate load
cd load-tests && mvn gatling:test

# 5. Compare: tear down (`docker-compose --profile polling down -v`), switch to
#    `--profile cdc`, rerun the load test, and diff the same dashboard window.
```

**Dashboard panels** (`../observability/grafana/dashboards/outbox-pattern.json`):
1. **Event Latency (p50/p95) by mode** — the star panel; answers the polling-vs-CDC question directly.
2. **Event Throughput by mode** — events/sec delivered, reusing the latency Timer's own count series.
3. **Polling Backlog** — PENDING outbox row count; shows "No data" under `--profile cdc`, by design.
4. **Avg Latency by Topic & Mode** — per-event-type breakdown (registration vs profile vs KYC).

**Load profile:** the Gatling simulation (`load-tests/OutboxRegistrationSimulation.java`) ramps
1→30 requests/sec over 30s then holds 30 req/s for 90s — deliberately above the polling
approach's steady-state ceiling (`batch-size=10` every `interval-ms=500` ≈ 20 events/sec), so the
backlog panel visibly grows then drains under polling while staying flat under CDC. In a live run
this pushed the polling backlog to ~7,600 pending events with p50 latency saturating the
histogram's top bucket, while CDC sustained ~54 events/sec with no backlog concept at all —
exactly the gap this pattern exists to close.

**Known tradeoff:** Prometheus scrapes both `user-service-polling:8081` and
`user-service-cdc:8081` as separate jobs, but the two profiles share host port 8081 and can't run
concurrently — whichever one isn't currently up will show as a `DOWN` target. That's expected,
not a misconfiguration.

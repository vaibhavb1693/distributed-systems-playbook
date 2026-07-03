# Staff Engineer Deep Dive — Distributed Systems Playbook

This document covers six areas across all 5 patterns: end-to-end flow, configuration reference,
infrastructure topology, production failure modes at 10k+ RPS, metrics/observability from
collection to visualization, and the Gatling load testing setup. Written as if you are explaining
this — and could rebuild it — in a Staff/Principal interview.

**Honesty check before you read further:** as of this writing, only **outbox-pattern** has been
verified end-to-end in Docker, has a wired Grafana dashboard, and has a Gatling load test. The
other 4 patterns (circuit-breaker, idempotent-consumer, rate-limiter, dead-letter-queue) are
code-complete and unit-tested but have **not** been brought up via `docker-compose` yet, have no
Prometheus scrape job or Grafana dashboard configured, and have no load test module. Everywhere
below, this is called out explicitly rather than glossed over — see `PROGRESS.md` for the current
source of truth on what's actually been run versus what's built-but-unverified.

---

## Part 1 — End-to-End Flow (per pattern)

### 1.1 Outbox Pattern — `outbox-pattern/`

```
Client
  │  POST /api/users/register  {"name","email","phone"}
  ▼
┌────────────────────────────────────────────────────────────────────┐
│                    user-service :8081                               │
│                                                                      │
│  UserController.register()                                          │
│    └── UserService.registerUser()  @Transactional                   │
│          ├── 1. INSERT INTO users (Postgres)                        │
│          └── 2. createOutboxEvent(aggregateId, USER_REGISTERED, map)│
│                 ├── payload map enriched with 2 extra fields:       │
│                 │     eventCreatedAtMs = System.currentTimeMillis() │
│                 │     mode = ${outbox.mode}  ("polling" | "cdc")    │
│                 └── INSERT INTO outbox_events                       │
│                       topic_name = 'finflow.user.registered'        │
│                       payload    = <JSON string, incl. the 2 fields>│
│                       status     = PENDING                          │
│                                                                      │
│          Both INSERTs in ONE @Transactional — atomic. No dual-write.│
└────────────────────────────────────────────────────────────────────┘
         │
         ├──────────────────────────────┬───────────────────────────────
         │  Approach A: POLLING          │  Approach B: CDC
         ▼                                ▼
┌──────────────────────────┐   ┌──────────────────────────────────────┐
│  OutboxPoller             │   │  Debezium PostgresConnector           │
│  @Scheduled/500ms         │   │  (Kafka Connect, separate container)  │
│                            │   │                                        │
│  1. claimPendingEvents(10):│   │  Tails Postgres WAL via logical       │
│     SELECT ... WHERE       │   │  replication slot (pgoutput plugin).  │
│     status='PENDING'       │   │  Sees the INSERT into outbox_events   │
│     ORDER BY created_at    │   │  the instant it's WAL-committed —     │
│     LIMIT 10                │   │  no polling, no query load on PG.     │
│     FOR UPDATE SKIP LOCKED  │   │                                        │
│     → mark PROCESSING       │   │  Outbox EventRouter SMT:              │
│                            │   │    routes by topic_name column        │
│  2. kafkaTemplate.send()   │   │    table.expand.json.payload=true     │
│     per event, 5s timeout   │   │    flattens the payload JSON's        │
│                            │   │    top-level keys into the outgoing   │
│  3a. success → PUBLISHED   │   │    Kafka message (so eventCreatedAtMs │
│  3b. failure → back to     │   │    and mode land in the message the   │
│      PENDING, retryCount++  │   │    same way as the polling path)      │
│      (FAILED after 5x)      │   │                                        │
└──────────────────────────┘   └──────────────────────────────────────┘
         │                                │
         └────────────────┬───────────────┘
                           ▼
              Kafka topic: finflow.user.registered
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
┌───────────────┐  ┌───────────────┐  ┌──────────────────────────┐
│Notification    │  │Audit          │  │OutboxLatencyMetrics       │
│Consumer         │  │Consumer       │  │Consumer                   │
│(group:          │  │(group:        │  │(group: finflow-metrics-   │
│ finflow-         │  │ finflow-      │  │  service — INDEPENDENT    │
│ notification-    │  │ audit-        │  │  copy of every message)   │
│ service)         │  │ service)      │  │                            │
│                  │  │               │  │ reads eventCreatedAtMs +   │
│ logs simulated   │  │ logs immutable│  │ mode from payload, records │
│ email/SMS         │  │ audit record  │  │ Micrometer Timer           │
│                  │  │ w/ Kafka       │  │ outbox.event.latency        │
│                  │  │ metadata       │  │ tagged mode+topic           │
└───────────────┘  └───────────────┘  └──────────────────────────┘
```

**Why 3 separate consumer groups on the same 3 topics:** each consumer group gets its own
independent copy of every message (that's how Kafka consumer groups work — partitions are only
shared *within* a group). Putting the latency-metric recording inside `NotificationConsumer`
instead would still work correctness-wise, but conflates "business consumer" with "observability
consumer" — a dedicated group keeps the metric recording from ever being skipped by a bug in
notification logic, and vice versa.

**The one field that makes CDC and polling comparable:** `UserService.createOutboxEvent()` is
the *only* place `eventCreatedAtMs`/`mode` get injected — a single-point change. Because Debezium
just reads the `payload` TEXT column and flattens whatever JSON is in it, and the poller sends
`payload` verbatim, both delivery paths carry the same two fields with zero Debezium-side config
for it.

---

### 1.2 Circuit Breaker — `circuit-breaker/`

```
Client
  │  POST /api/transactions  {"payerId","payeeId","amount"}
  ▼
┌────────────────────────────────────────────────────────────────┐
│                 transaction-service :8091                        │
│                                                                    │
│  TransactionController.create()                                   │
│    └── TransactionService.processTransaction()                   │
│          └── FraudDetectionClient.assessRisk(request)             │
│                (interface; impl = FraudDetectionClientImpl)       │
│                                                                    │
│                AOP proxy chain around assessRisk(), Resilience4j's│
│                documented default composition order:               │
│                                                                    │
│                @Retry(outer)                                      │
│                  → @CircuitBreaker                                 │
│                      → @Bulkhead(inner, closest to the real call)  │
│                          → restTemplate.postForObject(             │
│                              fraudServiceUrl + "/api/fraud/score") │
└──────────────────────┬─────────────────────────────────────────┘
                        │  synchronous HTTP call
                        ▼
┌────────────────────────────────────────────────────────────────┐
│               fraud-detection-service :8092                       │
│                                                                    │
│  ScoreController.score()                                          │
│    checks DegradeState.mode first:                                 │
│      NONE    → RiskScoringService.score(amount)                   │
│                  amount >100,000 → HIGH                            │
│                  amount > 10,000 → MEDIUM                          │
│                  else             → LOW                            │
│      LATENCY → Thread.sleep(latencyMs), then score normally        │
│      ERROR   → HTTP 500                                            │
│      DOWN    → HTTP 503                                            │
│                                                                    │
│  DegradeState flipped via AdminController                          │
│    POST /admin/degrade?mode=latency|error|down|reset               │
│    (@Profile("!prod") — never exists in a real prod deployment)    │
└────────────────────────────────────────────────────────────────┘

Back in transaction-service, three outcomes reach TransactionService:

1. NORMAL CALL SUCCEEDS
   RiskAssessment(riskLevel, flaggedForReview=false)
   riskLevel HIGH → status BLOCKED, else → status APPROVED

2. CIRCUIT CLOSED, CALL FAILS (error/timeout/slow), RETRIES EXHAUSTED,
   CIRCUIT STILL CLOSED OR NOW OPEN
   → CircuitBreaker's fallbackMethod fires:
     fallbackAssessRisk(request, throwable)
       returns RiskAssessment(MEDIUM, flaggedForReview=true)
   → status APPROVED (fail-open), but flagged for human review

3. CIRCUIT OPEN (already tripped from prior failures)
   → CallNotPermittedException thrown INSTANTLY, no network call at all
   → same fallbackMethod fires → same fail-open MEDIUM+flagged result
```

**State machine driving outcome 2 vs 3** (`fraudCircuitBreaker` instance):
```
CLOSED ──(of last 10 calls, ≥50% failed OR ≥50% took >2s)──► OPEN
   (only evaluated once ≥5 calls have happened — minimumNumberOfCalls)

OPEN ──(30s elapses, automaticTransitionFromOpenToHalfOpenEnabled)──► HALF_OPEN

HALF_OPEN ──(3 permitted probe calls all succeed)──► CLOSED
HALF_OPEN ──(a probe call fails)──► OPEN (30s timer restarts)
```

`CircuitBreakerEventLogger` subscribes to `CircuitBreakerRegistry.circuitBreaker("fraudCircuitBreaker").getEventPublisher()`
and logs every `onStateTransition` / `onCallNotPermitted` / `onError` / `onSuccess` /
`onSlowCallRateExceeded` / `onFailureRateExceeded` event as one structured JSON log line.

---

### 1.3 Idempotent Consumer — `idempotent-consumer/`

```
Client
  │  POST /api/loans/{loanId}/submit-application?times=N
  │  POST /api/loans/{loanId}/kyc-verified?expectedVersion=V&times=N
  │  POST /api/loans/{loanId}/credit-score?times=N
  │  POST /api/loans/{loanId}/decision?times=N
  ▼
┌──────────────────────────────────────────────────────────────┐
│            loan-application-service :8101 (producer only)      │
│                                                                  │
│  LoanEventController → LoanEventPublisher                       │
│    builds a payload Map, serializes once, then sends the        │
│    IDENTICAL json string `times` times via kafkaTemplate.send   │
│    (key = loanId, so all events for one loan hit the same       │
│     partition and stay ordered)                                 │
│                                                                  │
│  `times>1` is the entire redelivery-simulation mechanism —      │
│  no Kafka replay tooling needed to demo dedup.                  │
└──────────────────────────────────────────────────────────────┘
         │
         ▼  4 topics, one per lifecycle event
┌──────────────────────────────────────────────────────────────────────┐
│                loan-processing-service :8102 (the consumer)            │
│                                                                          │
│  finflow.loan.application.submitted → LoanApplicationSubmittedConsumer  │
│    STRATEGY 1: Redis TTL dedup                                         │
│      redisTemplate.opsForValue().setIfAbsent(                          │
│          "dedup:loan-application-submitted:"+eventId, "1", 24h)        │
│      true  (we won the SET NX race) → INSERT loan_applications          │
│                (id = loanId, manually assigned, not @GeneratedValue)    │
│      false (key already existed)    → log + skip, no DB write           │
│                                                                          │
│  finflow.kyc.document.verified → KycDocumentVerifiedConsumer            │
│    STRATEGY 3: optimistic lock / version compare-and-swap                │
│      UPDATE loan_applications SET kyc_verified=true, version=version+1  │
│      WHERE id=:loanId AND version=:expectedVersion                      │
│      rows updated = 1 → processed;  rows updated = 0 → stale/duplicate  │
│      (direct JPQL bulk update, NOT JPA's load-then-save @Version flow)  │
│                                                                          │
│  finflow.credit.score.received → CreditScoreReceivedConsumer            │
│    STRATEGY 2: DB unique constraint                                     │
│      native INSERT INTO processed_credit_events (event_id PK, ...)     │
│      succeeds → UPDATE loan_applications SET credit_score=?             │
│      DataIntegrityViolationException (PK clash) → catch, skip           │
│                                                                          │
│  finflow.loan.decision.made → LoanDecisionMadeConsumer                  │
│    STRATEGY 4: natural idempotency via upsert                           │
│      native INSERT INTO loan_decisions (loan_id PK, decision, ...)     │
│      ON CONFLICT (loan_id) DO UPDATE SET decision=EXCLUDED.decision     │
│      — applying this N times always converges to the same row          │
└──────────────────────────────────────────────────────────────────────┘
```

**Why `save()` doesn't work for Strategies 2 and 4:** `JpaRepository.save()` on an entity with a
manually-assigned `@Id` (no `@GeneratedValue`) calls `EntityManager.merge()`, which does a SELECT
to check existence first, then an INSERT or UPDATE. A duplicate `event_id` would silently succeed
as an UPDATE instead of raising the `DataIntegrityViolationException` Strategy 2's whole dedup
mechanism depends on. Both repositories instead expose one native `@Modifying @Query` method
(`insertOrThrow`, `upsert`) that always attempts a raw `INSERT`.

---

### 1.4 Redis Rate Limiter — `rate-limiter/`

```
Client
  │  GET /ob/{algorithm}/accounts
  │  Header: X-Partner-Id: partner-free-1
  ▼
┌──────────────────────────────────────────────────────────────────┐
│                  open-banking-api :8111                             │
│                                                                       │
│  RateLimitInterceptor.preHandle()   ← runs BEFORE any controller     │
│                                                                       │
│  1. partnerId = header("X-Partner-Id")           (400 if missing)    │
│  2. algorithm = regex "^/ob/([a-z-]+)/.*$" on the URI                │
│                 (400 if not one of the 5 known algorithm names)      │
│  3. tier = PartnerRegistry.tierFor(partnerId)                        │
│            (unregistered partnerId → defaults to FREE)               │
│  4. config = PartnerRegistry.configFor(tier)                         │
│              (limit / windowSeconds / burstCapacity from application.yml)│
│  5. rateLimiter = RateLimiterRegistry.find(algorithm)                 │
│  6. result = rateLimiter.tryConsume(partnerId, config)                 │
│              ↓                                                        │
│         ONE atomic Redis Lua script — the entire read-check-write    │
│         happens as a single Redis command, so concurrent requests    │
│         from the same partner can't race each other                  │
│              ↓                                                        │
│  7. sets X-RateLimit-Limit / -Remaining / -Reset headers on response │
│  8. NOT allowed → 429 + Retry-After header, controller NEVER runs     │
│     allowed     → request proceeds to OpenBankingController,         │
│                    which just returns 3 canned account IDs            │
│                    (it has zero knowledge of rate limiting —          │
│                     all of that happened in the interceptor)          │
└──────────────────────────────────────────────────────────────────┘
```

**The 5 Lua scripts** (`src/main/resources/scripts/*.lua`), one Redis key pattern each:

| Algorithm | Redis key(s) | Mechanics |
|---|---|---|
| Fixed window | `rl:fixed:{partner}:{windowBucket}` | `INCR` the bucket key, `EXPIRE` on first increment. `count > limit` → reject. |
| Sliding log | `rl:sliding-log:{partner}` (ZSET) | `ZREMRANGEBYSCORE` to drop entries older than the window, `ZCARD` to count what's left, `ZADD` if under limit. |
| Sliding counter | `rl:sliding-counter:{partner}:{cur}` + `:{prev}` | `weightedCount = prevCount*(1-elapsedFraction) + currCount`; `INCR` current bucket if under limit. |
| Token bucket | `rl:token-bucket:{partner}` (HASH: tokens, lastRefill) | Refill `tokens += elapsedSeconds * refillRate` (capped at capacity) on every call, then spend 1 if ≥1 available. |
| Leaky bucket | `rl:leaky-bucket:{partner}` (HASH: level, lastLeak) | Drain `level -= elapsedSeconds * leakRate` (floored at 0) on every call, then admit if `level < capacity`. |

Every algorithm is checked against the **same** `RateLimitConfig` for a partner's tier — the
algorithm changes how the limit is enforced, not what the nominal limit is. That's deliberate: it
makes `/ob/fixed/accounts` and `/ob/token-bucket/accounts` for `partner-pro-1` a genuine
apples-to-apples comparison of algorithm behavior under the same load.

---

### 1.5 Dead Letter Queue — `dead-letter-queue/`

```
Client
  │  POST /api/kyc/documents/upload
  │  {"userId","documentType","simulate": "NONE"|"VENDOR_TIMEOUT"|"DB_ERROR"|
  │                                        "CORRUPTED"|"UNSUPPORTED_FORMAT"|
  │                                        "FRAUD_FLAG"|"OCR_FAILURE"}
  ▼
┌──────────────────────────────────────────────────────────────────┐
│                     kyc-service :8121                               │
│                                                                       │
│  DocumentUploadController → KycEventPublisher                        │
│    generates documentId = UUID.randomUUID()                          │
│    publishes finflow.kyc.document.uploaded                            │
│    (key = documentId)                                                │
└──────────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────┐
│  KycDocumentUploadedConsumer.onDocumentUploaded()  → processWithRetries│
│                                                                          │
│  loop (attempt = 0):                                                    │
│    try KycPipelineService.process(event)                                │
│      switch(event.simulate()):                                          │
│        NONE              → returns normally → SUCCESS, loop exits       │
│        VENDOR_TIMEOUT     → throws TransientFailureException             │
│        DB_ERROR           → throws TransientFailureException             │
│        CORRUPTED          → throws PermanentFailureException             │
│        UNSUPPORTED_FORMAT → throws PermanentFailureException             │
│        OCR_FAILURE        → throws PermanentFailureException             │
│        FRAUD_FLAG         → throws PermanentFailureException             │
│                                                                          │
│    catch TransientFailureException:                                     │
│      attempt < 3?  → backoffSleeper.accept(BACKOFF_MS[attempt])         │
│                       BACKOFF_MS = {1000, 2000, 4000}                    │
│                       attempt++, loop again  (BLOCKING — Thread.sleep    │
│                       on the Kafka listener thread itself)               │
│      attempt >= 3? → publishToDlq(TRANSIENT, reasonCode, ..., attempt=3) │
│                                                                          │
│    catch PermanentFailureException:                                     │
│      → publishToDlq(PERMANENT, reasonCode, ..., attempt=0)  IMMEDIATELY │
│        (no retry loop iterations at all)                                │
└──────────────────────────────────────────────────────────────────────┘
         │  finflow.kyc.document.dlq  +  6 headers:
         │  X-Failure-Reason, X-Failure-Type, X-Retry-Count,
         │  X-Original-Topic, X-Failed-At, X-Document-Id
         ▼
┌────────────────────────────────────────────────────────────────────┐
│                  kyc-dlq-processor :8122                              │
│                                                                         │
│  KycDlqConsumer.onDlqMessage()                                         │
│    reads X-Failure-Type header — does NOT re-run any pipeline logic    │
│                                                                         │
│    TRANSIENT →  INSERT kyc_dlq_pending_replay                          │
│                  (document_id, payload, replay_at = now+5min,          │
│                   status = PENDING)                                    │
│                                                                         │
│    PERMANENT →  INSERT kyc_manual_review                                │
│                  (document_id, userId [parsed from payload JSON],      │
│                   failure_reason, payload, status = PENDING_REVIEW)    │
│                  → ManualReviewAlertService.checkAndAlertIfThresholdExceeded()│
│                     countByStatus(PENDING_REVIEW) > 10?                 │
│                       → logs one structured JSON alert line             │
│                         (simulates PagerDuty/Slack webhook)             │
└────────────────────────────────────────────────────────────────────┘
         │
         ▼  @Scheduled every 30s (kyc.dlq.replay-poll-interval-ms)
┌────────────────────────────────────────────────────────────────────┐
│  KycDlqReplayPoller.pollAndReplay()   (mirrors outbox-pattern's       │
│                                          OutboxPoller structurally)     │
│                                                                         │
│  SELECT * FROM kyc_dlq_pending_replay                                  │
│  WHERE status='PENDING' AND replay_at <= now()                         │
│  ORDER BY replay_at ASC LIMIT 20                                       │
│                                                                         │
│  for each due row:                                                      │
│    kafkaTemplate.send(finflow.kyc.document.uploaded, documentId,       │
│                        payload).get(5, SECONDS)                        │
│    success → status = REPLAYED                                         │
│    failure → status = FAILED                                          │
│                                                                         │
│  → republished event re-enters kyc-service's normal pipeline           │
└────────────────────────────────────────────────────────────────────┘

Separately, the Replay API (/dlq/kyc/*) lets a human:
  POST /replay/{documentId}       — checks manual_review first, then pending_replay
  POST /replay/bulk?failureType=  — TRANSIENT bypasses the 5-min cooldown entirely
  GET  /messages?status=&failureReason=  — paginated list
  PATCH /messages/{id}/resolve    — status=RESOLVED, resolutionNote, resolvedBy
  GET  /stats                     — totals, replay success rate, pending count
```

---

## Part 2 — Configuration Reference

### 2.1 Kafka Producer Config
Used identically in `user-service`, `loan-application-service`, `kyc-service`, `kyc-dlq-processor`:

```yaml
spring:
  kafka:
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all
      retries: 3
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
```

| Config | Value | What it controls | When to change |
|---|---|---|---|
| `acks` | `all` | Producer waits for all in-sync replicas to ack before the send future completes | Never lower for these patterns — `all` is what makes `enable.idempotence` meaningful |
| `retries` | `3` | Auto-retry on transient send failures (broker not leader yet, etc.) | Bump if brokers are flaky; `delivery.timeout.ms` bounds total retry time regardless |
| `enable.idempotence` | `true` | Producer attaches a sequence number per partition so the broker de-dupes retried sends — prevents a network retry from creating a duplicate message | Required for `acks=all` + `retries>0` to be safe; never disable |
| `max.in.flight.requests.per.connection` | `5` | Max unacked requests in flight per connection | With idempotence on, values up to 5 preserve ordering guarantees; going higher risks reordering on retry |
| `linger.ms` / `batch.size` | not set (defaults `0` / `16384`) | Batching before a send | Not tuned anywhere in this repo — first thing to touch for real throughput (see Part 4) |
| `compression.type` | not set (`none`) | Message compression | Not tuned — `lz4` or `snappy` in production |

### 2.2 Kafka Consumer Config
Used in `event-consumers`, `loan-processing-service`, `kyc-service`, `kyc-dlq-processor`:

```yaml
spring:
  kafka:
    consumer:
      group-id: <service-name>
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      enable-auto-commit: false
    listener:
      ack-mode: RECORD
```

| Config | Value | What it controls | When to change |
|---|---|---|---|
| `auto-offset-reset` | `earliest` | On first start with no committed offset, replay from the beginning | Fine for demo/dev; in prod you'd usually want `latest` for a brand-new consumer group so you don't replay history on every new deployment of a *new* group |
| `enable-auto-commit` | `false` | Spring Kafka commits offsets manually, after the listener method returns | Never set `true` — you'd lose at-least-once guarantees (offset could commit before processing finishes) |
| `ack-mode: RECORD` | — | Commits the offset after **each record** is processed, not each batch | `BATCH` (commit once per poll batch) is faster but widens the redelivery window on crash; `RECORD` was chosen everywhere here for the tightest at-least-once window |
| `listener.concurrency` | **not set anywhere** (defaults to 1 thread per `@KafkaListener`) | Number of consumer threads per listener instance | This is the single biggest un-tuned throughput knob in the whole repo — see Part 4 failure modes |
| `max.poll.records` | not set (default `500`) | Max records per `poll()` call | Not tuned; matters once processing-per-record is slow (DLQ's blocking retry, for instance) |

### 2.3 Postgres / Flyway / HikariCP
Used in `user-service`, `loan-processing-service`, `kyc-dlq-processor` — identical shape each time:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://<host>:<port>/finflow
    hikari:
      maximum-pool-size: 10
      minimum-idle: 2
  jpa:
    hibernate:
      ddl-auto: validate
  flyway:
    enabled: true
    locations: classpath:db/migration
```

| Config | Value | What it controls | When to change |
|---|---|---|---|
| `hikari.maximum-pool-size` | `10` | Max concurrent DB connections per service instance | This is a real ceiling at load — see F-series failure modes in Part 4. A single 10-connection pool caps you well under 1,000 concurrent in-flight queries |
| `hikari.minimum-idle` | `2` | Connections kept warm even when idle | Low for a demo; production would tune this closer to expected steady-state concurrency to avoid connection-creation latency spikes |
| `ddl-auto` | `validate` | Hibernate checks the schema matches entities but never mutates it | This is the correct production value — schema changes are Flyway migrations only, never Hibernate auto-DDL |
| `flyway.locations` | `classpath:db/migration` | Where `V1__*.sql`, `V2__*.sql` etc. are found | Standard Flyway convention, unchanged across all 3 services |

**Important gotcha discovered and fixed while building this repo:** `flyway-database-postgresql`
is **not** a dependency in any `pom.xml` here — only `flyway-core`. Spring Boot 3.2.0's managed
Flyway version is 9.x, where Postgres support ships inside `flyway-core` itself; the separate
`flyway-database-postgresql` artifact only exists from Flyway 10 (Spring Boot 3.3+). Adding it
unversioned (as a first draft of this repo did) breaks the build with "version missing" — worth
knowing if you ever bump the Spring Boot parent version, since you'd need to add it back then.

### 2.4 Resilience4j (Circuit Breaker + Retry + Bulkhead)
`transaction-service/application.yml`, the single most config-dense file in the repo:

```yaml
resilience4j:
  circuitbreaker:
    instances:
      fraudCircuitBreaker:
        registerHealthIndicator: true
        slidingWindowType: COUNT_BASED
        slidingWindowSize: 10
        minimumNumberOfCalls: 5
        failureRateThreshold: 50
        slowCallDurationThreshold: 2s
        slowCallRateThreshold: 50
        waitDurationInOpenState: 30s
        permittedNumberOfCallsInHalfOpenState: 3
        automaticTransitionFromOpenToHalfOpenEnabled: true
  retry:
    instances:
      fraudCircuitBreaker:
        maxAttempts: 3
        waitDuration: 200ms
        retryExceptions:
          - java.io.IOException
          - org.springframework.web.client.ResourceAccessException
          - org.springframework.web.client.HttpServerErrorException
  bulkhead:
    instances:
      fraudCircuitBreaker:
        maxConcurrentCalls: 10
        maxWaitDuration: 0
```

| Config | Value | What it controls | When to change |
|---|---|---|---|
| `slidingWindowType` | `COUNT_BASED` | The failure-rate window is "last N calls" not "last N seconds" | `TIME_BASED` is often better in production for consistent behavior under bursty traffic — count-based windows can be dominated by a single burst |
| `slidingWindowSize` | `10` | N in "last N calls" | Small on purpose for a fast demo — production circuit breakers typically use 50-100 for statistical stability |
| `minimumNumberOfCalls` | `5` | Circuit won't evaluate failure rate until at least 5 calls have happened | Prevents 1-2 unlucky calls from tripping the circuit on a cold start |
| `failureRateThreshold` | `50` (%) | ≥50% of the window failing → circuit opens | Lower = more sensitive/defensive; higher = more tolerant of a flaky dependency |
| `slowCallDurationThreshold` | `2s` | A call taking longer than this counts as "slow" (separate from "failed") | Tune to your actual p99 SLA — should be well above normal latency, well below what you consider unacceptable |
| `slowCallRateThreshold` | `50` (%) | ≥50% slow calls → circuit opens, independent of the failure-rate check | This is what makes the circuit breaker catch *degradation*, not just outright errors |
| `waitDurationInOpenState` | `30s` | How long the circuit stays OPEN before allowing probe calls | Should roughly match how long your downstream typically takes to recover from a blip |
| `permittedNumberOfCallsInHalfOpenState` | `3` | Probe calls allowed through in HALF_OPEN before deciding CLOSED/OPEN | Too low = flaky decision based on 1 lucky/unlucky call; too high = slow recovery detection |
| `registerHealthIndicator` | `true` | Exposes circuit state at `/actuator/health` under a `circuitBreakers` component | Needed for the `management.health.circuitbreakers.enabled: true` line below it to actually surface anything |
| retry `maxAttempts` | `3` | Total attempts including the first (so 2 retries) | — |
| retry `waitDuration` | `200ms` | Fixed delay between retries (no backoff multiplier configured) | A production system would likely add `enableExponentialBackoff: true` here |
| retry `retryExceptions` | `IOException`, `ResourceAccessException`, `HttpServerErrorException` | **Only** these exception types trigger a retry — everything else (e.g. a 4xx from `HttpClientErrorException`) fails immediately | This allow-list is deliberate: retrying a 400 Bad Request would never succeed and just wastes time before the circuit breaker/fallback kicks in |
| bulkhead `maxConcurrentCalls` | `10` | Max simultaneous in-flight calls to the fraud service, regardless of circuit state | This is a **semaphore** bulkhead (the `resilience4j-spring-boot3` `@Bulkhead` annotation default), not a thread-pool bulkhead — it limits concurrency via a semaphore permit, it does not isolate the calls onto a separate thread pool |
| bulkhead `maxWaitDuration` | `0` | How long a call waits for a free permit before rejecting | `0` = fail immediately if all 10 permits are taken, no queueing at all |

**Composition order:** all three annotations (`@Retry`, `@CircuitBreaker`, `@Bulkhead`) target the
same instance name `fraudCircuitBreaker` on one method. No `*AspectOrder` properties are set
anywhere, so Resilience4j's documented default applies: `Retry(outermost) → CircuitBreaker →
Bulkhead(innermost, closest to the real call)`. A `CallNotPermittedException` from an OPEN
circuit is a local, instant rejection (no network round-trip), so Retry wrapping CircuitBreaker
doesn't waste real work even though "retry outside circuit breaker" sounds backwards at first.

### 2.5 Debezium Connector Config (`outbox-pattern/debezium/connector-config.json`)

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "table.include.list": "public.outbox_events",
  "plugin.name": "pgoutput",
  "slot.name": "finflow_outbox_slot",
  "publication.name": "finflow_outbox_pub",
  "publication.autocreate.mode": "filtered",
  "heartbeat.interval.ms": "5000",
  "transforms": "outbox",
  "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
  "transforms.outbox.table.field.event.id": "id",
  "transforms.outbox.table.field.event.key": "aggregate_id",
  "transforms.outbox.table.field.event.type": "event_type",
  "transforms.outbox.table.field.event.payload": "payload",
  "transforms.outbox.route.by.field": "topic_name",
  "transforms.outbox.route.topic.replacement": "${routedByValue}",
  "transforms.outbox.table.expand.json.payload": "true",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "tombstones.on.delete": "false"
}
```

| Config | What it controls | Notes |
|---|---|---|
| `plugin.name: pgoutput` | Which Postgres logical decoding plugin to use | `pgoutput` is built into Postgres 10+, no extra extension install needed (the alternative, `decoderbufs`/`wal2json`, needs a compiled Postgres extension) |
| `slot.name` | Name of the logical replication slot Postgres creates | **Operationally critical**: if Debezium disconnects and never comes back, Postgres retains WAL segments forever waiting for this slot to be consumed — this is the #1 way to fill a production Postgres disk with CDC. Monitor `pg_replication_slots` |
| `publication.autocreate.mode: filtered` | Debezium creates a Postgres `PUBLICATION` scoped only to `table.include.list`, not the whole database | Keeps WAL volume down — only `outbox_events` changes are captured, not every table's |
| `transforms.outbox.route.by.field: topic_name` + `route.topic.replacement: ${routedByValue}` | The Outbox EventRouter SMT reads the `topic_name` column's value and routes the Kafka message to that literal topic name | This is why polling and CDC never need separate topic-mapping logic — the DB row itself says where it goes |
| `transforms.outbox.table.expand.json.payload: true` | Parses the `payload` TEXT column as JSON and flattens its top-level keys into the outgoing message, instead of nesting it as a string field | **The property name has a `table.` prefix that's easy to get wrong** (a first draft of this repo had `transforms.outbox.expand.json.payload` — missing `table.` — which Debezium silently ignored as an unknown property, so payloads arrived double-JSON-encoded). Every sibling property (`table.field.event.*`) uses the same `table.` prefix, which is the tell that this one should too |
| `value.converter.schemas.enable: false` | Schemaless JSON — the Kafka message body is plain JSON with no embedded Avro-style schema envelope | Matches what `event-consumers`' plain `StringDeserializer` + Jackson expects; if you flip this to `true` you'd need a schema registry and a different consumer deserializer |
| `heartbeat.interval.ms: 5000` | Debezium sends a heartbeat message to keep the replication slot's LSN advancing even when `outbox_events` has no activity | Without this, a quiet table can cause WAL to accumulate purely from *other* tables' unrelated activity, since the slot's position doesn't advance |

### 2.6 Redis Rate Limiter Config (`open-banking-api/application.yml`)

```yaml
rate-limiter:
  tiers:
    FREE:       { limit: 60,    window-seconds: 60, burst-capacity: 60 }
    PRO:        { limit: 1000,  window-seconds: 60, burst-capacity: 1000 }
    ENTERPRISE: { limit: 10000, window-seconds: 60, burst-capacity: 10500 }
  partners:
    partner-free-1: FREE
    partner-pro-1: PRO
    partner-enterprise-1: ENTERPRISE
```

Bound via `@ConfigurationProperties(prefix = "rate-limiter")` directly into a `RateLimitConfig`
**record** (Spring Boot 3.2's relaxed binding supports constructor-binding records automatically —
no `@ConstructorBinding` annotation needed for a single-constructor type).

| Config | What it controls | When to change |
|---|---|---|
| `limit` / `window-seconds` | The nominal rate — used directly by fixed/sliding-log/sliding-counter, and to derive `ratePerSecond = limit/windowSeconds` for token/leaky bucket | Change per business tier pricing |
| `burst-capacity` | Only matters for token bucket (max tokens the bucket can hold) and leaky bucket (max queue depth) | FREE/PRO have `burst-capacity == limit` (no extra burst allowance); ENTERPRISE gets `10500` vs a `10000` limit — 500 extra burst tokens, matching the "Enterprise gets burst allowance" business requirement |
| `partners` | Fixed demo partner→tier map, no onboarding DB | An unregistered `partnerId` defaults to `FREE` (`PartnerRegistry.tierFor()`) — the safe default for an unknown caller |

### 2.7 DLQ-Specific Config (`kyc-dlq-processor/application.yml`)

```yaml
kyc:
  dlq:
    replay-cooldown-minutes: 5
    replay-poll-interval-ms: 30000
    replay-batch-size: 20
    manual-review-alert-threshold: 10
```

| Config | Value | What it controls | When to change |
|---|---|---|---|
| `replay-cooldown-minutes` | `5` | How long a TRANSIENT DLQ arrival waits before auto-replay | Should roughly match your typical downstream-blip recovery time — same reasoning as the circuit breaker's `waitDurationInOpenState` |
| `replay-poll-interval-ms` | `30000` | How often `KycDlqReplayPoller` checks for due rows | This is a real throughput ceiling — see Part 4 |
| `replay-batch-size` | `20` | Max rows replayed per poll cycle | `20 rows / 30s ≈ 0.67/sec` — see Part 4 for why this matters at scale |
| `manual-review-alert-threshold` | `10` | Pending `PENDING_REVIEW` count above which the structured alert fires | Checked synchronously on every new PERMANENT insert (threshold-crossing, not polled) |

### 2.8 Actuator / Micrometer Config

| Service | `exposure.include` | Extra config |
|---|---|---|
| `user-service`, `loan-application-service`, `loan-processing-service`, `open-banking-api`, `kyc-service`, `kyc-dlq-processor` | `health,info,metrics,prometheus` | — |
| `event-consumers` | `health,info,metrics,prometheus` | `management.metrics.distribution.percentiles-histogram."[outbox.event.latency]": true` — bracket syntax required because the meter name contains dots, which YAML's relaxed binding would otherwise read as nested map levels instead of a literal key |
| `transaction-service` | `health,info,metrics,prometheus,circuitbreakers,circuitbreakerevents` | `management.health.circuitbreakers.enabled: true` — surfaces circuit state at `/actuator/health` |

None of the services set `management.tracing.*` or wire a Zipkin/OTel endpoint — unlike
`kafka-saga-patterns`, this repo has no distributed tracing configured anywhere. Metrics-only.

---

## Part 3 — Infrastructure: Docker Topology

### 3.1 The shared network trick

`observability/docker-compose.yml` is the **only** compose file that creates the network:

```yaml
networks:
  finflow-net:
    name: finflow-net     # explicit name, not compose-project-prefixed
    driver: bridge
```

Every pattern's own `docker-compose.yml` only *references* it:

```yaml
networks:
  finflow-net:
    external: true
```

...and lists it alongside `default` on exactly the app services that expose `/actuator/prometheus`:

```yaml
  user-service-polling:
    networks:
      - default        # MUST be re-listed explicitly
      - finflow-net     # lets Prometheus (in the observability stack) reach this container
```

**Why `default` must be re-listed:** Compose auto-joins every service to the project's `default`
network *unless* that service declares an explicit `networks:` key at all — the moment you add
one, you opt out of the implicit default join and must list it yourself, or the service loses
connectivity to its own Postgres/Kafka/Redis siblings in the same compose file.

**Operational consequence:** `observability/` must be brought up *before* any pattern's compose
file, or the `external: true` reference fails with "network not found." This is why every
pattern's `docker-compose.yml` has a comment reminding you of the order.

**Infra containers (Postgres, Kafka, Zookeeper, Redis) never join `finflow-net`** — they only
need to talk to their own pattern's app services on the `default` network, and there's nothing on
them for Prometheus to scrape.

### 3.2 Container Map — Outbox Pattern (both profiles)

```
                        ┌─────────────────────────┐
                        │   observability stack     │
                        │   (bring up first)          │
                        │  prometheus:9090  grafana:3000│
                        └────────────┬───────────────┘
                                     │ finflow-net (external)
        ┌────────────────────────────┼────────────────────────────┐
        │                                                          │
┌───────▼────────┐   ┌──────────────┐   ┌──────────────────────┐  │
│  postgres :5432  │   │zookeeper     │   │kafka :9092             │  │
│  wal_level=logical│   │(no host port)│   │(internal: 29092)       │  │
└───────┬────────┘   └──────────────┘   └──────────┬─────────────┘  │
        │                                             │              │
        │           ── profile: polling ──            │              │
        │           user-service-polling :8081 ────────┤ produces to  │
        │                                             │              │
        │           ── profile: cdc ──                 │              │
        │           kafka-connect :8083 ─── watches WAL via slot      │
        │           init-connector (one-shot, registers connector)   │
        │           user-service-cdc :8081 (OutboxPoller disabled)   │
        │                                             │              │
        │                                             ▼              │
        │                                event-consumers :8082 ──────┘
        │                                (both profiles)
        └─────────────────────────────────────────────
```

### 3.3 Full Container/Port Reference — All Patterns

| Pattern | Container | Image | Host Port | Notes |
|---|---|---|---|---|
| observability | `finflow-prometheus` | `prom/prometheus:v2.53.0` | 9090 | scrapes every 5s |
| observability | `finflow-grafana` | `grafana/grafana:11.1.0` | 3000 | admin/admin, auto-provisioned |
| outbox-pattern | `finflow-postgres` | `postgres:15` | 5432 | `wal_level=logical` set unconditionally so the same image works for both profiles |
| outbox-pattern | `finflow-zookeeper` | `confluentinc/cp-zookeeper:7.5.0` | — | no host port |
| outbox-pattern | `finflow-kafka` | `confluentinc/cp-kafka:7.5.0` | 9092 | internal advertised listener `kafka:29092` |
| outbox-pattern | `finflow-kafka-connect` | `quay.io/debezium/connect:2.5` | 8083 | `cdc` profile only |
| outbox-pattern | `finflow-init-connector` | `curlimages/curl:8.5.0` | — | one-shot, `restart: on-failure`, `cdc` profile only |
| outbox-pattern | `finflow-user-service-polling` / `-cdc` | custom build | 8081 | mutually exclusive — same host port, pick one profile |
| outbox-pattern | `finflow-event-consumers` | custom build | 8082 | both profiles |
| circuit-breaker | `finflow-fraud-detection-service` | custom build | 8092 | no DB/Kafka at all — pure REST-to-REST |
| circuit-breaker | `finflow-transaction-service` | custom build | 8091 | — |
| idempotent-consumer | `finflow-idempotent-postgres` | `postgres:15` | **5433** | offset from outbox's 5432 |
| idempotent-consumer | `finflow-idempotent-redis` | `redis:7-alpine` | **6380** | |
| idempotent-consumer | `finflow-idempotent-kafka` | `confluentinc/cp-kafka:7.5.0` | **9093** | internal `kafka:29093` |
| idempotent-consumer | `finflow-loan-application-service` | custom build | 8101 | |
| idempotent-consumer | `finflow-loan-processing-service` | custom build | 8102 | |
| rate-limiter | `finflow-ratelimiter-redis` | `redis:7-alpine` | **6381** | offset from idempotent-consumer's 6380 |
| rate-limiter | `finflow-open-banking-api` | custom build | 8111 | no Postgres, no Kafka |
| dead-letter-queue | `finflow-dlq-postgres` | `postgres:15` | **5434** | |
| dead-letter-queue | `finflow-dlq-zookeeper` | `confluentinc/cp-zookeeper:7.5.0` | — | |
| dead-letter-queue | `finflow-dlq-kafka` | `confluentinc/cp-kafka:7.5.0` | **9094** | internal `kafka:29094` |
| dead-letter-queue | `finflow-kyc-service` | custom build | 8121 | |
| dead-letter-queue | `finflow-kyc-dlq-processor` | custom build | 8122 | |

The deliberate host-port offsetting (5432/5433/5434, 9092/9093/9094, 6379-mapped-to-6380/6381)
means all 5 pattern stacks **could** run simultaneously without port collisions — though only
outbox-pattern is actually wired into Prometheus's scrape config today (see Part 5).

### 3.4 Startup Dependency Chains

Every pattern follows the same shape: infra containers gate on Docker healthchecks
(`pg_isready`, `redis-cli ping`, `echo ruok | nc`, `kafka-broker-api-versions`), app containers
`depends_on` those with `condition: service_healthy`. The one extra hop is outbox-pattern's CDC
profile:

```
zookeeper (healthy)
   └── kafka (healthy)
          ├── kafka-connect (healthy)
          │      └── init-connector (registers connector, restart:on-failure until Connect is ready)
          └── user-service-cdc, event-consumers
postgres (healthy) ──────────┘  (kafka-connect also depends on postgres — needs it for the
                                  initial WAL snapshot / replication slot creation)
```

---

## Part 4 — Production Failure Modes at 10k+ RPS

### 4.1 Outbox Pattern

**F1 — OutboxPoller throughput ceiling (Approach A only).**
Symptom: at sustained high write volume, `outbox_backlog_size` (the gauge) climbs continuously
and event latency grows unbounded. Root cause: `batch-size: 10` every `interval-ms: 500` = a hard
ceiling of **20 events/sec per poller instance** — this was deliberately proven in the load test
(pushed backlog to ~7,600 pending events under a 30 req/s load). Remediation: raise `batch-size`
and lower `interval-ms` (diminishing returns — you're still polling), or run **multiple poller
instances** — the schema already supports this safely: `FOR UPDATE SKIP LOCKED` in
`OutboxEventRepository.findPendingEventsWithLock()` means concurrent pollers never double-claim
the same row. At 10k RPS you'd want N poller instances, not a tuned single one — Approach B (CDC)
sidesteps this ceiling entirely since Debezium isn't polling-interval-bound.

**F2 — Debezium replication slot growth (Approach B only).**
Symptom: Postgres disk usage grows unboundedly even though `outbox_events` itself isn't huge.
Root cause: a logical replication slot (`finflow_outbox_slot`) retains WAL segments until
Debezium consumes them — if the Kafka Connect container crashes, falls behind, or the connector
gets stuck, Postgres has no way to know it's safe to reclaim that WAL. At 10k RPS, WAL volume is
substantial and a stalled slot fills disk within hours, not days. Remediation: alert on
`pg_replication_slots.confirmed_flush_lsn` lag, and have a runbook for dropping/recreating a
truly-dead slot (with the understanding that any un-replicated events are lost — which is why
Approach A's Postgres-table-based retry has a durability edge here).

**F3 — HikariCP pool exhaustion.**
Symptom: `hikaricp_connections_pending` climbs, request latency spikes. Root cause:
`maximum-pool-size: 10` on `user-service` — at 10k RPS with even a few ms per DB round-trip,
10 connections is nowhere near enough concurrency. Remediation: raise the pool size (with
Postgres `max_connections` raised to match across all service instances), and consider PgBouncer
in front of Postgres for connection multiplexing if you're running many service replicas.

### 4.2 Circuit Breaker

**F1 — Bulkhead becomes the real bottleneck, not the circuit breaker.**
Symptom: at 10k RPS, most requests get rejected via `BulkheadFullException` well before the
circuit breaker's failure-rate logic ever has a chance to matter. Root cause:
`maxConcurrentCalls: 10` with `maxWaitDuration: 0` means only 10 simultaneous calls to the fraud
service are ever in flight — at 10k RPS with even 20ms average latency, you need roughly
`10,000 × 0.02 = 200` concurrent calls just to keep up in steady state. This value was sized for a
demo, not load. Remediation: raise `maxConcurrentCalls` to match `throughput × avg_latency`, and
separately consider whether a **thread-pool bulkhead** (isolating fraud calls onto their own
executor, via the `resilience4j-spring-boot3` thread-pool-bulkhead namespace) is more appropriate
than the current semaphore bulkhead once you're running `RestTemplate`'s blocking I/O at scale.

**F2 — Retry amplification during partial degradation.**
Symptom: when the fraud service is slow (not fully down), each of the 3 retry attempts adds
~200ms `waitDuration` plus a full network round-trip — so a struggling-but-not-dead fraud service
gets 3x the load from transaction-service's retries, which can be exactly what tips it over into
full failure (a self-inflicted thundering herd). Remediation: this is precisely why
`retryExceptions` is scoped to only `IOException`/`ResourceAccessException`/`HttpServerErrorException`
(never retrying on things retries can't fix) — but at 10k RPS you'd also want the circuit
breaker's `slowCallRateThreshold` to trip *before* retries pile up further load onto an already
degrading dependency. Consider lowering `slowCallDurationThreshold` and `minimumNumberOfCalls` so
the circuit opens faster under real load.

**F3 — Tomcat thread pool exhaustion.**
Symptom: `transaction-service` itself stops accepting new connections. Root cause: default
embedded Tomcat is 200 threads; a blocking `RestTemplate` call means each in-flight transaction
request holds a Tomcat thread for the full duration of the (possibly slow, possibly bulkhead-queued)
fraud call. Remediation: `server.tomcat.threads.max` tuning (not configured anywhere in this repo
today), or — the more durable fix at this scale — move to a non-blocking client (`WebClient`)
so a thread isn't pinned for the call's duration.

### 4.3 Idempotent Consumer

**F1 — Redis as an unhandled single point of failure for Strategy 1.**
Symptom: if Redis is unreachable, `redisTemplate.opsForValue().setIfAbsent()` throws a
`RedisConnectionFailureException` — which is **not caught anywhere** in
`LoanApplicationSubmittedConsumer`. Root cause: the current implementation has no fallback
behavior defined for this case at all; the exception propagates out of the `@KafkaListener`
method, the message is never acknowledged (since ack happens after successful return), and Kafka
redelivers it — meaning the consumer effectively stalls on `finflow.loan.application.submitted`
until Redis recovers. At 10k RPS this is a full-topic outage, not a degraded mode. Remediation:
this is a genuine gap worth fixing before production — decide explicitly whether Redis-down
should fail-open (skip dedup, risk processing duplicates) or fail-closed (current behavior,
blocks the topic) and code that decision deliberately, the same way circuit-breaker's fail-open
decision was made deliberately for the fraud service.

**F2 — `processed_credit_events` unbounded growth.**
Symptom: identical to the classic saga-pattern failure mode — every credit-score event ever
processed leaves a permanent row, and at 10k RPS that's ~864M rows/day. The PK lookup (implicit
in the `INSERT ... ON CONFLICT`-style duplicate check) slows down as the table bloats. Remediation:
scheduled purge job on `processed_at` past a retention window, with an index to make the delete
efficient, or table partitioning by day in Postgres so old partitions can be dropped instantly.

**F3 — Single-threaded consumers, un-tuned concurrency.**
Symptom: none of the 4 `@KafkaListener` methods in `loan-processing-service` set
`concurrency` — Spring Kafka defaults to 1 consumer thread per listener bean regardless of how
many partitions the topic has (each topic is created with 3 partitions via `TopicBuilder`, so
2 of 3 partitions sit idle per consumer group). At 10k RPS this is a hard serial bottleneck.
Remediation: set `spring.kafka.listener.concurrency` (or `@KafkaListener(concurrency = "3")`) to
match partition count, and raise partition count itself for real throughput headroom.

### 4.4 Redis Rate Limiter

**F1 — Redis is on the hot path for every single request.**
Symptom: at 10k RPS, `open-banking-api` issues at least 10,000 Redis operations/sec just for
rate-limit checks — before any actual business logic runs. Redis is single-threaded for command
execution, so this becomes the global throughput ceiling for the *entire service*, not just a
component of it. Remediation: at real scale you'd shard partners across multiple Redis
instances/cluster nodes (hash `partnerId` to pick a shard) so no single Redis node sees all
10k RPS — the current single-`StringRedisTemplate`-to-single-Redis design doesn't support this
without code changes (the Lua scripts and key naming are shard-agnostic, but the
`RedisConnectionFactory` wiring today points at one Redis).

**F2 — Sliding Window Log memory growth under high per-partner throughput.**
Symptom: for a partner sending sustained high traffic through `/ob/sliding-log/`, the backing
ZSET grows to hold one entry per request within the window — at ENTERPRISE's 10,000/min limit,
that's up to 10,000 ZSET members per partner at any moment, `O(log N)` for every `ZADD`/`ZREMRANGEBYSCORE`.
This is the exact trade-off the pattern's own README calls out ("memory grows with request
volume, not just partner count") — it's fine at FREE/PRO's lower limits but becomes real memory
pressure at ENTERPRISE-tier sustained load. Remediation: reserve sliding-log for genuinely
low-volume/high-stakes endpoints (billing/audit APIs, per the tier table) and never point a
high-throughput partner at it.

**F3 — No circuit breaker around the Redis calls themselves.**
Symptom: if Redis latency degrades (not down, just slow), every request blocks on the Lua script
round-trip with no timeout configured beyond `RedisConnectionFactory` defaults, and no fallback
behavior (unlike the fraud service, which has circuit-breaker fail-open). At 10k RPS a slow Redis
directly becomes slow `open-banking-api` response times with no isolation. Remediation: this
pattern would benefit from the same Resilience4j treatment `circuit-breaker/` demonstrates —
wrapping the Redis calls in a circuit breaker with a fail-open (allow the request) or fail-closed
(reject the request) fallback, which is a legitimate combination of two patterns in this repo that
was never built.

### 4.5 Dead Letter Queue

**F1 — Synchronous blocking retry collapses partition throughput under high transient-failure rates.**
Symptom: at 10k RPS with even a modest transient-failure rate (say 5%), 500 events/sec each tie
up a Kafka listener thread for up to `1+2+4=7` seconds before finally routing to DLQ. With
`listener.concurrency` unset (1 thread), that's a hard ceiling of roughly `1/7 ≈ 0.14` failing-message
throughput per second before the consumer falls permanently behind — and because retries block
the *same* thread that would otherwise process the next message, even successfully-processing
messages queue up behind a slow retry sequence. Remediation: this was a deliberate, documented
trade-off (see `dead-letter-queue/README.md`) favoring durability-simplicity over throughput — at
real scale you'd want Spring Kafka's `@RetryableTopic` (separate retry topics, doesn't block the
main partition) or raise `listener.concurrency` substantially so blocked retry threads don't
starve the whole consumer group.

**F2 — Replay poller batch size is a hard throughput ceiling.**
Symptom: `kyc_dlq_pending_replay` backlog grows unboundedly if the transient-failure rate exceeds
what the poller can drain. Root cause: `replay-batch-size: 20` every `replay-poll-interval-ms: 30000`
= **0.67 replays/sec**, full stop — regardless of how much Kafka/Postgres headroom actually
exists. At 10k RPS this ceiling is reached almost immediately if any non-trivial fraction of
traffic is transient failures. Remediation: same fix as `OutboxPoller` (F1 above) — raise batch
size, lower interval, or run multiple poller instances (this table doesn't yet have a
`SELECT ... FOR UPDATE SKIP LOCKED` clause the way `outbox_events` does, so multi-instance
pollers would currently double-process rows — a gap to fix before scaling this out).

**F3 — Alert threshold has no debouncing.**
Symptom: once `PENDING_REVIEW` count crosses 10, `ManualReviewAlertService` fires its structured
alert log on **every subsequent PERMANENT insert**, not just the crossing. If a real PagerDuty/Slack
webhook were wired in here (currently just a log line), sustained permanent-failure traffic at
10k RPS would produce an alert storm — hundreds of pages instead of one. Remediation: track
whether the threshold was already breached (e.g., a boolean flag or a "last alerted at" timestamp
with a cooldown) so the alert fires once per incident, not once per message.

### 4.6 Cross-Pattern Summary

| Pattern | P0 fix before real load | P1 fix |
|---|---|---|
| Outbox | Multiple poller instances (schema already supports it via SKIP LOCKED) | Alert on Debezium replication slot lag |
| Circuit Breaker | Raise bulkhead `maxConcurrentCalls` to match throughput×latency | Move to non-blocking `WebClient` |
| Idempotent Consumer | Set `listener.concurrency` to match partition count | Handle Redis-down explicitly in Strategy 1 |
| Rate Limiter | Shard Redis by partnerId | Add circuit breaker around Redis calls |
| DLQ | Raise `listener.concurrency` (or move to `@RetryableTopic`) | Add `SKIP LOCKED` to the replay poller for safe multi-instance scaling |

---

## Part 5 — Metrics & Observability, End to End

### 5.1 Collection layer — Micrometer inside each service

Micrometer is a facade — application code never talks to Prometheus directly. Every service that
has `io.micrometer:micrometer-registry-prometheus` on its classpath gets an auto-configured
`PrometheusMeterRegistry` Spring bean; app code just injects `MeterRegistry` and creates
instruments against it. Three instrument types are used in this repo, all hand-written except
where noted:

**Counter** — cumulative, only goes up. Example (`LoanApplicationSubmittedConsumer`):
```java
meterRegistry.counter("idempotent.consumer.events",
        "strategy", "redis-ttl", "topic", "finflow.loan.application.submitted", "outcome", "processed")
    .increment();
```
Every tag combination becomes its own time series. `strategy` × `topic` × `outcome` here is
low-cardinality by design (a handful of fixed strings), which is the right way to tag — never tag
a Counter/Timer with something unbounded like a `userId` or `documentId`.

**Timer** — records durations, and if configured for histogram buckets, gives you percentiles.
Example (`OutboxLatencyMetricsConsumer`):
```java
meterRegistry.timer("outbox.event.latency", "mode", mode, "topic", topic)
    .record(Duration.ofMillis(latencyMs));
```
This one meter name explodes into **multiple** Prometheus series once scraped:
`outbox_event_latency_seconds_count`, `_sum`, `_max`, and (because
`management.metrics.distribution.percentiles-histogram."[outbox.event.latency]": true` is set in
`event-consumers/application.yml`) a full set of `_bucket{le="..."}` series for
`histogram_quantile()` to consume in Grafana.

**Gauge** — a live value, re-read on every scrape, not something you increment. Two examples,
both registered once via `@PostConstruct` against a *function* rather than a stored number, so
they always reflect current DB state with no manual bookkeeping to keep in sync:
```java
// OutboxBacklogMetrics (user-service) — gated by the same @ConditionalOnProperty as OutboxPoller
Gauge.builder("outbox.backlog.size", outboxEventRepository,
        repo -> repo.countByStatus(OutboxStatus.PENDING))
    .register(meterRegistry);

// DlqMetrics (kyc-dlq-processor)
Gauge.builder("kyc.dlq.manual_review.pending", manualReviewRepository,
        repo -> repo.countByStatus("PENDING_REVIEW"))
    .register(meterRegistry);
```
The `OutboxBacklogMetrics` bean is gated behind `@ConditionalOnProperty(name = "outbox.poller.enabled", ...)`
— under the CDC profile that bean doesn't exist at all, so the gauge is simply absent from
`/actuator/prometheus` rather than reporting a number that would be meaningless (CDC never
transitions outbox row status, so a naive ungated gauge would show an ever-growing, misleading
"backlog").

**Auto-instrumentation with zero custom code — Resilience4j.** Because `resilience4j-spring-boot3`
and `micrometer-registry-prometheus` are both on `transaction-service`'s classpath, Resilience4j's
own autoconfiguration binds `TaggedCircuitBreakerMetrics` / `TaggedRetryMetrics` /
`TaggedBulkheadMetrics` to the `MeterRegistry` automatically. No line of application code creates
these; they just appear at `/actuator/prometheus` once both dependencies are present:
`resilience4j_circuitbreaker_state`, `resilience4j_circuitbreaker_calls`,
`resilience4j_circuitbreaker_buffered_calls`, `resilience4j_retry_calls`,
`resilience4j_bulkhead_available_concurrent_calls`, and more. Combined with the JVM/HTTP/HikariCP
metrics Micrometer emits automatically for *every* Spring Boot service (again, zero code — just
`spring-boot-starter-actuator` + the Prometheus registry dependency), this means most of a
service's observability surface exists before you write a single custom metric.

### 5.2 Exposition layer — `/actuator/prometheus`

`management.endpoints.web.exposure.include` must list `prometheus` (it's not in Actuator's
default-exposed set) for the endpoint to be reachable at all — every service's `application.yml`
in this repo does this explicitly. The endpoint renders every registered meter in Prometheus's
text exposition format:
```
# HELP outbox_event_latency_seconds
# TYPE outbox_event_latency_seconds histogram
outbox_event_latency_seconds_bucket{mode="polling",topic="finflow.user.registered",le="0.001",} 0.0
...
outbox_event_latency_seconds_count{mode="polling",topic="finflow.user.registered",} 1.0
outbox_event_latency_seconds_sum{mode="polling",topic="finflow.user.registered",} 0.585
```
This is a **pull** model — the service does no pushing at all; it just serves this text on
request. Whoever scrapes it (Prometheus, or literally `curl`) decides when to ask.

### 5.3 Aggregation/storage layer — Prometheus

`observability/prometheus/prometheus.yml` (today, only outbox-pattern's services are actually
configured as scrape targets — see 5.5 below):
```yaml
global:
  scrape_interval: 5s
scrape_configs:
  - job_name: user-service-polling
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ["finflow-user-service-polling:8081"]
        labels: { mode: polling }
```
Targets are addressed by **Docker Compose container name** (`finflow-user-service-polling`), only
reachable because that container also joined `finflow-net` (see Part 3.1). `static_configs` is
appropriate here because the topology is fixed and known ahead of time; a real Kubernetes
deployment would replace this with `kubernetes_sd_configs` service discovery instead of hardcoded
hostnames. The `labels: { mode: polling }` on the scrape config itself (not from the app) is a
nice trick — it lets Prometheus attach a label Grafana can group by, even though the metric being
scraped doesn't independently carry that same label from every service (the `event-consumers`
job has no such static label since its own metrics already carry `mode` as an app-emitted tag).

**5-second scrape interval** — much tighter than a typical production default (usually 15-30s).
Deliberate for a demo/load-test context where you want the dashboard to visibly react within
seconds of `curl`-ing an endpoint, at the cost of more Prometheus storage churn than you'd want
long-term.

### 5.4 Visualization layer — Grafana

Two provisioning files under `observability/grafana/provisioning/` get mounted read-only into
the Grafana container and auto-load on startup — **no manual "add data source" clicking**:
```yaml
# provisioning/datasources/datasource.yml
datasources:
  - name: Prometheus
    type: prometheus
    url: http://prometheus:9090
    isDefault: true
```
```yaml
# provisioning/dashboards/dashboards.yml
providers:
  - name: default
    type: file
    options: { path: /var/lib/grafana/dashboards }
```
The dashboard JSON itself (`grafana/dashboards/outbox-pattern.json`) is hand-authored, not
exported from the UI, with 4 panels:

1. **Event Latency (p50/p95) by Mode** — the star panel:
   `histogram_quantile(0.50, sum(rate(outbox_event_latency_seconds_bucket[$__rate_interval])) by (le, mode))`.
   This is the canonical Prometheus percentile pattern: `rate()` over the `_bucket` counters gives
   a per-second rate for each histogram bucket boundary (`le` = "less than or equal"),
   `sum(...) by (le, mode)` collapses across all scrape targets while keeping the bucket and mode
   dimensions, and `histogram_quantile()` interpolates the actual percentile value from those
   bucketed rates.
2. **Event Throughput by Mode** — reuses the same Timer's `_count` series
   (`sum(rate(outbox_event_latency_seconds_count[...])) by (mode)`) rather than a separate
   Counter, deliberately avoiding double-instrumentation.
3. **Polling Backlog** — a `stat` panel on the raw `outbox_backlog_size` gauge, with an explicit
   panel description warning that it shows "No data" under the CDC profile (by design, per 5.1).
4. **Avg Latency by Topic & Mode** — `sum(_sum) by (mode,topic) / sum(_count) by (mode,topic)`, a
   manual mean rather than a percentile, giving per-event-type granularity.

`refresh: "5s"` on the dashboard matches Prometheus's own scrape interval — no point refreshing
the panel faster than new data can possibly arrive.

### 5.5 What's actually wired versus what's ready-but-unwired

This is the honest state as of now:

| Pattern | Metrics exist in code? | Prometheus scrape job? | Grafana dashboard? |
|---|---|---|---|
| Outbox | ✅ (`outbox.event.latency`, `outbox.backlog.size`) | ✅ | ✅ (4 panels) |
| Circuit Breaker | ✅ (all Resilience4j metrics, auto-instrumented) | ❌ | ❌ |
| Idempotent Consumer | ✅ (`idempotent.consumer.events`) | ❌ | ❌ |
| Rate Limiter | ✅ (`ratelimit.requests`) | ❌ | ❌ |
| DLQ | ✅ (`kyc.pipeline.processed`, `kyc.dlq.*`) | ❌ | ❌ |

Every service already exposes `/actuator/prometheus` correctly (verified by `curl` during
development) — the only missing piece for the other 4 patterns is adding their scrape targets to
`prometheus.yml` and building their dashboard JSON, exactly the same shape as outbox-pattern's.
That's explicitly deferred, batched work (see `PROGRESS.md`), not a design gap.

---

## Part 6 — Load Testing Setup (Gatling)

### 6.1 Why Gatling, and where it lives

`outbox-pattern/load-tests/` is a **standalone Maven module** — not a Spring Boot app, no
`spring-boot-starter-parent`. It's a sibling to `user-service`/`event-consumers`, matching the
convention that a load-test simulation is pattern-specific (it knows the exact REST shapes) while
the observability stack is generic infra shared across patterns.

```xml
<properties>
  <gatling.version>3.13.5</gatling.version>
  <gatling-plugin.version>4.16.3</gatling-plugin.version>
</properties>
<dependencies>
  <dependency>
    <groupId>io.gatling.highcharts</groupId>
    <artifactId>gatling-charts-highcharts</artifactId>
    <version>${gatling.version}</version>
    <scope>test</scope>
  </dependency>
</dependencies>
<build>
  <plugins>
    <plugin>
      <groupId>io.gatling</groupId>
      <artifactId>gatling-maven-plugin</artifactId>
      <version>${gatling-plugin.version}</version>
    </plugin>
  </plugins>
</build>
```
Java DSL (`io.gatling.javaapi.*`), not Scala — chosen to stay consistent with the rest of this
Java-only repo. The `gatling-maven-plugin` auto-discovers any class under `src/test/java` that
extends `Simulation` — no explicit simulation-class registration needed unless you have more
than one and want to pick a specific one (`-Dgatling.simulationClass=...`).

### 6.2 Reading `OutboxRegistrationSimulation.java` line by line

```java
private static final String baseUrl = System.getProperty("baseUrl", "http://localhost:8081");

private final HttpProtocolBuilder httpProtocol = http
        .baseUrl(baseUrl)
        .acceptHeader("application/json")
        .contentTypeHeader("application/json");
```
`baseUrl` is a JVM system property with a default — override at the command line with
`mvn gatling:test -DbaseUrl=http://localhost:8091` to point the exact same simulation shape at a
different service without touching code (useful once you write simulations for the other 4
patterns).

```java
private final ScenarioBuilder scn = scenario("Register + Update KYC Status")
        .exec(
                http("Register User")
                        .post("/api/users/register")
                        .body(StringBody(session -> """
                                {"name":"Gatling User","email":"gatling-%s@finflow.com","phone":"+91-9000000000"}
                                """.formatted(UUID.randomUUID())))
                        .check(status().is(201), jsonPath("$.id").saveAs("userId"))
        )
        .pause(Duration.ofMillis(100))
        .exec(
                http("Update KYC Status")
                        .put("/api/users/#{userId}/kyc-status")
                        .body(StringBody("""
                                {"status":"VERIFIED"}
                                """))
                        .check(status().is(200))
        );
```
- `StringBody(session -> ...)` — a **lambda**, not a static string, so `UUID.randomUUID()`
  generates a genuinely unique email *per virtual user per request* (a static body would send the
  same email every time and every request past the first would 400 on the unique-email constraint).
- `.check(status().is(201), jsonPath("$.id").saveAs("userId"))` — asserts the HTTP status AND
  extracts a value from the JSON response into that virtual user's **session**, so it's available
  in later steps of the same scenario via Gatling's `#{userId}` EL syntax.
- `.pause(Duration.ofMillis(100))` — a fixed think-time between the two calls in the scenario,
  simulating a real client, not a tight loop.

```java
setUp(
        scn.injectOpen(
                rampUsersPerSec(1).to(30).during(Duration.ofSeconds(30)),
                constantUsersPerSec(30).during(Duration.ofSeconds(90))
        )
)
        .protocols(httpProtocol)
        .assertions(global().successfulRequests().percent().gt(95.0));
```
- Injection profile: ramp 1→30 new virtual users/sec over the first 30 seconds, then hold steady
  at 30/sec for 90 more seconds — roughly 3,200 total requests across the two-step scenario.
- **This load level is deliberately chosen above the polling approach's steady-state ceiling**
  (`batch-size:10` every `interval-ms:500` = 20 events/sec — see Part 4.1's F1) so the resulting
  backlog-then-drain effect is visible on the dashboard, not just noise.
- `.assertions(global().successfulRequests().percent().gt(95.0))` — a **pass/fail gate** for the
  whole run; if fewer than 95% of requests succeed, `mvn gatling:test` exits non-zero (useful in
  CI, though this repo doesn't currently wire it into any pipeline).

### 6.3 Running it

```bash
cd outbox-pattern/load-tests
mvn gatling:test                                    # default baseUrl=http://localhost:8081
mvn gatling:test -DbaseUrl=http://localhost:8091      # point at a different service
```
Output: a live console summary every 5 seconds during the run (request counts, response time
percentiles, OK/KO breakdown), plus a full interactive HTML report at
`target/gatling/outboxregistrationsimulation-<timestamp>/index.html` after it finishes — this
report is a useful secondary view alongside Grafana, since it gives Gatling's own percentile
breakdown independent of whatever Micrometer/Prometheus captured server-side.

### 6.4 Things to change to build intuition

**Change the injection profile shape.** Gatling's `openInjectionStep` has several besides the two
used here:
```java
// Instant spike — all 500 users start at once, no ramp
scn.injectOpen(atOnceUsers(500))

// Staged, more control than a single ramp
scn.injectOpen(
    nothingFor(Duration.ofSeconds(5)),                          // warm-up gap
    rampUsersPerSec(1).to(50).during(Duration.ofSeconds(60)),   // slower, longer ramp
    constantUsersPerSec(50).during(Duration.ofMinutes(5)),      // longer soak
    rampUsersPerSec(50).to(0).during(Duration.ofSeconds(30))    // ramp-down instead of a hard stop
)
```
Swapping in `atOnceUsers(500)` against the polling profile is a good way to *see* the backlog
spike happen almost instantly on the Grafana dashboard, versus the current gradual ramp.

**Add response-time assertions, not just success-rate.**
```java
.assertions(
    global().successfulRequests().percent().gt(95.0),
    global().responseTime().percentile(95.0).lt(1000)   // p95 under 1s
)
```
Given Part 4.1's F1, you'd expect this stricter assertion to **fail** under the current polling
config at this load level — a good way to prove the failure mode concretely rather than just
reading about it.

**Point a new simulation at a different pattern.** The shape generalizes directly — e.g. for
circuit-breaker's `POST /api/transactions`:
```java
private final ScenarioBuilder scn = scenario("Process Transactions")
        .exec(http("Process Transaction")
                .post("/api/transactions")
                .body(StringBody("""
                        {"payerId":"p1","payeeId":"p2","amount":5000}
                        """))
                .check(status().is(200)));
```
run with `-DbaseUrl=http://localhost:8091`, ramping past `bulkhead.maxConcurrentCalls: 10` (Part
4.2's F1) to watch `BulkheadFullException` rejections show up. For rate-limiter, you'd add a
per-request header:
```java
.exec(http("Get Accounts")
        .get("/ob/token-bucket/accounts")
        .header("X-Partner-Id", "partner-free-1")
        .check(status().in(200, 429)))   // 429 is an EXPECTED outcome here, not a failure
```
— note `status().in(200, 429)` instead of `.is(200)`, since hitting the rate limit is the point of
the test, not a bug to assert against.

**No load-test module exists yet for circuit-breaker, idempotent-consumer, rate-limiter, or DLQ**
— same "deferred, batched" status as their missing Grafana dashboards (Part 5.5). Building one is
a copy-paste of `load-tests/pom.xml` plus a new `Simulation` class per pattern.

---

## Quick Reference — Where to Find What

| Question | Where to look |
|---|---|
| How does polling vs CDC stay comparable? | `UserService.createOutboxEvent()` in `outbox-pattern/user-service` |
| What breaks the circuit? | `resilience4j.circuitbreaker.instances.fraudCircuitBreaker` in `transaction-service/application.yml` |
| Which dedup strategy applies to which event? | `idempotent-consumer/loan-processing-service/.../consumer/*.java`, one class per strategy |
| Which Lua script backs which rate-limit algorithm? | `rate-limiter/open-banking-api/src/main/resources/scripts/*.lua` |
| How does a DLQ message get auto-replayed? | `KycDlqReplayPoller` in `kyc-dlq-processor` — structurally identical to `OutboxPoller` |
| What metrics does a service emit for free? | Any service with `spring-boot-starter-actuator` + `micrometer-registry-prometheus` — JVM/HTTP/HikariCP with zero code, plus Resilience4j metrics in `transaction-service` specifically |
| What's actually dashboarded today? | Only `outbox-pattern` — `observability/grafana/dashboards/outbox-pattern.json` |
| How do I run a load test? | `cd outbox-pattern/load-tests && mvn gatling:test` |
| What Kafka config is active per service? | Each service's `application.yml` under `spring.kafka` |
| Why must `observability/` start before any pattern? | It owns the `finflow-net` Docker network every pattern references as `external: true` |

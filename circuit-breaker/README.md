# Circuit Breaker Pattern — FinFlow Transaction & Fraud Detection

**Problem:** `transaction-service` calls `fraud-detection-service` synchronously on every
payment to get a risk score. If the fraud service slows down or starts failing, threads in
`transaction-service` pile up waiting on it — under load, that exhausts the thread pool and takes
down transaction processing entirely, even though the actual bug is in a downstream dependency.

**Why this way:** Wrap the call in a circuit breaker. After enough failures or slow calls, the
circuit opens and every subsequent call fails instantly (no network round-trip, no thread pinned
waiting) until a cooldown elapses, at which point a few probe calls decide whether to close the
circuit again. Paired with a **fail-open** fallback — not fail-closed — because blocking every
transaction during a fraud service outage is worse than letting them through flagged for manual
review.

**How to run:**
```bash
docker-compose up -d --build
```

---

## How It Works

```
┌──────────────────────┐        ┌─────────────────────────────────────────┐        ┌──────────────────────────┐
│   transaction-service │        │      Resilience4j (fraudCircuitBreaker)  │        │  fraud-detection-service │
│                       │        │                                           │        │                          │
│  POST /api/transactions├───────►  Retry(3) ─► CircuitBreaker ─► Bulkhead(10)├───────►│  POST /api/fraud/score   │
│                       │        │                                           │        │                          │
│                       │◄───────┤  CLOSED: calls pass through               │◄───────┤  /admin/degrade?mode=... │
│                       │        │  OPEN: fails fast, no call made           │        │  (latency|error|down)    │
│                       │        │  HALF_OPEN: a few probe calls decide      │        │                          │
└──────────────────────┘        └─────────────────────────────────────────┘        └──────────────────────────┘
        │
        ▼
  riskLevel == HIGH  → BLOCKED
  otherwise          → APPROVED (flaggedForReview=true if this came from the fallback)
```

**Circuit states:**
```
CLOSED ──(≥50% of last 10 calls failed/slow)──► OPEN
OPEN ──(30s wait elapses)──► HALF_OPEN
HALF_OPEN ──(3 probe calls succeed)──► CLOSED
HALF_OPEN ──(a probe call fails)──► OPEN
```

---

## All Resilience4j Features Implemented

| Feature | Config | What It Demonstrates |
|---|---|---|
| State machine | CLOSED → OPEN → HALF_OPEN | Core circuit breaker lifecycle |
| Failure rate threshold | 50% of last 10 calls | Opens on error rate |
| Slow call rate threshold | 50% of calls > 2s in last 10 | Opens on latency, not just errors |
| Wait duration in OPEN | 30s | How long before attempting recovery |
| Permitted calls in HALF_OPEN | 3 | Probe calls before fully closing |
| Fallback | `MEDIUM` risk + `flaggedForReview=true` | Fail-open with degraded behavior |
| Bulkhead (semaphore) | Max 10 concurrent calls | Prevents thread exhaustion independent of circuit state |
| Retry | 3 attempts (2 retries), 200ms wait | Absorbs transient blips before the circuit breaker counts a failure |
| Health indicator | `/actuator/health` → `fraudCircuitBreaker` component | Ops visibility |
| Circuit breaker events | Logged as structured JSON (`CircuitBreakerEventLogger`) | State transition observability |

All three annotations (`@Retry`, `@CircuitBreaker`, `@Bulkhead`) target the same instance name
(`fraudCircuitBreaker`) on `FraudDetectionClientImpl.assessRisk()`, so their config lives together
under one key in `application.yml`. Composition follows Resilience4j's documented default order:
`Retry(outer) → CircuitBreaker → Bulkhead(inner, closest to the actual call)`. A
`CallNotPermittedException` (circuit OPEN) is cheap to retry — no network round-trip — so Retry
wrapping CircuitBreaker doesn't waste real work.

---

## Fail-Open vs Fail-Closed

When the fraud service is unavailable, `FraudDetectionClientImpl.fallbackAssessRisk()` returns
`MEDIUM` risk with `flaggedForReview=true` — the transaction is **approved but marked for human
review**, not blocked outright. This is a deliberate, consequential call:

- **Fail-open (chosen):** revenue keeps flowing during a fraud service outage; a downstream
  review queue catches anything that later turns out to be fraudulent. Right for a payments
  platform where uptime and cash flow matter and most transactions are legitimate.
- **Fail-closed (rejected):** every transaction blocks during any fraud service hiccup — safer
  against fraud, but turns a dependency's bad day into a full platform outage. Right for
  contexts where a false negative is catastrophic and false positives are cheap (this isn't
  that context).

---

## Failure Simulation

`fraud-detection-service` exposes `POST /admin/degrade` (excluded from a `prod` profile via
`@Profile("!prod")` — this class of endpoint should never exist in a real deployment):

| `mode` | Effect |
|---|---|
| `latency` | Sleeps `latencyMs` (default 5000) before responding — triggers the slow-call threshold |
| `error` | Returns HTTP 500 on every `/api/fraud/score` call — triggers the failure-rate threshold |
| `down` | Returns HTTP 503 — simulates the service being unreachable |
| `reset` | Restores normal scoring behavior |

---

## Demo Script

```bash
# 1. Baseline — normal scoring
curl -X POST http://localhost:8091/api/transactions \
  -H "Content-Type: application/json" \
  -d '{"payerId":"p1","payeeId":"p2","amount":5000}'
# -> riskLevel LOW, status APPROVED

# 2. Degrade the fraud service
curl -X POST "http://localhost:8092/admin/degrade?mode=error"

# 3. Drive enough calls to trip the circuit (failureRateThreshold=50%, slidingWindowSize=10,
#    minimumNumberOfCalls=5) — after ~5 failing calls the circuit opens
for i in $(seq 1 8); do
  curl -s -X POST http://localhost:8091/api/transactions \
    -H "Content-Type: application/json" \
    -d '{"payerId":"p1","payeeId":"p2","amount":5000}'
  echo
done

# 4. Watch transaction-service logs for STATE_TRANSITION CLOSED -> OPEN (structured JSON)

# 5. Circuit is OPEN — subsequent calls fail fast via the fallback, no network call made
curl -X POST http://localhost:8091/api/transactions \
  -H "Content-Type: application/json" \
  -d '{"payerId":"p1","payeeId":"p2","amount":5000}'
# -> riskLevel MEDIUM, flaggedForReview true, status APPROVED (fallback path)

# 6. Restore the fraud service
curl -X POST "http://localhost:8092/admin/degrade?mode=reset"

# 7. Wait 30s (waitDurationInOpenState) — circuit moves to HALF_OPEN automatically,
#    admits 3 probe calls; if they succeed it closes again
sleep 30
curl -X POST http://localhost:8091/api/transactions \
  -H "Content-Type: application/json" \
  -d '{"payerId":"p1","payeeId":"p2","amount":5000}'
# -> watch logs for STATE_TRANSITION HALF_OPEN -> CLOSED
```

Health + metrics visibility:
```bash
curl http://localhost:8091/actuator/health | jq .components.circuitBreakers
curl http://localhost:8091/actuator/prometheus | grep resilience4j_circuitbreaker
```

---

## When to Use the Circuit Breaker Pattern

✅ You have a synchronous call to a dependency that can degrade independently of your service
✅ Cascading failure (thread exhaustion from a slow downstream) is a real risk under load
✅ There's a sensible degraded/fallback behavior when the dependency is unavailable
✅ You want fast failure + automatic recovery detection, not just a fixed retry count

## When NOT to Use It

❌ The call is to something with no sensible fallback — a circuit breaker just changes the
  failure mode from "slow" to "fast," it doesn't invent a fallback where none exists
❌ Single-instance, low-throughput internal calls where thread exhaustion isn't a realistic risk
❌ You need strict consistency and a degraded response would be actively wrong (fail-open on a
  balance check, for instance, is a very different call than fail-open on a fraud score)

---

## Project Structure

```
circuit-breaker/
├── docker-compose.yml
├── fraud-detection-service/          # Spring Boot — the flaky downstream dependency
│   └── src/main/java/com/vaibhav/circuitbreaker/frauddetection/
│       ├── controller/                # ScoreController, AdminController (degrade modes)
│       ├── service/                   # RiskScoringService, DegradeState
│       └── domain/                    # RiskLevel, DegradeMode enums
└── transaction-service/              # Spring Boot — the resilient caller
    └── src/main/java/com/vaibhav/circuitbreaker/transaction/
        ├── controller/                # TransactionController
        ├── service/                   # TransactionService
        ├── client/                    # FraudDetectionClient(+Impl) — @Retry/@CircuitBreaker/@Bulkhead
        ├── config/                    # RestTemplateConfig, CircuitBreakerEventLogger
        └── domain/                    # RiskLevel, TransactionStatus enums
```

---

## API Reference

```bash
# Process a transaction (calls fraud-detection-service under the hood)
curl -X POST http://localhost:8091/api/transactions \
  -H "Content-Type: application/json" \
  -d '{"payerId": "p1", "payeeId": "p2", "amount": 5000}'

# Score directly (bypasses the circuit breaker — useful to isolate fraud-detection-service)
curl -X POST http://localhost:8092/api/fraud/score \
  -H "Content-Type: application/json" \
  -d '{"payerId": "p1", "payeeId": "p2", "amount": 5000}'

# Inject a failure mode
curl -X POST "http://localhost:8092/admin/degrade?mode=latency&latencyMs=3000"
```

Risk thresholds (`RiskScoringService`, deterministic by design so the demo is reproducible):
amount > 100,000 → `HIGH`; amount > 10,000 → `MEDIUM`; otherwise → `LOW`.

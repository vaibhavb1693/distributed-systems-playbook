# Redis Rate Limiter Pattern — FinFlow Open Banking API

**Problem:** FinFlow's Open Banking API is consumed by licensed third-party partners on shared
infrastructure. Without rate limiting, one partner's batch job (or bug) can starve every other
partner, and there's no lever to make Free-tier usage costlier than Enterprise. Different
partners also have genuinely different traffic shapes — a reconciliation job sends bursts, a
real-time balance checker sends smooth traffic — so no single algorithm is "correct" for all of
them.

**Why this way:** Implement all 5 classic algorithms behind one atomic Redis Lua script each (no
read-modify-write race under concurrent requests), and expose every algorithm as its own endpoint
prefix against the *same* partner-tier limit. That means a partner can be pointed at any of the 5
endpoints and see the same nominal limit enforced completely differently — which is the point:
the algorithm choice changes the traffic *shape* it allows, not just a number.

**How to run:**
```bash
docker-compose up -d --build
```

---

## How It Works

```
Request → X-Partner-Id header → PartnerRegistry (tier lookup) ─┐
                                                                  │
Request path /ob/{algorithm}/accounts → algorithm selection ────┤
                                                                  ▼
                                              RateLimitInterceptor
                                          (runs before any controller)
                                                       │
                              ┌────────────────────────┼────────────────────────┐
                              ▼                         ▼                         ▼
                    RateLimiter.tryConsume()  →  one atomic Lua script  →  Redis (rl:{algo}:{partnerId})
                              │
                    allowed?  ├── yes → sets X-RateLimit-* headers → controller runs
                              └── no  → 429 + Retry-After, controller never runs
```

---

## All 5 Algorithms — One Partner Limit, Five Different Shapes

| Algorithm | Endpoint Prefix | Redis Structure | Tier Fit | Key Trade-off |
|---|---|---|---|---|
| Fixed Window Counter | `/ob/fixed/` | `INCR` + `EXPIRE` per window bucket | Free tier | Simple, but a client can send 2x the limit across a window boundary |
| Sliding Window Log | `/ob/sliding-log/` | ZSET, one entry per request timestamp | Billing/audit APIs | Exact, but memory grows with request volume, not just partner count |
| Sliding Window Counter | `/ob/sliding-counter/` | 2 counters, weighted by elapsed fraction | Pro tier default | Good balance — no hard boundary burst, bounded memory |
| Token Bucket | `/ob/token-bucket/` | Hash: `tokens`, `lastRefill` | Enterprise, SDKs | Burst-friendly — idle time accumulates spendable allowance |
| Leaky Bucket | `/ob/leaky-bucket/` | Hash: `level`, `lastLeak` | Traffic shaping to a fragile downstream | Smoothest output — idle time earns **no** burst allowance |

**Why Lua scripts, not a Java-side check-then-write:** every algorithm's read-modify-write has to
be atomic, or two concurrent requests from the same partner can both read "under limit" before
either writes back — a classic TOCTOU race that would let a partner exceed their limit under
load. Each `.lua` script (in `open-banking-api/src/main/resources/scripts/`) does the entire
check-and-update as one Redis command, so Redis's own single-threaded execution model gives us
the atomicity for free.

**Redis key namespace:** `rl:{algorithm}:{partnerId}[:{windowBucket}]` — isolates every
algorithm × partner combination, so hitting `/ob/fixed/` and `/ob/token-bucket/` for the same
partner tracks two completely independent limit states (deliberately — that's what makes the
side-by-side comparison meaningful; a partner isn't double-penalized for testing multiple
endpoints).

---

## Response Headers

Every request through `/ob/**` gets:
- `X-RateLimit-Limit` — the partner's configured limit for their tier
- `X-RateLimit-Remaining` — requests left (algorithm-dependent estimate for token/leaky bucket)
- `X-RateLimit-Reset` — epoch seconds when the window/allowance resets

`429 Too Many Requests` responses additionally get `Retry-After`.

---

## Partner Tiers (`application.yml`)

| Tier | Limit | Window | Burst Capacity |
|---|---|---|---|
| FREE | 60 req/min | 60s | 60 (no burst) |
| PRO | 1,000 req/min | 60s | 1,000 (no burst) |
| ENTERPRISE | 10,000 req/min | 60s | 10,500 (500 burst allowance) |

No partner-onboarding database in this demo — `PartnerRegistry` reads a fixed `partnerId → tier`
map from config (`rate-limiter.partners`). An unregistered `partnerId` defaults to `FREE`, the
safe default for an unknown caller.

---

## Demo Script

```bash
# FREE tier, fixed window — 60/min. Send 65 requests, expect ~60 allowed then 429s.
for i in $(seq 1 65); do
  curl -s -o /dev/null -w "%{http_code} " \
    -H "X-Partner-Id: partner-free-1" \
    http://localhost:8111/ob/fixed/accounts
done
echo

# Same partner, same nominal limit, token bucket instead — burst-friendly, so an idle
# partner can spend up to full capacity immediately, then throttles to the steady rate.
curl -i -H "X-Partner-Id: partner-free-1" http://localhost:8111/ob/token-bucket/accounts

# Missing partner header — 400
curl -i http://localhost:8111/ob/fixed/accounts

# Unknown algorithm in path — 400
curl -i -H "X-Partner-Id: partner-free-1" http://localhost:8111/ob/nonexistent/accounts
```

Inspect Redis state directly to see each algorithm's data shape:
```bash
docker exec -it finflow-ratelimiter-redis redis-cli KEYS "rl:*"
docker exec -it finflow-ratelimiter-redis redis-cli HGETALL "rl:token-bucket:partner-free-1"
```

---

## When to Use the Rate Limiter Pattern

✅ Multi-tenant infrastructure where one caller's traffic can degrade service for others
✅ Different callers have genuinely different traffic shapes — bursty vs. smooth vs. steady
✅ You need standard, discoverable limit signaling (`X-RateLimit-*`, `Retry-After`) for API consumers

## When NOT to Use It

❌ Single-tenant internal services with no fairness concern — the Redis round-trip and key
  bookkeeping is pure overhead
❌ You need hard backpressure on a downstream system, not just fairness — a rate limiter rejects
  excess requests; if you need to actually queue and drain them, that's a different pattern
  (a real message queue, not a rate limiter)

---

## Project Structure

```
rate-limiter/
├── docker-compose.yml
└── open-banking-api/
    ├── src/main/resources/scripts/    # one .lua file per algorithm
    └── src/main/java/com/vaibhav/ratelimiter/openbanking/
        ├── ratelimit/                  # RateLimiter interface + 5 impls + RateLimiterRegistry
        ├── partner/                    # PartnerTier, PartnerRegistry
        ├── interceptor/                # RateLimitInterceptor — all enforcement happens here
        ├── controller/                 # OpenBankingController — algorithm-agnostic
        └── config/                     # RateLimiterProperties, RateLimiterScriptConfig, WebMvcConfig
```

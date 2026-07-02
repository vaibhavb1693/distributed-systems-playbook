# Q: How should Rate Limiter and DLQ extend the patterns already established?

> Decided while implementing patterns 4 and 5, the last two in the playbook. Both build directly
> on precedent set earlier in this repo rather than introducing new structural ideas.

---

## Rate Limiter: one endpoint per algorithm, same limit — not one endpoint with an algorithm parameter

QnA/02 specified 5 algorithms with distinct endpoint prefixes (`/ob/fixed/`, `/ob/token-bucket/`,
etc.) and a "tier fit" table suggesting each tier defaults to a particular algorithm in a real
deployment. The open question was whether the demo should *enforce* that tier→algorithm mapping
(a FREE partner can only hit `/ob/fixed/`) or expose all 5 algorithms to every partner against
their same tier limit.

**Decision: all 5 algorithms, same limit, any partner.** The whole reason this repo cares about
visualizing patterns (see `QnA/03`) is to make tradeoffs *comparable*. If a FREE partner could
only ever hit the fixed-window endpoint, there'd be no way to show — on the same dashboard, later
— how token bucket's burst tolerance differs from leaky bucket's smoothing for identical traffic.
`RateLimitConfig` is keyed by tier only; `RateLimiterRegistry` maps the URL's algorithm segment to
an implementation independently. The "tier fit" column in the README documents production
guidance, not a demo restriction.

**Mechanical consequence:** every algorithm needed the same config shape
(`limit`, `windowSeconds`, `burstCapacity`) so one `RateLimitConfig` record could serve all 5 —
fixed/sliding-log/sliding-counter read `limit`+`windowSeconds` directly, token/leaky bucket derive
capacity and refill/leak rate from the same three fields. No per-algorithm config types.

## Rate Limiter: Lua scripts as classpath resources, not Java string literals

Every algorithm's read-modify-write must be atomic — two concurrent requests from the same
partner must not both read "under limit" before either writes back. Redis's single-threaded
script execution gives this for free, *if* the entire check-and-update happens inside one script.
Loading each `.lua` file via `DefaultRedisScript.setLocation(ClassPathResource)` (rather than
inlining the script as an escaped Java string) keeps the atomic logic reviewable as actual Lua —
worth calling out because it's a small choice with an outsized effect on whether a future reader
can actually verify the atomicity claim by reading the script.

---

## DLQ: reusing the outbox poller pattern for durable replay

QnA/02 said TRANSIENT DLQ arrivals get "scheduled auto-replay after a 5-minute cooldown (scheduled
task republishes to original topic)" without specifying whether that schedule needs to survive a
restart. An in-memory `ScheduledExecutorService` is the obvious naive implementation — schedule a
delayed task per message, done. It's also fragile: a pod restart mid-cooldown silently loses every
pending replay with no trace they ever existed.

**Decision:** persist pending replays to a `kyc_dlq_pending_replay` table (`replay_at` column) and
poll it with `@Scheduled`, structurally identical to `outbox-pattern`'s `OutboxPoller` (poll a
table for due work → publish to Kafka → mark done). This wasn't a new idea — it's the same
insight (a schedule that must survive a restart belongs in the database, not in JVM heap) applied
a second time in this repo. Worth noting as precedent for future patterns that need "come back to
this later" semantics: check whether the outbox-poller shape fits before reaching for
`ScheduledExecutorService`.

## DLQ: manual retry loop over Spring Kafka's `@RetryableTopic`

Spring Kafka has built-in retry-then-DLT support (`@RetryableTopic`, `DeadLetterPublishingRecoverer`)
that would replace `KycDocumentUploadedConsumer`'s manual while-loop with a few annotations.
Rejected for this implementation because: (1) the 6 header names are explicitly spec'd
(`X-Failure-Reason`, `X-Failure-Type`, etc.) and customizing `DeadLetterPublishingRecoverer`'s
headers requires overriding internals that are harder to get right without live Kafka to verify
against (code-only phase, no Docker); (2) `@RetryableTopic` retries via separate retry topics with
generated names (`-retry-0`, `-retry-1`, `-dlt`), which is more moving parts to reason about
correctness for than a synchronous loop. Documented as a known, deliberate trade-off in
`dead-letter-queue/README.md` rather than silently choosing the less-idiomatic path — a real
high-throughput system would likely make the opposite call.

## DLQ: per-document failure simulation, not global service degradation

Circuit-breaker's `fraud-detection-service` has one global `/admin/degrade` switch because a
fraud service outage is genuinely a whole-service event. KYC failures aren't — a corrupted
document is a property of *that document*, not evidence the whole `kyc-service` is unhealthy. So
`kyc-service`'s failure injection is a `simulate` field on the upload request itself, not a
service-wide toggle. Two patterns, two different failure-injection shapes, each matching what the
real failure mode actually looks like.

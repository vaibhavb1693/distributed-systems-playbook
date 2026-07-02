# Q: How should Circuit Breaker and Idempotent Consumer extend the patterns already established in QnA/02?

> Decided while implementing patterns 2 and 3. QnA/02 specified the use cases and Resilience4j
> feature list in detail but left some structural questions open — this fills those in.

---

## Circuit Breaker: fail-open, confirmed and implemented as designed

QnA/02 already called for fail-open (`MEDIUM` risk + `FLAGGED_FOR_REVIEW`) over fail-closed. Built
exactly that: `FraudDetectionClientImpl.fallbackAssessRisk()` returns `RiskAssessment(MEDIUM, flaggedForReview=true)`.
No change to the original decision — worth restating here because it's the single most
consequential engineering call in this pattern, and `circuit-breaker/README.md` documents the
tradeoff explicitly (blocking every transaction during an outage is worse than letting them
through flagged, for a payments platform where most transactions are legitimate).

One implementation detail QnA/02 didn't specify: **aspect composition order**. `@Retry`,
`@CircuitBreaker`, and `@Bulkhead` all target the same instance name (`fraudCircuitBreaker`) on
one method. Rather than override Resilience4j's `*AspectOrder` properties, we kept the documented
default: `Retry(outer) → CircuitBreaker → Bulkhead(inner)`. Reasoning: a `CallNotPermittedException`
from an OPEN circuit is a local, instant rejection — no network round-trip — so Retry wrapping
CircuitBreaker doesn't waste real work even though at first glance "retry outside circuit breaker"
sounds backwards.

---

## Idempotent Consumer: one service or two?

QnA/02 only specified `loan-processing-service` (the consumer). But a consumer needs something to
consume — and demoing 4 different dedup strategies means being able to deliberately redeliver a
specific event, which isn't something you can do by just waiting for Kafka's natural at-least-once
behavior to happen to trigger.

**Decision: two services**, `loan-application-service` (producer) + `loan-processing-service`
(consumer) — mirroring outbox-pattern's `user-service`/`event-consumers` split. The producer's 4
endpoints all accept `times` (default 1), republishing the *identical* event (same `eventId` for
Strategies 1/2, same `expectedVersion` for Strategy 3) N times. This keeps the "how do we demo
redelivery" concern entirely in the producer, out of the pattern actually under test.

**Rejected alternative:** a single service that's both producer and consumer of its own events
(publish-then-immediately-consume in one process). Rejected because it conflates two concerns —
demoing redelivery and testing dedup — and doesn't generalize to a real system, where the producer
and consumer are always separate services by definition.

## The `JpaRepository.save()` trap for manually-assigned IDs

Both Strategy 2 (DB unique constraint) and Strategy 4 (upsert) needed a real INSERT to run — for
Strategy 2 specifically so a duplicate `eventId` throws `DataIntegrityViolationException`. The
naive approach, `processedCreditEventRepository.save(new ProcessedCreditEvent(eventId, ...))`,
does **not** do this: Spring Data JPA's `save()` on an entity with a manually-assigned `@Id`
(no `@GeneratedValue`) calls `EntityManager.merge()`, not `persist()` — which does a SELECT to
check existence, then an INSERT or UPDATE. A duplicate `eventId` would silently succeed as an
UPDATE instead of raising the exception the whole strategy depends on.

**Fix:** both repositories expose a single native `@Modifying @Query` method
(`insertOrThrow` / `upsert`) instead of using `save()`. This is now the established pattern in
this repo for any manually-keyed insert-or-detect-duplicate scenario — see also
`outbox-pattern`'s `OutboxEventRepository` for a different but related native-query rationale
(`FOR UPDATE SKIP LOCKED`).

## Why a direct compare-and-swap update instead of JPA's built-in `@Version` optimistic locking

Strategy 3 could have used JPA's standard flow: load the entity, set a field, call `save()`, let
Hibernate throw `OptimisticLockException` on a stale version. Chose a direct
`UPDATE ... WHERE id=? AND version=?` returning a row count instead, because "stale event, skip
and move on" is a normal, expected outcome in a Kafka consumer — not an exceptional one worth a
try/catch around an entity load. Matches QnA/02's original spec literally ("0 rows updated → stale
or duplicate event → skip with warning log").

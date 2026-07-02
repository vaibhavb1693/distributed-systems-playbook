# Q: How do we make each pattern's tradeoffs measurable and visual, not just documented?

> Decided after `outbox-pattern` reached functional completeness. The goal: for every pattern in
> this playbook, be able to actually see the tradeoff the pattern's README describes — polling vs
> CDC latency, circuit breaker state transitions, rate limiter algorithm behavior under burst, DLQ
> lifecycle — on a dashboard, under real load, not just assert it in prose.

---

## Approaches considered for metrics collection

| Approach | Verdict |
|---|---|
| **Micrometer → Prometheus** | **Chosen.** Native to Spring Boot Actuator; Resilience4j (used by the future circuit-breaker pattern) auto-registers Micrometer metrics for state transitions with zero custom code |
| Structured logs → Loki/ELK | Rejected — better suited to debugging traces than rate/latency dashboards |
| Push-based (StatsD/InfluxDB/Telegraf) | Rejected — fights the Spring ecosystem's pull-based defaults for no benefit here |

## Approaches considered for observability infra topology

| Approach | Verdict |
|---|---|
| **One shared `observability/` stack at repo root** | **Chosen.** Patterns are explored one/few at a time; a single Prometheus+Grafana pair avoids duplicated config and port conflicts across all 5 patterns, and makes cross-pattern comparison possible later |
| One Prometheus+Grafana pair duplicated into each pattern's own docker-compose | Rejected — fully self-contained per folder, but duplicated config and port-conflict risk if two patterns ever run together |

Mechanically: `observability/docker-compose.yml` owns an external Docker network
(`finflow-net`) that each pattern's compose file joins (only the app services that expose
`/actuator/prometheus` — not infra containers like Postgres/Kafka). The observability stack must
be brought up first so the network exists before a pattern's compose file references it as
`external: true`.

## Approach considered for load testing

| Approach | Verdict |
|---|---|
| **Gatling, Java DSL** | **Chosen.** Code-first, version-controlled simulations match this repo's as-code style; built-in HTML/percentile reports complement Grafana's live view |
| JMeter | Rejected — GUI-driven, XML test plans don't version-control as cleanly |

Each pattern gets its own `<pattern>/load-tests/` Maven module (sibling to its app modules) rather
than a shared repo-root load-test project — a simulation is inherently pattern-specific (it knows
the exact REST endpoints/payload shapes), unlike the generic `observability/` infra.

---

## The key mechanism: measuring polling vs CDC latency with zero Debezium changes

The hard part wasn't picking tools — it was finding a way to tag every outbox event with *how it
was published* (`polling` vs `cdc`) and *when it was created*, such that both delivery paths carry
that information identically.

The insight: Debezium's outbox event router (`transforms.outbox.table.expand.json.payload=true`)
flattens whatever top-level JSON keys exist in the `outbox_events.payload` column straight into
the outgoing Kafka message. The polling path (`OutboxPoller`) sends `payload` verbatim too. So any
field added to the payload map **before** it's serialized in `UserService.createOutboxEvent()`
shows up identically in the final Kafka message under both approaches — no connector config
branching needed. This is why `eventCreatedAtMs` + `mode` are injected at that one call site
rather than, say, as Kafka headers (which would require separate producer-side code for polling
vs a Debezium SMT config change for CDC).

A dedicated consumer (`OutboxLatencyMetricsConsumer`, its own consumer group
`finflow-metrics-service`) reads those two fields and records the latency — kept separate from
`NotificationConsumer`/`AuditConsumer` specifically to avoid double-counting, since both already
consume all 3 topics.

---

## What this surfaced: three real, pre-existing bugs

Wiring up live verification (actually running `docker-compose up` end-to-end for both profiles,
not just `mvn test`) surfaced defects that had been sitting in "✅ Complete" code, invisible to
unit tests because they only manifest under a full Spring context or a real Debezium round-trip:
missing `spring-boot-starter-web` in `event-consumers` (no `ObjectMapper` bean, no web server for
actuator), a wrong Debezium SMT property name (`expand.json.payload` needed a `table.` prefix,
silently ignored otherwise — payload arrived double-JSON-encoded), and a YAML folded-scalar
indentation bug that corrupted the `init-connector` service's `curl` command. See
`PROGRESS.md`'s "Bugs found and fixed" note under the Observability Stack section for the full
list. Takeaway: "tests pass" and "verified end-to-end in Docker" are different claims — this
repo's patterns should get the second, not just the first, before being marked complete.

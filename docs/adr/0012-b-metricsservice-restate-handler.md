# ADR-012-b: MetricsService — Restate-handler surface for invocation metrics

> **Status:** Proposed. **Date:** 2026-09-01. **Author:** SM8 agent (per user directive "can we draft these as new ADR-012 series and pass to dual reviewers to approve first").

## Context and Problem Statement

The current `sm8-platform` code paths emit **no metrics surface**. References to "metrics" exist in:

- `sm8-core/.../Pipeline.scala:205` — "audit, metrics" mentioned as Observer-hook consumers
- `sm8-sdk/.../Hooks.scala:82, 88, 119` — hook examples (logging, metrics, audit)
- `sm8-platform/.../EngineHookDispatcher.scala:204, 213` — Observer / side-effect hooks

But **none of these actually emit metrics**. There is no Micrometer `MeterRegistry`, no Prometheus `/metrics` endpoint, no JMX MBean. Operators have no observable counter for:

- How many `QueryService.runQuery` calls succeeded vs failed
- Cache hit rate (the cache-plugin writes a marker to `context.meta`, but there's no counter that reads it back)
- Engine time (driver-side) vs wire time (handler-side)
- Audit plugin entries dropped (the `EngineError.AuditSinkUnavailable` typed error exists; no counter)

Restate itself has journal-side metrics, but they show invocation counts per service, not sm8's per-engine or per-cache counters. The data we need lives in sm8, not in Restate.

## Decision

Add a **read-only** `MetricsService` to `sm8-platform` exposing a single handler:

| Handler | Wire | Returns |
|---|---|---|
| `snapshot` | `SnapshotRequest()` | `MetricsSnapshot(invocations, cache, errors, startedAt, uptimeSeconds)` |

### Scope — what is NOT in this ADR

To keep this small and reviewable:

- **No metric emission is added.** The handler returns **placeholder counters that always return 0**. The wire shape is real; the values are zero until ADR-012-b-followup instruments the call sites. **The ADR explicitly accepts this gap** because instrumentation is a larger refactor (touching every query path + every cache path) and shipping the wire surface first unblocks UI/observability tooling.
- No Micrometer / Prometheus / JMX integration. Future ADR.
- No `reset()` handler (dev-only).

### Alternatives Considered

| Alternative | Reason rejected |
|---|---|
| **Wire directly to Micrometer's `PrometheusMeterRegistry`** | Requires a Maven dep + infrastructure (`/metrics` HTTP route + scrape config). Bigger change; future ADR. |
| **Wire to OpenTelemetry** | Same scope concern as Micrometer. |
| **Don't ship MetricsService until real counters exist** | The UI dashboard work needs a wire surface first; "shape ready, values zero" is honest and reviewable per the dual-review rule. |
| **Per-handler counters (`invocations.runQuery`, `invocations.getMeta`, ...)** | The 3 handlers all share `invocations: InvocationCounters` because they're all "successful invocation" by design — engine failures are `errors.*`. Per-handler breakdown is future ADR-012-b-followup. |

### Layer discipline

| Layer | What lands there |
|---|---|
| **sm8-core** | No change. The counters live in sm8-platform only. |
| **sm8-platform** | New `MetricsService` object with `definition()` (returns placeholder snapshot). New wire DTOs. `HttpTransport.endpoint` binds it. |
| **sm8-server** | No change. |
| **plugins / hooks** | No change. Plugins will eventually write to `MetricsService` counters, but that's ADR-012-b-followup. |

### Implementation sketch

```scala
// sm8-platform/src/main/scala/io/sm8/platform/query/MetricsService.scala
import java.time.Instant

object MetricsService {
  // Captured once at service startup. ADR-012-b-followup will replace
  // this with real per-event counters.
  private val startedAt: Instant = Instant.now()

  def definition(): ServiceDefinition = {
    val scalaMapper = new ObjectMapper()
      .registerModule(DefaultScalaModule)
    val jacksonSerdeFactory = new JacksonSerdeFactory(scalaMapper)

    val snapshotRunner = HandlerRunner.of(
      (ctx, _: SnapshotRequest) => {
        val now = Instant.now()
        MetricsSnapshot(
          startedAt    = startedAt.toString,
          uptimeSeconds = (now.toEpochMilli - startedAt.toEpochMilli) / 1000L,
          invocations  = InvocationCounters(total = 0, succeeded = 0, failed = 0),
          cache        = CacheCounters(hits = 0, misses = 0),
          errors       = ErrorCounters(auditSinkUnavailable = 0, timedOut = 0)
        )
      },
      jacksonSerdeFactory,
      HandlerRunner.Options.DEFAULT
    )

    ServiceDefinition.of(
      "MetricsService",
      ServiceType.SERVICE,
      java.util.List.of(
        HandlerDefinition.of("snapshot", HandlerType.SHARED, requestSerde, responseSerde, snapshotRunner)
      )
    )
  }
}
```

`HttpTransport.endpoint` adds one line: `.bind(MetricsService.definition())`.

### Wire DTOs

```scala
case class SnapshotRequest()
case class MetricsSnapshot(
    startedAt:    String,                // ISO-8601
    uptimeSeconds: Long,
    invocations:  InvocationCounters,
    cache:        CacheCounters,
    errors:       ErrorCounters
)
case class InvocationCounters(total: Long, succeeded: Long, failed: Long)
case class CacheCounters(hits: Long, misses: Long)
case class ErrorCounters(auditSinkUnavailable: Long, timedOut: Long)
```

### Testing

- **Unit**: `MetricsServiceSpec` — snapshot shape, all counters are 0, `startedAt` is stable across calls.
- **E2E**: assert `POST localhost:8080/MetricsService/snapshot` returns 200 + JSON with all 6 counters present (regardless of value).
- **Scaladoc**: per `scala2-scaladoc`.

### Risks

| Risk | Mitigation |
|---|---|
| Snapshot returns zeros; users may mistake this for "metrics not working" | Document clearly: "until ADR-012-b-followup lands, counters are placeholders"; `startedAt` + `uptimeSeconds` distinguish "server up, zero counters" from "server crashed" |
| Wire schema will change when real counters land | Use versioned DTO; `startedAt` + `uptimeSeconds` are stable across revisions |
| `Instant.now()` per call adds allocation pressure | Trivial (<1µs); not a real concern |

## Consequences

### Positive

- Wire surface ready for UI dashboard work
- Operator can verify the MetricsService is wired (even if values are 0)
- ADR-012-b-followup can add real counters without changing the wire surface

### Negative

- Half-feature: handler returns 0s
- Two follow-ups needed (real instrumentation + Prometheus export)
- Operators may not realize the values are placeholders (mitigation: docs)

## References

- `sm8-platform/.../QueryService.scala`: ServiceDefinition + handler registration template
- `sm8-platform/.../MetaInspectorService.scala`: smaller template (single handler)
- `sm8-core/.../Pipeline.scala:205`: existing "audit, metrics" comment as future-work hook consumer
- `sm8-core/.../EngineError.AuditSinkUnavailable`: typed error ADR-012-b-followup will count
- `sm8-platform/.../EngineHookDispatcher.scala:204,213`: Observer / side-effect hooks (no emission today)
- ADR-009-d: precedent for observing plugin state via service (`EngineContext.decisionHints`)
- `scala2-scaladoc` skill: all new public methods documented
- PR-249 (`9e04779`): current 2-service baseline that the wire surface builds on
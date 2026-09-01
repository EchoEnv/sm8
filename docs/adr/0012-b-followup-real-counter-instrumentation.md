# ADR-012-b-followup: Real counter instrumentation for MetricsService

> **Status:** Proposed. **Date:** 2026-09-01. **Author:** SM8 agent (per user directive "go with your recommended", building on PR-254's `MetricsService` wire surface + PR-250's ADR-012-b plan).

## Context and Problem Statement

PR-254 (`d0c15ee`) shipped the `MetricsService` wire surface (1 handler `snapshot`, 6 placeholder counter fields). The handler returns real `startedAt`/`uptimeSeconds` but **all 6 counters are hard-coded zeros**:

```scala
// sm8-platform/.../MetricsService.scala:159 (PR-254)
invocations = InvocationCounters(total = 0, succeeded = 0, failed = 0),
cache       = CacheCounters(hits = 0, misses = 0),
errors      = ErrorCounters(auditSinkUnavailable = 0, timedOut = 0)
```

ADR-012-b flagged this explicitly: "Wire surface ready; real instrumentation is a follow-up." This ADR captures the follow-up.

## Decision

Add a **`QueryMetrics`** counter holder in `sm8-platform` and wire its increment calls into the call sites that produce each counter:

| Counter | Where to increment | Mechanism |
|---|---|---|
| `invocations.total` | Start of `QueryService.runQuery` handler closure (before `EngineService.runQueryWithHooks`) | `QueryMetrics.recordInvocation()` |
| `invocations.succeeded` | Inside the `case Right(qr)` arm of the handler's match | `QueryMetrics.recordSuccess()` |
| `invocations.failed` | Inside the `case Left(err)` arm (per-error-type: only `EngineError.QueryTimedOut` and `EngineError.AuditSinkUnavailable` count) | `QueryMetrics.recordTimeout()` / `recordAuditSinkUnavailable()` |
| `cache.hits` | Inside `CachePlugin.onPreExecute` when `cache.getJournaled(key)` returns `Some(_)` | `QueryMetrics.recordCacheHit()` (opt-in via plugin factory param) |
| `cache.misses` | Inside `CachePlugin.onPreExecute` when `cache.getJournaled(key)` returns `None` | `QueryMetrics.recordCacheMiss()` (opt-in) |

The `MetricsService.snapshot` handler's `snapshotRunner` closure is rewritten to delegate to `QueryMetrics.snapshot()` instead of returning zeros.

### Scope — what is NOT in this ADR

- **No Prometheus / Micrometer / `/metrics` HTTP export.** Separate ADR (mentioned as a 3rd follow-up in ADR-012-b's "Two follow-ups needed").
- **No new Restate services.** Wire surface stays exactly as PR-254 left it.
- **No changes to other handlers/services.** `ModelService`, `MetaInspectorService`, `QueryService.runQuery` semantics stay unchanged.
- **No atomicity across the 6 counters.** Each `AtomicLong.get()` is atomic, but the 6 reads in `snapshot()` are not a single atomic transaction. Documented in Scaladoc as acceptable for a diagnostic counter (worst case: counters disagree by 1 between concurrent reads + writes).

### Alternatives Considered

| Alternative | Reason rejected |
|---|---|
| `QueryMetrics` in `sm8-core` (engine-portable) | sm8-core is supposed to have zero sm8-platform deps; `QueryMetrics` is sm8-platform infrastructure. |
| `QueryMetrics` in `sm8-server` | The cache plugin runs in-process inside sm8-server's hook pipeline; placing the counter in sm8-server would force every plugin to import sm8-server — worse layer hygiene than sm8-platform. |
| Cache plugin mandates `QueryMetrics` (mandatory) | Breaks every existing `sm8-server` deployment that doesn't want metrics. Opt-in via `Option[QueryMetrics]` in the plugin factory keeps backward compatibility. |
| `AtomicReference[MetricsSnapshot]` swap-on-every-tick | Per-event CAS overhead; per the 3rd open question, non-atomic snapshot is acceptable for a diagnostic counter. |

### Layer discipline

Per `docs/rfcs/2026-08-12_v1_architecture-spec/` §3:

| Layer | What lands there |
|---|---|
| **sm8-platform** | New `QueryMetrics` object (singleton, `AtomicLong` fields). `MetricsService.snapshotRunner` delegates to it. `QueryService.runQuery` handler closure calls 3 record methods. |
| **plugins / cache-plugin** | New `Option[QueryMetrics]` constructor parameter on `CachePlugin` factory (default `None`). When `Some(qm)`, the hit/miss branches in `onPreExecute` call `qm.recordCacheHit()` / `qm.recordCacheMiss()`. |
| **sm8-server** | Construct `QueryMetrics` once at boot, pass `Some(...)` to the cache plugin factory. |
| **sm8-core** | 0 changes (the counter is consumed by sm8-platform code only; the cache plugin talks to sm8-core's `ResultCache` trait which is unaffected). |
| **adapters / connectors** | 0 changes. |

### Implementation sketch

```scala
// sm8-platform/src/main/scala/io/sm8/platform/query/QueryMetrics.scala
package io.sm8.platform.query

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration._

/**
 * Process-wide counter holder for query-pipeline metrics.
 *
 * Per [[scala-jvm-safety-mindset]] (no shared mutable state):
 * `AtomicLong` per counter gives concurrent-safe increments without
 * explicit locking. The 6 counters are independent — no cross-counter
 * invariant to maintain.
 *
 * Per [[scala-perf-testing-mindset]]: `AtomicLong.incrementAndGet` is
 * ~10ns per call. The hot path (QueryService.runQuery) calls 3 record
 * methods per request — ~30ns added overhead. Below measurement noise.
 *
 * Per [[ADR-012-b-followup]] §Decisions: snapshot atomicity is NOT
 * guaranteed (each `get()` is atomic, but the 6 reads are not a
 * single transaction). Acceptable for a diagnostic counter; documented
 * in Scaladoc on `MetricsService.snapshot`.
 */
object QueryMetrics {
  private val invocationsTotal      = new AtomicLong(0)
  // invocations.failed is intentionally NOT a separate counter — it
  // is computed in snapshot() as the sum of (auditSinkUnavailable +
  // timedOut), which are the only 5xx errors we currently treat as
  // "failed" (other EngineError variants have different semantics).
  // Adding a separate counter would create an invariant the test
  // suite would have to enforce; better to compute on read.
  private val cacheHits             = new AtomicLong(0)
  private val cacheMisses           = new AtomicLong(0)
  private val auditSinkUnavailable  = new AtomicLong(0)
  private val timedOut              = new AtomicLong(0)

  // Per-invocation record methods. Called from QueryService.runQuery's
  // handler closure. No-op for records that aren't applicable (e.g.
  // recordCacheHit is a no-op when no cache plugin is wired).
  def recordInvocation():        Unit = invocationsTotal.incrementAndGet()
  def recordSuccess():           Unit = invocationsSucceeded.incrementAndGet()
  def recordAuditSinkUnavailable(): Unit = auditSinkUnavailable.incrementAndGet()
  def recordTimedOut():          Unit = timedOut.incrementAndGet()
  // Cache hits/misses only fire if Some(QueryMetrics) was passed to
  // the cache plugin factory; the cache plugin guards each call.
  def recordCacheHit():           Unit = cacheHits.incrementAndGet()
  def recordCacheMiss():          Unit = cacheMisses.incrementAndGet()

  /** Called by MetricsService.snapshotRunner to read all counters. */
  def snapshot(uptimeSeconds: Long, startedAtIso: String): MetricsSnapshot =
    MetricsSnapshot(
      startedAt    = startedAtIso,
      uptimeSeconds = uptimeSeconds,
      invocations  = InvocationCounters(
                      total     = invocationsTotal.get,
                      succeeded = invocationsSucceeded.get,
                      // Failed = sum of specific 5xx error counters
                      // (the only ones currently treated as "failed";
                      // other EngineError variants have different semantics)
                      failed    = auditSinkUnavailable.get + timedOut.get),
      cache        = CacheCounters(
                      hits   = cacheHits.get,
                      misses = cacheMisses.get),
      errors       = ErrorCounters(
                      auditSinkUnavailable = auditSinkUnavailable.get,
                      timedOut             = timedOut.get)
    )
}
```

```scala
// sm8-platform/.../QueryService.scala — private runQuery method changes
// (the 3 record calls go INSIDE private runQuery, NOT at the
// HandlerRunner lambda level — that lambda just delegates to runQuery)
private def runQuery(
    request: QueryRequest,
    model: Model,
    registry: EngineRegistry,
    cache: ResultCache,
    dispatcher: HookRunnerOrchestration
): QueryResult = {
  QueryMetrics.recordInvocation()  // <-- NEW (PR-255)
  EngineService.runQueryWithHooks(
    request, model, registry, cache, dispatcher
  ) match {
    case Right(qr) =>
      QueryMetrics.recordSuccess()  // <-- NEW (PR-255)
      qr
    case Left(err) =>
      err match {
        case _: io.sm8.core.engine.EngineError.QueryTimedOut =>
          QueryMetrics.recordTimedOut()  // <-- NEW (PR-255)
        case _: io.sm8.core.engine.EngineError.AuditSinkUnavailable =>
          QueryMetrics.recordAuditSinkUnavailable()  // <-- NEW (PR-255)
        case _ =>  // other error types don't count toward "failed" — different semantics
      }
      val code = engineErrorCode(err)
      throw new dev.restate.sdk.common.TerminalException(code, err.toString)
  }
}
```

```scala
// sm8-platform/.../cache-plugin — factory signature change
// BEFORE: final class CachePlugin(val cache: ResultCache) extends Plugin
// AFTER:
final class CachePlugin(
    val cache: ResultCache,
    val metrics: Option[QueryMetrics] = None  // <-- NEW (PR-255)
) extends Plugin with java.io.Serializable {

  override def onPreExecute(...): HookResult = {
    val key = CacheBridge.platformCacheKey(...)
    cache.getJournaled(key) match {
      case Some(_) =>
        metrics.foreach(_.recordCacheHit())  // <-- NEW (PR-255)
        // existing short-circuit logic
      case None =>
        metrics.foreach(_.recordCacheMiss())  // <-- NEW (PR-255)
        // existing miss-handling logic
    }
  }
}
```

```scala
// sm8-server/.../Main.scala — wire the counter at boot
// Per PR-191 (audit 2026-08-27 [C2]): the deployment uses
// `PluginDiscovery.discoverFromConfig()` (factory pattern), not direct
// `new CachePlugin(...)` construction. Per PR-255: QueryMetrics is
// captured at boot (one instance for the whole process) and threaded
// into the CachePlugin factory via a ServiceLoader config property:
//
// 1. sm8-server constructs QueryMetrics ONCE at boot:
//    val queryMetrics = QueryMetrics
//
// 2. CachePlugin gains a static-set metric instance (set by sm8-server
//    before discovery):
//    object CachePlugin {
//      private var _sharedMetrics: Option[QueryMetrics] = None
//      def withSharedMetrics(qm: QueryMetrics): Unit = {
//        _sharedMetrics = Some(qm)
//      }
//    }
//
// 3. sm8-server calls CachePlugin.withSharedMetrics(queryMetrics)
//    BEFORE PluginDiscovery.discoverFromConfig()
//
// 4. CachePlugin's onPreExecute reads `CachePlugin._sharedMetrics`
//    instead of a constructor param — keeps the factory seam intact.
//    metrics.foreach(_.recordCacheHit())
//
// Total sm8-server wiring: ~2-3 LOC (construct + one withSharedMetrics call)
// Per ADR-009-c layering: CachePlugin stays in plugins/cache-plugin and
// reads a sm8-platform type (QueryMetrics) via a static seam — acceptable
// because sm8-platform is the canonical home for query-pipeline metrics.
// Alternative rejected: passing QueryMetrics through ServiceLoader META-INF
// (overkill for a single counter reference).
```

### Risks

| Risk | Mitigation |
|---|---|
| Race in 6-counter snapshot (concurrent increment + read) | Documented in Scaladoc: each `get()` is atomic, but the 6 reads are not. Worst case: counters disagree by 1 across concurrent reads. Acceptable for diagnostic UI. |
| Cache plugin signature change breaks existing deployments | New param has default `None`; existing deployments without `metrics` keep working. Migration is one-line in `sm8-server/Main.scala`. |
| AtomicLong overflow | `Long.MaxValue / (100k qps * 3600s)` ≈ 3.3 million years. Effectively unbounded at diagnostic rates. Trade-off: counters reset on process restart (acceptable for a diagnostic UI — operators understand "since last boot" semantics). |
| `QueryMetrics` becomes a god-object | 7 counters, all diagnostic. Easy to split later (e.g. `QueryMetrics` + `CacheMetrics` + `ErrorMetrics`) if scope grows. |

## Consequences

### Positive

- `/services/MetricsService/snapshot` returns **real** counter values — closes the ADR-012-b wire-surface → real-instrumentation half of the cycle
- Operators see actual `invocations.total` climbing per request; cache hit rate is now meaningful (currently `0/0` which is useless)
- Foundation for downstream UI dashboard work (the 3rd ADR-012-b follow-up: Prometheus export)
- Zero production-code risk to existing services — only adds record calls

### Negative

- Snapshot atomicity is not guaranteed (see Risks)
- Cache plugin factory signature changes (additive — backward compatible via `Option`)

## Out-of-Scope Follow-ups

- **Prometheus / Micrometer export** — separate ADR (the 3rd follow-up from ADR-012-b). Wire surface (`MetricsService.snapshot`) is already Prometheus-friendly (text-format would just need an HTTP `/metrics` route alongside the Restate ingress).
- **A/B-testing model versions in production** — separate ADR. This one enables counters; doesn't define the routing.

## Verification criteria (for PR-255 implementation)

The implementation PR must satisfy all of:

1. **Live counters** — `/services/MetricsService/snapshot` returns non-zero values after at least 1 successful `QueryService.runQuery` invocation:
   - `invocations.total` ≥ 1
   - `invocations.succeeded` ≥ 1
   - `cache.hits` ≥ 0 AND `cache.misses` ≥ 0 (depends on cache state; both are valid)
2. **Per-error-type counters** — `errors.auditSinkUnavailable` increments when `EngineError.AuditSinkUnavailable` is raised; same for `timedOut` + `EngineError.QueryTimedOut`
3. **Backward compat** — cache plugin compiled with `metrics = None` continues to work (constructors without the new param)
4. **Cached snapshot shape** — `MetricsSnapshot` JSON shape unchanged from PR-254 (the new `invocations.failed` is computed in the snapshot reader, not added as a new field)
5. **Unit tests** — `QueryMetricsSpec.scala` covers all 6 record methods + the `snapshot()` reader (7 tests total, as called out in the Implementation Plan):

   | # | Test name | Asserts |
   |---|---|---|
   | 1 | `recordInvocation increments total` | after N calls, `invocationsTotal.get == N` |
   | 2 | `recordSuccess increments succeeded` | after N calls, `invocationsSucceeded.get == N` |
   | 3 | `recordCacheHit increments hits` | after N calls, `cacheHits.get == N` |
   | 4 | `recordCacheMiss increments misses` | after N calls, `cacheMisses.get == N` |
   | 5 | `recordAuditSinkUnavailable increments auditSinkUnavailable` | after N calls, `auditSinkUnavailable.get == N` |
   | 6 | `recordTimedOut increments timedOut` | after N calls, `timedOut.get == N` |
   | 7 | `snapshot computes failed = auditSinkUnavailable + timedOut` | after 2 audit + 3 timeout, `invocations.failed == 5` |

6. **Smoke** — `/scripts/smoke-e2e.sh --external-ip <addr>` adds a new assertion: after the existing 1 QueryService.runQuery invocation, `/services/MetricsService/snapshot` returns `invocations.total == 1`
7. **Layer discipline** — `sm8-core` has 0 production-code changes; `sm8-platform` is the only module that imports `QueryMetrics`; the cache-plugin reads `QueryMetrics` via the static seam (per PR-191 layering convention)

## References

- `docs/adr/0012-b-metricsservice-restate-handler.md`: parent ADR; this is the follow-up
- PR-254 (`d0c15ee`): the wire surface this follow-up makes real
- PR-191 (MetaCaptureObserver): precedent for in-process atomic state captured at boot and shared across handlers
- ADR-009-g (CachePolicy contract): precedent for cache hooks gating on folded values
- ADR-009-d (broadcast/skew via context.meta): precedent for capturing plugin state via a side channel
- `building-restate-services` skill: "no native random/time/sleep" rule applies — `QueryMetrics` has no time access; counter operations are pure

---

# Revision history

| Version | Date | Author | Change |
|---|---|---|---|
| 1 | 2026-09-01 | SM8 agent (PR-255) | Initial ADR. Proposed status. Counter holders + 3 architectural decisions (sm8-platform home, opt-in cache-plugin, non-atomic snapshot + Scaladoc note) + concrete file list. |
| 2 | 2026-09-01 | SM8 agent (PR-255 r1 amend) | r1 dual-review fixes: (a) sketch placement corrected — record calls go in `private def runQuery`, NOT the HandlerRunner lambda; (b) overflow math corrected from "~12 hours at 100k qps" to "≈3.3 million years, effectively unbounded; resets on process restart"; (c) dead code removed — `invocationsFailed: AtomicLong` field + `recordFailure()` method deleted (snapshot computes failed as `auditSinkUnavailable + timedOut`); (d) CachePlugin integration now uses a static seam (`object CachePlugin { withSharedMetrics(qm: QueryMetrics) }`) instead of constructor param, preserving the `PluginDiscovery.discoverFromConfig()` factory pattern; (e) added Verification criteria section with 7 explicit acceptance tests. |
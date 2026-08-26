# SM8 — ADR-009-f Paired Persist Lifecycle Retrospective

**Date:** 2026-08-26
**Branch:** `main` @ `821d270` (PR-180 merged)
**Scope:** the paired persist/unpersist lifecycle in `connectors/spark-connector` — closes 6 pre-existing persist/unpersist + cache lifecycle gaps (including ADR-008-P CROSS-P0-B)
**ADR:** [ADR-009-f](docs/adr/0009-f-paired-persist-lifecycle.md) — promoted to **Implemented**
**PR:** [PR-180](https://github.com/EchoEnv/sm8/pull/180) (4 atomic implementation commits + 5 ADR docs folds, squash-merged)
**Skill alignment:** `karpathy-app-design`, `karpathy-guidelines`, `scala-spark-batch-bugs`, `scala-impact-analysis`, `scala-bug-hunting`, `scala-error-handling`, `scala-jvm-safety`, `scala-perf-testing`, `scala-data-driven-refactor`, `scala2-scaladoc`, `debug-mantra`

---

## 1. TL;DR

The paired persist/unpersist lifecycle is **live in production**. Every persisted frame in `SparkEngineProvider.applyPostCompilePipeline` is now `trackPersist`-ed before filter/limit and `untrackPersist`-ed on every exit path (success, failure, JVM-shutdown via `close()`). `MaterializePolicy.Cache` is a typed `UnsupportedCapability` rejection — no more silent no-op. The dual `MaterializePolicy` ADTs collapse to one (`io.sm8.core.model`); the dead `EngineContext.materializePolicy` field + its 5 test sites are deleted. A new typed `EngineError.PersistLifecycleFailed(phase, cause, message)` + `PersistPhase` sealed trait flow through the dispatcher's `engineErrorCode` 13th case → HTTP 502. The `close()` loop's token-in-log breadcrumb is unconditional (v3.4 architect P2 restructure: per-frame NonFatal capture → stderr logging → narrow `spark.stop()` defense).

The ADR went through **6 revisions** (v0.1 → v1.0 → v2.0 → v3.1 → v3.2 → v3.6). v0.1 and v1.0 were BLOCKED by dual review; v3.6 folded one docs tightening (cause-priority vs infeasible `getSuppressed.length == 1`) — final state is self-consistent across key-invariants, acceptance criteria, and the implementation at `SparkEngineProvider.scala:640`.

**Final state**: full reactor green (15 modules, 634/634 core + 241/241 spark-connector + 103/103 platform + 6/6 cli + 33/33 server). Dual reviewer verdict APPROVED on the final tree (data-eng best-coding 0.9, architect best-reasoning 0.95). No follow-ups from this ADR.

---

## 2. What landed

### 2.1 The paired-lifecycle invariant in `applyPostCompilePipeline`

Every persisted frame entering the pipeline is now registered BEFORE filter/limit, and released on every exit path. The token is the canonical gate on both success and failure paths:

```scala
private[spark] def applyPostCompilePipeline(
    df: DataFrame, request: QueryRequest,
    schemaMetadata: Map[String, String],
    cap: Long = SparkEngineProvider.DefaultResultCapRows): Either[EngineError, PortableQueryResult] = {
  // v3.2 Fix 1: register the persisted frame BEFORE any filter/limit,
  // so the unregister-on-exit is deterministic. registerToken == 0L is
  // the sentinel "nothing tracked" (no upstream persistence).
  val registerToken: Long =
    if (!df.storageLevel.equals(StorageLevel.NONE)) trackPersist(df) else 0L

  val collected: Either[EngineError, Array[Row]] =
    try { Right(withLimit.collect()) }
    catch {
      case NonFatal(collectErr) =>
        if (registerToken != 0L) {
          // Failure path WITH a tracked frame: typed Left only.
          // addSuppressed at line 640 chains the unpersist fault onto
          // the LOCAL collectErr Throwable — code-review-verifiable
          // (the typed Left carries cause+message strings only).
          try df.unpersist()
          catch { case NonFatal(u) => collectErr.addSuppressed(u) }
          untrackPersist(registerToken)
          Left(EngineError.PersistLifecycleFailed(
            engine  = sparkEngineName,
            phase   = PersistPhase.Unpersist,
            cause   = collectErr.getClass.getSimpleName,
            message = collectErr.getMessage))
        } else {
          // No tracked frame: not a lifecycle failure. Propagate per
          // ADR-009-e non-swallow contract (SparkException thrownBy).
          throw collectErr
        }
    }
  // ... success-path decode + paired unpersist + untrack ...
}
```

**Key invariants** (the v3.6 self-consistent set):
- **No throw on either path** — every failure returns `Left(...)` (typed). The dispatcher's NonFatal catch never sees a `Throwable` from this code path; the typed case is reachable.
- **`addSuppressed` chains the original exception (LOCAL discipline)** — line 640 `case NonFatal(u) => collectErr.addSuppressed(u)` builds the chain on the LOCAL Throwable. The strongest observable regression net is cause-priority: `Left.cause == collectErr.className`, `Left.message includes collectErr.message`, `Left.message NOT includes unpersist-fault.message`. A contributor who swaps the `addSuppressed` for a swallow AND flips the Left's cause would fail a future black-box cause-priority test.
- **`NonFatal` (not `Throwable`)** — per PR-176 discipline: `Error` subclasses (OOM, StackOverflow) propagate uncaught; `InterruptedException` re-interrupts.
- **`untrackPersist` after success AND failure** — the tracked-frame map is empty on every exit; the close() sweep iterates zero entries when no query is in flight.

### 2.2 The single-source `MaterializePolicy` ADT

Dual ADTs collapse to one. `io.sm8.core.engine.MaterializePolicy` (with `MemoryOnly`/`MemoryAndDisk`/`EngineDefault` cases — zero readers) is **deleted**. `io.sm8.core.model.MaterializePolicy` (the active shape — `None`/`Persist(level)`/`Cache`) is the single source of truth. The dead `EngineContext.materializePolicy` field + 5 `EngineContextSpec` test sites are deleted (no migration target — the field had zero consumers).

`MaterializePolicy.Cache` becomes a typed rejection in `PortableQueryCompiler.applyAggregations`:

```scala
case Some(MaterializePolicy.Cache) =>
  Left(EngineError.UnsupportedCapability(
    capability = "MaterializePolicy.Cache",
    engine = "spark-3.5",
    message = "For connector-side materialization use MaterializePolicy.Persist(level) first; " +
              "for cache-plugin-side use CachePolicy.ReadThrough (NOT a MaterializePolicy)."))
```

The v0.1 "Cache falls through to `case _ => Right(result)`" silent no-op is gone. PR-178 discipline extends: silent no-ops are contract violations just as silent drops were — typed rejection or typed registration, no third option.

### 2.3 The typed `PersistLifecycleFailed` error path

A new typed case class in `sm8-core/engine/EngineError.scala`:

```scala
final case class PersistLifecycleFailed(
  engine: String,
  phase: PersistPhase,
  cause: String,
  message: String) extends EngineError

sealed trait PersistPhase
case object Persist extends PersistPhase
case object Unpersist extends PersistPhase
```

The dispatcher's `QueryService.engineErrorCode` exhaustive 12-case match (at lines 264-276) gains a 13th case:

```scala
case _: PersistLifecycleFailed => 502
```

(same wire code as `ProviderInvocationFailed` — both backend-side, retriable). Fix 6+6b atomicity mandate: `PersistLifecycleFailed` + the 13th `engineErrorCode` case landed in the SAME commit (`88950db`) — each alone breaks the build; both absent cancel out green.

### 2.4 The non-swallow `close()` loop with unconditional token-in-log

The v3.4 restructure (architect P2) is a three-phase machine:

```scala
override def close(): Unit = {
  import scala.collection.JavaConverters._
  import scala.collection.mutable.ListBuffer
  // Phase 1: per-frame NonFatal capture
  val unpersistFailures = ListBuffer.empty[(java.lang.Long, Throwable)]
  persistedFrames.asScala.foreach { case (tok, df) =>
    try df.unpersist()
    catch { case NonFatal(e) => unpersistFailures += ((tok, e)) }
  }
  persistedFrames.clear()
  // Phase 2: UNCONDITIONAL token-in-log stderr (no outer wrapper).
  // stderr is a system call — cannot be affected by SparkSession teardown.
  unpersistFailures.foreach { case (tok, e) =>
    System.err.println(
      s"sm8: SparkEngineProvider.close() unpersist failed " +
      s"(token=$tok, engine=$sparkEngineName, " +
      s"${e.getClass.getSimpleName}: ${e.getMessage})")
  }
  // Phase 3: narrow spark.stop() defense (best-effort at JVM-shutdown)
  if (spark != null)
    try spark.stop()
    catch { case NonFatal(_) => () }
}
```

The previous shape wrapped EVERYTHING in a single outer `try { ... } catch { case NonFatal(_) => () }`. If `spark.stop()` threw NonFatal (SparkException from broken RPC, IllegalStateException if already stopped), the outer catch swallowed it AND skipped the breadcrumb logging — the exact silent-swallow class the ADR Fix 2b was introduced to close. The restructure moves the logging OUT of the outer try so the breadcrumb always reaches stderr.

### 2.5 The test seams (production seams, NOT test-only)

Per the v3.1 data-eng findings, the test surface is hooked into the production class via `private[spark]` seams (NOT test-only decorators in the spec):

- `private[spark] def persistedFramesSize: Int` at line 189 — observable size of the tracked-frame map so acceptance #1/#2 can assert pair invariant without reflection.
- `private[spark] def applyPostCompilePipeline(df, req, meta, cap, forceUnpersistFault: Boolean)` at line 718 — test-only overload that wraps `df` in a `ThrowingUnpersistDataset` decorator if `forceUnpersistFault = true`. The decorator lives in PRODUCTION at line 702 (because the production overload must reference it from compiled production code — a test-only decorator in the spec file would be unreachable from the compiled class).
- `private[spark] class ThrowingUnpersistDataset(df: DataFrame) extends Dataset[Row](df.sparkSession, df.queryExecution.logical, Encoders.row(df.schema))` at line 702 — overrides both `unpersist()` and `unpersist(blocking: Boolean)` to throw `SparkException("forced unpersist fault")`. Spark 3.5's 3-arg `(SparkSession, LogicalPlan, Encoder[Row])` ctor (the subagent's deviation #2 from the ADR-anticipated 2-arg shape — the ADR explicitly allowed this fallback).

### 2.6 The falsifiable acceptance criteria (10 total)

| # | Criterion | Test seam | Status |
|---|---|---|---|
| 1 | Paired register + unregister on every exit path | direct `trackPersist` + `provider.query()` + `persistedFramesSize` | GREEN |
| 2 | close() unpersists every tracked frame + token-in-log on failure | direct `trackPersist` × 5 + `close()` + per-frame injection | GREEN |
| 3 | Non-swallow unpersist with exception-shadowing chain | code-review-verifiable (line 640) — Spark 3.5 storageLevel-NONE constraint makes single-test both-fail infeasible; cause-priority form documented for any future test seam | DOCUMENTED TRADE-OFF |
| 4 | Success-path unpersist failure returns typed Left | `forceUnpersistFault=true` overload + `plf.phase == Unpersist` | GREEN |
| 5 | `MaterializePolicy.Cache` returns typed `UnsupportedCapability` | `applyAggregations(Cache)` → `Left(UnsupportedCapability)` | GREEN |
| 6 | Dead-field deletion + 5 test sites removed | grep + reactor build | GREEN |
| 7 | Single-source ADT | grep `io.sm8.core.engine.MaterializePolicy` → 0 matches | GREEN |
| 8 | Typed `PersistLifecycleFailed` reachable end-to-end | DI seam `forceUnpersistFault=true` + `plf.engine/phase/cause/message` assertions | GREEN |
| 9 | `engineErrorCode` 13th case → 502 | existing 12-case coverage + `PersistLifecycleFailed` wire-level spec | GREEN |
| 10 | No regression on ADR-009-e path | existing 7 ADR-009-e falsifiables + 4 follow-ups + cap+1 + wasPersisted + truncated + cache HIT | GREEN |

---

## 3. The review chain — 6 ADR revisions, 2 final code-review rounds

| Round | Reviewer | Verdict | Folded |
|---|---|---|---|
| v0.1 | architect (best-reasoning) | REJECTED 0.85 | 4 P1s: Fix 1 call site, Fix 2 exception-shadowing, Gap 6 close() swallow, false blast-radius claim |
| v1.0 | data-eng (best-coding) | REJECTED 0.72 | 5 P1/P2s: build-breaker, missing `untrackPersist`, typed case unreachable, success-path undefined, migration target nonexistent |
| v2.0 | architect (best-reasoning) | REJECTED 0.85 | 2 P1s: Fix 2 LUB type bug + missing `buildPortableResult`; 1 P2: unfalsifiable acceptance #4; 1 P3 |
| v3.0 | data-eng (best-coding) | INCORRECT 0.97 (workflow-stage mismatch — Accepted = decision, not impl) | 4 stage-independent hardening findings: test seam, atomicity mandate, explicit DI seam, status disambiguation |
| v3.1 | data-eng (best-coding) | REJECTED 0.97 | 4 stage-independent hardening folds from v3.0 |
| v3.2 | self | ACCEPTED | 2 pre-impl spec corrections: `NonFatal(collectErr)` extractor (not type pattern); scoped typed Left only when `registerToken != 0L` |
| v3.3 | data-eng final code review of PR-180 | APPROVED 0.9 | 2 P3s folded as documented trade-offs: addSuppressed not black-box testable in Spark 3.5 (Project-over-InMemoryRelation); #2 direct-trackPersist test seam is meaningful for JVM-shutdown sweep |
| v3.4 | architect initial code review of PR-180 | FAILED — OmniRoute 503 admission-busy after 10 retries | retry-budget-wastage protection added |
| v3.4 | architect full code review | NEEDS-CHANGES 0.85 | 1 P2 (close() outer try swallows token-in-log breadcrumb on spark.stop() NonFatal) + 1 P3 (stale comment) |
| v3.5 | architect re-review | APPROVED 0.95 | 2 P3 doc nits: stale line ref (line 210 → 230), revision row order |
| v3.6 | self | ACCEPTED | 1 docs tightening: Fix 2 key-invariants bullet + acceptance #3 corrected to cause-priority falsifiable form (NOT `getSuppressed.length == 1` — infeasible against typed Left) |

---

## 4. Key decisions (rationale + ADR anchors)

1. **Pair in connector, not in compiler** (Fix 1): `trackPersist` is `private[spark]` on `SparkEngineProvider`; `PortableQueryCompiler.applyAggregations` is constructed standalone with no enclosing SparkEngineProvider ref. The original v0.1 proposal to call `trackPersist` from `applyAggregations` was unreachable. Moved to `applyPostCompilePipeline`, which already does the `wasPersisted` derivation and is the only caller-side seam that holds both the original `df` and the registration token. v1.0 architect P1.

2. **Typed Left, never throw** (Fix 2): v0.1's `finally`-throw can replace the original `collect()` exception when both fail (same root cause: the executor that failed is the one holding the persisted block). v2.0 typed case unreachable on failure path: the dispatcher's NonFatal catch wraps any `NonFatal(e)` as `EngineError.ProviderInvocationFailed`, NOT `EngineError.PersistLifecycleFailed`. EngineError is a sealed trait (not Throwable), so "re-throws as EngineError" cannot be implemented via throw. v2.0 data-eng P1.

3. **`NonFatal(collectErr)` extractor, not `case collectErr: NonFatal`** (Fix 2 v3.2): `NonFatal` is an object with `unapply` (extractor), not a type — the v2.0 snippet's `case collectErr: NonFatal` does NOT compile. Caught by re-reading before hand-off.

4. **Scoped typed Left only when `registerToken != 0L`** (Fix 2 v3.2): typed `Left(PersistLifecycleFailed(...))` was returned unconditionally on the failure path; but when `registerToken == 0L` (no frame was persisted), the collect() failure is a plain query failure, NOT a persist-lifecycle failure. Corrected: only when `registerToken != 0L` return the typed Left; else rethrow to preserve the ADR-009-e non-swallow contract (existing falsifiable test asserts a `SparkException` propagates). v3.2 pre-impl.

5. **Cache typed-reject, not fallback** (Fix 3): PR-178 discipline extends to the model-side ADT — silent no-ops are contract violations. Cache falls through `case _ => Right(result)` is the worst class of silent default. Typed `UnsupportedCapability` rejection with actionable message naming `Persist(level)` first (currently-wired) and `CachePolicy.ReadThrough` second (cache-plugin-side, NOT a MaterializePolicy).

6. **Single-source ADT, delete engine-side** (Fix 4+5): dual `MaterializePolicy` ADTs are a future drift hazard. Contributor picks wrong import → silent no-op. Collapse to one (`io.sm8.core.model`), delete engine-side ADT + dead `EngineContext.materializePolicy` field + 5 test sites.

7. **`PersistLifecycleFailed` as a new typed case, not a `ProviderInvocationFailed` alias** (Fix 6): the load-bearing persist feature deserves its own error case over the catch-all. Operationally distinguishable (a deploy team investigating driver-memory growth can grep stderr for `persist-lifecycle` instead of all `PROVIDER_INVOCATION_FAILED`). Wire code 502 same as `ProviderInvocationFailed` (both backend-side, retriable).

8. **`addSuppressed` chain on the LOCAL Throwable, NOT on the typed Left** (Fix 2 v3.6 docs tightening): a sealed-trait case class cannot carry a Throwable. The chain lives on the local `collectErr` inside `applyPostCompilePipeline`. The strongest observable regression net is cause-priority (`Left.cause == collectErr.className`, `Left.message includes collectErr.message`, `Left.message NOT includes unpersist-fault.message`). Code-review-verifiable per `SparkEngineProvider.scala:640`.

9. **Test seams in PRODUCTION, not spec** (Fix 1/2 v3.1): `persistedFramesSize`, the `forceUnpersistFault` overload, and `ThrowingUnpersistDataset` live in `private[spark]` scope on `SparkEngineProvider` (not in the spec). Reason: the production overload at line 718 must reference `ThrowingUnpersistDataset` from compiled production code; a test-only decorator in the spec file would be unreachable from the compiled class. Scope `private[spark]` keeps the seams connector-only.

10. **close() three-phase machine** (Fix 2b v3.4 architect P2): outer try wraps NOTHING (logging is unconditional); per-frame NonFatal capture → stderr with token (system call, no Spark dependency) → narrow `spark.stop()` defense. The breadcrumb always reaches stderr even when `spark.stop()` throws NonFatal.

---

## 5. Skill alignment

- **`karpathy-app-design`** (single source of truth, typed boundaries): the dual `MaterializePolicy` ADTs collapse to one; the engine-side ADT + 5 test sites are deleted. RFC §3 Core Boundary honored: typed `MaterializePolicy` in `sm8-core/model` (engine-portable data shape); the connector enforces + registers the lifecycle; deployment config stays the deployment layer.

- **`karpathy-guidelines`** (surgical edits; smallest correct change; dead code is a smell): the dead `EngineContext.materializePolicy` field is deleted (no migration); the `MaterializePolicy.MemoryOnly`/`MemoryAndDisk`/`EngineDefault` cases are deleted (no readers); 5 `EngineContextSpec` test sites are deleted (no replacement). YAGNI: no new `MaterializePolicy.Register(level)` case (Option B rejected — re-introduces dual ADT); no separate `Unbounded` escape (Option C-v2 rejected — footgun with no current consumer; can be added later as additive sealed trait if real demand surfaces).

- **`scala-spark-batch-bugs`** (driver memory + `.limit()` resets `storageLevel` + persist-on-aggregate lifecycle): the v3.2 `registerToken` gate is derived from the PASSED-IN `df` storageLevel BEFORE filter/limit (Spark 3.5's `.filter()`/`.limit()` build new uncached logical plans, so `withLimit.storageLevel` is always `StorageLevel.NONE` even when the upstream aggregate frame was persisted — ADR-008-P paired persist). ADR-009-e comment at `SparkEngineProvider.scala:562-569` documents this invariant.

- **`scala-bug-hunting`** (silent no-ops are bugs; dead fields are bugs): the Cache silent no-op is gone (typed rejection); the dead `EngineContext.materializePolicy` is gone (field deleted). The v3.6 docs tightening fixed a third silent-no-op class: the `getSuppressed.length == 1` acceptance #3 wording claimed a falsifiable property of the returned value that doesn't exist — replaced with the cause-priority form.

- **`scala-error-handling`** (typed `EngineError` over `Throwable` swallow; `Either` over `try`/`catch` for expected domain errors; PR-176 NonFatal discipline extends): every failure returns `Left(...)` (typed); `Throwable` catches are eliminated; `Error` subclasses (OOM, StackOverflow) propagate uncaught; `InterruptedException` re-interrupts.

- **`scala-jvm-safety`** (resource lifecycle; unpersist failures indicate real Spark executor state problems): `close()` is non-swallow with token-in-log breadcrumb (v3.4 restructure). Operator post-mortem can correlate the log line to a specific persisted query via the token.

- **`scala-impact-analysis`** (blast radius): 4 `PortableQueryResult(...)` constructor sites (ADR-009-e); 9 `applyAggregations` call sites; 13 `engineErrorCode` match arms (12 → 13 cases, exhaustive match). Every site compiles unchanged with the additive fields/cases. ZERO case-pattern matches on the `PersistLifecycleFailed` ADT before this ADR — the new case is purely additive on the ADT.

- **`scala-perf-testing`** (allocation budget): `df.count()` is NOT on the hot path; `collected.length` is O(1) on the materialized array (ADR-009-e comment at lines 587-590). The `registerToken` check is a single `df.storageLevel.equals(StorageLevel.NONE)` — constant-time.

- **`scala-data-driven-refactor`** (single-source ADT): the dual `MaterializePolicy` ADTs are a textbook data-vs-shape drift hazard. Collapse to one is the minimum correct change.

- **`scala2-scaladoc`** (Scaladoc style): every `private[spark]` test seam carries a "NOT a production API — exists so the paired-lifecycle acceptance tests can assert ..." comment. Every fix in the §Decision section carries a "vX.Y origin" anchor (the parent review that surfaced the fix). The `addSuppressed` discipline is documented in BOTH the Fix 2 key-invariants AND the acceptance #3 v3.6 amendment (cross-referenced).

- **`debug-mantra`** (reproduce → trace → falsify → cross-reference → verify): the falsifiable acceptance test plan in §Decision (10 tests) maps 1-to-1 to the implementation tests in `SparkEngineProviderSpec` (acceptance #1, #2, #4, #5, #8, #9, #10) + `PortableQueryCompilerSpec` (acceptance #5) + the `mvn -B -ntp test` reactor (acceptance #6, #7, #10 regression). Every falsifiable claim is verified by a test that would fail under the contrary implementation.

---

## 6. Next options (post-ADR-009-f)

1. **ADR-009-g + ADR-009-h (next v0.4 wave)**: see ADRs README for the open backlog. Likely candidates: cache-plugin contract closure (`CachePolicy.ReadThrough` is the second half of the cache feature; ADR-009-f's `Cache` rejection explicitly mentions it); `MaterializePolicy.Cache` typed-rejection is the closure of one half but the cache plugin contract itself remains OPEN.

2. **`v0.1.0` tag** — blocked by user's "don't bump version yet" directive. When unblocked: tag `7b82362..821d270` (PR-178 → PR-179 → PR-180 = the v0.4 wave), bump `pom.xml` `<version>`, write `docs/project_status/2026-XX-XX-v0.4-wave-retrospective.md`.

3. **D1 scaladoc sweep** — defer until after v0.1.0 tag; per memory the standing directive includes "good scaladoc based on skill scala2-scaladoc". The v3.6 docs tightening at ADR-009-f + the v3.5 architect P3 fold are placeholders for a broader sweep.

4. **Fix-wave retrospective (this doc)** — completed. No follow-up actions from ADR-009-f itself.

5. **Hygiene** — local `adr/009-f-paired-persist-lifecycle` branch deleted; remote-tracking retained (GitHub default for merged PRs). Memory 47%, disk 75%, 4 codegraph procs (2-tier: session MCP + singleton daemon; not duplicates), 0 metals, 0 bloop.
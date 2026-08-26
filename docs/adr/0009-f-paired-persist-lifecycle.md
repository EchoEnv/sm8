# ADR-009-f: Paired persist lifecycle — typed registration, non-swallow unpersist, single-source `MaterializePolicy`

| Field | Value |
|---|---|
| **Status** | **Proposed (v1.0)** — architect review (best-reasoning, 6m 0.85 confidence) rejected v0.1 as "incorrect as written": execution under-specified, two P1s in the fix shape (Fix 1 unreachable call site; Fix 2 exception-shadowing), one P1 in the audit (close() has the same Throwable swallow — a 6th gap missed), one P1 in the blast-radius claim (5 test sites in EngineContextSpec reference the field/cases). All findings folded into this revision. Pending data-eng review (best-coding, serial). |
| **Date** | 2026-08-26 |
| **Module** | `connectors/spark-connector/.../SparkEngineProvider.scala` (`applyPostCompilePipeline` does the paired `trackPersist`/`untrackPersist` — the only place that can reach the `private[spark]` registrar; `close()` unpersist loop is non-swallow too) + `connectors/spark-connector/.../PortableQueryCompiler.scala` (`MaterializePolicy.Cache` typed-rejection; `applyAggregations` stays a pure compile step — no registration there) + `sm8-core/engine/EngineError.scala` (new typed `PersistLifecycleFailed` case for the unpersist-failure path; the load-bearing persist feature deserves its own error case over the catch-all `ProviderInvocationFailed`) + `sm8-core/engine/EngineContext.scala` (`materializePolicy` removed; 5 sites in `EngineContextSpec.scala` deleted) + `sm8-core/model/Model.scala` (the single-source `MaterializePolicy.Persist`/`Cache` ADT) + `sm8-core/test/.../EngineContextSpec.scala` (5 sites referencing the deleted field/cases — updated to test the consolidated model-side ADT instead) |
| **Supersedes scope** | The pre-existing persist/unpersist-lifecycle gaps surfaced by the PR-176 / PR-179 wave and ADR-008-P's CROSS-P0-B (still OPEN): (1) `applyAggregations` calls `result.persist(...)` but never registers the persisted frame in `SparkEngineProvider.persistedFrames` — `close()` iterates an empty map; (2) the `finally`'s `unpersist()` at `SparkEngineProvider.scala:580-590` swallows `Throwable` to a stderr log instead of a typed `EngineError`; (3) `MaterializePolicy.Cache` is a silent no-op (falls through `applyAggregations` as `case _ => Right(result)`); (4) `EngineContext.materializePolicy: io.sm8.core.engine.MaterializePolicy` is dead — declared, defaulted, never read (5 test sites in `EngineContextSpec.scala` reference it — false "zero readers" claim in v0.1, corrected); (5) two `MaterializePolicy` ADTs coexist (`io.sm8.core.engine.MaterializePolicy` with `MemoryOnly`/`MemoryAndDisk`/`EngineDefault` + `io.sm8.core.model.MaterializePolicy` with `Persist(level)`/`Cache`), only one is active. v1.0 also surfaces a (6) `close()` unpersist loop at `SparkEngineProvider.scala:176-178` swallows `Throwable` to nothing — the symmetric anti-pattern to Gap 2, surfaced by the architect review. |
| **Skill alignment** | `karpathy-app-design` (single source of truth, typed boundaries), `karpathy-guidelines` (surgical edits; smallest correct change; dead code is a smell), `scala-spark-batch-bugs` (driver memory + `.limit()` resets `storageLevel` + persist-on-aggregate lifecycle), `scala-bug-hunting` (silent no-ops are bugs — `Cache` falls through; dead fields are bugs — `EngineContext.materializePolicy`), `scala-jvm-safety` (resource lifecycle; `unpersist` failures indicate real Spark executor state problems), `scala-error-handling` (typed `EngineError` over `Throwable` swallow; `Either` over `try`/`catch` for expected domain errors; PR-176 NonFatal discipline extends), `scala-impact-analysis` (dual ADTs are a blast-radius hazard — every caller of one must migrate to the other), `scala-data-driven-refactor` (closed sealed-trait dispatch; remove unused cases), `scala-perf-testing` (no extra Spark action on the hot path; register at the boundary, not after `collect`), `scala2-scaladoc` (WHY prose on every public surface), `debug-mantra` (falsifiable acceptance per finding). |
| **Architecture alignment** | RFC §3 Core Boundary: `MaterializePolicy` lives once in `sm8-core/model` (engine-portable data shape); the connector enforces + registers the lifecycle; the cache-plugin manages its own cache lifecycle (separate concern, no dual implementation); deployment config stays the deployment layer. PR-178 discipline extends: silent no-ops (Cache falls through) are contract violations just as silent drops were — typed rejection or typed registration, no third option. |

---

## Revision history

| Version | Date | Change |
|---|---|---|
| v0.1 (Proposed) | 2026-08-26 | Initial draft; 5 findings (paired-register, non-swallow unpersist, Cache typed reject, single-source ADT, dead `EngineContext.materializePolicy`); 4 options considered (Option A: closure via paired register + typed errors + single-source ADT; rejected Options B/C/D detailed in §Decision). Investigation files: this audit, codegraph probes (`persist unpersist`, `MaterializePolicy`, `cache plugin InMemoryResultCache`), `cross-engine audit`, `/tmp/oom-surfaces-investigation.md`. |
| v1.0 (Proposed) | 2026-08-26 | Architect review (best-reasoning, 0.85 confidence) rejected v0.1. 4 P1s folded: (a) **Fix 1 call site** — `trackPersist` is `private[spark]` on `SparkEngineProvider`; unreachable from `PortableQueryCompiler.applyAggregations` (which is constructed standalone). Moved to **`applyPostCompilePipeline`**, which already does the `wasPersisted` derivation and is the only caller-side reach point. `applyAggregations` stays a pure compile step. (b) **Fix 2 exception-shadowing** — `finally`-throw can replace the original `collect()` exception when both fail (same root cause: executor that failed is the one holding the persisted block). Use `Throwable.addSuppressed` to chain the original `collect()` exception onto the `unpersist()` failure. (c) **Gap 6** — `close()` unpersist loop at `SparkEngineProvider.scala:176-178` swallows `Throwable` to nothing. Same anti-pattern as Gap 2; new fix `close()` unpersist to typed errors too. (d) **Blast-radius claim false** — `EngineContextSpec.scala` has 5 sites referencing the field/cases (lines 16-23, 27-28, 117, 127, 134). v0.1's "zero callers" was wrong; test sites count. 3 P2s folded: typed `EngineError.PersistLifecycleFailed` case; Cache rejection message order (Persist first, currently-wired); `Persist(level: String)` engine-specific drift hazard named for future ADR. 3 P3s folded: acceptance #3 concrete test seam (close-time unpersist path on a tracked frame); acceptance #5 grep widened to src/main+src/test; per-query session storage boundary documented. |

---

## Context and problem statement

The persist/unpersist lifecycle in `connectors/spark-connector` has five distinct gaps, each independently reproducible and each silently degrading the system:

### Gap 1 — `trackPersist` is dead in production (`SparkEngineProvider.scala:155-181`)

The connector declares a `@transient val persistedFrames: ConcurrentHashMap[Long, Dataset[_]]` (line 155-156), a private `trackPersist(df: Dataset[_]): Long` registrar (line 164-167), and a `close()` that iterates `persistedFrames` and calls `df.unpersist()` on every tracked frame (line 174-180). The mechanism exists; **nothing calls it in production.** `applyAggregations` at line 506-519 calls `result.persist(StorageLevel.fromString(level))` on the aggregated DataFrame but does not register the persisted frame — the persisted DataFrame reference is returned to the caller and goes out of scope. `close()` therefore iterates an always-empty `persistedFrames` map (the test-only spec at `SparkEngineProviderCloseLifecycleSpec.scala:46` exercises the registration path; production never reaches it).

Consequence: every persisted frame leaks until the JVM exits (relying on Spark's own GC + storage cleanup, which is best-effort and not contractually bounded). For a model with `MaterializePolicy.Persist` that runs hundreds of dashboard refreshes per hour, this is a slow driver-memory growth with no observed signal.

### Gap 2 — unpersist failure swallowed (`SparkEngineProvider.scala:580-590`)

The paired unpersist in `applyPostCompilePipeline`'s `finally` catches `Throwable` and logs to stderr:

```scala
try df.unpersist()
catch {
  case e: Throwable =>
    System.err.println(s"sm8: SparkEngineProvider unpersist failed: ${e.getClass.getSimpleName}: ${e.getMessage}")
}
```

Per `scala-error-handling` §4 ("don't swallow real errors"), `unpersist` failures indicate a real Spark executor state problem (`NotSerializableException`, executor OOM, `SparkException` from storage). The PR-176 NonFatal discipline (`dispatcher` wraps only `NonFatal` + re-interrupts on `InterruptedException`) explicitly rejects the `Throwable` catch — but this site predates PR-176 and was missed. ADR-008-P's CROSS-P0-B (still OPEN) called this out at PR-O4 P1-1. The fix was deferred; this ADR closes it.

Consequence: a real persist-side failure silently produces a clean-looking `Right(PortableQueryResult)` and a leaked frame — the two failure modes compound.

### Gap 3 — `MaterializePolicy.Cache` is a silent no-op (`PortableQueryCompiler.scala:516-518`)

The model-level ADT (`io.sm8.core.model.MaterializePolicy`) has three cases: `None`, `Persist(level)`, `Cache`. `applyAggregations` handles `Persist` and falls through to `case _ => Right(result)` for everything else (line 516-518). The inline comment at line 502-503 says "Cache is owned by the cache-plugin, not the connector" — but the cache-plugin is not invoked anywhere on the persist path. `Cache` is therefore silently a no-op: a model declaring `materialize = Cache` runs identically to `None`, with no typed signal.

Consequence: a model that the developer set to `Cache` thinking "this caches the aggregate" gets no caching and no error — the worst class of silent default.

### Gap 4 — `EngineContext.materializePolicy` is dead (`sm8-core/engine/EngineContext.scala:38,60`)

The engine-portable context declares `materializePolicy: MaterializePolicy` with default `MaterializePolicy.None` (line 38, line 60). No adapter reads it. `materializePolicy` flows through `EngineService.runQueryWithHooks` → `provider.query(model, request, ctx)` unchanged, with zero consumers. The default makes it look meaningful; it is not.

Consequence: the `EngineContext` API surface advertises a capability that does not exist — a footgun for the next caller who wires it.

### Gap 5 — dual `MaterializePolicy` ADTs (`sm8-core/engine/EngineContext.scala:75-94` + `sm8-core/model/Model.scala:71-76`)

Two sealed traits named `MaterializePolicy`:

- **`io.sm8.core.engine.MaterializePolicy`** (engine-context-side): cases `None`, `MemoryOnly`, `MemoryAndDisk`, `EngineDefault`. **Zero readers.**
- **`io.sm8.core.model.MaterializePolicy`** (model-attached-side): cases `None`, `Persist(level: String)`, `Cache`. **The active one**, read by `applyAggregations` at line 506.

Both shapes coexist in `sm8-core` with no clear boundary. `MemoryOnly`/`MemoryAndDisk`/`EngineDefault` are not used by any adapter; `Persist(level)` is engine-specific (Spark `StorageLevel.fromString`) leaking through a model-attached ADT. The dual shape is a future drift hazard: a contributor who picks the wrong import gets a silent no-op (the same class of bug as Gap 3).

### Gap 6 — `close()` unpersist loop swallows `Throwable` (`SparkEngineProvider.scala:176-178`)

The JVM-shutdown sweep in `close()` iterates `persistedFrames` and calls `df.unpersist()` on every tracked frame, with the same anti-pattern as Gap 2:

```scala
try df.unpersist()
catch { case _: Throwable => () }
```

The bare `()` swallow masks every unpersist failure at JVM shutdown. Today the loop is empty in production (Gap 1) so the bug is dormant; once Gap 1 is fixed and the loop iterates real tracked frames, JVM-shutdown-time unpersist failures become invisible. The fix is identical in shape to Gap 2 (typed errors, not silent), applied to the loop.

Consequence: a deployment that hit a real storage-side failure during shutdown would silently lose the unpersist; the next startup sees stale persisted state with no signal.

---

## Decision

**Option A — paired lifecycle via typed registration + typed errors + single-source `MaterializePolicy` (the active ADT).**

Options considered and rejected:

| Option | Why rejected |
|---|---|
| B. Add a new typed `MaterializePolicy` case `Register(level: String)` to the engine-context-side ADT and route the registration there | Re-introduces the dual ADT (Gap 5). The active shape already lives in `ModelPolicyDefaults`; adding a parallel engine-context-side case duplicates the policy. Rejected per RFC §3. |

### Fix 1 — Paired persist registration at `applyPostCompilePipeline` (v1.0 correction)

v0.1 proposed calling `trackPersist` from `PortableQueryCompiler.applyAggregations` — **wrong**. `trackPersist` is `private[spark]` on `SparkEngineProvider` (`SparkEngineProvider.scala:164`); `PortableQueryCompiler` is constructed standalone as `new PortableQueryCompiler(querySession)` (`PortableQueryCompiler.scala` ctor) and holds no reference to an enclosing `SparkEngineProvider`. The call site is unreachable.

v1.0 moves the registration to `SparkEngineProvider.applyPostCompilePipeline` — which already does the `wasPersisted` derivation (ADR-009-e fix at line 535-536: `!df.storageLevel.equals(StorageLevel.NONE)`), is in the `private[spark]` scope (so `trackPersist` is reachable), and is the only caller-side seam that holds both the original `df` and the registration token:

```scala
private[spark] def applyPostCompilePipeline(
    df: org.apache.spark.sql.DataFrame,
    request: QueryRequest,
    schemaMetadata: Map[String, String],
    cap: Long = SparkEngineProvider.DefaultResultCapRows
): Either[EngineError, PortableQueryResult] = {
  // v1.0: register the persisted frame BEFORE any filter/limit, so
  // the unregister-on-finally can release it deterministically.
  val registerToken: Long =
    if (!df.storageLevel.equals(org.apache.spark.storage.StorageLevel.NONE))
      trackPersist(df) // returns the unregister-token; held in this scope
    else 0L // sentinel "nothing tracked"
  try {
    // ... existing cap+1 probe, collect, decode logic ...
  } finally {
    if (registerToken != 0L) {
      untrackPersist(registerToken) // release the token even on failure
    }
  }
}
```

`PortableQueryCompiler.applyAggregations` stays a pure compile step (no registration). The persist/unpersist pair stays in one place (the connector), not spread across the model → adapter boundary — the v0.1 prose intent was right; only the call site was wrong.

### Fix 2 — Non-swallow unpersist + exception-shadowing chain (v1.0 correction)

v0.1 proposed replacing the `try df.unpersist() catch { case e: Throwable => System.err.println(...) }` block at `SparkEngineProvider.scala:580-590` with an untyped throw. **Wrong** — the new shape introduces an exception-shadowing bug: if `withLimit.collect()` throws (executor OOM, `SparkException`) AND `df.unpersist()` in the `finally` also throws (same root cause — the executor that failed is the one holding the persisted block), the `finally` exception replaces the original. The typed error returned to the dispatcher is the unpersist failure, not the root cause.

v1.0 chains the original `collect()` exception via `Throwable.addSuppressed` so the dispatcher sees both:

```scala
val collected: Array[org.apache.spark.sql.Row] =
  try {
    withLimit.collect()
  } catch {
    case collectErr: Throwable =>
      // If the persisted upstream also fails to unpersist (same root
      // cause: the executor that failed is the one holding the
      // persisted block), chain it as suppressed so the dispatcher
      // sees the original collect() failure first, the unpersist
      // failure as context. Per scala-jvm-safety §3.
      if (registerToken != 0L) {
        try df.unpersist()
        catch { case unpersistErr: Throwable => collectErr.addSuppressed(unpersistErr) }
        untrackPersist(registerToken)
      }
      throw collectErr
  }
// success path: registerToken is untracked in the success-finally below
```

The `finally` only fires on the success path; the failure path handles both unpersist and untrack inline. Both unpersist errors and collect errors propagate with full context. **No silent unpersist failures; no exception-shadowing.**

### Fix 2b — Non-swallow `close()` unpersist loop (v1.0: new fix for Gap 6)

Symmetric anti-pattern at `SparkEngineProvider.scala:176-178`. After Fix 1 properly populates `persistedFrames`, the JVM-shutdown sweep becomes the most-likely-to-fail unpersist site. The fix mirrors Fix 2 — typed error, no swallow — wrapped in a per-frame attempt that aggregates failures:

```scala
override def close(): Unit = {
  import scala.collection.JavaConverters._
  val unpersistFailures = scala.collection.mutable.ListBuffer.empty[Throwable]
  persistedFrames.asScala.foreach { case (_, df) =>
    try df.unpersist()
    catch { case e: Throwable => unpersistFailures += e }
  }
  persistedFrames.clear()
  if (spark != null) spark.stop()
  unpersistFailures.foreach { f =>
    System.err.println(s"sm8: SparkEngineProvider.close() unpersist failed: ${f.getClass.getSimpleName}: ${f.getMessage}")
  }
}
```

The per-frame `try/catch` is necessary at JVM-shutdown time (a single failing frame should not abort the cleanup of the remaining frames); failures aggregate and log per the RFC §9 stderr channel. **Not silent, but not aborting the sweep either** — the JVM-shutdown use case differs from the per-query use case (one is best-effort, the other is contractually typed).

### Fix 3 — `MaterializePolicy.Cache` becomes a typed rejection

The comment at `PortableQueryCompiler.scala:502-503` ("Cache is owned by the cache-plugin, not the connector") describes a future design that does not exist. Until the cache-plugin's persist-handoff is implemented (a separate ADR), `Cache` must surface as a typed error rather than silently no-op. **Message order corrected in v1.0**: name the currently-wired alternative first (`Persist`); the cache-plugin handoff is named as future work, not as a present alternative.

```scala
case io.sm8.core.model.MaterializePolicy.Cache =>
  Left(EngineError.UnsupportedCapability(
    engine  = "spark-3.5",
    capability = "MaterializePolicy.Cache",
    message = "MaterializePolicy.Cache is not yet wired to the cache-plugin persist handoff. " +
              "For connector-side materialization, use MaterializePolicy.Persist(<storage-level>) " +
              "(e.g. Persist(\"MEMORY_ONLY\")). For result caching, set ModelPolicyDefaults.cache = " +
              "CachePolicy.ReadThrough(<cache-name>) — this routes through the cache-plugin, not " +
              "the materialize path. The cache-handoff for materialize-side Cache is a separate ADR."))
```

This preserves the silent-no-op-as-bug invariant (PR-178 discipline): a model declaring `Cache` gets a typed rejection with an actionable message, not a quiet no-op that the developer assumes is correct.

### Fix 4 — Drop the dead `EngineContext.materializePolicy` (v1.0 blast-radius correction)

Remove the field from `EngineContext` (line 38), the default (line 60), and the case-class copy/signature site. v0.1 claimed "zero callers" — **wrong**. `EngineContextSpec.scala` has 5 reference sites:

| Line | Reference |
|---|---|
| 16-23 | `MaterializePolicy has 4 cases` enumerating `MemoryOnly`/`MemoryAndDisk`/`EngineDefault` |
| 27-28 | `distinct singletons` referencing `MemoryOnly` + `MemoryAndDisk` |
| 117 | `ctx.materializePolicy shouldBe MaterializePolicy.None` |
| 127 | `materializePolicy = MaterializePolicy.MemoryAndDisk` (case-class construction) |
| 134 | `ctx.materializePolicy shouldBe MaterializePolicy.MemoryAndDisk` |

All 5 sites must be deleted (the ADT cases they reference are removed in Fix 5; the field they construct/read is removed here). The test file's coverage migrates to `MaterializePolicySpec` (testing the model-side ADT — the new single-source).

### Fix 5 — Single-source `MaterializePolicy` consolidation

Keep **`io.sm8.core.model.MaterializePolicy`** (the active one). Remove **`io.sm8.core.engine.MaterializePolicy`** entirely (`EngineContext.scala:75-94`). The `EngineContext` ADT (after Fix 4) has no policy field, so removing the engine-context-side ADT removes the only home of the unused cases — `MemoryOnly`, `MemoryAndDisk`, `EngineDefault`. These were speculative future cases that never landed.

The remaining `MaterializePolicy` is engine-portable data (lives in `sm8-core/model`); the connector enforces + registers the lifecycle. The cache-plugin is a separate concern with its own (separate) `CachePolicy` ADT.

### Fix 6 — New typed `EngineError.PersistLifecycleFailed` case (v1.0: P2-5)

The unpersist failure path needs a typed error case — a load-bearing persist feature deserves its own error case over the catch-all `ProviderInvocationFailed`. Add to `sm8-core/engine/EngineError.scala`:

```scala
/** Persistence-lifecycle failure: the connector attempted a paired
  * persist/unpersist (MaterializePolicy.Persist) and either the
  * persist() failed with a non-IlllegalArgumentException (e.g.
  * SparkException from the storage layer) or the unpersist() failed.
  * Distinct from UnsupportedCapability (the policy was honored; the
  * lifecycle execution failed) and from ProviderInvocationFailed
  * (which is the catch-all for non-engine-portable exceptions).
  */
final case class PersistLifecycleFailed(
  engine: String,
  phase: PersistPhase, // Persist | Unpersist
  cause: String,
  message: String
) extends EngineError {
  override def toErrorDetail: ErrorDetail =
    ErrorDetail(ErrorCode.PROVIDER_INVOCATION_FAILED,
      s"persist-lifecycle($phase): $cause — $message", Some(engine))
}

sealed trait PersistPhase
object PersistPhase { case object Persist extends PersistPhase; case object Unpersist extends PersistPhase }
```

The new case carries `phase` so the dispatcher / log-reader can distinguish persist-side from unpersist-side failures (different operator runbooks). `toErrorDetail` maps to `PROVIDER_INVOCATION_FAILED` (no new `ErrorCode` is needed; the typed ADT case is the surface for engine-portable consumers; the wire code reuses the existing one).

The applyPostCompilePipeline finally catches the `unpersist` `Throwable` and re-throws as `EngineError.PersistLifecycleFailed(engine = "spark-3.5", phase = Unpersist, cause = e.getClass.getSimpleName, message = e.getMessage)` after the `addSuppressed` chain (Fix 2). Per PR-176 NonFatal discipline, the dispatcher wraps `NonFatal` and re-interrupts on `InterruptedException`.

---
| D. Drop the persist feature entirely (remove `MaterializePolicy.Persist`) | The feature is used by tests (`SparkEngineProviderCloseLifecycleSpec`) and is part of the documented model contract (`ModelPolicyDefaults.materialize`). Removing it is a breaking change without an approved deprecation cycle. Rejected — the right move is closure, not removal. |


## Falsifiable acceptance (v1.0)

```
1. Paired register + unregister: a model with materialize == Persist("MEMORY_ONLY")
   and a follow-up query() call → after collect() (success or failure),
   the connector's persistedFrames map is EMPTY. The token is untracked
   on every exit path (success-finally, failure-finally with addSuppressed
   chain). Without Fix 1, the map grows by 1 per query.

2. close() actually unpersists + non-swallow: a model with materialize == Persist
   and 5 queries → after 5 query() calls, persistedFrames.size == 5; after
   provider.close(), persistedFrames.size == 0 AND every unpersist succeeded
   OR a per-frame failure was logged to stderr with the typed exception class.
   Assert via persistedFrames.size + a stderr-spy on the System.err channel
   (the per-frame failure aggregation is testable via the existing close()
   lifecycle spec).

3. Non-swallow unpersist with exception-shadowing chain: a model with
   materialize == Persist AND a SparkException injected via a UDF that
   throws on every row → withLimit.collect() throws AND df.unpersist() throws
   (simulated by wrapping df in a decorator that throws on unpersist).
   The query path returns Left(EngineError.PersistLifecycleFailed(phase=Unpersist,
   cause=SparkException, ...)) — AND the dispatched EngineError carries the
   original collect() SparkException as a suppressed exception
   (assert via .getSuppressed.length == 1 and the suppressed is the SparkException).

4. MaterializePolicy.Cache rejection: a model with materialize == Cache
   returns Left(EngineError.UnsupportedCapability(
     capability = "MaterializePolicy.Cache",
     engine = "spark-3.5",
     ...))
   with the actionable message naming Persist first (currently-wired) and
   CachePolicy.ReadThrough second (cache-plugin-side, NOT a MaterializePolicy).
   Assert message starts with the Persist guidance (matches "^For connector-side
   materialization.*") and mentions "CachePolicy.ReadThrough" not
   "MaterializePolicy.ReadThrough".

5. Dead-field removal + 5 test sites migrated: grep -rn 'materializePolicy'
   sm8-core/src/main sm8-platform/src/main connectors/spark-connector/src/main
   sm8-core/src/test sm8-platform/src/test connectors/spark-connector/src/test
   → ZERO matches in src/main (the field + ADT deleted), AND the test sites
   in EngineContextSpec.scala migrated to test the model-side ADT (or removed).
   Reactor build is the assertion; the grep + build must both pass.

6. Single-source ADT: grep -rn 'io.sm8.core.engine.MaterializePolicy' →
   ZERO matches in src/main OR src/test (the file is deleted; EngineContextSpec
   has no references). All references to "MaterializePolicy" now resolve to
   io.sm8.core.model.MaterializePolicy. Verifiable: the test for case #4 compiles,
   the cross-engine audit doesn't break.

7. Typed PersistLifecycleFailed: a model with materialize == Persist AND an
   unpersist fault injected (decorator df.unpersist() throws) → the query path
   returns Left(EngineError.PersistLifecycleFailed(
     engine = "spark-3.5",
     phase = PersistPhase.Unpersist,
     cause = "<thrown-class>",
     message = "<thrown-message>")).
   Assert the typed case (NOT ProviderInvocationFailed) — the test
   pattern-matches on the EngineError ADT shape, asserting the
   PersistLifecycleFailed constructor with the PersistPhase tag.

8. No regression on the ADR-009-e path: the cap+1 probe + wasPersisted fix
   + trunc journal round-trip + cache HIT truncated propagation — all
   remain green after this ADR's changes. The full reactor build + the
   7 ADR-009-e falsifiable tests + the 4 ADR-009-e follow-up tests are
   the assertion. This ADR touches the same finally-block as the
   wasPersisted fix (line 575-592); the regression risk is non-trivial.
```

---
## Consequences (v1.0)

**Positive**

- Closes ADR-008-P's CROSS-P0-B (still OPEN) + 5 sibling gaps (1 + 6 = 6 distinct lifecycle gaps)
- Paired persist lifecycle is contractually bounded — `close()` unpersists what production registered, not an always-empty map; the JVM-shutdown sweep is also non-swallow
- The silent-no-op class of bug (`MaterializePolicy.Cache` falling through) is eliminated at the boundary; the typed rejection carries an actionable message naming the real alternatives (Persist first, currently-wired; CachePolicy.ReadThrough second, cache-plugin-side)
- Dead code is removed (single source of truth per `karpathy-guidelines` §"smallest correct core"); the dual ADT is gone — a future contributor picks one import and gets the right shape
- Typed `PersistLifecycleFailed` case gives the dispatcher a precise signal for the persist-lifecycle failure class (distinct from `UnsupportedCapability` (policy rejected) and `ProviderInvocationFailed` (catch-all))
- Exception-shadowing chain via `addSuppressed` preserves the root-cause exception when both `collect()` and `unpersist()` fail (same root cause is the executor that held the persisted block — common in OOM scenarios)

**Negative**

- `MaterializePolicy.Cache` becomes a breaking change for any model that previously declared it (was no-op, now errors). The typed rejection is loud and actionable; the migration is a 1-line config change to `Persist(<storage-level>)` or `CachePolicy.ReadThrough(<cache-name>)`
- `unpersist` failures now propagate to the dispatcher as typed `EngineError.PersistLifecycleFailed` — a deployment that was silently swallowing unpersist problems will see them as user-visible errors. This is the desired behavior (PR-176 NonFatal discipline); the surfacing is the point
- `EngineContext` loses a field + the engine-side `MaterializePolicy` ADT loses 3 cases (`MemoryOnly`/`MemoryAndDisk`/`EngineDefault`). Blast radius: 5 test sites in `EngineContextSpec.scala` + 0 production callers. Test sites migrate to the model-side ADT; production callers are unaffected
- New typed `EngineError.PersistLifecycleFailed` case is wire-stable (no new `ErrorCode`; the existing `PROVIDER_INVOCATION_FAILED` carries the message; the typed ADT case is the surface for engine-portable consumers)

---
## Out of scope (deferred, named for future ADRs)

- **`MaterializePolicy.Cache` → cache-plugin handoff**: the future design where `Cache` is actually routed to the cache-plugin's persist hook. Requires a typed `PersistHook` SDK Protocol and a per-query wiring (mirrors the `HookRunner` discipline in PR-O4). Separate ADR when a real consumer surfaces.
- **Persisted frame on a different session** (v1.0: P3-10): per-query sessions (`spark.newSession()`) have their own storage; `unpersist` on the per-query frame vs the base session is a separate boundary. The current `trackPersist` registers the dataset ref as returned; per-query session lifecycle is independent. Documented failure mode: `close()` runs at JVM shutdown, long after the per-query session is GC'd; `df.unpersist()` on a GC'd session may either silently no-op or throw `SparkException` (session stopped). Fix 2b's per-frame attempt+aggregate absorbs this; further investigation only if a real leak pattern surfaces.
- **`MaterializePolicy` as a typed engine-portable shape in `EngineContext`**: the original design intent. Restored only if a real engine-portable materialize concern surfaces (e.g. a non-Spark engine with a real `MemoryAndDisk` semantic). The current ADR removes the dead field; reintroducing it requires a real consumer + a typed `MemoryAndDisk` mapping.
- **Typed `Materialize.Persist(StorageLevel)` ADT** (v1.0: P2-7, architect finding): `Persist(level: String)` is engine-specific (Spark `StorageLevel.fromString`) leaking through a model-attached ADT — the same drift hazard the ADR attributes to Gap 5 in reverse. A future Trino / DuckDB adapter would need either to reject `Persist` entirely or to maintain a parallel level-to-engine-native mapping. A typed `Persist(StorageLevel)` ADT (engine-portable enum) is the proper fix; deferred to a future ADR when a non-Spark adapter with a real `Persist` semantic surfaces.

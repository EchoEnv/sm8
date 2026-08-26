# ADR-009-f: Paired persist lifecycle — typed registration, non-swallow unpersist, single-source `MaterializePolicy`

| Field | Value |
|---|---|
| **Status** | **Proposed** — pending dual senior review (architect `best-reasoning` + data-eng `best-coding`). Serial solo (OmniRoute 3.8.49 admission gate blocks parallel). |
| **Date** | 2026-08-26 |
| **Module** | `connectors/spark-connector/.../SparkEngineProvider.scala` (paired `trackPersist`/`untrackPersist` + non-swallow unpersist) + `connectors/spark-connector/.../PortableQueryCompiler.scala` (`MaterializePolicy.Cache` typed-rejection; `MaterializePolicy.Persist` registered) + `sm8-core/engine/EngineContext.scala` (`materializePolicy` removed; single-source ADT consolidated) + `sm8-core/model/Model.scala` (the single-source `MaterializePolicy.Persist`/`Cache` ADT) + `plugins/cache-plugin/.../InMemoryResultCache.scala` (`close()` clears `inflight` futures) |
| **Supersedes scope** | The pre-existing persist/unpersist-lifecycle gaps surfaced by the PR-176 / PR-179 wave and ADR-008-P's CROSS-P0-B (still OPEN): (1) `applyAggregations` calls `result.persist(...)` but never registers the persisted frame in `SparkEngineProvider.persistedFrames` — `close()` iterates an empty map; (2) the `finally`'s `unpersist()` swallows `Throwable` to a stderr log instead of a typed `EngineError`; (3) `MaterializePolicy.Cache` is a silent no-op (falls through `applyAggregations` as `case _ => Right(result)`); (4) `EngineContext.materializePolicy: io.sm8.core.engine.MaterializePolicy` is dead — declared, defaulted, never read; (5) two `MaterializePolicy` ADTs coexist (`io.sm8.core.engine.MaterializePolicy` with `MemoryOnly`/`MemoryAndDisk`/`EngineDefault` + `io.sm8.core.model.MaterializePolicy` with `Persist(level)`/`Cache`), only one is active. |
| **Skill alignment** | `karpathy-app-design` (single source of truth, typed boundaries), `karpathy-guidelines` (surgical edits; smallest correct change; dead code is a smell), `scala-spark-batch-bugs` (driver memory + `.limit()` resets `storageLevel` + persist-on-aggregate lifecycle), `scala-bug-hunting` (silent no-ops are bugs — `Cache` falls through; dead fields are bugs — `EngineContext.materializePolicy`), `scala-jvm-safety` (resource lifecycle; `unpersist` failures indicate real Spark executor state problems), `scala-error-handling` (typed `EngineError` over `Throwable` swallow; `Either` over `try`/`catch` for expected domain errors; PR-176 NonFatal discipline extends), `scala-impact-analysis` (dual ADTs are a blast-radius hazard — every caller of one must migrate to the other), `scala-data-driven-refactor` (closed sealed-trait dispatch; remove unused cases), `scala-perf-testing` (no extra Spark action on the hot path; register at the boundary, not after `collect`), `scala2-scaladoc` (WHY prose on every public surface), `debug-mantra` (falsifiable acceptance per finding). |
| **Architecture alignment** | RFC §3 Core Boundary: `MaterializePolicy` lives once in `sm8-core/model` (engine-portable data shape); the connector enforces + registers the lifecycle; the cache-plugin manages its own cache lifecycle (separate concern, no dual implementation); deployment config stays the deployment layer. PR-178 discipline extends: silent no-ops (Cache falls through) are contract violations just as silent drops were — typed rejection or typed registration, no third option. |

---

## Revision history

| Version | Date | Change |
|---|---|---|
| v0.1 (Proposed) | 2026-08-26 | Initial draft; 5 findings (paired-register, non-swallow unpersist, Cache typed reject, single-source ADT, dead `EngineContext.materializePolicy`); 4 options considered (Option A: closure via paired register + typed errors + single-source ADT; rejected Options B/C/D detailed in §Decision). Investigation files: this audit, codegraph probes (`persist unpersist`, `MaterializePolicy`, `cache plugin InMemoryResultCache`), `cross-engine audit`, `/tmp/oom-surfaces-investigation.md`. |

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

---

## Decision

**Option A — paired lifecycle via typed registration + typed errors + single-source `MaterializePolicy` (the active ADT).**

Options considered and rejected:

| Option | Why rejected |
|---|---|
| B. Add a new typed `MaterializePolicy` case `Register(level: String)` to the engine-context-side ADT and route the registration there | Re-introduces the dual ADT (Gap 5). The active shape already lives in `ModelPolicyDefaults`; adding a parallel engine-context-side case duplicates the policy. Rejected per RFC §3. |
| C. Keep both ADTs; document the model-side one as "the active shape" and add scaladoc to the engine-context-side one warning it's reserved | Documentation-only closure of Gap 5 leaves the dead field (Gap 4) and the silent no-op (Gap 3). The dual ADT is still a drift hazard. Rejected. |
| D. Drop the persist feature entirely (remove `MaterializePolicy.Persist`) | The feature is used by tests (`SparkEngineProviderCloseLifecycleSpec`) and is part of the documented model contract (`ModelPolicyDefaults.materialize`). Removing it is a breaking change without an approved deprecation cycle. Rejected — the right move is closure, not removal. |

### Fix 1 — Paired persist registration

`SparkEngineProvider.applyAggregations` (line 506-519) currently:

```scala
case io.sm8.core.model.MaterializePolicy.Persist(level) =>
  try {
    Right(result.persist(org.apache.spark.storage.StorageLevel.fromString(level)))
  } catch {
    case e: java.lang.IllegalArgumentException =>
      Left(EngineError.UnsupportedCapability(...))
  }
```

Replace with a typed lifecycle that registers the persisted frame:

```scala
case io.sm8.core.model.MaterializePolicy.Persist(level) =>
  try {
    val persisted = result.persist(org.apache.spark.storage.StorageLevel.fromString(level))
    trackPersist(persisted) // returns the unregister-token; held by the connector
    Right(persisted)
  } catch {
    case e: java.lang.IllegalArgumentException =>
      Left(EngineError.UnsupportedCapability(...))
  }
```

The registration token is **held by the connector's applyPostCompilePipeline** (or equivalent paired consumer), not by the caller — so the persist/unpersist pair stays in one place (the connector), not spread across the model → adapter boundary. `close()` then iterates the now-populated `persistedFrames` and unpersists each. A new `private[spark] def untrackPersist(token: Long): Unit` removes a token on explicit early release; the default path (paired unpersist in `applyPostCompilePipeline.finally`) calls `untrackPersist` after `df.unpersist()` succeeds.

### Fix 2 — Non-swallow unpersist

Replace the `try df.unpersist() catch { case e: Throwable => System.err.println(...) }` block at `SparkEngineProvider.scala:580-590` with a typed error path:

```scala
finally {
  if (wasPersisted) {
    untrackPersist(registerToken) // remove from tracked set on success
    df.unpersist() // throws → propagates via the finally + NonFatal discipline
  }
}
```

`df.unpersist()` is allowed to throw. The `finally`'s body is itself inside the `try { withLimit.collect() } finally { ... }` shape — `collect()` either succeeds (and the finally's `unpersist` runs as part of normal completion) or throws (and the finally's `unpersist` runs as part of the failure path, then the `collect()` exception propagates). Per PR-176 NonFatal discipline: `EngineError` (typed) is the expected failure shape; `SparkException` (the real unpersist failure) is the right propagation — the dispatcher wraps only `NonFatal`. The current stderr-log swallow is removed; failures bubble to the dispatcher → typed `EngineError` mapping. **No more silent unpersist failures.**

### Fix 3 — `MaterializePolicy.Cache` becomes a typed rejection

The comment at line 502-503 ("Cache is owned by the cache-plugin, not the connector") describes a future design that does not exist. Until the cache-plugin's persist-handoff is implemented (a separate ADR), `Cache` must surface as a typed error rather than silently no-op:

```scala
case io.sm8.core.model.MaterializePolicy.Cache =>
  Left(EngineError.UnsupportedCapability(
    engine  = "spark-3.5",
    capability = "MaterializePolicy.Cache",
    message = "MaterializePolicy.Cache is not yet wired to the cache-plugin persist handoff; " +
              "use MaterializePolicy.Persist(<storage-level>) for connector-side materialization, " +
              "or set cache = ReadThrough(<cache-name>) for cache-plugin-side memoization."))
```

This preserves the silent-no-op-as-bug invariant (PR-178 discipline): a model declaring `Cache` gets a typed rejection with an actionable message, not a quiet no-op that the developer assumes is correct.

### Fix 4 — Drop the dead `EngineContext.materializePolicy`

Remove the field from `EngineContext` (line 38), the default (line 60), and the case-class copy/signature site. Since `materializePolicy` has zero readers, the blast radius is bounded to **0 callers** — verified by codegraph's "0 callers" report. The deletion is a single-file change in `sm8-core` plus any case-class copy/test that names the field (search the test tree for `materializePolicy =` patterns; the existing tests construct `EngineContext.defaultContext` without naming fields, so most are unaffected).

### Fix 5 — Single-source `MaterializePolicy` consolidation

Keep **`io.sm8.core.model.MaterializePolicy`** (the active one). Remove **`io.sm8.core.engine.MaterializePolicy`** entirely (lines 75-94). The `EngineContext` ADT (after Fix 4) has no policy field, so removing the engine-context-side ADT removes the only home of the unused cases — `MemoryOnly`, `MemoryAndDisk`, `EngineDefault`. These were speculative future cases that never landed; their removal is a deletion with **zero callers** (verified).

This makes the dual ADT go away by removing the unused side. The remaining `MaterializePolicy` is engine-portable data (lives in `sm8-core/model`); the connector enforces + registers the lifecycle. The cache-plugin is a separate concern with its own (separate) `CachePolicy` ADT.

---

## Falsifiable acceptance

```
1. Paired register: a model with materialize == Persist("MEMORY_ONLY") and
   a follow-up query() call → after collect(), the connector's persistedFrames
   map is EMPTY (token removed via untrackPersist after successful unpersist).
   Without Fix 1, the map grows by 1 per query (reproducible: count entries
   after N queries; assert == 0; unfixed code asserts == N).

2. close() actually unpersists: a model with materialize == Persist and 5 queries
   → provider.close() calls unpersist on 5 distinct DataFrames. Assert via a
   Spark listener or a tracking Map<Long, Boolean> on the dataset level
   (the test seam persistedFrames already exposes; assert size == 5 pre-close,
   0 post-close).

3. Non-swallow unpersist: inject a SparkException into df.unpersist() (via a
   throwing StorageLevel mock or a Spark-listener-driven fault). The query
   path returns Left(EngineError) — NOT Right(PortableQueryResult) with a
   stderr line. Assert the propagated error type matches the dispatcher
   contract (NonFatal wrapped; fatal propagated uncaught).

4. MaterializePolicy.Cache rejection: a model with materialize == Cache
   returns Left(EngineError.UnsupportedCapability(
     capability = "MaterializePolicy.Cache",
     engine = "spark-3.5",
     ...))
   with the actionable message naming both alternatives
   (Persist + ReadThrough). Assert message contains "Persist" AND "ReadThrough".

5. Dead-field removal: grep -r 'materializePolicy' sm8-core/src/main
   sm8-platform/src/main connectors/spark-connector/src/main → ZERO matches.
   Also assert no test breaks (the reactor build is the assertion).

6. Single-source ADT: grep -rn 'io.sm8.core.engine.MaterializePolicy' →
   ZERO matches in src/main (the file is deleted). All references to
   "MaterializePolicy" now resolve to io.sm8.core.model.MaterializePolicy
   (verifiable: the test for case #4 compiles, the cross-engine audit
   doesn't break).

7. No regression on the ADR-009-e path: the cap+1 probe + wasPersisted fix
   + trunc journal round-trip + cache HIT truncated propagation — all
   remain green after this ADR's changes. The full reactor build + the
   4 ADR-009-e falsifiable tests are the assertion.
```

---

## Consequences

**Positive**

- Closes ADR-008-P's CROSS-P0-B (still OPEN) + the 4 sibling gaps surfaced by the v0.3 retrospective + the PR-176/PR-179 reviews
- Paired persist lifecycle is contractually bounded — `close()` unpersists what production registered, not an always-empty map
- The silent-no-op class of bug (`MaterializePolicy.Cache` falling through) is eliminated at the boundary; the typed rejection carries an actionable message naming the real alternatives
- Dead code is removed (single source of truth per `karpathy-guidelines` §"smallest correct core")
- The dual ADT is gone — a future contributor picks one import and gets the right shape; no silent drift hazard

**Negative**

- `MaterializePolicy.Cache` becomes a breaking change for any model that previously declared it. The typed rejection is loud and actionable (alternatives named in the message), but a model that previously "worked" by being a no-op will now fail at compile. Mitigation: the typed error names both alternatives (`Persist` for connector-side, `ReadThrough` for cache-plugin-side); the migration is a 1-line config change.
- `unpersist` failures now propagate to the dispatcher as typed `EngineError` — a deployment that was silently swallowing unpersist problems will see them as user-visible errors. This is the desired behavior (PR-176 NonFatal discipline); the surfacing is the point.
- `EngineContext` loses a field. Any external caller (plugins, third-party adapters) that built an `EngineContext` with `materializePolicy = ...` will not compile. Mitigation: the field has zero readers today; the blast radius is the field's own constructor calls in tests (rebuild-and-test is the verification).

---

## Out of scope (deferred, named for future ADRs)

- **`MaterializePolicy.Cache` → cache-plugin handoff**: the future design where `Cache` is actually routed to the cache-plugin's persist hook. Requires a typed `PersistHook` SDK Protocol and a per-query wiring (mirrors the `HookRunner` discipline in PR-O4). Separate ADR when a real consumer surfaces.
- **Persisted frame on a different session**: per-query sessions (`spark.newSession()`) have their own storage; `unpersist` on the per-query frame vs the base session is a separate boundary. The current `trackPersist` registers the dataset ref as returned; per-query session lifecycle is independent. Future ADR if a leak pattern surfaces.
- **`MaterializePolicy` as a typed engine-portable shape in `EngineContext`**: the original design intent. Restored only if a real engine-portable materialize concern surfaces (e.g. a non-Spark engine with a real `MemoryAndDisk` semantic). The current ADR removes the dead field; reintroducing it requires a real consumer + a typed `MemoryAndDisk` mapping.

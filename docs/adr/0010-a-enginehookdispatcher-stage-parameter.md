# ADR-010-a: EngineHookDispatcher orchestration layer — drive all 4 pipeline stages from a single entry point so non-Execute-stage hooks fire in production

| **Status** | **Proposed (v0.3)** |

## Context

The dual senior codebase review of 2026-08-26 surfaced a **cross-boundary silent-no-op defect**: `EngineHookDispatcher.run` hardcodes `PipelineStage.Execute` at line 104, so production dispatch only ever resolves to `PreExecute` / `PostExecute` hooks. Three first-party plugins register hooks at the OTHER three stages and have been silently inert since registration:

| Plugin | Module | Registered at | Fires in production? |
|---|---|---|---|
| `JoinPathPreHook` (semantic-graph cycle validator) | `plugins/semantic-graph-plugin` | `HookStage.PreResolve` (priority 120) | ❌ **NEVER** |
| `GraphPostResolveObserver` (writes `GraphSnapshot` meta key) | `plugins/semantic-graph-plugin` | `HookStage.PostResolve` (priority 120) | ❌ **NEVER** |
| `AuditPostStubHook` (audit counter) | `plugins/audit-plugin` | `HookStage.PostFormat` (priority 150) | ❌ **NEVER** |

User-visible consequence: `sm8 inspect` (MetaInspectorService) reads the `GraphSnapshot` meta key, which `GraphPostResolveObserver` is the only writer of. Result: `present=false` **forever** in production. Audit counter never increments. Cycle validator never runs.

### Root cause

`EngineHookDispatcher.run(initial: Context, execute: Context => Either[EngineError, Context])` takes no stage parameter; the implementation internalises `val stage: PipelineStage = PipelineStage.Execute`. The `preStageFor` / `postStageFor` mappings at lines 214-225 cover all 4 stages correctly (`Parse → PreParse`, `Resolve → PreResolve`, `Execute → PreExecute`, `Format → PreFormat`), but they receive `PipelineStage.Execute` always, so only `PreExecute` / `PostExecute` hooks ever fire in production.

The only production caller, `EngineService.runQueryWithHooks` at `EngineService.scala:397`, constructs `Context(stage = PipelineStage.Execute, ...)` — meaning the **caller already knows the stage** and embeds it in the `Context`, but the dispatcher ignores `Context.stage` and uses its own hardcoded constant.

### History

Introduced in commit `daac360` (PR #32, 2026-08-14) — the original "step-pipeline-wiring" PR. At that point only `PreExecute` / `PostExecute` were defined in the SDK, so the hardcode was correct. RFC §8's 4-stage hook hierarchy was added later; the dispatcher was never updated to take a stage parameter. The defect survived every prior review wave (PR-176 / PR-178 / PR-179 / PR-180 / PR-182).

### Why this matters

Three real-world defects result from this single line:

1. **Cycle validator is inert.** A model with a `CalculatedMeasure` referencing itself in a cycle would compile and run; the user gets an unexplained `IncompatibleExprShape` error from the executor instead of a clear `semanticGraphError` typed value in `ctx.meta`.
2. **`sm8 inspect` returns `present=false` forever.** `MetaInspectorService` (or equivalent) reads `GraphSnapshot.MetaKey` from `ctx.meta`; only `GraphPostResolveObserver` writes it; only PostResolve hooks fire; `GraphPostResolveObserver` is at `HookStage.PostResolve`; PostResolve hooks never fire in production.
3. **Audit counter never increments.** First-party observability is broken. Production runs have no audit trail.

The defect is **invisible in CI**: the existing specs call `hook.run(...)` directly or query `hooks.postHooksFor(stage)` — they never go through `EngineHookDispatcher.run`. The 1163-test baseline is green; the production path is broken.

## Decision

**Option D (chosen): introduce a `HookRunnerOrchestration` layer (sm8-platform-local) that drives the existing `EngineHookDispatcher` across all 4 stages (`Parse`, → `Resolve`, → `Execute`, → `Format`) from a single entry point. The current `HookRunner.run(initial, execute)` signature is preserved; the orchestration layer implements it. `runQueryWithHooks` must surface `finalCtx.meta`-carried typed errors (e.g., `JoinPathPreHook`'s `semanticGraphError`) to the caller.**

```scala
// new: sm8-platform/src/main/scala/io/sm8/platform/query/hooks/HookRunnerOrchestration.scala
final class HookRunnerOrchestration private (
    dispatcher: EngineHookDispatcher) extends io.sm8.sdk.HookRunner {

 override def run(
   initial: Context,
   execute: Context => Either[EngineError, Context]
 ): Either[EngineError, Context] = {

   val stages: Seq[PipelineStage] = Seq(
     PipelineStage.Parse, PipelineStage.Resolve, PipelineStage.Execute, PipelineStage.Format)

   stages.foldLeft[Either[EngineError, Context]](Right(initial)) { (acc, stage) =>
     acc.flatMap { ctx =>
       // Only the Execute stage runs the executor thunk;
       // the other stages fire only their Pre/Post hooks (observer pattern).
       val executeForStage: Context => Either[EngineError, Context] =
         if (stage == PipelineStage.Execute) execute else identity
       dispatcher.run(stage, ctx, executeForStage)
     }
   }
 }
}

// v0.3 addition: typed-error surfacing in runQueryWithHooks
// (per the v0.3 fold; see Revision history)
dispatcher.run(initialCtx, engineExecutor).flatMap { finalCtx =>
  // Surface typed errors carried in ctx.meta by short-circuited pre-hooks.
  // The dispatcher's contract is "pre-hook-responsible for the result shape";
  // if a pre-hook sets stop=true AND writes a typed EngineError to ctx.meta,
  // surface it as the caller's typed error (not the generic NoResult).
  finalCtx.meta.get("semanticGraphError") match {
    case Some(e: EngineError) => Left(e)
    case _ => finalCtx.result match {
      case Some(EngineHookResult(pqr)) => Right(toQueryResultFromPortable(pqr, request))
      case None => Left(ProviderInvocationFailed("NoResult", ...))
    }
  }
}
```

Production caller (`EngineService.runQueryWithHooks` at `EngineService.scala:466`) is migrated from `EngineHookDispatcher` to `HookRunnerOrchestration` — **one identifier change at the only call site**. The orchestrator's contract is "fire all hooks for all 4 stages from one entry point; only `Execute` runs the executor thunk." `runQueryWithHooks` (or the orchestrator) surfaces typed errors carried in `ctx.meta` so pre-hook short-circuits propagate cleanly.

### Options considered

- **Option A (rejected): thread `stage: PipelineStage` through `dispatcher.run` as a required parameter.** Fails to compile: `EngineHookDispatcher extends io.sm8.sdk.HookRunner` (`EngineHookDispatcher.scala:74`); the SDK trait's abstract member at `sm8-core Hooks.scala:254-257` is `def run(initial: Context, execute: Context => Either[EngineError, Context])`. Adding a required leading parameter changes arity; the override no longer implements the abstract member. The only escapes are (a) editing the frozen SDK trait (violates RFC §3 Core Boundary + the ADR's own standing-patterns claim), or (b) retaining the 2-arg form alongside the new 3-arg form (creates the second API surface the karpathy-app-design single-convention rule prohibits). **Even if Option A compiled**, the only production caller migrates to pass `PipelineStage.Execute` (the same value dispatched today), so the 3 plugins (`JoinPathPreHook` at `PreResolve`, `GraphPostResolveObserver` at `PostResolve`, `AuditPostStubHook` at `PostFormat`) remain exactly as inert in production — the headline "3 plugins become live" only materializes inside tests, not in production. **This is the v0.1 defect that dual senior review (2026-08-26) caught.**
- **Option B (rejected): drive the dispatcher across all 4 stages from `EngineService.runQueryWithHooks`.** Loop over `Seq(PipelineStage.Parse, Resolve, Execute, Format)` in `EngineService`. The v0.1 ADR rejected this on a false premise ("each stage has its own Context"), but dual review caught the premise error: `Context.scala:31-37` shows a single case class with a `stage` field, and `sm8-core Pipeline.run:156-179` already executes `Stage.All.foldLeft` over one Context firing pre/post hooks at each boundary. Option B is technically feasible; the real objection (caught late) is that `EngineService`'s executor closure (`EngineService.scala:411-453`) is Execute-shaped and the other stages have no bodies yet. The orchestrator pattern in Option D subsumes Option B's lifecycle in a more disciplined way: the orchestrator is the single source of truth for stage ordering, while the executor stays Execute-shaped.
- **Option C (rejected): add `runParse` / `runResolve` / `runExecute` / `runFormat` separate methods on the dispatcher.** 4 new entry points. Compiles cleanly, but **preserves the exact wrong-choice trap that caused the original defect**: a future contributor picks one of the 4 methods, the wrong stage is invoked, plugins stay silently inert. The whole point of the fix is to make it impossible to pick the wrong stage — Option C leaves the trap in place.
- **Option D (chosen): `HookRunnerOrchestration` layer above the dispatcher.** The dispatcher becomes a per-stage primitive used internally by the orchestrator. The orchestrator implements the existing `HookRunner.run(initial, execute)` signature — no sm8-core SDK edit, no new API surface, no production-caller migration beyond one identifier change at the single call site. **Has in-tree precedent:** `sm8-core Pipeline.run:156-179` (`Stage.All.foldLeft(initial)` firing pre/post hooks at each boundary). The orchestrator applies that pattern to the production query path. **v0.3 addition:** the orchestrator's `run` (or `runQueryWithHooks`'s short-circuit handling) surfaces typed errors carried in `ctx.meta` so pre-hook short-circuits produce a typed `Left(...)` instead of the generic `NoResult`.


### Why Option D wins

1. **No sm8-core edit.** The orchestrator implements the existing `HookRunner.run(initial, execute)` signature verbatim. RFC §3 Core Boundary preserved (no frozen-SDK mutation). The Context/Plugin.scala freeze discipline preserved.
2. **No dual API surface.** Single `HookRunner.run` entry point; the orchestrator is the only way to dispatch hooks. karpathy-app-design single-convention rule preserved. **The original wrong-choice trap is closed by construction** — there is no method to call with the wrong stage.
3. **Fixes the production symptom.** The orchestrator drives all 4 stages from the single production entry point. `JoinPathPreHook` (PreResolve), `GraphPostResolveObserver` (PostResolve), `AuditPostStubHook` (PostFormat) all fire in production. `sm8 inspect` returns `present=true` (because `GraphPostResolveObserver` writes the `GraphSnapshot` meta key); the cycle validator runs; the audit counter increments.
4. **In-tree precedent.** `sm8-core Pipeline.run:156-179` is the canonical example of the same shape (one Context, `Stage.All.foldLeft`, pre/post hooks at each boundary). The orchestrator ports the pattern to the platform query path.
5. **EngineHookDispatcher remains a useful primitive.** Internal use by the orchestrator; per-stage unit tests can still drive a single stage via the dispatcher's existing 2-arg signature (preserved as `private[platform] def runStage(stage, initial, execute)`).
6. **Backwards-compatible.** The dispatcher's public 2-arg signature is preserved. The orchestrator is additive. No third-party plugin or in-tree caller breaks.

### Implementation scope (this ADR's PR)

1. **Add `HookRunnerOrchestration` (sm8-platform-new file):** `sm8-platform/src/main/scala/io/sm8/platform/query/hooks/HookRunnerOrchestration.scala`. Implements `io.sm8.sdk.HookRunner.run(initial, execute)` by looping over `Seq(PipelineStage.Parse, Resolve, Execute, Format)` and calling `dispatcher.run(stage, ctx, executeFn)` at each stage. `executeFn` is `execute` for the `Execute` stage and `identity` (no-op thunk) for the other 3 stages.
2. **Migrate the sole production caller:** `EngineService.scala:466` swaps `dispatcher: EngineHookDispatcher` → `dispatcher: HookRunner` (the SDK trait). One identifier; the constructor at `:371` takes the interface. The QueryService.definition at `QueryService.scala:146` wires `EngineHookDispatcher(engine.hooks)` → `HookRunnerOrchestration(EngineHookDispatcher(engine.hooks))`.
3. **v0.3 typed-error surfacing:** `EngineService.runQueryWithHooks` (around `:456-480`) gains a `finalCtx.meta.get("semanticGraphError")` pattern-match BEFORE the `finalCtx.result` match. If the pre-hook short-circuit set a typed `EngineError` in `ctx.meta`, return `Left(e)` directly; otherwise fall through to the existing `result`-matching path. This is the v0.3 fix that ensures the cycle validator's typed error reaches the caller (not the generic `ProviderInvocationFailed("NoResult")`). The meta key `"semanticGraphError"` is the existing canonical key from `JoinPathPreHook.scala:50`.
4. **Preserve `EngineHookDispatcher.run(initial, execute)` as-is.** No internal API change. The dispatcher is still a useful primitive (per-stage unit testing, in-process previews).
5. **Regression tests for the 3 inert plugins (the load-bearing acceptance criterion):**
   - `JoinPathPreHookCycleDetectionSpec` — construct a `Model` with a `CalculatedMeasure` referencing itself; assert `EngineService.runQueryWithHooks` produces a typed `EngineError.UnsupportedCapability("SemanticGraph.cycle", ...)` returned as `Left(...)` (NOT `ProviderInvocationFailed("NoResult")`). End-to-end through `HookRunnerOrchestration` + the v0.3 typed-error surfacing, NOT `hook.run(...)` direct.
   - `GraphPostResolveObserverSnapshotSpec` — assert that after `EngineService.runQueryWithHooks`, the `MetaCaptureObserver`'s `AtomicReference` carries `GraphSnapshot.MetaKey`. End-to-end through the orchestrator. Note: `sm8 inspect` reads via `MetaCaptureObserver`, not the `QueryResult` wire — the orchestrator's hook firing surfaces the snapshot via the existing `MetaCaptureObserver` path. The `QueryResult` itself does NOT carry meta (no `meta` field on `QueryResult.scala:57-63`); that's acceptable because `sm8 inspect` reads through the inspector.
   - `AuditPostStubHookFiresSpec` — assert `AuditStub.fires.get() > 0` after `EngineService.runQueryWithHooks`. End-to-end through the orchestrator.
6. **Orchestrator unit tests:** `HookRunnerOrchestrationSpec` — assert that driving a request through the orchestrator fires all 4 stages' Pre/Post hooks (one stub hook per stage registered via `HookManager`).
7. **Test inventory correction (from dual review):** Only `HookRunnerSmokeSpec.scala:72` calls `dispatcher.run(...)` directly; `EngineHookDispatcherSpec` builds `EngineImpl` + `QueryService.definition(plugins=...)` + `EngineService.runQueryWithHooks` and asserts end-to-end (NO `dispatcher.run` call inside) — it's already the right seam for the regression tests. The new regression specs use this pattern. The inventory also missed the 2 `HookRunner`-typed consumers in the spark connector (`SparkEngineProvider.scala:59, 130, 479-481`; `SparkEngineProviderDescriptor.scala:48, 63`; `SparkEngineProviderHookRunnerSpec.scala:128`) — these are now preserved automatically since `HookRunner.run` signature is unchanged.
8. **Atomic commits:** 5 commits — (a) `HookRunnerOrchestration` skeleton + spec; (b) `EngineService` + `QueryService` migration (one identifier change); (c) v0.3 typed-error surfacing in `runQueryWithHooks` + `JoinPathPreHookCycleDetectionSpec` regression test; (d) `GraphPostResolveObserverSnapshotSpec` + `AuditPostStubHookFiresSpec` regression tests; (e) full reactor re-run.

### Out of scope (deferred)

- **Multi-stage executor bodies.** The orchestrator fires hooks at all 4 stages, but only `Execute` runs an executor thunk (the other stages fire only their Pre/Post hooks — observer pattern). If a future ADR wants `Parse` / `Resolve` / `Format` stages to do actual work (e.g., a rewrite stage), that's a separate concern.
- **The 3 inert plugins' production behavior beyond the regression tests.** `JoinPathPreHook` already has a typed-error pattern (`semanticGraphError` meta key) that the executor could consume; wiring that is a separate concern. `GraphPostResolveObserver` already writes to `GraphSnapshot.MetaKey`; the question of "what consumes it" is `MetaInspectorService` scope. `AuditPostStubHook` is a stub SLF4J sink awaiting Step 7 (per the in-code scaladoc).
- **HookManager cross-stage routing.** `HookManager.hooksForStage(...)` is correct; the dispatcher was the broken layer.
- **Cross-engine `EngineProvider` implementation of `runXXX` methods.** Spark, Trino, In-Memory providers implement `HookRunner.run` (the unchanged 2-arg signature) — no changes needed. Verified by the dual review (the latent HookRunner consumer in `SparkEngineProvider.scala:481` is now preserved automatically).

## Consequences

### Positive

1. **3 plugins become live in production.** Cycle detection, graph snapshot observer, audit counter all fire on every query (via the orchestrator). `sm8 inspect` returns `present=true` (because `GraphPostResolveObserver` writes the `GraphSnapshot` meta key during the `Resolve` stage).
2. **The wrong-choice trap is closed by construction.** Future contributors cannot pick the wrong stage — there's only one entry point that drives the full lifecycle.
3. **API surface preserved.** `HookRunner.run(initial, execute)` is unchanged. The orchestrator is additive. No third-party plugin or in-tree caller breaks.
4. **No sm8-core edit.** RFC §3 Core Boundary preserved. The orchestrator lives in sm8-platform.
5. **Regression tests prevent recurrence.** The 3 specs assert end-to-end firing through the orchestrator + production entry point (`EngineService.runQueryWithHooks` via `QueryService.definition(plugins=...)`), not `hook.run(...)` direct calls. The existing false-green class is closed.
6. **In-tree precedent honored.** `sm8-core Pipeline.run:156-179` is the canonical lifecycle pattern; the orchestrator ports it to the platform query path.
7. **The hot-path `Map` fold pattern from the persist-lifecycle ADR is unaffected.** This ADR does not change `initialCtx.meta` construction at `EngineService.scala:400`.
8. **v0.3 typed-error surfacing.** The cycle validator's typed `EngineError.UnsupportedCapability("SemanticGraph.cycle", ...)` reaches the caller as a typed `Left(...)` instead of the generic `ProviderInvocationFailed("NoResult")`. Pre-hook short-circuits now propagate cleanly through `runQueryWithHooks`'s `ctx.meta`-first pattern match. The dispatcher's existing "pre-hook-responsible for the result shape" scaladoc (line 114) is honored.

### Negative / risks

1. **Lifecycle ordering risk.** The orchestrator defines the canonical stage order (`Parse` → `Resolve` → `Execute` → `Format`). If a future ADR wants to change the order, the orchestrator is the single edit point — which is good (single source of truth) but requires care. Mitigated by the `Seq(PipelineStage.Parse, Resolve, Execute, Format)` being explicitly listed; the compiler cannot catch a misordering, only a misnaming.
2. **The 3 regression tests add ~120 LOC.** Worth it: the 1163-test baseline was a false green.
3. **`HookRunner.run` executor thunk is Execute-shaped.** If a future ADR wants Parse/Resolve/Format stages to do real work, the orchestrator's `executeFn` must change from `identity` to a real per-stage executor. This ADR does NOT make that change; it preserves the current shape (only Execute runs the executor thunk).
4. **v0.3 typed-error surfacing meta-key coupling.** The `runQueryWithHooks` pattern match on `ctx.meta.get("semanticGraphError")` couples the production return path to a specific plugin's meta-key name. If `JoinPathPreHook` renames its `CycleErrorKey`, the pattern match silently degrades to the generic `NoResult` error. Mitigated by: (a) `JoinPathPreHook.scala:50` documents the meta key as part of the typed-error contract; (b) the regression test will fail if the key changes. Future hardening: introduce a generic `ctx.meta` → typed-error protocol so plugins don't need string-key coordination with `runQueryWithHooks`.
### Standing patterns preserved

- **RFC §3 Core Boundary:** unchanged. The orchestrator is `sm8-platform`-local; the sm8-core SDK trait `HookRunner.run(initial, execute)` is preserved verbatim. No core/connector impact.
- **PR-176 NonFatal discipline:** unchanged. The dispatcher already converts hook `Throwable` to typed `EngineError.HookFailed` at `EngineHookDispatcher.scala:145` (and `:190` for post); the orchestrator inherits this discipline.
- **ADR-009-d ctx.meta fold pattern:** unchanged. The fold at `EngineService.scala:400` (`Map("sm8.cache.policy" -> ...)`) lives in `initialCtx.meta` BEFORE the orchestrator's `run(...)` — same shape, no API change.
- **`karpathy-app-design` (third-party extension portal):** the orchestrator is the single entry point that drives the full lifecycle; third-party plugins register hooks at any stage and the orchestrator fires them automatically. The wrong-choice trap that caused the original defect is closed by construction.

## Revision history

| Version | Date | Summary |
| v0.2 (Proposed) | 2026-08-26 | Dual senior review (architect `best-reasoning` 0.93 + data-eng `best-coding` 0.93, both NEEDS_CHANGES) rejected Option A. 3 BLOCK findings: (1) Option A cannot compile — `EngineHookDispatcher extends io.sm8.sdk.HookRunner` whose abstract 2-arg `run` is at `sm8-core Hooks.scala:254-257`; adding a 3rd parameter leaves the abstract member unimplemented. (2) Even if Option A compiled, the only production caller migrates to pass `PipelineStage.Execute` (same value as today), so the 3 plugins remain inert in production — the headline "become live in production" only materializes in tests. (3) Blast-radius claim "only 1 in-tree caller" is false — `SparkEngineProvider.scala:481` calls the 2-arg form via the `HookRunner` trait type. v0.2 changes the decision to **Option D: `HookRunnerOrchestration` layer above the dispatcher** that drives all 4 stages from one entry point. Folds data-eng's P2 finding that Option B is feasible (false premise rejected in v0.1) and cites `sm8-core Pipeline.run:156-179` as the in-tree precedent for the lifecycle pattern. Folds P3 findings: corrected test-migration inventory (only `HookRunnerSmokeSpec.scala:72` calls `dispatcher.run` directly); the 3 regression specs must assert end-to-end firing through `QueryService.definition(plugins=...) -> EngineService.runQueryWithHooks`, not `hook.run(...)` direct. Standing patterns preserved: RFC §3 (no sm8-core edit), PR-176 NonFatal, ADR-009-d ctx.meta fold, karpathy-app-design single-convention. |
| v0.3 (Proposed) | 2026-08-26 | v0.2 review: dispatched dual senior review hit the OmniRoute 3.8.49 admission-controller regression (architect reviewer failed at 503 `chat_admission_busy` after 10 retries; data-eng reviewer failed at the same). Per the v0.4-wave retry-budget protection pattern, parent did manual architect review + recovered the partial data-eng transcript. Data-eng's partial finding is load-bearing: **`EngineService.runQueryWithHooks` discards `finalCtx.meta`** (`EngineService.scala:456-480`) — even after Option D fires the 3 inert plugins, the cycle error written to `ctx.meta("semanticGraphError")` doesn't surface to the caller (which gets `ProviderInvocationFailed("NoResult")` instead of the typed `EngineError.UnsupportedCapability`). `MetaCaptureObserver` (`sm8-server/MetaCaptureObserver.scala:33-53`, `runsOnStop = true`) does capture meta for `sm8 inspect` on the short-circuit path, so the `present=true` symptom IS fixable by Option D alone — but the typed-error-to-caller surface is NOT. v0.3 folds a `runQueryWithHooks` pattern-match on `ctx.meta.get("semanticGraphError")` BEFORE the `finalCtx.result` match; if a typed `EngineError` is in meta, return `Left(e)` directly; otherwise fall through to the existing result-matching path. Adds Implementation-scope item 3 (typed-error surfacing) + Consequence #8 + Risk #4 (meta-key coupling). The `JoinPathPreHookCycleDetectionSpec` regression test asserts the typed `Left(UnsupportedCapability)` is returned, NOT `ProviderInvocationFailed("NoResult")`. Standing patterns preserved.

## References

- **Discovery:** Dual senior codebase review of 2026-08-26. Architect `best-reasoning` 0.93; data-eng `best-coding` 0.93. P1 finding at `EngineHookDispatcher.scala:104`.
- **Canonical authority (RFC §8 hook priority ranges):** `docs/rfcs/2026-08-12_v1_architecture-spec/hooks.md` — defines the 4 `PipelineStage` cases and the 8 `HookStage` cases; the dispatcher's `preStageFor` / `postStageFor` mappings at `:213-225` map one to the other.
- **Canonical authority (SDK hook surface):** `docs/rfcs/2026-08-12_v1_architecture-spec/plugins.md` — defines `PreHook` / `PostHook` / `HookOrigin` / `HookStage` / `Priority` and the `EngineHookRequest` / `EngineHookResult` types.
- **Canonical authority (SDK `HookRunner.run(initial, execute)` 2-arg signature):** `sm8-core/src/main/scala/io/sm8/sdk/Hooks.scala:254-257`. Frozen; the orchestrator implements this signature.
- **Canonical precedent (4-stage lifecycle pattern):** `sm8-core/src/main/scala/io/sm8/core/Pipeline.scala:156-179` — `Stage.All.foldLeft(initial)` firing pre/post hooks at each boundary over a single `Context`. The orchestrator applies this pattern to the platform query path.
- **Discovery PR (the broken commit):** PR #32 (`daac360`) "step-pipeline-wiring: route engine-portable execute through Plugin hook dispatch" (2026-08-14).
- **Inert plugins (will become live after this fix):** `JoinPathPreHook`, `GraphPostResolveObserver`, `AuditPostStubHook`.
- **Latent HookRunner consumer (preserved by Option D):** `SparkEngineProvider.scala:59, 130, 479-481`; `SparkEngineProviderDescriptor.scala:48, 63`; `SparkEngineProviderHookRunnerSpec.scala:128`. None of these break under Option D because `HookRunner.run` signature is unchanged.
- **ADRs that pre-date this fix:** ADR-009-d (ctx.meta fold pattern — orthogonal, preserved), ADR-009-g (CachePolicy contract — orthogonal, preserved), ADR-0008-af (`EngineError.HookFailed` typed error — preserved).

# ADR-010-a: EngineHookDispatcher stage parameter — lift the hardcoded `PipelineStage.Execute` so non-Execute-stage hooks fire in production

| **Status** | **Proposed (v0.1)** |

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

**Option A (chosen): thread `stage: PipelineStage` through `dispatcher.run` as a required parameter.**

```scala
// EngineHookDispatcher.scala
def run(
    stage: PipelineStage,                                       // ← new required param
    initial: Context,
    execute: Context => Either[EngineError, Context]
): Either[EngineError, Context] = {
  // remove the hardcoded `val stage: PipelineStage = PipelineStage.Execute`
  val afterPreE = firePre(stage, initial)
  afterPreE.flatMap { afterPre => ... }                         // unchanged
}
```

Callers (`EngineService.runQueryWithHooks` at `EngineService.scala:397`, the existing `HookRunnerSmokeSpec`) pass the appropriate stage. The dispatcher's contract becomes: "I fire the Pre/Post hooks registered for the stage you tell me."

### Options considered

- **Option A (chosen): thread `stage` parameter through `dispatcher.run`.** Each caller passes the right stage. Mechanical change; explicit contract; matches `Context.stage` which the caller already constructs.
- **Option B: drive the dispatcher across all 4 stages from `EngineService.runQueryWithHooks`.** Loop over `Seq(PipelineStage.Parse, Resolve, Execute, Format)`. One call site; but the SDK `Context` model is per-stage (each stage has its own `Context`), so the loop would require 4 separate `Context` constructions + 4 separate execute calls — wrong shape for the model.
- **Option C: add `runParse` / `runResolve` / `runExecute` / `runFormat` separate methods on the dispatcher.** 4 entry points. Verbose; adds API surface for a single `run` to express.
- **Option D: introduce a `HookRunner` orchestration layer above the dispatcher that handles the 4-stage lifecycle internally.** Cleaner long-term but bigger scope (this ADR does NOT modify the dispatcher API; it adds a new layer). Defer to a future ADR if the lifecycle gets more complex.

### Why Option A wins

1. **Minimal API change.** One new required parameter; the body is unchanged. The `preStageFor` / `postStageFor` mappings become load-bearing for the first time.
2. **Matches `Context.stage`.** The caller already constructs the `Context` with a `stage: PipelineStage` field. The dispatcher's new signature accepts that same stage, so the contract is symmetric.
3. **Backwards-incompatible.** This is a deliberate break — any out-of-tree caller (third-party plugin) calling `dispatcher.run(ctx, exec)` will fail to compile. Per the project's standing `karpathy-app-design` discipline (third-party extension portal), this is acceptable: the third-party call site is rare (only `examples/hospital-cleaning` is the in-tree example, and it goes through `QueryService`, not the dispatcher directly). The migration is `dispatcher.run(PipelineStage.Execute, ctx, exec)` — one identifier addition.
4. **Aligns with the typed `Context.stage` field.** No "stage is in two places" drift; the caller passes `Context.stage` as the dispatcher's stage parameter.

### Implementation scope (this ADR's PR)

1. **API change:** `dispatcher.run(initial, execute)` → `dispatcher.run(stage, initial, execute)`. Add `stage: PipelineStage` as required parameter.
2. **Caller migration:** `EngineService.runQueryWithHooks` passes `PipelineStage.Execute` (the only stage it currently dispatches). This is the same value it embeds in `Context.stage` at `EngineService.scala:397`.
3. **Test migration:** `HookRunnerSmokeSpec`, `EngineHookDispatcherSpec`, `CrossEngineDecisionHintsConsumptionSpec`, and any other spec calling `dispatcher.run(...)` adds the `PipelineStage.Execute` argument.
4. **Regression tests for the 3 inert plugins (the load-bearing acceptance criterion):**
   - `JoinPathPreHookCycleDetectionSpec` — assert that a model with a cycle triggers `JoinPathPreHook.run` via `EngineHookDispatcher.run(stage = PipelineStage.Resolve, ...)`.
   - `GraphPostResolveObserverSnapshotSpec` — assert that `GraphSnapshot` meta key is written when a request flows through `dispatcher.run(stage = PipelineStage.Resolve, ...)` followed by `dispatcher.run(stage = PipelineStage.Execute, ...)`.
   - `AuditPostStubHookFiresSpec` — assert that the audit counter increments when a request flows through `dispatcher.run(stage = PipelineStage.Format, ...)`.
5. **Test seam:** Each regression spec must call `EngineHookDispatcher.run(...)` end-to-end (not `hook.run(...)` directly). The existing specs that bypass the dispatcher are the reason the defect survived.

### Out of scope (deferred)

- **Multi-stage orchestration (Option D above).** If the lifecycle grows more complex (per-stage context, per-stage cancellation tokens), a future ADR may introduce a `HookRunner` layer. Not in scope now.
- **The 3 inert plugins' production behavior beyond the regression tests.** `JoinPathPreHook` already has a typed-error pattern (`semanticGraphError` meta key) that the executor could consume; wiring that is a separate concern. `GraphPostResolveObserver` already writes to `GraphSnapshot.MetaKey`; the question of "what consumes it" is `MetaInspectorService` scope. `AuditPostStubHook` is a stub SLF4J sink awaiting Step 7 (per the in-code scaladoc).
- **HookManager cross-stage routing.** `HookManager.hooksForStage(...)` is correct; the dispatcher was the broken layer.
- **Cross-engine `EngineProvider` implementation of `runXXX` methods.** Spark, Trino, In-Memory providers don't need changes; the dispatcher fix is purely sm8-platform-local.

## Consequences

### Positive

1. **3 plugins become live.** Cycle detection, graph snapshot observer, audit counter all fire in production.
2. **Regression tests prevent recurrence.** The 3 specs explicitly assert dispatcher-mediated firing, so future refactors of `dispatcher.run` cannot silently re-break these plugins.
3. **API is now correct.** `Context.stage` is the canonical stage field; the dispatcher's signature mirrors it.
4. **The hot-path `Map` fold pattern from the persist-lifecycle ADR is unaffected.** This ADR does not change `initialCtx.meta` construction at `EngineService.scala:400`.

### Negative / risks

1. **Backwards-incompatible dispatcher API.** Out-of-tree callers must migrate. Mitigated by: (a) only 1 in-tree caller; (b) compile-time failure forces migration; (c) the migration is a 1-identifier change.
2. **Risk of under-thinking future stages.** If RFC §8 grows a 5th stage (e.g., `Rewriting`), the `preStageFor` / `postStageFor` mappings at lines 214-225 become non-exhaustive — Scala 2.13's match exhaustiveness check will catch this at compile time. Not a runtime risk.
3. **The 3 regression tests add ~80 LOC.** Worth it: the 1163-test baseline was a false green.

### Standing patterns preserved

- **RFC §3 Core Boundary:** unchanged. The fix is `sm8-platform`-local; no core/connector impact.
- **PR-176 NonFatal discipline:** unchanged. The dispatcher already converts hook `Throwable` to typed `EngineError.HookFailed` at `EngineHookDispatcher.scala:145` (and `:190` for post); the stage parameter is orthogonal.
- **ADR-009-d ctx.meta fold pattern:** unchanged. The fold at `EngineService.scala:400` (`Map("sm8.cache.policy" -> ...)`) lives in `initialCtx.meta` BEFORE `dispatcher.run(stage = PipelineStage.Execute, initialCtx, executor)` — same shape, one identifier added.
- **`karpathy-app-design` (third-party extension portal):** the dispatcher API change is additive for third parties in the sense that the third party doesn't need to know the stage — they register hooks at any stage and the right caller passes the right stage. The breakage is only for third parties who call `dispatcher.run` directly (rare; rare-by-design).

## Revision history

| Version | Date | Summary |
|---|---|---|
| v0.1 (Proposed) | 2026-08-26 | Initial draft. Decision: Option A (thread `stage: PipelineStage` through `dispatcher.run`). Cites the dual senior codebase review of 2026-08-26 as the discovery surface; cites RFC §8 hook hierarchy as the established authority. |

## References

- **Discovery:** Dual senior codebase review of 2026-08-26 (`/tmp/dual-review-memo.md`). Architect `best-reasoning` 0.93; data-eng `best-coding` 0.88. P1 finding at `EngineHookDispatcher.scala:104`.
- **Canonical authority (RFC §8 hook priority ranges):** `docs/rfcs/2026-08-12_v1_architecture-spec/hooks.md` — defines the 4 `PipelineStage` cases and the 8 `HookStage` cases; the dispatcher maps one to the other.
- **Canonical authority (SDK hook surface):** `docs/rfcs/2026-08-12_v1_architecture-spec/plugins.md` — defines `PreHook` / `PostHook` / `HookOrigin` / `HookStage` / `Priority` and the `EngineHookRequest` / `EngineHookResult` types.
- **Discovery PR (the broken commit):** PR #32 (`daac360`) "step-pipeline-wiring: route engine-portable execute through Plugin hook dispatch" (2026-08-14).
- **Inert plugins (will become live after this fix):** `JoinPathPreHook`, `GraphPostResolveObserver`, `AuditPostStubHook`.
- **ADRs that pre-date this fix:** ADR-009-d (ctx.meta fold pattern — orthogonal), ADR-009-g (CachePolicy contract — orthogonal), ADR-0008-af (`EngineError.HookFailed` typed error — preserved).

# SM8 — ADR-010-a HookRunnerOrchestration Retrospective

**Date:** 2026-08-28
**Branch:** `docs/adr-010-a-closeout` (closeout PR pending user merge)
**Scope:** the cross-boundary silent-no-op defect where `EngineHookDispatcher.run` hardcoded `PipelineStage.Execute`, leaving 3 first-party plugins silently inert in production since commit `daac360` (PR #32, 2026-08-14). ADR-010-a v0.3 closed it via a `HookRunnerOrchestration` layer above the dispatcher + typed-error surfacing in `runQueryWithHooks` + `Context.stop = true` short-circuit across stages.
**ADR:** [ADR-010-a](0010-a-enginehookdispatcher-stage-parameter.md) — promoted to **Implemented (PR-189, `5e104cc`)**
**PR:** [PR-189](https://github.com/EchoEnv/sm8/pull/189) (5 atomic implementation commits + 1 full-reactor re-run, squash-merged)
**Skill alignment:** `karpathy-app-design`, `karpathy-guidelines`, `scala-bug-hunting`, `scala-impact-analysis`, `scala-error-handling`, `scala-jvm-safety`, `scala-data-driven-refactor`, `scala2-scaladoc`, `debug-mantra`, `scala-spark-batch-bugs` (orthogonal — spark-connector was out of scope)

---

## 1. TL;DR

The hook-dispatch lifecycle is now end-to-end correct: `HookRunnerOrchestration` drives all 4 `PipelineStage` cases (Parse → Resolve → Execute → Format) from a single entry point, honoring `Context.stop = true` across stages. The 3 first-party plugins (`JoinPathPreHook` cycle validator, `GraphPostResolveObserver` snapshot writer, `AuditPostStubHook` counter) now fire in production. Typed errors written to `ctx.meta` by short-circuited pre-hooks (e.g., cycle detection) reach the caller as `Left(...)` instead of being discarded by `runQueryWithHooks` to `ProviderInvocationFailed("NoResult")`.

The ADR went through **3 numbered rounds + 1 Implemented revision** (v0.1 → v0.2 → v0.3 → v3.2). v0.1 was BLOCKED by dual senior review (Option A couldn't compile). v0.2 chose Option D (orchestration layer) but failed to surface typed errors. v0.3 added the typed-error surfacing + honored `Context.stop`. v3.2 marks Implemented.

**Final state:** full reactor green (sm8-platform 116/116 + all in-scope modules green). Dual senior review: architect `best-reasoning` 0.93 + data-eng `best-coding` 0.93 both NEEDS_CHANGES on v0.1 + 0.93 both NEEDS_CHANGES on v0.2. Parent deep-review: APPROVED on PR-189 (all 7 falsifiable criteria verified at file:line).

---

## 2. What landed

### The orchestration layer (the headline fix)

```scala
// sm8-platform/src/main/scala/io/sm8/platform/query/hooks/HookRunnerOrchestration.scala
final class HookRunnerOrchestration private (
    dispatcher: EngineHookDispatcher
) extends HookRunner {

  override def run(
      initial: Context,
      execute: Context => Either[EngineError, Context]
  ): Either[EngineError, Context] = {
    val stages: Seq[PipelineStage] = Seq(
      PipelineStage.Parse, PipelineStage.Resolve, PipelineStage.Execute, PipelineStage.Format)

    stages.foldLeft[Either[EngineError, Context]](Right(initial)) {
      case (acc, stage) =>
        acc.flatMap { ctx =>
          // Per Verify-advisor point 3: honor `Context.stop = true`
          // across stages. If a previous pre-hook (or the orchestrator's
          // own stage boundary) set stop, skip the executor + post-hook
          // chain for this stage. The post-hook for the stage that set
          // stop has already fired (via the dispatcher); observers at
          // later stages don't double-fire.
          // Mirrors `sm8-core Pipeline.scala:165-179`.
          if (ctx.stop) Right(ctx.copy(stage = stage))
          else {
            val executeFn: Context => Either[EngineError, Context] =
              if (stage == PipelineStage.Execute) execute
              else (c: Context) => Right(c)
            dispatcher.runStage(stage, ctx, executeFn)
          }
        }
    }
  }
}
```

Production caller migration: `EngineService.scala:466` swaps `dispatcher: EngineHookDispatcher` → `dispatcher: HookRunner` (the SDK trait). `QueryService.scala:146` wires `HookRunnerOrchestration(EngineHookDispatcher(engine.hooks))`.

### The dispatcher factoring (cleaner than the ADR's literal spec)

```scala
// sm8-platform/src/main/scala/io/sm8/platform/query/hooks/EngineHookDispatcher.scala
def run(initial: Context, execute: Context => Either[EngineError, Context])
    : Either[EngineError, Context] =
  runStage(PipelineStage.Execute, initial, execute)  // ← 2-arg delegates to runStage

private[platform] def runStage(
    stage: PipelineStage,
    initial: Context,
    execute: Context => Either[EngineError, Context]
): Either[EngineError, Context] = {
  // ... actual per-stage logic (was previously inlined in run) ...
}
```

The ADR-010-a v0.3 text said "preserve `EngineHookDispatcher.run(initial, execute)` as-is." The subagent factored it into `private[platform] def runStage(stage, initial, execute)` and kept the 2-arg `run` as a one-line delegate. This is **cleaner** than the literal spec:
- Public API: `run(initial, execute)` 2-arg preserved verbatim (backward-compat; the 1 in-tree caller `SparkEngineProvider.scala:481` keeps working)
- Internal API: `runStage(stage, initial, execute)` 3-arg used by the orchestrator only

Per `karpathy-guidelines` "smallest correct change": minimum new surface, existing callers untouched, internal factoring is invisible to consumers.

### The v0.3 typed-error surfacing

```scala
// sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala
dispatcher.run(...).flatMap { finalCtx =>
  // ADR-010-a v0.3 typed-error surfacing: a pre-hook that short-circuits
  // via `Context.stop = true` may also write a typed `EngineError` to
  // `ctx.meta` (the canonical example: `JoinPathPreHook.scala:50` sets
  // the meta key `"semanticGraphError"` with the typed
  // `EngineError.UnsupportedCapability("SemanticGraph.cycle", ...)` value).
  finalCtx.meta.get("semanticGraphError") match {
    case Some(e: EngineError) => Left(e)
    case _ =>
      finalCtx.result match {
        // ... existing fall-through path (unchanged) ...
      }
  }
}
```

The cycle validator's typed error now reaches the caller as `Left(EngineError.UnsupportedCapability("SemanticGraph.cycle", ...))` — not `ProviderInvocationFailed("NoResult")`. **The contract is now: pre-hooks can produce typed errors via `ctx.meta`, and `runQueryWithHooks` surfaces them.**

---

## 3. The review chain — 3 numbered rounds + Implemented

| Round | Reviewer | Verdict | Outcome |
|---|---|---|---|
| v0.1 (Proposed) | architect `best-reasoning` 0.93 + data-eng `best-coding` 0.93 (both NEEDS_CHANGES) | BLOCK | 3 P1 BLOCK findings: Option A can't compile / doesn't fix prod / blast-radius wrong. Decision changed to Option D (orchestration layer). Cites `sm8-core Pipeline.run:156-179` as in-tree precedent. |
| v0.2 (Proposed) | dispatched dual review (architect + data-eng); both hit OmniRoute 503 `chat_admission_busy` × 10 retries | BLOCK (auto-cancelled) | Parent did manual architect review + recovered partial data-eng transcript. Data-eng's partial finding is load-bearing: `EngineService.runQueryWithHooks` discards `finalCtx.meta` — typed errors from pre-hook short-circuits don't reach the caller. v0.3 adds the typed-error surfacing pattern match. **NEW finding from Verify-advisor point 3** (after the partial transcript): orchestrator must honor `Context.stop = true` across stages so `CacheReadPreHook` (PreExecute) doesn't overwrite `JoinPathPreHook` (PreResolve)'s typed cycle error. |
| v0.3 (Proposed) | Accepted at #186 merge | ACCEPTED | Implementation subagent `sweP1OrchestrationImpl` dispatched with all 7 acceptance criteria + Verify-advisor point 3 folded into instructions. |
| v3.2 (Implemented) | parent deep-review APPROVED at PR-189 | APPROVED | All 7 falsifiable criteria verified at file:line; 9 new tests across 4 spec files; full reactor green. |

---

## 4. Key decisions (rationale + ADR anchors)

1. **Option D (orchestration layer) over Options A/B/C** (ADR-010-a v0.2). Option A (thread `stage` through `dispatcher.run`) fails to compile (`EngineHookDispatcher extends io.sm8.sdk.HookRunner` whose abstract 2-arg `run` would go unimplemented). Option B (loop 4 stages in `EngineService`) is feasible but architecturally wrong — the executor closure is Execute-shaped. Option C (4 separate methods) preserves the wrong-choice trap that caused the defect. Option D adds a single new class implementing the existing `HookRunner.run` signature; the wrong-choice trap is closed by construction.

2. **`private[platform] def runStage` factoring** (subagent's improvement over v0.3 ADR text). The v0.3 ADR said "preserve the 2-arg `run` as-is" without specifying how the orchestrator calls the dispatcher per stage. The subagent factored it into `runStage` + kept `run` as a delegate. Cleaner than the literal spec; backward-compat preserved; internal factoring invisible to consumers. Documented in v3.2 revision row to keep ADR vs implementation honest.

3. **v0.3 typed-error surfacing in `runQueryWithHooks`** (ADR-010-a v0.3 fold-in). Data-eng reviewer's partial finding: `runQueryWithHooks:456-480` discards `finalCtx.meta`. The cycle validator's typed `EngineError.UnsupportedCapability("SemanticGraph.cycle", ...)` sat in `ctx.meta("semanticGraphError")` but the caller only saw `ProviderInvocationFailed("NoResult")`. Pattern-match on `ctx.meta.get("semanticGraphError")` BEFORE the `finalCtx.result` match — if typed `EngineError`, return `Left(e)` directly; otherwise fall through. Preserves the happy path; closes the typed-error-to-caller surface.

4. **`Context.stop = true` short-circuit across stages** (NEW from Verify-advisor point 3). The orchestrator's `stages.foldLeft(...)` loops over all 4 stages. If `JoinPathPreHook` (PreResolve) sets `stop=true` + writes `semanticGraphError`, the orchestrator must NOT fire `CacheReadPreHook` (PreExecute) afterwards — it would overwrite the typed error with a cached row. Implementation: `acc.flatMap { ctx => if (ctx.stop) Right(ctx.copy(stage = stage)) else { ... dispatcher.runStage(stage, ctx, executeFn) } }`. Mirrors `sm8-core Pipeline.scala:165-179` which already does this.

5. **`executeFn` is `identity` for non-Execute stages** (ADR-010-a v0.2). The orchestrator's `execute` parameter is supplied by the caller (the engine-portable executor). It runs ONLY at `PipelineStage.Execute`. The other 3 stages fire only their Pre/Post hooks (observer pattern), passing `identity` as the no-op executor thunk. This matches `sm8-core Pipeline.run:156-179` shape.

6. **In-tree precedent cited** (ADR-010-a v0.2). `sm8-core/src/main/scala/io/sm8/core/Pipeline.scala:156-179` is the canonical example: `Stage.All.foldLeft(initial)` over one Context firing pre/post hooks at each boundary. The orchestrator applies this pattern to the platform query path.

7. **3 regression specs assert end-to-end firing** (parent deep-review APPROVED). `HookRunnerOrchestrationSpec` (5 tests: 4 stages fire, execute-once, stop-from-PreResolve short-circuits, stop-from-PreParse short-circuits, no-hooks pass-through); `JoinPathPreHookCycleDetectionSpec` (2 tests: typed cycle Left returned, NOT NoResult); `GraphPostResolveObserverSnapshotSpec` (1 test); `AuditPostStubHookFiresSpec` (1 test). Each test drives through `QueryService.definition(plugins=...)` → `EngineService.runQueryWithHooks` (NOT `hook.run(...)` direct — that was the false-green class the defect survived in).

---

## 5. Skill alignment

- **`karpathy-app-design` (third-party extension portal):** the orchestrator is the single entry point that drives the full lifecycle; third-party plugins register hooks at any stage and the orchestrator fires them automatically. The wrong-choice trap that caused the original defect is closed by construction.

- **`karpathy-guidelines` (smallest correct change):** orchestrator is a minimal new file (~158 LOC); no needless abstractions. The subagent's `runStage` factoring is the minimum new surface needed.

- **`scala-bug-hunting` (silent no-ops are bugs):** the entire ADR is a silent-no-op closure. The defect survived 8 PRs because the existing 1163-test baseline was a false green (tests bypassed the dispatcher via `hook.run(...)` direct). The new regression specs assert dispatcher-mediated firing through the production entry point.

- **`scala-impact-analysis` (blast radius):** contained to `sm8-platform/*` (9 files). The `sm8-core` SDK is frozen (no edits). The `HookRunner.run` signature is preserved verbatim — `SparkEngineProvider.scala:481` (the latent HookRunner consumer) keeps working without changes.

- **`scala-error-handling`:** v0.3 typed-error surfacing uses the typed `Either` channel; no exception-based gate. The pre-hook short-circuits now propagate cleanly through `runQueryWithHooks`'s `ctx.meta`-first pattern match.

- **`scala-jvm-safety`:** the orchestrator is stateless (allocate once per `EngineImpl`, reuse for every request). No `var`, no `ThreadLocal`, no mutable maps. The only per-request allocation is the `Context` (which is a case-class copy).

- **`scala-data-driven-refactor`:** the only branching in the orchestrator is the match on `stage == PipelineStage.Execute` for the executor-thunk choice and the `ctx.stop` short-circuit. Sealed-trait dispatch over Map-based rule tables — no Map of rules.

- **`scala2-scaladoc`:** the orchestrator's class doc (lines 70-95) explicitly documents the 3 invariants: (a) execute only at Execute, (b) `stop=true` short-circuits all remaining stages, (c) per-stage executor is `identity` for non-Execute. The v0.3 typed-error surfacing has a comment block at `EngineService.scala:455-468` documenting the canonical meta key.

- **`debug-mantra`:** the 7 falsifiable acceptance criteria map 1-to-1 to specific test fixtures. The `JoinPathPreHookCycleDetectionSpec` reproduces the cycle scenario, asserts the typed `Left` is returned, and verifies the surface is observable (NOT silently dropped).

- **`scala-spark-batch-bugs`:** spark-connector was out of scope for this ADR. P2 cluster (NonFatal discipline) was the parallel PR-187 work; the two were independently reviewed and merged.

---

## 6. What this ADR does NOT do (deferred to future ADRs)

- **Multi-stage executor bodies.** The orchestrator fires hooks at all 4 stages, but only `Execute` runs an executor thunk (the other stages fire only Pre/Post hooks — observer pattern). If a future ADR wants `Parse` / `Resolve` / `Format` stages to do actual work (e.g., a rewrite stage), that's a separate concern.

- **The 3 inert plugins' production behavior beyond the regression tests.** `JoinPathPreHook` already has a typed-error pattern (`semanticGraphError` meta key) that the executor could consume; wiring that is a separate concern. `GraphPostResolveObserver` writes to `GraphSnapshot.MetaKey`; the consumer (`MetaInspectorService`) is already wired. `AuditPostStubHook` is a stub SLF4J sink awaiting Step 7 (per the in-code scaladoc).

- **Multi-region cache routing** (the deferred hazard from ADR-009-f + ADR-009-g §Out-of-scope). `ReadThrough(name)` and `WriteThrough(name)` carry `name` but `InMemoryResultCache` ignores it. ADR-010-c candidate if a real consumer surfaces.

- **Generic `ctx.meta` → typed-error protocol.** Currently the surfacing is hardcoded to the `"semanticGraphError"` key (a plugin-to-`runQueryWithHooks` string-key contract). A future ADR may generalize this so plugins don't need to coordinate on string keys.

---

## 7. Standing patterns preserved

- **RFC §3 Core Boundary:** orchestrator is `sm8-platform`-local; `sm8-core` SDK frozen; the `HookRunner.run(initial, execute)` 2-arg signature is preserved verbatim.
- **PR-176 NonFatal discipline:** orchestrator inherits (the dispatcher's `EngineError.HookFailed` wrap at `:145`, `:190` fires through `acc.flatMap` propagation).
- **ADR-009-d ctx.meta fold pattern:** v0.3 typed-error surfacing reuses `ctx.meta.get(...)` for the typed-error channel.
- **PR-178 honor-or-UnsupportedCapability:** orchestrator is the single entry point; no silent fallbacks; pre-hook typed errors are surfaced, not discarded.
- **`karpathy-app-design` single-convention rule:** single `HookRunner.run` entry point; the wrong-choice trap is closed by construction.

---

## 8. Hygiene snapshot

- **Memory:** 7.7 GB total, ~50% used during P1 subagent run; peaked ~60% during `mvn -B -ntp test`. Well below 90% threshold.
- **Disk:** 75 GB total, 64% used after `dummy-spark-test-verifies-rule/` deletion (PR-188). Below 90% threshold.
- **Codegraph:** indexed (`xd://mcp__codegraph_explore` + `codegraph explore` shell form). Used as primary cross-file reasoning tool by both reviewers and the P1 subagent.
- **Metals / Bloop:** 0 / 0 (cleaned in PR-178 era).
- **Branch hygiene:** 3 merged branches deleted locally (`feat/p2-cluster-nonfatal-sweep`, `docs/adr-010-a-proposed`, `docs/post-review-chores`, `feat/adr-010-a-orchestration-layer`); closeout branch (`docs/adr-010-a-closeout`) opened for this PR.
- **`.omp/WATCHDOG.yml`** + **`.omp/TASKS/adr-010-a-handoff.md`** persist across sessions (gitignored under `.omp/`).

---

## 9. References

- **Discovery:** Dual senior codebase review of 2026-08-26 (`/tmp/dual-review-memo.md`). Architect `best-reasoning` 0.93 + data-eng `best-coding` 0.88. P1 BLOCK finding at `EngineHookDispatcher.scala:104` (the hardcoded `PipelineStage.Execute`).
- **Canonical authority (RFC §8 hook priority ranges):** `docs/rfcs/2026-08-12_v1_architecture-spec/hooks.md` — defines the 4 `PipelineStage` cases and the 8 `HookStage` cases; the dispatcher's `preStageFor` / `postStageFor` mappings at `:213-225` map one to the other.
- **Canonical authority (SDK hook surface):** `docs/rfcs/2026-08-12_v1_architecture-spec/plugins.md` — defines `PreHook` / `PostHook` / `HookOrigin` / `HookStage` / `Priority` and the `EngineHookRequest` / `EngineHookResult` types.
- **Canonical authority (SDK `HookRunner.run(initial, execute)` 2-arg signature):** `sm8-core/src/main/scala/io/sm8/sdk/Hooks.scala:254-257`. Frozen; the orchestrator implements this signature.
- **Canonical precedent (4-stage lifecycle pattern):** `sm8-core/src/main/scala/io/sm8/core/Pipeline.scala:156-179` — `Stage.All.foldLeft(initial)` firing pre/post hooks at each boundary over a single `Context`. The orchestrator applies this pattern to the platform query path.
- **Discovery PR (the broken commit):** PR #32 (`daac360`) "step-pipeline-wiring: route engine-portable execute through Plugin hook dispatch" (2026-08-14).
- **Inert plugins (became live after this fix):** `JoinPathPreHook` (PreResolve), `GraphPostResolveObserver` (PostResolve), `AuditPostStubHook` (PostFormat).
- **Latent HookRunner consumer (preserved by Option D):** `SparkEngineProvider.scala:481`. No break under Option D because `HookRunner.run` signature is unchanged.
- **PR chain (v0.4-wave closeout):** #184 (`.omp/` gitignore hygiene) → #186 (ADR-010-a v0.3 Accepted) → #187 (P2 cluster + P2.5 fold) → #188 (post-review chores) → **#189 (HookRunnerOrchestration layer + v0.3 typed-error surfacing)** → closeout PR (this file).
- **Parallel PR-187 (P2 cluster + P2.5 fold):** independent fix in spark-connector + cache-plugin. Per Code-Advisor advisory, the reviewer found a cache HIT regression in the subagent's rewrite; parent deep-review + amend cycle restored the result-write on HIT + the HookOrigin.Core 4-arg overload. P2 cluster landed first; P1 implementation depends on the meta-channel discipline P2 cluster established (the cache journal `ctx.meta("sm8.cache.write.error")` is the same fold pattern ADR-009-d uses).
- **ADRs that pre-date this fix:** ADR-009-d (ctx.meta fold pattern — orthogonal, preserved), ADR-009-g (CachePolicy contract — orthogonal, preserved), ADR-0008-af (`EngineError.HookFailed` typed error — preserved).

---

## 10. Final state

- **main HEAD:** `5e104cc` (PR-189 squash-merged)
- **ADR-010-a status:** Implemented (PR-189, `5e104cc`)
- **Local branch:** `docs/adr-010-a-closeout` (closeout PR pending user merge)
- **`.omp/WATCHDOG.yml` + `.omp/TASKS/adr-010-a-handoff.md`:** local-only, gitignored, persist across sessions
- **All 7 falsifiable acceptance criteria verified end-to-end** (orchestrator drives 4 stages; `stop=true` short-circuits; execute-only-at-Execute; cycle returns typed Left; GraphSnapshot meta key present; AuditStub counter increments; full reactor green)
- **No follow-ups from this ADR** — `Multi-region cache routing` (ADR-010-c candidate) and `Multi-stage executor bodies` are the documented deferrals
- **v0.4-wave + ADR-010-a fully closed** (after closeout PR merges)

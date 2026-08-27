# Architecture Decision Records (ADRs)

ADRs document significant architectural decisions made in this repo:
their context, the options considered, and the consequences of the chosen path.

## Conventions

- File naming: `NNNN-<kebab-case-slug>.md`, monotonically increasing `NNNN`
- Status values: `Proposed`, `Accepted`, `Superseded`, `Deprecated`
- Each ADR has sections: **Context and Problem Statement**, **Decision**, **Consequences**, **Alternatives Considered**, **References**
- ADRs are immutable once accepted; supersede via a new ADR that links back

## Current ADRs

| # | Title | Status |
|---|---|---|
| [0008-h](0008-h-rel-package.md) | rel/ IR package | Accepted |
| [0008-i](0008-i-casewhen-alias.md) | CaseWhen + Alias expressions | Accepted |
| [0008-j](0008-j-model-extensions.md) | Model extensions (typed fields + joins + calc measures) | Accepted |
| [0008-k](0008-k-spark-compile.md) | Spark compile of joins + aggregates | Accepted |
| [0008-l](0008-l-querybuilder.md) | QueryBuilder — Model → RelOp lowering (with 8 GAPs appendix) | Accepted |
| [0008-m1](0008-m1-parser-loader.md) | ExprParser CaseWhen/Alias/MeasureRef/All grammar + ModelLoader joins/calcMeasures | Accepted |
| [0008-m2](0008-m2-model-validator.md) | ModelValidator — cross-reference validation against ResolvedSource.Scan.schema | Accepted |
| [0008-m4](0008-m4-production-wiring.md) | Production wiring: closes ADR-008-L GAPs 5/6/7/8 | Accepted |
| [0008-m5](0008-m5-calculator-lowerer.md) | Calculator (Expr visitor) + MinimalRelOpLowerer | Accepted |
| [0008-m6](0008-m6-h-hardening.md) | PR-M6 hardening: explain / multi-key / direct lowerer / Persist dispatch | Accepted |
| [0008-o](0008-o-hardening.md) | O-series hardening (4 PRs in 1, ~1100 LOC) + PR-O1c follow-up | Accepted |
| [0008-p](0008-p-post-review-followup.md) | Post-review follow-up plan (10 P0 + 11 P1 + 14 P2, 6 phases) | Implemented (awaiting PR-6 v0.1.0 tag cut) |
| [0008-q](0008-q-sdk-redesign-rename-phantom-typed.md) | Post-ADR-008-P SDK redesign: `MCPEngine* → Engine*` rename + phantom-typed SDK + typed URL + `EngineLoader` (3 atomic PRs: PR-14 + PR-15 + PR-16) | Proposed |
| [0008-r](0008-r-aggregation-groupby-having-limit-parts-window.md) | Aggregation, groupBy, having, limit, parts + window functions (3 atomic PRs: PR-17 + PR-18 + PR-19; closes ADR-008-L GAPs 5/7/8; per ADR-008-P §DE-P2-5) | Proposed |
| [0009-a](0009-a-adapter-side-spark-hints.md) | Adapter-side join strategy from `JoinSpec.estimatedRows` — seed the Spark broadcast byte-threshold | Accepted |
| [0009-b](0009-b-adaptive-skew-wiring.md) | AQE skew wiring — deferred (operator-precedence is non-negotiable on a shared session; per-query factor not expressible until ADR-009-c) | Superseded by ADR-009-c |
| [0009-c](0009-c-per-query-clone-session.md) | Per-session-deployment follow-up: per-query `newSession()` so `JoinHints.skewFactor` binds per query | Implemented (PR-171, `0466841`) |
| [0009-d](0009-d-broadcast-skew-decision-via-context-meta.md) | Broadcast + skew decision lives in the plugin's hook; spark connector consumes via `EngineContext.decisionHints` (v0.3 rebuilt after v0.1 + v0.2 were BLOCKED by dual review; resolves swallow-vs-throw + fold-placement + adds `broadcastThresholdBytes` to `DecisionHints`) | Implemented (PR-174, `0161b7b`) |

| [0009-f](0009-f-paired-persist-lifecycle.md) | Paired persist lifecycle — typed registration (`trackPersist`/`untrackPersist`), non-swallow unpersist with `addSuppressed` chain + typed `Left` on both paths, `MaterializePolicy.Cache` typed reject, single-source `MaterializePolicy` ADT (deletes dead `EngineContext.materializePolicy` + unused cases + 5 test sites), typed `EngineError.PersistLifecycleFailed` + `PersistPhase`, 13th `engineErrorCode` case → 502, close() non-swallow + token-in-log. Closes ADR-008-P CROSS-P0-B + 6 gaps. | Implemented (PR-180, `821d270`) |
| [0009-g](0009-g-cache-policy-contract.md) | CachePolicy contract closure — both cache hooks (read + write) gate on the folded `cachePolicy` value (`ctx.meta.get("sm8.cache.policy")`); single-source `io.sm8.core.model.CachePolicy` (3 cases, `ReadOnly` deleted); fold lives in `initialCtx.meta` before `dispatcher.run`; per-case counter discipline (readFires only on read paths; writeFires only on WriteThrough). Closes 6 pre-existing cache gaps; completes the deferred half of ADR-009-f §Out-of-scope. | Implemented (PR-182, `8d5e4cb`) |
| [0010-a](0010-a-enginehookdispatcher-stage-parameter.md) | EngineHookDispatcher orchestration layer — drive all 4 pipeline stages (`Parse` → `Resolve` → `Execute` → `Format`) from a single `HookRunnerOrchestration` entry point so non-Execute-stage hooks fire in production. The `HookRunner.run(initial, execute)` SDK signature is preserved verbatim (no sm8-core edit); the orchestrator implements it by looping `PipelineStage.All` and calling the dispatcher per stage. **v0.3** folds typed-error surfacing in `runQueryWithHooks` (pattern-match on `ctx.meta.get("semanticGraphError")` to surface the cycle validator's typed `EngineError.UnsupportedCapability` to the caller, NOT the generic `NoResult`). 3 first-party plugins become live: `JoinPathPreHook` cycle validator (PreResolve), `GraphPostResolveObserver` snapshot writer (PostResolve), `AuditPostStubHook` counter (PostFormat). The orchestrator honors `Context.stop = true` across stages (per Verify-advisor point 3); only `Execute` runs the supplied executor thunk; the other 3 stages use `identity`. Subagent factored the dispatcher into `private[platform] def runStage(stage, initial, execute)` while keeping the 2-arg `run` as a delegate (`runStage(Execute, ...)`) for backward-compat — CLEANER than the v0.3 ADR text. In-tree precedent: `sm8-core Pipeline.run:156-179`. | Implemented (PR-189, `5e104cc`) |
## Tools

`adr-tools`, `log4brains`, and similar tools expect `./docs/adr/` at the repo root —
that's where we are.

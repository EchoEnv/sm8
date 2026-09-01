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
| [0011-a](0011-a-remove-deprecated-connector-sdk.md) | Remove the deprecated `Connector SDK` surface (the `io.sm8.core.connector.*` legacy package) from `sm8-core`. Per `007`-series docs: the v0.1.0 connector refactor (PR-197) split engine discovery from connector wiring; PR-211 unified engine-identity literals; PR-218 substituted `ResultCache.NoOp` for the cache-plugin import. The remaining `io.sm8.core.connector.*` types are unused and a layer-discipline hazard (sm8-server, sm8-platform, plugins still reference them by name). Single atomic PR: ~9 files deleted, 0 production code added, ~15 import sites updated to point at `io.sm8.core.engine.*` + `io.sm8.core.cache.*`. **Conformance + shape**: `ConnectorContractSpec` (sm8-core test) deleted; the test-jar contract bases that remain are `HookContractSpec` + `PluginContractSpec`. Historical docs (5 ADR-008 files, ADR-0010-a, 4 cross-engine matrix entries) annotated with "removed by ADR-011-a" breadcrumbs so future readers know the surface is gone, not missing. | Implemented (PR-213, `da00de3`) |
| [0012-a](0012-a-modelservice-restate-handler.md) | **ModelService — Restate-handler surface for the loaded model(s).** Add a 3rd Restate service to `sm8-platform` (`listModels`, `getModel`, `describe`) so Restate's web UI's Services + Invocations pages can discover which model a deployment is serving. Read-only; no persistence; piggybacks on the existing `HttpTransport.endpoint` composition. `ModelSummary.fromModel` handles all 3 `ModelStatus` cases (Draft/Published/Deprecated) and the 3-arg `SourceRef.ByName(catalog, namespace, table)` shape. | Proposed |
| [0012-b](0012-b-metricsservice-restate-handler.md) | **MetricsService — Restate-handler surface for invocation metrics.** Single `snapshot` handler returning `MetricsSnapshot` (invocations/cache/errors counters + `startedAt` ISO-8601 + `uptimeSeconds`). Handlers return **placeholder zeros** until ADR-012-b-followup instruments the call sites; wire shape ready for UI dashboard work. Implemented by PR-254 (sm8-platform `MetricsService.scala` + HttpTransport binding + 9 unit tests + smoke assertion). | Accepted (wire surface only) |
| [0012-b-followup](0012-b-followup-real-counter-instrumentation.md) | **Real counter instrumentation for MetricsService.** Add `QueryMetrics` singleton in sm8-platform with 6 `AtomicLong` counters (invocations total/succeeded; cache hits/misses; errors auditSinkUnavailable/timedOut). Wire increment calls into `QueryService.runQuery` (3 calls per request) + `CachePlugin.onPreExecute` hit/miss branches (opt-in via `MetricsRegistry` static seam in sm8-core — preserves backward compat). Snapshot atomicity NOT guaranteed across the 6 reads — documented in Scaladoc as acceptable for a diagnostic counter (worst case: counters disagree by 1 across concurrent reads + writes). | Accepted (impl. PR-256) |
| [0012-c](0012-c-configservice-restate-handler.md) | **ConfigService — NEGATIVE DECISION.** Hold. Document why dynamic config is out of scope for sm8 (auth + persistence + conflict + audit are 4 questions that don't have engine-shaped answers); document 5 triggers for revisiting (real operator ask, per-tenant config, A/B testing, schema evolution, real-time push). Revisit-gate-1 (PR-253) confirmed no trigger fired between PR-250 and PR-252. | Accepted (hold) |

## Tools

`adr-tools`, `log4brains`, and similar tools expect `./docs/adr/` at the repo root —
that's where we are.

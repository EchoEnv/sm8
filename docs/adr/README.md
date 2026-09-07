# Architecture Decision Records (ADRs)

ADRs document significant architectural decisions made in this repo:
their context, the options considered, and the consequences of the chosen path.

## Conventions

- File naming: `NNNN-<kebab-case-slug>.md`, monotonically increasing `NNNN`
- Status values: `Proposed`, `Accepted`, `Implemented`, `Superseded`, `Deprecated`
- **Status changes are atomic**: when an ADR file's status changes, the README index cell MUST change in the same commit (drift between file and index is a review finding)
- Each ADR has sections: **Context and Problem Statement**, **Decision**, **Consequences**, **Alternatives Considered**, **References**
- ADRs are immutable once accepted; supersede via a new ADR that links back

## Current ADRs

| # | Title | Status |
|---|---|---|
| [0001-0004](0001-0004-engine-portable-architecture.md) | The 4 architectural decisions from the engine-portable refactor (PRs #32-#51): Engine interface + EngineContext + plugin wiring + typed errors, in MADR format | Accepted |
| [0005](0005-expr-parser-is-null-postfix.md) | `IS [NOT] NULL` postfix in `ExprParser` | Accepted |
| [0006](0006-step-11-sm8-mcp-server.md) | Step 11 — SM8 MCP server integration | Accepted |
| [0007](0007-v0.1.0-cut-plan.md) | v0.1.0 cut plan + RFC §12/§13 conformance gaps | Accepted |
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
| [0008-q](0008-q-sdk-redesign-rename-phantom-typed.md) | Post-ADR-008-P SDK redesign: `MCPEngine* → Engine*` rename + phantom-typed SDK + typed URL + `EngineLoader` (3 atomic PRs: PR-14 + PR-15 + PR-16) | Implemented (PR-105, `8b4c8ac`) |
| [0008-r](0008-r-aggregation-groupby-having-limit-parts-window.md) | Aggregation, groupBy, having, limit, parts + window functions (3 atomic PRs: PR-17 + PR-18 + PR-19; closes ADR-008-L GAPs 5/7/8; per ADR-008-P §DE-P2-5) | Implemented (10-PR sequence, PR-19 through PR-29) |
| [0008-s](0008-s-expr-ergonomics-sugar.md) | Expr Ergonomics Sugar (PR-35) | Implemented (PR-123, `2d875e6`) |
| [0008-t](0008-t-measure-sugar.md) | MeasureSugar — infix ergonomics for `Measure.aggregate`, `Expr.MeasureRef`, `Expr.All`, `Expr.Cast` | Implemented (PR-131, `5839980`) |
| [0008-u](0008-u-widenable-typeclass.md) | `Widenable[T[_]]` typeclass — phantom-type erasure unification | Proposed (v1.0 DRAFT) |
| [0008-v](0008-v-boundary-cast-cleanup.md) | Boundary-cast cleanup — `realizeAs[T]`, `RunnerCallback` alias, `row.get(i): AnyRef` ascription | Proposed (v1.0 DRAFT) |
| [0008-w](0008-w-model-validator-count-star-fix.md) | ModelValidator COUNT(*) false-positive fix | Implemented (PR-132, `c01c64d`) |
| [0008-x](0008-x-lowering-layer-input-required.md) | Lowering-layer input-required fix — mirroring PR-132 at the Spark `aggregateToColumn` boundary | Implemented (PR-133, `153c3cb`) |
| [0008-y](0008-y-platformmodelloader-typed-io-boundary.md) | PlatformModelLoader.fromPath — typed-IO boundary (design doc) | v1.1 — review fixes applied |
| [0008-z](0008-z-restate-cached-row-typed-io-boundary.md) | RestateCachedRow validate — typed-IO boundary at the journal (design doc) | v1.1 — review fixes applied |
| [0008-aa](0008-aa-querybuilder-detectcalccycles-iterative-dfs.md) | QueryBuilder.detectCalcCycles — Gray-node continuation bug fix + regression tests | v1.1 — review fixes applied |
| [0008-ab](0008-ab-exprparser-tailrec-loop.md) | ExprParser parseOrExpr/parseAndExpr — `@tailrec` loop refactor | v1.3 — third-review fixes applied |
| [0008-ac](0008-ac-stub-plugins-rename-and-noop-contract.md) | Stub plugins — rename + verify no-op contract | v1.0 — under senior dual review |
| [0008-ad](0008-ad-parent-pom-banneddependencies-hoist.md) | Parent POM — hoist `bannedDependencies=org.apache.spark:*` (Zero-Spark invariant) | v1.2 — dummy-module negative test removed |
| [0008-ae](0008-ae-hookmanager-no-eviction-invariant.md) | HookManager — document no-eviction invariant at SDK boundary | DRAFT (under senior dual review) |
| [0008-af](0008-af-enginehookdispatcher-hookfailed-typed-error.md) | EngineHookDispatcher — typed HookFailed error when a hook throws | v1.1 — review fixes applied |
| [0008-ag](0008-ag-scaladoc-skill-wikilink-sweep.md) | scaladoc sweep — strip skill-wikilink pattern from production source | v1.0 — mechanical sweep approved |
| [0008-ah](0008-ah-spark-serialize-defensive-fixes.md) | Spark/serialization defensive fixes (L1 + L2 from audit) | v1.0 — approved |
| [0008-ai](0008-ai-semantic-graph-rfc-review-and-fixes.md) | Semantic Graph RFC review + v1.1 fixes (PR-149) | v1.0 — approved |
| [0008-aj](0008-aj-join-cardinality-estimates.md) | Join cardinality estimates — `JoinSpec.estimatedRows` → decision-only planner consumers | v1.0 — approved |
| [0009-a](0009-a-adapter-side-spark-hints.md) | Adapter-side join strategy from `JoinSpec.estimatedRows` — seed the Spark broadcast byte-threshold | Accepted |
| [0009-b](0009-b-adaptive-skew-wiring.md) | AQE skew wiring — deferred (operator-precedence is non-negotiable on a shared session; per-query factor not expressible until ADR-009-c) | Superseded by ADR-009-c |
| [0009-c](0009-c-per-query-clone-session.md) | Per-session-deployment follow-up: per-query `newSession()` so `JoinHints.skewFactor` binds per query | Implemented (PR-171, `0466841`) |
| [0009-d](0009-d-broadcast-skew-decision-via-context-meta.md) | Broadcast + skew decision lives in the plugin's hook; spark connector consumes via `EngineContext.decisionHints` (v0.3 rebuilt after v0.1 + v0.2 were BLOCKED by dual review; resolves swallow-vs-throw + fold-placement + adds `broadcastThresholdBytes` to `DecisionHints`) | Implemented (PR-174, `0161b7b`) |
| [0009-e](0009-e-driver-materialization-bounds.md) | Driver-materialization bounds — server-side cap + typed `truncated` on the wire (Option A: no escape hatch) | Accepted |
| [0009-f](0009-f-paired-persist-lifecycle.md) | Paired persist lifecycle — typed registration (`trackPersist`/`untrackPersist`), non-swallow unpersist with `addSuppressed` chain + typed `Left` on both paths, `MaterializePolicy.Cache` typed reject, single-source `MaterializePolicy` ADT (deletes dead `EngineContext.materializePolicy` + unused cases + 5 test sites), typed `EngineError.PersistLifecycleFailed` + `PersistPhase`, 13th `engineErrorCode` case → 502, close() non-swallow + token-in-log. Closes ADR-008-P CROSS-P0-B + 6 gaps. | Implemented (PR-180, `821d270`) |
| [0009-g](0009-g-cache-policy-contract.md) | CachePolicy contract closure — both cache hooks (read + write) gate on the folded `cachePolicy` value (`ctx.meta.get("sm8.cache.policy")`); single-source `io.sm8.core.model.CachePolicy` (3 cases, `ReadOnly` deleted); fold lives in `initialCtx.meta` before `dispatcher.run`; per-case counter discipline (readFires only on read paths; writeFires only on WriteThrough). Closes 6 pre-existing cache gaps; completes the deferred half of ADR-009-f §Out-of-scope. | Implemented (PR-182, `8d5e4cb`) |
| [0010-a](0010-a-enginehookdispatcher-stage-parameter.md) | EngineHookDispatcher orchestration layer — drive all 4 pipeline stages from a single entry point so non-Execute-stage hooks fire in process | Implemented (PR-189, `5e104cc`) |
| [0011-a](0011-a-remove-deprecated-connector-sdk.md) | Remove the deprecated `Connector SDK` surface (the `io.sm8.core.connector.*` legacy package) from `sm8-core`. Per `007`-series docs: the v0.1.0 connector refactor (PR-197) split engine discovery from connector wiring; PR-211 unified engine-identity literals; PR-218 substituted `ResultCache.NoOp` for the cache-plugin import. The remaining `io.sm8.core.connector.*` types are unused and a layer-discipline hazard (sm8-server, sm8-platform, plugins still reference them by name). Single atomic PR: ~9 files deleted, 0 production code added, ~15 import sites updated to point at `io.sm8.core.engine.*` + `io.sm8.core.cache.*`. **Conformance + shape**: `ConnectorContractSpec` (sm8-core test) deleted; the test-jar contract bases that remain are `HookContractSpec` + `PluginContractSpec`. Historical docs (5 ADR-008 files, ADR-0010-a, 4 cross-engine matrix entries) annotated with "removed by ADR-011-a" breadcrumbs so future readers know the surface is gone, not missing. | Implemented (PR-213, `da00de3`) |
| [0012-a](0012-a-modelservice-restate-handler.md) | **ModelService — Restate-handler surface for the loaded model(s).** Add a 3rd Restate service to `sm8-platform` (`listModels`, `getModel`, `describe`) so Restate's web UI's Services + Invocations pages can discover which model a deployment is serving. Read-only; no persistence; piggybacks on the existing `HttpTransport.endpoint` composition. `ModelSummary.fromModel` handles all 3 `ModelStatus` cases (Draft/Published/Deprecated) and the 3-arg `SourceRef.ByName(catalog, namespace, table)` shape. | Proposed |
| [0012-b](0012-b-metricsservice-restate-handler.md) | **MetricsService — Restate-handler surface for invocation metrics.** Single `snapshot` handler returning `MetricsSnapshot` (invocations/cache/errors counters + `startedAt` ISO-8601 + `uptimeSeconds`). Handlers return **placeholder zeros** until ADR-012-b-followup instruments the call sites; wire shape ready for UI dashboard work. Implemented by PR-254 (sm8-platform `MetricsService.scala` + HttpTransport binding + 9 unit tests + smoke assertion). | Implemented (wire surface; PR-254, `d0c15ee`) |
| [0012-b-followup](0012-b-followup-real-counter-instrumentation.md) | **Real counter instrumentation for MetricsService.** Add `QueryMetrics` singleton in sm8-platform with 6 `AtomicLong` counters (invocations total/succeeded; cache hits/misses; errors auditSinkUnavailable/timedOut). Wire increment calls into `QueryService.runQuery` (3 calls per request) + `CachePlugin.onPreExecute` hit/miss branches (opt-in via `MetricsRegistry` static seam in sm8-core — preserves backward compat). Snapshot atomicity NOT guaranteed across the 6 reads — documented in Scaladoc as acceptable for a diagnostic counter (worst case: counters disagree by 1 across concurrent reads + writes). | Accepted (impl. PR-256) |
| [0012-b-export](0012-b-export-prometheus-metrics.md) | **Prometheus metrics export.** Add a separate Vert.x `HttpServer` on a dedicated port (default `--metrics-port 9090`) — the standard Prometheus sidecar pattern. Exposes 9 metrics in `text/plain; version=0.0.4` Prometheus exposition format: `sm8_invocation_total`, `sm8_invocation_succeeded_total`, `sm8_invocation_failed_total`, `sm8_cache_hits_total`, `sm8_cache_misses_total`, `sm8_error_audit_sink_unavailable_total`, `sm8_error_timed_out_total`, `sm8_process_uptime_seconds`, `sm8_process_start_time_seconds`. Reads live values from the `QueryMetrics` singleton (PR-256). | Proposed |
| [0012-c](0012-c-configservice-restate-handler.md) | **ConfigService — NEGATIVE DECISION.** Hold. Document why dynamic config is out of scope for sm8 (auth + persistence + conflict + audit are 4 questions that don't have engine-shaped answers); document 5 triggers for revisiting (real operator ask, per-tenant config, A/B testing, schema evolution, real-time push). Revisit-gate-1 (PR-253) confirmed no trigger fired between PR-250 and PR-252. | Accepted (hold) |
| [0013](0013-mcp-server.md) | MCP server — expose sm8 tools to LLM clients over MCP stdio (5 tools; separate `sm8-mcp` binary) | Implemented (PR-259 + PR-260, `17e0f8a`) |
| [0014](0014-mcp-http-transport.md) | MCP server — Streamable HTTP transport (in-process Vert.x) added to the sm8-server process | Implemented (PR-261 + PR-263, `4121ec7`) |
| [0015](0015-mcp-inprocess-stdio.md) | MCP server — in-process `--mcp-transport stdio` (stdout-redirect refactor; no separate binary) | Implemented (PR-264, `772b29d`) |
| [0016](0016-engine-factory-companion.md) | `io.sm8.core.EngineFactory` companion + zero-I/O core boundary (adapters construct via `EngineFactory.create`, never `new EngineImpl`) | Implemented (#285, `1bcc395`) |
| [0017](0017-engine-impl-discover-iostream.md) | `EngineImpl` discovery I/O cleanup — `ModelLoader.fromPath` removed; `fromStream`/`fromString` entry points keep sm8-core IO-free | Implemented (#286, `03d1a59`) |
| [0018](0018-hook-firing-audit-plugin.md) | hook-firing-audit plugin — self-probing every stage attachment point to detect registered-but-never-fired hooks | Implemented (PR-315, `aa0c208`) |
| [0019](0019-executeengine-typed-error-catch-ladder.md) | typed-error catch-ladder at the `executeEngine` IO boundary — preserves the `EngineError` ADT surface (Wayfinder Ticket #1) | Implemented (PR-317, `847d263`) |
| [0020](0020-typed-error-meta-namespace.md) | `<scope>:error` meta-key namespace for typed-error surfacing + `HookErrorChannel.surfaceTypedError` plugin-author helper (Wayfinder Ticket #2) | Implemented (PR-318, `d93cfd4`) |
| [0021](0021-mcp-route-split.md) | McpHttpRoute / McpStdioRoute / Sm8ToolHandlers structural split — ToolRegistry helper + HTTP lifecycle/state/protocol trio + stdio 2-way mirror (Wayfinder Ticket #3) | Implemented (PR-319/320/321, `e8c85ff`/`a844e5d`/`26813f6`) |
| [0022](0022-pre-aggregation-sm8-native.md) | Pre-aggregation (rollups) — sm8-native design; external-engine + SQL-rewrite proposal evaluated and adapted (6-ticket wayfinder map, all landed #329-#334) | Implemented (#327-#334) |
| [0023](0023-aggregate-call-algebraic-routing.md) | Algebraic measure routing over rollups — two-phase Aggregate+Project re-aggregation (no IR extension); Welford-merge state (n, sum, m2); supersedes the reverted PR-338 gate-flip approach | Implemented (#340 + #341) |
| [0024](0024-time-grain-bucketing.md) | Time-grain rollup bucketing — declared grain dimension + temporal-type value-domain contract + date_trunc materialization + grain-subsumption routing (coarsen Additive/Avg only) | Proposed |
| [0025](0025-rollup-staleness-gate.md) | RollupSchemaStale detection placement — sealed ADT case in core, lowerScan gate in connector, single-vocabulary helper (closes #341 duck M-1) | Accepted |

## Tools

`adr-tools`, `log4brains`, and similar tools expect `./docs/adr/` at the repo root —
that's where we are.

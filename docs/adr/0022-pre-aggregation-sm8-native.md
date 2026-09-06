# ADR-0022: Pre-aggregation (rollups) — sm8-native design; external-engine + SQL-rewrite proposal evaluated and adapted

## Status

Proposed. **Date:** 2026-09-06. **Author:** SM8 agent (per user directive 2026-09-06: "see code if we already do rollup (pre-aggregation process as cache) yet"; external proposal by a third-party assistant (ClickHouse + Apache Calcite) evaluated by dual review).

**IMPLEMENTED — all six tickets landed** (dual review + final gate each):
- Ticket 1: typed `Decomposability` taxonomy (`sm8-core/rel/AggregateFn.scala`, #327).
- Ticket 2: query-shape instrumentation, DEBUG per-run record on logger `io.sm8.platform.query.QueryShape` (#329).
- Ticket 3: `Model.rollups` / `RollupSpec` ADT + validation + YAML loading (#330).
- Ticket 4: `RollupRewriter` fail-open routing (#331) — canonical-shape recognizer, sealed refusal taxonomy (incl. recoverable `AlgebraicStateNotWired`), engine-parity NULL guards pinned at the Expr level, exact identity-match gate.
- Ticket 5: `RollupMaterializer` + `refreshModelJ` JDK seam + end-to-end parity regression (#332) — closure-safe write path (built-in Column aggregations only, no UDFs), integral-data exact parity, NULL-parity, speedup documented.
- Ticket 6: `RollupRefresher` + `RollupRefreshService/refresh` + `sm8 rollup-refresh` CLI command + `query-frequency-observer` plugin + runbook (#333).

**Known v1 limits / follow-ups (deferred by review, not regressions):**
- Algebraic measures (Avg/Stddev/Variance) are declared and validated but refused by BOTH the materializer and the router until partial-state columns land; when wired, prefer Welford-merge columns (n, mean, M2) over raw (n, sum, sumSq) and pin stddev n<2 -> NULL parity end-to-end (the guard builders exist in `RollupRewriter`).
- Time-grain rollups declared but not bucketed/materialized; grain-dim value-domain contract must precede it.
- Shared parser-hardening for joins/filters/calculated_measures YAML blocks (same silent asMap/asSeq paths the rollups parser fixed).
- Schema-TYPE reconciliation between core `rollupSchema` and written tables (name parity test-pinned in `RollupSchemaParitySpec`).
- Trino/DuckDB materialization per the original evaluation (Spark first).

## Context and Problem Statement

sm8 has no rollup / pre-aggregation machinery. Evidence (all verified at main `889aa2b`):

- `sm8-core/src/main/scala/io/sm8/core/model/Model.scala:30-42` — the Model ADT carries dimensions / measures / filters / calculatedMeasures / joins; **no rollups field**. `ModelLoader.scala:22-29`: "rollups remain IR — deferred per the plan".
- `docs/adr/0008-o-hardening.md:96` — "Model-rollup machinery + extension / plugin-as-data (deferred to post-v1.0 per Data Engineer P2-9)".
- `sm8-core/src/main/scala/io/sm8/core/cache/ResultCache.scala` — the only caching is an **exact-result cache**: key = (engine, model, version, measures, dimensions, where) via `CacheBridge.platformCacheKey` (sm8-platform `EngineService.scala:502-510`). A query grouped by day cannot serve a query grouped by month; there is no cross-query reuse.
- **No job-scheduler/cron/orchestration library or subsystem exists** in main sources (the only "scheduler" text matches are Reactor-internal scheduler comments in the MCP transport — not a job orchestrator).

A third-party proposal recommended: **ClickHouse** as a single physical rollup store (MergeTree family, `uniqCombined` HLL for distinct counts) + **Apache Calcite** as a SQL query-rewriting layer (`SubstitutionVisitor` matches incoming SQL against registered rollup definitions; dialect transpilation via `PostgresqlSqlDialect` / `TrinoSqlDialect`). The proposal explicitly assumed (a) an existing orchestration that decides what/when to materialize, and (b) incoming queries as raw SQL text from Postgres/Trino/Spark client ecosystems.

### Why the proposal's premises do not hold for sm8

1. **No SQL text enters the pipeline.** The wire DTO is typed: `QueryRequest(modelName, measures: List[String], dimensions: List[String], where: String, engine)` (`sm8-platform/src/main/scala/io/sm8/platform/query/QueryRequest.scala:49-55`). Internally the platform lowers Model → **RelOp IR** (`sm8-core/src/main/scala/io/sm8/core/rel/RelOp.scala`: Scan/Filter/Project/Aggregate/Join/Sort/Limit/Window) via `QueryBuilder`, and each connector lowers RelOp natively (spark-connector `MinimalRelOpLowerer` → DataFrames; duckdb currently a `SELECT * FROM table` stub; trino `FeatureDeferred`). Calcite's `SubstitutionVisitor` has **nothing to intercept**: it matches SQL/RelNode plans, and its output (rewritten SQL text) has no consumer here.
2. **RelOp is a closed ADT by design.** `RelOp.scala:12-15`: a free-form plan string would let engines invent plan shapes the validator/compiler can't classify. Inserting Calcite RelNode as a second plan IR in front of RelOp inverts that discipline and adds a heavyweight dependency to a dependency-ascetic, Spark-free, IO-free core.
3. **The matching sm8 needs is registry subsumption, not plan-equivalence search.** Because the semantic layer owns every query, a rollup match is a deterministic structural check on IR we control: (query measures ⊆ rollup measures ∧ composable) ∧ (query grouping keys ⊆ rollup keys) ∧ (filters evaluable over rollup columns). No visitor needed.
4. **Per-engine storage beats a single external store here.** Rollup tables in each engine's own storage (Spark-managed datasets first) inherit that engine's lifecycle and need no new connector, no new EngineIdentity/lowerer, no OLAP-server ops on a memory-constrained host. ClickHouse may later enter as an **optional rollup-storage connector** if a genuine sub-second-concurrency use case appears — as one connector among several, not a pillar.

### What IS salvageable from the proposal (adopted here)

- **Fail-open matching**: unmatched ⇒ byte-identical original plan. A missed optimization is latency; a wrong rewrite is silently wrong numbers.
- **Decomposable-aggregate discipline**: routing must consult a correct compositability classification (see Ticket 1 — the current comments contain errors).
- **Rollup-vs-raw regression tests**: every rollup path must prove result equality against the base path.
- **Match-rate monitoring** and **loud fall-through logging**.
- **Instrument query shapes before building routing** (the proposal's own roadmap advice, kept).

## Decision

Build pre-aggregation **sm8-natively**, at the RelOp level, in the layers ADR-008-O reserved — and treat it as a first-class roadmap (no post-v1.0 gating), sequenced by the wayfinder map `docs/wayfinder/2026-09-06-pre-aggregation.md` (6 tickets):

1. **core/model** — `Model.rollups: List[RollupSpec]` (name, dimensions, measures, timeGrain; refs validated like calculatedMeasures). Loader extends per `ModelLoader.scala:29`.
2. **core/rel** — promote aggregate compositability from prose to a typed, exhaustive function (`Decomposability`): Additive {Sum, Count, Min, Max}, Algebraic {Avg, Stddev*, Variance*}, Positional {First, Last}, Holistic {Median, Percentile*}, Approximable {CountDistinct, ApproxPercentile — re-aggregable only via explicit sketch state, and then approximate; refused in v1 routing}.
3. **core/rel** — `RollupRewriter`: fail-open RelOp→RelOp rewrite (deterministic subsumption; no new pipeline Stage — the Stage ADT is frozen; no PreExecute hook — pre-hooks must not mutate the request).
4. **connector (spark first)** — materialization write + rollup-table read; correctness regression suite (rollup path ≡ base path).
5. **adapter/plugin** — refresh triggers (CLI/endpoint + external cron for v1); PostExecute query-frequency observer plugin informing what to build.
6. **ClickHouse** only as an optional future rollup-storage connector, if measured demand appears.

## Consequences

- Same cost/latency win as the proposal's goal, on infrastructure we already run; no new external engine, no planner dependency in core, no dialect surface nobody consumes.
- Routing correctness becomes a tested, typed concern in core (`Decomposability` exhaustiveness makes a misfiled aggregate a compile-time-visible gap).
- The known `AggregateFn` comment errors (Min/Max mislabeled non-additive; First misfiled; CountDistinct under the additive banner) get fixed as Ticket 1 — a prerequisite for any router.
- Honest gap: refresh orchestration is external (cron) until demand justifies a subsystem.
- Approx-distinct substitution is rejected for v1: silently converting exact `CountDistinct` to HLL approximations at the storage layer would violate sm8's own typed-distinction principle (Median vs ApproxPercentile are separate cases for exactly this reason).

## Alternatives Considered

- **Adopt ClickHouse + Calcite as proposed.** Rejected: premises false for sm8 (no SQL entry, no orchestrator); the mechanism (SQL rewrite) targets a seam that does not exist; strictly more work than the native design because the Model metadata (rollups) is needed regardless.
- **Do nothing until v1.0.** Rejected per user directive: v1.0 is not a meaningful gate; rollup value does not depend on it.
- **Spark Catalyst optimizer rule** (from the proposal). Rejected for now, per the proposal's own analysis: unstable internal APIs; our Spark consumption is RelOp-lowered DataFrames, not user SQL.

## References

- `docs/wayfinder/2026-09-06-pre-aggregation.md` — 6-ticket execution map
- External proposal (ClickHouse + Calcite) — evaluated by dual review 2026-09-06 (architect `hog`, data-eng `kitten`; both verdict ADAPT)
- `sm8-core/src/main/scala/io/sm8/core/rel/AggregateFn.scala` — compositability source material (errors noted)
- `sm8-core/src/main/scala/io/sm8/core/model/Model.scala`, `ModelLoader.scala:22-29` — rollup field reservation
- `docs/adr/0008-o-hardening.md:96` — original deferral being superseded
- `sm8-platform/src/main/scala/io/sm8/platform/query/QueryRequest.scala:49-55`, `EngineService.scala:502-510` — typed query surface + cache key precedent

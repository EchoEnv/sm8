# Wayfinder map — Pre-aggregation (rollups), sm8-native (2026-09-06)

**Status:** Tickets #1-2 landed (see ADR-0022 status note); awaiting Ticket #3.
**Surveyed by:** session-level architecture investigation + dual review (architect + data-eng, both verdict ADAPT) on main `889aa2b`.
**Driver ADR:** ADR-0022 (`docs/adr/0022-pre-aggregation-sm8-native.md`) — supersedes the post-v1.0 deferral in ADR-008-O:96.

---

## Destination

Six tickets that take sm8 from "no rollup machinery, exact-result cache only" to "declared, materialized, routed, regression-proven pre-aggregation" — without adding an external engine (ClickHouse) or a SQL-rewrite layer (Calcite), both evaluated and rejected as mismatches for sm8's typed-query architecture (see ADR-0022 for the full evaluation).

1. **Ticket #1** — `AggregateFn` decomposability taxonomy. Today the additive/algebraic/holistic classification lives in **comments, and the comments are wrong** (Min/Max labeled "non-additive" though `min(min(part)) = min(total)`; `First` labeled "additive for some rollups" though plain first/last carries no re-aggregable state; `CountDistinct` sits under the Additive banner while its own comment says NOT additive). Any rollup router built from these comments makes wrong routing decisions. Ticket: a typed, exhaustive `Decomposability` classification + corrected comments + spec. **Prerequisite for everything else.**
2. **Ticket #2** — query-shape instrumentation. Log (model, measures, dims, group-set) tuples at QueryBuilder output for a period, so rollup selection is driven by measured query patterns, not guesses (the external proposal's own advice, kept). Observer-plugin or platform-side logging, no behavior change.
3. **Ticket #3** — `Model.rollups: List[RollupSpec]` + validation + loader support. RollupSpec(name, dimensions, measures, timeGrain). Refs cross-validated like calculatedMeasures (PR-M2 discipline). `ModelLoader.scala:29` already reserves the extension point.
4. **Ticket #4** — `RollupRewriter`: fail-open RelOp→RelOp routing (dims ⊆ check, composable-aggregates check, filter-evaluability check; canonical Scan→Filter→Aggregate shape only in v1). Applied at QueryBuilder output. NOT a new pipeline Stage (Stage ADT frozen) and NOT a PreExecute hook (pre-hooks must not mutate the request — RFC hooks.md Rule 2).
5. **Ticket #5** — Spark materialization + correctness regression: rollup table as a Spark-managed dataset; write path + SourceRef read path; regression suite asserting rollup-path ≡ base-path results on a known dataset for Sum/Count/Min/Max/Avg (avg via sum+count pair); exact CountDistinct only at matching grain. Regression MUST cover: (a) integral data or float tolerance (partial-sum non-associativity); (b) single-observation groups for stddev_samp/var_samp (both paths must return NULL for n<2); (c) documented exact/approx posture for Median on Spark (base-path `percentile_approx`/`median` is itself approximate — the rollup path must not be held to a stricter exactness than the base path it replaces; see Ticket 4).
6. **Ticket #6** — refresh triggers: CLI command / server endpoint to (re)build rollups for a model + external cron (v1); PostExecute query-frequency observer plugin informing which rollups are worth building. No scheduler subsystem until measured demand justifies one.

## Notes

Skills every ticket must apply (per the standing Execution Rules Checklist):

- `scala-data-driven-refactor-mindset` (sealed-trait dispatch for Decomposability; pure data RollupSpec)
- `scala-impact-analysis-mindset` (blast-radius via `mcp__codegraph__codegraph_explore` on every changed symbol)
- `scala-jvm-safety-mindset` + Spark closure serialization (Ticket 5 write path captures only Serializable state)
- `scala-perf-testing-mindset` (Ticket 5: measure that the rollup path actually beats base-path on the regression dataset before claiming value)
- `karpathy-guidelines-mindset` (smallest correct change per ticket; fail-open = the smallest correct router)
- `scala2-scaladoc-mindset` (skill scripts MUST run on every changed `.scala` file)
- `building-restate-services` (only if a ticket touches sm8-platform wire DTOs — none currently does)

Standing preferences honored: never PR directly to main (rule #5); user is sole merger (rule #9); review-clone freshness; codegraph-first analysis; scaladoc scripts on every changed file.

---

## Decisions so far

- ADR-0022 (Proposed): sm8-native RelOp-level rollups; ClickHouse+Calcite proposal adapted (mechanisms rejected, disciplines adopted); no post-v1.0 gating.

---

## Not yet specified

- Rollup table naming convention + SourceRef shape for rollups (Ticket 3/4 design detail).
- Whether CountDistinct-at-matching-grain is allowed in v1 routing or deferred to v2 (Ticket 4 decision; default: defer).
- Time-grain rollup semantics for Positional (First/Last) aggregates (deferred; v1 routing simply refuses them).

---

## Out of scope for this map

- ClickHouse connector (only if a measured sub-second-concurrency use case appears later; separate ADR then).
- Calcite / any SQL-text rewrite layer (no SQL surface exists to rewrite).
- Trino and DuckDB materialization (Trino connector is FeatureDeferred; DuckDB semantic bridge not landed — Spark first, others follow the same pattern).
- Approx-distinct (HLL) substitution for exact CountDistinct — rejected in ADR-0022 (wire-contract semantics change).
- New HookStage values (requires its own ADR per the frozen-pipeline contract).

---

## Tickets (decision tickets, not build slices)

Each ticket is bounded to ~1 session and produces its own ADR-0022-status-note update + branch + dual-review + PR. Recommended order is #1 → #2 → #3 → #4 → #5 → #6: #1 unblocks the router's correctness vocabulary, #2 informs #3's rollup selection, #3 is the data model #4 routes against, #5 proves it end-to-end, #6 makes it operational.

### Ticket #1 — `AggregateFn` decomposability taxonomy

**Question:** how do we promote the (partially wrong) aggregate-classification comments in `sm8-core/src/main/scala/io/sm8/core/rel/AggregateFn.scala:34-109` into a typed, exhaustive classification that a rollup router can safely consult?

**Scope:** `sm8-core/src/main/scala/io/sm8/core/rel/AggregateFn.scala` (+ new companion types in the same package) + a new `AggregateFnDecomposabilitySpec.scala`. `sm8-core` only; zero behavior change to existing query paths.

**Acceptance criteria:**
1. New sealed taxonomy (in `io.sm8.core.rel`), e.g. `sealed trait Decomposability` with `Additive` (re-aggregable from partials: Sum, Count, Min, Max), `Algebraic` (re-aggregable from named partial states: Avg → (sum, count); Stddev/Variance → (n, sum, sumSq)), `Positional` (First, Last — not re-aggregable without argmin/argmax state), `Holistic` (Median, PercentileContinuous, PercentileDiscrete — not re-aggregable), `Approximable` (ApproxPercentile, CountDistinct — re-aggregable only via explicit sketch/approx state and labeled approx).
2. `def decomposability(fn: AggregateFn): Decomposability` — total function over the sealed ADT (compiler-exhaustive), with Scaladoc stating the routing contract per class.
3. The wrong prose comments are corrected in place (Min/Max → additive; First → positional; CountDistinct moved out of the additive banner).
4. New spec asserts the classification of all 16 aggregate fns, including the regression guards for the three previously-wrong entries.
5. Zero behavior change: existing greps/tests that match on AggregateFn cases pass unchanged (`grep -r 'org.apache.spark' sm8-core/src/main/scala/io/sm8/core/rel/AggregateFn.scala` stays empty — core stays IO-free).
6. ADR-0022 status note updated (Ticket 1 landed).
7. Scaladoc shape + noise scripts clean on changed files.
8. Dual review + final gate + PR + STOP.

**Non-goals:** no router, no Model changes, no engine-side changes. This ticket only makes the vocabulary correct and typed.

**Effort:** ~half a session.

### Ticket #2 — query-shape instrumentation

**Question:** what are the actual (measures, dimensions, group-set) shapes hitting the pipeline, so rollup selection is measured rather than guessed?

**Scope:** sm8-platform query path (observer plugin or EngineService logging), no core change.

**Acceptance criteria:** shapes logged with counts; no query-path latency regression; a documented way to read the stats. Dual review + PR + STOP.


**Landed (query-shape instrumentation):** one DEBUG record per `runQueryWithHooks` invocation on logger `io.sm8.platform.query.QueryShape` carrying (model, version, measures, dimensions, timeGrain) — zero hot-path cost at INFO, zero behavior change. **Reading the stats:** enable that logger at DEBUG and aggregate the lines, grouping on the constant format-string prefix `query-shape model=`. **Counts** (frequency per shape) are deferred to Ticket #6's PostExecute frequency observer — this ticket's records are the shape half; the observer attaches the count half.

**Effort:** ~half a session.

### Ticket #3 — `Model.rollups` ADT + validation + loader

**Question:** how do users declare a rollup in the semantic layer, and how is the declaration validated?

**Scope:** `sm8-core/model/Model.scala` (+RollupSpec), `ModelValidator` cross-ref validation, `ModelLoader` YAML mapping, tests.

**Acceptance criteria:** Round-trip YAML → Model.rollups → validation errors for unknown refs (same discipline as calculatedMeasures). Existing models unchanged (default Nil). Dual review + PR + STOP.

**Effort:** ~1 session.

### Ticket #4 — `RollupRewriter` (fail-open routing)

**Question:** how does a query's RelOp get re-scanned from a rollup table instead of the base table, deterministically and fail-open?

**Scope:** `sm8-core/rel/` (or `sm8-core/query/`) — RollupRewriter + registry + spec. Applied after QueryBuilder.build; NOT a pipeline Stage; NOT a hook.

**Acceptance criteria:** match requires dims ⊆ ∧ composable aggregates (Ticket 1 vocabulary) ∧ filter-evaluability; non-match returns the original plan unchanged (byte-identical); spec covers match, no-match, partial-overlap, and unsplittable-aggregate cases; Algebraic re-aggregation must apply engine-parity NULL/undefined guards (e.g. stddev_samp with total n<2 → NULL, matching the base path — humpback finding); the n=1-single-observation-group edge is in the regression set. Dual review + PR + STOP.

**Effort:** ~1 session.

### Ticket #5 — Spark materialization + correctness regression

**Question:** how does a rollup get physically built and read in the Spark connector, with proof that rollup results equal base results?

**Scope:** spark-connector (write job + SourceResolver read support) + regression spec.

**Acceptance criteria:** one rollup end-to-end on Spark; regression suite asserts rollup-path ≡ base-path on a known dataset (Sum/Count/Min/Max exact; Avg via sum+count); float partial sums are non-associative, so equality assertions use integral data or an explicit tolerance; for large-mean variance data prefer Welford-merge columns (n, mean, M2) over raw (n, sum, sumSq) to avoid catastrophic cancellation; measured speedup documented. Spark closure serialization audited on the write path. Dual review + PR + STOP.

**Effort:** ~1-2 sessions.

### Ticket #6 — refresh triggers + frequency observer

**Question:** how do rollups get (re)built operationally, and how do we know which rollups are worth it?

**Scope:** sm8-server/sm8-cli trigger surface + optional PostExecute observer plugin; external cron document.

**Acceptance criteria:** one command rebuilds a model's rollups; observer counts query shapes (feeding Ticket 2's instrumentation); runbook documents cron wiring. Dual review + PR + STOP.

**Effort:** ~1 session.

---

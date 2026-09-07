# ADR-0024: Time-grain rollup bucketing — grain-dim value-domain contract + materialization + routing

## Status

Proposed. **Date:** 2026-09-07. **Author:** SM8 agent. Closes the last ADR-0022 "Known v1 limits" item: "Time-grain rollups declared but not bucketed/materialized; grain-dim value-domain contract must precede it."

## Context and Problem Statement

`RollupSpec.timeGrain: Option[String]` and `QueryRequest.timeGrain: Option[String]` exist, and `RollupRewriter.grainsAgree` routes on **exact normalized equality** (`normalizeGrain` = trim + lowercase; `KnownGrains = {hour, day, week, month, quarter, year}`). But three gaps make grain rollups unusable end-to-end:

1. **The materializer refuses all grain rollups.** `RollupMaterializer.validateSpec` returns `UnsupportedCapability("RollupMaterializer.timeGrain")` when `spec.timeGrain.isDefined` — nothing is ever written.
2. **No dim is designated as the grain dimension.** A `RollupSpec` declares `dimensions: List[String]` and a `timeGrain` label, but nothing says WHICH dimension (if any) is the time axis the grain applies to, nor what its element type is (`Date`? `Timestamp`? `Varchar` ISO strings?).
3. **Routing is equality-only.** A query at `timeGrain = "day"` cannot be served from a `"month"` rollup even though month-level rows could in principle be re-aggregated… except they can't for dispersion measures (see Decision), and that asymmetry is exactly what needs a contract.

The question: what is the **minimum contract** that makes grain-bucketed rollups safe to materialize and route, without inventing a calendar engine in core?

## Decision

### 1. The grain dimension is an explicitly declared field, not an inference

`RollupSpec` gains one field:

```scala
final case class RollupSpec(
    name: String,
    dimensions: List[String],
    measures: List[String],
    timeGrain: Option[String] = None,
    grainDimension: Option[String] = None   // NEW
) extends Product with Serializable
```

- `grainDimension` MUST name a dimension already in `dimensions` (validated in `ModelValidator` alongside the existing ref checks; duplicate/unknown refs fail loud per the existing PR-M2 discipline).
- It is `Option` (not required) because grain-less rollups (the entire existing surface) must keep validating unchanged.
- **Validation rule**: `timeGrain.isDefined` ⇔ `grainDimension.isDefined` — a grain without an axis and an axis without a grain are each a distinct `ModelValidationError`. (One is meaningless; the other changes query semantics without the label that governs routing.)

Why explicit over inference: an inference rule ("the dimension whose values look like dates") is exactly the silent-defaulting class the closed-ADT discipline forbids. The declaration site states its intent; the validator fails loud.

### 2. The value-domain contract: the grain dim's declared element type must be a temporal type

The grain dimension's **declared** `SealedDataType` (from `Dimension.dataType`, already carried on the model) must be one of `Date` or `Timestamp`. Varchar ISO-8601 strings are deliberately **excluded** in v1: string comparison/ordering is lexicographic, and `Date`-truncation semantics over strings would require a parsing contract core cannot verify. If a source only has strings, the model author adds a calculated `Date` dimension (existing transform surface) and declares that.

The bucketing function itself lives in the ENGINE adapter (this is date arithmetic, engine-specific by nature):

- Spark: `date_trunc(grain, col)` (or `date_trunc` on the timestamp).
- The core contract is only: *the bucketed value is deterministic, order-preserving within a grain, and identical for all rows in the same bucket*. The adapter proves this by test (parity suite, below).

### 3. KnownGrains stays the closed vocabulary; truncation is the only bucketing op

`KnownGrains = {hour, day, week, month, quarter, year}` remains the canonical set (`RollupRewriter.KnownGrains`, unchanged). `normalizeGrain` stays trim+lowercase. **Bucketing = calendar truncation** (`date_trunc`) to the declared grain — nothing else (no custom buckets, no fiscal calendars, no timezone parameter in v1; see Open questions).

Rationale for truncation-only: it is the only op whose inverse relationship ("a day-bucket row represents all events in that day") is simple enough to make the ROUTING decision below provable rather than heuristic.

### 4. Routing: a grain rollup serves ONLY the exact grain (v1) — coarsening is refused, not approximated

The tempting feature — serve a `month` query from a `day` rollup by re-aggregating across buckets — is **allowed only for Additive measures and refused for everything else**:

| Measure class | Serve coarser query from finer rollup? |
|---|---|
| Additive (Sum/Count) | YES — `Sum` over day-totals is month-total |
| Min/Max | YES — `Min`/`Max` over bucket extremes is the coarser extreme |
| Avg | YES via the existing two-phase re-aggregation (sum + count totals are Additive) |
| Stddev*/Variance* | **NO in v1** — M2 IS additive, so technically servable… but see below |
| Positional (First/Last) | NO — permanent refusal (unchanged) |
| Holistic / Approximable | NO — permanent refusal (unchanged) |

**Why Stddev*/Variance* coarsening is refused despite M2 being additive**: it is mathematically servable (M2, n, sum all coarsen exactly). It is refused in v1 for a scope reason, not a correctness one: coarsening multiplies the routing matrix (exact-grain × coarsen-across-grain) and doubles the parity surface. The exact-grain path via T7/T8's Aggregate→Project already works for these fns; coarsening adds no capability that exact-grain rollups at the coarser grain don't already provide (declare the month rollup too — materialization is cheap). A future ADR can enable it behind the same state-column machinery.

**The routing rule concretely** (`RollupRewriter.grainsAgree` evolves from equality to a subsumption check):

```
serveFrom(rollupGrain, queryGrain):
  rollupGrain == None                 -> only queryGrain == None (unchanged exact semantics)
  queryGrain == None                  -> NO (a grain-less query groups across all time;
                                          a grained rollup stores per-bucket rows and
                                          re-aggregation would need the coarsening matrix
                                          — deferred with the table above)
  rollupGrain == queryGrain           -> YES (exact; the T7/T8 path)
  rollupGrain finer than queryGrain   -> YES for Sum/Count/Min/Max/Avg only,
                                          via the same Aggregate→Project shape with
                                          the grain dim re-truncated in the inner Aggregate
  rollupGrain coarser than queryGrain -> NO (can't split a month bucket into days)
```

Grain ordering (`hour < day < week < month < quarter < year`) becomes a total order on `KnownGrains`; "finer/coarser" is that order. Week is the known odd case (`date_trunc('week')` = Monday-start, ISO) — pinned by test, not by convention.

**Refusal taxonomy**: a query whose grain cannot be served from an otherwise-matching rollup falls through to the base path (fail-open, perf loss — never wrong numbers), consistent with every existing refusal. No new refusal case is required for v1 (the existing `GrainMismatch` already covers "no rollup at the right grain"); the coarsening matrix is expressed as an extension of `grainsAgree`, not a new ADT case.

### 5. Materialization: truncate in the materializer's group-by, keyed by the declared grain

`RollupMaterializer.validateSpec` drops its blanket `timeGrain` refusal and replaces it with the type check (Decision §2). The write path groups by `(declared dims with the grain dimension replaced by date_trunc(grain, dim))`:

- Spark: `date_trunc(grain, col(grainDimension)).as(grainDimension)` in the `groupBy`, mirroring what the rewriter's routed plan will compute at read time.
- The rollup table's grain-dim column becomes Timestamp-typed (truncation of a Date promotes to timestamp in `date_trunc`; pinned by the schema-parity pin).

The existing state-column machinery (including T8's Welford triple) is untouched — grain bucketing changes WHAT the groups are, not WHAT is aggregated per group.

### 6. Refresh semantics: unchanged

`RollupRefresher` already re-materializes whole rollup tables (`saveAsTable` overwrite). Grain buckets are deterministic functions of the source rows, so refresh = recompute; no incremental bucket bookkeeping in v1.

## Consequences

**Core (sm8-core):**
- `RollupSpec` + `grainDimension: Option[String]`; `ModelValidator` gains the co-presence + ref + temporal-type checks. Loader parses `grain_dimension` / `grainDimension` (same dual-key pattern as `time_grain`).
- `grainsAgree` generalizes to the subsumption matrix (§4); `KnownGrains` gains an ordering function (`finerThan`/rank), keeping the closed vocabulary.
- The rewriter's routed plan, when coarsening, wraps the grain dim in a truncation Expr — a NEW small Expr surface (`Expr.FunctionCall("date_trunc", [grain, col])`-shaped) that the Spark connector's builtin allowlist (T8) extends with `date_trunc` (2-arg). The allowlist pattern from #341 is the template.

**Connector (spark-connector):**
- `RollupMaterializer`: replace the blanket refusal with the temporal-type check; group-by truncation as above.
- `PortableExprCompiler`: `date_trunc(grain, col)` builtin arm (2-arg; grain must be a compile-time string literal from the plan, not a column).
- Parity suite extends the existing harness: grain rollups materialize → coarsening + exact-grain queries route → both paths agree (incl. NULL/empty buckets, month boundaries, week-Monday-start pin).

**Docs:** `RollupSpec` scaladoc's "opaque label" paragraph is superseded by the contract; the rollup-refresh runbook's "declare grain-less" caveat is removed when this ships.

**Non-consequences:**
- No timezone handling in v1 (`date_trunc` operates in the session zone; pinning the session zone is an ops concern, documented in the runbook).
- No fiscal calendars, no custom buckets, no string-typed grain dims.
- No change to grain-less rollups (the entire existing test surface).

## Alternatives Considered

1. **Infer the grain dim from the value domain** (pick the dimension whose type is Date/Timestamp; error if ambiguous). Rejected: silent-defaulting discipline — ambiguity becomes a runtime surprise instead of a declaration-time error; also breaks when a rollup has TWO date dims (order date vs ship date).
2. **Coarsen for all re-aggregable fns including dispersion in v1**. Rejected for scope: doubles the parity matrix; exact-grain rollups at the coarser grain already deliver the capability; M2-additivity makes it a safe future extension behind the same machinery.
3. **Serve grain-less queries from grained rollups** (aggregate across all buckets). Rejected: equivalent to a full-table aggregate — the base path already does that with less machinery; no win.
4. **String-typed grain dims via a parse contract**. Rejected in v1: the parsing contract (which ISO forms? implicit cast failures?) is a larger surface than the feature; calculated-Date dims cover it.
5. **A typed `Grain` ADT replacing `Option[String]`.** Rejected for now: `KnownGrains` + `normalizeGrain` already function as the closed vocabulary with fail-safe unknown handling; a full ADT migration touches the wire DTO (`QueryRequest.timeGrain: Option[String]`) for no behavioral gain in v1. Noted as a v2 refinement if grain-specific behavior accumulates.

## Open questions

1. **Timezone**: is session-zone truncation acceptable for all consumers, or must the rollup declaration carry an explicit zone? (Ops decision; default v1 = session zone, documented.)
2. **Week start**: ISO Monday-start via `date_trunc('week')` — acceptable, or does any consumer need Sunday-start (US convention)? Pin in test either way.
3. **`timestamp`-typed grain dims with sub-second components**: truncation handles them; confirm no consumer needs sub-day grains below `hour` (minute buckets would balloon cardinality).

## References

- ADR-0022 — parent design; the "Known v1 limits" item this closes.
- ADR-0023 — two-phase Aggregate→Project + builtin-allowlist pattern this ADR extends with `date_trunc`.
- #341 — Welford state + allowlist implementation the coarsening path rides on.
- `RollupRewriter.KnownGrains` / `normalizeGrain` / `grainsAgree` — the surface this ADR evolves.
- `examples/hospital-cleaning/models/patients.yml` — `smallest_time_grain` precedent from the upstream semantic layer (informs the declaration shape, not adopted verbatim).

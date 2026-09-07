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
- **Where the checks live** (one place each, no duplication):
  - Co-presence + ref-membership: `ModelValidator.validate` (pure, IO-free), inside the existing rollup-ref loop — the same block that already rejects unknown measure/dimension refs.
  - Temporal-type check (§2): `ModelValidator.validateAgainstSchema` (the existing schema-aware entry point), because it needs the RESOLVED type when the declared `Dimension.dataType` is `None`.

Why explicit over inference: an inference rule ("the dimension whose values look like dates") is exactly the silent-defaulting class the closed-ADT discipline forbids. The declaration site states its intent; the validator fails loud.

### 2. The value-domain contract: the grain dimension's element type must be temporal

The grain dimension's element type must be `Date` or `Timestamp`. The check runs in TWO stages, matching the existing pure/schema validator split:

1. `ModelValidator.validate` (pure): if the dimension's DECLARED `Dimension.dataType` is defined, it must be `Date` or `Timestamp` — anything else (e.g. `Varchar`) is a validation error at declaration time.
2. `ModelValidator.validateAgainstSchema` (schema-aware): if the declared `dataType` is `None` (the loader fallback), the RESOLVED source schema decides — the resolved column type must be `Date`/`Timestamp`, else fail loud.

A `None`-typed grain dim that reaches materialization WITHOUT passing either check is impossible: `Model.of` runs `validate`, and the engine path runs `validateAgainstSchema` after source resolution. Varchar ISO-8601 strings are deliberately **excluded** in v1: string ordering is lexicographic, and Date-truncation semantics over strings would require a parsing contract core cannot verify. If a source only has strings, the model author adds a calculated `Date` dimension (existing transform surface) and declares that.

On portability: the op (calendar truncation) is portable; the function (`date_trunc`) is the v1 Spark implementation. The IR shape `FunctionCall("date_trunc", _)` is portable in name across connectors; each engine maps it to its native truncation primitive (or refuses via the typed allowlist mechanism until it can).

### 3. KnownGrains stays the closed vocabulary; truncation is the only bucketing op

`KnownGrains = {hour, day, week, month, quarter, year}` remains the canonical set (`RollupRewriter.KnownGrains`, unchanged). `normalizeGrain` stays trim+lowercase. **Bucketing = calendar truncation** (`date_trunc`) to the declared grain — nothing else (no custom buckets, no fiscal calendars, no timezone parameter in v1; see Open questions).

Rationale for truncation-only: it is the only op whose inverse relationship ("a day-bucket row represents all events in that day") is simple enough to make the ROUTING decision below provable rather than heuristic.

### 4. Routing: exact grain + a closed coarsening matrix

The tempting feature — serve a `month` query from a `day` rollup by re-aggregating across buckets — is **allowed only for Additive measures and Avg, refused for everything else**. The routing rule is a total case-arm function over `(rollupGrain, queryGrain)` after normalization:

```
grainsAgree(rollupGrain, queryGrain):            // both Option[String], normalized
  case (None, None)        -> YES  // exact match: the unchanged grain-less path
  case (Some(_), None)     -> NO   // grain-less query over a grained rollup:
                                    //   re-aggregating across ALL buckets is a
                                    //   full-table aggregate — the base path already
                                    //   does that with less machinery
  case (None, Some(_))     -> NO   // grained query over a grain-less rollup:
                                    //   the rollup has no time bucketing to serve
  case (Some(r), Some(q)) if r == q     -> YES  // exact grain: the T7/T8 path
  case (Some(r), Some(q)) if r finer q  -> YES for Sum/Count/Min/Max (Additive —
                                          bucket totals re-add) and Avg (via the
                                          existing two-phase shape: sum_total and
                                          count_total are both Additive). The YES set
                                          is {Additive, Avg-via-algebraic};
                                          Stddev*/Variance* are REFUSED despite being
                                          algebraically servable (M2 is additive) — v1
                                          scope discipline, not a correctness wall
                                          (declare the coarser rollup instead).
  case (Some(r), Some(q)) if r coarser q -> NO  // cannot split a month bucket into days
```

This REPLACES today's equality-only `grainsAgree`. Two existing behaviors are preserved exactly: `(None, None)` matches (the entire existing grain-less test surface), and grained-query-vs-day-rollup mismatch falls to `GrainMismatch` (today's fail-safe). Grain ordering (`hour < day < week < month < quarter < year`) becomes a total order on `KnownGrains`; "finer/coarser" is that order. Week is the known odd case (`date_trunc('week')` = Monday-start, ISO) — pinned by test, not by convention.

**Refusal taxonomy**: unchanged. A query whose grain cannot be served from an otherwise-matching rollup falls through to the base path (fail-open, perf loss — never wrong numbers) via the existing `GrainMismatch`. No new refusal ADT case.

### 5. Materialization: truncate in the materializer's group-by, keyed by the declared grain

`RollupMaterializer.validateSpec` drops its blanket `timeGrain` refusal and replaces it with the type check (Decision §2). The write path groups by `(declared dims with the grain dimension replaced by date_trunc(grain, dim))`:

- Spark: `date_trunc(grain, col(grainDimension)).as(grainDimension)` in the `groupBy`, mirroring what the rewriter's routed plan will compute at read time.
- **Schema reconciliation rule**: `date_trunc` always emits Timestamp (it promotes a `Date` input). Both `rollupSchema` and `reconciledRollupSchema` therefore OVERRIDE the grain dimension's type to `Timestamp` when it is the rollup's `grainDimension` — regardless of the declared `Dimension.dataType`. Without this override the rewriter's IR scan schema (declared type) and the materializer's physical table (truncated type) drift — exactly the schema-parity seam `RollupMaterializerSpec`'s drift pin guards; the pin extends to cover the grain case.
- The existing state-column machinery (including T8's Welford triple) is untouched — grain bucketing changes WHAT the groups are, not WHAT is aggregated per group.

### 6. Refresh semantics: unchanged

`RollupRefresher` already re-materializes whole rollup tables (`saveAsTable` overwrite — the eager persistence path; the temp-view path serves same-session reads; both keep the `<model>__<rollup>` naming convention the rewriter re-scans). Grain buckets are deterministic functions of the source rows, so refresh = recompute; no incremental bucket bookkeeping in v1.

## Consequences

**BEHAVIOR CHANGE (the one silent flip this ADR makes):** a grain-less query (`requestGrain = None`) NO LONGER matches a grained rollup. Pre-ADR: `Rewritten` (the existing `grainsAgree` equality treats None==None as a match regardless of the rollup's grain — reachable today by declaring a grained rollup). Post-ADR: `GrainMismatch` → base path. Every model that declares a grained rollup and runs grain-less queries against it loses rollup serving silently (a perf regression, never wrong numbers — same fail-open discipline as every other refusal). The migration note goes in the rollup-refresh runbook when this ships, and the existing `RollupRewriterSpec` grain-match tests are updated to pin the new contract.

**Core (sm8-core):**
- `RollupSpec` + `grainDimension: Option[String]`; `ModelValidator.validate` (the pure entry point — the declared `Dimension.dataType` on the model is sufficient; no resolved-scan needed) gains three checks: co-presence (`timeGrain.isDefined == grainDimension.isDefined`), ref validity (`grainDimension` must name a dimension in the rollup's `dimensions` — checked in the existing rollup-ref loop), and temporal type (the named dimension's declared type must be `Date` or `Timestamp`). Loader parses `grain_dimension` / `grainDimension` (same dual-key pattern as `time_grain`).
- `grainsAgree` generalizes to the subsumption matrix (§4); `KnownGrains` gains an ordering function (`finerThan`/rank), keeping the closed vocabulary.
- The rewriter's routed plan, when coarsening, wraps the grain dim in a truncation Expr — `Expr.FunctionCall("date_trunc", [grainLiteral, col])` where the grain literal is sourced from `spec.timeGrain` (the rollup's declared grain — it matches the physical column encoding on disk; a compile-time string by construction). The Spark connector's builtin allowlist (T8) extends with `date_trunc` (2-arg). The allowlist pattern from #341 is the template. Portability note: the OP (calendar truncation) is portable; `date_trunc` is the v1 Spark spelling — each connector maps the portable IR shape to its native truncation primitive.
- File-level change list: `RollupSpec.scala` (field), `ModelLoader.scala` dual-key region (~:521), `ModelValidator.scala` rollup-ref loop, `RollupRewriter.scala` (`grainsAgree` subsumption + coarsening emit + `rollupSchema`/`reconciledRollupSchema` Timestamp override), `RollupRewriterSpec.scala` (grain-match tests + new coarsening pins).

**Connector (spark-connector):**
- `RollupMaterializer`: replace the blanket refusal with the temporal-type check; group-by truncation as above; schema override per §5.
- `PortableExprCompiler`: `date_trunc(grain, col)` builtin arm (2-arg; grain is a compile-time string literal from the plan, not a column).
- Parity suite extends the existing harness: grain rollups materialize → coarsening + exact-grain queries route → both paths agree (incl. NULL/empty buckets, month boundaries, week-Monday-start pin, Date-declared grain dims materializing as Timestamp).
- File-level change list: `RollupMaterializer.scala` (refusal drop ~:122 + groupBy truncation), `PortableExprCompiler.scala` (allowlist arm), `RollupMaterializerSpec` / parity specs (new cases).

**Docs:** `RollupSpec` scaladoc's "opaque label" paragraph is superseded by the contract; the rollup-refresh runbook's "declare grain-less" caveat is removed when this ships.

**Non-consequences:**
- No timezone handling in v1 (`date_trunc` operates in the session zone; session-zone truncation is the pinned v1 default, documented in the runbook).
- No fiscal calendars, no custom buckets, no string-typed grain dims.
- Week start is pinned to ISO Monday (`date_trunc('week')`); no Sunday-start variant.
- No sub-day grains below `hour`; no change to grain-less rollups (the entire existing test surface).

## Alternatives Considered

1. **Infer the grain dim from the value domain** (pick the dimension whose type is Date/Timestamp; error if ambiguous). Rejected: silent-defaulting discipline — ambiguity becomes a runtime surprise instead of a declaration-time error; also breaks when a rollup has TWO date dims (order date vs ship date).
2. **Coarsen for all re-aggregable fns including dispersion in v1**. Rejected for scope: doubles the parity matrix; exact-grain rollups at the coarser grain already deliver the capability; M2-additivity makes it a safe future extension behind the same machinery.
3. **Serve grain-less queries from grained rollups** (aggregate across all buckets). Rejected: equivalent to a full-table aggregate — the base path already does that with less machinery; no win.
4. **String-typed grain dims via a parse contract**. Rejected in v1: the parsing contract (which ISO forms? implicit cast failures?) is a larger surface than the feature; calculated-Date dims cover it.
5. **A typed `Grain` ADT replacing `Option[String]`.** Rejected for now: `KnownGrains` + `normalizeGrain` already function as the closed vocabulary with fail-safe unknown handling; a full ADT migration touches the wire DTO (`QueryRequest.timeGrain: Option[String]`) for no behavioral gain in v1. Noted as a v2 refinement if grain-specific behavior accumulates.

## Open questions

1. **Sub-day grains below `hour`** (minute buckets): truncation handles them mechanically, but cardinality balloons toward base-table size. Deferred unless a consumer surfaces the need — the grain vocabulary stays `{hour, day, week, month, quarter, year}`.

(The timezone and week-start questions from earlier drafts are CLOSED in v1: session-zone truncation and ISO Monday-start week are pinned decisions, documented under Non-consequences.)

## References

- ADR-0022 — parent design; the "Known v1 limits" item this closes.
- ADR-0023 — two-phase Aggregate→Project + builtin-allowlist pattern this ADR extends with `date_trunc`.
- #341 — Welford state + allowlist implementation the coarsening path rides on.
- `RollupRewriter.KnownGrains` / `normalizeGrain` / `grainsAgree` — the surface this ADR evolves.
- `examples/hospital-cleaning/models/patients.yml` — `smallest_time_grain` precedent from the upstream semantic layer (informs the declaration shape, not adopted verbatim).

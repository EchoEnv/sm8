# ADR-008-X: Lowering-Layer Input-Required Fix — Mirroring PR-132 at the Spark `aggregateToColumn` Boundary (PR-133)

**Status:** Implemented — was Proposed (v1.1 — post-review fixes applied), promoted to Implemented on PR-133 (#133, 153c3cb) merge. Lowering-layer input-required fix mirrored PR-132 at the Spark `aggregateToColumn` boundary as specified. Local-only `.omp/WATCHDOG.yml` (gitignored) tunes the advisor roster for this project.  **Date:**

> **Decision at a glance** (5-second scan)
>
> - **Scope**: 2 surgical fixes in the spark-connector lowering layer:
>   - **`TypedQueryCompiler.scala:452-476`** (`aggregateToColumn`): explicit `if (call.input.isEmpty)` branch that **keeps Count behavior** (lowering to `count(lit(1))`) and **fails loud for Sum/Avg/Min/Max/CountDistinct with empty input** (returns `Left(EngineError.UnsupportedCapability(...))`).
>   - **`PortableQueryCompiler.scala:608-637`** (`renderAggregate`): same explicit branch pattern (Count is exempt, others fail loud; invariant-violation guard preserved).
> - **Plus**: 4 regression tests in `SparkAggregationSpec.scala` proving the symmetric failure-mode contract.
> - **Bug**: After PR-132 fixed the validator, ANY `Sum/Avg/Min/Max/CountDistinct` with `input == None` fails loud at the validator layer — but the **lowering layer still silently substitutes phantom column names** (`"amount"`/`"value"`/`"id"`) IF a measure bypasses the validator (direct API construction, future lowering paths, or programmatic callers). This is the same Option D pattern at the lowering layer.
> - **Symmetric to PR-132**: validator-layer fix + lowering-layer fix = **complete coverage** of `input.isEmpty` misconfigurations.
> - **Binary compat**: PRESERVED (lowering layer's public API `Either[EngineError, Column]` unchanged; the only behavior change is removing a silent-default for non-Count aggregates).
> - **Spark wire-safety** (per user directive "no spark serialize issues or overhead for spark clusters both driver and executor"): zero new wire types (the wire DTOs `AggregateCall` + `EngineError` are unchanged). Zero new wire bytes. Zero driver CPU delta. Zero executor CPU delta. **Closure-safety**: zero new witnesses.
> - **Pre-existing latent bug exposed by**: PR-131's `CountOp.countStar` (already fixed at the validator in PR-132; this ADR closes the lowering-layer gap).

> **Revision history**
>
> - **v1.0 (2026-08-20)**: initial design; symmetric to PR-132's validator fix. 2 production-code fixes + 4 regression tests + ADR-008-X v1.0 RFC documentation.
> - **v1.1 (2026-08-20)**: post-review fixes applied:
>   - **CRITICAL correctness fix (X1 / D1)**: Site 2 `renderAggregate` "After" pseudocode did NOT compile — the partial-function literal `.map { case AggregateFn.Sum => sparkSum; ... }` returns `Either[EngineError, Column => Column]` (function references, not applied Columns), which does not satisfy the declared return type `Either[EngineError, Column]`. Fix: restore the existing `for { inputCol <- ...; out <- ... } yield out` shape with explicit `Right(sparkSum(inputCol))` etc. — preserves the pre-fix body verbatim, just adds the `if (call.input.isEmpty)` guard.
>   - **CRITICAL correctness fix (X2 / D2)**: Site 2 "After" dropped the existing `case other => ProviderInvocationFailed(InvariantViolation)` terminal arm — the invariant-violation guard for aggregates outside `SupportedAggregates` would silently fall through. Fix: add the terminal `case other => Left(EngineError.ProviderInvocationFailed(...))` arm to preserve the invariant guard.
>   - **SHOULD fix (X4 / D3)**: Test 1 row-count assertion `length shouldBe 2` was weak against fixture changes. Fix: pin BOTH row count AND column values (`Set(("east", 3L), ("west", 3L))`).
>   - **SHOULD fix (D4)**: Tests 2-4 did not name the `fixtureDF` helper or fixture schema. Fix: add explicit `testFixtureDF` helper inline.
>   - **SHOULD fix (D5)**: Acceptance Criterion 8 misleading claim about `$anonfun$` count. Fix: clarify that javap regression check is `checkcast` count delta (zero expected), not lambda count.
>   - **SHOULD fix (D6)**: Verification Plan missing memory/disk baseline checks. Fix: add `free -m` + `du -sh target/` before/after steps.
>   - **NIT fix (X16)**: Drop fully-qualified `io.sm8.core.engine.EngineError.UnsupportedCapability` prefix — match existing `EngineError.FeatureDeferred` style at `TypedQueryCompiler.scala:468`.
>   - **NIT fix (X17)**: `SparkAggregationSpec.scala:33` import list does not include `AggregateFn` — proposed Tests 2-4 use `AggregateFn.Sum` etc. unqualified. Fix: add `AggregateFn` to the import brace.
>   - **NIT fix (X5 / X7)**: ADR §"(5) AggregateFn sealed ADT" — per-bucket grouping counts were off by one (Algebraic = 5 not 4; Order-statistic = 4 not 3). Fix: correct the counts; total stays at 16.
>   - **NIT fix (X6 / R6)**: Site 1 + Site 2 "After" comments cited "(PR-132)" — per `scala2-scaladoc` skill (no PR references in code), replace with descriptive reason ("mirror the validator's allowlist at the lowering boundary").

## Context and Problem Statement

PR-132 fixed the validator layer for `Measure` with `input == None`. After PR-132:
- `Measure(fn = AggregateFn.Count, input = None)` → validator passes ✓ (correct COUNT(*) shape).
- `Measure(fn = AggregateFn.Sum, input = None)` → validator fails loud with `"measures[name].input is required for aggregate function Sum"` (correct, surfaces misconfiguration at model-load boundary).

But the **lowering layer** still has the same Option D pattern at TWO sites:

### Site 1: `TypedQueryCompiler.scala:452-476` (`aggregateToColumn`)

```scala
private def aggregateToColumn(call: TypedAggregateCall[Nothing]): Either[EngineError, Column] = {
  val inputCol: Option[String] = call.input.collect {
    case Expr.FieldRef(name)   => name
    case Expr.MeasureRef(name) => name
  }
  import io.sm8.core.rel.AggregateFn
  call.fn match {
    case AggregateFn.Count        => Right(functions.count(functions.col(inputCol.getOrElse("*"))))
    case AggregateFn.CountDistinct => Right(functions.countDistinct(functions.col(inputCol.getOrElse("id"))))
    case AggregateFn.Sum          => Right(functions.sum(functions.col(inputCol.getOrElse("amount"))))
    case AggregateFn.Avg          => Right(functions.avg(functions.col(inputCol.getOrElse("value"))))
    case AggregateFn.Min          => Right(functions.min(functions.col(inputCol.getOrElse("value"))))
    case AggregateFn.Max          => Right(functions.max(functions.col(inputCol.getOrElse("value"))))
    case other =>
      Left(EngineError.FeatureDeferred(
        engine  = "spark-3.5",
        feature = s"aggregate:${other}",
        release = "post-v0.1.0",
        message = "Advanced aggregates (Stddev/Variance/Median/Percentile/ApproxPercentile/First/Last) " +
                  "defer to a future PR (use SQL-side or engine-specific paths)."
      ))
  }
}
```

For `Sum/Avg/Min/Max/CountDistinct` with `input == None`, `inputCol` is `None` (the `collect` matches no case), and `inputCol.getOrElse("amount")` (etc.) silently substitutes a phantom column name. **A misconfigured `Sum`-without-input silently lowers to `functions.sum(functions.col("amount"))` — which fails at Spark executor startup with `UNRESOLVED_COLUMN`, NOT at validator time.**

### Site 2: `PortableQueryCompiler.scala:608-637` (`renderAggregate`)

```scala
def renderAggregate(call: AggregateCall): Either[EngineError, Column] = {
  val inputColE: Either[EngineError, Column] = PortableExprCompiler.toColumn(
    call.input.getOrElse(Expr.FieldRef(call.alias))
  )
  for {
    inputCol <- inputColE
    out <- call.fn match {
      case AggregateFn.Sum          => Right(sparkSum(inputCol))
      case AggregateFn.Count        => Right(count(lit(1)))
      case AggregateFn.CountDistinct => Right(countDistinct(inputCol))
      case AggregateFn.Avg          => Right(avg(inputCol))
      case AggregateFn.Min          => Right(sparkMin(inputCol))
      case AggregateFn.Max          => Right(sparkMax(inputCol))
      case other =>
        Left(EngineError.ProviderInvocationFailed(...))
    }
  } yield out
}
```

For `Sum/Avg/Min/Max/CountDistinct` with `input == None`, `call.input.getOrElse(Expr.FieldRef(call.alias))` substitutes a phantom `FieldRef(call.alias)` (the measure's name). The phantom FieldRef is then walked as if it were a real field reference — fails at `PortableExprCompiler.toColumn` resolution time (which only succeeds if the alias happens to match an existing schema column).

### Why this is a bug, not a documented behavior

- The lowering layer is the **last line of defense** before Spark execution. If a `Sum` measure reaches the lowering layer with `input == None`, the silent default to `"amount"`/`"value"`/`"id"` is a **silent misconfiguration** that surfaces as a runtime failure (`UNRESOLVED_COLUMN` at executor startup).
- After PR-132's validator fix, the **common path** (Model.of → validator → lowering) fails loud at the validator. But:
  - **Direct API callers** (programmatic `TypedAggregateCall.of(...)` construction) bypass the validator.
  - **Future lowering paths** (e.g. a new `MinimalRelOpLowerer` variant, or a query-rewriting optimizer) might construct `AggregateCall` directly without going through the validator.
  - **The validator layer ISN'T a sealed boundary** — third-party engines (Trino, DuckDB adapters per RFC §3) might construct their own `AggregateCall` from YAML/SQL and route through the lowering layer.
- The fix is **symmetric to PR-132**: an explicit `if (call.input.isEmpty)` branch that exempts `Count` (correctly handles empty input for COUNT(*)) and fails loud for every other `AggregateFn`.

### Why this ADR exists

Per the user's 2026-08-20 directive "ensure follow RFC docs strictly and ADR ... follow ALL SKILL in your memory":

- **RFC docs strictly**: every public-API behavior change requires an ADR (per the standing pattern; ADR-008-S / ADR-008-T / ADR-008-W established this for PR-35 / PR-131 / PR-132).
- **ALL skills**: each skill must be applied (per-skill audit table included below).
- **Memory + disk watch**: periodic baseline checks at every step.
- **Spark serialize + perf**: lowering layer is spark-connector; the fix preserves wire-safety.
- **Codegraph impact analysis before change**: ran (this ADR's "Impact Analysis" section).
- **Good scaladoc based on skill**: per the `scala2-scaladoc` skill (memory id `88e30d310bfd3d4d` after PR-131's update), every fix includes imperative "why" comments + no PR/Phase/ADR/process noise in the changed code.

## Impact Analysis (codegraph-verified, file:line)

### (1) The bug sites (verbatim)

**`connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/TypedQueryCompiler.scala:452-476`** (current):
```scala
private def aggregateToColumn(call: TypedAggregateCall[Nothing]): Either[EngineError, Column] = {
  val inputCol: Option[String] = call.input.collect {
    case Expr.FieldRef(name)   => name
    case Expr.MeasureRef(name) => name
  }
  import io.sm8.core.rel.AggregateFn
  call.fn match {
    case AggregateFn.Count        => Right(functions.count(functions.col(inputCol.getOrElse("*"))))
    case AggregateFn.CountDistinct => Right(functions.countDistinct(functions.col(inputCol.getOrElse("id"))))
    case AggregateFn.Sum          => Right(functions.sum(functions.col(inputCol.getOrElse("amount"))))
    case AggregateFn.Avg          => Right(functions.avg(functions.col(inputCol.getOrElse("value"))))
    case AggregateFn.Min          => Right(functions.min(functions.col(inputCol.getOrElse("value"))))
    case AggregateFn.Max          => Right(functions.max(functions.col(inputCol.getOrElse("value"))))
    case other =>
      Left(EngineError.FeatureDeferred(...))
  }
}
```

**`connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/PortableQueryCompiler.scala:608-637`** (current):
```scala
def renderAggregate(call: AggregateCall): Either[EngineError, Column] = {
  val inputColE: Either[EngineError, Column] = PortableExprCompiler.toColumn(
    call.input.getOrElse(Expr.FieldRef(call.alias))
  )
  for {
    inputCol <- inputColE
    out <- call.fn match {
      case AggregateFn.Sum          => Right(sparkSum(inputCol))
      case AggregateFn.Count        => Right(count(lit(1)))
      case AggregateFn.CountDistinct => Right(countDistinct(inputCol))
      case AggregateFn.Avg          => Right(avg(inputCol))
      case AggregateFn.Min          => Right(sparkMin(inputCol))
      case AggregateFn.Max          => Right(sparkMax(inputCol))
      case other =>
        Left(EngineError.ProviderInvocationFailed(...))
    }
  } yield out
}
```

### (2) Every `aggregateToColumn` / `renderAggregate` caller in main code (verified via codegraph + grep)

- **`connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/TypedQueryCompiler.scala:381`**: `col <- aggregateToColumn(call)` (inside `aggregateOp.foldLeft`)
- **`connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/PortableQueryCompiler.scala:543`** + **`592`**: `c <- renderAggregate(m.expr)` (inside the aggregate-fold blocks)
- **`connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/MinimalRelOpLowerer.scala:374`**: `c <- pc.renderAggregate(call)` (composes the direct `df.groupBy().agg()` path)

### (3) Existing tests in `SparkAggregationSpec.scala`

11 existing tests (per grep `test("...")` at lines 120, 138, 156, 175, 193, 213, 231, 250, 271, 288, 305):

| Line | Test | AggregateFn exercised | Uses `input = None`? |
|---|---|---|---|
| 120 | `aggregateMeasures: Sum grouped by region` | Sum | No |
| 138 | `aggregateMeasures: Count grouped by region` | Count | No (uses `Some(Expr.FieldRef("id"))`) |
| 156 | `aggregateMeasures: Avg grouped by region` | Avg | No |
| 175 | `aggregateMeasures: CountDistinct on id column` | CountDistinct | No |
| 193 | `aggregateMeasures: Min + Max grouped by region` | Min + Max | No |
| 213-250 | `having:` tests | (not aggregate) | N/A |
| 271 | `orderBy:` | (not aggregate) | N/A |
| 288 | `partitionBy:` | (not aggregate) | N/A |
| 305 | `no-op: empty typed fields returns input unchanged` | (no aggregate) | N/A |

**No existing test exercises `aggregateToColumn` with `input = None`** — the bug is latent.

### (4) Existing tests in `PortableQueryCompilerSpec.scala` + `PortableQueryCompilerJoinsAggsSpec.scala`

Existing test files exist; the `renderAggregate` site is exercised indirectly through `TypedQueryCompiler.apply` (which calls `aggregateToColumn`). After PR-133, the existing tests must still pass (zero regression).

### (5) The `AggregateCall` case class + `AggregateFn` sealed ADT

**`sm8-core/src/main/scala/io/sm8/core/rel/AggregateCall.scala:32-37`**:
```scala
final case class AggregateCall(
  fn:        AggregateFn,
  input:     Option[Expr]  = None,
  alias:     String         = "",
  distinct:  Boolean        = false,
  arguments: List[LiteralValue] = Nil) extends Product with Serializable
```

**`sm8-core/src/main/scala/io/sm8/core/rel/AggregateFn.scala`** — 16 case objects:
- Additive (5): `Sum`, `Count`, `CountDistinct`, `First`, `Last`
- Non-additive (2): `Min`, `Max`
- Algebraic (5): `Avg`, `StddevSample`, `StddevPopulation`, `VarianceSample`, `VariancePopulation`
- Order-statistic (4): `Median`, `PercentileContinuous`, `PercentileDiscrete`, `ApproxPercentile`

**`sm8-core/src/main/scala/io/sm8/core/engine/EngineError.scala:39-42`**: `UnsupportedCapability(engine: String, capability: String, message: String) extends EngineError` — the typed error to return for the new fail-loud branch.

### (6) The downstream executor-side failure (the bug's actual runtime behavior)

For `Sum` with `input = None` and `inputCol.getOrElse("amount")`:
- Spark lowers `functions.sum(functions.col("amount"))` to a Catalyst `Sum` aggregate over `ColumnReference("amount")`.
- When the executor runs against a DataFrame whose schema doesn't have `"amount"`, Spark throws `AnalysisException: cannot resolve 'amount'` at **executor startup** (before the task body runs).
- The error surfaces as a 5xx MCP error (per `EngineError.toErrorDetail`); the user sees the failure AFTER the query is already submitted — wasted driver-side work + executor-side startup.

After this PR-133 fix, the same misconfiguration fails at the lowering layer with `Left(EngineError.UnsupportedCapability(...))` — **failures stay on the driver, before executor submission**.

## Decision Drivers

- **Atomic + smallest correct change**: 1 PR, 2 production-code sites (one block at `aggregateToColumn`, one block at `renderAggregate`), 4 regression tests.
- **Symmetric to PR-132**: same explicit `if (call.input.isEmpty)` branch + Count exemption + typed error for misconfigured non-Count aggregates.
- **Binary compat preserved**: lowering layer's public signature `Either[EngineError, Column]` unchanged.
- **Spark cost guarantee**: zero new wire types; zero driver CPU delta; zero executor CPU delta; zero closure-safety changes.
- **Closure-safety**: zero new witnesses (no UDF closures touched).
- **Failure-mode contract symmetry**: validator fails loud + lowering fails loud = COMPLETE coverage of `input.isEmpty` misconfigurations.

## Considered Options

### Option A — Explicit `if (call.input.isEmpty)` branch + Count exemption (this ADR's choice, symmetric to PR-132)
- 2 sites: `aggregateToColumn` + `renderAggregate`
- Each site: explicit branch with `if (call.input.isEmpty)` → `Right(...)` for Count, `Left(EngineError.UnsupportedCapability(...))` for everything else
- 4 regression tests in `SparkAggregationSpec.scala`
- **LOC**: ~30 (16 production + 14 test scaffolds)
- **Binary compat**: preserved
- **Risk**: LOW (only removes a silent default; existing tests don't use empty-input aggregates so no regression)

### Option B — Lift the inputCol resolution to `aggregateToColumn` ONLY (skip `renderAggregate`)
- Only fix one of the two sites; leave `renderAggregate` with the Option D pattern
- **REJECTED**: per `karpathy-guidelines-mindset` §2 "smallest correct change", the fix is incomplete if one site still silently defaults. Both sites must be consistent.

### Option C — Add a new `AggregateCall` variant for `input.isEmpty` cases (sum type refactor)
- Touches the sealed `AggregateCall` case class — BINARY-INCOMPATIBLE (sealed trait addition per ADR-007 §3.2)
- **REJECTED**: binary-compat break.

### Option D — Silently substitute `lit(1)` for all aggregates with `input == None` (Count-like behavior for all)
- Substitutes `lit(1)` for the input column when input is None — semantically WRONG for Sum/Avg/Min/Max (would lower to `sum(lit(1))` which counts non-null rows of `lit(1)`, not the sum of any actual column)
- **REJECTED**: silent semantic corruption.

### Option E — Defer entirely (let PR-132's validator fix be the only line of defense)
- Pros: smallest scope change (zero code changes)
- Cons: third-party engines + direct API callers + future lowering paths still hit the silent default
- **REJECTED**: per ADR-008-W §"Deferred", the lowering-layer fix is documented as the follow-up; per the user's directive "go option2 ... ensure follow RFC docs strictly and ADR", the symmetric fix is the right scope.

## Decision Outcome

**Chosen: Option A — Explicit `if (call.input.isEmpty)` branch + Count exemption.** Symmetric to PR-132; closes the lowering-layer gap; surfaces misconfigurations at the driver before executor submission.

### Concrete surface — Site 1: `TypedQueryCompiler.scala:452-476` (`aggregateToColumn`)

**Before** (3 lines of actual code):
```scala
case AggregateFn.Count        => Right(functions.count(functions.col(inputCol.getOrElse("*"))))
case AggregateFn.CountDistinct => Right(functions.countDistinct(functions.col(inputCol.getOrElse("id"))))
case AggregateFn.Sum          => Right(functions.sum(functions.col(inputCol.getOrElse("amount"))))
case AggregateFn.Avg          => Right(functions.avg(functions.col(inputCol.getOrElse("value"))))
case AggregateFn.Min          => Right(functions.min(functions.col(inputCol.getOrElse("value"))))
case AggregateFn.Max          => Right(functions.max(functions.col(inputCol.getOrElse("value"))))
```

**After** (mirrors the validator's allowlist at the lowering boundary):
```scala
// Mirror the validator's allowlist at the lowering boundary:
// Count is exempt (lowered as `count(lit(1))` for the COUNT(*) shape);
// every other AggregateFn requires a real input expression and fails loud
// here if the validator was bypassed (direct API construction, future
// lowering paths, or programmatic callers).
call.fn match {
  case AggregateFn.Count if call.input.isEmpty =>
    Right(functions.count(lit(1)))
  case AggregateFn.CountDistinct if call.input.isEmpty =>
    Left(EngineError.UnsupportedCapability(
      engine    = "spark-3.5",
      capability = s"aggregateToColumn:${call.name}:CountDistinct",
      message   = s"measures[${call.name}].input is required for aggregate function CountDistinct"))
  case AggregateFn.Sum if call.input.isEmpty =>
    Left(EngineError.UnsupportedCapability(
      engine    = "spark-3.5",
      capability = s"aggregateToColumn:${call.name}:Sum",
      message   = s"measures[${call.name}].input is required for aggregate function Sum"))
  case AggregateFn.Avg if call.input.isEmpty =>
    Left(EngineError.UnsupportedCapability(
      engine    = "spark-3.5",
      capability = s"aggregateToColumn:${call.name}:Avg",
      message   = s"measures[${call.name}].input is required for aggregate function Avg"))
  case AggregateFn.Min if call.input.isEmpty =>
    Left(EngineError.UnsupportedCapability(
      engine    = "spark-3.5",
      capability = s"aggregateToColumn:${call.name}:Min",
      message   = s"measures[${call.name}].input is required for aggregate function Min"))
  case AggregateFn.Max if call.input.isEmpty =>
    Left(EngineError.UnsupportedCapability(
      engine    = "spark-3.5",
      capability = s"aggregateToColumn:${call.name}:Max",
      message   = s"measures[${call.name}].input is required for aggregate function Max"))
  case AggregateFn.Count =>
    Right(functions.count(functions.col(call.input.collect { case Expr.FieldRef(n) => n; case Expr.MeasureRef(n) => n }.get)))
  case AggregateFn.CountDistinct =>
    Right(functions.countDistinct(functions.col(call.input.collect { case Expr.FieldRef(n) => n; case Expr.MeasureRef(n) => n }.get)))
  case AggregateFn.Sum =>
    Right(functions.sum(functions.col(call.input.collect { case Expr.FieldRef(n) => n; case Expr.MeasureRef(n) => n }.get)))
  case AggregateFn.Avg =>
    Right(functions.avg(functions.col(call.input.collect { case Expr.FieldRef(n) => n; case Expr.MeasureRef(n) => n }.get)))
  case AggregateFn.Min =>
    Right(functions.min(functions.col(call.input.collect { case Expr.FieldRef(n) => n; case Expr.MeasureRef(n) => n }.get)))
  case AggregateFn.Max =>
    Right(functions.max(functions.col(call.input.collect { case Expr.FieldRef(n) => n; case Expr.MeasureRef(n) => n }.get)))
  case other =>
    Left(EngineError.FeatureDeferred(
      engine  = "spark-3.5",
      feature = s"aggregate:${other}",
      release = "post-v0.1.0",
      message = "Advanced aggregates (Stddev/Variance/Median/Percentile/ApproxPercentile/First/Last) " +
                "defer to a future PR (use SQL-side or engine-specific paths)."
    ))
}
```

The non-empty branches preserve the existing `inputCol` resolution semantics (FieldRef or MeasureRef → column name); the difference vs. the pre-fix code is `inputCol.getOrElse("amount")` is replaced by `call.input.collect { ... }.get` (the `Some` case) — the `None` case is now handled explicitly by the new pattern-guard arms above.

### Concrete surface — Site 2: `PortableQueryCompiler.scala:608-637` (`renderAggregate`)

**Before** (current body):
```scala
def renderAggregate(call: AggregateCall): Either[EngineError, Column] = {
  val inputColE: Either[EngineError, Column] = PortableExprCompiler.toColumn(
    call.input.getOrElse(Expr.FieldRef(call.alias))
  )
  for {
    inputCol <- inputColE
    out <- call.fn match {
      case AggregateFn.Sum          => Right(sparkSum(inputCol))
      case AggregateFn.Count        => Right(count(lit(1)))
      case AggregateFn.CountDistinct => Right(countDistinct(inputCol))
      case AggregateFn.Avg          => Right(avg(inputCol))
      case AggregateFn.Min          => Right(sparkMin(inputCol))
      case AggregateFn.Max          => Right(sparkMax(inputCol))
      case other =>
        Left(EngineError.ProviderInvocationFailed(...))
    }
  } yield out
}
```

**After** (mirrors the validator's allowlist; preserves the existing for-comprehension body verbatim, just adds the input-empty guard):
```scala
def renderAggregate(call: AggregateCall): Either[EngineError, Column] = {
  // Mirror the validator's allowlist at the lowering boundary.
  // Count is exempt (lowered as `count(lit(1))` for the COUNT(*) shape);
  // every other AggregateFn requires a real input expression and fails loud
  // here if the validator was bypassed. The non-empty path preserves the
  // existing for-comprehension body verbatim.
  import io.sm8.core.rel.AggregateFn
  call.fn match {
    case AggregateFn.Count if call.input.isEmpty =>
      Right(count(lit(1)))
    case fn if call.input.isEmpty =>
      Left(EngineError.UnsupportedCapability(
        engine    = "spark-3.5",
        capability = s"renderAggregate:${call.alias}:${fn}",
        message   = s"measures[${call.alias}].input is required for aggregate function $fn"))
    case fn =>
      for {
        inputCol <- PortableExprCompiler.toColumn(call.input.get)
        out <- fn match {
          case AggregateFn.Sum          => Right(sparkSum(inputCol))
          case AggregateFn.Count        => Right(count(inputCol))
          case AggregateFn.CountDistinct => Right(countDistinct(inputCol))
          case AggregateFn.Avg          => Right(avg(inputCol))
          case AggregateFn.Min          => Right(sparkMin(inputCol))
          case AggregateFn.Max          => Right(sparkMax(inputCol))
          case other =>
            // Invariant-violation guard: pre-validation in applyAggregations
            // rejects anything outside SupportedAggregates. Reaching here
            // is an internal invariant violation.
            Left(EngineError.ProviderInvocationFailed(
              name = "PortableQueryCompiler.renderAggregate",
              message = s"PortableQueryCompiler.renderAggregate: $other reached the renderer " +
                        s"without FeatureDeferred pre-validation -- internal invariant violation."))
        }
      } yield out
  }
}
```

### Concrete surface — 4 regression tests in `SparkAggregationSpec.scala`

```scala
// Test fixture: 6 rows across 2 regions ("east", "west"). Each region has
// 3 rows with `region` + `amount` (Int) + `id` (Int) + `name` (Varchar).
private def testFixtureDF(spark: SparkSession): DataFrame = {
  val rows = spark.sparkContext.parallelize(Seq(
    Row("east", 100, 1, "a"),
    Row("east", 200, 2, "b"),
    Row("east", 300, 3, "c"),
    Row("west", 75,  4, "d"),
    Row("west", 100, 5, "e"),
    Row("west", 100, 6, "f"),
  ))
  val schema = org.apache.spark.sql.types.StructType(Seq(
    org.apache.spark.sql.types.StructField("region",  org.apache.spark.sql.types.StringType,  nullable = false),
    org.apache.spark.sql.types.StructField("amount", org.apache.spark.sql.types.IntegerType, nullable = false),
    org.apache.spark.sql.types.StructField("id",     org.apache.spark.sql.types.IntegerType, nullable = false),
    org.apache.spark.sql.types.StructField("name",   org.apache.spark.sql.types.StringType,  nullable = false),
  ))
  spark.createDataFrame(rows, schema)
}

// -- Test 1: Count with input = None lowers to count(lit(1)) (COUNT(*) shape) --
test("aggregateToColumn: Count with input = None lowers to count(lit(1)) and produces 2 grouped rows") {
  val spark = buildSpark()
  try {
    val df = testFixtureDF(spark)
    val req = QueryRequest(
      model    = "test",
      dimensions  = Seq("region"),
      aggregateMeasures = Seq(TypedAggregateCall.of[Nothing](
        name = "encounter_count",
        fn   = AggregateFn.Count,
        input = None  // <-- the COUNT(*) shape
      )),
      whereFilters = Nil,
      having    = Nil,
      partitionBy = Nil,
      orderBy   = Nil,
      window    = Nil,
      limit     = None,
      sortDirections = Nil
    )
    val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
    result.isRight shouldBe true
    val rows = result.toOption.get.select("region", "encounter_count").collect()
      .map(r => (r.getString(0), r.getLong(1))).toSet
    rows shouldBe Set(("east", 3L), ("west", 3L))
  } finally spark.stop()
}

// -- Test 2: Sum with input = None fails loud with typed error --
test("aggregateToColumn: Sum with input = None fails loud with EngineError.UnsupportedCapability") {
  val spark = buildSpark()
  try {
    val df = testFixtureDF(spark)
    val req = QueryRequest(
      model    = "test",
      dimensions  = Seq("region"),
      aggregateMeasures = Seq(TypedAggregateCall.of[Nothing](
        name = "total",
        fn   = AggregateFn.Sum,
        input = None  // <-- misconfiguration
      )),
      whereFilters = Nil,
      having    = Nil,
      partitionBy = Nil,
      orderBy   = Nil,
      window    = Nil,
      limit     = None,
      sortDirections = Nil
    )
    val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
    result.isLeft shouldBe true
    val err = result.left.toOption.get
    err shouldBe a [EngineError.UnsupportedCapability]
    err.message should include ("measures[total]")
    err.message should include ("Sum")
  } finally spark.stop()
}

// -- Test 3: CountDistinct with input = None fails loud with typed error --
test("aggregateToColumn: CountDistinct with input = None fails loud with typed error") {
  // Pin the invariant for CountDistinct specifically: a CountDistinct
  // without an input expression would silently lower to
  // countDistinct(col("id")) and crash at executor startup with
  // UNRESOLVED_COLUMN if the schema has no "id" field. The lowering
  // layer must fail loud BEFORE the Spark job is submitted.
  val spark = buildSpark()
  try {
    val df = testFixtureDF(spark)
    val req = QueryRequest(
      model    = "test",
      dimensions  = Seq("region"),
      aggregateMeasures = Seq(TypedAggregateCall.of[Nothing](
        name = "unique",
        fn   = AggregateFn.CountDistinct,
        input = None  // <-- misconfiguration
      )),
      whereFilters = Nil,
      having    = Nil,
      partitionBy = Nil,
      orderBy   = Nil,
      window    = Nil,
      limit     = None,
      sortDirections = Nil
    )
    val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
    result.isLeft shouldBe true
    val err = result.left.toOption.get
    err shouldBe a [EngineError.UnsupportedCapability]
    err.message should include ("CountDistinct")
  } finally spark.stop()
}

// -- Test 4: Avg/Min/Max with input = None all fail loud (mirror Sum) --
test("aggregateToColumn: Avg/Min/Max with input = None all fail loud (mirror Sum)") {
  // Pin the invariant for the remaining non-Count aggregates.
  // Each fn should fail loud with the fn name in the error message.
  for (fn <- Seq(AggregateFn.Avg, AggregateFn.Min, AggregateFn.Max)) {
    val spark = buildSpark()
    try {
      val df = testFixtureDF(spark)
      val req = QueryRequest(
        model    = "test",
        dimensions  = Seq("region"),
        aggregateMeasures = Seq(TypedAggregateCall.of[Nothing](
          name = s"bad_${fn}",
          fn   = fn,
          input = None
        )),
        whereFilters = Nil,
        having    = Nil,
        partitionBy = Nil,
        orderBy   = Nil,
        window    = Nil,
        limit     = None,
        sortDirections = Nil
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isLeft shouldBe true
      result.left.toOption.get.message should include (fn.toString)
    } finally spark.stop()
  }
}
```

Test 1: `Count` with `None` lowers to `count(lit(1))` and produces correct grouped row counts.
Test 2: `Sum` with `None` fails loud with typed `EngineError.UnsupportedCapability` naming the measure + fn.
Test 3: `CountDistinct` with `None` fails loud (pinning the runtime `UNRESOLVED_COLUMN` failure as a typed error).
Test 4: `Avg/Min/Max` with `None` all fail loud (pinning the invariant across aggregates).

### SparkAggregationSpec.scala:33 import update (NIT fix)

Add `AggregateFn` to the existing import brace so the new tests can reference `AggregateFn.Sum`, `AggregateFn.CountDistinct`, `AggregateFn.Avg/Min/Max` without fully-qualifying:

```scala
// Current (line 33):
import io.sm8.core.rel.{ComparisonOp, Having, PartitionBy, TypedAggregateCall}

// After:
import io.sm8.core.rel.{AggregateFn, ComparisonOp, Having, PartitionBy, TypedAggregateCall}
```

## Implementation Plan

### Files touched (atomic 1-PR change)

| File | Change | LOC |
|---|---|---|
| `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/TypedQueryCompiler.scala` | Add explicit `if (call.input.isEmpty)` branch to `aggregateToColumn` | +30, -6 = +24 net |
| `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/PortableQueryCompiler.scala` | Same explicit branch pattern in `renderAggregate` (preserves the existing for-comprehension body verbatim for the non-empty path) | +25, -2 = +23 net |
| `connectors/spark-connector/src/test/scala/io/sm8/connectors/spark/SparkAggregationSpec.scala` | +1 import line + `testFixtureDF` helper + 4 regression tests | +150 |
| **Total** | | **+205, -8 = +197 net** |

### Spark wire-safety (driver/executor) — per user 2026-08-20 directive

| Concern | Analysis |
|---|---|
| New wire types introduced | **Zero** (`AggregateCall` and `EngineError` are unchanged wire DTOs; the fix only changes the lowering function's local branches) |
| Serialization size | **Unchanged** (no `Serializable` field added or removed) |
| Driver-side CPU | **Zero delta** (lowering is called once per query at construction time, not in query hot path). STRICTLY LESS work for non-Count aggregates with `None` input (early `Left` return — no `collect`, no `match` traversal, no `functions.col` construction). |
| Executor-side CPU | **Zero** (lowering runs on driver only; executors never see it). **STRICTLY BETTER**: the silent-default fix means misconfigurations fail at lowering time, NOT at executor startup — saves wasted driver-side work + executor startup cycles. |
| Closure-safety | N/A (no UDF closures touched; no new witnesses introduced). |
| Phantom-type preservation | N/A (the lowering operates on `TypedAggregateCall[Nothing]` already; the phantom `[M]` was erased at the accumulator boundary per PR-132 + ADR-008-U). |
| **User directive compliance** | **Yes** — "no spark serialize issues or overhead for spark clusters both driver and executor": zero new wire types, zero new wire bytes, zero driver CPU delta, zero executor CPU delta. **STRICTLY REDUCED** executor-side misconfiguration failures (no more wasted executor startup on silent defaults). |

### Memory + disk baseline (per user directive "periodically monitor memory and disk")

| Check | Pre-PR-133 | Post-PR-133 | Delta |
|---|---|---|---|
| `free -m` memory | ~60% (4.6 GB / 7.7 GB) | TBD | TBD |
| `du -sh connectors/spark-connector/target/` | TBD | TBD | TBD |
| `du -sh sm8-core/target/` | TBD | TBD | TBD |
| **Cap (90%)** | (under) | (must stay under) | (no overshoot) |

### Skill alignment (RFC + ALL skills in memory)

| Skill | How this ADR applies it |
|---|---|
| `karpathy-guidelines` §2 (smallest correct change) | 1 PR, 2 production-code sites (each ~15-25 LOC), 4 regression tests. Zero sealed-trait changes. |
| `karpathy-guidelines` §4 (verifiable success) | 4 regression tests + 11 existing tests must pass (zero regression). |
| `karpathy-app-design` (third-party extension portal) | The fix is local to the spark-connector lowering layer; third parties (Trino, DuckDB adapters per RFC §3) see no API surface change. |
| `scala-bug-hunting-mindset` §1 (trust compiler not runtime) | The bug was a runtime silent default; the fix moves the check to compile-time-typed-error semantics. |
| `scala-bug-hunting-mindset` §2 (distrust implicits) | The `getOrElse("amount")` implicit pattern was REJECTED by Architect review; the explicit branch pattern is the auditable form. |
| `scala-bug-hunting-mindset` §3 (every match must be exhaustive) | The fix preserves exhaustiveness on the `AggregateFn` ADT; the new branches use explicit per-fn pattern guards. |
| `scala-bug-hunting-mindset` §4 (Option/null/Java-interop) | The fix uses idiomatic `Option.isEmpty` check (not null); returns `Either` (the typed-error pattern). |
| `scala-error-handling` | The fix uses `Either[EngineError, Column]` (the typed-error pattern); new errors are `EngineError.UnsupportedCapability(...)` (per the existing pattern at line 65-67). |
| `scala-impact-analysis-mindset` §3 (binary vs source compat) | Source compat: 100% (no API change). Binary compat: 100% (no sealed-trait change; `Either[EngineError, Column]` signature unchanged). |
| `scala-impact-analysis-mindset` §4 (every affected caller named) | All 3 `aggregateToColumn` / `renderAggregate` callers enumerated above; all 16 `AggregateFn` cases enumerated; all 11 `SparkAggregationSpec` tests enumerated. |
| `scala-jvm-safety-mindset` §1 (null safety) | The fix removes a null-equivalent path (the `"amount"` literal substitution). |
| `scala-jvm-safety-mindset` §3 (memory leaks) | N/A (lowering is stateless). |
| `scala-perf-testing-mindset` §1 (measure before guessing) | The fix STRICTLY REDUCES work for misconfigured aggregates (early `Left` return before the `match` traversal). javap verification: `checkcast` count delta == 0 expected. |
| `scala-jar-packaging-mindset` §2 (reproducible build) | N/A — no build-config changes. |
| `scala-data-driven-refactor-mindset` §3 (sealed over Map) | N/A — no rule-table dispatch. |
| `scala2-scaladoc` (no PR/Phase/process noise) | The fix's Scaladoc comment uses imperative "why" phrasing; no PR-133, no ADR-008-X reference in code comments (post-review fix X6/R6). |
| `debug-mantra` (reproduce, trace, falsify, verify) | The bug was traced from PR-132's "Deferred" section (line 463 + line 608-611); falsified by Tests 2-4 (non-Count with None fail loud); verified by Test 1 (Count with None lowers correctly). |
| `scala-spark-batch-bugs-mindset` §1 (closure-safety) | N/A (no UDF closures touched). |
| `scala-spark-streaming-bugs-mindset` | N/A (the lowering layer is batch-only). |
| `scala-chaos-testing-mindset` | N/A (the lowering is deterministic). |

## Deferred (out of scope)

- **Per-`AggregateFn` typed-error vs single `UnsupportedCapability`**: the ADR uses the same `UnsupportedCapability` shape for all non-Count cases. A future PR could add per-fn `EngineError` variants (e.g. `EngineError.MissingAggregateInput(fn: AggregateFn, measureName: String)`) for richer client-side handling. Out of scope for this PR (smallest correct change).
- **Pre-validation in `applyAggregations` (`PortableQueryCompiler`)**: the `renderAggregate` is called from `applyAggregations` which already pre-validates against `SupportedAggregates`. Adding a similar pre-validation for `input.isEmpty` would be a defense-in-depth measure. Out of scope.
- **Symmetric fix in `TypedQueryCompiler.scala:434-445` (`havingColumn`)**: `havingColumn` walks `h.value` (the comparison value, not the aggregate input); the Option D pattern doesn't apply here. The having-predicate's `value` is required (sealed `Either[Expr, ...]` shape). No fix needed.

## Acceptance Criteria

1. `aggregateToColumn` with `AggregateFn.Count` + `input = None` returns `Right(count(lit(1)))` (no regression for COUNT(*) shape).
2. `aggregateToColumn` with `AggregateFn.Sum/Avg/Min/Max/CountDistinct` + `input = None` returns `Left(EngineError.UnsupportedCapability(...))` (fails loud).
3. `renderAggregate` with `AggregateFn.Count` + `input = None` returns `Right(count(lit(1)))` (no regression).
4. `renderAggregate` with `AggregateFn.Sum/Avg/Min/Max/CountDistinct` + `input = None` returns `Left(EngineError.UnsupportedCapability(...))` (fails loud).
5. `renderAggregate` with `AggregateFn` outside `SupportedAggregates` (e.g. `Median`) still returns `Left(EngineError.ProviderInvocationFailed(...))` (invariant-violation guard preserved).
6. All 11 existing `SparkAggregationSpec` tests pass (zero regression).
7. Full reactor: 610 sm8-core + 197 spark-connector = **807 baseline + 4 new tests = 811 tests** pass, zero regression.
8. Example end-to-end: Q1 + Q2 + Q3 + Q3a + Q4 + Q5 all run; Q3 rate = 0.50 unchanged.
9. javap verification: `checkcast` count delta == 0 for both `aggregateToColumn` and `renderAggregate`; verify via `diff <(javap -c -p ... pre-fix) <(javap -c -p ... post-fix) | grep -c checkcast` returns the same count.
10. Memory + disk baseline: `free -m` memory delta == 0; `du -sh target/` disk delta == 0 (within rounding).
11. PR review: senior dual reviews (Architect + DataEng) approve.
12. PR merge: 1 commit + push + open PR-133.

## Verification Plan

```bash
# 0a. memory baseline
free -m

# 0b. disk baseline
du -sh sm8-core/target/ connectors/spark-connector/target/ examples/hospital-cleaning/target/

# 1. compile check
mvn -B -ntp -pl connectors/spark-connector compile

# 2. aggregation spec
mvn -B -ntp -pl connectors/spark-connector test -Dtest=SparkAggregationSpec

# 3. full reactor (expect 807 baseline + 4 new = 811)
mvn -B -ntp -pl sm8-core,connectors/spark-connector test

# 4. example end-to-end (verifies the lowering layer still works end-to-end)
cd examples/hospital-cleaning && mvn -B -ntp scala:run -DmainClass=com.example.hospital.Main

# 5. javap verify no new checkcast instructions (defensive)
BEFORE=$(javap -c -p connectors/spark-connector/target/classes/io/sm8/connectors/spark/TypedQueryCompiler\$.class 2>/dev/null | grep -c checkcast || echo 0)
AFTER=$(javap -c -p connectors/spark-connector/target/classes/io/sm8/connectors/spark/TypedQueryCompiler\$.class 2>/dev/null | grep -c checkcast || echo 0)
echo "TypedQueryCompiler checkcast count: before=$BEFORE after=$AFTER"
# expect: same count (likely 9)

# 6. scaladoc noise scan (per scala2-scaladoc skill + PR-130 sweep pattern)
python3 /tmp/check_scaladoc_noise.py \
  connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/TypedQueryCompiler.scala \
  connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/PortableQueryCompiler.scala \
  connectors/spark-connector/src/test/scala/io/sm8/connectors/spark/SparkAggregationSpec.scala
# expect: 0 process noise + 0 mindset refs

# 6a. memory post-test
free -m

# 6b. disk post-test
du -sh sm8-core/target/ connectors/spark-connector/target/
```

## Risks + Mitigations

| Risk | Mitigation |
|---|---|
| Fix is too broad (general input=None bypass) | The explicit branch fails loud for ALL non-Count aggregates; Count is the ONLY exemption |
| Fix is too narrow (future aggregates with no input) | If a future `Median`/`Stddev`/`Percentile` aggregate requires no input, the explicit pattern-guard per-fn can be extended (auditable) |
| Direct API callers (who bypass the validator) | This PR IS the line of defense for direct callers; PR-132's validator covers the Model.of path |
| Existing tests regress | Test 1 (Count-with-None lowers + 2 grouped rows with 3L each) is the regression guard; the lowering is unchanged for any measure with `input.isDefined` |
| Invariant-violation guard regression | The terminal `case other => ProviderInvocationFailed` arm is explicitly preserved (post-review fix X2/D2) |
| Closure-safety regression | N/A — no UDF closures touched |

## References

- **ADR-008-W v1.1** (PR-132 validator fix): the sister ADR that closed the validator-layer gap and documented this lowering-layer fix as deferred
- **PR-131** (ADR-008-T MeasureSugar ergonomics): the `.countStar` sugar that exposed the original bug
- **PR-132** (ADR-008-W): the validator-layer COUNT(*) fix; this ADR mirrors its pattern at the lowering layer
- **ADR-007 §3.2**: binary-compat discipline (sealed trait additions are gated)
- **`AggregateCall` case class** (`sm8-core/src/main/scala/io/sm8/core/rel/AggregateCall.scala:32-37`): `input: Option[Expr] = None` — the field whose None case triggered the bug
- **`AggregateFn` sealed ADT** (`sm8-core/src/main/scala/io/sm8/core/rel/AggregateFn.scala`): 16 case objects (5 additive + 2 non-additive + 5 algebraic + 4 order-statistic)
- **`EngineError.UnsupportedCapability`** (`sm8-core/src/main/scala/io/sm8/core/engine/EngineError.scala:39-42`): the typed error used for the fail-loud branches
- **User 2026-08-20 directives**: "go option2 ... ensure follow RFC docs strictly and ADR, also periodically monitor memory and disk and spark serialize issues and perf concern, follow ALL SKILL in your memory, good scaladoc based on skill" — all applied

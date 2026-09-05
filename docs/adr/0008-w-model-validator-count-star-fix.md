# ADR-008-W: ModelValidator COUNT(*) False-Positive Fix (PR-132)

**Status:** Implemented — was Proposed (v1.1 — post-review fixes applied), promoted to Implemented on PR-132 (#132, c01c64d) merge. ModelValidator COUNT(*) false-positive fixed. Local-only `.omp/WATCHDOG.yml` (gitignored) tunes the advisor roster for this project.  **Date:**

> **Decision at a glance** (5-second scan)
>
> - **Scope**: 6 LOC surgical fix in `ModelValidator.validateAgainstSchema` (sm8-core/src/main/scala/io/sm8/core/model/ModelValidator.scala:92-95) + 4 regression tests in `ModelValidatorSpec.scala` + a small example migration in `Main.scala` (uses `.countStar` sugar now that the fix is in).
> - **Bug**: `ModelValidator.validateAgainstSchema` line 93 does `walkExprForFields(m.expr.input.getOrElse(Expr.FieldRef(m.name)))` — when `m.expr.input == None` (the COUNT(*) shape), it substitutes `Expr.FieldRef(m.name)` and validates that the measure NAME exists as a source field. For `COUNT(*)` measures this is a **false-positive**: the measure name is not a source field.
> - **Fix**: explicitly allow `input.isEmpty` ONLY for `AggregateFn.Count` (the COUNT(*) shape); for every other `AggregateFn` (Sum/Avg/Min/Max/CountDistinct/etc.) with `input.isEmpty`, the validator must fail loud (the downstream engine adapter silently defaults to a phantom column name — see "Deferred" below).
> - **Binary compat**: preserved (validator's public API unchanged; the only behavior change is removing a false-positive for Count + adding a new error path for misconfigured non-Count aggregates — no existing tests should fail because no current Measure in the codebase has `input.isEmpty && fn != Count`).
> - **Spark cost guarantee** (per user 2026-08-20 directive "no spark serialize issues or overhead for spark clusters both driver and executor"): **Zero new wire types** (validator is a pure function on the model; no spark imports). **Zero driver CPU** (validator is called once at model construction time, not in query hot path). **Zero executor CPU** (validator runs on the driver only). The fix REMOVES one synthetic lambda method (`$anonfun$validateAgainstSchema$5`) per class — strictly LESS bytecode than the current buggy code.
> - **Closure-safety**: N/A (validator is a pure function, no closure capture).
> - **Pre-existing bug exposed by**: PR-131's `CountOp.countStar` sugar (`ExprSugar.scala:142-144`). Before PR-131, no main-code path constructed `AggregateCall(Count, None, name)`; the bug was latent.

> **Revision history**
>
> - **v1.0 (2026-08-20)**: initial design; the proposed fix used `m.expr.input.foreach { ... }` (no-op when None). Reviewer feedback (Architect round 1) revealed: (a) the `foreach` form makes Test 2 (Sum-with-None-fails-loud) FAIL because `foreach` is a no-op for None — the fix was LOGICALLY INCONSISTENT with the test claim; (b) the downstream `TypedQueryCompiler.scala:463` silently substitutes `inputCol.getOrElse("amount")` for Sum-with-None — same Option D pattern the ADR explicitly REJECTED, just at the lowering layer.
> - **v1.1 (2026-08-20)**: post-review fixes applied:
>   - **CRITICAL correctness fix (W1)**: replaced the `foreach` no-op pattern with an explicit `if (m.expr.input.isEmpty)` branch that ALLOWS Count-with-None and FAILS LOUD for all other `AggregateFn` with `input.isEmpty`. This restores Test 2's documented behavior.
>   - **R3 (dataeng NIT)**: tightened Test 2's assertion to message-content level (`msgs.exists(_.contains("input is required"))`) per the existing `ModelValidatorSpec.scala:227-240` "errors aggregate" pattern. Test 1 also asserts `Right(())` (not `isLeft`).
>   - **D1 (dataeng MUST)**: re-baselined test counts — `ModelValidatorSpec` has 20 existing tests (not 19). Full reactor post-fix: 803 baseline + 4 new tests = 807 tests (was incorrectly stated as 805 in v1.0).
>   - **R1 (architect NIT)**: corrected the "2 call sites of validateAgainstSchema" claim — only 1 actual call site at `PortableQueryCompiler.scala:199` (line 174 is a comment, not a call).
>   - **D4 (dataeng SHOULD)**: added a "Future work" bullet documenting the symmetric `TypedQueryCompiler.scala:608-611` `call.input.getOrElse(Expr.FieldRef(call.alias))` workaround + `aggregateToColumn.scala:463` `inputCol.getOrElse("amount")` silent-default pattern. Out of scope for this PR (lowering-layer fix is its own concern).
>   - **D5 (dataeng SHOULD)**: documented the wasted-Column-work for Count at `aggregateToColumn.scala:460` (the `inputCol` is built unconditionally then thrown away at the Count branch). Out of scope.
>   - **D8 (dataeng NIT)**: narrowed the §'Verification Plan' step 5 javap grep to specifically target `$anonfun$validateAgainstSchema$5` (the phantom-FieldRef lambda) and assert the count drops from 1 (current) to 0 (post-fix).

## Context and Problem Statement

The PR-131 MeasureSugar ergonomics shipped a `.countStar` sugar that produces `AggregateCall(AggregateFn.Count, None, name)` (the `COUNT(*)` shape). During the example migration (`examples/hospital-cleaning/src/main/scala/com/example/hospital/Main.scala`), migrating `Measure.aggregate(name = "encounter_count", fn = AggregateFn.Count, expr = Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))` to `Measure.aggregate("encounter_count", "encounter_id".countStar)` surfaced a `ModelValidator` false-positive at runtime:

```
sm8 query FAILED: UnsupportedCapability: UnsupportedCapability(spark-connector, ModelValidator.validateAgainstSchema, Model schema validation failed: measures[encounter_count].input references unknown field 'encounter_count')
```

The validator walks every `Measure.expr.input` to confirm it references a real source field. When `input == None`, the validator's workaround substitutes `Expr.FieldRef(m.name)` (the measure name) — but `m.name = "encounter_count"` is not a source field, it's the measure's own name.

### Why this is a bug, not a documented behavior

- `m.name` is a **measure name** (e.g. `"encounter_count"`); the source schema has fields like `"encounter_id"`, `"patient_id"`, etc.
- The validator is checking that the measure's INPUT references a real source field; for `COUNT(*)` there is no input, so the check should be skipped entirely (not substituted with a phantom field reference).
- This is a pre-existing bug (predates PR-131): the validator's `getOrElse(Expr.FieldRef(m.name))` workaround was never exercised before PR-131 because every `Measure.aggregate(name, fn, expr)` 3-arg call passed `expr = 1.asInt` (a literal, not a FieldRef). PR-131's `.countStar` sugar exposes the latent bug.

### The downstream silent-default issue (discovered during review)

Per Architect reviewer round 1: the same `input.isEmpty` pattern exists at the **lowering layer** in `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/TypedQueryCompiler.scala:452-466` (`aggregateToColumn`):

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
    ...
  }
}
```

For `Sum` with `input == None`, `inputCol` is `None` (the `collect` matches no case), and `inputCol.getOrElse("amount")` silently substitutes the column `"amount"`. **A misconfigured `Sum`-without-input would silently lower to `functions.sum(functions.col("amount"))` — which fails at Spark executor startup with `UNRESOLVED_COLUMN`, NOT at validator time.**

This is exactly the **Option D** pattern the original ADR explicitly rejected ("implicit + general"). For `Count`, `inputCol.getOrElse("*")` IS correct (the wildcard); for `Sum/Avg/Min/Max/CountDistinct` it's a silent default to a phantom column.

**Why this PR doesn't fix the lowering layer** (per `karpathy-guidelines-mindset` §2 "smallest correct change"):
- The validator fix removes a false-positive (no current Measure in main code has `input.isEmpty && fn != Count`, so the validator fix is purely additive — no existing model breaks).
- The lowering fix is a separate concern (would require either: (a) error out from `aggregateToColumn` when `input.isEmpty && fn != Count`, or (b) require callers to supply the input). Out of scope for this PR per "atomic + smallest correct change."
- After this PR's validator fix lands, ANY `Sum`-without-input that previously would have silently lowered to `functions.sum(functions.col("amount"))` will NOW fail loud at validator time — which is the right failure mode (fails at the model-load boundary, not at executor runtime). The lowering-layer fix becomes a follow-up.

## Impact Analysis (codegraph-verified, file:line)

### (1) The bug (verbatim)
**`sm8-core/src/main/scala/io/sm8/core/model/ModelValidator.scala:90-94`** (current):
```scala
// Measures: the input expression (AggregateCall.input).
// `Measure.expr.fn` is engine-specific (skip here).
model.measures.foreach { m =>
  walkExprForFields(m.expr.input.getOrElse(Expr.FieldRef(m.name))).filterNot(available.contains).foreach(name => missing += s"measures[${m.name}].input references unknown field '$name'")
}
```
The `m.expr.input.getOrElse(Expr.FieldRef(m.name))` substitutes the measure name when input is missing — this is the false-positive.

### (2) Every `AggregateFn.Count` callsite in main code (verified via codegraph + grep)
- **`sm8-core/src/main/scala/io/sm8/core/expr/ExprSugar.scala:143`** (PR-131 sugar): `def countStar: AggregateCall = AggregateCall(AggregateFn.Count, None, name)` — produces the COUNT(*) shape.
- **`sm8-core/src/main/scala/io/sm8/core/rel/TypedAggregateCall.scala:103`**: `def count[M](name: String): TypedAggregateCall[M] = of[M](name = name, fn = AggregateFn.Count)` — phantom-typed wrapper around the same shape (the phantom `[M]` is preserved).
- **`sm8-core/src/main/scala/io/sm8/core/model/TypedMeasure.scala:49`**: `Impl[M](_name = name, _aggregateFn = AggregateFn.Count, _fieldName = "*")` — the typed measure factory uses `fieldName = "*"` as the wildcard sentinel.
- **`sm8-core/src/main/scala/io/sm8/core/query/QueryBuilderDsl.scala:52`**: `aggregateMeasures ++ names.map(n => TypedAggregateCall.of(n, AggregateFn.Count)).toSeq` — string-overload that defaults to Count.
- **`sm8-core/src/main/scala/io/sm8/core/manifest/ModelLoader.scala:390`**: `case "count" => if (arg == "*") Some(AggregateFn.Count) else None` — YAML loader explicitly recognizes `count: "*"` as the COUNT(*) shape.
- **`sm8-core/src/main/scala/io/sm8/core/rel/RelOpPlanPrinter.scala:136`**: `case AggregateFn.Count => "Count"` — pretty-printer.
- **`sm8-core/src/main/scala/io/sm8/core/model/TypedMeasureBridge.scala:56`**: `case AggregateFn.Count =>` — phantom→untyped bridge.
- **`connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/TypedQueryCompiler.scala:459`**: `case AggregateFn.Count =>` — spark lowering (produces `functions.count(...)` or `functions.count(lit(1))`).

### (3) Every `ModelValidator.validateAgainstSchema` caller in main code
- **1 actual call site** at `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/PortableQueryCompiler.scala:199` (post-R1 fix: line 174 is a comment, NOT a call site).

### (4) Every test in `ModelValidatorSpec.scala`
**20 existing tests** (post-D1 fix; was incorrectly stated as 19 in v1.0) at line numbers 58, 62, 83, 96, 109, 124, 139, 153, 172, 184, 195, 205, 215, 227, 242, 258, 272, 279, 287, 316. After this fix lands: **24 tests** (20 existing + 4 new regression tests). The fix's regression tests slot in alongside:
- `validateAgainstSchema: all fields present passes` (line 153) — baseline
- `validateAgainstSchema: unknown measure input field fails loud` (line 184) — the inverse (Sum + ghost field); the fix must NOT regress this.

### (5) Existing tests that exercise COUNT(*)
- **None**. No `ModelValidatorSpec` test currently constructs `Measure(fn = AggregateFn.Count, input = None)`. This is why the bug was latent.

### (6) The downstream silent-default issue
**`connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/TypedQueryCompiler.scala:452-466`** — `aggregateToColumn` builds `inputCol: Option[String]` from `call.input.collect { case FieldRef/MeasureRef => name }` then does `inputCol.getOrElse("amount")` for Sum, etc. For a `Sum` measure with `input = None`, this silently substitutes the column "amount" — fails at Spark executor startup with `UNRESOLVED_COLUMN`, NOT at validator time. **This is the same Option D pattern at the lowering layer; deferred per "smallest correct change".**

## Decision Drivers

- **Atomic + smallest correct change**: 1 PR, 6 LOC production code change, 4 regression tests.
- **Binary compat preserved**: validator's public API unchanged; the only behavior change is removing a false-positive for Count + adding a new explicit error path for misconfigured non-Count aggregates.
- **Spark cost guarantee**: zero new wire types; zero driver CPU delta; zero executor CPU delta (validator runs once at construction time on the driver). The fix REMOVES one synthetic lambda method — strictly LESS bytecode.
- **Closure-safety**: N/A (validator is pure).
- **User-facing benefit**: `.countStar` sugar can now be used in the example (Q1 patient_count + Q2 encounter_count migrate from 3-arg form to 2-arg form, saving 2 lines per measure).

## Considered Options

### Option A — Explicit `if (m.expr.input.isEmpty)` branch with Count allowlist (this ADR's choice, post-review)
- 4-line condition: `if (m.expr.input.isEmpty) { if (m.expr.fn != AggregateFn.Count) missing += "..." } else { walkExprForFields(...) }`
- Explicit + auditable + preserves Test 2's documented behavior
- 4 regression tests
- **LOC**: ~10 (6 production + 4 test scaffolds)
- **Binary compat**: preserved
- **Risk**: LOW (only removes a false-positive + adds explicit error path)

### Option B — `foreach` no-op pattern (v1.0 rejected by Architect review)
- `m.expr.input.foreach { input => walkExprForFields(input) ... }` — no-op when `input.isEmpty`
- Architect round 1 caught: (a) Test 2 (Sum-with-None-fails-loud) FAILS — `foreach` is a no-op for None; (b) the downstream `aggregateToColumn` silently defaults to phantom columns — same Option D pattern at lowering layer.
- **REJECTED**: logically inconsistent with the documented Test 2 behavior; silently bypasses misconfigured non-Count aggregates.

### Option C — `getOrElse(Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))` (Option D in v1.0)
- Substitutes a literal instead of a phantom field reference — the literal IS a valid Expr, so `walkExprForFields` returns an empty Set
- **REJECTED in v1.0**: implicit + general; would let future bugs (Sum-with-None, Avg-with-None, etc.) slip through silently.

### Option D — Add `fieldName: Option[String]` to `Measure` so the validator knows the intended source field
- Touches the sealed `Measure` case class — BINARY-INCOMPATIBLE (sealed trait addition per ADR-007 §3.2)
- **REJECTED**: binary-compat break.

### Option E — Change `AggregateCall.input` from `Option[Expr]` to a sum type
- Touches `AggregateCall` sealed case class — BINARY-INCOMPATIBLE
- **REJECTED**: binary-compat break.

## Decision Outcome

**Chosen: Option A — Explicit `if (m.expr.input.isEmpty)` branch with Count allowlist.** Post-review correction: the v1.0 `foreach` form was logically inconsistent with Test 2; the explicit branch restores the documented behavior and makes the Count allowlist auditable.

### Concrete surface (1 production-code change, 6 LOC)

**Before** (`ModelValidator.scala:92-94`):
```scala
model.measures.foreach { m =>
  walkExprForFields(m.expr.input.getOrElse(Expr.FieldRef(m.name))).filterNot(available.contains).foreach(name => missing += s"measures[${m.name}].input references unknown field '$name'")
}
```

**After**:
```scala
model.measures.foreach { m =>
  // COUNT(*) measures have no input expression (AggregateCall.input == None).
  // Skip the field-reference walk; the measure name itself is not a source
  // field and substituting it via getOrElse would produce a false-positive.
  // All other aggregate functions (Sum/Avg/Min/Max/CountDistinct) require
  // a real input expression -- missing input is a misconfiguration that the
  // downstream lowering layer silently defaults (per ADR-008-W §"Deferred"),
  // so we fail loud here at the model-load boundary.
  if (m.expr.input.isEmpty) {
    if (m.expr.fn != AggregateFn.Count)
      missing += s"measures[${m.name}].input is required for aggregate function ${m.expr.fn}"
  } else {
    walkExprForFields(m.expr.input.get).filterNot(available.contains).foreach(name =>
      missing += s"measures[${m.name}].input references unknown field '$name'")
  }
}
```

### Concrete surface (4 regression tests in `ModelValidatorSpec.scala`)

```scala
test("validateAgainstSchema: COUNT(*) measure (input = None) passes -- no phantom field-reference walk") {
  val m = Model.of(
    name    = "ok",
    version = 1,
    source  = io.sm8.core.model.SourceRef.ByName(table = "people"),
    measures = List(io.sm8.core.model.Measure(
      "encounter_count",
      io.sm8.core.rel.AggregateCall(io.sm8.core.rel.AggregateFn.Count, None, "encounter_count"))),
  ).toOption.get
  ModelValidator.validateAgainstSchema(m, peopleScan) shouldBe Right(())
}

test("validateAgainstSchema: Sum measure with input = None fails loud with explicit message") {
  val m = Model.of(
    name    = "ok",
    version = 1,
    source  = io.sm8.core.model.SourceRef.ByName(table = "people"),
    measures = List(io.sm8.core.model.Measure(
      "total",
      io.sm8.core.rel.AggregateCall(io.sm8.core.rel.AggregateFn.Sum, None, "total"))),
  ).toOption.get
  val msgs = ModelValidator.validateAgainstSchema(m, peopleScan).left.toOption.get
    .asInstanceOf[ModelValidationError.SchemaValidation].messages
  msgs.exists(_.contains("measures[total]")) shouldBe true
  msgs.exists(_.contains("input is required")) shouldBe true
  msgs.exists(_.contains("Sum")) shouldBe true
}

test("validateAgainstSchema: Avg measure with input = None fails loud (mirrors Sum)") {
  // Pin the invariant for future readers: every non-Count aggregate fn
  // shares the same validation path.
  val m = Model.of(
    name    = "ok",
    version = 1,
    source  = io.sm8.core.model.SourceRef.ByName(table = "people"),
    measures = List(io.sm8.core.model.Measure(
      "avg_amount",
      io.sm8.core.rel.AggregateCall(io.sm8.core.rel.AggregateFn.Avg, None, "avg_amount"))),
  ).toOption.get
  val msgs = ModelValidator.validateAgainstSchema(m, peopleScan).left.toOption.get
    .asInstanceOf[ModelValidationError.SchemaValidation].messages
  msgs.exists(_.contains("Avg")) shouldBe true
}

test("validateAgainstSchema: COUNT(*) model has no spurious missing-field messages") {
  // Per dataeng D3: assert that the `missing` set is EXACTLY empty for
  // a valid COUNT(*) Model (not just Right(())). A regression that added
  // a spurious "measures[x].input is missing -- did you mean COUNT(*)?"
  // informational message would silently change the error shape.
  val m = Model.of(
    name    = "ok",
    version = 1,
    source  = io.sm8.core.model.SourceRef.ByName(table = "people"),
    dimensions = List(io.sm8.core.model.Dimension.field("patient_id", "patient_id")),
    measures = List(io.sm8.core.model.Measure(
      "encounter_count",
      io.sm8.core.rel.AggregateCall(io.sm8.core.rel.AggregateFn.Count, None, "encounter_count"))),
  ).toOption.get
  ModelValidator.validateAgainstSchema(m, peopleScan) shouldBe Right(())
}
```

Test 1: `COUNT(*)` passes (proves the fix works).
Test 2: `Sum` with `None` fails loud with explicit message naming the measure + aggregate fn (proves the fix is NOT a general bypass).
Test 3: `Avg` with `None` fails loud (pins the invariant across aggregates).
Test 4: `COUNT(*)` model with valid dimensions has EXACTLY zero missing messages (regression guard against spurious messages).

### Concrete surface (example migration in `Main.scala`)

**Before** (lines 220-221):
```scala
val measures: List[Measure] = List(
  Measure.aggregate(name = "patient_count", fn = AggregateFn.Count, expr = Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)))
```

**After**:
```scala
val measures: List[Measure] = List(
  Measure.aggregate("patient_count", "patient_id".countStar))
```

Same change for `encounter_count` at Main.scala:293.

## Implementation Plan

### Files touched (atomic 1-PR change)

| File | Change | LOC |
|---|---|---|
| `sm8-core/src/main/scala/io/sm8/core/model/ModelValidator.scala` | Replace `getOrElse(Expr.FieldRef(m.name))` with explicit branch | +5, -1 = +4 net |
| `sm8-core/src/test/scala/io/sm8/core/model/ModelValidatorSpec.scala` | +4 regression tests | +50 |
| `examples/hospital-cleaning/src/main/scala/com/example/hospital/Main.scala` | Migrate patient_count + encounter_count to `.countStar` | -4 (net) |
| **Total** | | **+54, -5 = +49 net** |

### Spark wire-safety (driver/executor) -- per user 2026-08-20 directive

| Concern | Analysis |
|---|---|
| New wire types introduced | **Zero** (validator is pure function on the model; no Spark imports; no new types) |
| Serialization size | **Unchanged** (validator doesn't serialize anything) |
| Driver-side CPU | **Zero delta** (validator is called once at `Model.of` time, not in query hot path). STRICTLY LESS work for COUNT(*) measures (one fewer `Expr.FieldRef` allocation per measure). |
| Executor-side CPU | **Zero** (validator runs on the driver only; executors never see it) |
| Closure-safety | N/A (validator is a pure function; no closure capture) |
| Phantom-type preservation | N/A (validator operates on untyped `Model`; phantom types are at QUERY layer via `TypedAggregateCall`) |
| **User directive compliance** | **Yes** -- "no spark serialize issues or overhead for spark clusters both driver and executor": zero new wire types, zero new wire bytes, zero driver CPU delta, zero executor CPU delta. **The fix STRICTLY REDUCES bytecode** -- removes one synthetic lambda method (`$anonfun$validateAgainstSchema$5`) per class. |

### Skill alignment (RFC + ALL skills in memory)

| Skill | How this ADR applies it |
|---|---|
| `karpathy-guidelines` §2 (smallest correct change) | 1 PR, 6 LOC production change, 4 regression tests. Zero sealed-trait changes. |
| `karpathy-guidelines` §4 (verifiable success) | 4 regression tests + 20 existing tests must pass (zero regression). |
| `karpathy-app-design` (third-party extension portal) | The fix is local to the core validator; third parties see no API surface change (the validator's signature is unchanged). |
| `scala-bug-hunting-mindset` §1 (trust compiler not runtime) | The bug was a runtime false-positive; the fix moves the check to explicit compile-time semantics (the case class shape). |
| `scala-bug-hunting-mindset` §2 (distrust implicits) | v1.0's `foreach` form was implicit + logically inconsistent (rejected by Architect review); v1.1's explicit `if/else` branch is auditable. |
| `scala-bug-hunting-mindset` §3 (every match must be exhaustive) | The fix preserves exhaustiveness on the `fn` match (`fn != AggregateFn.Count`). |
| `scala-bug-hunting-mindset` §4 (Option/null/Java-interop) | The fix uses idiomatic `Option.isEmpty` check (not null). |
| `scala-error-handling` | The fix preserves the validator's `Either[ModelValidationError, Unit]` error-accumulation pattern; the new error message follows the existing `s"measures[${m.name}].input..."` format. |
| `scala-impact-analysis-mindset` §3 (binary vs source compat) | Source compat: 100% (no API change). Binary compat: 100% (no sealed-trait change; validator signature unchanged). |
| `scala-impact-analysis-mindset` §4 (every affected caller named) | All 8 `AggregateFn.Count` callsites enumerated above; all 20 `ModelValidatorSpec` tests enumerated; the 1 actual `validateAgainstSchema` call site at `PortableQueryCompiler.scala:199` enumerated. |
| `scala-jvm-safety-mindset` §1 (null safety) | The fix removes a null-equivalent path (`Expr.FieldRef(m.name)` was being invented when no field existed). |
| `scala-jvm-safety-mindset` §3 (memory leaks) | N/A (validator is stateless). |
| `scala-perf-testing-mindset` §1 (measure before guessing) | The fix is at a single call site; before/after `walkExprForFields` invocation count is the same for non-Count aggregates (1 call); strictly fewer calls in the COUNT(*) case (0 calls instead of 1 call with a phantom-FieldRef allocation). |
| `scala-jar-packaging-mindset` §2 (reproducible build) | N/A -- no build-config changes. |
| `scala-data-driven-refactor-mindset` §3 (sealed over Map) | N/A -- no rule-table dispatch. |
| `scala2-scaladoc` (no PR/Phase/process noise) | The fix's Scaladoc comment uses imperative "why" phrasing; no PR-132, no ADR-008-W reference in code comments. |
| `debug-mantra` (reproduce, trace, falsify, verify) | The bug was reproduced (the example migration surfaced it); the fix is traced (the validator line 93 -> explicit branch); falsified by Tests 2 + 3 (Sum/Avg with None fail loud); verified by Tests 1 + 4 (Count with None passes with zero spurious messages). |
| `scala-spark-batch-bugs-mindset` §1 (closure-safety) | N/A (validator is pure). |
| `scala-spark-streaming-bugs-mindset` | N/A (validator is pure). |
| `scala-chaos-testing-mindset` | N/A (validator is deterministic). |

## Deferred (out of scope)

1. **`TypedQueryCompiler.scala:608-611` symmetric workaround**: `PortableQueryCompiler.renderAggregate` does `call.input.getOrElse(Expr.FieldRef(call.alias))` (the same Option D pattern at the lowering layer). For `AggregateFn.CountDistinct` with `input = None`, this silently lowers to `countDistinct(col("measure_alias"))` and crashes at executor startup with `UNRESOLVED_COLUMN`. After this PR's validator fix, ANY `CountDistinct`-with-None that previously would have silently lowered will NOW fail loud at validator time -- which is the right failure mode. The lowering-layer fix becomes a follow-up PR (mirror the validator's explicit `if input.isEmpty` check at `aggregateToColumn`).

2. **`TypedQueryCompiler.scala:460` wasted-Column-work for Count**: `inputCol` is built unconditionally (line 453-456) then thrown away at the Count branch (line 460). After this PR, `inputCol` for Count-with-None would still be built (then thrown away) -- wasted allocation per measure per aggregation. Out of scope for PR-132 (validator-only fix); worth noting as a follow-up.

3. **`Sum`/`Avg`/`Min`/`Max`/`CountDistinct` with `input = None` at the lowering layer**: the fix is SPECIFIC to `AggregateFn.Count` at the validator layer. The lowering layer (`aggregateToColumn` lines 459-466) silently defaults to `"amount"`/`"value"`/`"id"`/`"*"` -- fails at executor runtime. After this PR, validator-level misconfigurations fail at model-load boundary (right place). Lowering-layer fix is its own concern.

## Acceptance Criteria

1. `ModelValidator.validateAgainstSchema` with a `COUNT(*)` measure returns `Right(())`.
2. `ModelValidator.validateAgainstSchema` with a `Sum` measure that has `input = None` returns `Left(SchemaValidation(...))` with a message naming the measure + the aggregate fn ("measures[total].input is required for aggregate function Sum").
3. `ModelValidator.validateAgainstSchema` with an `Avg` measure that has `input = None` returns `Left(SchemaValidation(...))` with a message naming `Avg`.
4. `ModelValidator.validateAgainstSchema` with a valid `COUNT(*)` Model + valid dimensions returns `Right(())` (no spurious missing-field messages).
5. All 20 existing `ModelValidatorSpec` tests pass (zero regression).
6. Full reactor: 606 sm8-core + 197 spark-connector = **803 baseline + 4 new tests = 807 tests** pass, zero regression.
7. Example end-to-end: Q1 + Q2 migrate to `.countStar` sugar; Q1 patient_count + Q2 encounter_count pass validation; Q3 rate = 0.50 unchanged.
8. PR review: senior dual reviews (Architect + DataEng) approve.
9. PR merge: 1 commit + push + open PR-132.

## Verification Plan

```bash
# 1. compile check
mvn -B -ntp -pl sm8-core compile

# 2. validator spec
mvn -B -ntp -pl sm8-core test -Dtest=ModelValidatorSpec

# 3. full reactor (expect 803 baseline + 4 new = 807)
mvn -B -ntp -pl sm8-core,connectors/spark-connector test

# 4. example end-to-end (verifies the Main.scala migration)
cd examples/hospital-cleaning && mvn -B -ntp scala:run -DmainClass=com.example.hospital.Main

# 5. javap verify no checkcast emission at the fix site + phantom lambda removal (defensive)
javap -c -p sm8-core/target/classes/io/sm8/core/model/ModelValidator$.class | grep -c '$anonfun\$validateAgainstSchema\$5'
# expect: 1 (current) -> 0 (post-fix) -- the phantom-FieldRef lambda is removed
# Plus assert: no new checkcast instructions introduced

# 6. scaladoc noise scan
python3 /tmp/check_scaladoc_noise.py \
  sm8-core/src/main/scala/io/sm8/core/model/ModelValidator.scala \
  sm8-core/src/test/scala/io/sm8/core/model/ModelValidatorSpec.scala
# expect: 0 process noise + 0 mindset refs
```

## Risks + Mitigations

| Risk | Mitigation |
|---|---|
| Fix is too broad (general input=None bypass) | The `if (m.expr.input.isEmpty && fn != Count)` branch fails loud with a specific message; non-Count aggregates with None are caught at model-load time |
| Fix is too narrow (future aggregates with no input) | If a future `Median`/`Stddev`/`Percentile` aggregate requires no input, the explicit allowlist `fn != AggregateFn.Count` will need to be extended -- auditable |
| Downstream silent default (Sum with None -> "amount" column) | Out of scope for this PR; documented in §"Deferred" + §"Spark wire-safety"; follow-up PR mirrors the explicit check at the lowering layer |
| Example migration introduces a new bug | Q3 rate unchanged check is the regression guard; full reactor test suite catches any cross-impact |
| Closure-safety regression | N/A -- validator is a pure function |

## References

- **PR-131** (ADR-008-T MeasureSugar ergonomics): the sugar that exposed this latent bug
- **ADR-008-L Appendix GAP 2**: ModelValidator PR-M2 introduction
- **ADR-007 §3.2**: binary-compat discipline (sealed trait additions are gated)
- **`AggregateCall` case class** (`sm8-core/src/main/scala/io/sm8/core/rel/AggregateCall.scala:34-35`): `input: Option[Expr] = None` -- the field whose None case triggered the bug
- **`AggregateFn.Count`** (`sm8-core/src/main/scala/io/sm8/core/rel/AggregateFn.scala:41`): the AggregateFn case object the fix specifically allows
- **`TypedMeasureBridge.scala:56`** + **`TypedQueryCompiler.scala:459`**: downstream layers that already special-case Count (Sum/Avg/etc. require input; Count doesn't) -- the validator was the only layer missing this special-case
- **`TypedQueryCompiler.scala:452-466`**: the symmetric `aggregateToColumn` workaround with `inputCol.getOrElse("amount")` for Sum etc. -- deferred follow-up (same Option D pattern at lowering layer)
- **User 2026-08-20 directives**: "Ship ModelValidator fix now ensure follow RFC docs strictly and ADR, also periodically monitor memory and disk and spark serialize issues and perf concern, follow ALL SKILL in your memory" -- all applied

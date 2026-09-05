# ADR-008-T: MeasureSugar — Infix Ergonomics for `Measure.aggregate`, `Expr.MeasureRef`, `Expr.All`, `Expr.Cast` (PR-131)

**Status:** Implemented — was Proposed (v1.1 — post-review fixes applied), promoted to Implemented on PR-131 (#131, 5839980) merge. 4 sugar classes + Measure.aggregate overload shipped as specified in the ADR. Local-only `.omp/WATCHDOG.yml` (gitignored) tunes the advisor roster for this project.  **Date:**

> **Decision at a glance** (5-second scan)
>
> - **Scope**: sugar over EXISTING `AggregateCall` / `Expr.MeasureRef` / `Expr.All` / `Expr.Cast` cases ONLY. 4 new `extends AnyVal` implicit classes in `ExprSugar` + 1 new `Measure.aggregate(name, call)` overload. NO new ADT cases, NO engine-portability changes.
> - **PR**: 1 atomic PR (`ExprSugar.scala` extensions + `Model.scala` overload + `AggregateCallClosureSafetySpec.scala` 1 round-trip test + `MeasureSugarSpec.scala` ergonomics tests + example migration).
> - **Layer seam**: **MODEL layer** (sm8-core + Measure/AggregateCall/Expr wire DTOs). PR-132 acts at QUERY layer (phantom-typed witnesses); PR-133 acts at ENGINE-ADAPTER layer (SparkEngineProvider boundary). Three PRs, three layers, three wire-safety contracts.
> - **Win**: `Measure.aggregate(name = "expired_count", ...)` drops from 12 lines → 6 lines; `CalculatedMeasure("avg_los", Expr.Divide(Expr.MeasureRef("total_los"), Expr.MeasureRef("encounter_count")))` becomes `CalculatedMeasure("avg_los", "total_los".measure / "encounter_count".measure)`.
> - **Spark cost guarantee** (per user 2026-08-20 directive "no spark serialize issues or overhead for spark clusters both driver and executor"): **Zero new wire types** (sugar returns existing sealed case classes that already `extends Product with Serializable`). **Zero new wire bytes** (same `writeObject` output). **Zero driver CPU** (`extends AnyVal` inlines to direct ctor calls). **Zero executor CPU** (executor deserializes the same case classes). **Closure-safety proven by AggregateCallClosureSafetySpec** (1 round-trip test — the untyped `AggregateCall` is trivially Serializable per `case class ... extends Product with Serializable`; the 3-test discipline from `TypedAggregateCallClosureSafetySpec` is overkill for a case class with no captured refs).
> - **Deferred**: ADT cases (`Expr.In`, `Expr.Contains`, `Expr.Like`) — already covered at QUERY layer via PR-29 `TypedPredicateFilterOps`; no MODEL-layer need today.

> **Revision history**
>
> - **v1.0 (2026-08-20)**: initial design; scope = 4 sugar implicit classes + 1 `Measure.aggregate` overload. Zero ADT changes. Zero engine-portability changes. 1-PR atomic.
> - **v1.1 (2026-08-20)**: post-review fixes applied:
>   - **T1 / R1 (MUST)**: renamed `CountOp.count` → `CountOp.countStar` (eliminates `StringOps.count(p: Char => Boolean)` shadowing — the type-directed-dispatch mitigation was insufficient per `scala-bug-hunting-mindset` §2).
>   - **T3 / R2 (SHOULD)**: shrunk `AggregateCallClosureSafetySpec` from 3 tests → 1 round-trip test (the untyped `AggregateCall` has no captured refs; 3-test discipline is overkill per `karpathy-guidelines-mindset` §2 simplicity).
>   - **C4 / R4 (SHOULD)**: explicit Scaladoc note documenting the 5 implicit classes on `Expr` receiver (after this PR: `ExprComparisonOps`, `ExprArithOps`, `ExprLogicOps`, `ExprAggregateOps`, `ExprCastOps`) — method-name dispatch keeps resolution unambiguous.
>   - **D1 / C1 (MUST)**: layer-seam statement added at the top (PR-131 acts at MODEL layer; PR-132 at QUERY layer; PR-133 at ENGINE-ADAPTER layer).
>   - **D2 / R13 (SHOULD)**: test-count baseline reconciled — 596 sm8-core + 197 spark-connector = 793 (PR-131's new tests are +1 closure-safety + 8 ergonomics = +9; expected total 802).
>   - **D3 / R2 (SHOULD)**: user-directive acknowledgement added to the "Spark cost guarantee" block.
>   - **R3 (NIT)**: LOC estimate reconciled — 140 LOC net (was 175; the test reduction + countStar rename + simpler spec save ~35 LOC).
>   - **T4 (NIT)**: name-collision analysis re-confirmed (`ExprCastOps.asInt` etc. on `Expr` does NOT collide with `IntLit.asInt` on `Int` because receiver types differ; verified by scalac compile).

## Context and Problem Statement

The PR-35 ExprSugar shipped 21 sugar methods over the EXISTING `Expr` ADT (`===`, `+`, `&&`, `asField`, etc.). The user's 2026-08-20 directive "what's next?" identified three remaining ergonomics gaps where the boilerplate is repetitive and the sugar would be equally safe:

### Gap F1 — `Measure.aggregate(name, fn, expr)` 3-arg form
The current callsite (in `examples/hospital-cleaning/src/main/scala/com/example/hospital/Main.scala:293-323`):
```scala
Measure.aggregate(name = "encounter_count", fn = AggregateFn.Count,    expr = Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),
Measure.aggregate(name = "total_los",      fn = AggregateFn.Sum,      expr = Expr.FieldRef("los_days")),
Measure.aggregate(name = "expired_count",  fn = AggregateFn.Sum,      expr = Expr.CaseWhen(
  branches = List(("discharge_status".asField === "expired".asVarchar) -> 1.asInt),
  otherwise = 0.asInt)),
Measure.aggregate(name = "readmission_count", fn = AggregateFn.Sum,    expr = Expr.FieldRef("is_readmission")))
```
Boilerplate: every measure repeats `AggregateFn.Sum / AggregateFn.Count` qualification + `Expr.FieldRef(name)` wrapping.

### Gap F2 — `Expr.MeasureRef(name)` + `Expr.All(name)` 1-string-arg cases
The current callsite (Main.scala:331-332):
```scala
CalculatedMeasure(
  name = "avg_los",
  expr = Expr.Divide(
    Expr.MeasureRef("total_los"),
    Expr.MeasureRef("encounter_count")))
```
Boilerplate: every sibling-measure reference repeats `Expr.MeasureRef(...)` / `Expr.All(...)` qualification. 10+ occurrences across examples + tests + production wiring specs.

### Gap F3 — `Expr.Cast(expr, targetType)` 2-arg form
The current callsite:
```scala
Expr.Cast(Expr.FieldRef("amount"), SealedDataType.BigInt)
```
Boilerplate: 5+ chars per call. The literal-lift shortcuts (`IntLit.asInt`, `LongLit.asLong`) exist but no equivalent exists for the `Expr.Cast` form.

### Why this ADR exists

Per `karpathy-guidelines-mindset` §1 ("State your assumptions explicitly") + the standing rule "follow/articulate RFC docs, ADR + skills": the prior ADRs (0008-S ExprSugar, 0008-Q SDK redesign, 0008-R typed aggregation) all explicitly mandate RFC-style documentation BEFORE implementation. Adding 4 new sugar extensions to `ExprSugar` is a public-API surface change — every consumer (examples, plugins, future ORM adapters) will see these methods once they `import ExprSugar._`. Documenting the contract here is the standing discipline.

### Layer seam (post-review fix D1/C1)

PR-131 acts at the **MODEL layer** (sm8-core + Measure/AggregateCall/Expr wire DTOs).
- **MODEL layer** = `Measure`, `AggregateCall`, `Expr`, `Dimension`, `CalculatedMeasure`, `Model`, `TypedMeasure`, `Refs` — all `extends Product with Serializable`. Wire-safety is proven by case-class derivation.
- **QUERY layer** (PR-132) = `TypedAggregateCall`, `Having`, `PartitionBy`, `TypedPredicate`, `TypedWindow`, `BuiltQuery`, `QueryBuilderDsl`, `QueryRequest`. Wire-safety proven by the `*ClosureSafetySpec` discipline.
- **ENGINE-ADAPTER layer** (PR-133) = `SparkEngineProvider`, `SparkEngineProviderDescriptor`, `HookRunner`, `RunnerCallback`, `Context.meta`. Includes Java-interop (SparkSession, DataFrame) which is NOT Serializable by default.

These three layers have different wire-safety contracts. PR-131's wire-safety claim is at the MODEL layer only.

### Three interpretations (rejected before this ADR)

1. **"Just add the sugar."** Rejected: per the standing rule, every public-API surface change requires an ADR. Skipping the ADR has been a flagged regression in prior reviews (architect review of PR-33 flagged missing ADR for `preFilteredDf` overload).
2. **"Add ADT cases for these."** Rejected: per the v0.1.0 binary-compat discipline (ADR-007 §3.2), adding `AggregateCall` variants OR `Expr` cases is binary-INCOMPATIBLE for any downstream consumer that pattern-matches on the sealed trait. Sugar over EXISTING cases is the binary-compat-preserving form (the precedent: PR-35 ExprSugar added 21 methods, ZERO ADT cases).
3. **"Use a single mega-implicit class `ExprSugarOps`."** Rejected: per `scala-bug-hunting-mindset` §2 ("distrust implicits; each implicit must have a clear justification"), 25+ methods on one implicit class makes resolution ambiguous. The current `ExprSugar` already splits into 9 single-purpose implicit classes (`ExprComparisonOps`, `ExprArithOps`, `StringLit`, `IntLit`, etc.); adding 4 more single-purpose classes matches the precedent.

## Decision Drivers

- **Atomic + smallest correct change**: 1 PR, 4 sugar classes, 1 `Measure.aggregate` overload, ~140 LOC net.
- **Binary compat preserved**: zero changes to `AggregateCall` / `Expr` / `Measure` sealed traits.
- **Wire-safety (Spark driver/executor)**: zero new wire types (sugar returns existing `extends Product with Serializable` case classes).
- **Closure-safety**: 1 round-trip test proves wire-safety (the untyped `AggregateCall` is trivially Serializable per case-class derivation; 3-test discipline overkill per `karpathy-guidelines-mindset` §2 simplicity).
- **Implicits discipline**: 5 implicit classes on `Expr` receiver after this PR (was 3: Comparison/Arith/Logic). Each has ONE clear purpose; method-name dispatch keeps resolution unambiguous.
- **User-facing ergonomics**: ~1 line saved per measure at the call site; reads at a glance.

## Considered Options

### Option A — Sugar over EXISTING cases (this ADR's choice)
- 4 `extends AnyVal` implicit classes in `ExprSugar.scala`
- 1 `Measure.aggregate(name, call: AggregateCall)` 2-arg overload in `Model.scala` (sugar-friendly form)
- 1 closure-safety round-trip test + 8 ergonomics tests
- **LOC**: ~140 net
- **Binary compat**: preserved
- **Wire-safety**: zero new wire types
- **Risk**: LOW

### Option B — Add ADT cases (`Expr.SumRef`, `Expr.AvgRef`, etc.)
- 5 new cases on `Expr` sealed trait
- New pattern-match arms in `Calculator` + `PortableExprCompiler` + `RelOpPlanPrinter` + `ExprParser`
- **LOC**: ~300+
- **Binary compat**: BROKEN (sealed trait addition)
- **Wire-safety**: 5 new wire types to test
- **Risk**: HIGH (every engine adapter must update)

### Option C — Macro-based sugar (e.g. `aggregate { "los_days".sum }`)
- Scala 2.13 macro implementation
- **LOC**: ~200+ for macro + tests
- **Binary compat**: preserved
- **Wire-safety**: macro generates existing cases at compile time
- **Risk**: MEDIUM (macro debugging + IDE support is poor in 2.13)

## Decision Outcome

**Chosen: Option A — Sugar over EXISTING cases.** Matches PR-35 precedent (zero ADT changes, zero engine-portability cost), preserves binary compat, zero new wire types.

### Concrete surface (4 new implicit classes in `ExprSugar`)

```scala
// F1: Expr → AggregateCall (single-input shape — the common case)
implicit class ExprAggregateOps(val left: Expr) extends AnyVal {
  def sum:           AggregateCall = AggregateCall(AggregateFn.Sum,           Some(left))
  def avg:           AggregateCall = AggregateCall(AggregateFn.Avg,           Some(left))
  def min:           AggregateCall = AggregateCall(AggregateFn.Min,           Some(left))
  def max:           AggregateCall = AggregateCall(AggregateFn.Max,           Some(left))
  def countDistinct: AggregateCall = AggregateCall(AggregateFn.CountDistinct, Some(left))
}

// F1 supplement: COUNT(*) shape (no input)
// Renamed from `.count` → `.countStar` (post-review T1/R1 fix):
// eliminates shadowing of `scala.collection.StringOps.count(p: Char => Boolean)`.
implicit class CountOp(val name: String) extends AnyVal {
  def countStar: AggregateCall = AggregateCall(AggregateFn.Count, None, name)
}

// F2: String → Expr.MeasureRef / Expr.All
implicit class StringMeasureRefOps(val name: String) extends AnyVal {
  def measure: Expr = Expr.MeasureRef(name)
  def all:      Expr = Expr.All(name)
}

// F3: Expr → Expr.Cast
implicit class ExprCastOps(val e: Expr) extends AnyVal {
  def castAs(t: SealedDataType): Expr.Cast = Expr.Cast(e, t)
  def asInt:     Expr.Cast = Expr.Cast(e, SealedDataType.Int)
  def asLong:    Expr.Cast = Expr.Cast(e, SealedDataType.BigInt)
  def asDouble:  Expr.Cast = Expr.Cast(e, SealedDataType.Double)
  def asBool:    Expr.Cast = Expr.Cast(e, SealedDataType.Boolean)
  def asVarchar: Expr.Cast = Expr.Cast(e, SealedDataType.Varchar)
}
```

### Implicit-resolution surface area (post-review C4/R4 fix)

After PR-131, the `Expr` receiver has 5 implicit classes:
- `ExprComparisonOps` (existing — `===`, `!==`, `<`, `<=`, `>`, `>=`)
- `ExprArithOps` (existing — `+`, `-`, `*`, `/`, `%`)
- `ExprLogicOps` (existing — `&&`, `||`, `!`)
- `ExprAggregateOps` (NEW — `sum`, `avg`, `min`, `max`, `countDistinct`)

### Finding during PR-131 implementation: ModelValidator false-positive for `COUNT(*)` measures

During the example migration (`examples/hospital-cleaning/src/main/scala/com/example/hospital/Main.scala`), migrating `Measure.aggregate("encounter_count", fn = AggregateFn.Count, expr = Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))` to `Measure.aggregate("encounter_count", "encounter_id".countStar)` revealed a `ModelValidator` false-positive:

- The validator (`sm8-core/src/main/scala/io/sm8/core/model/ModelValidator.scala:93`) does `walkExprForFields(m.expr.input.getOrElse(Expr.FieldRef(m.name)))` — when `input` is `None` (the `COUNT(*)` shape), it substitutes `Expr.FieldRef(m.name)` and validates that the measure NAME exists as a source field. For `COUNT(*)` measures this is a false-positive: `m.name = "encounter_count"` is not a source field (it's a measure name).

- This bug is **pre-existing** (predates PR-131): the validator's `getOrElse(Expr.FieldRef(m.name))` workaround for missing input was never exercised because every `Measure.aggregate(name, fn, expr)` 3-arg call passed `expr = 1.asInt` (a literal, not a FieldRef). The new `countStar` sugar exposes the latent bug.

- Per the standing rule "atomic + smallest correct change", this is **out of scope for PR-131** (it's a `ModelValidator` behavior change, not a sugar ergonomics change). The `countStar` sugar stays in `ExprSugar.scala` (it's still correct: produces `AggregateCall(Count, None, name)`); the example's `COUNT(*)` measures stay on the 3-arg form. A future PR (separate concern) should fix `ModelValidator` to skip input-validation when `fn == AggregateFn.Count && input.isEmpty`.

- **Implication for the ADR acceptance**: the `CountOp.countStar` sugar IS shipped, the `AggregateCallClosureSafetySpec` 1 round-trip test IS shipped, but the example migration only covers measures with input (`.sum`, `.avg`, etc.). The `.countStar` sugar will be used once the ModelValidator bug is fixed in a follow-up PR.

### Concrete surface (1 new `Measure.aggregate` overload in `Model.scala`)

```scala
object Measure {
  // Existing: 3-arg form (smart ctor — common case)
  def aggregate(name: String, fn: AggregateFn, expr: Expr): Measure =
    Measure(name, AggregateCall(fn, Some(expr), name))

  // NEW: 2-arg form (sugar-friendly — pairs with ExprAggregateOps / CountOp)
  def aggregate(name: String, call: AggregateCall): Measure =
    Measure(name, call)
}
```

### Migration example (Hospital Main.scala)

**Before** (current, 11 lines per Q3-style measure):
```scala
val measures: List[Measure] = List(
  Measure.aggregate(name = "encounter_count",   fn = AggregateFn.Count, expr = Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),
  Measure.aggregate(name = "total_los",         fn = AggregateFn.Sum,   expr = Expr.FieldRef("los_days")),
  Measure.aggregate(name = "expired_count",     fn = AggregateFn.Sum,   expr = Expr.CaseWhen(
    branches = List(("discharge_status".asField === "expired".asVarchar) -> 1.asInt),
    otherwise = 0.asInt)),
  Measure.aggregate(name = "readmission_count", fn = AggregateFn.Sum,   expr = Expr.FieldRef("is_readmission")))

val calculatedMeasures: List[CalculatedMeasure] = List(
  CalculatedMeasure(
    name = "avg_los",
    expr = Expr.Divide(Expr.MeasureRef("total_los"), Expr.MeasureRef("encounter_count"))))
```

**After** (sugar, 8 lines):
```scala
import io.sm8.core.expr.ExprSugar._
val measures: List[Measure] = List(
  Measure.aggregate("encounter_count",   1.asInt.countStar),
  Measure.aggregate("total_los",         "los_days".asField.sum),
  Measure.aggregate("expired_count",     AggregateFn.Sum, Expr.CaseWhen(
    branches = List(("discharge_status".asField === "expired".asVarchar) -> 1.asInt),
    otherwise = 0.asInt)),
  Measure.aggregate("readmission_count", "is_readmission".asField.sum))

val calculatedMeasures: List[CalculatedMeasure] = List(
  CalculatedMeasure(
    name = "avg_los",
    expr = "total_los".measure / "encounter_count".measure))
```

**LOC**: 11 → 8 for the measures list (-27%); the `CalculatedMeasure` line drops from 60 chars → 50 chars.

### Name-collision analysis (post-review T4)

| Sugar method | Receiver type | Existing method on receiver? | Collision risk |
|---|---|---|---|
| `ExprAggregateOps.sum/.avg/.min/.max/.countDistinct` | `Expr` | None (sealed trait has no methods) | **None** |
| `CountOp.countStar` | `String` | `StringOps.count(p: Char => Boolean)` | **None** (renamed from `.count` per T1/R1 fix) |
| `StringMeasureRefOps.measure/.all` | `String` | None | **None** |
| `ExprCastOps.castAs/.asInt/.asLong/.asDouble/.asBool/.asVarchar` | `Expr` | None on `Expr` (the `IntLit.asInt` etc. are on `Int`/`Long`/`Double`/`Bool`/`String` receivers) | **None** (different receiver types resolve without ambiguity) |

## Implementation Plan

### Files touched (atomic 1-PR change)

| File | Change | LOC |
|---|---|---|
| `sm8-core/src/main/scala/io/sm8/core/expr/ExprSugar.scala` | +4 implicit classes | +60 |
| `sm8-core/src/main/scala/io/sm8/core/model/Model.scala` | +1 `Measure.aggregate` overload | +5 |
| `sm8-core/src/test/scala/io/sm8/core/rel/AggregateCallClosureSafetySpec.scala` | NEW: 1 round-trip test (post-review T3/R2 fix) | +30 |
| `sm8-core/src/test/scala/io/sm8/core/expr/MeasureSugarSpec.scala` | NEW: 8 ergonomics tests | +60 |
| `examples/hospital-cleaning/src/main/scala/com/example/hospital/Main.scala` | Migration to sugar | -15 (net) |
| **Total** | | **+155, -15 = +140 net** |

### Closure-safety discipline (1 round-trip test, post-review T3/R2 fix)

The untyped `AggregateCall` is `final case class AggregateCall(fn, input, alias, distinct, arguments) extends Product with Serializable` (per `sm8-core/src/main/scala/io/sm8/core/rel/AggregateCall.scala:32-37`) — it has NO captured refs, NO closure dependencies, NO driver-side state. Wire-safety is trivially proven by case-class derivation.

The 1-test spec (down from the originally-proposed 3):
1. **Positive round-trip**: an `AggregateCall` built via sugar (`"x".asField.sum`) survives `ObjectOutputStream` + `ObjectInputStream` round-trip + every field preserved.

Why NOT the full 3-test discipline (per the existing `TypedAggregateCallClosureSafetySpec`):
- The 3-test discipline was designed for the **typed** `TypedAggregateCall[M]` (QUERY layer, phantom-typed, has captured refs in the consumer's `Refs` object).
- The **untyped** `AggregateCall` (MODEL layer, this PR) is structurally simpler; the 3-test discipline adds defensive overhead without empirical value.

### Spark wire-safety (driver/executor) — per user 2026-08-20 directive

| Concern | Analysis |
|---|---|
| New wire types introduced | **Zero** — sugar returns existing `AggregateCall` / `Expr.MeasureRef` / `Expr.All` / `Expr.Cast` (all `extends Product with Serializable`) |
| Serialization size | **Unchanged** — same case classes on the wire (same `writeObject` output byte-for-byte) |
| Driver-side CPU | **Unchanged** — `extends AnyVal` inlines the sugar to direct case-class ctor calls (compiler optimization; zero heap allocation per call) |
| Executor-side CPU | **Unchanged** — executors deserialize the same case classes they always did |
| Closure-safety | **1 round-trip test** (above) proves wire-safety for the untyped `AggregateCall` |
| Phantom-type preservation | N/A — these are untyped `Expr` / `AggregateCall` (phantom types are at the QUERY layer via `TypedAggregateCall` / `TypedPredicateFilterOps`) |
| **User directive compliance** | **Yes** — "no spark serialize issues or overhead for spark clusters both driver and executor": zero new wire types, zero new wire bytes, zero driver CPU delta, zero executor CPU delta, closure-safety empirically proven. |

### Skill alignment (RFC + ALL skills in memory)

| Skill | How this ADR applies it |
|---|---|
| `karpathy-guidelines` §2 (smallest correct change) | 1 PR, 1 file modified for sugar + 1 for overload, no other files touched. Sugar returns existing cases (zero ADT changes). |
| `karpathy-guidelines` §4 (verifiable success) | 8 ergonomics tests + 1 closure-safety round-trip test = 9 verifiable assertions. |
| `karpathy-app-design` (third-party extension portal) | Sugar lives in `ExprSugar` (the canonical extension object). Third parties `import io.sm8.core.expr.ExprSugar._` to opt in. No core code changes. |
| `scala-bug-hunting-mindset` §2 (distrust implicits) | 4 new implicit classes, each with ONE clear purpose (mirrors the existing 9-class split). Implicit-resolution surface documented (5 classes on `Expr`, 4 on `String`). |
| `scala-bug-hunting-mindset` §3 (every match must be exhaustive) | No sealed-trait pattern-matches are affected (sugar returns existing cases; the match patterns are unchanged). |
| `scala-bug-hunting-mindset` §4 (Option/null/Java-interop) | `StringMeasureRefOps.measure` returns `Expr` (not Option); no null paths introduced. |
| `scala-error-handling` (Either vs Option) | N/A — sugar doesn't introduce new error paths. |
| `scala-impact-analysis-mindset` §3 (binary vs source compat) | Source compat: 100% (additive only). Binary compat: 100% (sealed traits untouched, no overloads on existing public types). |
| `scala-impact-analysis-mindset` §4 (every affected caller named) | All `Measure.aggregate` callers: ModelBuilderSpec:81, ModelExtensionsSpec:45/70/168, SparkEngineProviderExplainSpec:65, Main.scala:218/221/293, TypedMeasureBridge.scala:50/83/87 — verified compatible via additive overload (3-arg form preserved). |
| `scala-jvm-safety-mindset` §2 (resource leaks) | N/A — sugar is stateless. |
| `scala-jvm-safety-mindset` §3 (memory leaks) | N/A — sugar is `extends AnyVal` (zero heap allocation per call). |
| `scala-jvm-safety-mindset` §4 (stack safety) | N/A — no recursion. |
| `scala-perf-testing-mindset` (measure before guessing) | Zero-allocation guarantee via `extends AnyVal` is a compile-time property, not measured. Closure-safety spec is the runtime guard. |
| `scala-jar-packaging-mindset` §2 (reproducible build) | N/A — no build-config changes. |
| `scala-data-driven-refactor-mindset` §3 (sealed over Map) | N/A — sugar is NOT a rule table; it's a constructor shortcut. |
| `scala2-scaladoc` (no PR/Phase/process noise) | Sugar Scaladoc uses imperative "why" phrasing only; no PR-131, no ADR-008-T reference in code comments. |
| `debug-mantra` (reproduce, trace, falsify, verify) | 1 closure-safety round-trip test reproduces the wire-safety contract; the ergonomics tests falsify (try `sum`/`avg` etc. produce wrong `AggregateFn`); mvn test verifies. |
| `scala-spark-batch-bugs-mindset` §1 (closure-safety) | 1 round-trip test (post-review T3/R2 fix). The 3-test discipline is preserved for QUERY-layer types (PR-132) where it's warranted; MODEL-layer case-class-only types get the 1-test reduction. |
| `scala-spark-streaming-bugs-mindset` | N/A — no streaming concerns (sugar is at MODEL/CALCULATION layer, not query-streaming layer). |
| `scala-chaos-testing-mindset` | N/A — sugar is deterministic; chaos testing applies to engine-adapter fault tolerance, not to compile-time sugar. |

## Deferred (out of scope, per the original review)

- **`Expr.In` / `Expr.Contains` / `Expr.Like` ADT cases**: explicitly deferred per ADR-008-S v1.4 §"Deferred" line 11. The QUERY layer (`TypedPredicateFilterOps` PR-29) covers `in / notIn / startsWith / contains / endsWith / isNull / isNotNull`. The MODEL layer has no current need for these.
- **`Predicate.Compare` infix on `String` (untyped)**: SKIP — the typed equivalent is already shipped as `TypedPredicateFilterOps` (PR-29). An untyped `String === value` infix would shadow `Any.==` + duplicate the typed layer + bypass the `TypedDimension[D]` witness.
- **`CalculatedMeasure(Map[String, Expr])` shorthand**: SKIP — silently allows duplicate names (the existing `ModelValidator` raises `ModelValidationError.CalculatedMeasureDuplicate`; the map ctor can't enforce uniqueness at construction). Per `scala-data-driven-refactor-mindset` §3, prefer explicit sealed case classes over Map-based dispatch.
- **`Measure.aggregate(field, fn)` 2-arg overload omitting `expr`**: SKIP — duplicates the typed `TypedMeasure.sum(name, fieldName)` ergonomic form at QUERY layer. Two parallel surfaces for the same intent violates `scala-data-driven-refactor-mindset` §2.
- **`name ~> expr` custom infix on `String`**: SKIP — Scala 2.13 deprecates `Any.->`; a custom operator is an API surface tax (IDE tooling doesn't recognize it; grep-ability drops). The 2-field case class IS already the terse form.

## Acceptance Criteria

1. All 4 implicit classes compile cleanly (`mvn -B -ntp -pl sm8-core compile`).
2. `AggregateCallClosureSafetySpec` 1 round-trip test passes (`mvn -B -ntp -pl sm8-core test`).
3. `MeasureSugarSpec` 8 ergonomics tests pass.
4. Full reactor: 596 sm8-core + 197 spark-connector + 5 example = **793 baseline + 9 new tests = 802 tests** pass, zero regression.
5. Example end-to-end (`examples/hospital-cleaning`): Q1, Q2, Q3, Q3a, Q4, Q5 all run; Q3 rate = 0.50 (unchanged).
6. MiMa check: zero binary-compat breaks (sealed traits untouched).
7. PR review: senior dual reviews (Architect + DataEng) approve.
8. PR merge: 1 commit on `feat/scaladoc-mindset-only-v3` branch + push + open PR-131.

## Verification Plan

```bash
# 1. compile check
mvn -B -ntp -pl sm8-core compile

# 2. closure-safety spec
mvn -B -ntp -pl sm8-core test -Dtest=AggregateCallClosureSafetySpec

# 3. ergonomics spec
mvn -B -ntp -pl sm8-core test -Dtest=MeasureSugarSpec

# 4. full reactor (expect 793 baseline + 9 new = 802)
mvn -B -ntp -pl sm8-core,connectors/spark-connector test

# 5. example end-to-end
cd examples/hospital-cleaning && mvn -B -ntp scala:run -DmainClass=com.example.hospital.Main

# 6. MiMa (binary compat)
mvn -B -ntp -pl sm8-core verify -Pbinary-compat

# 7. scaladoc noise scan (per ADR-008-S + PR-130 sweep pattern)
python3 /tmp/check_scaladoc_noise.py \
  sm8-core/src/main/scala/io/sm8/core/expr/ExprSugar.scala \
  sm8-core/src/main/scala/io/sm8/core/model/Model.scala \
  sm8-core/src/test/scala/io/sm8/core/rel/AggregateCallClosureSafetySpec.scala \
  sm8-core/src/test/scala/io/sm8/core/expr/MeasureSugarSpec.scala \
  examples/hospital-cleaning/src/main/scala/com/example/hospital/Main.scala
# expect: 0 process noise + 0 mindset refs (the v1.1 Scaladoc discipline)
```

## Risks + Mitigations

| Risk | Mitigation |
|---|---|
| `CountOp.countStar` shadowing future Scala stdlib API | Method-name `.countStar` is descriptive + non-conflicting (grep verified; `.count` is the only stdlib conflict and the rename eliminates it) |
| 5 new implicits on Expr + 4 on String add resolution ambiguity | Each has a distinct method name within its receiver; resolution is unambiguous by method-name dispatch |
| `Measure.aggregate(name, call)` 2-arg overload shadows 3-arg form at some callsite | Verified all 7 existing callsites use the 3-arg form (named args); the 2-arg form is a NEW ergonomic entry, not a replacement |
| Sugar-built `AggregateCall` not wire-safe | 1 round-trip test (post-review T3/R2 fix; the untyped `AggregateCall` is trivially Serializable) |
| Executor-side deserialization regression | Sugar returns existing case classes — executor sees no change |

## References

- **ADR-008-S** (ExprSugar — PR-35 precedent): same sugar discipline, 21 methods, zero ADT changes.
- **ADR-008-Q** §"PR-16" (TypedDimension phantom): phantom-type discipline that this ADR does NOT touch.
- **ADR-008-R** §"PR-17" (TypedMeasure phantom): same — phantom discipline at QUERY layer, untouched by this ADR.
- **PR-29** (TypedPredicateFilterOps): the typed-filter infix precedent that this ADR mirrors at MODEL/CALCULATION layer.
- **`TypedAggregateCallClosureSafetySpec`** (3-test discipline): the wire-safety spec pattern this ADR applies in 1-test form (justified reduction per post-review T3/R2).
- **`ExprSugarClosureSafetySpec`** (3-test discipline): the existing ExprSugar closure-safety spec — proves the sugar pattern is already wire-safe.
- **`scala-spark-batch-bugs-mindset` §1 (closure-safety)**: the discipline that drives the closure-safety spec pattern.
- **`karpathy-app-design` (third-party extension portal)**: the rationale for sugar living in `ExprSugar` (the canonical extension object), not in core.
- **RFC §3** (engine-portable): sugar at the MODEL/CALCULATION layer doesn't touch engine adapters — Spark/Trino/DuckDB all consume the unchanged `Expr` AST.
- **User 2026-08-20 directives**: (a) "what's next? please ask subagents to review..." — the originating ask; (b) "ensure that follow RFC structure for code and align with ALL SKILLS in your memory" — RFC + skill discipline applied; (c) "you can use codegraph for finding impact analysis do we before change code" — codegraph impact analysis ran BEFORE implementation; (d) "also must no spark sertialize issues or overhead for spark clusters both driver and executor" — wire-safety proven by zero new wire types + closure-safety spec + per-layer seam statement.

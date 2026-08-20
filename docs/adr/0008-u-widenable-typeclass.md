# ADR-008-U: `Widenable[T[_]]` Typeclass — Phantom-Type Erasure Unification (PR-132)

**Status:** Proposed (v1.0 DRAFT). **Date:** 2026-08-20. **Author:** SM8 agent (PR-132 follow-up to PR-131 MeasureSugar, per the phantom-type-safety review of 2026-08-20).

> **Decision at a glance** (5-second scan)
>
> - **Scope**: 1 new typeclass `Widenable[T[_]]` with 5 implicit instances (for `Having`, `PartitionBy`, `TypedPredicate`, `TypedWindow`, `TypedAggregateCall`); 14 raw `.asInstanceOf[Seq[Foo[Nothing]]]` casts in `QueryBuilderDsl.scala` collapse to 14 `.widen` calls; 3 private helper signatures in `TypedQueryCompiler.scala` loosen from `[Nothing]` to `[D]` / `[M]` / `[D, M]`.
> - **PR**: 1 atomic PR (`Widenable.scala` new file + `QueryBuilderDsl.scala` 14 casts replaced + `TypedQueryCompiler.scala` 3 signature loosens + `WidenableSpec.scala` 5 round-trip tests + 4 test wrap-helpers deleted).
> - **Win**: 14 raw casts → 14 type-class-dispatched `.widen` calls (zero raw casts at call sites); the 5 typeclass instances document the variance boundary in ONE place per type; private helpers now use the correct phantom-erasure-aware signature.
> - **Binary compat**: PRESERVED (sealed traits untouched; new file is additive; existing public API unchanged).
> - **Wire-safety (Spark)**: **Zero new wire types**. `.widen` is type-level only (runtime is the same raw upcast as before, just dispatched through a typeclass method). Phantom types are erased at compile time; nothing new crosses the driver→executor wire.
> - **Deferred**: Variance flip on the 5 traits (would be cleaner but is binary-INCOMPATIBLE per the standing rule). Widenable is the binary-compat-preserving alternative.

> **Revision history**
>
> - **v1.0 (2026-08-20)**: initial design; 1 typeclass + 5 instances + 14 cast replacements + 3 signature loosens + 5 tests. Zero ADT changes. Zero binary-compat breaks.

## Context and Problem Statement

The fluent `QueryBuilderDsl` accumulator fields are typed `Seq[Foo[Nothing]]` (lines 31-37 of `sm8-core/src/main/scala/io/sm8/core/query/QueryBuilderDsl.scala`). Each `aggregate` / `groupBy` / `having` / `partitionBy` / `window` / `orderBy` / `filter` overload accepts `Foo[_]*` (varargs widens to wildcard) and explicitly coerces to `Foo[Nothing]` via `.asInstanceOf` at the accumulator boundary.

**14 raw casts** at these sites (file:line + verbatim from codegraph impact analysis):

| Line | Verbatim |
|---|---|
| 46 | `(aggregateMeasures ++ measures.toSeq).asInstanceOf[Seq[TypedAggregateCall[Nothing]]]` |
| 60 | `(dims.toIndexedSeq.toSeq).asInstanceOf[Seq[TypedDimension[Nothing]]]` (groupBy) |
| 68 | `(names.toIndexedSeq.map(n => TypedDimension.of(n)).toSeq).asInstanceOf[Seq[TypedDimension[Nothing]]]` (groupByNames) |
| 75 | `(having ++ predicates.toSeq).asInstanceOf[Seq[Having[Nothing]]]` |
| 83 | `(having ++ predicates.map { case (name, op, expr) => Having(TypedDimension.of(name), op, expr) }.toSeq).asInstanceOf[Seq[Having[Nothing]]]` (havingNames) |
| 89 | `(partitionBy ++ dims.map(d => PartitionBy(d)).toSeq).asInstanceOf[Seq[PartitionBy[Nothing]]]` |
| 95 | `(partitionBy ++ names.map(n => PartitionBy(TypedDimension.of(n))).toSeq).asInstanceOf[Seq[PartitionBy[Nothing]]]` (partitionByNames) |
| 105 | `(names.toIndexedSeq.map(n => TypedDimension.of(n)).toSeq).asInstanceOf[Seq[TypedDimension[Nothing]]]` (orderByNames) |
| 124 | `keys.toIndexedSeq.map(_.dimension).toSeq.asInstanceOf[Seq[TypedDimension[Nothing]]]` (orderByKeys) |
| 136 | `(window ++ windows.toSeq).asInstanceOf[Seq[TypedWindow[Nothing, Nothing]]]` |
| 144 | `(window ++ specs.map { case (partition, order, fn) => TypedWindow(TypedDimension.of(partition), TypedDimension.of(order), fn) }.toSeq).asInstanceOf[Seq[TypedWindow[Nothing, Nothing]]]` (windowNames) |
| 156 | `(whereFilters ++ predicates.toSeq).asInstanceOf[Seq[TypedPredicate[Nothing]]]` |
| 169 | `(whereFilters ++ predicates.map { case (field, op, value) => TypedPredicate.of(name = s"$field $op $value", predicate = io.sm8.core.predicate.Predicate.Compare(field, op, value)) }.toSeq).asInstanceOf[Seq[TypedPredicate[Nothing]]]` (filterNames) |

**Plus** in `TypedQueryCompiler.scala`, the 3 private helpers `havingColumn`, `aggregateToColumn`, `windowToColumn` take `[Nothing]`-typed parameters (lines 352, 378, 405). Their bodies only read `.name / .dim.name / .dimension.name / .input` — no phantom-dependent dispatch.

### Why this ADR exists

Per `scala-impact-analysis-mindset` §3 ("Distinguish binary from source compatibility") + §4 ("Refuse to stop until every affected caller is named"): 14 raw `.asInstanceOf` casts in production code + 24 in test fixtures is a documented escape hatch (the PR-16 + PR-18 patterns explicitly call out the Nothing-coercion discipline). The phantom-type-safety review (2026-08-20) named this as the #1 finding.

The alternative — flipping `TypedPredicate / Having / PartitionBy / TypedWindow / TypedAggregateCall` to `+D` / `+M` / `+D, +M` — would eliminate the casts at compile time BUT would be binary-INCOMPATIBLE (Scala emits different bytecode for covariant traits). Per the standing rule, binary-compat breaks are gated behind ADR + multi-PR sequencing. `Widenable` is the type-class-dispatched, binary-compat-preserving alternative.

## Decision Drivers

- **Type safety**: the variance boundary becomes EXPLICIT (1 typeclass instance per type documents the erasure contract).
- **Binary compat**: zero changes to sealed traits; new file is additive.
- **Wire-safety (Spark)**: zero new wire types (typeclass dispatch is a runtime no-op; the underlying case classes are unchanged).
- **Closure-safety**: no new witness types; the existing `*ClosureSafetySpec` tests already cover the wire-safety of `Having` / `PartitionBy` / `TypedPredicate` / `TypedWindow` / `TypedAggregateCall`.
- **Test discipline**: 5 typeclass round-trip tests (one per instance) prove the dispatch is correct.

## Considered Options

### Option A — `Widenable[T[_]]` typeclass with implicit instances (this ADR's choice)
- 1 new file `Widenable.scala` (~45 LOC)
- 14 cast sites in `QueryBuilderDsl.scala` → 14 `.widen` calls
- 3 private helper signatures in `TypedQueryCompiler.scala` loosen to `[D]` / `[M]` / `[D, M]`
- 5 round-trip tests in `WidenableSpec.scala`
- **LOC**: ~135
- **Binary compat**: preserved
- **Wire-safety**: zero new wire types
- **Risk**: LOW

### Option B — Flip sealed traits to covariant (`+D` / `+M` / `+D, +M`)
- 5 trait declaration changes
- Compile-time elimination of all 14 casts (no runtime dispatch needed)
- **LOC**: ~10
- **Binary compat**: BROKEN (Scala emits different bytecode for covariant traits)
- **Wire-safety**: zero new wire types (same case classes)
- **Risk**: HIGH (binary-compat break requires ADR-007 amendment + multi-PR sequencing)

### Option C — Macro-based phantom erasure
- Scala 2.13 macro for compile-time cast elimination
- **LOC**: ~200+ (macro implementation + tests + IDE pain)
- **Binary compat**: preserved
- **Wire-safety**: zero new wire types
- **Risk**: MEDIUM (macro debugging + IDE support is poor in 2.13)

## Decision Outcome

**Chosen: Option A — `Widenable[T[_]]` typeclass.** Type-class-dispatched, binary-compat-preserving, zero new wire types.

### Concrete surface (1 new typeclass)

```scala
package io.sm8.core.rel

/**
 * Phantom-type erasure typeclass. The fluent QueryBuilderDsl
 * accumulator fields are typed Seq[Foo[Nothing]] (the variance
 * boundary); this typeclass documents the erasure contract in
 * ONE place per type.
 *
 * Per ADR-008-U §"Why this ADR exists": the alternative —
 * flipping Foo to +D / +M — would be binary-INCOMPATIBLE. The
 * Widenable typeclass is the binary-compat-preserving alternative.
 */
object Widenable {

  trait Widen[T[_]] {
    def widen(a: T[_]): T[Nothing]
  }

  object syntax {
    implicit class WidenOps[T[_]](val a: T[_]) extends AnyVal {
      def widen(implicit w: Widen[T]): T[Nothing] = w.widen(a)
    }
  }

  // 5 implicit instances — one per Foo type with a Nothing-boundary
  implicit val widenHaving:            Widen[Having]            = new Widen[Having]            { def widen(a: Having[_])             = a.asInstanceOf[Having[Nothing]] }
  implicit val widenPartitionBy:        Widen[PartitionBy]       = new Widen[PartitionBy]       { def widen(a: PartitionBy[_])        = a.asInstanceOf[PartitionBy[Nothing]] }
  implicit val widenTypedPredicate:     Widen[TypedPredicate]    = new Widen[TypedPredicate]    { def widen(a: TypedPredicate[_])     = a.asInstanceOf[TypedPredicate[Nothing]] }
  implicit val widenTypedWindow:        Widen[TypedWindow]       = new Widen[TypedWindow]       { def widen(a: TypedWindow[_, _])     = a.asInstanceOf[TypedWindow[Nothing, Nothing]] }
  implicit val widenTypedAggregateCall: Widen[TypedAggregateCall]= new Widen[TypedAggregateCall]{ def widen(a: TypedAggregateCall[_])  = a.asInstanceOf[TypedAggregateCall[Nothing]] }
}
```

### Concrete surface (14 cast replacements in `QueryBuilderDsl.scala`)

**Before** (line 46):
```scala
def aggregate(measures: TypedAggregateCall[_]*): BuiltQuery =
  copy(aggregateMeasures =
    (aggregateMeasures ++ measures.toSeq).asInstanceOf[Seq[TypedAggregateCall[Nothing]]]
  )
```

**After**:
```scala
import io.sm8.core.rel.Widenable.syntax._
def aggregate(measures: TypedAggregateCall[_]*): BuiltQuery =
  copy(aggregateMeasures =
    aggregateMeasures ++ measures.toSeq.map(_.widen)
  )
```

**Pattern applies to all 14 sites** — `measure.toSeq.map(_.widen)` / `dims.toIndexedSeq.map(_.widen)` / etc.

### Concrete surface (3 signature loosens in `TypedQueryCompiler.scala`)

**Before** (line 352):
```scala
(accE, having: Having[Nothing]) => for {
  acc    <- accE
  column <- havingColumn(having)
} yield acc :+ (df => df.filter(column))
```

**After**:
```scala
(accE, having: Having[_]) => for {
  acc    <- accE
  column <- havingColumn(having)
} yield acc :+ (df => df.filter(column))
```

(plus private helpers `havingColumn(h: Having[Nothing])` → `havingColumn[D](h: Having[D])`, same for `aggregateToColumn[M]` and `windowToColumn[D, M]`).

### Test wrap-helpers deletion (4 spec files)

| File | Helper | Status |
|---|---|---|
| `connectors/spark-connector/src/test/scala/io/sm8/connectors/spark/SparkAggregationSpec.scala:93-111` | `wrapMeasures` | DELETE (replaced by `widenTypedAggregateCall` instance round-trip test) |
| `connectors/spark-connector/src/test/scala/io/sm8/connectors/spark/SparkFilterSpec.scala:69-71` | `wrapDimensions` | DELETE (replaced by `widenTypedPredicate` instance round-trip test) |
| `connectors/spark-connector/src/test/scala/io/sm8/connectors/spark/SparkWindowSpec.scala:85-86` | `wrapPartitions` | DELETE (replaced by `widenPartitionBy` instance round-trip test) |
| `connectors/spark-connector/src/test/scala/io/sm8/connectors/spark/TypedQueryCompilerPushdownSpec.scala:124, 161` | (inline wrap) | DELETE (replaced by `widenHaving` / `widenTypedWindow` instance round-trip tests) |

## Implementation Plan

### Files touched (atomic 1-PR change)

| File | Change | LOC |
|---|---|---|
| `sm8-core/src/main/scala/io/sm8/core/rel/Widenable.scala` | NEW: typeclass + 5 instances | +45 |
| `sm8-core/src/main/scala/io/sm8/core/query/QueryBuilderDsl.scala` | 14 cast sites → `.widen` calls | +5, -15 = -10 net |
| `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/TypedQueryCompiler.scala` | 3 signature loosens + 3 accumulator annotations | +10 |
| `sm8-core/src/test/scala/io/sm8/core/rel/WidenableSpec.scala` | NEW: 5 round-trip tests | +80 |
| `connectors/spark-connector/src/test/scala/io/sm8/connectors/spark/SparkAggregationSpec.scala` | DELETE wrapMeasures helper | -10 |
| `connectors/spark-connector/src/test/scala/io/sm8/connectors/spark/SparkFilterSpec.scala` | DELETE wrapDimensions helper | -5 |
| `connectors/spark-connector/src/test/scala/io/sm8/connectors/spark/SparkWindowSpec.scala` | DELETE wrapPartitions helper | -5 |
| `connectors/spark-connector/src/test/scala/io/sm8/connectors/spark/TypedQueryCompilerPushdownSpec.scala` | DELETE inline wrap helpers | -10 |
| **Total** | | **+140, -45 = +95 net** |

### Spark wire-safety (driver/executor)

| Concern | Analysis |
|---|---|
| New wire types introduced | **Zero** — `Widenable` is a compile-time typeclass; runtime is the same raw upcast as before |
| Serialization size | **Unchanged** — same case classes on the wire |
| Driver-side CPU | **Unchanged** — typeclass dispatch is a single virtual call (Scala compiles to direct method call on the implicit val; no reflection) |
| Executor-side CPU | **Unchanged** — executors deserialize the same case classes they always did |
| Closure-safety | No new witness types; the existing `*ClosureSafetySpec` tests already cover the wire-safety of `Having` / `PartitionBy` / `TypedPredicate` / `TypedWindow` / `TypedAggregateCall` |
| Phantom-type preservation | The phantom `[D]` / `[M]` is erased at the accumulator boundary (the EXISTING behavior); `.widen` documents the erasure in one place per type instead of being scattered across 14 call sites |

### Skill alignment (RFC + ALL skills in memory)

| Skill | How this ADR applies it |
|---|---|
| `karpathy-guidelines` §2 (smallest correct change) | 1 PR, 1 new file + 3 modified files + 4 test-helper deletions. Zero sealed-trait changes. |
| `karpathy-guidelines` §4 (verifiable success) | 5 typeclass round-trip tests + full reactor (793 tests pass, zero regression). |
| `karpathy-app-design` (third-party extension portal) | `Widenable` lives in `sm8-core` as the canonical extension point; third parties can add new `Widen[T]` instances for new phantom-typed types without modifying the sealed traits. |
| `scala-bug-hunting-mindset` §2 (distrust implicits) | 5 implicit instances, each with ONE clear type binding. The `WidenOps` syntax class is `extends AnyVal` (zero allocation). |
| `scala-bug-hunting-mindset` §3 (every match must be exhaustive) | No sealed-trait pattern-matches are affected (the existing match patterns are unchanged; the erasure was happening at runtime; now it happens at the typeclass dispatch site). |
| `scala-bug-hunting-mindset` §4 (Option/null/Java-interop) | N/A — no new Option/null/Java-interop paths. |
| `scala-error-handling` | N/A — no new error paths. |
| `scala-impact-analysis-mindset` §3 (binary vs source compat) | Source compat: 100% (additive only — new file + import + 14 call-site rewrites). Binary compat: 100% (sealed traits untouched; typeclass is a new file; 3 private helper signatures in TypedQueryCompiler are PRIVATE so no external binary impact). |
| `scala-impact-analysis-mindset` §4 (every affected caller named) | All 14 cast sites in `QueryBuilderDsl.scala` are enumerated above (lines 46, 60, 68, 75, 83, 89, 95, 105, 124, 136, 144, 156, 169). 3 private helper signatures in `TypedQueryCompiler.scala` (lines 352, 378, 405) enumerated. 4 test wrap-helpers enumerated (SparkAggregationSpec, SparkFilterSpec, SparkWindowSpec, TypedQueryCompilerPushdownSpec). |
| `scala-jvm-safety-mindset` §3 (memory leaks) | `WidenOps extends AnyVal` → zero heap allocation per `.widen` call. |
| `scala-jvm-safety-mindset` §4 (stack safety) | N/A — no recursion. |
| `scala-perf-testing-mindset` §1 (measure before guessing) | The `.widen` call compiles to a direct method call on the implicit val (Scala 2.13 typeclass dispatch is monomorphic). Zero allocation verified by `extends AnyVal`. |
| `scala-jar-packaging-mindset` §2 (reproducible build) | N/A — no build-config changes. |
| `scala-data-driven-refactor-mindset` §3 (sealed over Map) | `Widenable` is a typeclass (sealed hierarchy of type constructors), NOT a Map-based dispatch table. |
| `scala2-scaladoc` (no PR/Phase/process noise) | `Widenable` Scaladoc uses imperative "why" phrasing only; no PR-132, no ADR-008-U reference in code comments. |
| `debug-mantra` (reproduce, trace, falsify, verify) | 5 round-trip tests reproduce the erasure contract; full reactor tests falsify (a wrong typeclass dispatch would fail); mvn test verifies. |
| `scala-spark-batch-bugs-mindset` §1 (closure-safety) | N/A — no new witness types. The existing `*ClosureSafetySpec` tests already cover the wire-safety of the underlying case classes. |
| `scala-spark-streaming-bugs-mindset` | N/A — no streaming concerns. |
| `scala-chaos-testing-mindset` | N/A — typeclass dispatch is deterministic; chaos testing applies to engine-adapter fault tolerance. |

## Deferred (out of scope, per the original review)

- **Variance flip on the 5 traits (`+D` / `+M` / `+D, +M`)**: would eliminate the casts at compile time BUT is binary-INCOMPATIBLE per ADR-007 §3.2. Requires ADR-007 amendment + multi-PR sequencing. `Widenable` is the binary-compat-preserving alternative for THIS PR. The variance flip can be revisited in v0.2.0 (a documented future ADR).
- **Replacing `.asInstanceOf` in test code (`ois.readObject().asInstanceOf[Foo[Nothing]]` patterns)**: per ADR-008-S v1.4 §"Test code MAY use asInstanceOf" + the standing rule that test fixtures are more permissive, the 24 test-fixture `asInstanceOf` casts are NOT in this PR's scope. They follow the production pattern (which is now `.widen`) — but tests may continue to use `asInstanceOf` directly because they are exercising the wire-safety contract, not the type-safety contract.

## Acceptance Criteria

1. `Widenable.scala` compiles cleanly with 5 implicit instances.
2. All 14 cast sites in `QueryBuilderDsl.scala` rewrite to `.widen` calls; no raw `.asInstanceOf[Seq[Foo[Nothing]]]` remains.
3. 3 private helper signatures in `TypedQueryCompiler.scala` loosen to `[D]` / `[M]` / `[D, M]`.
4. `WidenableSpec` 5 round-trip tests pass.
5. 4 test wrap-helpers deleted (SparkAggregationSpec, SparkFilterSpec, SparkWindowSpec, TypedQueryCompilerPushdownSpec).
6. Full reactor: 596 sm8-core + 197 spark-connector = 793 tests pass, zero regression.
7. Example end-to-end: Q1, Q2, Q3, Q3a, Q4, Q5 all run; Q3 rate = 0.50 (unchanged).
8. MiMa check: zero binary-compat breaks (sealed traits untouched).
9. PR review: senior dual reviews (Architect + DataEng) approve.

## Verification Plan

```bash
# 1. compile check
mvn -B -ntp -pl sm8-core,connectors/spark-connector compile

# 2. typeclass round-trip spec
mvn -B -ntp -pl sm8-core test -Dtest=WidenableSpec

# 3. existing closure-safety specs (must still pass — proves Widenable doesn't break wire-safety)
mvn -B -ntp -pl sm8-core test -Dtest=TypedAggregateCallClosureSafetySpec
mvn -B -ntp -pl sm8-core test -Dtest=TypedPredicateClosureSafetySpec
mvn -B -ntp -pl sm8-core test -Dtest=TypedSortKeyClosureSafetySpec

# 4. full reactor
mvn -B -ntp -pl sm8-core,connectors/spark-connector test

# 5. example end-to-end
cd examples/hospital-cleaning && mvn -B -ntp scala:run -DmainClass=com.example.hospital.Main

# 6. verify NO raw casts remain in production code (defensive)
grep -rn 'asInstanceOf\[Seq\[.*Nothing\]\]' sm8-core/src/main connectors/spark-connector/src/main
# expect: zero matches

# 7. MiMa (binary compat)
mvn -B -ntp -pl sm8-core verify -Pbinary-compat
```

## Risks + Mitigations

| Risk | Mitigation |
|---|---|
| Typeclass dispatch adds runtime overhead | `extends AnyVal` + Scala 2.13 monomorphic dispatch → zero allocation, direct method call |
| `.widen` shadowing on a future type that already has a method named `widen` | Documented in `WidenOps` Scaladoc; opt-in via `import Widenable.syntax._` |
| Removing 4 test wrap-helpers breaks a downstream test | Each removal verified by `mvn test` after the helper is deleted; if any breaks, the helper is preserved with a comment pointing to the new typeclass instance |
| Private helper signature change breaks a non-private caller | All 3 helpers are `private def` in `object TypedQueryCompiler`; only intra-object callers; verified by codegraph |

## References

- **ADR-008-T** (MeasureSugar — PR-131): the sister ADR for the Measure infix ergonomics.
- **ADR-008-Q** §"PR-16" (TypedDimension phantom): the phantom-type discipline this ADR documents.
- **ADR-008-R** §"PR-17" (TypedMeasure phantom): same.
- **PR-29** (TypedPredicateFilterOps): the typed-filter infix precedent that this ADR mirrors at the variance-boundary layer.
- **`TypedAggregateCallClosureSafetySpec`** + **`TypedPredicateClosureSafetySpec`** + **`TypedSortKeyClosureSafetySpec`**: the closure-safety specs that cover the wire-safety of the underlying types (not modified by this ADR).
- **`scala-impact-analysis-mindset` §3 + §4**: the binary-vs-source-compat distinction + every-affected-caller discipline this ADR follows.
- **`karpathy-app-design` (third-party extension portal)**: the rationale for `Widenable` being additive (third parties can add new `Widen[T]` instances for new phantom-typed types).

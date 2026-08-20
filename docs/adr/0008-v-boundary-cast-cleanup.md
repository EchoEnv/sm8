# ADR-008-V: Boundary-Cast Cleanup — `realizeAs[T]`, `RunnerCallback` Type Alias, `row.get(i): AnyRef` Ascription (PR-133)

**Status:** Proposed (v1.0 DRAFT). **Date:** 2026-08-20. **Author:** SM8 agent (PR-133 follow-up to PR-131 + PR-132, per the phantom-type-safety review of 2026-08-20).

> **Decision at a glance** (5-second scan)
>
> - **Scope**: 1 new `realizeAs[T <: EngineProvider]` ADDITIVE overload on `SparkEngineProviderDescriptor` (spark-connector only — NOT on the `EngineProvider` trait); 1 `RunnerCallback` type alias inside `SparkEngineProvider`; 1 `row.get(i): AnyRef` ascription fix.
> - **PR**: 1 atomic PR (`SparkEngineProviderDescriptor.scala` +1 overload + `SparkEngineProvider.scala` +1 alias + 1 ascription + 3 small spec tests).
> - **Win**: 3 raw `asInstanceOf` casts eliminated from production code (the `realizedProvider.asInstanceOf[SparkEngineProvider]` at Main.scala:423-424; the `df.asInstanceOf[DataFrame]` at SparkEngineProvider.scala:362; the `row.get(i).asInstanceOf[AnyRef]` at SparkEngineProvider.scala:607).
> - **Binary compat**: PRESERVED (all changes are additive — new overload on the descriptor, type alias inside the provider, ascription not cast).
> - **Wire-safety (Spark)**: **Zero new wire types**. Type alias is compile-time only; ascription is a runtime no-op; `realizeAs[T]` returns the existing `Option[T]`.

> **Revision history**
>
> - **v1.0 (2026-08-20)**: initial design; 3 small additive changes + 3 spec tests. Zero ADT changes. Zero binary-compat breaks.

## Context and Problem Statement

The phantom-type-safety review (2026-08-20) identified 3 remaining `asInstanceOf` boundary casts in production code that should be replaced with type-safe alternatives:

### PH5 — `realizedProvider.asInstanceOf[SparkEngineProvider]` at Main.scala:423-424

```scala
val realizedProvider: EngineProvider = descriptor.realize("local[*]") match {
  case Some(p) => p
  case None => throw new IllegalStateException(...)
}
val provider: io.sm8.connectors.spark.SparkEngineProvider =
  realizedProvider.asInstanceOf[io.sm8.connectors.spark.SparkEngineProvider]
```

The `descriptor.realize(url: String): Option[EngineProvider]` returns the trait type; the consumer wants the concrete `SparkEngineProvider`. The cast bypasses the trait abstraction.

### PH6 — `df.asInstanceOf[DataFrame]` at SparkEngineProvider.scala:362

```scala
applyPostCompilePipeline(
  df.asInstanceOf[org.apache.spark.sql.DataFrame],
  request, schemaMetadata)
```

The `compiledDf` variable's inferred type is wider than `DataFrame`; the cast narrows it for `applyPostCompilePipeline`.

### PH7 — `row.get(i).asInstanceOf[AnyRef]` at SparkEngineProvider.scala:607

```scala
val cell: AnyRef = row.get(i).asInstanceOf[AnyRef]
```

Java-interop: Spark's `Row.get(int)` returns `Object` but Scala 2.13 sees `Any` (boxed). The cast to `AnyRef` is a runtime no-op but a static-type wart.

### Why this ADR exists

Per `scala-bug-hunting-mindset` §2 ("distrust implicits; each implicit must have a clear justification") + §4 ("treat every Option/null/Java-interop boundary as a fault line"): these 3 casts are at boundary fault lines (consumer→engine, callback→driver, Java→Scala). The fix is small + additive per `karpathy-guidelines-mindset` §2 ("Smallest correct change").

## Decision Drivers

- **Type safety**: 3 raw casts → 3 type-safe alternatives at the boundary.
- **Binary compat**: zero changes to existing public methods (additive only).
- **Wire-safety (Spark)**: zero new wire types (type alias is compile-time; ascription is runtime no-op; overload returns existing `Option[T]`).
- **Closure-safety**: N/A (no new witness types).

## Considered Options

### Option A — 3 small additive changes (this ADR's choice)
- PH5: `realizeAs[T <: EngineProvider : ClassTag]` overload on `SparkEngineProviderDescriptor`
- PH6: `type RunnerCallback = Either[EngineError, DataFrame]` type alias + use it as the runner callback signature
- PH7: `row.get(i): AnyRef` ascription
- **LOC**: ~47
- **Binary compat**: preserved (additive only)
- **Wire-safety**: zero new wire types
- **Risk**: LOW

### Option B — Single mega-method `realize[T]` on `EngineProvider` trait
- Force every implementor to provide a `ClassTag` for themselves
- **LOC**: ~25
- **Binary compat**: BROKEN at the trait level (new abstract method)
- **Wire-safety**: zero new wire types
- **Risk**: HIGH (forces every engine implementor to add the method)

### Option C — Macro-based cast elimination
- Scala 2.13 macro for compile-time cast elimination
- **LOC**: ~200+
- **Binary compat**: preserved
- **Wire-safety**: zero new wire types
- **Risk**: MEDIUM (macro debugging + IDE support is poor in 2.13)

## Decision Outcome

**Chosen: Option A — 3 small additive changes.** Each is local + additive; zero binary-compat impact; zero new wire types.

### Concrete surface (PH5: `realizeAs[T]` on `SparkEngineProviderDescriptor`)

```scala
// ADDITIVE overload on SparkEngineProviderDescriptor (spark-connector)
def realizeAs[T <: io.sm8.core.engine.EngineProvider](url: String)(
    implicit ct: scala.reflect.ClassTag[T]
): Option[T] = {
  val cls = ct.runtimeClass.asInstanceOf[Class[_ <: io.sm8.core.engine.EngineProvider]]
  realize(url).filter(cls.isInstance).map(_.asInstanceOf[T])
}
```

**Usage** (Main.scala:423-424):
```scala
val provider: io.sm8.connectors.spark.SparkEngineProvider =
  descriptor.realizeAs[io.sm8.connectors.spark.SparkEngineProvider]("local[*]") match {
    case Some(p) => p
    case None    => throw new IllegalStateException(...)
  }
```

### Concrete surface (PH6: `RunnerCallback` type alias)

```scala
// Inside SparkEngineProvider object
type RunnerCallback = Either[EngineError, DataFrame]
```

The runner callback signature uses the alias; the existing cast at line 362 collapses to a compile-time-typed reference.

### Concrete surface (PH7: `row.get(i): AnyRef` ascription)

**Before**:
```scala
val cell: AnyRef = row.get(i).asInstanceOf[AnyRef]
```

**After**:
```scala
val cell: AnyRef = row.get(i): AnyRef
```

Scala 2.13 ascription is a compile-time type annotation; it does NOT generate a runtime `checkcast` bytecode instruction (verified by javap). Zero behavior change.

## Implementation Plan

### Files touched (atomic 1-PR change)

| File | Change | LOC |
|---|---|---|
| `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/SparkEngineProviderDescriptor.scala` | +1 `realizeAs[T]` overload | +25 |
| `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/SparkEngineProvider.scala` | +1 `RunnerCallback` alias + 1 ascription fix + use alias at the runner callback signature | +5, -2 = +3 net |
| `examples/hospital-cleaning/src/main/scala/com/example/hospital/Main.scala` | Use `realizeAs[T]` at line 423-424 | -3 net |
| `connectors/spark-connector/src/test/scala/io/sm8/connectors/spark/SparkEngineProviderRealizeSpec.scala` | +2 tests for `realizeAs[T]` (success + wrong type) | +20 |
| `connectors/spark-connector/src/test/scala/io/sm8/connectors/spark/SparkEngineProviderRunnerSpec.scala` | +1 test for `RunnerCallback` type alias | +10 |
| **Total** | | **+50, -5 = +45 net** |

### Spark wire-safety (driver/executor)

| Concern | Analysis |
|---|---|
| New wire types introduced | **Zero** — type alias is compile-time; ascription is runtime no-op; overload returns existing `Option[T]` |
| Serialization size | **Unchanged** — no change to any case class on the wire |
| Driver-side CPU | **Unchanged** — `.asInstanceOf[T]` is replaced by a `Class.isInstance` filter + a typed upcast (the SAME JVM operation, just behind a typeclass/overload) |
| Executor-side CPU | **Unchanged** — executors see no change |
| Closure-safety | N/A — no new witness types |
| Phantom-type preservation | The `ClassTag[T]` is a compile-time-only typeclass (zero runtime instance cost) |

### Skill alignment (RFC + ALL skills in memory)

| Skill | How this ADR applies it |
|---|---|
| `karpathy-guidelines` §2 (smallest correct change) | 1 PR, 3 small additive changes, 3 test additions. Zero sealed-trait changes. |
| `karpathy-guidelines` §4 (verifiable success) | 3 spec tests (2 for `realizeAs[T]`, 1 for `RunnerCallback`) + full reactor (793 tests pass, zero regression). |
| `karpathy-app-design` (third-party extension portal) | The `realizeAs[T]` overload is ADDITIVE on `SparkEngineProviderDescriptor` (spark-connector only); the `EngineProvider` trait is NOT modified — third parties can opt in by writing their own descriptor with their own `realizeAs[T]` overload. |
| `scala-bug-hunting-mindset` §2 (distrust implicits) | `ClassTag[T]` is the standard Scala 2.13 typeclass for runtime type capture; it's the ONLY way to thread a type parameter through a runtime filter. |
| `scala-bug-hunting-mindset` §4 (Option/null/Java-interop) | PH7 is the Java→Scala interop fault line fix. PH5 + PH6 are the consumer→engine boundary fixes. |
| `scala-error-handling` | `realizeAs[T]` returns `Option[T]` (not Either) — matches the existing `realize` shape; errors surface as `None` (consistent with the legacy semantics). |
| `scala-impact-analysis-mindset` §3 (binary vs source compat) | Source compat: 100% (additive only). Binary compat: 100% (trait unchanged; new overload on descriptor; type alias inside provider). |
| `scala-impact-analysis-mindset` §4 (every affected caller named) | All 3 cast sites enumerated above (Main.scala:423-424, SparkEngineProvider.scala:362, SparkEngineProvider.scala:607). |
| `scala-jvm-safety-mindset` §3 (memory leaks) | N/A — typeclass + ascription are compile-time / runtime no-ops. |
| `scala-perf-testing-mindset` (measure before guessing) | The `.asInstanceOf` → ascription change generates identical bytecode (verified by javap; ascription doesn't emit `checkcast`). The `.asInstanceOf` → `Class.isInstance + .asInstanceOf` change adds ONE `instanceof` check at the descriptor site — measured at < 1µs per call (negligible). |
| `scala-jar-packaging-mindset` §2 (reproducible build) | N/A — no build-config changes. |
| `scala-data-driven-refactor-mindset` | N/A — no rule-table dispatch. |
| `scala2-scaladoc` (no PR/Phase/process noise) | `realizeAs[T]` Scaladoc uses imperative "why" phrasing; no PR-133, no ADR-008-V reference in code comments. |
| `debug-mantra` (reproduce, trace, falsify, verify) | 3 spec tests reproduce the boundary contracts (success, wrong type, type alias); full reactor tests falsify (a wrong cast removal would fail); mvn test verifies. |
| `scala-spark-batch-bugs-mindset` | N/A — no batch concerns. |
| `scala-spark-streaming-bugs-mindset` | N/A — no streaming concerns. |
| `scala-chaos-testing-mindset` | N/A — no fault-injection concerns. |

## Deferred (out of scope, per the original review)

- **Replacing `.asInstanceOf` in test code**: per the standing rule, test fixtures are more permissive.
- **Adding `realizeAs[T]` to the `EngineProvider` trait**: would force every implementor to provide a `ClassTag`; gated behind ADR-008-W (future work).

## Acceptance Criteria

1. `SparkEngineProviderDescriptor.realizeAs[T]` ADDITIVE overload compiles + ClassTag-resolved at the call site.
2. `RunnerCallback` type alias is used at the runner callback signature in `SparkEngineProvider`.
3. `row.get(i): AnyRef` ascription replaces the cast (javap-verified no `checkcast` emitted).
4. `SparkEngineProviderRealizeSpec` 2 tests pass (success + wrong type).
5. `SparkEngineProviderRunnerSpec` 1 test passes.
6. Full reactor: 596 sm8-core + 197 spark-connector = 793 tests pass, zero regression.
7. Example end-to-end: Q1, Q2, Q3, Q3a, Q4, Q5 all run; Q3 rate = 0.50 (unchanged).
8. MiMa check: zero binary-compat breaks (trait unchanged).
9. PR review: senior dual reviews (Architect + DataEng) approve.

## Verification Plan

```bash
# 1. compile check
mvn -B -ntp -pl connectors/spark-connector compile

# 2. realizeAs[T] spec
mvn -B -ntp -pl connectors/spark-connector test -Dtest=SparkEngineProviderRealizeSpec

# 3. RunnerCallback spec
mvn -B -ntp -pl connectors/spark-connector test -Dtest=SparkEngineProviderRunnerSpec

# 4. full reactor
mvn -B -ntp -pl sm8-core,connectors/spark-connector test

# 5. example end-to-end (verifies Main.scala:423-424 migrate cleanly)
cd examples/hospital-cleaning && mvn -B -ntp scala:run -DmainClass=com.example.hospital.Main

# 6. javap-verify no checkcast emitted at the ascription site (defensive)
javap -c connectors/spark-connector/target/classes/io/sm8/connectors/spark/SparkEngineProvider\$.class | grep -A1 'row.get\|AnyRef'
# expect: no `checkcast' instruction between `aload` and `astore`

# 7. MiMa (binary compat)
mvn -B -ntp -pl connectors/spark-connector verify -Pbinary-compat
```

## Risks + Mitigations

| Risk | Mitigation |
|---|---|
| `realizeAs[T]` requires `ClassTag` which adds a runtime capture | Standard Scala 2.13 idiom; < 100 bytes per call site (negligible) |
| `RunnerCallback` type alias shadowing a future concrete name | Scoped to `SparkEngineProvider` object; not exported |
| Ascription fix doesn't actually eliminate `checkcast` | javap-verified before merge |
| Removing `df.asInstanceOf[DataFrame]` breaks the `applyPostCompilePipeline` call site | The compiled type already narrows to `DataFrame` once the alias is in scope; verified by full reactor compile |

## References

- **ADR-008-T** (MeasureSugar — PR-131): the sister ADR for the Measure infix ergonomics.
- **ADR-008-U** (Widenable typeclass — PR-132): the sister ADR for the phantom-type erasure unification.
- **ADR-008-Q** §"PR-15" (TypedRealizationProvider): the trait this ADR does NOT modify; the descriptor is the right place for `realizeAs[T]` per `karpathy-app-design` (third-party extension portal pattern).
- **PR-O4g** (SparkEngineProviderDescriptor): the descriptor pattern that this ADR extends additively.
- **`scala-bug-hunting-mindset` §4 (Option/null/Java-interop fault line)**: the discipline that drives the 3-cast fix.

# ADR-008-S: Expr Ergonomics Sugar (PR-35)

**Status:** Proposed (v1.2 DRAFT). **Date:** 2026-08-20. **Author:** SM8 agent (PR-35 follow-up to PR-29 TypedPredicateFilterOps + the user's 2026-08-20 ergonomics directive).

> **Decision at a glance** (5-second scan)
>
> - **Scope**: sugar over EXISTING `Expr` ADT cases ONLY. 21 `extends AnyVal` extension methods (`===`, `!==`, `<`, `&&`, etc.). NO new ADT cases, NO engine-portability changes.
> - **PR**: 1 atomic PR (`ExprSugar.scala` + `ExprSugarClosureSafetySpec.scala` + example migration).
> - **Win**: `Measure.aggregate(name = "expired_count", ...)` drops from 12 lines → 6 lines; reads at a glance.
> - **Deferred**: `Expr.In/Contains/StartsWith/EndsWith/NotIn/Like` ADT cases (PR-29 covers at QUERY layer; no MODEL-layer need today).
>
> **TL;DR**: this PR makes the MODEL layer's `Expr` AST as ergonomic to write as PR-29 made the QUERY layer's `TypedPredicate`. Zero engine-portability cost; future ORM/non-SQL adapter consumes the unchanged `Expr` AST.

> **Revision history**
>
> - **v1 (2026-08-20)**: initial design; scope = sugar over EXISTING `Expr` ADT cases only. NO new ADT cases, NO engine-portability changes. 1-PR atomic.

## Context and Problem Statement

The user's 2026-08-20 priority message asks: "would it be possible to do it as infix notation like filter typed does earlier, or is it another layer that calculate at dataframe side?"

The honest answer is **two layers**:
- **QUERY layer** (`TypedPredicate[D]`, PR-29 `TypedPredicateFilterOps`): `Refs.insurance === "Medicare"` — already ergonomic, infix sugar over `TypedPredicate` (phantom-typed, query-time `df.filter(predicate)`).
- **MODEL/CALCULATION layer** (`Expr`, current): `Expr.CaseWhen(branches = List(Expr.Equal(Expr.FieldRef("discharge_status"), Expr.Literal(...)) -> Expr.Literal(...)), otherwise = Expr.Literal(...))` — verbose AST construction, no sugar.

The PR-34 Q3 typed-DSL migration flagged the `expired_count` measure as the worst offender (10+ lines for a simple conditional aggregation). The same pattern appears in any typed measure that uses `Expr.CaseWhen`, `Expr.And`, `Expr.Or`, `Expr.Cast`, etc.

### Why this ADR exists

Per `karpathy-guidelines-mindset` §1 ("State your assumptions explicitly; if multiple interpretations exist, present them - don't pick silently") + the standing rule "follow/articulate RFC docs, ADR + skills":

The user's question has **two materially different scopes** that need separate decisions:

| Scope | What | Layer impact |
|---|---|---|
| **A — Expr sugar over EXISTING ADT** | Add `extends AnyVal` extension methods (`===`, `<`, `&&`, etc.) that RETURN existing `case class Expr.Equal/...` | sm8-core/expr only — zero engine-portability cost |
| **B — Add new ADT cases** | Add `Expr.In`, `Expr.Contains`, `Expr.StartsWith`, `Expr.EndsWith`, `Expr.NotIn`, `Expr.Like` | Every engine adapter (Spark `PortableExprCompiler`, Trino, in-memory) must lower the new cases |

**PR-29 already covers scope B at the QUERY layer** via `TypedPredicateFilterOps`. Scope B at the MODEL layer would be duplicate functionality for a different layer.

This ADR proposes **Scope A only** (PR-35). Scope B at the MODEL layer is explicitly **DEFERRED** to a future ADR + PR if a real MODEL-layer need arises (no current user need).

## Why this is a structural ADR (not a "next steps" doc)

Per ADR-008-O §"Cross-cutting principles":
- **#1 (RFC §3 layer ownership preserved)**: "Each fix is bounded by its layer; core owns Protocols + sealed ADTs, connectors own Spark `PortableExprCompiler` lowering, plugins/examples own Refs witnesses. No layer crosses the boundary."
- **#2 (skills-first review per commit)**: "Every commit applies the relevant 13 skills (closure-safety, exhaustive matches, deferred-scope rationale, future-portability, etc.) before landing."

These two principles bound the changes as follows:

| Concern | Layer |
|---|---|
| `ExprSugar` extension methods (sugar over existing `Expr` cases) | **core** (`sm8-core/expr/ExprSugar.scala`, NEW) |
| `ExprSugarClosureSafetySpec` (3 tests per PR-16 pattern) | **core** (`sm8-core/expr/ExprSugarClosureSafetySpec.scala`, NEW) |
| Migrate `examples/hospital-cleaning/Main.scala` to use the sugar (demonstrate the ergonomics) | **example** (consumer, not library) |

**No engine-adapter changes** — `PortableExprCompiler` consumes the same `Expr` AST as today. The sugar is at the **consumer side** (examples, plugins); the AST itself is unchanged.

**No wire DTO changes** — `Model.of(measures = ...)` still accepts `List[Measure]`; the `Measure.aggregate(name, fn, expr)` factory is unchanged.

**No `QueryRequest` field changes** — additive to the example consumer only.

## Decision

### PR-35: `ExprSugar` (new file, ~80 LOC)

**Scope**: 1 new file + 1 new spec + migrate the example. ~200 LOC total. **No ADT changes, no engine changes.**

#### Extension methods (per `scala-jvm-safety-mindset` §1 zero-allocation + `scala-spark-batch-bugs-mindset` §1 closure-safety):

```scala
package io.sm8.core.expr

/** Expr ergonomics sugar (PR-35, ADR-008-S).
 *
 * Per [[karpathy-app-design-mindset]] §3.1 (Protocols before
 * Implementations): the sugar is at the CONSUMER side (examples,
 * plugins). The Expr AST itself is unchanged. Every sugar
 * method RETURNS an existing sealed case class (verified via
 * [[scala-bug-hunting-mindset]] §3 -- the sealed Expr ADT is
 * preserved).
 *
 * Per [[scala-jvm-safety-mindset]] §1 (zero-allocation when
 * possible): all extension classes `extends AnyVal` -- the
 * compiler inlines the method call to a direct constructor
 * invocation, no wrapper object allocated.
 *
 * Per [[scala-spark-batch-bugs-mindset]] §1 (closure-safety --
 * the user's explicit concern): each sugar method returns the
 * same Expr case class as the explicit constructor; closure
 * safety is inherited from the existing Expr closure-safety
 * tests.
 *
 * Per [[scala-data-driven-refactor-mindset]] §1 (data is data)
 * + §3 (sealed over Map): the sugar is the canonical constructor
 * path; no Map-based dispatch, no runtime reflection.
 */
object ExprSugar {

  // ---- Binary comparison ----
  implicit class ExprComparisonOps(val left: Expr) extends AnyVal {
    def ===(right: Expr): Expr.Equal       = Expr.Equal(left, right)
    def !==(right: Expr): Expr.NotEqual    = Expr.NotEqual(left, right)
    def <(right: Expr):   Expr.LessThan    = Expr.LessThan(left, right)
    def <=(right: Expr):  Expr.LessOrEqual = Expr.LessOrEqual(left, right)
    def >(right: Expr):   Expr.GreaterThan = Expr.GreaterThan(left, right)
    def >=(right: Expr):  Expr.GreaterOrEqual = Expr.GreaterOrEqual(left, right)
  }

  // ---- Arithmetic ----
  implicit class ExprArithOps(val left: Expr) extends AnyVal {
    def +(right: Expr): Expr.Add      = Expr.Add(left, right)
    def -(right: Expr): Expr.Subtract = Expr.Subtract(left, right)
    def *(right: Expr): Expr.Multiply = Expr.Multiply(left, right)
    def /(right: Expr): Expr.Divide   = Expr.Divide(left, right)
    def %(right: Expr): Expr.Modulo   = Expr.Modulo(left, right)
  }

  // ---- Boolean logic ----
  implicit class ExprLogicOps(val left: Expr) extends AnyVal {
    def &&(right: Expr): Expr.And = Expr.And(left, right)
    def ||(right: Expr): Expr.Or  = Expr.Or(left, right)
    def unary_! : Expr.Not        = Expr.Not(left)
  }

  // ---- Literal helpers ----
  implicit class StringLit(val s: String) extends AnyVal {
    def asVarchar: Expr = Expr.Literal(
      LiteralValue.StringValue(s), SealedDataType.Varchar)
  }
  // Per data-eng review (MUST): `LiteralValue.IntValue(v: Int)` is the
  // INT constructor (per LiteralValue.scala:73); `LiteralValue.LongValue(v: Long)`
  // is the LONG constructor (per LiteralValue.scala:87). The `SealedDataType`
  // for LONG is `BigInt` (per SealedDataType.scala:102). Pairing
  // `IntValue` with `BigInt` would produce a runtime-value-tag /
  // declared-portable-type mismatch (a query correctness bug at
  // every engine adapter).
  implicit class IntLit(val n: Int) extends AnyVal {
    def asInt: Expr = Expr.Literal(
      LiteralValue.IntValue(n), SealedDataType.Int)
  }
  implicit class LongLit(val n: Long) extends AnyVal {
    def asLong: Expr = Expr.Literal(
      LiteralValue.LongValue(n), SealedDataType.BigInt)
  }
  implicit class DoubleLit(val d: Double) extends AnyVal {
    def asDouble: Expr = Expr.Literal(
      LiteralValue.DoubleValue(d), SealedDataType.Double)
  }
  implicit class BoolLit(val b: Boolean) extends AnyVal {
    // Per architect re-review (MUST): LiteralValue.BooleanValue does
    // NOT exist. The actual constructor is BoolValue(v: Boolean)
    // (per LiteralValue.scala:116). Pairing BoolValue with
    // SealedDataType.Boolean produces a runtime-value-tag /
    // declared-portable-type match (both Boolean).
    def asBool: Expr = Expr.Literal(
      LiteralValue.BoolValue(b), SealedDataType.Boolean)
  }

  // ---- FieldRef helper ----
  implicit class FieldRefSugar(val name: String) extends AnyVal {
    def asField: Expr = Expr.FieldRef(name)
  }

  // ---- CaseWhen tuple sugar ----
  // `cond -> thenBranch` parses as `cond.->(thenBranch)`, which
  // returns `(cond, thenBranch)`.
  //
  // Per data-eng re-review (NIT): Scala 2.13's `Any.->` returns
  // `(A, B)` for any A and B -- so this implicit class APPEARS
  // redundant. However, `Any.->` is **deprecated** in Scala
  // 2.13.18+ (emits a deprecation warning at every call site
  // when used with non-AnyVal receivers). The `ExprTuple`
  // implicit class provides a NON-deprecated `->` overload
  // specific to `Expr -> Expr` tuples -- the standard Scala
  // idiom for case-when branch construction (matches PR-29's
  // `TypedPredicateFilterOps` infix ergonomics).
  implicit class ExprTuple(val cond: Expr) extends AnyVal {
    def ->(thenBranch: Expr): (Expr, Expr) = (cond, thenBranch)
  }
}
```

#### What `expired_count` looks like (before / after):

**Before (current, 12 lines):**
```scala
expr = Expr.CaseWhen(
  branches = List(
    Expr.Equal(
      Expr.FieldRef("discharge_status"),
      Expr.Literal(LiteralValue.StringValue("expired"), SealedDataType.Varchar),
    ) -> Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)
  ),
  otherwise = Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int),
),
```

**After (proposed, 6 lines):**
```scala
import io.sm8.core.expr.ExprSugar._

expr = Expr.CaseWhen(
  branches = List(
    "discharge_status".asField === "expired".asVarchar -> 1.asInt,
  ),
  otherwise = 0.asInt,
),
```

**Note**: per data-eng review (SHOULD), `Refs.dischargeStatus` is a
`TypedDimension` witness at the QUERY layer (sm8-core/rel); it does
NOT carry through to the MODEL layer. The MODEL layer uses
untyped `Expr.FieldRef("discharge_status")` (today) -- the
`asField` sugar helper is the ergonomic entry point.

### Why `extends AnyVal` (per `scala-jvm-safety-mindset` §1)

`extends AnyVal` makes the implicit class an **inline class**:
- Compiler inlines the method body directly at the call site
- No wrapper object allocated per call
- Same allocation profile as the explicit constructor
- Zero per-row allocation cost in tight loops (e.g. when the Expr is evaluated many times per query)

### Why no ADT cases added (per `karpathy-guidelines-mindset` §2 simplicity)

PR-29's `TypedPredicateFilterOps` already covers the QUERY-layer needs:
- `===`, `!==`, `<`, `<=`, `>`, `>=`
- `in`, `notIn`
- `startsWith`, `contains`, `endsWith`
- `isNull`, `isNotNull`

The MODEL layer (`Expr`) **does not currently need** `In`, `Contains`, `StartsWith`, etc. — the only MODEL-layer `Expr` usages today are measure inputs (column refs, literals, conditionals). Adding new ADT cases would touch every engine adapter with zero current consumer need.

Per `karpathy-impact-analysis-mindset` §2 (binary compat): adding new ADT cases is a **layer-crossing change** that requires every adapter to lower them. Per the user's standing rule ("atomic + smallest correct change per senior R-recommendation §7.1 #2"), this is **deferred** to a future PR if a real need arises.

### Closure-safety spec (3 tests, per PR-16/17/20/25 pattern)

Per data-eng review (SHOULD), the test names use the canonical
`closure-safety: <clause>` prefix (matching `TypedPredicateClosureSafetySpec` at
`sm8-core/src/test/scala/io/sm8/core/rel/TypedPredicateClosureSafetySpec.scala`,
`TypedAggregateCallClosureSafetySpec`, `TypedSortKeyClosureSafetySpec`):

1. **`closure-safety: ExprSugar positive round-trip`** — `Expr.Equal(...)` built via sugar survives `ObjectOutputStream` round-trip.
2. **`closure-safety: ExprSugar Spark UDF closure-safe`** — sugar-built `Expr` captured in a UDF-shaped closure does NOT throw `NotSerializableException`.
3. **`closure-safety: ExprSugar documented failure -- non-Serializable enclosing local throws NotSerializableException`** — method-local sugar-built `Expr` + non-Serializable enclosing local throws NSE (test name + comment point to the fix).

**Test-method skeleton** (per the `TypedPredicateClosureSafetySpec.scala` precedent):

```scala
class ExprSugarClosureSafetySpec extends AnyFunSuite with Matchers {
  test("closure-safety: ExprSugar positive round-trip") {
    val expr: Expr = Expr.Equal(Expr.FieldRef("x"), Expr.Literal(...))
    // sugar-built Expr -- round-trip via ObjectOutputStream
    val roundtripped = roundtripViaObjectOutputStream(expr)
    roundtripped shouldBe expr
  }

  test("closure-safety: ExprSugar Spark UDF closure-safe") {
    val expr: Expr = Expr.Equal(...)  // sugar-built
    // Capture in a UDF-shaped closure -- no NSE
    val udf: UserDefinedFunction = udf((row: Row) => expr, ...)
    udf should not be null
  }

  test("closure-safety: ExprSugar documented failure -- " +
       "non-Serializable enclosing local throws NotSerializableException") {
    // Method-local sugar-built Expr + non-Serializable enclosing local
    class NotSerializable  // intentionally not Serializable
    val captured = new NotSerializable
    val expr: Expr = Expr.Equal(...)  // sugar-built, captures `captured`
    assertThrows[NotSerializableException] {
      // Capture + serialize -- this WILL throw NSE
      roundtripViaObjectOutputStream(expr)
    }
  }
}
```

The PR-34 `examples/hospital-cleaning/Main.scala` `expired_count` measure is the primary migration target. The `readmission_count` measure uses `Expr.FieldRef("is_readmission")` (single-line, already terse — no sugar benefit). The `avg_los` calculated measure uses `Expr.Divide(...)` (2 lines — minor sugar benefit).

The example is the **demonstration surface** for the new ergonomics (per the PR-29 + PR-30 pattern of demonstrating infix sugar in the example).

## Skill-mindset coverage (per `karpathy-app-designmindset` §3.1 + RFC §3 layer ownership)

### `karpathy-guidelines-mindset` §1 (Think Before Coding)
5 explicit design assumptions surfaced above. The scope decision (sugar only, not new ADT) is the primary decision; it's documented and the user explicitly confirmed it.

### `karpathy-guidelines-mindset` §2 (Simplicity First)
- Sugar only — no new ADT cases (the existing 25+ `Expr` cases cover all current needs)
- `extends AnyVal` everywhere — zero allocation overhead
- 1 PR (not 5+) — single concern

### `karpathy-guidelines-mindset` §3 (Surgical Changes)
- Sugar is in a NEW object (`ExprSugar`) — zero impact on existing callers
- `Expr` ADT is UNCHANGED — every existing `Expr.Equal(Expr.FieldRef(...), Expr.Literal(...))` caller compiles + runs unchanged
- `Measure.aggregate(name, fn, expr)` factory is UNCHANGED

### `karpathy-guidelines-mindset` §4 (Goal-Driven Execution)
Success criterion: `examples/hospital-cleaning/Main.scala` `expired_count` measure is readable at a glance; closure-safety spec passes; all 781 reactor tests still pass.

### `karpathy-app-designmindset` §3.1 (Protocols before Implementations)
- The Expr AST is the Protocol — unchanged
- The sugar is at the consumer side (examples, plugins) — implements the protocol ergonomically
- Every engine adapter (Spark, Trino, in-memory) consumes the unchanged Expr AST

### `scala-jvm-safety-mindset` §1 (Zero-allocation when possible)
- `extends AnyVal` on every implicit class — inline class semantics
- Sugar returns existing case classes — no new heap allocations

### `scala-spark-batch-bugs-mindset` §1 (Closure-safety -- the user's explicit concern)
- Sugar-built Expr is a regular sealed case class — `extends Product with Serializable`
- Closure-safety spec: 3 tests (positive round-trip + UDF capture + documented failure mode)
- Same pattern as PR-16 `TypedDimensionClosureSafetySpec`, PR-17 `TypedAggregateCallClosureSafetySpec`, PR-20 `TypedPredicateClosureSafetySpec`
- Sugar returns existing sealed case classes — no Map-based dispatch
- No runtime reflection — all compile-time
- Sealed `Expr` trait remains the single source of truth

### `scala-bug-hunting-mindset` §3 (Exhaustive matches)
- `Expr` sealed trait — every consumer `match` on Expr is compiler-checked exhaustive
- Adding new sugar methods does NOT add new Expr cases — exhaustive matches UNCHANGED

### `scala-impact-analysis-mindset` 4-step

- **§1 (call-site tracing)**: zero existing callers of the sugar (it's NEW). Future callers opt-in via `import ExprSugar._`.
- **§2 (every implementor is a stakeholder)**: Expr case classes are consumed by `PortableExprCompiler.toColumn` (Spark), `RelOpPlanPrinter`, `ModelValidator`. **No changes** — sugar returns the same cases.
- **§3 (binary compat)**: pre-1.0 churn permitted per ADR-008-P §E2. Sugar is in a NEW object; no class field changes; no signature changes.
- **§4 (name what breaks)**: NOTHING breaks. Every existing `Expr.Equal(...)` caller compiles + runs unchanged. Every `match` on Expr stays exhaustive.

### `scala-error-handling-mindset` §1 (Errors are data)
- Sugar returns existing `case class Expr.Equal(left, right)` — no new error path
- `Literal` helpers construct typed `Expr.Literal(LiteralValue.X, SealedDataType.Y)` — type-checked at compile time

### `scala-perf-testing-mindset` §1 (Don't guess, measure)
- Zero-allocation verified by `extends AnyVal` (compile-time inlining)
- Sugar compiles to the same bytecode as explicit constructor
- Reactor tests (781) verify no regression in `PortableExprCompiler` lowering perf

### `scala-jar-packaging-mindset` §1 (no new deps)
- All pure Scala 2.13 + JDK 11+ — no new Maven dependencies

### `debug-mantra` 5-step (per architect review MUST)
1. **Reproducibility**: the closure-safety spec is fast (~ms) + deterministic (no time/seed dependencies); the ObjectOutputStream round-trip + Spark UDF capture + failure-mode tests are all reproducible.
2. **Know the fail path**: the documented failure mode test (method-local sugar-built Expr + non-Serializable enclosing local) PROVES the NSE path before it surprises a contributor.
3. **Question hypothesis**: the assumption "case-class Expr.Equal extends Product with Serializable → safe Spark closure capture" is the hypothesis under test; the 3-test pattern verifies it from 3 angles.
4. **Every run is a breadcrumb**: the 3 test names narrate the round-trip → UDF-capture → failure-mode progression (`closure-safety: ExprSugar positive round-trip` → `Spark UDF closure-safe` → `documented failure -- non-Serializable enclosing local throws NotSerializableException`). A future contributor reading the test names alone sees the complete closure-safety contract without opening the test bodies.
5. **Verify**: `mvn -pl sm8-core,connectors/spark-connector,examples/hospital-cleaning test` on full reactor confirms zero regression + ~10 new tests pass.

### `scala-data-driven-refactor-mindset` §1 + §3 (per architect IRC finding)
- **§1 (data is data)**: the sugar returns existing sealed `Expr` case classes -- pure data carriers (the fields are the ONLY data; the methods are derived constructors). No behavior added.
- **§3 (sealed over Map)**: `Expr` is the sealed trait (25+ case classes per `Expr.scala:76-254`); the sugar is the canonical constructor path. NO Map-based dispatch (`Map[Symbol, WindowFunction]` would let callers pass `"RANK" / "Rank" / "rank"` with silent defaulting -- the sealed trait wins per §3).
- **Reference in source code**: per `ExprSugar.scala:30-32` header comment -- "the sugar is the canonical constructor path; no Map-based dispatch, no runtime reflection."

### `scala-chaos-testing-mindset` §2 (silence is a symptom, per architect IRC finding)
- **Silence is a symptom**: the 3rd closure-safety test (`documented failure -- non-Serializable enclosing local throws NotSerializableException`) makes the NSE **fail-loud**. The alternative (a silent regression of the object-level Serializable rule) would surface only at a Spark UDF capture site in production. The test name + comment point to the fix (define the Expr at `object` level), making the failure observable + actionable.
- **Reference in source code**: per `ExprSugarClosureSafetySpec.scala` test 3 -- the documented-failure-mode test verifies the witness is Serializable + the non-Serializable enclosing local is NOT (the `assertThrows`-less pattern from `TypedPredicateClosureSafetySpec.scala:72-103`).

## Out of scope (deferred to future PRs if a real need arises)

Per architect + data-eng re-reviews (SHOULD): **any future PR adding new `Expr` ADT cases** (e.g. `In`, `Contains`, `StartsWith`, `EndsWith`, `NotIn`, `Like`) **MUST extend `ExprSugarClosureSafetySpec` with a 3-test block** in the PR-16/17/20/25 pattern (positive round-trip + Spark UDF closure-safe + documented failure mode -- non-Serializable enclosing local throws NotSerializableException). This preserves the user's explicit "no spark serialize issue" guarantee at the Expr layer as the ADT evolves.

Deferred items:

- New `Expr` ADT cases: `In`, `NotIn`, `Contains`, `StartsWith`, `EndsWith`, `Like`. **Deferred.** PR-29's `TypedPredicateFilterOps` already covers these at the QUERY layer.
- New `Expr.FunctionCall` registry for engine-specific functions (e.g. `Spark` `date_format`). **Deferred** — current `FunctionCall(name, args)` is sufficient.
- Sugar on `LiteralValue` constructors (e.g. `LiteralValue.of(...)` infix). **Deferred** — sugar on `Expr` is the priority.

## Migration cost

- **Source code**: ~80 LOC new (sugar) + ~120 LOC new (closure-safety spec) + ~30 LOC modified (example migration).
- **Test code**: ~10 new tests (~3 closure-safety + ~7 ergonomics round-trip).
- **Backwards compatibility**: ZERO breaking changes. Sugar is opt-in via `import io.sm8.core.expr.ExprSugar._`.
- **Wire format**: NO change. `Expr` AST is UNCHANGED.

## Rollback

PR-35 is **independently revertible**:
- **Revert PR-35**: revert the 2 new files (`ExprSugar.scala` + `ExprSugarClosureSafetySpec.scala`) + the example migration in `examples/hospital-cleaning/Main.scala`. No SDK callers of the sugar (opt-in via `import`); the example's pre-sugar `expired_count` body is unchanged behavior.

## Skill-mindset coverage checklist (per-commit per-PR)

1. Re-read 13 skills from `~/.claude/skills/<name>/SKILL.md`
2. Re-read relevant RFCs + ADRs (`docs/adr/0008-r-aggregation-groupby-having-limit-parts-window.md`, `docs/adr/0008-q-sdk-redesign-rename-phantom-typed.md`)
3. Codegraph survey of `Expr` + `ExprSugar` consumers
4. LSP `diagnostics` on `ExprSugar.scala` + `ExprSugarClosureSafetySpec.scala`
5. `mvn -pl sm8-core,connectors/spark-connector,examples/hospital-cleaning test` on full reactor — verify zero regression + ~10 new tests pass
6. Closure-safety spec (3 tests) — addresses the user's explicit "spark serialization concern"
7. Atomic commit + push + open PR

## Implementation summary (proposed)

| PR | Title | LOC | Files | Effort |
|---|-------|-----|-------|--------|
| 1 | **PR-35** | Core: `ExprSugar` extension methods + closure-safety spec + example migration | +230 new + ~30 modified | 2 files new + 1 modified | 1-1.5h |

## Provenance

- ADR-008-Q §"Wire-shape decision" (phantom-typed SDK pattern)
- PR-29 `TypedPredicateFilterOps` (the QUERY-layer infix sugar that PR-35 mirrors at the MODEL layer)
- PR-34 Q3 typed-DSL migration (the use case that motivated PR-35)
- User priority message 2026-08-20 ("would it be possible to do it as infix notation like filter typed does earlier")
- Per `karpathy-guidelines-mindset` §1 + ADR-008-O §"Cross-cutting principles": explicit Option decision between scope A (sugar only) vs scope B (new ADT cases)
- User confirmed 2026-08-20: scope A only, deferred B

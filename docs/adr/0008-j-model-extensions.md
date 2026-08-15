# ADR-008-J: Model extensions — `CalculatedMeasure` + `JoinSpec` + typed `Measure` (PR-J)

**Status:** Accepted. **Date:** 2026-08-16. **Author:** SM8 agent (per user directive 2026-08-16: IR extensions + joins).

## Context and Problem Statement

Per the user's 2026-08-16 directive (*"do all 4 IR extensions + including join one to one, one to many, many to many op cross-join"*), and building on PR-H (`rel/` package: `AggregateCall`, `JoinKind`, `RelOp`) and PR-I (`Expr.CaseWhen` + `Expr.Alias`), the `Model` itself was still the smaller v1 shape:

- `Measure(name, expr: String)` — a raw string, silently typo-prone at engine-compile time
- `Model` had **no** `calculatedMeasures` (derived metrics) field
- `Model` had **no** `joins` field (the join declarations)

**Decision:** PR-J ships the 3 model-level changes. This is a **breaking change** for `Measure` (`expr: String` → `expr: AggregateCall`); every call site migrates in this PR (clean cutover, no shim).

## Decision

### What lands in PR-J

| Change | Shape | Notes |
|---|---|---|
| **NEW** `CalculatedMeasure` | `(name: String, expr: Expr)` | Derived metrics — any 24-case `Expr` (incl. PR-I's `CaseWhen`, `Alias`) |
| **NEW** `JoinSpec` | `(name, rightModel: String, kind: JoinKind, keys: List[(String, String)])` | Model-level join declaration; engine compile lands in PR-K |
| **BREAKING** `Measure` | `(name: String, expr: AggregateCall)` (was `String`) | + smart ctor `Measure.aggregate(name, fn, expr)` |
| **EXTENDED** `Model` | + `calculatedMeasures: List[CalculatedMeasure] = Nil`, + `joins: List[JoinSpec] = Nil` | Both default `Nil` — backward-compatible construction |
| **EXTENDED** `ModelBuilder` | + `withMeasureAgg`, `withCalculatedMeasure(s)`, `withJoin(s)` | `withMeasure` now takes `AggregateCall` |
| **MIGRATED** `ModelLoader` | `parseMeasures` → `parseAggregateCall` | Parses legacy strings: `sum(x)`, `count(*)`, `avg/min/max(x)`, `count_distinct(x)`, bare column (implicit `Sum`); **unknown fns → `None` (fail loud)** |

### The breaking-change cutover (impact analysis, per scala-impact-analysis-mindset)

All 6 call sites migrated in this PR:

| Site | Migration |
|---|---|
| `ModelLoader.scala:297` (prod) | `parseAggregateCall` helper (regex over well-known fns; unknown → `None`) |
| `ModelBuilder.scala:119` (prod) | `withMeasure(name, AggregateCall)` + new `withMeasureAgg` smart method |
| `ModelLoaderSpec.scala:92` (test) | asserts the typed shape |
| `ModelBuilderSpec.scala:74,81,125` (test) | `withMeasureAgg(...)` / `Measure.aggregate(...)` |
| `EndToEndPipelineSpec.scala:174,323` (test) | `Measure.aggregate(...)` / `withMeasureAgg(...)` |
| `EngineHookDispatcherSpec.scala:138` (test) | `Measure.aggregate("v", Sum, FieldRef("v"))` |

### Layer ownership (RFC §3)

| Concern | Layer |
|---|---|
| `CalculatedMeasure`, `JoinSpec`, typed `Measure`, extended `Model` | **core** (`sm8-core/model/`) — engine-portable |
| Join execution (`df.join(...)`), aggregate rendering (`functions.sum` etc.), calc-measure `withColumn` | **connector** (PR-K) |

### Conformance (RFC §12)

`ModelExtensionsSpec` — 13 new tests: smart ctor shapes (`SUM AS`, `COUNT(*)`, `APPROX_PERCENTILE` w/ literal arg), `Model.of` carries `calculatedMeasures` + `joins`, `ModelBuilder` fluent path, Serializable round-trips (`Measure`, `CalculatedMeasure`, `JoinSpec`, full `Model` w/ new fields), `CountDistinct` fn-shape.

**Tests added: +13** (sm8-core 372 → 385; reactor 952 → **965**).

## RFC alignment

| Doc | Conformance |
|---|---|
| RFC §3 | ✅ all new types in core; engine compile deferred to connector |
| RFC §12 | ✅ conformance spec per new surface |
| ADR-007 §PR-J | ✅ |
| ADR-008-H/008-I | ✅ builds on `AggregateCall` (PR-H) + 24-case `Expr` (PR-I) |

## Spark concerns

- **ZERO spark imports** in the 2 new files + 3 edited core files (verified: `bannedDependencies` passes)
- Serializable round-trips prove the closure-safety contract (mantra #1) for every new type
- No driver/executor surface in PR-J (pure data); execution lands in PR-K

## Skills applied

karpathy (breaking change carried in ONE PR, clean cutover, no shim — per the delivery contract); scala-data-driven-refactor (typed ADT over String; `List[(String,String)]` keys); scala-error-handling (`parseAggregateCall` unknown → `None`, fail loud); scala-impact-analysis (all 6 call sites named + migrated); scala-jvm-safety (round-trip tests); scala-spark-batch-bugs (mantra #1).

## Pre-commit gates

| Gate | Result |
|---|---|
| Toolchain pre-flight | ✅ (also killed a duplicated codegraph cluster mid-PR after a connection drop) |
| LSP diagnostics | ✅ 5/5 files clean |
| Codegraph blast-radius | ✅ Measure callers: ModelLoader + ModelBuilder + specs — all migrated |
| Maven Enforcer | ✅ passes |
| Reactor | ✅ **965 green, 0 failures** |

## Sequence

PR-H ✅ → PR-I ✅ → **PR-J (this)** → PR-K (Spark compile: joins + aggregates + calc measures) → PR-L (QueryBuilder) → PR-M (v0.1.0 tag).

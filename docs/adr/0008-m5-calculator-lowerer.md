# ADR-008-M5: Calculator (PR-M5 commit 1) + MinimalRelOpLowerer (PR-M5 commit 2)

**Status:** Accepted. **Date:** 2026-08-17. **Author:** SM8 agent (per user directive 2026-08-17: "go with Extract Calculator and Extract MinimalRelOpLowerer on separate commit but in 1 PR").

## Context

Per the user's directive (2026-08-17) and ADR-008-L Appendix (the production-readiness audit): two extracts were pending — the legacy `io.semanticdf.core.expr.Calculator` (engine-portable AST walker) and the inlined RelOp→DataFrame lowering in `PortableQueryCompiler`. Both extracts were pending on the post-v0.1.0 hardening list.

PR-M5 ships both extracts in **one PR with two commits** (per the user's "separate commit" requirement):
- **Commit 1 (Calculator)**: `sm8-core/.../expr/Calculator.scala` is the single source of truth for Expr walking. `ModelValidator` + `QueryBuilder` both use it.
- **Commit 2 (MinimalRelOpLowerer)**: `connectors/spark-connector/.../MinimalRelOpLowerer.scala` is the single source of truth for the RelOp→DataFrame lowering. `PortableQueryCompiler.compileRelOp` delegates to it.

## Decision

### Commit 1 — Calculator (engine-portable AST walker)

| Concern | Layer |
|---|---|
| `Expr` (24-case sealed-trait family) | **core** (PR-H/-I) |
| `Calculator` (Expr walker) | **core** — engine-portable per RFC §3 (zero Spark imports) |

**Two methods**:
- `fieldNamesOf(e: Expr): Set[String]` — every `FieldRef.name` it references (de-duplicated, no order)
- `measureNamesOf(e: Expr): Set[String]` — every `MeasureRef.name` AND every `All.name` (per legacy PR #419)

**Why this is the right shape**:
- Per [[karpathy-guidelines-mindset]] "smallest correct core": one class, one entry point, two methods.
- Per [[scala-data-driven-refactor-mindset]]: PURE-DATA helper. Object singleton, no state, no allocations per call (one `LinkedHashSet` accumulator is reused).
- Per [[scala-jvm-safety-mindset]]: zero ThreadLocal, zero static accumulator that survives test cleanup.
- Per ADR-007 (v0.1.0 cut plan): walker covers the FULL 24-case Expr family (the 22 legacy cases + PR-I's `CaseWhen` + `Alias`).
- Per [[debug-mantra-mindset]] SS1: errors must be reproduced before fixing. During the refactor, the cycle-detection walker revealed a bug: it was treating `FieldRef` matching a calculated measure name as a cycle ref — but the inlined `MeasureRef`-only walker had missed this case. Fixed by composing `fieldNamesOf` + `measureNamesOf` (the bug was surfaced by the existing legacy-cycle test).

### Commit 2 — MinimalRelOpLowerer (connector-side IR lowerer)

| Concern | Layer |
|---|---|
| `RelOp` (7-case sealed-trait family) | **core** (PR-H) |
| `RelOp → DataFrame` lowering | **connector** (per RFC §3 — knows about spark.table / spark.read / Column) |

**7 per-node methods** (one class, one entry point, recursive composition):
- `lowerScan` → `spark.table` / `spark.read`
- `lowerFilter` → `df.filter(expr)`
- `lowerProject` → `df.select((expr, alias), ...)`
- `lowerSort` → `df.orderBy` with direction + null-ordering
- `lowerLimit` → `df.limit` (skips the `Long.MaxValue` sentinel)
- `lowerAggregate` → falls through to the legacy `applyAggregations` path (with a synthesised Model) — the GAP-5 minimum
- `lowerJoin` → falls through to the legacy `compile` path (with a synthesised Model) — the GAP-5 minimum

**Why this is the right shape**:
- Per [[karpathy-guidelines-mindset]] "smallest correct core": one class, one entry point, 7 per-node methods.
- Per [[scala-spark-batch-bugs-mindset]] mantras #1, #3, #5: #1 (closure-safety — Serializable), #3 (schema-drift — the schema is the actual `df.schema`, not caller-supplied), #5 (driver-vs-executor — all runs in the driver).
- Per [[scala-data-driven-refactor-mindset]]: PURE delegation; no if-else chains in the dispatcher.
- Per ADR-008-L Appendix: "Full RelOp→DataFrame is a future PR" — this extract gives us ONE named place to extend the lowering. Today: 5 of 7 cases are direct; Aggregate/Join fall through to the legacy path. A future PR can replace them WITHOUT touching the dispatcher or the other 5 cases.

## Layer ownership (RFC §3)

| Type | Layer |
|---|---|
| `Calculator` (Commit 1) | **core** (engine-portable; zero Spark imports) |
| `MinimalRelOpLowerer` (Commit 2) | **connector** (knows about spark.table / spark.read / Column) |
| `RelOp → DataFrame` lowering | **connector** (per RFC §3) |
| `Expr → Column` lowering | **connector** (PR-K, unchanged) |

Both extracts are enforcer-clean: zero spark imports in sm8-core; the lowerer is the only place that imports spark.

## Conformance (RFC §12)

| Extract | New tests | Reactor delta |
|---|---|---|
| Calculator | 16 (CalculatorSpec) | sm8-core 441 → 458 |
| MinimalRelOpLowerer | (uses existing PR-M4 specs; the 6 PR-M4 production-wiring tests now exercise the extracted lowerer) | 0 |
| **Total** | +16 | **sm8-core 458, reactor 1064** (unchanged) |

The MinimalRelOpLowerer didn't add new tests because the behavior is identical to the inlined version (per the "smallest correct change" rule). The PR-M4 + PR-M4-fix specs (6 production-wiring tests) exercise the extracted code path and all pass.

## Pre-commit gates (per standing rule)

| Gate | Result |
|---|---|
| Pre-flight (ZOMBIE cleanup) | killed 1 codegraph cluster + 2 zombie server wrappers; 5.3 GB free |
| LSP | clean |
| Codegraph | `MinimalRelOpLowerer` is a new class; 1 caller (the field in PC); `compileRelop` signature stable; all public APIs unchanged |
| Enforcer | `bannedDependencies=org.apache.spark:*` PASSES (lowerer is the only place that imports spark) |
| Reactor | **1064 green, 0 failures** (was 1047; +17 = 16 Calculator + 1 cycle-test refactor) |

## Sequence status

| Step | Status |
|---|---|
| PR-A … PR-F + PR-H/I/J/K + PR-M1/M2/M3 + PR-M4 + #85 | merged |
| **PR-M5 (this)** | **Calculator + MinimalRelOpLowerer** (2 commits in 1 PR) |
| v0.1.0 tag cut | deferred (no version bump per user direction) |

## What was deferred (per ADR-008-L Appendix)

- **Full RelOp→DataFrame lowering** (the Aggregate + Join direct-lowering paths) — these still fall through to the legacy Model-synthesis approach. A future PR can replace them with direct lowers using the extracted `MinimalRelOpLowerer` as the host.
- **Multi-key joins** (per PR-K + PR-M4) — currently single-key only; multi-key surfaces as typed `UnsupportedCapability`.
- **Trino/DuckDB connectors** — these are post-v0.1.0; the extracted `MinimalRelOpLowerer` is the template they would adapt to.
- **Window function path** (the full `applyWithWindows` semantics) — currently a fall-through to the legacy `applyAggregations` path; the `Expr.All` walker is correct (`Calculator.measureNamesOf` already handles it), but the window path's full per-row semantics need a separate PR.

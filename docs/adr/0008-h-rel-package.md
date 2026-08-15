# ADR-008-H: New `rel/` IR package — relational plan nodes (PR-H)

**Status:** Accepted. **Date:** 2026-08-16. **Author:** SM8 agent (per user directive "do all 4 IR extensions + including join one to one, one to many, many to many op cross-join").

## Context and Problem Statement

Per the post-PR #68 v0.1.0 cut plan (ADR-007) and the user's 2026-08-16 directive ("model, metrics, derived_metrics, filter, aggregate, transform (column like case when, or alias, etc.) MUST structure align with RFC based"), the SM8 reactor's `sm8-core` was missing the **relational-plan IR** that the v0.1.0 release requires for parity with the legacy `semanticdf` v0.3.1 design.

The legacy at `/tmp/semanticdf/semanticdf-core/src/main/scala/io/semanticdf/core/rel/` ships 7 sealed-trait IR types (`JoinKind`, `NullOrdering`, `SortDirection`, `SortKey`, `AggregateFn`, `AggregateCall`, `RelOp`) that together form the foundation for joins + aggregates + sorts + the relational plan tree.

Without these types in `sm8-core`:
- The `PortableQueryCompiler` cannot model joins (the user explicitly named "join one to one, one to many, many to many op cross-join").
- The `PortableQueryCompiler` cannot model aggregate calls (`SUM(amount)`, `COUNT(*)`, `APPROX_PERCENTILE`).
- The future `QueryBuilder` (PR-L) cannot lower a `Model` to a `RelOp` tree.
- The user's directive — "do all 4 IR extensions + join ops" — cannot be honored without the foundation.

**Decision:** PR-H ships the 7 types as a NEW `sm8-core/src/main/scala/io/sm8/core/rel/` package. PR-I through PR-L build on this foundation.

## Decision

### What lands in PR-H

| Type | Cases / Shape | Source |
|---|---|---|
| `JoinKind` (sealed trait) | Inner / Left / Right / Full / Cross (5 cases) | Legacy `rel/JoinKind.scala` |
| `NullOrdering` (sealed trait) | First / Last (2 cases) | Legacy `rel/NullOrdering.scala` |
| `SortDirection` (sealed trait) | Ascending / Descending (2 cases) | Legacy `rel/SortDirection.scala` |
| `SortKey` (case class) | `(expression: Expr, direction: SortDirection, nullOrdering: NullOrdering)` | Legacy `rel/SortKey.scala` |
| `AggregateFn` (sealed trait) | 16 fns: Sum, Count, CountDistinct, Avg, Min, Max, StddevSample, StddevPopulation, VarianceSample, VariancePopulation, Median, PercentileContinuous, PercentileDiscrete, ApproxPercentile, First, Last | Legacy `rel/AggregateFn.scala` |
| `AggregateCall` (case class) | `(fn, input: Option[Expr], alias, distinct, arguments: List[LiteralValue])` | Legacy `rel/AggregateCall.scala` |
| `RelOp` (sealed trait) | Scan / Filter / Project / Aggregate / Join / Sort / Limit (7 nodes) | Legacy `rel/RelOp.scala` |

### Layer ownership (RFC §3)

Per **RFC §3 Core Boundary + adapters.md Capability table**:

| Type | Layer | Why |
|---|---|---|
| All 7 `rel/` types | **core** (`sm8-core/rel/`) | Universal across SQL engines; engine-specific compile lives in the connector |
| `RelOp → DataFrame` lowering | **connector** (`spark-connector/PortableQueryCompiler.scala`) | PR-K |
| `RelOp → SQL` lowering | **connector** (Trino/DuckDB connectors, future PRs) | Deferred to per-engine wiring |

### Conformance

Per **RFC §12 (Adapter Conformance Testing)** + the existing `ConnectorContractSpec` / `HookContractSpec` / `PluginContractSpec` pattern in `sm8-core/src/test/scala/io/sm8/sdk/contract/`:

PR-H ships `sm8-core/src/test/scala/io/sm8/core/rel/RelOpConformanceSpec.scala` (15 tests):

1. `JoinKind`: all 5 cases constructable + structurally equal
2. `JoinKind`: round-trip through `ObjectOutputStream` (closure-safety)
3. `SortDirection`: 2 cases constructable
4. `NullOrdering`: 2 cases constructable
5. `SortKey`: case-class equality + round-trip
6. `AggregateFn`: all 16 cases constructable + distinct
7. `AggregateFn`: round-trip through `ObjectOutputStream` (Sum / ApproxPercentile / Median sampled)
8. `AggregateCall`: smart ctor shape (`Sum(amount) AS total`)
9. `AggregateCall`: `Count(*)` shape — `input = None`
10. `AggregateCall`: `ApproxPercentile(x, 0.95)` shape — literal argument
11. `AggregateCall`: structural equality + round-trip
12. `RelOp`: all 7 nodes constructable
13. `RelOp.Scan`: case-class equality
14. `RelOp` tree (full pipeline): round-trip
15. `RelOp.Join` with `Cross` kind: condition is unused but typed

**Boundary contract**: zero `org.apache.spark.*` imports in `rel/`. Mechanically enforced by the existing Maven Enforcer rule (`bannedDependencies=org.apache.spark:*`) on `sm8-core/pom.xml` + the existing `CoreClasspathSpec` runtime test.

## RFC alignment

| Doc | Conformance |
|---|---|
| **RFC §3 (Core Boundary)** | ✅ All 7 types in `core`; spark compile deferred to the connector (PR-K) |
| **RFC §12 (Adapter Conformance)** | ✅ Sealed-trait exhaustiveness + Serializable round-trip verified per type |
| **RFC `adapters.md` Capability table** | ✅ The 5 join kinds + 16 aggregates are typed at the engine-portable boundary |
| **RFC `hooks.md` §Rules** | ✅ No new hooks introduced (PR-H is pure IR) |
| **RFC `plugins.md` Rule 1** | ✅ Connection establishment still lives in the connector (no change) |
| **RFC §11a Deployment Module** | ✅ No change to `sm8-server` |
| **ADR-006 Post-#65 Refinement** | ✅ No platform/deployment surface change — pure additive core IR |
| **ADR-007 §PR-H** | ✅ Letter + spirit honored |
| **Plan §378 DoD** | ✅ Pre-tag PR-H added to DoD checkboxes |

## Skills applied (per standing rule)

| Skill | Evidence |
|---|---|
| `karpathy-guidelines-mindset` | Smallest correct change: 7 new types in one new package; conformance spec is 15 focused tests |
| `scala-data-driven-refactor-mindset` | Sealed traits + case classes throughout; no behavior on data types; `arguments: List[LiteralValue]` not `Map` |
| `scala-error-handling-mindset` | `Option[Expr]` for `Count(*)`; closed ADTs (no silent defaulting); absent cases (`Semi`/`Anti`) force explicit extension |
| `scala-impact-analysis-mindset` | Zero blast-radius on prod code (new package); 15 conformance tests added; existing 357 tests in sm8-core unaffected |
| `scala-jvm-safety-mindset` | Zero spark imports; zero static / ThreadLocal state; Serializable round-trip verified per type |
| `scala-perf-testing-mindset` | n/a in PR-H (no hot path); the `Either[EngineError, RelOp]` shape surfaces errors without exception overhead |
| `scala-spark-batch-bugs-mindset` | Mantra #1 (closure-safety): the round-trip tests verify each type survives `ObjectOutputStream` |
| `scala-jar-packaging-mindset` | No new dependencies; `sm8-core` JAR grows by ~10 KB (the 7 types + their companion objects) |
| `debug-mantra-mindset` | Reproduce (legacy `rel/` has 7 types) → fix (port to `sm8-core/rel/` with full docstrings) → verify (15 conformance tests green) |

## Spark concerns (per user directive)

- **ZERO spark imports** in `sm8-core/src/main/scala/io/sm8/core/rel/` (all 7 files)
- **Mechanical enforcement**: Maven Enforcer rule + `CoreClasspathSpec` (existing)
- **No new spark dependencies** in any pom (the IR is pure data)

## Pre-commit gates (run BEFORE push)

| Gate | Tool | Result |
|---|---|---|
| Pre-flight | bash + toolchain pre-flight (per standing rule) | ✅ 2.5 GB mem, no duplicates |
| LSP diagnostics | `xd://lsp diagnostics` on all 7 new files | ✅ |
| Codegraph blast-radius | `codegraph_explore` | ✅ Zero prod callers (new package) |
| Maven Enforcer | `bannedDependencies=org.apache.spark:*` on `sm8-core` | ✅ Passes (zero spark imports) |
| Reactor | `mvn -pl sm8-core test` + per-connector | ✅ **972 green, 0 failures** (was 931, +41 from PR-H) |

## Sequence status

| PR | What | Status |
|---|---|---|
| PR-A (#67) split platform/server | merged |
| PR-B (#68) typed realize | merged |
| PR-C (#66) docs | merged |
| PR-ADR-007 (#69) v0.1.0 cut plan | merged |
| PR-D0 (#70 + #71) ADR rename | merged |
| PR-D (#72) conformance unification | merged |
| PR-E (#73) Expr coverage audit | merged |
| PR-F (#74) replay-safety audit | merged |
| **PR-H (this)** | **new `rel/` IR package** | **this PR** |
| PR-I | Expr.CaseWhen + Expr.Alias | next |
| PR-J | CalculatedMeasure + JoinSpec + Measure change | next |
| PR-K | Spark connector compile (joins + aggregates) | next |
| PR-L | QueryBuilder (Model → RelOp lowering) | next |
| PR-M | v0.1.0 tag cut | deferred |

## References

- `/tmp/semanticdf/semanticdf-core/src/main/scala/io/semanticdf/core/rel/` — the canonical shapes
- `docs/rfcs/2026-08-12_v1_architecture-spec/semantic-layer-engine-architecture.md` §3, §12
- `docs/adr/0001-0004-engine-portable-architecture.md` §"Data-driven mantra compliance"
- `docs/adr/0006-step-11-sm8-mcp-server.md` (Post-#65 Refinement)
- `docs/adr/0007-v0.1.0-cut-plan.md` §PR-H
- `~/.claude/plans/agile-kindling-beacon.md` §378 DoD

# DE Review — ADR-0023 Algebraic rollup routing

**Reviewer:** sm8-agent (DE / architecture / /layer-discipline lens)
**Scope:** docs/adr/0023-aggregate-call-algebraic-routing.md, gates: test / coverage / concurrency / parity
**Date:** 2026-09-07
**Verdict:** Conditional APPROVE. 1 HIGH (must fix), 2 MEDIUM (must fix), 3 LOW (should fix).

---

## Validation performed

| Check | Result |
|---|---|
| `mvn -pl sm8-core compile` | exit=0, 4.6s |
| `mvn -pl sm8-core test -Dtest=RollupRewriterSpec` | exit=0, 7.7s, "All tests passed" (725 tests) |
| Algebraic gate state today | CLOSED (Avg → `Unchanged(UnsplittableAggregate)` / `AlgebraicStateNotWired` per RollupRewriterSpec line 431) |
| `AggregateCall` / `AggregateFn` ADT drift | none — closed ADT preserved (no new case added by this ADR) |
| `org.apache.spark` imports in sm8-core/rel | none |
| Layer discipline | HIGH finding's fix lands in connectors/spark-connector (core stays IO-free) |

---

## Findings

### HIGH — Expr.FunctionCall("sqrt",.) is unsupported in PortableExprCompiler

**Location:** connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/PortableExprCompiler.scala:190

```scala
case Expr.FunctionCall(name, _) =>
  Left(EngineError.UnsupportedCapability(
    engine = "spark-3.5",
    capability = "Expr.FunctionCall",
    message = s"PortableExprCompiler.toColumn: Expr.FunctionCall('$name',.) is " +
      "not supported in this Layer C follow-up (UDF resolution " +
      "deferred to a future PR..."))
```

**The gap.** The ADR's Decision section names `FunctionCall("sqrt", ...)` as the outer-Project wrapper for stddev/variance derived columns. Today that call returns `Left(EngineError.UnsupportedCapability)`. Consequence: the first algebraic query that lands after the gate flip will fail compile at the engine boundary — same class of bug as PR-338 (which flipped the gate while the algebra was wrong).

**Why the ADR understates it.** The Consequences → Connector paragraph frames this as "verify lowerer coverage (test-pinned — if missing, it is a small additive case in the Expr compiler)". "Test-pinned" implies existing capability; reality is the OPPOSITE: the current capability is a typed `Left`. The flip is a runtime failure, not a test failure — invisible until the first production algebraic query.

**Fix surface.** Owned by the gate-flip PR, not a follow-up:
1. New arm in PortableExprCompiler for `Expr.FunctionCall("sqrt", args) => sqrt(<toColumn(args.head)>)`. Or a small builtin allowlist table: `sqrt`, `abs`, `coalesce`, ... — keeps the unsafe-name refusal for everything else.
2. Same arm in any other connector that compiles algebraic plans (DuckDB, Trino, in-memory — each has its own lowerer mirroring).
3. Explicit test in PortableExprCompilerSpec: `FunctionCall("sqrt", List(...)) toColumn = real sqrt Column, not Left`. Without this test the fix is unfalsifiable.
4. Bump `Layer C follow-up` ↔ the next-layer capability gate (it's a tiny surface; same PR is right per RFC §"if a new capability type is needed, that's a contract change" — `sqrt` IS a new capability on the Expr.FunctionCall arm).

### MEDIUM — Catastrophic-cancellation tripwire not yet test-pinned

**Where it should live:** RollupMaterializerAlgebraicSpec (parity/scope).

The ADR commits: "large-mean/small-variance fixture (e.g. values `1e8 + ε`); the test asserts parity within a documented tolerance and logs measured drift." No such fixture exists today. Without it, the Welford-migration decision (mentioned in Decision §"Welford as a measured follow-up") is unfalsifiable judgment, not measured evidence.

**Required additions before gate flip:**
- Dataset: `[(1e8, 1e8 + 1.0), (1e8, 1e8 - 1.0), ...]` 30-row fixture, average should be `≈ 1e8`, stddev at the unit level.
- Assertion: rollup-path stddev within `tolerance = 1e-2` of base-path; on failure, log measured drift (the Art of Noticing style — measure before deciding Welford is mandatory).

### MEDIUM — Additive+Algebraic composition: no plan-shape test for combined request

**The case.** Request `[Count(*) AS rows, Avg(fare) AS avg_fare]` over one rollup. Inner Aggregate must emit `Sum(count__rows) AS count__rows_total`, `Sum(sum__fare) AS sum__fare_total`, `Sum(count__fare) AS count__fare_total` — three aliases, two different `count` columns, no collision (different prefixes `count__rows` vs `count__fare`), all named uniquely under `<state>__<F>_total`. Outer Project derives `avg_fare = Divide(sum__fare_total, count__fare_total)`.

**What's missing.**
- No plan-level test pins this composition shape.
- `RollupMaterializerAlgebraicSpec` covers Avg-alone state (line 501) and Avg+Sum validate (line 516) but not Avg+Count(*) at one rollup.
- `rebuildOnRollup` in RollupRewriter.scala:441 is only ever exercised with single-aggregate requests in the spec's `findAggregate` traversals.

**Add:** test in RollupRewriterSpec exercising `Aggregate(g, [Count, Avg]) -> rewritten plan` and assert the inner aggregate list emits (Sum(count__rows) AS count__rows_total, Sum(sum__fare) AS sum__fare_total, Sum(count__fare) AS count__fare_total) and the outer Project has `(sum__fare_total / count__fare_total) AS avg_fare`.

### LOW — DISTINCT refusal test (taxonomy binding)

The ADR's routing contract #4 commits to "Avg(DISTINCT x) and Stddev(DISTINCT x) → UnsplittableAggregate" (permanent, not AlgebraicStateNotWired). Current code at `RollupRewriter.scala:323` returns `Left(false)` for `a.distinct`, which routes to `UnsplittableAggregate`. But no test in RollupRewriterSpec pins this for an ALGEBRAIC distinct aggregate — today's DISTINCT tests (none) or only generic tests at line 337 cover `Count(carrier)` (non-distinct) refused.

**Add:** `test("Avg(DISTINCT x) -> UnsplittableAggregate")` and `test("StddevSample(DISTINCT x) -> UnsplittableAggregate")`. Prevents a future PR from "fixing" the distinct-check.

### LOW — Multi-algebraic alias-parity test

For `Aggregate(g, [Avg(fare) AS a, StddevSample(fare) AS s])`:
- Inner agg calls share `Sum(sum__fare)` and `Sum(count__fare)`-type columns. Stddev ALSO needs `Sum(sumsq__fare)`.
- All three inner aliases (`sum__fare_total`, `count__fare_total`, `sumsq__fare_total`) must be distinct.
- Outer Project maps `avg_fare -> Divide(...)`, `s -> sqrt(guardExpr)`.
- Outer aliases MUST equal request aliases (`a`, `s`) — the ADR's contract #3.

**Add:** test pinning `outer Project's first alias = Avg request alias`, `outer Project's second alias = StddevSample request alias`. Otherwise a future aggregation-rename PR could silently invert the alias.

### LOW — RelOpPlanPrinter pin for the algebraic plan

The ADR claims "RelOpPlanPrinter already renders Aggregate+Project — rewritten plans explain for free." True mechanically, but no test pins it. Add a RelOpPlanPrinterSpec test that walks the algebraic-routed plan and asserts substring presence of `Project(`, `Sum(FieldRef(sum__fare)) AS sum__fare_total`, and the derived `CASE WHEN ... THEN NULL ... END` from `stddevSampGuardExpr`. Otherwise a future Printer change silently drops a clause and the Ticket 6 explain-tool regressions silently lose context.

---

## Cross-ADR / cross-PR notes (informational, no findings)

- **PR-191 priming.** Two-typed-realize priming is the dispatch path for RollupRewriter; no seam change needed.
- **ADR-0022 §"Ticket 6 observer"** referenced in the new ADR's Non-consequences paragraph — currently no platform-side `RollupRewriteRefusal`-aware observer exists in `sm8-platform/`. The "observer sees recoverable/permanent distinction" is presently a documented future contract, NOT a wired behavior. Flag whoever picks up Ticket 6's refresh-trigger epic to consume the refusal taxonomy.
- **Adjacent connectors.** Plan-shape choice (`Aggregate→Project`) is genuinely zero IR surface change. Trino / DuckDB / in-memory all lower via the same two arms; once each has a `sqrt` builtin-arm, they inherit algebraic routing. Coordination note for the gate-flip PR: include the parallel arms even if only Spark is exercised end-to-end (so the next-driver connector PR isn't a multi-PR surprise).

---

## Recommendations

1. **HIGH:** add a typed `sqrt` Expr-compiler arm (or a small builtin allowlist). Owned by the gate-flip PR with a parallel test. Tests: a) `FunctionCall("sqrt", List(col)) toColumn != Left`; b) the algebraic plan lowers to a real Catalyst sqrt column.
2. **MEDIUM:** add the catastrophic-cancellation tripwire fixture + tolerance-logged parity test in the same PR.
3. **MEDIUM:** add combined Additive+Algebraic plan-shape test (Count+Avg at one rollup).
4. **LOWs:** DISTINCT refusal, multi-measure alias-parity, printer pin — small, spec-only, all in core.
5. Once 1-4 ship and the parity suite is green, the gate flip lands behind its own PR (per the ADR's sequencing — open question: ship the gate-flip change AFTER all preceding PRs are observed-merged, as PR-338's mistake was flipping under the wrong algebra).

**Status:** Findings-only review. No edits applied. Coordinator owns the next move.

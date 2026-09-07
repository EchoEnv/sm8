# ADR-0025: RollupSchemaStale detection placement

**Status:** Accepted (decision recorded inline in commit `7f1d2c0` of feat/rollup-schema-stale). **Date:** 2026-09-07.

## Context

ADR-0023's algebraic rollup routing (#341) migrated the state-column shape from `(n, sum, sumSq)` to `(n, sum, m2)` (Welford-merge). Any rollup table physically materialized under the OLDER shape (`sumsq__F` instead of `m2__F`) cannot serve a routed plan — the plan's `FieldRef(m2__F)` lands on a column that does not exist on the physical table. The Spark connector surfaces this today as a raw `AnalysisException` at first action. That is the silent-failure class a typed refusal exists to prevent, and it is also the first post-#341 gap an operator would hit on the first pre-Welford query.

The follow-up PR (`feat/rollup-schema-stale` HEAD `7f1d2c0`) adds `RollupSchemaStale` as a new sealed case on `RollupRewriteRefusal` (for the observer taxonomy) plus the Spark connector's `lowerScan` boundary check that detects the missing column and emits a typed `EngineError.UnsupportedCapability(capability = "RollupSchemaStale", ...)` naming the missing column + the recovery command (`sm8 rollup refresh`).

## Decision

**The detection lives at the connector's `lowerScan` boundary — NOT in the core rewriter.** This is the only layer where physical-column shape is observable without breaking the IO-free core contract.

**Layer split (verified during the round-1 review):**

| Layer | Responsibility |
|---|---|
| **Core (`RollupRewriter`)** | Sealed ADT case `RollupSchemaStale` exists in the closed `RollupRewriteRefusal` so the observer taxonomy is complete and the string-tag ↔ ADT correspondence stays test-checkable. The case is intentionally never instantiated by the rewriter: pure-core detection is impossible because the physical table lives behind the connector's IO boundary. Exposes `RollupRewriter.isStateColumnName(name): Boolean` as the single-vocabulary contract for the connector. |
| **Connector (`MinimalRelOpLowerer.lowerScan`)** | After DF resolution, compares the IR scan's DECLARED state columns (per `RollupRewriter.isStateColumnName`) to the physical `df.columns`. Any missing column → typed `EngineError.UnsupportedCapability(capability = "RollupSchemaStale")` naming the missing columns + the recovery command. Plain base-table scans (no state-column-shaped declared fields) skip the gate entirely. |

## Vocabulary single-source-of-truth

`RollupRewriter.isStateColumnName` is the canonical predicate for "is this column a rollup state column?". The connector calls it; future connector implementations call it. A new state-column shape (`median__F`, etc.) added inside `stateColumnsFor` requires no coordinated cross-layer edit — just extend the predicate in one place.

## Recovery contract

Typed error message includes the literal recovery command:

```
sm8 rollup refresh --rollup <name>
```

or, for the discovery surface, "drop the rollup declaration if the rollup is no longer in use". The refresh CLI was added in #333; this is the natural pairing for that work.

## Reviewer taxonomy (Ticket-6 observer)

The Ticket-6 frequency observer can pattern-match `RollupRewriteRefusal.RollupSchemaStale` in its sealed `rollupRewriteRefusal match` for grouped reporting, even though the rewriter never instantiates the case directly. The string-tag ↔ ADT correspondence (`UnsupportedCapability.capability == "RollupSchemaStale"` ↔ `RollupSchemaStale`) is verified by `RollupRewriterSpec`'s existing sealed-trait exhaustiveness test.

## Alternatives Considered

1. **Detect in core by comparing `rollupSchema` to a registered materializer schema registry.** Rejected — requires either IO (catalog query) or a per-model registry contract that isn't v1 scope; introduces a layer-discipline question (where does the registry live?).
2. **Source-resolver detects staleness** (`SparkSourceResolver.resolve` returns an `Incompatible` case). Considered; rejected for v1 because it conflates "source schema changed under us" (legitimate `Incompatible`) with "rollup state columns are stale" (a separable concern requiring the recovery command in the error message).
3. **Just let `AnalysisException` propagate.** Rejected — the exact silent-failure class the refusal ADT exists to prevent; defeats duck's M-1 from #341.

## References

- #341 (T8) — Welford migration that introduced the `m2__F` vs `sumsq__F` hazard.
- `RollupRewriter.SchematStale` — sealed case; observed by Ticket-6 observer but emitted only from the connector.
- `RollupRewriter.isStateColumnName` — vocabulary helper consumed by `MinimalRelOpLowerer.lowerScan`.

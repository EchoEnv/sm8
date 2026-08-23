# ADR-009-a: Adapter-side join strategy from JoinSpec.estimatedRows — seed the Spark broadcast byte-threshold

| Field | Value |
| **Status** | **v1.1 — revised (2 dual reviews; v1.0 rejected: wrong seam anchor + ambiguity no-op/Cross-regression + unit mislabel)** |
| **Date** | 2026-08-23 |
| **Module** | `connectors/spark-connector` (`SparkEngineProvider.query` request layer + `MinimalRelOpLowerer.lowerJoin` byte gate) + `sm8-core` (`JoinSpec.estimatedRows`) |
| **Supersedes scope** | RFC §2 'Reuse for planning' — 'Feeding broadcast-plugin / skew-plugin' concretized adapter-side |
| **Skill alignment** | `scala-spark-batch-bugs-mindset`, `scala-impact-analysis-mindset`, `karpathy-app-design-mindset`, `scala-data-driven-refactor-mindset`, `scala-jvm-safety-mindset`, `scala-error-handling-mindset`, `scala-perf-testing-mindset`, `scala2-scaladoc-mindset` |

## Decision-at-a-glance

The spark-connector **already** broadcasts via a real byte gate (`MinimalRelOpLowerer.lower`, the LIVE join path): `ctx.joinHints.broadcastRightBelowBytes: Option[Long]` → when set, the lowerer reads `rightDf.queryExecution.analyzed.stats.sizeInBytes` and broadcasts the right side iff `sizeInBytes <= threshold`. When unset, it does `false` (Spark's own `autoBroadcastJoinThreshold` heuristic governs).

`JoinSpec.estimatedRows` (PR-163) is never read by the adapter today. This ADR seeds the **byte threshold** from the estimate, nothing else:

- **Wiring axis (single, spelled out)**: the request layer (`SparkEngineProvider.query`, where the `Model` with `joins[].estimatedRows` is available) computes `effectiveBroadcastThreshold := ctx.joinHints.broadcastRightBelowBytes.orElse(seed)` where `seed = Some(defaultBytes)` whenever the model has a join with `estimatedRows` present. The lowerer's existing `shouldBroadcast = sizeInBytes <= effectiveThreshold` gate is UNCHANGED.
- `ctx.joinHints.preferredStrategy` is NEVER set from the seed → the existing `(Cross, Some(strategy)) → UnsupportedCapability` guard is never triggered → **no Cross-join regression**.
- `RelOp.Join` is unchanged (no new field, no `RelOpPlanPrinter` cascade).
- The runtime `sizeInBytes` measurement stays authoritative (O(1) planner metadata) — the user estimate is an **arm** (presence signal that turns ON an active byte check when the caller set none), never a physical broadcast trigger itself.
- `defaultBytes` defaults to Spark's `autoBroadcastJoinThreshold` (10 MiB) unless overridden by config, and the seed only applies when the model signals a small row count.

**AQE skew is a separate follow-up** (session-level config; boot-time `spark.sql.adaptive.skewJoin.*` from a skewed-join signal) — out of scope, recorded in Consequences.

## Revision history

| Version | Date | Change |
|---|---|---|
| v1.0 | 2026-08-23 | Initial draft — wrong: not anchor to adapter's `default I` (user chose Option A); ambiguous seed axis |
| v1.1 | 2026-08-23 | Revised: named the byte-axis wiring exactly, removed `preferredStrategy` seeding, anchored at the LIVE `lowerJoin` path + request layer, corrected the unit mislabel (rows vs bytes) |

---

## Context

### Verified adapter reality (2026-08-23, both reviewers + codegraph)

- **Live broadcast path**: `SparkEngineProvider.query` wires `QueryBuilder.build → compileRelOp → MinimalRelOpLowerer.lower → lowerJoin` (`MinimalRelOpLowerer.scala:428-505`). `lowerJoin` reads `ctx.joinHints.broadcastRightBelowBytes` vs `sizeInBytes` → `functions.broadcast(rightDf)`.
- The old `PortableQueryCompiler.applyJoin` (~330-400) is a legacy path that ALSO honors hints — the architects looked there too; `lowerJoin` is the production one. The ADR anchors to `lowerJoin` + the request-layer seed.
- `RelOp.Join(left,right,kind,condition)` — 4 fields, no estimate. `QueryBuilder` constructs it; `RelOpPlanPrinter` matches it. Adding a field cascade costs both; the seed avoids it. (Considered + rejected in review: too large for the value.)
- `JoinSpec.estimatedRows: Option[Long]` — present; unread by the adapter.

### The Gap

A user who declares `estimated_rows: 1000` gets no adapter-side effect: the byte-threshold path is unset (→ `false` → Spark's own heuristic), and nothing consults the estimate. The estimate is the declarative small-table signal the RFC §2 wanted the planner to consult; it is currently dead at the adapter.

## Decision

1. **Request-layer seed** (in `SparkEngineProvider.query` where the model is bound): compute `effectiveBroadcastThresholdBytes = ctx.joinHints.broadcastRightBelowBytes.orElse(seedIfSmall)`, where `seedIfSmall = Some(broadcastSeedBytes)` iff the model has a join with `estimatedRows` present (regardless of value; the value is advisory).
2. **Threshold source**: `broadcastSeedBytes` default = Spark `autoBroadcastJoinThreshold` (10 MiB) unless connector config overrides. (Unit: BYTES, not rows — the seed only armes a byte gate; no rows-vs-bytes comparison.)
3. **Precedence (single axis)**: caller `broadcastRightBelowBytes` > seeded fallback > Spark default heuristic. `preferredStrategy` untouched.
4. **Correctness net**: the lowerer's sizeInBytes gate (O(1) planner metadata) stays authoritative — the seed merely switches `None → Some(default)`; a real large right side never gets `broadcast()` because `sizeInBytes <= threshold` still must hold. Stats-unavailable (`AnalysisException`) → `-1L` → no broadcast (falls to Spark heuristic), mirroring the existing narrow catch in `lowerJoin` (which already avoids the legacy broad `catch { _:Throwable => MaxValue }`).

### Why exactly this wiring

- **No Cross-join regression**: never sets `preferredStrategy`; the `(Cross, Some(strategy))` guard is not triggered.
- **The seed actually does something**: today `broadcastRightBelowBytes = None` in the default/ adapter path → no-op; with the seed it becomes `Some(default)` in the small-estimate case → the size gate takes effect. The motivating "declared estimate, no explicit hint" scenario gets a real effect.
- **OOM-safe**: user estimate is an arm; bytes measured by Spark remain the physical truth — a 1M-estimate on a 100GB wide table does NOT get broadcast() (size gate blocks it).

## Consequences

- **Positive**: RFC §2 "estimates the planner can consult" realized on the adapter; zero plugin change; zero RelOp change; OOM-safety retained; Cross-join unaffected; plan-assertable (`BroadcastHashJoinExec` when small, `SortMergeJoin` when large).
- **Negative**: estimate is an arm (presence), not a numeric threshold — user expectations that a specific row count sets a threshold must be documented as "arms the default byte gate." Rows≠bytes remains (never compared directly).
- **Follow-up (separate ADR)**: AQE skew — boot-time `spark.sql.adaptive.skewJoin.enabled` when the catalog shows a join estimate ≥ skew threshold; session-level, no hint API.

## Alternatives considered

- Option B (plugins own Spark) — rejected (ADR-008-AD, portability).
- Wire the seed to `preferredStrategy := Broadcast` — rejected: Cross-join compile regression (`(Cross, Some)` guard) + no OOME guard.
- Add `estimatedRows` to `RelOp.Join` — rejected: cascade into `QueryBuilder` + `RelOpPlanPrinter`; the byte-axis avoids both and preserves the O(1) measured check.
- Rely only on runtime sizeInBytes (no seed) — loses the declarative estimate signal; the seed (armed default byte budget) is the minimal honest bridge.

## References

- `docs/rfcs/2026-08-12_v1_architecture-spec/semantic-layer-engine-architecture.md` §3 (adapter owns engine behavior; plugin portable)
- `docs/rfcs/2026-08-23_feat_semantic-graph/semantic-graph_proposal.md` §2 (estimates consulted by the planner)
- `docs/adr/0008-aj-join-cardinality-estimates.md` (estimatedRows + decision-only)
- `connectors/spark-connector` `MinimalRelOpLowerer.lowerJoin`, `SparkEngineProvider.query`, `EngineContext.JoinHints`
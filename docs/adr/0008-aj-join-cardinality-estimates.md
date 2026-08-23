# ADR-008-AJ: Join cardinality estimates — JoinSpec.estimatedRows → decision-only planner consumers

| Field | Value |
| **Status** | **v1.0 — approved (RFC §2 'Feeding broadcast-plugin/skew-plugin' use case; user-directed 2026-08-23 choice)** |
| **Date** | 2026-08-23 |
| **Module** | `sm8-core` (`JoinSpec`) + `plugins/semantic-graph-plugin` + `plugins/broadcast-plugin` + `plugins/skew-plugin` |
| **Closes** | RFC §2 use case 'Feeding broadcast-plugin/skew-plugin' — join edges annotated with cardinality/size estimates the planner can consult instead of guessing |
| **Skill alignment** | `scala-impact-analysis-mindset`, `karpathy-app-design-mindset`, `karpathy-guidelines-mindset`, `scala-jvm-safety-mindset`, `scala-error-handling-mindset`, `scala2-scaladoc-mindset` |

## Decision-at-a-glance

Add an optional `estimatedRows: Option[Long] = None` field to `JoinSpec` (sm8-core). This is the **single source of truth** for join cardinality: the user supplies it declaratively at model-construction (or via manifest `estimated_rows`). Three consumers read it:

1. **`SemanticGraphBuilder`** — annotates each join edge's weight with the estimate (falling back to `1.0` when absent), replacing the RFC §5 "weight is a placeholder" gap, and exposes a `joinCardinalities` query.
2. **`GraphSnapshot`** — new `joinCardinalities` field projected through `toMetaValue` for the meta-inspector.
3. **`broadcast-plugin` / `skew-plugin`** — a **decision-only** `consult` step: read `EngineHookRequest.model.joins[].estimatedRows`, apply a threshold, and set a decision (broadcast / no-broadcast, skew / no-skew). **They do NOT set Spark config** — Spark integration remains deferred per the existing stub comments. The plugins stay stubs that merely expose the decision path + count fires.

The plugins consume via `EngineHookRequest.model` (Core-visible) — **no cross-module dependency** is introduced (broadcast/skew depend only on `sm8-core`; they never import the semantic-graph module).

## Revision history

| Version | Date | Change |
| v1.0 | 2026-08-23 | Initial — JoinSpec.estimatedRows + decision-only broadcast/skew consumers |

---

## Context

### RFC mandate

`docs/rfcs/2026-08-23_feat_semantic-graph/semantic-graph_proposal.md` §2 use case:

> **Feeding broadcast-plugin / skew-plugin** | Presumably heuristic | Join edges annotated with cardinality/size estimates the planner can consult instead of guessing

And §5, line 297:

> `joinPath`'s weights are placeholders (`1.0` per join hop) until `broadcast-plugin`/`skew-plugin` actually expose cardinality estimates to consult.

### User-directed decision (2026-08-23)

The user selected:
- **Source**: user-supplied estimate (new `JoinSpec` field) — NOT a `StatisticsProvider` abstraction (over-engineering: no connector exposes statistics today) and NOT a plugin-side map (breaks the single-source-of-truth when multiple plugins need the number).
- **Consumer scope**: **decision-only** — the planner-consult path returns a decision; the actual Spark config is NOT set (Spark integration deferred).

### Codegraph evidence (2026-08-23)

- `sm8-core/.../model/JoinSpec.scala:46-50`: 4-field case class (`name, rightModel, kind, keys`) + `Product with Serializable`.
- `sm8-core/.../manifest/ModelLoader.scala:301-335` (parseJoins): builds `JoinSpec(name, rightModel, k, keys)` from YAML `{name, rightModel, kind, keys}`.
- `sm8-core/.../model/ModelValidator.scala:127-133`: validates join left/right keys.
- `plugins/semantic-graph-plugin/.../SemanticGraphBuilder.scala:266-276`: join edges added with hardcoded `1.0` weight (the placeholder the RFC flags).
- `plugins/broadcast-plugin/.../BroadcastStub.scala` + `plugins/skew-plugin/.../SkewStub.scala`: counter-only `PreExecute` stubs; comment "Real implementation will set the broadcast threshold... (deferred)".
- `sm8-core/.../engine/EngineHookTypes.scala:6-10`: `EngineHookRequest(model, mcpRequest, cacheKey)` — pre-hooks (incl. PreExecute) receive the `Model`, so broadcast/skew read `model.joins` without any new dependency.

### Constraints honored

- **Frozen SDK list** (`Plugin, Connector, PreHook, PostHook, Transformer, Context, Engine`) is untouched. `JoinSpec` is Core-owned but **not** frozen — evolvable via ADR.
- **Zero Spark types captured**: all four modules keep `enforce-no-spark`. The new field is `Option[Long]` (pure Scala).
- **Scaladoc shape**: `@param` / `@return` present; zero process noise (per `scala2-scaladoc-mindset`).
- **No cross-module plugin dependency**: broadcast/skew consume via `EngineHookRequest.model`, not by importing the semantic-graph module.

---

## Decision

### 1. `JoinSpec.estimatedRows: Option[Long] = None`

Add as a trailing case-class field with a default, so all existing positional callsites (`JoinSpec(name, rightModel, kind, keys)`) compile unchanged. `None` = "no estimate declared"; the graph weight falls back to `1.0` (unchanged placeholder behavior).

### 2. `ModelLoader` manifest `estimated_rows`

`parseJoins` reads an optional `estimated_rows` (or camelCase `estimatedRows`) field per join entry. Absent → `None`. Present-but-invalid (non-numeric or negative) → typed `ManifestError.ParseFailure` (never silent, matching the wave's typed-error convention).

- Each join edge's weight = `js.estimatedRows.getOrElse(1.0).toDouble` (replaces the hardcoded `1.0` placeholder the RFC flags).
- New query `SemanticGraph.joinCardinality(from: GraphNode, to: GraphNode): Option[Long]` — where the edge is a join-edge with a user-supplied estimate, returns it; otherwise `None`. The graph edge already stores the weight, so this is a lookup over `g.getEdgeWeight`, not a separate structure. Join edges are the only edges with non-zero weight (calc-measure / dimension edges are `0`), so a weight `> 0` identifies a join edge.
- The graph keeps its `AsSynchronizedGraph` wrapper (thread-safety, PR-149) — the new query reads through it.

### 4. `GraphSnapshot`

New `joinCardinalities` field projected through `toMetaValue` — for the meta-inspector. Shape: a `List` of `(edge-endpoints, estimate)` tuples for each join edge that has a user-supplied estimate, sorted deterministically like the existing `dependents` projection. Keys are the `(GraphNode, GraphNode)` join-edge endpoints (the same two-vertex shape as `edges`).

### 5. broadcast/skew `consult` — decision-only

Each plugin (still a stub — the counter + `setup` registration are retained verbatim) gains a `consult(model, threshold)` helper returning a decision. It reads `model.joins.flatMap(_.estimatedRows)` and decides:

- **broadcast**: `true` if any join's estimated row count is below the broadcast threshold (small enough to ship as a broadcast / map-side artifact).
- **skew**: `true` if any join's estimated row count is above the skew threshold (skewed enough to warrant AQE skew handling).

The decision is exposed for tests + the meta-inspector. **The plugins do NOT set Spark config** — Spark integration remains deferred per the existing stub comments. Actual Spark config wiring is a future change under its own PR.

- Join edge weight = `js.estimatedRows.getOrElse(1.0).toDouble` (replaces the hardcoded `1.0`).
- Add `SemanticGraph.joinCardinalities: Map[(GraphNode, GraphNode, Long]` if present within the graph — no wait. From codegraph evidence, expose the per-join estimate students via `Some GraphNode, GraphNode) => Option[Long]` query.
- The observer already holds the model reference — the graph exposes the join edges' weights (readable as cardinality estimates through the existing edge-weight API). Concrete: `SemanticGraph.joinCardinalities` returns a `Map[GraphNode, GraphNode, Long]` keyed by join edge endpoints.

### 4. `GraphSnapshot`

New `joinCardinality field + `joinCardinalities` field projected through `toMetaValue` — for the meta-inspector. Map keys are the `(GraphNode, GraphNode)` edge endpoints that correspond to join edges

- `edges` are `List[(GraphNode, GraphNode]` — the current shape.

### 5. broadcast/skew `consult` decision-only step. Each plugin (still stub) gains a `consult(model, threshold): BroadcastDecision` type returning whether to broadcast. It reads `model.joins.flatMap(_.estimatedRows).minOption` vs the threshold

- The existing counter is retained — the plugin `setup` still registers the same-named PreExecute stub. Actual Spark config set: NOT.

(Update the RFC "Presumably heuristic" — the plugins' `fires` counter + the consult decision path is what the planner consults.)

---

## Consequences

- **Positive**: single source of truth for cardinality (declared by the user where it's knowable); no heuristic guessing; no new module dependency; the RFC's §5 "weight is a placeholder" gap closed; broadcast/skew get a consultable decision path without touching Spark (which is deferred anyway).
- **Negative**: estimates are user-supplied, not learned — a future `StatisticsProvider` (real catalog) can supersede this ADR later; absent estimates still fall back to `1.0` (unchanged).
- **Risk**: `JoinSpec` field-addition is source-compatible but **binary-incompatible** for the case-class constructor. Per the wave's convention this is acceptable (in-repo, single version). Called out in the PR body.

## Alternatives Considered

- **`StatisticsProvider` abstraction**: rejected by user — over-engineering; no connector exposes statistics today; adds a moving part with zero consumers.
- **Plugin-side cardinality map**: rejected by user — two consumers would need the same number; a map parametered per-build fragments the single source of truth.
- **Full Spark config wiring in plugins**: rejected by user — Spark integration is deferred; would require the `enforce-no-spark` grants + plugin Spark deps the RFC forbids.

## References

- `docs/rfcs/2026-08-23_feat_semantic-graph/semantic-graph_proposal.md` §2 + §5 (origin use case + placeholder weight gap)
- `docs/adr/0008-ai-semantic-graph-rfc-review-and-fixes.md` (the parent semantic-graph ADR)
- `docs/rfcs/2026-08-12_v1_architecture-spec/plugins.md` (plugin module shape; depend-only-on-sm8-core)
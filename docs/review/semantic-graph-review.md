# Semantic Graph RFC — Deep Review (5-section audit)

| Field | Value |
|---|---|
| **Status** | **Review complete — implementation gated on 3 fixes** |
| **Date** | 2026-08-23 |
| **RFC** | `docs/rfcs/2026-08-23_feat_semantic-graph/semantic-graph_proposal.md` |
| **Architecture spec** | `docs/rfcs/2026-08-12_v1_architecture-spec/` (4 docs) |
| **Repo HEAD** | `16cfdaa` |
| **Reviewer** | Main agent + `scout` subagent (5-section audit) |

## TL;DR

The proposal is **architecturally sound** (Core-boundary compliant, frozen-SDK respected, plugin-only). **Three real issues must be fixed before implementation**:
1. **THREAD-SAFETY (MEDIUM)**: JGraphT's `DefaultDirectedWeightedGraph` is NOT thread-safe. The "safe to cache" claim is wrong. Use `org.jgrapht.graph.concurrent.AsSynchronizedGraph` or drop the cache.
2. **TYPED-ERROR (MEDIUM)**: `context.meta` is a String-keyed map; using `meta = ... + ("semanticGraphError" -> s"...")` violates `scala-error-handling-mindset` rule #1 ("errors are data"). Surface as a typed error.
3. **DANGLING-JOIN-NODE (LOW)**: RFC says dangling right-nodes are "a validation signal, not a crash" — but the proposal's `JoinPathPreHook` only checks `hasCycle`. The dangling signal is silent.

## Section 1 — Core Boundary compliance (PASS)

Per `architecture-spec §3`:

| Rule | Status | Evidence |
|---|---|---|
| Plugin imports Core | OK | `import io.sm8.core.model.{Model, JoinSpec, CalculatedMeasure}` (model is in `io.sm8.core.model`, owned by Core but not in the 7-frozen-types surface). |
| Plugin does NOT import a specific adapter | OK | Proposal imports only JGraphT (`org.jgrapht.*`) — pure JVM, zero Spark transitive deps. |
| Plugin does NOT import a specific other plugin | OK | Proposal only imports `io.sm8.core.*` + `io.sm8.sdk.*` + JGraphT. |
| Core imports Plugin | N/A | No Core changes; the proposal adds only a new module. |
| Hook lives in plugin module | OK | `JoinPathPreHook` is in `plugins/semantic-graph-plugin/src/main/scala/io/sm8/plugins/semanticgraph/`. |
| Per-module `maven-enforcer-plugin` rule | OK | JGraphT does NOT bring `org.apache.spark:*` transitively (verified: `grep org.apache.spark @ plugins/` returned 0). |

**Verdict: Core Boundary PASS.**

## Section 2 — Frozen-SDK compliance (PASS)

Per the README's Contributing section — 7 frozen types: `Plugin`, `Connector`, `PreHook`, `PostHook`, `Transformer`, `Context`, `Engine`.

| Check | Status | Evidence |
|---|---|---|
| Plugin uses ONLY the 7 frozen types | OK | Uses `Plugin` (in `SemanticGraphPlugin`), `PreHook` (in `JoinPathPreHook`), `Context` (in hook return). |
| `HookStage.PreResolve` exists | OK | Verified at `sm8-core/src/main/scala/io/sm8/core/Pipeline.scala:216`: `case PipelineStage.Resolve => HookStage.PreResolve`. |
| `priority = 120` ∈ [100, 899] | OK | `HookOrigin.FirstParty.lowerBound = 100`, `FirstParty.upperBound = 899`. Verified at `sm8-core/src/main/scala/io/sm8/sdk/HookOrigin.scala:43-54`. |
| No new SDK types added | OK | `EngineHookRequest` already exists; `Model` already in `io.sm8.core.model` (not frozen). |

**Verdict: Frozen-SDK PASS.**

## Section 3 — Code-correctness concerns

### 3a. THREAD-SAFETY of cached `SemanticGraph` (MEDIUM)

The proposal says "Built once per Model and safe to cache". **This is incorrect**: JGraphT's `DefaultDirectedWeightedGraph` is NOT thread-safe. If two queries hit the same Model concurrently, both will try to read the cached graph → potential `ConcurrentModificationException` or stale reads (JGraphT 1.5+ has `org.jgrapht.graph.concurrent.AsSynchronizedGraph` exactly for this).

**Concrete failure path**:
- Query A reaches `JoinPathPreHook` → builds graph → caches.
- Query B reaches `JoinPathPreHook` → looks up cache → reads graph concurrently with Query A's `joinPath(from, to)` call (which calls `DijkstraShortestPath(g).getPath(...)`).
- JGraphT's `DijkstraShortestPath` reads `g.outgoingEdgesOf(v)` and `g.getEdgeSource(e)` — these are NOT synchronized on `DefaultDirectedWeightedGraph`.
- Result: a CME, a stale path, or a wrong shortest path. Silent corruption.

**Fix** (3 options, listed by simplicity):
1. **Wrap at construction**:
   ```scala
   val raw = new DefaultDirectedWeightedGraph[GraphNode, DefaultWeightedEdge](classOf[DefaultWeightedEdge])
   val g = new AsSynchronizedGraph(raw)
   ```
   The cache stores the synchronized view. JGraphT 1.5+ ships `AsSynchronizedGraph` in `org.jgrapht.graph.concurrent`. The proposal's pom version `1.5.2` includes it.
2. **Drop the cache**: rebuild per request. Loses ~1ms per request but eliminates the concurrency hazard entirely.
3. **Serialize via `synchronized` block** at every `SemanticGraph` method. Brittle — recommend option 1.

### 3b. Cycle-detection on cached vs fresh graph (PASS)

The proposal's `SemanticGraphBuilder.build(model)` walks `model.calculatedMeasures.foreach { c => refs = Calculator.measureNamesOf(c.expr) ++ Calculator.fieldNamesOf(c.expr); ... }`. Core's `QueryBuilder.detectCalcCycles` (verified at `sm8-core/src/main/scala/io/sm8/core/query/QueryBuilder.scala:255-`) uses the same walkers (`Calculator.measureNamesOf` + `Calculator.fieldNamesOf`) to build its adjacency map. The two graphs are **semantically equivalent**.

### 3c. Dangling right-hand-model nodes (LOW)

RFC §3 in `SemanticGraphBuilder.scala:182` says: *"Right-hand model may not have been loaded into `byName` yet (cross-catalog case) — record the edge anyway; a dangling right node is a validation signal, not a crash."* But the proposed `JoinPathPreHook` only checks `graph.hasCycle` and sets `stop = true`. The dangling node case is **silent** — the proposal creates a dangling vertex but never surfaces it.

**Fix** (2 options):
1. **Surface dangling nodes** as a typed error in `context.result` or `context.meta` with a structured key (e.g. `semantic-graph: dangling-joins: List[(leftModel, leftKey, rightModel, rightKey)]`).
2. **Skip dangling edges** at graph-build time (don't add the vertex). Trades fidelity for simplicity.

Recommend option 1 — surface as typed error per `scala-error-handling-mindset`.

### 3d. `Calculator.measureNamesOf` / `.fieldNamesOf` re-walk cost (LOW)

The proposal calls these walkers at every hook call. For a model with N calc-measures, each referencing M measures, the walker call is O(N × M). Quantify:
- `examples/hospital-cleaning` has 1 calc-measure (`avg_los`) referencing 2 measures (`los_days`, `encounter_count`). Cost: trivial.
- A realistic enterprise model: ~50 calc-measures, each referencing ~5 measures → ~250 walker calls per graph build. Each walker is a tree-walk over the `Expr` ADT (24 cases). At ~1µs per walker call: ~250µs total. Negligible at the PreResolve hook stage (which runs once per query, before any Connector work).

**Verdict: Perf is acceptable. No fix needed.**

### 3e. Plugin `Refs` pattern compliance (PASS — different purpose)

The 4 existing plugins follow a `Refs` singleton pattern (phantom-typed dimension/measure witnesses — e.g. `plugins/audit-plugin/src/main/scala/io/sm8/plugins/audit/Refs.scala` exposes `Refs.ActorId: TypedDimension[...]` for use in `groupByKey`). The semantic-graph proposal uses a `GraphNode` case class — NOT a `Refs` singleton.

This is a **legitimate style drift**: `GraphNode` is a graph vertex (model + field), which is a different purpose than phantom-typed dimension/measure witnesses. The proposal's `GraphNode` is value-typed and intentionally cheap to allocate (JGraphT requires a stable `equals` + `hashCode` on its vertex type — a case class provides both for free).

**Verdict: NO fix needed. The `GraphNode` case class is justified.**

## Section 4 — Performance concerns (acceptable)

| Concern | Quantification | Verdict |
|---|---|---|
| Graph build cost | ~250µs for a realistic 50-calc-measure model | Acceptable (runs once per request at PreResolve). |
| `joinPath` Dijkstra | O(V log V + E) per call | Acceptable if called once per request; would profile if called many times. |
| `dependents()` recursive walk | O(V + E) per call; no caching | Acceptable if called once per request; if called many times, recommend caching the transitive closure at graph-build time. |

**Verdict: Performance acceptable. Profile if `dependents()` becomes hot.**

## Section 5 — Skill alignment audit

| Skill | Verdict | Notes |
|---|---|---|
| `karpathy-guidelines-mindset` | PASS | Surgical change (new module, no Core changes). |
| `karpathy-app-design-mindset` | PASS | Core Boundary table satisfied; new plugin extends Engine via hooks. |
| `scala-error-handling-mindset` | **WARN** | Meta-string error pattern violates "errors are data". Recommend typed error. |
| `scala-spark-batch-bugs-mindset` | PASS | No Spark types captured; hook is driver-side (PreResolve runs before any Connector). |
| `scala-impact-analysis-mindset` | PASS | Zero production code changes; only a new module is added. |
| `scala-jvm-safety-mindset` | **WARN** | Thread-safety of cached `SemanticGraph` — see §3a above. |
| `scala2-scaladoc-mindset` | PASS | Proposal's Scaladoc is clean (no `[[wikilinks]]` to skills, no PR/Phase refs). |
| `scala-bug-hunting-mindset` | PASS | Exhaustiveness check: `EngineHookRequest(model: Model, _, _)` pattern is valid; `case _ => context` handles non-EngineHookRequest gracefully. |

## Final verdict

**The proposal is implementation-ready WITH the following 3 fixes applied**:

| # | Fix | Severity | Effort |
|---|---|---|---|
| 1 | Wrap JGraphT in `AsSynchronizedGraph` (or drop the cache) | MEDIUM | 1 line + JGraphT 1.5+ dep already there |
| 2 | Surface cycle detection result as typed error in `context.result` (NOT `context.meta` String-keyed) | MEDIUM | ~10 lines (typed `EngineError.UnsupportedCapability` wrapper) |
| 3 | Surface dangling join nodes as a structured typed signal | LOW | ~10 lines (similar to fix 2) |

After these fixes, the proposal can be implemented in ~200 lines (per the RFC's own estimate) with 1 new file `SemanticGraphBuilder.scala`, 1 hook `JoinPathPreHook.scala`, 1 plugin `SemanticGraphPlugin.scala`, 1 pom.xml, 2 META-INF files.

The user-facing decision points:
- (a) Should the proposal proceed as-is and we apply the 3 fixes during implementation?
- (b) Should we update the RFC first, then implement?
- (c) Should we add a hook cache option (`semanticGraphCacheTTL` via `context.meta`) to control staleness?

## ADR

This review will be saved as `docs/adr/0008-ai-semantic-graph-rfc-review.md` once the user confirms the approach.
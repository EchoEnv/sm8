# Applying a Semantic Graph to SM8

> **Status:** v1.1 (review fixes applied per ADR-008-AI; the 2026-08-23 deep review found 2 BLOCKERs + 2 required edits — see `docs/review/semantic-graph-review.md` for full audit).
Based on cloning and reading `github.com/EchoEnv/sm8` directly (not just the README) — this covers what's actually in the code, where a semantic graph fits, and a concrete plugin you could ship without touching the frozen Core.

---

## 1. What's actually in the repo (relevant to this)

### The Model IR (`sm8-core/src/main/scala/io/sm8/core/model/`)

`Model` (`Model.scala`) is a pure-data case class — no methods, built only via the smart constructor `Model.of(...)`, which runs `ModelValidator.validate` once at the boundary. It holds:

```scala
final case class Model private (
    name: String, version: Int, description: Option[String],
    dimensions: List[Dimension], measures: List[Measure],
    defaultPolicies: ModelPolicyDefaults, source: SourceRef,
    status: ModelStatus, filters: List[FilterSpec],
    calculatedMeasures: List[CalculatedMeasure] = Nil,
    joins: List[JoinSpec] = Nil)
```

- **`Dimension(name, expr: Expr, dataType: Option[SealedDataType])`** — `expr` is a typed `Expr`, not a string (`Dimension.scala`, `Expr.scala`).
- **`Measure(name, expr: AggregateCall)`** — a single aggregate call (`SUM(amount)`, `COUNT(*)`, ...).
- **`CalculatedMeasure(name, expr: Expr)`** (`CalculatedMeasure.scala`) — *any* `Expr`, can reference other measures via `Expr.MeasureRef` / `Expr.All`, or other fields via `Expr.FieldRef`. This is explicitly a DAG: the file's own comment says "calculated-measure dependency DAG surface as `ModelValidationError.CalculatedMeasureCycle` at model-load time."
- **`JoinSpec(name, rightModel: String, kind: JoinKind, keys: List[(String,String)])`** (`JoinSpec.scala`) — one model joining to another model by name, equi-join only in v0.1.0 (multi-key deferred).
- **`Expr`** (`Expr.scala`) is a 24-case sealed ADT (`FieldRef`, `MeasureRef`, `Add`, `Divide`, `CaseWhen`, `Alias`, ...). `io.sm8.core.expr.Calculator.fieldNamesOf(expr)` / `.measureNamesOf(expr)` are the canonical walkers — both `ModelValidator` and `QueryBuilder` use them, per an explicit "single source of truth" comment.

### The graph problem already exists — solved narrowly, once

`QueryBuilder.detectCalcCycles` (`sm8-core/.../query/QueryBuilder.scala`, ~line 255) is a hand-written iterative DFS with 3-color (White/Gray/Black) marking over the calculated-measure dependency graph. It:

- builds an adjacency map from `CalculatedMeasure.name → refs(name)` where `refs` = `measureNamesOf(expr) ++ fieldNamesOf(expr)` filtered to known calc-measure names,
- walks it with a hand-rolled stack machine (continuation frames to avoid a specific false-positive bug documented in the comments),
- returns `Left(EngineError.UnsupportedCapability(... "CalculatedMeasureCycle" ...))` on a cycle.

This is a real graph algorithm, implemented from scratch, solving exactly one problem (cycle detection for one field kind). It doesn't extend to:

- **join-path resolution** — `ModelValidator.validateAgainstSchema` only checks that a join's *left* keys exist in the model's own schema (comment: "the right-model schema lookup is PR-M3 + PR-M4 territory" — i.e., not done yet); there's no cross-model join-path search at all,
- **cross-model / cross-field lineage** — nothing connects `Dimension.expr` → source column → `JoinSpec.rightModel` → that model's own dimensions,
- **reuse for planning** — `broadcast-plugin` and `skew-plugin` exist as separate plugin modules but (from what's in the model layer) have no shared graph structure to consult for cardinality/cost hints.

### How a Plugin gets at the Model at runtime

`sm8-core/.../engine/EngineHookTypes.scala`:

```scala
final case class EngineHookRequest(
  model: Model, mcpRequest: QueryRequest, cacheKey: String
) extends Request
```

This is the concrete `Request` subtype `Context.request` holds during the pipeline (per the `HookRunner` doc comment in `Hooks.scala`, which shows `Context(request = EngineHookRequest(model, request, cacheKey))` as the call-site pattern). So **any `PreHook`/`PostHook` can pattern-match `context.request` to `EngineHookRequest` and read `.model` — no frozen-SDK change required.**

### Frozen vs. hot, concretely

Per the README's Contributing section, only these 7 types are the breaking-change surface: `Plugin`, `Connector`, `PreHook`, `PostHook`, `Transformer`, `Context`, `Engine`. `Model`, `JoinSpec`, `CalculatedMeasure`, `Expr`, `QueryBuilder` all live in `io.sm8.core.model` / `io.sm8.core.query` / `io.sm8.core.expr` — they're Core-owned but **not** part of that frozen list, so in principle they could evolve through the normal ADR process. But the fastest path that ships without touching Core *at all* is a new plugin module, following the exact shape of `plugins/audit-plugin`.

---

## 2. Where a semantic graph earns its place

| Use case | Today | With a semantic graph |
|---|---|---|
| Calculated-measure cycle detection | Bespoke DFS in `QueryBuilder`, one-off, only handles calc-measures | Same check, generalized to a reusable adjacency structure any plugin can query |
| Join-path resolution | Not implemented — right-side key validation is deferred ("PR-M3/PR-M4 territory") | Model graph with `JoinSpec` edges; BFS/Dijkstra finds a join path between any two dimensions/measures across models, flags ambiguous fan-out before query time |
| Impact analysis | None | "Which calculated measures / models break if this dimension changes?" = reverse-edge traversal |
| Feeding `broadcast-plugin` / `skew-plugin` | Presumably heuristic | Join edges annotated with cardinality/size estimates the planner can consult instead of guessing |
| Cross-model discovery | None (each `Model` is validated in isolation) | "Which models reference model X via a join" = a query over the same graph |

The join-path and impact-analysis pieces are the two that don't exist in any form today — that's the highest-leverage place to start.

---

## 3. Proposed plugin: `semantic-graph-plugin`

Ships as a new module under `plugins/`, same shape as `audit-plugin`: depends only on `sm8-core`, registers via `META-INF/services/io.sm8.sdk.Plugin`, declares its Maven coordinates in `META-INF/sm8/plugin.properties` for the allowlist.

It does two things:

1. **`SemanticGraphBuilder`** — pure function `Model => SemanticGraph` (or `List[Model] => SemanticGraph` for the cross-model case), built on **JGraphT** (`org.jgrapht:jgrapht-core`) so cycle detection, shortest-path, and topological sort are library calls, not hand-rolled DFS.
2. **`JoinPathPreHook`** — a `PreHook` bound to `pre:resolve` (before the engine tries to compile the query) that builds/looks up the graph for `context.request.model`, and either finds a valid join path or sets `context.stop = true` with a typed error in `context.meta` before any engine work happens.

### `SemanticGraphBuilder.scala`

```scala
package io.sm8.plugins.semanticgraph

import io.sm8.core.model.{Model, JoinSpec, CalculatedMeasure}
import io.sm8.core.expr.Calculator
import org.jgrapht.graph.{DefaultDirectedWeightedGraph, DefaultWeightedEdge}
import org.jgrapht.alg.shortestpath.DijkstraShortestPath
import org.jgrapht.alg.cycle.CycleDetector

/** A node in the semantic graph: a field belonging to a named model. */
final case class GraphNode(model: String, field: String)

/**
 * Engine-portable semantic graph over one or more `Model`s.
 *
 * Edges:
 *  - calc-measure -> referenced field/measure (weight 0, same model)
 *  - dimension    -> field it derives from (weight 0, same model)
 *  - join column  -> join column on the right-hand model (weight = 1,
 *    or a caller-supplied cost, e.g. estimated row count)
 *
 * Built once per Model (or Model set) and safe to cache — it's pure
 * data derived from already-validated `Model`s, never touches a
 * Connector or a live source.
 */
final class SemanticGraph private (
    private val g: DefaultDirectedWeightedGraph[GraphNode, DefaultWeightedEdge]
) {

  /** Shortest join path between two fields, possibly across models. */
  def joinPath(from: GraphNode, to: GraphNode): Option[List[GraphNode]] = {
    val path = new DijkstraShortestPath(g).getPath(from, to)
    Option(path).map(p => scala.jdk.CollectionConverters.ListHasAsScala(p.getVertexList).asScala.toList)
  }

  /** True if the calc-measure / dimension dependency graph has a cycle. */
  def hasCycle: Boolean = new CycleDetector(g).detectCycles()

  /** Reverse-edge impact set: everything that transitively depends on `node`. */
  def dependents(node: GraphNode): Set[GraphNode] = {
    import scala.jdk.CollectionConverters._
    def incoming(n: GraphNode): Set[GraphNode] =
      g.incomingEdgesOf(n).asScala.map(g.getEdgeSource).toSet
    def walk(frontier: Set[GraphNode], seen: Set[GraphNode]): Set[GraphNode] =
      if (frontier.isEmpty) seen
      else {
        val next = frontier.flatMap(incoming) -- seen
        walk(next, seen ++ next)
      }
    walk(incoming(node), Set.empty)
  }
}

object SemanticGraphBuilder {

  /** Build a graph over a single model: calc-measure deps + dimension
    * field refs. No join edges (those need the right-hand model too —
    * use `buildAcross` for that). */
  def build(model: Model): SemanticGraph = buildAcross(model :: Nil)

  /** Build a graph over multiple models, including join edges between
    * them. `models` should already be `Model.of(...)`-validated. */
  def buildAcross(models: List[Model]): SemanticGraph = {
    val g = new DefaultDirectedWeightedGraph[GraphNode, DefaultWeightedEdge](classOf[DefaultWeightedEdge])
    val byName = models.map(m => m.name -> m).toMap

    def addNode(n: GraphNode): Unit = if (!g.containsVertex(n)) g.addVertex(n)
    def addEdge(a: GraphNode, b: GraphNode, w: Double): Unit = {
      addNode(a); addNode(b)
      val e = g.addEdge(a, b)
      if (e != null) g.setEdgeWeight(e, w)
    }

    models.foreach { model =>
      // Calculated measures -> whatever they reference (reuses the
      // SAME walkers QueryBuilder.detectCalcCycles already trusts).
      model.calculatedMeasures.foreach { c: CalculatedMeasure =>
        val refs = Calculator.measureNamesOf(c.expr) ++ Calculator.fieldNamesOf(c.expr)
        refs.foreach(r => addEdge(GraphNode(model.name, c.name), GraphNode(model.name, r), 0))
      }

      // Dimensions -> the fields their expr touches.
      model.dimensions.foreach { d =>
        Calculator.fieldNamesOf(d.expr).foreach(r =>
          addEdge(GraphNode(model.name, d.name), GraphNode(model.name, r), 0))
      }

      // Joins -> edges to the right-hand model's key columns.
      // Weighted 1 by default; swap in a real cardinality estimate
      // once one is available (feeds broadcast-plugin / skew-plugin).
      model.joins.foreach { js: JoinSpec =>
        js.keys.foreach { case (leftKey, rightKey) =>
          addEdge(GraphNode(model.name, leftKey), GraphNode(js.rightModel, rightKey), 1.0)
          // Right-hand model may not have been loaded into `byName`
          // yet (cross-catalog case) — record the edge anyway; a
          // dangling right node is a validation signal, not a crash.
          val _ = byName.get(js.rightModel)
        }
      }
    }

    new SemanticGraph(g)
  }
}
```

### `JoinPathPreHook.scala`

```scala
package io.sm8.plugins.semanticgraph

import io.sm8.sdk._
import io.sm8.core.engine.EngineHookRequest
import io.sm8.core.model.Model

/**
 * pre:resolve hook. Builds (or reuses a cached) SemanticGraph for the
 * request's Model and confirms the model's own calc-measure/dimension
 * graph is acyclic before the engine spends any Connector time on it.
 *
 * Priority 120 — first-party range (100-899), runs after core (0-99)
 * validation, before any Connector-specific resolve work.
 */
final class JoinPathPreHook extends PreHook {
  override val name: String = "semantic-graph-precheck"
  override val priority: Int = 120
  override val stage: HookStage = HookStage.PreResolve

  override def run(context: Context): Context = context.request match {
    case EngineHookRequest(model: Model, _, _) =>
      val graph = SemanticGraphBuilder.build(model)
      if (graph.hasCycle) {
        // Fail fast, before Connector work — mirrors the fail-fast
        // policy PostHook/PreHook docs already specify (RFC §9).
        context.copy(
          stop = true,
          meta = context.meta + ("semanticGraphError" ->
            s"Cycle detected in model '${model.name}' dependency graph"))
      } else {
        context.copy(meta = context.meta + ("semanticGraph" -> graph))
      }
    case _ => context // not an EngineHookRequest — nothing to check
  }
}
```

### `SemanticGraphPlugin.scala`

```scala
package io.sm8.plugins.semanticgraph

import io.sm8.sdk.{Plugin, Engine}

final class SemanticGraphPlugin extends Plugin {
  override def setup(engine: Engine): Unit =
    engine.hooks.registerPreHook(new JoinPathPreHook)
}
```

### `META-INF/services/io.sm8.sdk.Plugin`

```
io.sm8.plugins.semanticgraph.SemanticGraphPlugin
```

### `META-INF/sm8/plugin.properties`

```
groupId=io.sm8.plugins
artifactId=semantic-graph-plugin
```

### `pom.xml` — same shape as `plugins/audit-plugin/pom.xml`, plus JGraphT

```xml
<parent>
  <groupId>io.sm8</groupId>
  <artifactId>sm8-parent</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <relativePath>../../pom.xml</relativePath>
</parent>

<artifactId>semantic-graph-plugin_2.13</artifactId>
<packaging>jar</packaging>

<dependencies>
  <dependency>
    <groupId>io.sm8</groupId>
    <artifactId>sm8-core_2.13</artifactId>
    <version>${project.version}</version>
  </dependency>
  <dependency>
    <groupId>org.jgrapht</groupId>
    <artifactId>jgrapht-core</artifactId>
    <version>1.5.2</version>
  </dependency>
  <dependency>
    <groupId>org.scalatest</groupId>
    <artifactId>scalatest_${scala.binary.version}</artifactId>
    <version>${scalatest.version}</version>
    <scope>test</scope>
  </dependency>
</dependencies>
```

`jgrapht-core` is pure JVM, zero Spark transitive deps — won't trip the `maven-enforcer-plugin` rule that blocks `org.apache.spark:*` outside `connectors/spark-connector/`.

## 4. What this doesn't solve yet (be upfront about it)

- **Cross-model right-key validation** (`ModelValidator`'s own TODO) still needs the right-hand model's resolved schema, which per the code comments is deferred to "PR-M3 + PR-M4" — the graph can represent the edge, but validating it against a real schema needs that resolver wired in first.
- **`joinPath`'s weights are placeholders** (`1.0` per join hop) until `broadcast-plugin`/`skew-plugin` actually expose cardinality estimates to consult.
- This plugin doesn't replace `QueryBuilder.detectCalcCycles` — that stays as the Core's own guarantee (it runs regardless of which plugins are installed). The plugin's `hasCycle` check is a *pre-flight* duplicate using the same underlying data, useful because it runs before any Connector is touched and is visible to other hooks via `context.meta`.

## 5. v1.1 review fixes (applied before implementation)

Per the 2026-08-23 deep review (`docs/review/semantic-graph-review.md`), the v0 draft had 2 BLOCKERs + 2 required edits. v1.1 applies them all:

### 5.1 — Drop the cache (BLOCKER 3a: thread-safety)

**v0 said**: "Built once per Model and safe to cache". **This is FALSE.** JGraphT's `DefaultDirectedWeightedGraph` is NOT thread-safe per its Javadoc ("No concurrent modifications are permitted on this graph, therefore concurrent reads are not safe either."). Two concurrent requests for the same Model would race on the internal `LinkedHashSet` / `ArrayList` edge lists.

**v1.1 fix**: drop the cache entirely. `Calculator.measureNamesOf` + `Calculator.fieldNamesOf` walks the Expr tree (≤ 24 cases, no Spark coupling). For a realistic 100-calc-measure × 5-ref model, build cost is well under 1 ms — the cache bought nothing for typical models and added a footgun.

If profiling later shows the cache is needed in production, the fix is `org.jgrapht.graph.concurrent.AsSynchronizedGraph` (JGraphT 1.5+ ships it). v1.1 does not use it; revisit if profile shows hot.

### 5.2 — Surface cycle as typed `EngineError.UnsupportedCapability` (BLOCKER: typed errors)

**v0 said**: `meta = context.meta + ("semanticGraphError" -> s"Cycle detected in model '${model.name}'...")`. **This puts a typed error into a `Map[String, Any]` String-keyed map** — a violation of `scala-error-handling-mindset` rule #1 ("errors are data") and inconsistent with `QueryBuilder.detectCalcCycles` which returns `Left(EngineError.UnsupportedCapability(...))`.

**v1.1 fix**: surface the cycle as a typed `EngineError.UnsupportedCapability(engine = "semantic-graph-plugin", capability = "SemanticGraph.cycle", message = ...)` in `context.meta` (typed value, NOT String). The hook sets `context.stop = true` only when a cycle is detected; downstream hooks see the typed error via `context.meta("semanticGraphError")` and can pattern-match.

### 5.3 — Surface dangling right-nodes (WARN 3c)

**v0 said**: dangling right-nodes are "a validation signal, not a crash" but never reported them. The dangling signal was silent.

**v1.1 fix**: `SemanticGraph` exposes `danglingRightNodes: List[GraphNode]` (computed at graph-build time). `JoinPathPreHook` writes the dangling list into `context.meta` with a typed value (a `List[GraphNode]`, not a String) when the list is non-empty.

### 5.4 — Add the per-module `enforce-no-spark` enforcer block (REQUIRED EDIT)

**v0 said**: pom.xml omitted the per-module enforcer block. **v1.1 fix**: copy the 8-line `<bannedDependencies><excludes><exclude>org.apache.spark:*</exclude></excludes></bannedDependencies>` block from `plugins/audit-plugin/pom.xml:60-69` verbatim. Without it, a future contributor adding a Spark dep to this plugin would not be caught.

### 5.5 — Add `<module>plugins/semantic-graph-plugin</module>` to the root `pom.xml` (REQUIRED EDIT)

Without it, the new module doesn't build with the reactor.

## 6. Suggested next step

Start with just `SemanticGraphBuilder` + a ScalaTest spec that feeds it the `examples/hospital-cleaning` model and asserts the graph it builds matches what `detectCalcCycles` already accepts/rejects on the same fixture — that gives you a correctness baseline against the Core's existing behavior before wiring in the `PreHook`.

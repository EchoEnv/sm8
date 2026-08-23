/*
 * SM8 Semantic Graph Builder.
 *
 * Engine-portable semantic graph over one or more `Model`s. The
 * graph is pure data derived from already-validated `Model`s — no
 * Spark types captured, no closures crossing the executor
 * boundary.
 *
 * Built per request (no cache): JGraphT's
 * `DefaultDirectedWeightedGraph` is documented as not thread-safe.
 * The `Calculator.measureNamesOf` + `Calculator.fieldNamesOf`
 * walkers are reused so the graph's cycle detection matches Core's
 * `QueryBuilder.detectCalcCycles` semantics.
 */
package io.sm8.plugins.semanticgraph

import io.sm8.core.expr.Calculator
import io.sm8.core.model.{CalculatedMeasure, JoinSpec, Model}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.jgrapht.alg.cycle.CycleDetector
import org.jgrapht.alg.shortestpath.DijkstraShortestPath
import org.jgrapht.graph.DefaultDirectedWeightedGraph
import org.jgrapht.graph.DefaultWeightedEdge
import org.jgrapht.graph.concurrent.AsSynchronizedGraph

/**
 * A node in the semantic graph: a field belonging to a named model.
 *
 * `Product with Serializable` is auto-derived from the case class;
 * required for any future capture in a context.meta value (the
 * Context.meta is a `Map[String, Any]` whose values cross the
 * Restate journal boundary; per the project serialization contract,
 * every value must be Serializable).
 */

/**
 * Engine-portable semantic graph over one or more validated `Model`s.
 *
 * Edges:
 *  - calc-measure -> referenced field/measure (weight 0, same model)
 *  - dimension    -> field it derives from (weight 0, same model)
 *  - join column  -> join column on the right-hand model (weight = 1,
 *    or a caller-supplied cost, e.g. estimated row count)
 *
 * Built per request (no cache, per ADR-008-AI v1.1 fix 1). The
 * underlying JGraphT graph is wrapped in `AsSynchronizedGraph` for
 * additional defense — at no measurable cost for typical model sizes.
 *
 * NOT thread-isolated: per ADR-008-AI v1.1 the proposal dropped the
 * cache. Callers that share a graph across threads should re-build.
 *
 * @param g the synchronized graph (constructed via the companion factory)
 * @param loadedModelNames the set of model names that were passed in
 */
final class SemanticGraph private[semanticgraph] (
    private val g: AsSynchronizedGraph[GraphNode, DefaultWeightedEdge],
    loadedModelNames: Set[String]
) {

  /**
   * Shortest join path between two fields, possibly across models.
   *
   * @param from the source vertex
   * @param to   the target vertex
   * @return    the list of vertices along the shortest path from
   *            `from` to `to`, or `None` if no path exists
   */
  def joinPath(from: GraphNode, to: GraphNode): Option[List[GraphNode]] = {
    val path =
      new DijkstraShortestPath[GraphNode, DefaultWeightedEdge](g).getPath(from, to)
    Option(path).map(p => p.getVertexList.asScala.toList)
  }

  /**
   * Reports whether the calc-measure / dimension dependency graph
   * contains a cycle.
   *
   * Duplicates `QueryBuilder.detectCalcCycles` semantics so the
   * plugin can run the check as a pre-flight duplicate before any
   * Connector work, exposing the result via `context.meta` as a
   * typed `EngineError.UnsupportedCapability`.
   *
   * @return `true` if a cycle is detected, `false` otherwise
   */
  def hasCycle: Boolean = new CycleDetector[GraphNode, DefaultWeightedEdge](g)
    .detectCycles()
  /**
   * The deduplicated vertex set of the graph.
   *
   * Exposed for the `GraphPostResolveObserver` hook to project
   * into a wire-stable `GraphSnapshot`. Sorted for determinism
   * (test assertions).
   *
   * @return the sorted list of all vertices in the graph
   */
  def vertices: List[GraphNode] =
    g.vertexSet.asScala.toList.sortBy(n => (n.model, n.field))

  /**
   * The directed-edge set of the graph.
   *
   * Exposed for the `GraphPostResolveObserver` hook to project
   * into a wire-stable `GraphSnapshot`. Each tuple is
   * `(from, to)`. Sorted for determinism.
   *
   * @return the sorted list of directed edges
   */
  def edges: List[(GraphNode, GraphNode)] =
    g.edgeSet.asScala.toList
      .map(e => (g.getEdgeSource(e), g.getEdgeTarget(e)))
      .sortBy { case (a, b) => (a.model, a.field, b.model, b.field) }

  /**
   * The right-model references that did not resolve to a loaded
   * model (the cross-catalog case).
   *
   * Surfaced as a typed `List[GraphNode]` via `context.meta`
   * when non-empty.
   *
   * @return the deduplicated list of dangling right-nodes
   */
  def danglingRightNodes: List[GraphNode] = {
    val buf = mutable.ListBuffer.empty[GraphNode]
    g.vertexSet.asScala.foreach { v =>
      if (!loadedModelNames.contains(v.model)) buf += v
    }
    buf.toList.distinct
  }
}

/**
 * Companion factory: build a typed semantic graph over one or many
 * validated `Model`s.
 *
 * Per ADR-008-AI v1.1: NO cache. Each `build*` call constructs a fresh
 * graph (sub-millisecond for realistic model sizes; per
 * `scala-perf-testing-mindset` the cache buys nothing and adds a
 * thread-safety footgun).
 */
object SemanticGraphBuilder {

  /**
   * Build a graph over a single model: calc-measure deps + dimension
   * field refs. No join edges (those need the right-hand model too —
   * use `buildAcross` for that).
   *
   * @param model the model to graph; must already be `Model.of(...)`-validated
   * @return      the semantic graph
   */
  def build(model: Model): SemanticGraph = buildAcross(model :: Nil)

  /**
   * Build a graph over multiple models, including join edges between
   * them.
   *
   * `models` should already be `Model.of(...)`-validated. The graph
   * is built fresh on every call (no cache) per the project
   * decision to avoid JGraphT's thread-safety hazard.
   *
   * @param models the models to graph
   * @return       the semantic graph covering all supplied models
   */
  def buildAcross(models: List[Model]): SemanticGraph = {
    val byName: Map[String, Model] = models.map(m => m.name -> m).toMap

    val raw = new DefaultDirectedWeightedGraph[GraphNode, DefaultWeightedEdge](
      classOf[DefaultWeightedEdge]
    )
    val g = new AsSynchronizedGraph(raw)

    def addNode(n: GraphNode): Unit = if (!g.containsVertex(n)) g.addVertex(n)

    // A `Dimension` whose `expr` is a `FieldRef` of the SAME name
    // produces a self-loop (`dimAmount -> "amount" -> dimAmount`).
    // JGraphT's `CycleDetector.detectCycles()` would report this as a
    // cycle, but the dimension IS that field — the self-loop is
    // intentional (a dimension DERIVED from its own field is a
    // no-op), not a cycle. `QueryBuilder.detectCalcCycles` only
    // walks the calc-measure DAG and would not flag this case, so
    // we match its semantics here.
    //
    // A calc-measure that references ITSELF (e.g. `bad = bad + 1`)
    // IS a real cycle and MUST be reported.
    def addEdge(a: GraphNode, b: GraphNode, w: Double): Unit = {
      addNode(a)
      addNode(b)
      val e = g.addEdge(a, b)
      if (e != null) g.setEdgeWeight(e, w)
    }

    def addDimEdge(a: GraphNode, b: GraphNode, w: Double): Unit =
      if (a == b) () else addEdge(a, b, w)

    models.foreach { model =>
      // Calculated measures -> whatever they reference (reuses the
      // SAME walkers QueryBuilder.detectCalcCycles already trusts).
      model.calculatedMeasures.foreach { c: CalculatedMeasure =>
        val refs = Calculator.measureNamesOf(c.expr) ++
          Calculator.fieldNamesOf(c.expr)
        refs.foreach(r =>
          addEdge(GraphNode(model.name, c.name), GraphNode(model.name, r), 0)
        )
      }

      // Dimensions -> the fields their expr touches. Use `addDimEdge`
      // (NOT `addEdge`) so that self-referential dimensions
      // (`dimAmount -> FieldRef("amount")` where the dim IS that
      // field) don't trip the cycle detector. See addDimEdge comment.
      model.dimensions.foreach { d =>
        Calculator.fieldNamesOf(d.expr).foreach(r =>
          addDimEdge(GraphNode(model.name, d.name), GraphNode(model.name, r), 0)
        )
      }

      // Joins -> edges to the right-hand model's key columns.
      // Weighted 1 by default; swap in a real cardinality estimate
      // once one is available (feeds broadcast-plugin / skew-plugin).
      // Per ADR-008-AI v1.1 fix 3: dangling right-model references
      // are surfaced via `danglingRightNodes` rather than crashing.
      model.joins.foreach { js: JoinSpec =>
        js.keys.foreach { case (leftKey, rightKey) =>
          addEdge(
            GraphNode(model.name, leftKey),
            GraphNode(js.rightModel, rightKey),
            1.0
          )
        }
      }
    }

    new SemanticGraph(g, byName.keySet)
  }
}
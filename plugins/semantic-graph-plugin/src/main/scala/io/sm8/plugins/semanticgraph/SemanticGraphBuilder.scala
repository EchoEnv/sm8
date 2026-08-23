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
 * @param estimatedPairs the join-edge endpoint pairs that carry a
 *                       user-supplied estimate (a membership predicate,
 *                       distinct from the edge weight — the placeholder
 *                       1.0 and a real estimate of 1 row share a weight)
 */
final class SemanticGraph private[semanticgraph] (
    private val g: AsSynchronizedGraph[GraphNode, DefaultWeightedEdge],
    loadedModelNames: Set[String],
    private val estimatedPairs: Set[(GraphNode, GraphNode)]
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
   * The models that reference `targetModel` via a join edge.
   *
   * A join edge points FROM the left model's key field TO the
   * right-hand model's key field, so a join referencing model X is
   * an edge whose target vertex belongs to X and whose source vertex
   * belongs to a DIFFERENT model. Same-model edges (calc-measure /
   * dimension field refs) are excluded by construction: their source
   * and target share the model name.
   *
   * Cross-model discovery (RFC 'Where a semantic graph earns its
   * place'): answers "which models reference model X via a join" as
   * a query over the same graph that powers join-path resolution
   * and impact analysis.
   *
   * @param targetModel the model name whose referencing models to find
   * @return            the sorted distinct source models with a join
   *                    edge into `targetModel`
   */
  def referencingModels(targetModel: String): List[String] =
    g.edgeSet.asScala.toList
      .map(e => (g.getEdgeSource(e), g.getEdgeTarget(e)))
      .collect {
        case (from, to) if to.model == targetModel && from.model != targetModel =>
          from.model
      }
      .distinct
      .sorted

  /**
   * The user-supplied cardinality estimate for a single
   * left-field -> right-field join edge.
   *
   * The number comes from the graph edge weight (the edge is the
   * single source of truth; join edges are the only non-zero-weight
   * edges), but only endpoint pairs declared with an estimate are
   * reported. The placeholder weight `1.0` and a real estimate of
   * 1 row are indistinguishable by weight alone, so membership in
   * `estimatedPairs` gates the answer — the predicate is derived in
   * the same loop that adds the edges, so it cannot drift from the
   * graph.
   *
   * @param from the left-hand field node of the join edge
   * @param to   the right-hand field node of the join edge
   * @return    the user-declared row-count estimate, or `None` when
   *            the pair is not an estimated join edge
   */
  def joinCardinality(from: GraphNode, to: GraphNode): Option[Long] =
    if (estimatedPairs.contains((from, to)))
      g.getAllEdges(from, to).asScala.headOption.map(g.getEdgeWeight(_).toLong)
    else None

  /**
   * All user-declared join cardinality estimates, keyed by the
   * join edge endpoints.
   *
   * The values come from the edge weights; the key set is the
   * estimated-pairs predicate. For a (from, to) pair with multiple
   * edges (duplicate key-pair joins), the first edge's weight
   * wins, matching the edge builder's first-wins. Snapshotted into
   * `GraphSnapshot.joinCardinalities` for the meta-inspector.
   * Sorted for determinism (test assertions).
   *
   * @return sorted map of join-edge endpoints -> estimate
   */
  def joinCardinalities: Map[(GraphNode, GraphNode), Long] =
    estimatedPairs.toList.flatMap { case (a, b) =>
      g.getAllEdges(a, b).asScala.headOption.map { e =>
        ((a, b), g.getEdgeWeight(e).toLong)
      }
    }.toMap
      .toList
      .sortBy { case ((a, b), _) => (a.model, a.field, b.model, b.field) }
      .toMap
      .toMap

  /**
   * The reverse-closure: every node that transitively depends on
   * `node` (i.e. every node reachable by following incoming edges
   * backward from `node`).
   *
   * Used for impact analysis: "which calculated measures / models
   * break if this dimension changes?" — answered by calling
   * `dependents(dimensionNode)`.
   *
   * @param node the node whose dependents to compute
   * @return the sorted list of nodes that transitively depend on
   *         the given node (the node itself is NOT included)
   */
  def dependents(node: GraphNode): List[GraphNode] = {
    val result = scala.collection.mutable.LinkedHashSet.empty[GraphNode]
    // `incomingEdgesOf(v)` returns edges POINTING TO v (target = v).
    // `getEdgeSource` on those edges returns the nodes that
    // transitively depend on `node` (via these incoming edges).
    // The walk does a BFS: frontier -> next = sources-of-incoming-edges
    // for each node in frontier -> recurse on next.
    val initialSources =
      g.incomingEdgesOf(node).asScala.map(g.getEdgeSource).toSet -- Set(node)
    result ++= initialSources
    def walk(frontier: Set[GraphNode], seen: Set[GraphNode]): Unit =
      if (frontier.nonEmpty) {
        val next = frontier.flatMap { n =>
          g.incomingEdgesOf(n).asScala.map(g.getEdgeSource)
        } -- seen -- Set(node)
        result ++= next
        walk(next, seen ++ next)
      }
    walk(initialSources, initialSources)
    result.toList.sortBy(n => (n.model, n.field))
  }


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

    // The self-loop skip still adds the endpoint vertices (the
    // dimension IS that field — it should be discoverable via
    // dependents()). Without addNode, JGraphT's incomingEdgesOf
    // would throw "no such vertex" for self-referential dims.
    def addDimEdge(a: GraphNode, b: GraphNode, w: Double): Unit = {
      addNode(a)
      addNode(b)
      if (a == b) () else addEdge(a, b, w)
    }

    // Join-edge estimate bookkeeping (see the join loop below):
    // `seenPairs` records which endpoint pairs already have an edge
    // (JGraphT first-wins), `estimatedPairs` records the pairs whose
    // EDGE-CREATING join declared an estimate.
    val estimatedPairs = mutable.Set.empty[(GraphNode, GraphNode)]
    val seenPairs = mutable.Set.empty[(GraphNode, GraphNode)]

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

      // Join edges: weighted by the user-declared cardinality
      // estimate when present, else the 1.0 placeholder (nothing to
      // guess). The estimated-pairs predicate is built in this same
      // first-wins loop that creates the edges, so membership always
      // matches the join whose weight the graph carries — it cannot
      // diverge from the edge state. A pair whose winning edge is a
      // placeholder (the first alias declared no estimate) is NOT in
      // the predicate: joinCardinality returns None for it, per the
      // ADR "the edge is a join-edge with a user-supplied estimate".
      //
      // Per ADR-008-AI v1.1 fix 3: dangling right-model references
      // are surfaced via `danglingRightNodes` rather than crashing.
      model.joins.foreach { js: JoinSpec =>
        js.keys.foreach { case (leftKey, rightKey) =>
          val from = GraphNode(model.name, leftKey)
          val to = GraphNode(js.rightModel, rightKey)
          val firstSeen = seenPairs.add((from, to))
          // addEdge first-wins: JGraphT addEdge returns null for an
          // already-present pair, so setEdgeWeight is skipped and the
          // FIRST alias's weight is kept.
          addEdge(from, to, js.estimatedRows.getOrElse(1L).toDouble)
          // Membership only when the edge-creating join is the one
          // declaring an estimate.
          if (firstSeen && js.estimatedRows.isDefined) estimatedPairs += ((from, to))
        }
      }
    }

    new SemanticGraph(g, byName.keySet, estimatedPairs.toSet)
  }
}

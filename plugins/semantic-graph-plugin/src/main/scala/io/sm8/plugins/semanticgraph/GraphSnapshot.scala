/*
 * SM8 Semantic Graph Snapshot.
 *
 * The plugin owns the schema for the semantic graph. The wire DTO
 * that the transport serves is the same type as the plugin's
 * snapshot — no projection through a separate wire type. The
 * plugin's `GraphSnapshot` is Jackson-friendly (`String` + `List`
 * + `Option` only) so the transport can serve it as
 * `Map[String, Any]` without a separate wire DTO.
 *
 * The transport sees only `Map[String, Any]` keyed by the
 * well-known namespaced key (see `GraphSnapshot.MetaKey`).
 */
package io.sm8.plugins.semanticgraph

import io.sm8.core.engine.EngineError

/**
 * A vertex in the semantic graph: a field belonging to a named
 * model.
 *
 * `Product with Serializable` is auto-derived from the case
 * class; required for the Restate journal capture (any value in
 * `context.meta` that crosses the journal boundary must be
 * Serializable per the established project contract).
 */
final case class GraphNode(model: String, field: String)

/**
 * The wire-stable snapshot of a model's semantic graph.
 *
 * Published by `GraphPostResolveObserver` into `context.meta` at
 * the well-known key `GraphSnapshot.MetaKey`. Any transport
 * (HTTP, MCP, CLI) can read this snapshot generically via the
 * platform's meta-inspector — they don't need to know about
 * graphs.
 *
 * `cycleError` is a typed `Option[EngineError]`, not a String.
 * The type uses only `String` + `List` + `Option` + case classes
 * — serializes cleanly through Jackson with `DefaultScalaModule`.
 */
final case class GraphSnapshot(
    vertices: List[GraphNode],
    edges: List[(GraphNode, GraphNode)],
    hasCycle: Boolean,
    cycleError: Option[EngineError],
    danglingRightNodes: List[GraphNode],
    // Impact analysis — the reverse-closure of every node (i.e.
    // every node that transitively depends on this node). Answers
    // "which calculated measures break if this dimension changes?"
    // via the meta-inspector.
    dependents: Map[GraphNode, List[GraphNode]] = Map.empty,
    // Join cardinality — the user-declared row-count estimate per
    // join edge (keyed by edge endpoints). Consulted by the
    // broadcast/skew decision path; only joins that declared an
    // estimate appear here.
    joinCardinalities: Map[(GraphNode, GraphNode), Long] = Map.empty
) {
  /**
   * Projects the snapshot to a `Map[String, Any]` for the wire
   * boundary. Jackson serializes this directly.
   *
   * @return the wire-stable map representation
   */
  def toMetaValue: Map[String, Any] = Map(
    "hasCycle" -> hasCycle,
    "cycleError" -> cycleError.map(_.toString),
    "danglingRightNodes" -> danglingRightNodes.map(n =>
      Map("model" -> n.model, "field" -> n.field)
    ),
    "dependents" -> dependents.map { case (node, deps) =>
      Map("node" -> Map("model" -> node.model, "field" -> node.field),
          "dependents" -> deps.map(d => Map("model" -> d.model, "field" -> d.field)))
    }.toList
      .sortBy(m => nodeKey(m, "node")),
    "joinCardinalities" -> joinCardinalities.map { case ((from, to), est) =>
      Map("from" -> Map("model" -> from.model, "field" -> from.field),
          "to"   -> Map("model" -> to.model,   "field" -> to.field),
          "estimatedRows" -> est)
    }.toList
      .sortBy(m => (nodeKey(m, "from"), nodeKey(m, "to")))
  )

  /**
   * Extracts the deterministic sort key for an inner `node` (or
   * `from`/`to`) map: `(model, field)` as a `(String, String)` pair.
   *
   * Centralizes the once-unchecked `asInstanceOf[Map[String, Any]]`
   * that the prior sort lambdas relied on. If the literal shape
   * ever drifts away from `Map("model" -> String, "field" -> String)`,
   * the cast fails fast and locally — not silently at every sort site.
   *
   * @param inner the inner map keyed by `"model"` and `"field"`
   * @param key   the outer key whose value is the inner map
   *              (`"node"`, `"from"`, or `"to"`)
   * @return      the `(model, field)` pair for sorting
   */
  private def nodeKey(inner: Map[String, Any], key: String): (String, String) = {
    val m = inner(key).asInstanceOf[Map[String, Any]]
    (m("model").toString, m("field").toString)
  }
}

object GraphSnapshot {

  /**
   * The well-known `context.meta` key for the latest
   * semantic-graph snapshot. Namespaced keys prevent collision
   * with other plugins.
   *
   * @return the namespaced meta key
   */
  val MetaKey: String = "io.sm8.plugins.semanticgraph:graph-snapshot"
}
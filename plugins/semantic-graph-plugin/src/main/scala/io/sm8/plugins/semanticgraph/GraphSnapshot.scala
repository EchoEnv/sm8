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
    // PR-161: impact analysis — the reverse-closure of every
    // node (i.e. every node that transitively depends on this
    // node). Answers "which calculated measures break if this
    // dimension changes?" via the meta-inspector.
    dependents: Map[GraphNode, List[GraphNode]] = Map.empty
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
      .sortBy(m => (m("node").asInstanceOf[Map[String, Any]]("model").toString,
                     m("node").asInstanceOf[Map[String, Any]]("field").toString))
  )
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
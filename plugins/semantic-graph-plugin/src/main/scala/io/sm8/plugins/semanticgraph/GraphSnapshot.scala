/*
 * SM8 Semantic Graph Snapshot (PR-150, ADR-008-AI follow-up).
 *
 * Per the architect's 2026-08-23 design review
 * (`docs/review/graph-display-design-review.md`, verdict
 * REVERT_AND_RESHAPE): the **plugin owns the schema** for the
 * semantic graph. The wire DTO that the transport serves is the
 * SAME type as the plugin's snapshot — no projection through a
 * separate `GraphNodeSnapshot`. The plugin's `GraphSnapshot` is
 * Jackson-friendly (`String` + `List` + `Option` only) so the
 * transport can serve it as `Map[String, Any]` without a separate
 * wire DTO.
 *
 * Per  SS1 ("smallest correct
 * core"): one type, owned by the plugin. The transport
 * (`sm8-platform`) sees only `Map[String, Any]` keyed by the
 * well-known namespaced key (see `GraphSnapshot.MetaKey`).
 */
package io.sm8.plugins.semanticgraph

import io.sm8.core.engine.EngineError

/**
 * A vertex in the semantic graph: a field belonging to a named model.
 *
 * `Product with Serializable` is auto-derived from the case class;
 * required for the Restate journal capture (any value in
 * `context.meta` that crosses the journal boundary must be
 * Serializable per the established project contract).
 */
final case class GraphNode(model: String, field: String)

/**
 * The wire-stable snapshot of a model's semantic graph.
 *
 * Published by `GraphPostResolveObserver` into `context.meta` at the
 * well-known key `GraphSnapshot.MetaKey`. Any transport (HTTP, MCP,
 * CLI) can read this snapshot generically via the platform's
 * `MetaInspectorService` — they don't need to know about graphs.
 *
 * Per  SS1 ("errors are data"):
 * `cycleError` is a typed `Option[EngineError]`, not a String.
 * Per  SS1 (Jackson-friendly):
 * the type uses only `String` + `List` + `Option` + case classes —
 * serializes cleanly through Jackson with `DefaultScalaModule`.
 */
final case class GraphSnapshot(
    vertices: List[GraphNode],
    edges: List[(GraphNode, GraphNode)],
    hasCycle: Boolean,
    cycleError: Option[EngineError],
    danglingRightNodes: List[GraphNode]
) {
  // Per  SS1 ("Map[String, Any]"
  // round-trip): the snapshot projects itself to a `Map[String, Any]`
  // at the wire boundary. Jackson serializes this directly. The
  // transport layer's `MetaInspectorService` returns this map.
  def toMetaValue: Map[String, Any] = Map(
    "vertices" -> vertices.map(n => Map("model" -> n.model, "field" -> n.field)),
    "edges" -> edges.map { case (from, to) =>
      Map(
        "from" -> Map("model" -> from.model, "field" -> from.field),
        "to"   -> Map("model" -> to.model,   "field" -> to.field)
      )
    },
    "hasCycle" -> hasCycle,
    "cycleError" -> cycleError.map(_.toString),
    "danglingRightNodes" -> danglingRightNodes.map(n =>
      Map("model" -> n.model, "field" -> n.field)
    )
  )
}

object GraphSnapshot {

  /**
   * The well-known `context.meta` key for the latest semantic-graph
   * snapshot. Per  SS1 (namespaced keys
   * prevent collision with other plugins): every plugin that writes
   * a snapshot should namespace its key under its plugin id.
   */
  val MetaKey: String = "io.sm8.plugins.semanticgraph:graph-snapshot"
}
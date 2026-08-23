/*
 * SM8 Semantic Graph PostResolve Observer Hook.
 *
 * Per `architecture-spec hooks.md §6` "Types of Hooks, by What
 * They Do": this is an Observer — "Reads context, does not modify
 * it [in pipeline-relevant ways], causes an external effect". The
 * external effect here is "publish the graph snapshot for an
 * out-of-band consumer (HTTP/MCP/CLI)".
 *
 * The transport layer exposes a generic meta-inspector that reads
 * any `context.meta` key; this hook writes the snapshot, the
 * transport just serves it. The plugin owns the schema; the
 * transport knows nothing about graphs.
 *
 * Per  SS1 (run on every request but
 * return quickly): the snapshot is built per request (sub-ms).
 */
package io.sm8.plugins.semanticgraph

import io.sm8.core.engine.{EngineError, EngineHookRequest}
import io.sm8.core.model.Model
import io.sm8.sdk.{Context, HookStage, PostHook}

/**
 * `post:resolve` Observer hook. Builds a fresh `GraphSnapshot`
 * for the request's `Model` and writes it into `context.meta` at
 * the well-known `GraphSnapshot.MetaKey`. A transport-layer
 * meta-inspector reads this key and serves the snapshot over
 * HTTP / MCP / CLI.
 *
 * Priority 120 (first-party range 100-899) — runs after core
 * (0-99) hooks and after any other first-party hooks. The
 * build is O(N·M) for N calc-measures × M refs each (well under
 * 1ms for realistic models).
 */
final class GraphPostResolveObserver extends PostHook with java.io.Serializable {
  override val name: String = "semantic-graph-snapshot"
  override val priority: Int = 120
  override val stage: HookStage = HookStage.PostResolve

  /**
   * Builds a fresh `GraphSnapshot` for the request's `Model` and
   * writes it into `context.meta` at the well-known
   * `GraphSnapshot.MetaKey`. Non-`EngineHookRequest` requests are
   * passed through unchanged.
   *
   * @param context the hook context (the shared `Context` for the
   *                current request)
   * @return        the context with the snapshot added to `meta`
   */
  override def run(context: Context): Context = context.request match {
    case EngineHookRequest(model: Model, _, _) =>
      val graph = SemanticGraphBuilder.build(model)

      // Cycle as a typed EngineError value; dangling right-nodes
      // as a typed List[GraphNode].
      val cycleError: Option[EngineError] =
        if (graph.hasCycle)
          Some(
            EngineError.UnsupportedCapability(
              engine = "semantic-graph-plugin",
              capability = "SemanticGraph.cycle",
              message =
                s"Cycle detected in model '${model.name}' dependency graph"
            )
          )
        else None

      // Impact analysis: compute the reverse-closure for every
      // node. The user answers "which calc-measures break if I
      // change dimension X?" by inspecting `dependents(dim X)`
      // from the meta-inspector. Sub-ms for realistic models.
      val dependentsMap: Map[GraphNode, List[GraphNode]] =
        graph.vertices.iterator.map { n => n -> graph.dependents(n) }.toMap

      val snapshot = GraphSnapshot(
        vertices = graph.vertices,
        edges = graph.edges,
        hasCycle = graph.hasCycle,
        cycleError = cycleError,
        danglingRightNodes = graph.danglingRightNodes,
        dependents = dependentsMap,
        joinCardinalities = graph.joinCardinalities
      )

      context.copy(meta = context.meta + (GraphSnapshot.MetaKey -> snapshot))
    case _ => context
  }
}
/*
 * SM8 Semantic Graph PostResolve Observer Hook (PR-150, ADR-008-AI
 * follow-up).
 *
 * Per the architect's 2026-08-23 design review
 * (`docs/review/graph-display-design-review.md`): the plugin owns
 * the graph-display feature. The transport layer exposes a generic
 * `MetaInspectorService` that reads any `context.meta` key — the
 * plugin writes the snapshot, the transport just serves it.
 *
 * Per `architecture-spec hooks.md §6` "Types of Hooks, by What They
 * Do": this is an **Observer** — "Reads context, does not modify it
 * [in pipeline-relevant ways], causes an external effect". The
 * external effect here is "publish the graph snapshot for an
 * out-of-band consumer (HTTP/MCP/CLI)".
 *
 * Per  SS1 (run on every request but
 * return quickly): the snapshot is built per request (sub-ms,
 * no cache per ADR-008-AI v1.1 fix 1).
 */
package io.sm8.plugins.semanticgraph

import io.sm8.core.engine.{EngineError, EngineHookRequest}
import io.sm8.core.model.Model
import io.sm8.sdk.{Context, HookStage, PostHook}

/**
 * `post:resolve` Observer hook. Builds a fresh `GraphSnapshot` for
 * the request's `Model` and writes it into `context.meta` at the
 * well-known `GraphSnapshot.MetaKey`. A transport-layer
 * `MetaInspectorService` reads this key and serves the snapshot
 * over HTTP / MCP / CLI.
 *
 * Priority 120 — first-party range (100-899), runs after core
 * (0-99) hooks and after any other first-party `PreHook`s. The
 * build is O(N·M) for N calc-measures × M refs each (well under
 * 1ms for realistic models).
 */
final class GraphPostResolveObserver extends PostHook with java.io.Serializable {
  override val name: String = "semantic-graph-snapshot"
  override val priority: Int = 120
  override val stage: HookStage = HookStage.PostResolve

  override def run(context: Context): Context = context.request match {
    case EngineHookRequest(model: Model, _, _) =>
      val graph = SemanticGraphBuilder.build(model)

      // Per ADR-008-AI v1.1 fix 2: typed EngineError value (not
      // String). Per fix 3: dangling right-nodes as typed
      // List[GraphNode].
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

      val snapshot = GraphSnapshot(
        vertices = graph.vertices,
        edges = graph.edges,
        hasCycle = graph.hasCycle,
        cycleError = cycleError,
        danglingRightNodes = graph.danglingRightNodes
      )

      context.copy(meta = context.meta + (GraphSnapshot.MetaKey -> snapshot))
    case _ => context
  }
}
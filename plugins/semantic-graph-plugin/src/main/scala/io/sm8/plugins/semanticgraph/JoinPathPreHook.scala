/*
 * SM8 Semantic Graph PreHook (PR-149, ADR-008-AI).
 *
 * Per ADR-008-AI v1.1:
 *   - Cycle detection surfaces a TYPED EngineError.UnsupportedCapability
 *     value (NOT a String) into context.meta. Per
 *      SS1 ("errors are data"):
 *     the hook matches the convention established by
 *     QueryBuilder.detectCalcCycles.
 *   - Dangling right-model nodes surface as a typed List[GraphNode]
 *     value (NOT a String).
 *
 * Per  SS1 (priority range): 120 is in
 * the first-party range (100-899). Runs after Core validation (0-99)
 * and before any Connector-specific resolve work.
 *
 * Per  SS1 (closure-safety): the hook
 * reads `context.request`, builds a fresh SemanticGraph per call
 * (no cache, per ADR-008-AI v1.1), and writes results to context.meta.
 * No Spark types captured; no closures crossing the executor boundary.
 */
package io.sm8.plugins.semanticgraph

import io.sm8.core.engine.{EngineError, EngineHookRequest}
import io.sm8.core.model.Model
import io.sm8.sdk.{Context, HookStage, PreHook}

/**
 * `pre:resolve` hook. Builds a fresh `SemanticGraph` for the request's
 * `Model` and reports cycle + dangling-edge findings via `context.meta`
 * (typed values, per ADR-008-AI v1.1).
 *
 * Sets `context.stop = true` ONLY when a cycle is detected, matching
 * the fail-fast policy the existing `PostHook`/`PreHook` docs already
 * specify (RFC §9).
 *
 * Priority 120 — first-party range (100-899), runs after core (0-99)
 * validation, before any Connector-specific resolve work.
 */
final class JoinPathPreHook extends PreHook with java.io.Serializable {
  override val name: String = "semantic-graph-precheck"
  override val priority: Int = 120
  override val stage: HookStage = HookStage.PreResolve

  /**
   * Meta key for the typed cycle-detection error. Value type:
   * `EngineError.UnsupportedCapability` (NOT String — per
   * `scala-error-handling-mindset`).
   */
  /** Meta key written into `ctx.meta` for the typed cycle-detection
    * `EngineError`. ADR-0020: the platform collects any `ctx.meta`
    * entry whose key ends in `":error"` AND whose value is a typed
    * `EngineError`, so this key satisfies the convention. The legacy
    * `"semanticGraphError"` literal did NOT end in `":error"` and
    * would have been silently dropped by the upgraded platform;
    * migrating to the namespaced form is the backward-compat fix.
    */
  val CycleErrorKey = "io.sm8.plugins.semanticgraph:error"

  /**
   * Meta key for the typed dangling-right-nodes list. Value type:
   * `List[GraphNode]` (NOT String).
   */
  val DanglingKey = "semanticGraphDangling"

  override def run(context: Context): Context = context.request match {
    case EngineHookRequest(model: Model, _, _) =>
      val graph = SemanticGraphBuilder.build(model)

      // Per ADR-008-AI v1.1 fix 3: surface dangling right-nodes as a
      // typed List[GraphNode] (NOT a String).
      val dangling = graph.danglingRightNodes
      val withDangling =
        if (dangling.isEmpty) context
        else context.copy(meta = context.meta + (DanglingKey -> dangling))

      if (graph.hasCycle) {
        // Per ADR-008-AI v1.1 fix 2: typed EngineError.UnsupportedCapability.
        val cycleError = EngineError.UnsupportedCapability(
          engine = "semantic-graph-plugin",
          capability = "SemanticGraph.cycle",
          message =
            s"Cycle detected in model '${model.name}' dependency graph"
        )
        withDangling.copy(
          stop = true,
          meta = withDangling.meta + (CycleErrorKey -> cycleError)
        )
      } else {
        withDangling
      }
    case _ => context // not an EngineHookRequest — nothing to check
  }
}
/*
 * SM8 SDK — Context.
 *
 * The single shared data object that carries state through the pipeline.
 * The Core writes its output into Context; every extension reads from and
 * optionally writes to Context. Extensions never pass data to each other
 * directly — they communicate only through Context.
 *
 * Per RFC §7 (semantic-layer-engine-architecture.md) and the Step-1 plan:
 *   - stage   : current pipeline stage name (parse | resolve | execute | format)
 *   - request : original input, READ-ONLY by convention (RFC hooks.md Rule 2)
 *   - result  : written by execute/format stages; read by PostHooks and Transformers
 *   - meta    : scratch space for plugin-to-plugin communication
 *   - stop    : short-circuit flag; when true, no further stages or hooks run
 *
 * Frozen after Step 1. Any change to field set is a breaking SDK change.
 */
package io.sm8.sdk

/**
 * Shared state carried through the 4-stage pipeline (parse → resolve →
 * execute → format). Immutable snapshot at each stage boundary; hooks
 * receive a Context, return a (possibly mutated) Context.
 *
 * @param stage    current pipeline stage name
 * @param request  original input; do not mutate
 * @param result   output of execute/format; None until execute runs
 * @param meta     scratch space for cross-hook data sharing
 * @param stop     when true, the engine skips all remaining stages and hooks
 */
final case class Context(
    stage: PipelineStage,
    request: Request,
    result: Option[Result] = None,
    meta: Map[String, Any] = Map.empty,
    stop: Boolean = false
)

/**
 * The four named pipeline stages. Sealed — the engine cannot grow new stages
 * without a Core change (the pipeline shape is Core, per karpathy §1.2).
 */
sealed trait PipelineStage
object PipelineStage {
  case object Parse   extends PipelineStage
  case object Resolve extends PipelineStage
  case object Execute extends PipelineStage
  case object Format  extends PipelineStage
}

/**
 * Marker trait for the request type. The full request shape lands in Step 3
 * (Engine skeleton); for Step 1 we keep it abstract so the SDK compiles.
 *
 * Plugin authors should treat any subtype of Request as opaque input from
 * the consumer; they should NOT pattern-match on the concrete case unless
 * they themselves produced it.
 */
sealed trait Request

/**
 * Marker trait for the result type. Same status as Request — full shape
 * in Step 3.
 */
sealed trait Result
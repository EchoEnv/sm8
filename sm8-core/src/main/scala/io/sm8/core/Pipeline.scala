/*
 * SM8 Core — Pipeline.
 *
 * Runs the 4-stage pipeline (parse → resolve → execute → format) on
 * a Context. Step 3 is the minimal skeleton: each stage has its
 * default body; real hook dispatch lands in Step 4; real parse /
 * resolve bodies land in Step 0 when the IR moves in.
 *
 * Per [[scala-data-driven-refactor-mindset]] step 3 ("default to
 * sealed-trait/match over Map-based rule tables"): the 4 pipeline
 * stages are a sealed `Stage` hierarchy. The pipeline runner walks
 * a `List[Stage]` (DATA, not control flow) via `foldLeft`. Adding
 * a stage = adding a case class to `Stage` + adding it to
 * `Stage.All`. The compiler enforces both.
 *
 * Per [[scala-jvm-safety-mindset]]: the Context is immutable
 * (`val`, case class, no `var`). The pipeline is `foldLeft`-pure —
 * no shared mutable state, safe under concurrency.
 *
 * The Pipeline is internal (lives in `io.sm8.core`). Plugin authors
 * never construct a Pipeline directly — they go through
 * `Engine.run(request)`.
 */
package io.sm8.core

import io.sm8.sdk._

/**
 * Environment passed to each Stage at runtime. Bundles the
 * registries the stages need (per [[scala-data-driven-refactor-mindset]]
 * — data, not constructor-args scattered through each stage).
 */
final case class StageEnv(
    connectors: ConnectorRegistry,
    transformers: TransformerRegistry
)

/**
 * One pipeline stage. Each stage is a pure function
 * `Context => Context` — no side effects on shared state.
 *
 * Adding a stage = add a case object to `Stage` + add it to
 * `Stage.All`. The compiler enforces both (sealed trait forces
 * exhaustiveness; the runner iterates `All`).
 */
sealed trait Stage {
  /** Which `PipelineStage` value this Stage represents. */
  def name: PipelineStage

  /**
   * The stage body. Pure function `Context => Context`. By-name
   * hooks fire here when Step 4 lands (pre-hooks may set `stop =
   * true` to short-circuit).
   */
  def run(env: StageEnv)(ctx: Context): Context
}

object Stage {

  /**
   * Parse — convert raw request into an internal query representation.
   * Step 3: no-op. Step 0 will add YAML/JSON lowering here.
   */
  case object Parse extends Stage {
    override def name: PipelineStage = PipelineStage.Parse
    override def run(env: StageEnv)(ctx: Context): Context = ctx
  }

  /**
   * Resolve — pick which adapter(s) will serve the request.
   * Step 3: no-op (resolution is by connector name in
   * `ConnectorRequest`; real IR-driven resolution lands in Step 0).
   */
  case object Resolve extends Stage {
    override def name: PipelineStage = PipelineStage.Resolve
    override def run(env: StageEnv)(ctx: Context): Context = ctx
  }

  /**
   * Execute — run the query against the chosen adapter(s). Step 3:
   * routes `ConnectorRequest` to the named Connector, builds a
   * `ConnectorResult`. Unknown connector names produce a stub
   * empty result (real typed error in Step 0).
   */
  case object Execute extends Stage {
    override def name: PipelineStage = PipelineStage.Execute
    override def run(env: StageEnv)(ctx: Context): Context = ctx.request match {
      case ConnectorRequest(connectorName, query) =>
        env.connectors.get(connectorName) match {
          case Some(c) =>
            val rows = c.query(query)
            val sch  = c.schema()
            ctx.copy(result = Some(ConnectorResult(connectorName, sch, rows)))
          case None =>
            // Unknown connector — surface as a stub Result. Real
            // typed errors (EngineError.EngineUnavailable) land
            // in Step 0.
            ctx.copy(result = Some(ConnectorResult(
              connectorName, ConnectorSchema(Nil), ResultRows(Vector.empty))))
        }
      case other => ctx // unknown request type — pass through unchanged
    }
  }

  /**
   * Format — shape the raw result into the response. Step 3:
   * invokes the active Transformer if one is registered; otherwise
   * the Context passes through.
   */
  case object Format extends Stage {
    override def name: PipelineStage = PipelineStage.Format
    override def run(env: StageEnv)(ctx: Context): Context =
      env.transformers.active.fold(ctx)(_.transform(ctx))
  }

  /** The 4 stages, in execution order. Adding a stage = add a case + add here. */
  val All: List[Stage] = List(Parse, Resolve, Execute, Format)
}

/**
 * The 4-stage pipeline runner. Stateless — created fresh per
 * `Engine.run(request)` call.
 *
 * Pipeline stages are DATA (`Stage.All`), not control flow. The
 * runner is a single `foldLeft` — adding a stage is data, not code.
 */
final class Pipeline(
    connectors: ConnectorRegistry,
    hooks: HookManager,
    transformers: TransformerRegistry
) {

  /** Bundled environment for Stage.run. */
  private val env: StageEnv = StageEnv(connectors, transformers)

  /**
   * Run `request` through all stages. Returns the final Result.
   *
   * Pure: `foldLeft` over an immutable List of Stages produces a
   * new Context at each step. No `var`, no shared mutable state.
   *
   * @param request the Request to run
   * @return the final Result (the last Context's `result`, or a
   *         stub empty result if no stage set one)
   */
  def run(request: Request): Result = {
    val initial = Context(
      stage    = PipelineStage.Parse,
      request  = request,
      result   = None,
      meta     = Map.empty,
      stop     = false
    )

    val finalCtx = Stage.All.foldLeft(initial) { (ctx, stage) =>
      // Hook dispatch lands in Step 4 (pre-hooks, stop short-circuit,
      // post-hooks). Step 3: just run the stage body and tag the
      // Context with the stage name.
      val afterBody = stage.run(env)(ctx)
      afterBody.copy(stage = stage.name)
    }

    finalCtx.result.getOrElse(
      // No stage set a result — return a stub empty result. Real
      // typed-error path lands in Step 0 (EngineError.NotImplemented).
      ConnectorResult(
        connectorName = "",
        schema        = ConnectorSchema(Nil),
        rows          = ResultRows(Vector.empty)
      )
    )
  }
}
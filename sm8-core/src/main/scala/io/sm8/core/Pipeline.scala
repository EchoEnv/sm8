/*
 * SM8 Core — Pipeline.
 * Runs the 4-stage pipeline (parse → resolve → execute → format) on
 * a Context. Step 4: pre-hooks fire before each stage body, post-hooks
 * fire after; `Context.stop = true` short-circuits the rest of the
 * pipeline; hook throws abort the pipeline (RFC §9 fail-fast).
 * sealed-trait/match over Map-based rule tables"): the 4 pipeline
 * stages are a sealed `Stage` hierarchy. The pipeline runner walks
 * a `List[Stage]` (DATA, not control flow) via `foldLeft`. Adding
 * a stage = adding a case class to `Stage` + adding it to
 * `Stage.All`. The compiler enforces both.
 * (`val`, case class, no `var`). The pipeline is `foldLeft`-pure —
 * no shared mutable state, safe under concurrency.
 * fail-fast. The pipeline does NOT wrap them — they propagate to
 * `engine.run(.)`. The hook author chose to throw; we honor that.
 * The Pipeline is internal (lives in `io.sm8.core`). Plugin authors
 * never construct a Pipeline directly — they go through
 * `Engine.run(request)`.
 *
 * PRODUCTION HOOK-ERROR PATH: in-tree `runPreHooks`/`runPostHooks` here
 * intentionally do NOT wrap hook exceptions in `try/catch`; raw
 * `RuntimeException` propagates per RFC §9 fail-fast. Production paths
 * go through `sm8-platform`'s `EngineHookDispatcher` +
 * `HookRunnerOrchestration`, which wrap throws as typed
 * `Left(EngineError.HookFailed(...))` per ADR-0008-af (currently at
 * v1.1 per the ADR's own revision-history table; updated to v1.1
 * after reviewer feedback sanitized HookFailed.message with engineer =
 * "<dispatcher>" literal)
 * (`EngineError.scala:140-149`). See ADR-0010-a for the dispatcher
 * contract. The Pipeline here is the canonical in-tree fallback
 * (RFC §5) and the 5 plugin test suites' seam; it is DORMANT in
 * production. PR-194c (Round 1 Review A): no code change, doc only.
 */
package io.sm8.core

import io.sm8.core.engine.EngineError
import io.sm8.sdk._

/**
 * Environment passed to each Stage at runtime. Bundles the
 * registries the stages need (
 * — data, not constructor-args scattered through each stage).
 */
final case class StageEnv(
 connectors: ConnectorRegistry,
 hooks: HookManager,
 transformers: TransformerRegistry
)

/**
 * One pipeline stage. Each stage is a pure function
 * `Context => Context` — no side effects on shared state.
 * Adding a stage = add a case object to `Stage` + add it to
 * `Stage.All`. The compiler enforces both (sealed trait forces
 * exhaustiveness; the runner iterates `All`).
 */
sealed trait Stage {
 /** Which `PipelineStage` value this Stage represents. */
 def name: PipelineStage

 /**
 * The stage body. Pure function `Context => Context`. Hooks
 * (pre + post) fire around this body; if a pre-hook sets
 * `Context.stop = true`, the body is skipped.
 */
 def run(env: StageEnv)(ctx: Context): Context
}

object Stage {

 /**
 * Parse — convert raw request into an internal query representation. Step 0 will add YAML/JSON lowering here.
 */
 case object Parse extends Stage {
 override def name: PipelineStage = PipelineStage.Parse
 override def run(env: StageEnv)(ctx: Context): Context = ctx
 }

 /**
 * Resolve — pick which adapter(s) will serve the request.
 */
 case object Resolve extends Stage {
 override def name: PipelineStage = PipelineStage.Resolve
 override def run(env: StageEnv)(ctx: Context): Context = ctx
 }

 /**
 * Execute — run the query against the chosen adapter(s). A
 * `ConnectorRequest` is routed to the named Connector and becomes
 * a `ConnectorResult`; failures are typed, never silent:
 * an unregistered connector name yields
 * `ConnectorError(name, EngineError.EngineUnavailable)`, and any
 * other (unrecognized) request type yields
 * `ConnectorError("-", EngineError.UnsupportedCapability)` — both
 * in the Result envelope so callers pattern-match on the typed
 * failure instead of mistaking empty rows for success.
 */
 case object Execute extends Stage {
  override def name: PipelineStage = PipelineStage.Execute
  override def run(env: StageEnv)(ctx: Context): Context = ctx.request match {
   case ConnectorRequest(connectorName, query) =>
    env.connectors.get(connectorName) match {
     case Some(c) =>
      val rows = c.query(query)
      val sch = c.schema()
      ctx.copy(result = Some(ConnectorResult(connectorName, sch, rows)))
     case None =>
      // P1-A3-E4: an unregistered connector name must surface a
      // typed failure, not a silent empty success.
      ctx.copy(result = Some(ConnectorError(
       connectorName,
       EngineError.EngineUnavailable(
        engine     = connectorName,
        available  = Nil,
        wasDefault = false,
        message    = s"sm8: connector '$connectorName' is not registered"))))
    }
   case other =>
    // Unknown request type — surface a typed NotImplemented-style
    // failure instead of passing through unchanged.
    ctx.copy(result = Some(ConnectorError(
     "-",
     EngineError.UnsupportedCapability(
      engine     = "pipeline",
      capability = "RequestType",
      message    = s"sm8: unsupported request type ${other.getClass.getName}"))))
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
 * Pipeline stages are DATA (`Stage.All`), not control flow. The
 * runner is a single `foldLeft` — adding a stage is data, not code.
 */
final class Pipeline(
 connectors: ConnectorRegistry,
 hooks: HookManager,
 transformers: TransformerRegistry
) {

 /** Bundled environment for Stage.run. */
 private val env: StageEnv = StageEnv(connectors, hooks, transformers)

 /**
 * Run `request` through all stages. Returns the final Result.
 * Per RFC §9: hook throws abort the pipeline. We don't wrap them
 * — they propagate to `engine.run(.)`.
 * Per RFC §8 + Context semantics: `Context.stop = true`
 * short-circuits all remaining stages and hooks.
 * @param request the Request to run
 * @return the final Result (the last Context's `result`, or a
 *   stub empty result if no stage set one)
 */
 def run(request: Request): Result = {
 val initial = Context(
  stage = PipelineStage.Parse,
  request = request,
  result = None,
  meta  = Map.empty,
  stop  = false
 )

 val finalCtx = Stage.All.foldLeft(initial) { (ctx, stage) =>
  if (ctx.stop) {
  // Per RFC: short-circuit. Tag the Context with the stage
  // name so downstream observers know where the pipeline halted.
  ctx.copy(stage = stage.name)
  } else {
  // 1. Pre-hooks (priority-ordered; fail-fast on throw).
  val afterPre = runPreHooks(stage, ctx)
  // 2. Stage body (skip if a pre-hook set stop).
  val afterBody = if (afterPre.stop) afterPre.copy(stage = stage.name)
      else stage.run(env)(afterPre).copy(stage = stage.name)
  // 3. Post-hooks (priority-ordered; fail-fast on throw).
  runPostHooks(stage, afterBody)
  }
 }

 finalCtx.result.getOrElse(
  // No stage set a result (e.g. a pre-hook short-circuited via
  // Context.stop before Execute ran) — return a stub empty result.
  ConnectorResult(
   connectorName = "",
   schema  = ConnectorSchema(Nil),
   rows    = ResultRows(Vector.empty)
  )
 )
 }

 /**
 * Fire pre-hooks for `stage` in priority order. Each hook may
 * mutate the Context (read context.meta/write context.result/set
 * context.stop per RFC hooks.md). A hook that throws aborts the
 * pipeline (RFC §9 fail-fast — propagate, don't wrap).
 */
 private def runPreHooks(stage: Stage, ctx: Context): Context = {
 val hookStage = preStageFor(stage)
 val pre = env.hooks.preHooksFor(hookStage)
 pre.foldLeft(ctx) { (c, hookWithPriority) =>
  if (c.stop) c
  else hookWithPriority._1.run(c)
 }
 }

 /**
 * Fire post-hooks for `stage` in priority order. Same semantics
 * as `runPreHooks` EXCEPT: respects `PostHook.runsOnStop` to honor
 * the RFC `hooks.md` Observer / Mutator classification. Per
 * the current implementation (the design contract-D2): when a Pre-hook set `c.stop = true`
 * (cache HIT path), Observer hooks (default `runsOnStop = true`)
 * still fire (audit, metrics); Mutator hooks (`runsOnStop = false`)
 * skip (cache write — the cache already has the result; row-cap —
 * the result is already capped by the cache store).
 */
 private def runPostHooks(stage: Stage, ctx: Context): Context = {
 val hookStage = postStageFor(stage)
 val post = env.hooks.postHooksFor(hookStage)
 post.foldLeft(ctx) { (c, hookWithPriority) =>
  if (c.stop && !hookWithPriority._1.runsOnStop) c
  else hookWithPriority._1.run(c)
 }
 }

 /**
 * Map a `Stage` to its `HookStage.Pre*` companion. The 8 hook
 * points are named `pre:<stage>` / `post:<stage>` — there are
 * exactly 4 `PipelineStage` values, each maps to a `HookStage.Pre`
 * and a `HookStage.Post` value.
 */
 private def preStageFor(stage: Stage): HookStage = stage.name match {
 case PipelineStage.Parse => HookStage.PreParse
 case PipelineStage.Resolve => HookStage.PreResolve
 case PipelineStage.Execute => HookStage.PreExecute
 case PipelineStage.Format => HookStage.PreFormat
 }

 private def postStageFor(stage: Stage): HookStage = stage.name match {
 case PipelineStage.Parse => HookStage.PostParse
 case PipelineStage.Resolve => HookStage.PostResolve
 case PipelineStage.Execute => HookStage.PostExecute
 case PipelineStage.Format => HookStage.PostFormat
 }
}
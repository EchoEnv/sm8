/*
 * SM8 hook-firing-audit Hook Plugin.
 *
 * Detects registered-but-never-fired hooks by probing every pipeline
 * stage attachment point and diffing the observed stamp set against
 * the full expected set at PostFormat. The 8 probes (one Pre + one
 * Post per stage) each stamp their stage wireName into a shared
 * accumulator on context.meta; the PostFormat reporter diffs that
 * accumulator against all 8 expected stamps.
 *
 * A probe legitimately does not fire when an earlier pre-hook
 * short-circuits the pipeline (Context.stop = true), because the
 * orchestrator skips subsequent stages entirely. The reporter reads
 * Context.stage (the stage where the short-circuit occurred, as
 * stamped by the orchestrator / in-tree Pipeline) and classifies any
 * expected stamp at-or-after that stage as legitimately skipped
 * rather than anomalous. Only a missing stamp at a stage the pipeline
 * actually reached is an anomaly, surfaced as a typed
 * EngineError.UnsupportedCapability carrying the missing wireNames.
 *
 * All probes and the reporter are stateless — the only shared state
 * is the immutable stamp set on context.meta, which flows through the
 * Context like any other plugin-to-plugin channel. No Spark types,
 * no closures over engine state, safe for journal rehydration.
 */
package io.sm8.plugins.hookfiringaudit

import io.sm8.core.engine.EngineError
import io.sm8.sdk._

/** Namespace prefix for every meta key this plugin writes. Namespaced
  * keys prevent collision with other plugins' meta entries. */
private[hookfiringaudit] object HookFiringAuditKeys {

  /** Meta key holding the accumulated Set[String] of stage wireNames
    * whose probes have fired so far. */
  val StampsKey: String = "io.sm8.plugins.hookfiringaudit:stamps"

  /** Meta key holding the reporter's final audit report
    * (Map[String, Any], Jackson-friendly: only String / List / Option /
    * Map / Boolean values). */
  val ReportKey: String = "io.sm8.plugins.hookfiringaudit:report"

  /** Meta key holding the typed anomaly when a probe that should have
    * fired did not. Value type: EngineError.UnsupportedCapability. */
  val AnomalyKey: String = "io.sm8.plugins.hookfiringaudit:anomaly"
}

/**
 * Probe hook bound to one stage attachment point. Stamps the stage's
 * wireName into the shared accumulator on context.meta.
 *
 * Instances are stateless; the accumulator lives on the Context, not
 * the hook, so the same probe instance is safe to reuse across
 * concurrent requests.
 *
 * @param probeStage the stage this probe is bound to (Pre or Post
 *                   variant is selected by the subclass)
 */
/**
 * Stamp helper shared by the Pre and Post probe families. Holds no
 * per-request state: the accumulator lives on the Context.
 */
private[hookfiringaudit] object StageProbe {

  /** Adds this probe's stage wireName to the stamp accumulator on
    * context.meta.
    *
    * @param probeStage the attachment point being stamped
    * @param context    the incoming pipeline context
    * @return the context with the accumulator updated
    */
  def stamp(probeStage: HookStage, context: Context): Context = {
    val stamps = context.meta
      .get(HookFiringAuditKeys.StampsKey)
      .collect { case s: Set[_] => s.asInstanceOf[Set[String]] }
      .getOrElse(Set.empty[String])
    context.copy(meta = context.meta + (HookFiringAuditKeys.StampsKey -> (stamps + HookStage.wireName(probeStage))))
  }
}

/**
 * Pre-hook probe bound to one stage attachment point. Stamps the
 * stage's wireName into the accumulator on context.meta.
 *
 * Instances are stateless; the accumulator lives on the Context, not
 * the hook, so the same probe instance is safe to reuse across
 * concurrent requests.
 *
 * @param probeStage the stage attachment point this probe stamps
 */
abstract private[hookfiringaudit] class PreStageProbeHook(probeStage: HookStage)
    extends PreHook
    with java.io.Serializable {

  override val name: String = s"hook-firing-audit-probe-${HookStage.wireName(probeStage)}"
  override val priority: Int = 100

  /** The attachment point this probe stamps.
    *
    * @return the stage this probe is bound to
    */
  override def stage: HookStage = probeStage

  /** Stamps this probe's stage wireName into the accumulator.
    *
    * @param context the incoming pipeline context
    * @return the context with the accumulator updated
    */
  override def run(context: Context): Context = StageProbe.stamp(probeStage, context)
}

/**
 * Post-hook probe bound to one stage attachment point. Same stamping
 * behavior as the Pre variant; differs only in the SDK kind it
 * implements, because Pre and Post are separate hook families in the
 * engine.
 *
 * @param probeStage the stage attachment point this probe stamps
 */
abstract private[hookfiringaudit] class PostStageProbeHook(probeStage: HookStage)
    extends PostHook
    with java.io.Serializable {

  override val name: String = s"hook-firing-audit-probe-${HookStage.wireName(probeStage)}"
  override val priority: Int = 100

  /** The attachment point this probe stamps.
    *
    * @return the stage this probe is bound to
    */
  override def stage: HookStage = probeStage

  /** Stamps this probe's stage wireName into the accumulator.
    *
    * @param context the incoming pipeline context
    * @return the context with the accumulator updated
    */
  override def run(context: Context): Context = StageProbe.stamp(probeStage, context)
}

/** Pre-hook probe for the pre:parse attachment point. */
final private[hookfiringaudit] class PreParseProbe extends PreStageProbeHook(HookStage.PreParse)

/** Post-hook probe for the post:parse attachment point. */
final private[hookfiringaudit] class PostParseProbe extends PostStageProbeHook(HookStage.PostParse)

/** Pre-hook probe for the pre:resolve attachment point. */
final private[hookfiringaudit] class PreResolveProbe extends PreStageProbeHook(HookStage.PreResolve)

/** Post-hook probe for the post:resolve attachment point. */
final private[hookfiringaudit] class PostResolveProbe extends PostStageProbeHook(HookStage.PostResolve)

/** Pre-hook probe for the pre:execute attachment point. */
final private[hookfiringaudit] class PreExecuteProbe extends PreStageProbeHook(HookStage.PreExecute)

/** Post-hook probe for the post:execute attachment point. */
final private[hookfiringaudit] class PostExecuteProbe extends PostStageProbeHook(HookStage.PostExecute)

/** Pre-hook probe for the pre:format attachment point. */
final private[hookfiringaudit] class PreFormatProbe extends PreStageProbeHook(HookStage.PreFormat)

/**
 * PostFormat probe + reporter in one hook. Stamps its own stage, then
 * diffs the accumulated stamps against the 8 expected wireNames. A
 * missing stamp at a stage the pipeline reached is surfaced as a typed
 * EngineError.UnsupportedCapability anomaly on context.meta; a missing
 * stamp at-or-after the short-circuit stage (when Context.stop is
 * set) is a legitimate skip.
 *
 * The dual role (stamp + report) is deliberate: registering this as a
 * PostFormat PostHook guarantees it runs after every other PostFormat
 * hook at a lower priority, so its report reflects the complete
 * pipeline traversal including any other observers that fired.
 *
 * Post-hook with Observer semantics (runsOnStop = true, the default):
 * the report is written even when an earlier pre-hook short-circuited
 * the pipeline, so the audit trail covers the halted path too.
 */
final private[hookfiringaudit] class PostFormatReporter extends PostHook with java.io.Serializable {

  override val name: String = "hook-firing-audit-reporter"
  override val priority: Int = 898

  /** The post:format attachment point — the final hook slot, so the
    * report reflects every earlier hook's effect.
    *
    * @return the post:format stage
    */
  override def stage: HookStage = HookStage.PostFormat

  /** Stamps the post:format attachment point, then diffs the
    * accumulated stamps against all 8 expected wireNames, classifying
    * stamps at-or-after a short-circuit stage as legitimately skipped.
    * Writes the report to context.meta and, when any reached-stage
    * stamp is missing, a typed UnsupportedCapability anomaly.
    *
    * @param context the incoming pipeline context
    * @return the context with the report (and anomaly, if any) added
    */
  override def run(context: Context): Context = {
    val stamps = context.meta
      .get(HookFiringAuditKeys.StampsKey)
      .collect { case s: Set[_] => s.asInstanceOf[Set[String]] }
      .getOrElse(Set.empty[String]) + HookStage.wireName(HookStage.PostFormat)

    val allStages: List[String] =
      List("pre:parse", "post:parse", "pre:resolve", "post:resolve",
           "pre:execute", "post:execute", "pre:format", "post:format")

    // Stages the pipeline legitimately never reached, given where the
    // short-circuit occurred. Context.stage carries the stage where
    // the halt was stamped (see the orchestrator's per-stage copy).
    // WireName ranks are 2× the stage rank (pre) or 2×rank+1 (post),
    // and a short-circuit still fires the stopping stage's own
    // post-hooks, so the first legitimately-skipped wireName is the
    // one right after the stopping stage's post slot.
    val stopRank: Option[Int] =
      if (!context.stop) None
      else Some(2 * PipelineStageRank.rank(context.stage) + 2)

    val (fired, skipped, missing) = allStages.foldLeft(
      (List.empty[String], List.empty[String], List.empty[String])
    ) { case ((f, s, m), wireName) =>
      if (stamps.contains(wireName)) (wireName :: f, s, m)
      else
        stopRank match {
          case Some(rank) if PipelineStageRank.rankOfWireName(wireName) >= rank =>
            (f, wireName :: s, m)
          case _ =>
            (f, s, wireName :: m)
        }
    }

    val anomaly: Option[EngineError.UnsupportedCapability] =
      if (missing.isEmpty) None
      else Some(
        EngineError.UnsupportedCapability(
          engine     = "hook-firing-audit-plugin",
          capability = "HookNotFired",
          message    = s"hooks registered but never fired: ${missing.mkString(", ")}"
        )
      )

    val report: Map[String, Any] = Map(
      "fired"   -> fired.sorted,
      "skipped" -> skipped.sorted,
      "missing" -> missing.sorted,
      "stopped" -> context.stop
    )

    val base = context.copy(
      meta = context.meta +
        (HookFiringAuditKeys.ReportKey -> report)
    )

    anomaly match {
      case Some(err) => base.copy(meta = base.meta + (HookFiringAuditKeys.AnomalyKey -> err))
      case None      => base
    }
  }
}

/** Ranks PipelineStage and wireNames on the shared 0-7 stage axis so
  * the reporter can decide which stamps were legitimately skipped. */
private[hookfiringaudit] object PipelineStageRank {

  /** Rank of a PipelineStage value on the shared stage axis.
    *
    * @param stage the pipeline stage to rank
    * @return 0 for Parse through 3 for Format
    */
  def rank(stage: PipelineStage): Int = stage match {
    case PipelineStage.Parse   => 0
    case PipelineStage.Resolve => 1
    case PipelineStage.Execute => 2
    case PipelineStage.Format  => 3
  }

  /** Rank of a stage wireName on the shared stage axis.
    *
    * @param wireName a stage wireName of the form "pre:stage" or
    *                 "post:stage"
    * @return 0 for pre:parse through 7 for post:format
    */
  def rankOfWireName(wireName: String): Int = wireName match {
    case "pre:parse"    => 0
    case "post:parse"   => 1
    case "pre:resolve"  => 2
    case "post:resolve" => 3
    case "pre:execute"  => 4
    case "post:execute" => 5
    case "pre:format"   => 6
    case "post:format"  => 7
  }
}

/**
 * Plugin registering the 8 stage probes and the PostFormat reporter.
 * Set up is idempotent-safe: every registration is into the engine's
 * hook manager, which tolerates repeated registration of the same
 * hook instance (dispatch deduplicates by identity at read time).
 *
 * Serializable: the plugin itself carries no state (all per-request
 * state lives on the Context), so journal rehydration reconstructs it
 * trivially.
 */
final class HookFiringAuditPlugin extends Plugin {

  /** Stable plugin identity used by registry introspection surfaces.
    *
    * @return the plugin's stable name
    */
  override def name: String = "hook-firing-audit"

  /** Registers the 8 stage probes (priority 100) and the PostFormat
    * reporter (priority 898). Idempotent-safe: every registration goes
    * into the engine's hook manager at startup.
    *
    * @param engine the engine being configured
    */
  override def setup(engine: Engine): Unit = {
    engine.hooks.registerPreHook(HookStage.PreParse, new PreParseProbe, 100)
    engine.hooks.registerPostHook(HookStage.PostParse, new PostParseProbe, 100)
    engine.hooks.registerPreHook(HookStage.PreResolve, new PreResolveProbe, 100)
    engine.hooks.registerPostHook(HookStage.PostResolve, new PostResolveProbe, 100)
    engine.hooks.registerPreHook(HookStage.PreExecute, new PreExecuteProbe, 100)
    engine.hooks.registerPostHook(HookStage.PostExecute, new PostExecuteProbe, 100)
    engine.hooks.registerPreHook(HookStage.PreFormat, new PreFormatProbe, 100)
    engine.hooks.registerPostHook(HookStage.PostFormat, new PostFormatReporter, 898)
  }
}

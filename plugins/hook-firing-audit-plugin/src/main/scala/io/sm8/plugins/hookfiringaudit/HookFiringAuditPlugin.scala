/*
 * SM8 hook-firing-audit Hook Plugin.
 *
 * Detects registered-but-never-fired hooks by probing every pipeline
 * stage attachment point and reporting which attachment points fired.
 *
 * ==Probe priority (load-bearing)==

The probes register at the core-floor priority 1, not the more typical first-party 100. The dispatcher's firePre short-circuits after the first pre-hook that sets stop=true (EngineHookDispatcher.scala:175-177), so any stopper at a higher priority would suppress the probe for its slot and produce a false-positive anomaly in the terminal reporter. Priority 1 places the probe below any stopper a plugin author would register.

==Cross-stage short-circuit semantics (load-bearing contract)==
 *
 * A pre-hook can set Context.stop = true at any stage. On the
 * production path, the orchestrator folds over the 4 stages and, at
 * each stage boundary after the halt, returns early without invoking
 * that stage's dispatcher at all — so attachment points after the
 * stopping stage legitimately never fire, including any reporter
 * registered there. The one attachment point that always fires is the
 * stopping stage's OWN post phase (the dispatcher still runs that
 * stage's post-hooks after a within-stage short-circuit).
 *
 * The plugin exploits this: a reporter is registered at ALL FOUR post
 * points, and each reporter emits the final report exactly once — the
 * "terminal" reporter — using one rule: a post-point reporter is
 * terminal iff context.stop is true (nothing after its stage will
 * dispatch, so it is the last hook that will ever run) OR its own
 * stage is post:format (pipeline completed normally). Reporters at
 * non-terminal post points pass the context through unchanged, so the
 * single report always lands from the deepest post point the pipeline
 * actually reached.
 *
 * A short-circuit at a PRE point (e.g. pre:parse) still fires that
 * stage's post phase, so the terminal reporter observes: its own
 * attachment point, the stopping stage's pre + post slots, and every
 * earlier slot — and classifies everything after as legitimately
 * skipped. Missing stamps at reached slots remain a typed anomaly.
 */
package io.sm8.plugins.hookfiringaudit

import io.sm8.core.engine.EngineError
import io.sm8.sdk._

/** Meta keys written by the hook-firing-audit plugin. Public so
  * transports and platform-side integration specs can read the
  * report without reaching into private package internals. */
object HookFiringAuditKeys {

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
      .collect { case s: Set[_] if s.forall(_.isInstanceOf[String]) =>
        s.asInstanceOf[Set[String]]
      }
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
  override val priority: Int = 1

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
  override val priority: Int = 1

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

/** Post-hook probe for the post:format attachment point. The reporter
  * at PostFormat also stamps its own slot (the reporter stamps before
  * the diff math runs), so this probe is an additional observation at
  * the same wireName — the reporter self-stamp is what the diff math
  * reads; this probe exists for symmetry with the other 3 Post probes
  * and to keep `HookStage.values` traversal clean. */
final private[hookfiringaudit] class PostFormatProbe extends PostStageProbeHook(HookStage.PostFormat)

/**
 * Reporter at one post attachment point. Stamps its own stage, then —
 * only when it is the TERMINAL reporter — diffs the accumulated
 * stamps against the expected set and writes the report (plus a typed
 * anomaly when a reached slot never stamped).
 *
 * Terminal rule: a reporter is terminal iff context.stop is true (its
 * stage is the halting stage, so no later stage will dispatch) OR its
 * own stage is post:format (pipeline completed). This makes the
 * report fire exactly once per request on both the completed and the
 * halted path, regardless of which stage short-circuited.
 *
 * @param reportStage the post attachment point this reporter occupies
 */
final private[hookfiringaudit] class StageReporter(reportStage: HookStage)
    extends PostHook
    with java.io.Serializable {

  override val name: String = s"hook-firing-audit-reporter-${HookStage.wireName(reportStage)}"
  override val priority: Int = 898

  /** The post attachment point this reporter occupies.
    *
    * @return the stage this reporter is bound to
    */
  override def stage: HookStage = reportStage

  /** Stamps this reporter's own post point, and writes the report if
    * this reporter is terminal (see the class doc).
    *
    * @param context the incoming pipeline context
    * @return the context with the report (and anomaly, if any) added
    */
  override def run(context: Context): Context = {
    val afterStamp = StageProbe.stamp(reportStage, context)

    val isTerminal =
      afterStamp.stop ||
        reportStage == HookStage.PostFormat

    if (!isTerminal) afterStamp
    else {
      val stamps = afterStamp.meta
        .get(HookFiringAuditKeys.StampsKey)
        .collect { case s: Set[_] if s.forall(_.isInstanceOf[String]) =>
          s.asInstanceOf[Set[String]]
        }
        .getOrElse(Set.empty[String])

      val allStages: List[String] =
        List("pre:parse", "post:parse", "pre:resolve", "post:resolve",
             "pre:execute", "post:execute", "pre:format", "post:format")

      // Two legitimate reasons a slot's probe may not have stamped:
      //
      // 1. Cross-stage skip: the pipeline halted before the slot's
      //    stage dispatched at all. The first such slot is right
      //    after the deepest fired slot (the terminal reporter's own
      //    stamp marks the halt boundary), so rank >= deepest+1 is
      //    skipped. Deriving the bound from the stamp set keeps the
      //    classification independent of Context.stage conventions,
      //    which differ between the orchestrator and the in-tree
      //    pipeline.
      //
      // 2. Same-stage suppression: within one attachment point the
      //    dispatcher stops firing pre-hooks as soon as one sets
      //    Context.stop, so a stop-setter at a lower priority
      //    suppresses this plugin's pre-point probe at the SAME
      //    stage. The stage's post phase still dispatches, so a
      //    missing PRE slot whose own post slot IS stamped is
      //    suppression, not inertness.
      val stopBound: Option[Int] =
        if (!afterStamp.stop) None
        else Some(deepestFiredRank(stamps) + 1)

      // Same-stage probe suppression only applies when the report is
      // observing a stop the pipeline actually halted on (post = true).
      // Without that conjunction, a hypothetical future dispatch bug
      // that fired a stage's post-hooks but skipped its pre-hooks
      // would silently mask the asymmetry as 'suppressed' instead of
      // flagging it as the inertness anomaly this plugin exists to
      // detect.
      val (fired, skipped, missing) = allStages.foldLeft(
        (List.empty[String], List.empty[String], List.empty[String])
      ) { case ((f, s, m), wireName) =>
        val rank = PipelineStageRank.rankOfWireName(wireName)
        if (stamps.contains(wireName)) (wireName :: f, s, m)
        else {
          val suppressed =
            afterStamp.stop &&
              rank % 2 == 0 &&
              stamps.contains(PipelineStageRank.wireNameOfRank(rank + 1))
          if (suppressed || stopBound.exists(rank >= _)) (f, wireName :: s, m)
          else (f, s, wireName :: m)
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
        "stopped" -> afterStamp.stop
      )

      val withReport = afterStamp.copy(
        meta = afterStamp.meta + (HookFiringAuditKeys.ReportKey -> report)
      )

      anomaly match {
        case Some(err) =>
          withReport.copy(meta = withReport.meta + (HookFiringAuditKeys.AnomalyKey -> err))
        case None =>
          withReport
      }
    }
  }

  /** Highest wireName rank present in the stamp set, or -1 when
    * nothing has fired yet.
    *
    * @param stamps the accumulated stamp set
    * @return the deepest fired rank
    */
  private def deepestFiredRank(stamps: Set[String]): Int =
    stamps.foldLeft(-1)((acc, wireName) => math.max(acc, PipelineStageRank.rankOfWireName(wireName)))
}

/**
 * Plugin registering the 8 stage probes, 4 stage reporters (one per
 * post attachment point; exactly one of them emits the report per
 * request — see [[StageReporter]]), and nothing else.
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

  /** Explicit empty closure contract: this plugin captures no
    * constructor state; all per-request state lives on the Context.
    * The explicit override documents that decision for the
    * serialization-safety introspection seam.
    *
    * @return the captured variable names (none)
    */
  override def closedOverVars: Seq[String] = Seq.empty

  /** Registers the 8 stage probes (priority 1) and the 4 stage
    * reporters (priority 898). Idempotent-safe: every registration
    * goes into the engine's hook manager at startup.
    *
    * @param engine the engine being configured
    */
  override def setup(engine: Engine): Unit = {
    // Probes at the core-floor priority 1 — the dispatcher's firePre
    // short-circuits after the first pre-hook that sets stop=true
    // (EngineHookDispatcher.scala:175-177), so any stopper at a higher
    // priority would suppress the probe for its slot. Priority 1 is
    // below any stopper a plugin author would register; the SDK's
    // require(priority >= 0) prevents priority 0 or negative.
    engine.hooks.registerPreHook(HookStage.PreParse,    new PreParseProbe,    1)
    engine.hooks.registerPostHook(HookStage.PostParse,   new PostParseProbe,   1)
    engine.hooks.registerPreHook(HookStage.PreResolve,  new PreResolveProbe,  1)
    engine.hooks.registerPostHook(HookStage.PostResolve, new PostResolveProbe, 1)
    engine.hooks.registerPreHook(HookStage.PreExecute,  new PreExecuteProbe,  1)
    engine.hooks.registerPostHook(HookStage.PostExecute, new PostExecuteProbe, 1)
    engine.hooks.registerPreHook(HookStage.PreFormat,   new PreFormatProbe,   1)
    engine.hooks.registerPostHook(HookStage.PostFormat,  new PostFormatProbe,  1)
    // A reporter per post point: the terminal one (see StageReporter)
    // emits the single per-request report on both the completed and
    // the halted path.
    engine.hooks.registerPostHook(HookStage.PostParse,   new StageReporter(HookStage.PostParse),   898)
    engine.hooks.registerPostHook(HookStage.PostResolve, new StageReporter(HookStage.PostResolve), 898)
    engine.hooks.registerPostHook(HookStage.PostExecute, new StageReporter(HookStage.PostExecute), 898)
    engine.hooks.registerPostHook(HookStage.PostFormat,  new StageReporter(HookStage.PostFormat),  898)
  }
}

/** Ranks stage wireNames on a fixed 0-7 axis (pre:parse through
  * post:format) so the reporter can order attachment points and
  * derive skip bounds.
  */
private[hookfiringaudit] object PipelineStageRank {

  /** Rank of a PipelineStage value on the stage axis (half the
    * wireName resolution: each stage owns one pre and one post slot).
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

  /** Inverse of [[rankOfWireName]]: the wireName at a given rank.
    *
    * @param rank a rank on the 0-7 stage axis
    * @return the wireName occupying that rank
    */
  def wireNameOfRank(rank: Int): String = rank match {
    case 0 => "pre:parse"
    case 1 => "post:parse"
    case 2 => "pre:resolve"
    case 3 => "post:resolve"
    case 4 => "pre:execute"
    case 5 => "post:execute"
    case 6 => "pre:format"
    case _ => "post:format"
  }
}

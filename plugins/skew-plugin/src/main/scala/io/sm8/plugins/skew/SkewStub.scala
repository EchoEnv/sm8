/*
 * SM8 skew Hook Plugin.
 *
 * Skew handling decisions are based on the user-declared
 * cardinality estimate (join `estimated_rows`), not a heuristic.
 *
 * Decision-only: the AQE skew config is NOT set here — Spark
 * integration is deferred. The decision path (consult) is exposed
 * for the planner / tests to consume.
 */
package io.sm8.plugins.skew

import java.util.concurrent.atomic.AtomicInteger

import io.sm8.core.model.Model
import io.sm8.sdk._

/**
 * Skew Hook Plugin. Pre-execute hook that would tune AQE skew
 * handling. Counter-only for now; the consult path exposes the
 * skew decision from the join estimates.
 */
final class SkewStub extends Plugin with java.io.Serializable {

  /**
   * Decides whether a model's joins are skewed enough to warrant
   * adaptive skew handling.
   *
   * A join is skewed when its user-declared row-count estimate is
   * at or above the skew threshold (a few large keys dominate the
   * join). Only joins with an estimate are considered; joins
   * without one never trigger a skew decision.
   *
   * @param model     the validated model whose joins to inspect
   * @param threshold the row-count floor for a skewed join
   * @return          `true` if any join estimate meets the threshold
   */
  def consult(model: Model, threshold: Long): Boolean =
    model.joins.exists { js =>
      js.estimatedRows.exists(est => est >= threshold)
    }

  /**
   * The closed-over variable this plugin reads is the `fires` counter.
   *
   * @return the closed-over variable names
   */
  override def closedOverVars: Seq[String] = Seq("fires")

  val fires: AtomicInteger = new AtomicInteger(0)

  /**
   * Registers the PreExecute skew hook on the engine.
   *
   * @param engine the engine to register the hook on
   */
  override def setup(engine: Engine): Unit = {
    engine.hooks.registerPreHook(
      HookStage.PreExecute,
      new SkewPreStubHook(fires),
      priority = 250
    )
  }
}

/**
 * PreExecute skew hook. The per-query value-consult decision:
 * ARMS the skew seed when any join's estimated row count meets
 * or exceeds SkewThresholdRows (a few large keys dominate the
 * join). Writes the arm Boolean to context.meta; the platform
 * engineExecutor folds it into EngineContext.decisionHints
 * (typed transport per ADR-009-d v0.3). NO try/catch: a
 * throwing consult propagates to EngineHookDispatcher's
 * existing catch which constructs the 5-field HookFailed.
 */
private final class SkewPreStubHook(counter: AtomicInteger)
    extends PreHook with java.io.Serializable {
 // Value-consult decision: the inline rule in the spark connector
 // ARMS any join with estimatedRows; this rule ARMS only when
 // estimatedRows >= SkewThresholdRows (large-row join).
  private val SkewThresholdRows: Long = 1_000_000_000L

  override val name: String = "skew-stub"
  override val priority: Int = 250
  override def stage: HookStage = HookStage.PreExecute

  override def run(context: Context): Context = {
    counter.incrementAndGet()
    val model: io.sm8.core.model.Model = context.request match {
      case ehr: io.sm8.core.engine.EngineHookRequest => ehr.model
      case _ => return context
    }
    val arm: Boolean = model.joins.exists(_.estimatedRows.exists(_ >= SkewThresholdRows))
    context.copy(meta = context.meta + ("sm8.skew.arm" -> arm))
  }
}
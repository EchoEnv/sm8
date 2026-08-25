/*
 * SM8 broadcast Hook Plugin.
 *
 * Ensures small DataFrames in joins don't trigger a shuffle: the
 * decision to broadcast a join side is based on the user-declared
 * cardinality estimate (join `estimated_rows`), not a heuristic.
 *
 * Decision-only: the Spark broadcast config is NOT set here — Spark
 * integration is deferred. The decision path (consult) is exposed
 * for the planner / tests to consume.
 */
package io.sm8.plugins.broadcast

import java.util.concurrent.atomic.AtomicInteger

import io.sm8.core.model.Model
import io.sm8.sdk._

/**
 * Broadcast Hook Plugin. Pre-execute hook that would set broadcast
 * hints on the join plan. Counter-only for now; the consult path
 * exposes the broadcast decision from the join estimates.
 */
final class BroadcastStub extends Plugin with java.io.Serializable {

  /**
   * Decides whether to broadcast a join side for a model.
   *
   * A join should be broadcast when its user-declared row-count
   * estimate is at or below the broadcast threshold (small enough
   * to ship as a map-side / broadcast artifact). Only joins with
   * an estimate are considered; joins without one never trigger a
   * broadcast decision.
   *
   * @param model     the validated model whose joins to inspect
   * @param threshold the row-count ceiling for a broadcastable join
   * @return          `true` if any join estimate is within the threshold
   */
  def consult(model: Model, threshold: Long): Boolean =
    model.joins.exists { js =>
      js.estimatedRows.exists(est => est <= threshold)
    }

  /**
   * The closed-over variable this plugin's lifecycle state is `fires`
   * (the per-run hook counter).
   *
   * @return the closed-over variable names
   */
  override def closedOverVars: Seq[String] = Seq("fires")

  val fires: AtomicInteger = new AtomicInteger(0)

  /**
   * Registers the PreExecute broadcast hook on the engine.
   *
   * @param engine the engine to register the hook on
   */
  override def setup(engine: Engine): Unit = {
    engine.hooks.registerPreHook(
      HookStage.PreExecute,
      new BroadcastPreStubHook(fires),
      priority = 250
    )
  }
}

/**
 * PreExecute broadcast hook. The per-query value-consult
 * decision: ARMS the broadcast seed when any join's estimated
 * row count is at or below BroadcastThresholdRows (a small-row
 * side fits the byte-gate). Writes the arm Boolean AND the
 * threshold bytes to context.meta; the platform engineExecutor
 * folds these into EngineContext.decisionHints (typed transport
 * per ADR-009-d v0.3). NO try/catch: a throwing consult
 * propagates to EngineHookDispatcher's existing catch which
 * constructs the 5-field HookFailed (sanitized message).
 */
private final class BroadcastPreStubHook(counter: AtomicInteger)
    extends PreHook with java.io.Serializable {
 // Value-consult decision: the inline rule in the spark connector
 // ARMS any join with estimatedRows; this rule ARMS only when
 // estimatedRows <= BroadcastThresholdRows. A model with est >
 // threshold is DISARMED here but ARMED inline — the regimes
 // differ, making the wiring observable.
 private val BroadcastThresholdRows: Long = 10_000_000L
 // Byte budget the connector should use when the oracle arms.
 // Distinct from the row-count threshold above: the meta key
 // `sm8.broadcast.thresholdBytes` is documented as BYTES (per
 // DecisionHints scaladoc); writing a row-count value here would
 // be misinterpreted as a byte budget and disarm most real joins.
 // 10 MiB is the same default the spark connector seeds when no
 // oracle is wired.
 private val BroadcastThresholdBytes: Long = 10L * 1024L * 1024L

 override val name: String = "broadcast-stub"
 override val priority: Int = 250
 override def stage: HookStage = HookStage.PreExecute

 override def run(context: Context): Context = {
  counter.incrementAndGet()
  val model: io.sm8.core.model.Model = context.request match {
   case ehr: io.sm8.core.engine.EngineHookRequest => ehr.model
   case _ => return context
  }
  val arm: Boolean = model.joins.exists(_.estimatedRows.exists(_ <= BroadcastThresholdRows))
  context.copy(
   meta = context.meta +
    ("sm8.broadcast.arm" -> arm) +
    ("sm8.broadcast.thresholdBytes" -> BroadcastThresholdBytes))
 }
}
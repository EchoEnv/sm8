/*
 * SM8 row-cap Hook Plugin.
 *
 * (case class). `RowCapPlugin` is behavior (registers the hook).
 * The hook body pattern-matches on `Result` per the SDK shape.
 *
 *
 * runtime errors expected. Programmer errors (e.g., negative
 * `maxRows`) are rejected at the boundary (case class apply).
 *
 * Step 9a first cut: shape-correct (counter only). Real capping
 * (ctx.result = ctx.result.take(maxRows)) lands when the typed
 * Result shape ships (Step 0 — Result is still a marker trait).
 */
package io.sm8.plugins.rowcap

import java.util.concurrent.atomic.AtomicInteger

import io.sm8.sdk._

/**
 * Config for the row-cap hook. Case class — data only.
 *
 * validity": the smart constructor (below) validates non-negative
 * maxRows at the boundary.
 */
final case class RowCapConfig(maxRows: Int) {
  require(maxRows >= 0, s"sm8: row-cap maxRows must be non-negative, got $maxRows")
}

/**
 * RowCap Hook Plugin. Caps each engine.run result to
 * `config.maxRows`. Step 9a first cut: shape-correct (counter
 * only); real capping lands with the typed Result shape (Step 0).
 *
 * Per 
 * for Spark-closure safety.
 */
final class RowCapPlugin(config: RowCapConfig) extends Plugin with java.io.Serializable {

  /** 
    * captured `config` (RowCapConfig, Serializable case class) and
    * `fires` (AtomicInteger, Serializable). */
  override def closedOverVars: Seq[String] = Seq("config", "fires")

  /** Test-visible counter of hook fires. */
  val fires: AtomicInteger = new AtomicInteger(0)

  override def setup(engine: Engine): Unit = {
    engine.hooks.registerPostHook(
      HookStage.PostExecute,
      new RowCapPostHook(fires, config),
      priority = 200
    )
  }
}

/**
 * PostExecute row-cap hook. Step 9a: increments a counter. Real
 * implementation: `ctx.result` would be capped to `config.maxRows`
 * when the typed Result shape ships.
 *
 * Serializable: captured in closures must serialize cleanly.
 */
private final class RowCapPostHook(counter: AtomicInteger, config: RowCapConfig)
    extends PostHook with java.io.Serializable {
  override val name: String = "row-cap"
  override val priority: Int = 200
  override def stage: HookStage = HookStage.PostExecute
  override def run(context: Context): Context = {
    counter.incrementAndGet()
    // Real implementation will cap context.result per config.maxRows.
    context
  }
}

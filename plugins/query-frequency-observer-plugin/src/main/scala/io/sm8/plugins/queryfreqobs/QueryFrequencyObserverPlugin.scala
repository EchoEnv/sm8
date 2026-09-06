/*
 * SM8 query-frequency-observer Plugin (Ticket 6 of
 * docs/wayfinder/2026-09-06-pre-aggregation.md; pairs with Ticket 2's
 * query-shape logger in sm8-platform EngineService).
 *
 * A PostExecute OBSERVER plugin that counts query shapes per model:
 * one AtomicLong per (model, version, measures, dimensions) shape
 * key. This is the COUNT half of the Ticket 2 telemetry — Ticket 2
 * logs the shape per invocation (DEBUG), this plugin accumulates
 * the frequency so rollup selection (the map's Ticket 3) is driven
 * by measured demand: which shapes are hot, how often.
 *
 * ==Where the counts live==
 *
 * `QueryShapeCounters`, a process-wide singleton object holding a
 * concurrent map of AtomicLongs. A singleton (not instance state)
 * because the server's plugin instantiation and the read surface
 * (`sm8 inspect` -> MetaInspectorService / `sm8 metrics`) may run
 * in different objects; the counts must be JVM-global. Counters are
 * plain AtomicLongs: journal-safe, no Spark types, no IO. Cap on
 * distinct shapes prevents unbounded growth (CardinalityGuard).
 *
 * ==Reading the counts==
 *
 * Two surfaces, both non-invasive:
 *   - `QueryShapeCounters.snapshot()` — programmatic (tests, the
 *     meta-inspector): the plugin ALSO writes its counts into
 *     `context.meta` under `io.sm8.plugins.queryfreqobs:counts` on
 *     every invocation (the MetaCaptureObserver pattern), so the
 *     existing `sm8 inspect <key>` CLI reads them with zero new
 *     transport surface.
 *   - Ticket 2's DEBUG logger (`io.sm8.platform.query.QueryShape`)
 *     remains the per-invocation event stream; this plugin adds the
 *     aggregate dimension.
 *
 * ==Closure safety==
 *
 * The hook closes over NOTHING (all state lives in the singleton);
 * `closedOverVars` is Nil. Serializable by construction.
 */
package io.sm8.plugins.queryfreqobs

import io.sm8.core.engine.QueryRequest
import io.sm8.core.model.Model
import io.sm8.core.rel.{AggregateCall, AggregateFn}
import io.sm8.sdk.{Context, Engine, HookStage, Plugin}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters._

/** JVM-global shape counters (see file header for why a singleton).
  *
  * Shape key: `model|v<version>|m=a,b|d=x,y` — measures and
  * dimensions sorted + deduped so set-equal shapes collide
  * (canonically). Cap: `MaxShapes` distinct keys (beyond it, new
  * shapes are counted under the `__overflow__` key — frequency
  * telemetry degrades gracefully instead of leaking).
  */
object QueryShapeCounters {

  /** Cardinality guard: distinct-shape cap (frequency telemetry is
    * for hot-shape discovery; 10k shapes is far past any rollup
    * decision horizon). */
  val MaxShapes: Int = 10000
  val OverflowKey: String = "__overflow__"

  private val counters = new ConcurrentHashMap[String, AtomicLong]()

  /** Canonical shape key (sorted, deduped, set-equal collision).
    * Measures come from `aggregateMeasures` (TypedAggregateCall —
    * its `name` IS the measure alias); dimensions are plain
    * strings on the request but `groupBy` uses the request's
    * dimension list — both canonically sorted + deduped.
    *
    * @param model   the queried model (name + version)
    * @param request the normalized query request
    * @return the canonical shape key
    */
  def shapeKey(model: Model, request: QueryRequest): String = {
    /** Sort + dedupe a member list for the canonical form.
      *
      * @param xs the member list (measures or dimensions)
      * @return the canonical comma-joined member string
      */
    def canon(xs: Seq[String]): String = xs.distinct.sortBy(identity).mkString(",")
    val meas = canon(request.aggregateMeasures.map(_.name))
    val dims = canon(request.dimensions)
    s"${model.name}|v${model.version}|m=$meas|d=$dims"
  }

  /** Count one invocation of the shape.
    *
    * @param key the canonical shape key
    * @return the new count for the key
    */
  def increment(key: String): Long = {
    val effective =
      if (counters.size() >= MaxShapes && !counters.containsKey(key)) OverflowKey
      else key
    counters
      .computeIfAbsent(effective, _ => new AtomicLong(0))
      .incrementAndGet()
  }

  /** Immutable snapshot (key -> count), sorted by count desc then
    * key — hottest shapes first.
    *
    * @return (shapeKey, count) pairs, hottest first
    */
  def snapshot(): List[(String, Long)] =
    counters.asScala.toList
      .map { case (k, v) => (k, v.get()) }
      .sortBy { case (k, c) => (-c, k) }

  /** Reset (test seam + ops). */
  def reset(): Unit = counters.clear()
}

/** The plugin: registers one PostExecute observer that increments
  * the shape counter and publishes counts into `context.meta` for
  * the meta-inspector surface.
  */
final class QueryFrequencyObserverPlugin extends Plugin {

  /** Stable plugin identity for diagnostic surfaces.
    *
    * @return the stable plugin name
    */
  override def name: String = "query-frequency-observer"

  /** Registers the PostExecute observer (priority 900).
    *
    * @param engine the engine being configured
    */
  override def setup(engine: Engine): Unit = {
    engine.hooks.registerPostHook(
      HookStage.PostExecute,
      hook,
      900
    )
  }

  /** No captured state: all counters live in the JVM-global
    * QueryShapeCounters singleton.
    *
    * @return the captured-variable name list (empty)
    */
  override def closedOverVars: Seq[String] = Seq.empty

  /** The PostExecute observer hook (test seam). Increments the
    * shape counter and publishes the snapshot into context.meta.
    *
    * @return the registered PostHook
    */
  private[queryfreqobs] def hook: io.sm8.sdk.PostHook =
    new io.sm8.sdk.PostHook with java.io.Serializable {
      override val name: String = "query-frequency-observer"
      override val stage: HookStage = HookStage.PostExecute
      override val priority: Int = 900
      override val runsOnStop: Boolean = true // observer: always fire

      /** Observe one completed invocation: increment the shape
        * counter and attach the snapshot to context.meta.
        *
        * @param context the post-execute context
        * @return the context (meta enriched with the counts key)
        */
      override def run(context: Context): Context = {
        val counted: Option[(String, Long)] =
          for {
            model <- shapeModel(context)
            req   <- shapeRequest(context)
          } yield {
            val key = QueryShapeCounters.shapeKey(model, req)
            (key, QueryShapeCounters.increment(key))
          }
        counted match {
          case Some((key, _)) =>
            val counts = QueryShapeCounters.snapshot()
            context.copy(meta = context.meta +
              ("io.sm8.plugins.queryfreqobs:counts" -> counts))
          case None => context
        }
      }
    }

  /** Extract the Model from the hook context (best-effort: the
    * engine hook request carries it inside `context.request`). */
  private def shapeModel(context: Context): Option[Model] =
    context.request match {
      case r: io.sm8.core.engine.EngineHookRequest => Some(r.model)
      case _                                        => None
    }

  /** Extract the normalized QueryRequest. */
  private def shapeRequest(context: Context): Option[QueryRequest] =
    context.request match {
      case r: io.sm8.core.engine.EngineHookRequest => Some(r.mcpRequest)
      case _                                        => None
    }
}

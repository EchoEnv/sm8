/*
 * SM8 Server — MetaCaptureObserver.
 *
 * Deployment-local observer plugin that records the most recent
 * request's `Context.meta` into a caller-supplied
 * `AtomicReference[Map[String, Any]]`. The `MetaInspectorService`
 * engineFn reads that reference, so `sm8 inspect <key>` serves the
 * meta the plugins wrote on the last executed request.
 *
 * Registered as a PostExecute observer at the top of the community
 * priority range, so it runs last — it snapshots the fully-folded
 * meta after every other post-hook (cache write, audit, graph
 * snapshot, etc.) has run. `closingOver` lists the captured
 * reference (the only state), which is `Serializable`.
 *
 * This is deployment wiring (RFC §11a), not a published plugin: it
 * lives in the server module and is appended to the discovered
 * plugin list at the composite root.
 */
package io.sm8.server

import java.util.concurrent.atomic.AtomicReference

import io.sm8.sdk.{Context, Engine, HookStage, Plugin, PostHook}
import scala.collection.immutable.HashMap

/**
 * Snapshot the most recent request's `Context.meta` into `target`
 * after every execute. Observer semantics: always fires, including on
 * the short-circuit (cache-HIT) path.
 *
 * @param target the reference to write the latest meta into
 */
private[server] final class MetaCaptureObserver(
    target: AtomicReference[Map[String, Any]]
) extends Plugin with java.io.Serializable {

  override def setup(engine: Engine): Unit = {
    engine.hooks.registerPostHook(
      HookStage.PostExecute,
      postHook,
      999
    )
  }

  /**
   * The PostExecute observer that snapshots `context.meta` into `target`.
   *
   * Extracted as a `private[server]` def (rather than the previous
   * inline anonymous class) so test code in `sm8-server` can invoke
   * `run(ctx)` directly and assert snapshot semantics without
   * stubbing the full [[io.sm8.sdk.Engine]] + [[io.sm8.sdk.HookManager]]
   * pair. The hook itself remains `Serializable` (per closure-safety
   * spec; ADR-0008-ah) and registers at the same priority (999) and
   * stage (PostExecute) as the inline predecessor.
   *
   * Per audit 2026-08-30 H1: the pre-fix body
   * `target.set(context.meta)` aliased the SAME map instance — any
   * downstream mutation of `context.meta` (by a later hook, the
   * engine fold, or a future SDK relaxation that allows mutable
   * maps) would be visible via the inspector, creating a hidden
   * coupling between hook execution order and `sm8 inspect <key>`
   * output. The post-fix body `target.set(HashMap.from(context.meta))`
   * writes a fresh immutable HashMap, so the snapshot is decoupled
   * from later mutations of the source.
   *
   * Why `HashMap.from` (NOT `Map.from`): Scala 2.13's
   * `Map.from(immutable.Map)` short-circuits to return the input
   * instance unchanged — an optimization for the common case where
   * the caller already holds an immutable Map. That optimization
   * would defeat the defensive-copy intent: the snapshot would
   * still alias the source. `HashMap.from` is implemented via
   * `new HashMap` + `addOne` / bulk-load, ALWAYS producing a fresh
   * instance for non-empty input (the empty case returns the
   * `emptyHashMap` singleton, which is safe — there's no mutation
   * to defend against on an empty map). This is verified by the
   * identity-check test in [[MetaCaptureObserverSpec]].
   *
   * Skill alignment (per [[debug-mantra-mindset]] + scala-jvm-safety +
   * scala-perf-testing + ADR-0008-ah):
   *  - `HashMap.from(coll)` is total on a non-null `Iterable[(K, V)]`
   *    (NPE on null source; `Context.meta` is non-null by SDK
   *    contract — `Map.empty` default).
   *  - O(n) time + space where n = `context.meta.size`; runs once
   *    per query at PostExecute (negligible per scala-perf-testing).
   *  - The snapshotted value is a fresh `scala.collection.immutable
   *    .HashMap` which IS Serializable (closure-safety: the existing
   *    `target: AtomicReference` capture is unchanged; the
   *    snapshotted value's serializability is now guaranteed by
   *    `HashMap.from`).
   *  - Return type is `scala.collection.immutable.HashMap[String,
   *    Any]` (a subtype of `Map[String, Any]`); no client-visible
   *    type change.
   */
  private[server] def postHook: PostHook =
    new PostHook with java.io.Serializable {
      override val name: String = "MetaCaptureObserver"
      override val stage: HookStage = HookStage.PostExecute
      override val priority: Int = 999
      override def run(context: Context): Context = {
        target.set(HashMap.from(context.meta))
        context
      }
      // Observer: always fire, even when a pre-hook set stop=true.
      override val runsOnStop: Boolean = true
    }

  /** The only captured state is the (Serializable) meta reference. */
  override def closedOverVars: Seq[String] = Seq("target")
}
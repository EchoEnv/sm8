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
   * output.
   *
   * Why the post-fix body is `target.set(HashMap.from(context.meta
   * .iterator))` (NOT `Map.from(context.meta)`, NOT `HashMap.from
   * (context.meta)`, and NOT `HashMap.empty ++ context.meta`):
   * all three rejected alternatives have an `instanceof HashMap`
   * short-circuit that defeats the defensive-copy intent:
   *
   *   - `Map.from(immutable.Map) ⇒ same instance` for ALL
   *     immutable Map inputs (verified via Scala 2.13 bytecode at
   *     `scala/collection/immutable/Map$.class`, `from` method).
   *   - `HashMap.from(HashMap) ⇒ same instance` (bytecode at
   *     `scala/collection/immutable/HashMap$.class`, `from` method,
   *     offset 0-11: `aload_1; instanceof HashMap; ifeq 15`
   *     bypasses the `new HashMapBuilder` path when input is
   *     already a HashMap).
   *   - `HashMap.empty ++ HashMap` short-circuits when the
   *     receiver is empty and the argument is a HashMap, returning
   *     the argument unchanged (bytecode at
   *     `scala/collection/immutable/HashMap.class`, `concat`
   *     method, offset 13-22: `isEmpty; ifeq 25; aload other;
   *     goto 98`).
   *
   * The chosen pattern `HashMap.from(context.meta.iterator)` passes
   * an `Iterator` (which is NOT a HashMap) to `HashMap.from`, so
   * the `instanceof HashMap` check is false and the
   * `HashMapBuilder` path executes — always allocating a fresh
   * `new HashMap` regardless of the input's concrete type. The
   * empty case (`context.meta.isEmpty`) returns the
   * `emptyHashMap` singleton, which is safe — there's nothing to
   * defend against on an empty map. The HashMap-input regression
   * test (5+ keys, mirroring production after ~5 `+` operations)
   * is the tight falsifier: it would FAIL on the pre-fix code AND
   * on all three rejected alternative fixes.
   *
   * Skill alignment (per [[debug-mantra-mindset]] + scala-jvm-safety +
   * scala-perf-testing + ADR-0008-ah):
   *  - `HashMap.from(iter)` is total on a non-null `Iterator`
   *    (NPE on null iterator; `context.meta.iterator` is non-null
   *    per `Map.empty` default).
   *  - O(n) time + space where n = `context.meta.size`; runs once
   *    per query at PostExecute (negligible per scala-perf-testing).
   *  - The snapshotted value is a fresh `scala.collection.immutable
   *    .HashMap` which IS Serializable (closure-safety: the existing
   *    `target: AtomicReference` capture is unchanged; the
   *    snapshotted value's serializability is preserved).
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
        target.set(HashMap.from(context.meta.iterator))
        context
      }
      // Observer: always fire, even when a pre-hook set stop=true.
      override val runsOnStop: Boolean = true
    }

  /** The only captured state is the (Serializable) meta reference. */
  override def closedOverVars: Seq[String] = Seq("target")
}
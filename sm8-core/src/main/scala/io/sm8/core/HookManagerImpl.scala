/*
 * SM8 Core — internal HookManager implementation.
 *
 * Hooks are stored as priority+sequence entries; the dispatch list
 * is derived (sorted on read) — no in-place mutation of dispatch
 * order.
 *
 * Per RFC §13 conformance PR: the `require(priority >= 0)` check is
 * the typed-origin range check via
 * `io.sm8.sdk.HookOrigin.validate(origin, priority)`. Plugin authors
 * declare the origin of their plugin at registration time (default =
 * FirstParty, matching `io.sm8.plugins.*` reference plugins).
 * Out-of-range priorities throw `IllegalArgumentException` at the SDK
 * boundary (the SDK doc already declared this throw — the contract
 * is preserved).
 *
 * The hook storage map is a `mutable.Map` — documented
 * single-threaded use: register at startup, dispatch at request time.
 *
 * Hook throws abort the pipeline per RFC §9 (fail-fast) — NOT
 * runtime errors to be wrapped in Either. The hook author CHOSE to
 * throw; the engine honors that choice by propagating.
 *
 * Binary compat note: the SDK trait `HookManager` signature gained
 * one new overload per direction (`registerPreHook` / `registerPostHook`
 * with `HookOrigin` arg, default-implemented to delegate to the
 * int-only overload). The int-only overload is preserved with
 * identical semantics — so downstream Plugins and third-party
 * HookManagerImpl are unaffected at the source level. Plugins that
 * want strict origin enforcement migrate to the 4-arg overload at
 * their leisure.
 */
package io.sm8.core

import java.util.concurrent.atomic.AtomicLong

import io.sm8.sdk.{HookManager, HookOrigin, HookStage, Plugin, PostHook, PreHook}

/**
 * HookEntry — case class for a registered hook plus its scheduling
 * data (priority + sequence + origin + plugin). Per RFC §8 priority ranges
 * the HookManager is the only owner.
 */
private[core] final case class HookEntry[T](
 hook: T,
 priority: Int,
 seq: Long,
 origin: HookOrigin,
 pluginName: String
)

/**
 * Concrete HookManager. Owns:
 * - pre-hooks and post-hooks, grouped by `HookStage`
 * - a monotonic sequence counter for registration-order tie-breaking
 *
 * Not thread-safe for concurrent `register*` calls. The expected usage
 * is: all plugins register their hooks at startup; the engine then
 * reads `preHooksFor` / `postHooksFor` on the request path.
 */
final class HookManagerImpl extends HookManager {

 // ADDITIVE in C10-PR-A: thread-local set by `EngineImpl.use(plugin)`
 // before invoking `plugin.setup(engine)`. Hook registration reads it
 // to attribute each hook to the registering plugin. Cleared by
 // `EngineImpl.use` in a `finally` block. Falls back to "<core>" if
 // registration happens outside any `use(plugin)` call (defensive;
 // not the supported path).
 //
 // FIX C10-PR-A final-gate F1 (HIGH): use InheritableThreadLocal so
 // child threads spawned during a plugin's setup() inherit the
 // ambient plugin name. Plain ThreadLocal silently mis-attributes
 // hooks registered from worker threads to "<core>".
 private[core] val currentPlugin: InheritableThreadLocal[String] = new InheritableThreadLocal[String] {
  override def initialValue(): String = "<core>"
 }

 // Per-stage hook buffers. Sort key: (priority ASC, seq ASC).
 private val preHooks: scala.collection.mutable.Map[HookStage, scala.collection.mutable.Buffer[HookEntry[PreHook]]] = scala.collection.mutable.Map.empty
 private val postHooks: scala.collection.mutable.Map[HookStage, scala.collection.mutable.Buffer[HookEntry[PostHook]]] = scala.collection.mutable.Map.empty

 // AtomicLong so concurrent register* don't share a sequence slot.
 private val nextSeq: AtomicLong = new AtomicLong(0L)

 /**
 * SDK signature: priority-only. Delegates to the origin-aware
 * overload with `HookOrigin.FirstParty` (the default for
 * reference plugins in `io.sm8.plugins.*`).
 *
 * Same SDK throw contract as before: throws
 * `IllegalArgumentException` on negative priority. Out-of-FirstParty
 * priorities are NOT enforced at this overload — use the 4-arg
 * overload for strict RFC §8 conformance.
 */
 override def registerPreHook(stage: HookStage, hook: PreHook, priority: Int): HookManager = {
 require(priority >= 0, s"sm8: priority must be non-negative, got $priority")
 val entry = HookEntry(hook, priority, nextSeq.incrementAndGet(), HookOrigin.FirstParty, currentPlugin.get)
 preHooks.getOrElseUpdate(stage, scala.collection.mutable.Buffer.empty) += entry
 this
 }

 /**
 * Origin-aware SDK-surface overload (RFC §8 conformance).
 * The SDK trait defines this with a default implementation that
 * delegates to the int-only overload; here in the impl we
 * override to enforce the typed-origin range check.
 */
 override def registerPreHook(stage: HookStage, hook: PreHook, priority: Int, origin: HookOrigin): HookManager = {
 HookOrigin.validate(origin, priority) match {
  case Right(_) => // ok
  case Left(msg) =>
  throw new IllegalArgumentException(s"sm8: $msg [stage=$stage hook=${hook.name}]")
 }
 val entry = HookEntry(hook, priority, nextSeq.incrementAndGet(), origin, currentPlugin.get)
 preHooks.getOrElseUpdate(stage, scala.collection.mutable.Buffer.empty) += entry
 this
 }

 override def registerPostHook(stage: HookStage, hook: PostHook, priority: Int): HookManager = {
 require(priority >= 0, s"sm8: priority must be non-negative, got $priority")
 val entry = HookEntry(hook, priority, nextSeq.incrementAndGet(), HookOrigin.FirstParty, currentPlugin.get)
 postHooks.getOrElseUpdate(stage, scala.collection.mutable.Buffer.empty) += entry
 this
 }

 override def registerPostHook(stage: HookStage, hook: PostHook, priority: Int, origin: HookOrigin): HookManager = {
 HookOrigin.validate(origin, priority) match {
  case Right(_) => // ok
  case Left(msg) =>
  throw new IllegalArgumentException(s"sm8: $msg [stage=$stage hook=${hook.name}]")
 }
 val entry = HookEntry(hook, priority, nextSeq.incrementAndGet(), origin, currentPlugin.get)
 postHooks.getOrElseUpdate(stage, scala.collection.mutable.Buffer.empty) += entry
 this
 }

 /**
 * Return all PreHooks for `stage`, sorted by (priority ASC, seq ASC).
 * Empty if no PreHooks registered.
 */
 override def preHooksFor(stage: HookStage): Seq[(PreHook, Int)] =
 hooksForStage(preHooks, stage)

 /**
 * Return all PostHooks for `stage`, sorted by (priority ASC, seq ASC).
 * Empty if no PostHooks registered.
 */
 override def postHooksFor(stage: HookStage): Seq[(PostHook, Int)] =
 hooksForStage(postHooks, stage)

 /**
 * ADDITIVE in C10-PR-A: override the SDK's default `listAllHooks()`
 * with an impl that preserves the actual `origin` and `pluginName`
 * recorded at registration time. The SDK default returns
 * `HookOrigin.FirstParty` + `"<unknown>"` — good enough for clients
 * that don't override, but the impl has the real data.
  */
 override def listAllHooks(): Seq[io.sm8.sdk.RegisteredHook] = {
 val pre = HookStage.values.toSeq.flatMap { stage =>
  preHooks.get(stage).toSeq.flatten.map { e =>
   io.sm8.sdk.RegisteredHook(
    name       = e.hook.name,
    stage      = stage,
    priority   = e.priority,
    origin     = e.origin,
    pluginName = e.pluginName
   )
  }
 }
 val post = HookStage.values.toSeq.flatMap { stage =>
  postHooks.get(stage).toSeq.flatten.map { e =>
   io.sm8.sdk.RegisteredHook(
    name       = e.hook.name,
    stage      = stage,
    priority   = e.priority,
    origin     = e.origin,
    pluginName = e.pluginName
   )
  }
 }
 (pre ++ post).sortBy(h => (h.stage, h.priority, h.pluginName, h.name))
 }

 /**
 * Sort-by-read helper. `Map` is parametric over the hook type T
 * (PreHook | PostHook); each call site knows its concrete T.
 */
 private def hooksForStage[T](
  store: scala.collection.mutable.Map[HookStage, scala.collection.mutable.Buffer[HookEntry[T]]],
  stage: HookStage
 ): Seq[(T, Int)] = store.get(stage) match {
 case None  => Seq.empty
 case Some(buf) =>
  // Sort on read. The per-stage buffer is small (handful of hooks per
  // stage), so the sort cost is negligible compared to the hook
  // bodies themselves.
  buf.toSeq.sortBy(e => (e.priority, e.seq)).map(e => (e.hook, e.priority))
 }
}

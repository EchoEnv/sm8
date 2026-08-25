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
      new PostHook with java.io.Serializable {
        override val name: String = "MetaCaptureObserver"
        override val stage: HookStage = HookStage.PostExecute
        override val priority: Int = 999
        override def run(context: Context): Context = {
          target.set(context.meta)
          context
        }
        // Observer: always fire, even when a pre-hook set stop=true.
        override val runsOnStop: Boolean = true
      },
      999
    )
  }

  /** The only captured state is the (Serializable) meta reference. */
  override def closedOverVars: Seq[String] = Seq("target")
}
/*
 * SM8 example Plugin — the copy-and-modify starting point.
 *
 * Registers one PostExecute hook that stamps a trace tag into
 * `context.meta` and counts its fires. The single purpose (RFC
 * plugins.md Rule 2) is tracing: an observer hook that enriches the
 * context with a per-run tag for downstream consumers.
 *
 * A hook runs inside the same JVM as the engine, so anything it
 * captures must survive Java serialization — the engine may journal
 * the closure. Rules applied here (see the README in this directory):
 *   - the plugin and every hook are `final class ... with
 *     java.io.Serializable`;
 *   - captured state is limited to serializable values (an
 *     `AtomicInteger` counter) and listed in `closedOverVars`;
 *   - no `SparkSession`, connection, or driver-only object is
 *     captured;
 *   - the hook never throws: a validator would, an observer must
 *     not (fail-fast policy means a thrown hook kills the pipeline).
 */
package io.sm8.plugins.example

import java.util.concurrent.atomic.AtomicInteger

import io.sm8.sdk._

/**
 * The plugin class. Implementations of `io.sm8.sdk.Plugin` are the
 * only thing `engine.use(...)` consumes. `setup` is called exactly
 * once at startup and must only register — no connections, no IO.
 *
 * @see <a href="https://github.com/EchoEnv/sm8/blob/main/docs/rfcs/2026-08-12_v1_architecture-spec/plugins.md">plugins.md</a>
 */
final class ExamplePlugin extends Plugin with java.io.Serializable {

  /** Constructor-captured state, declared for the closure-safety
    * introspection contract. Every captured `val` must be listed and
    * must be `Serializable`.
    *
    * @return the names of every constructor-captured `val`/`var`
    */
  override def closedOverVars: Seq[String] = Seq("fires")

  /** Test-visible counter of hook fires. `AtomicInteger` because
    * hooks may run concurrently; `Serializable` so the journal
    * round-trip preserves the count. The counter increments on
    * every invocation including null-request passthroughs — the
    * hook DID fire, so the counter reflects that. */
  val fires: AtomicInteger = new AtomicInteger(0)

  /**
    * Register the plugin's hooks with the engine. Called once at
    * startup by `Engine.use(plugin)`; registration only, no IO.
    *
    * @param engine the engine to register hooks on
    */
  override def setup(engine: Engine): Unit = {
    // PostExecute = after the execute stage produced a raw result.
    // First-party priority range (100-899); 200 leaves room for
    // other first-party hooks around it. Lower runs first.
    engine.hooks.registerPostHook(
      HookStage.PostExecute,
      new ExampleTraceHook(fires),
      priority = 200
    )
  }
}

/**
 * Single source of truth for the namespaced trace tag the hook
 * writes into `context.meta`. Other plugins (or test code) read this
 * key — they MUST NOT depend on the hook class directly, since the
 * hook is `private[example]`. Namespacing per RFC plugins.md Rule 3
 * keeps the key from colliding with another plugin's meta entries.
 */
object ExamplePlugin {
  /** Namespaced meta key for the trace tag. Format:
    * `example:<RequestSimpleClassName>`. */
  val TraceTagKey: String = "sm8.example.traceTag"
}

/**
 * The hook itself. An Observer (see hooks.md "Types of Hooks"): it
 * reads the context, writes a derived value to `context.meta`, and
 * never throws. The tag is deterministic — same request, same tag.
 *
 * Captured state (`counter`) is an `AtomicInteger`, which is
 * `Serializable`. Anything non-serializable (a SparkSession, a
 * socket, a driver-only cache) would break journal capture at
 * runtime, not at compile time.
 */
private[example] final class ExampleTraceHook(counter: AtomicInteger)
    extends PostHook with java.io.Serializable {

  override val name: String = "example-trace"
  override val priority: Int = 200
  /** The pipeline stage this hook is bound to (PostExecute). */
  override val stage: HookStage = HookStage.PostExecute

  /** Meta key the hook writes. Namespaced so it can't collide with
    * other plugins' keys (RFC plugins.md Rule 3 — plugins
    * communicate through context.meta, never by importing each
    * other). Single source of truth: see [[ExamplePlugin.TraceTagKey]]. */
  final val TraceTagKey: String = ExamplePlugin.TraceTagKey

  /**
    * Run the observer: bump the counter and write the trace tag.
    * Must not throw (observer semantics; a throwing hook would
    * fail the whole pipeline).
    *
    * A null `context.request` is skipped rather than trusted: an
    * observer must survive any Context a future caller hands it,
    * including malformed ones with null fields — that is what
    * "never throws" means here.
    *
    * @param context the shared pipeline context
    * @return the context with `meta` extended by the trace tag
    */
  override def run(context: Context): Context = {
    counter.incrementAndGet()
    if (context.request == null) context
    else {
      // Deterministic: derive the tag from the request class name.
      // A real observer would read model name / result size / status.
      val tag = s"example:${context.request.getClass.getSimpleName}"
      context.copy(meta = context.meta + (TraceTagKey -> tag))
    }
  }
}


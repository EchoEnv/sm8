/*
 * SM8 SDK — Plugin.
 *
 * The unit of extension. A Plugin is a named, versioned bundle that, on
 * load, registers one or more Hooks and/or Transformers with
 * the engine. It is the only thing a contributor publishes and the only
 * thing `engine.use(.)` consumes.
 *
 * Per RFC §7 + plugins.md: `Plugin.setup(engine) -> void` registers
 * hooks via `engine.hooks.register(stage, fn, priority)` and
 * transformers via `engine.transformers.register(.)`. Data-source
 * wiring is not a Plugin concern — it is owned by the `EngineProvider`
 * ServiceLoader seam in the connector modules.
 *
 * Frozen after Step 1. The `setup(engine: Engine)` method signature is
 * the SDK contract. Any change is a breaking SDK change.
 */
package io.sm8.sdk

/**
 * A Plugin is the unit of extension. Implementations register Hooks
 * with the engine during `setup`.
 *
 * Plugin authors should:
 * - keep `setup` idempotent-safe (it is called once at startup per the
 *  RFC plugins.md Rule 1);
 * - NOT open connections, NOT touch external systems from setup — data
 *  sources are wired by the engine-portable `EngineProvider` SPI, not
 *  by a Plugin;
 * - hold one clear purpose (RFC plugins.md Rule 2);
 * - NOT import other Plugins directly — read what they need from
 *  `context.meta` at hook-time (RFC plugins.md Rule 3).
 */
trait Plugin extends java.io.Serializable {

 /**
 * Register this Plugin's Hooks with the engine.
 *
 * Called exactly once at startup by `Engine.use(plugin)`. Must not
 * throw under normal operation; if registration fails, return without
 * registering (the engine will log a warning, per RFC Q6 warn-and-skip).
 *
 * @param engine the engine being configured
 */
 def setup(engine: Engine): Unit

 /**
 * Self-documented closure-safety contract (RFC §7 + plugins.md Rule 1
 * + RFC §13 thread-safety).
 *
 * The Plugin author declares the names of any constructor-captured
 * state (in the usual case: an `AtomicInteger` for fire-counts, a
 * `ResultCache` reference, a `StorageLevel` for the materialize
 * plugin, etc.) so a future serialization-safety spec can introspect
 * this list and assert that the only state captured is `Serializable`.
 *
 * by Spark UDFs / lambdas in `Dataset.map` must avoid non-serializable
 * refs (`SparkSession`, `Iterator`, `Connection`)." This accessor
 * makes that contract mechanically introspectable: a Plugin's
 * `closedOverVars` must list every captured reference, and the
 * serialization spec asserts each one is `Serializable`.
 *
 * Default `Nil` (no constructor-captured state — pure setup-only
 * Plugin). Plugins that DO capture state override and list every
 * captured `val`/`var` name.
 *
 * ADDITIVE 
 * a default-implemented new method on the trait. Source-compatible
 * (existing Plugins compile unchanged; they inherit the `Nil`
 * default). Binary-compatible: the v-table slot is at the end of
 * the trait's method table, so any existing class that implements
 * `Plugin` continues to load and link (the JVM does not verify
 * completeness of an implementation against a trait's v-table
 * unless the method is called).
 */
 def closedOverVars: Seq[String] = Seq.empty
}
/*
 * SM8 Core — EngineImpl.
 * Concrete implementation of the `io.sm8.sdk.Engine` trait. Lives in
 * `io.sm8.core` (internal — not SDK). Plugin authors get an Engine
 * via `EngineImpl()` or via a factory method in a future step.
 * Audit fixes (Step 3 audit), per scala-jvm-safety-mindset:
 * - `seenPlugins` is now a `ConcurrentHashMap.newKeySet` (was
 *   `mutable.Set[Plugin]` — non-thread-safe under concurrent `use()`)
 * - `catch (Throwable)` replaced with `NonFatal` so `Error`
 *   subclasses propagate; `InterruptedException` restores the
 *   interrupt flag (no swallowed shutdown signals)
 * - `Pipeline` is hoisted to a `val` field — was allocated per
 *   `run(request)` (hot path; the Pipeline is stateless)
 * Plugin authors ship `META-INF/services/io.sm8.sdk.Plugin` (class
 * name) + `META-INF/sm8/plugin.properties` (groupId + artifactId).
 */
package io.sm8.core

import java.util.ServiceLoader
import java.util.concurrent.ConcurrentHashMap

import scala.util.control.NonFatal

import io.sm8.sdk._

/**
 * Concrete Engine. Holds the 3 registries + the Pipeline. `use(plugin)`
 * calls `plugin.setup(this)` and is forgiving (bad plugins warn, never
 * crash — per karpathy-app-design §4.2 + RFC Q6 fail-loud-but-survivable).
 */
final class EngineImpl extends Engine {

 private val _connectors: ConnectorRegistryImpl = new ConnectorRegistryImpl
 private val _hooks:   HookManagerImpl   = new HookManagerImpl
 private val _transformers: TransformerRegistryImpl = new TransformerRegistryImpl

 // Hoisted from per-run allocation; the Pipeline is stateless.
 private val pipeline: Pipeline = new Pipeline(_connectors, _hooks, _transformers)

 // Thread-safe set for plugin idempotency. ConcurrentHashMap.newKeySet
 // is the only Set in the standard library that scales under writes.
 private val seenPlugins: java.util.Set[Plugin] =
 ConcurrentHashMap.newKeySet[Plugin]()

 override def use(plugin: Plugin): Engine = {
 if (!seenPlugins.add(plugin)) return this // already seen → idempotent no-op
 try {
  plugin.setup(this)
 } catch {
  case NonFatal(e) =>
  // Per karpathy-app-design §4.2: bad plugins warn, never crash.
  // System.err is a stop-gap until SLF4J wiring (deferred to Step 7).
  System.err.println(
   s"[sm8] Plugin ${plugin.getClass.getName} failed to setup: ${e.getMessage}")
  seenPlugins.remove(plugin)
  case _: InterruptedException =>
  // Restore the interrupt flag and let the caller decide.
  seenPlugins.remove(plugin)
  Thread.currentThread().interrupt()
  throw new InterruptedException("sm8: plugin setup interrupted")
 }
 this
 }

 override def run(request: Request): Result =
 pipeline.run(request)

 override def connectors: ConnectorRegistry = _connectors
 override def hooks: HookManager    = _hooks
 override def transformers: TransformerRegistry = _transformers

 // ---- Portal (Step 7) ----

 /**
 * ServiceLoader-based Plugin discovery with Maven-coords allowlist.
 * Per RFC Q6: only Plugins whose `groupId:artifactId` is in
 * `allowed` are loaded. Bad coords / missing metadata → warning +
 * skip, never crash.
 * is an IO boundary — wrap in `NonFatal`, surface as a warning.
 * startup; no shared mutable state created.
 * @param allowed set of `groupId:artifactId` strings
 * @return the Plugins that were successfully loaded
 */
 def discover(allowed: Set[String]): List[Plugin] =
 discoverInternal(allowAll = false, allowed)

 /**
 * Discover every Plugin on the classpath, ignoring the allowlist.
 * Dev convenience only — production code must use
 * `discover(allowed)` per Q6.
 * @return all Plugins found, in ServiceLoader iteration order
 */
 def discoverAll(): List[Plugin] =
 discoverInternal(allowAll = true, allowed = Set.empty)

 /**
 * ServiceLoader-based Plugin discovery with Maven-coords allowlist
 * loaded from `sm8.plugins.allowed` on the classpath.
 * The allowlist file is a newline-separated list of `groupId:artifactId`
 * strings. Missing file = empty allowlist = load everything
 * (matches `discoverAll()` behavior, for development convenience).
 * Per the agile-kindling-beacon plan line 286 ("a third-party
 * Plugin JAR gets loaded when its coords are in `sm8.plugins.allowed`"):
 * this is the configuration mechanism for production deployments
 * to gate which Plugins load.
 * are skipped (warned), never crash.
 * `Class.getResourceAsStream`) — the classloader lookup is
 * classpath-root-relative; `Class.getResourceAsStream` without a
 * leading `/` is package-relative, which silently misses global
 * resources like `sm8.plugins.allowed`.
 * is a thin convenience method over `discover(allowed)`. It does
 * NOT introduce a new discovery mechanism.
 * @return the Plugins that were successfully loaded
 */
 def discoverFromConfig(): List[Plugin] = {
 val resource = "sm8.plugins.allowed"
 val stream = Option(getClass.getClassLoader).map(_.getResourceAsStream(resource)).orNull
 if (stream == null) {
  // No allowlist configured - behave like discoverAll().
  // This is NOT an error; the engine degrades to permissive discovery.
  discoverAll()
 } else {
  try {
  val allowed = scala.io.Source.fromInputStream(stream, "UTF-8").getLines().map(_.trim).filter(s => s.nonEmpty && !s.startsWith("#")).toSet
  discover(allowed)
  } catch {
  case NonFatal(e) =>
   System.err.println(
   s"[sm8] Could not read sm8.plugins.allowed: ${e.getMessage}")
   List.empty
  } finally stream.close()
 }
 }

 /**
 * Shared implementation. `allowAll = true` skips the allowlist
 * filter (for `discoverAll`).
 */
 private def discoverInternal(allowAll: Boolean, allowed: Set[String]): List[Plugin] = {
 import scala.jdk.CollectionConverters._
 // Per Scala 2.13 + JDK interop: `ServiceLoader.load(Plugin.class)`
 // returns a `ServiceLoader[Plugin]` whose iterator yields `Plugin`
 // instances directly (not `ServiceLoader.Provider[Plugin]` — that's
 // the JDK 9 API which is hidden by Scala's import). `next()` can
 // throw `ServiceConfigurationError` for malformed entries; we
 // catch it as `NonFatal`.
 val plugins = ServiceLoader.load(classOf[Plugin]).iterator().asScala
 val loaded = List.newBuilder[Plugin]
 plugins.foreach { plugin =>
  try loadMetadata(plugin.getClass) match {
  case Some(meta) if allowAll || allowed.contains(meta.coords) =>
   use(plugin)
   loaded += plugin
  case Some(meta) =>
   System.err.println(
   s"[sm8] Plugin ${plugin.getClass.getName} skipped — coords ${meta.coords} not in allowlist")
  case None =>
   System.err.println(
   s"[sm8] Plugin ${plugin.getClass.getName} skipped — no META-INF/sm8/plugin.properties")
  } catch {
  case NonFatal(e) =>
   // ServiceLoader can throw ServiceConfigurationError for
   // malformed META-INF/services entries or class-loading
   // failures. Surface as a warning; do NOT crash.
   System.err.println(
   s"[sm8] Plugin ${plugin.getClass.getName} could not be loaded: ${e.getMessage}")
  }
 }
 loaded.result()
 }

 /**
 * Load `META-INF/sm8/plugin.properties` from the Plugin's
 * classloader. Returns None if missing or malformed (caller logs).
 * `Class.getResourceAsStream`) — the classloader lookup is
 * classpath-root-relative; `Class.getResourceAsStream` without a
 * leading `/` is package-relative, which silently misses global
 * resources like `META-INF/sm8/.`.
 */
 private def loadMetadata(cls: Class[_]): Option[PluginMetadata] = {
 val resource = "META-INF/sm8/plugin.properties"
 val stream = Option(cls.getClassLoader).map(_.getResourceAsStream(resource)).orNull
 if (stream == null) None
 else {
  try {
  val props = new java.util.Properties()
  props.load(stream)
  Some(PluginMetadata(
   props.getProperty(PluginMetadata.GroupIdKey, ""),
   props.getProperty(PluginMetadata.ArtifactIdKey, "")
  ))
  } catch {
  case NonFatal(_) => None
  } finally stream.close()
 }
 }
}

/**
 * Factory for the default Engine implementation. Used by tests and
 * by callers who don't need a custom registry backing.
 * Returns the concrete type (not the trait) so the Portal methods
 * (`discover`, `discoverAll`) are visible at the call site.
 * The trait `Engine` is the SDK boundary; the concrete type
 * is internal and may add more methods without breaking the SDK.
 */
object EngineImpl {
 def apply(): EngineImpl = new EngineImpl
}
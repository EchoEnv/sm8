/*
 * SM8 SDK — PluginMetadata.
 *
 * Maven-coordinate identifier for a published SM8 Plugin. Plugin authors
 * embed this in `META-INF/sm8/plugin.properties` next to the
 * `META-INF/services/io.sm8.sdk.Plugin` declaration. The Portal uses
 * `coords` to gate which Plugins load (Q6 = C — public ecosystem +
 * Maven-coords allowlist).
 *
 * Loaded from a properties file (data, not code).
 *
 * Rejected at the boundary — empty groupId, artifactId, or version
 * are programmer errors on the Plugin author's part.
 */
package io.sm8.sdk

/**
 * Maven coordinates for a Plugin. Read from `META-INF/sm8/plugin.properties`
 * during Portal discovery.
 *
 * @param groupId  Maven groupId (e.g., "io.sm8.connectors")
 * @param artifactId Maven artifactId (e.g., "in-memory-connector")
 */
final case class PluginMetadata(
    groupId:    String,
    artifactId: String,
    version:    String = "0.0.0"
) {

 require(groupId.nonEmpty, "sm8: plugin groupId must not be empty")
 require(artifactId.nonEmpty, "sm8: plugin artifactId must not be empty")
 require(version.nonEmpty, "sm8: plugin version must not be empty")

 /** The "groupId:artifactId" form used in the allowlist. */
 def coords: String = s"$groupId:$artifactId"
}

/**
 * Setup status for a discovered plugin. Surfaced by C10's
 * `PluginDiscovery.discoverAll()` so callers can distinguish
 * "discovered on the classpath" from "actually registered via
 * `Engine.use(plugin)`" — useful for ops/debug endpoints.
 *
 * Per RFC `plugins.md` Rule 1: setup is called once at startup,
 * not per-request. If a plugin was discovered but never used, it
 * is harmless but unverified.
 */
sealed trait SetupStatus
object SetupStatus {

 /** Plugin was registered via `Engine.use(plugin)`. */
 final case class Registered(pluginName: String) extends SetupStatus

 /**
  * Plugin was discovered on the classpath but never registered.
  *
  * @param className the ServiceLoader-resolved class name
  * @param reason    why the plugin was not registered (e.g. allowlist gate, setup error)
  */
 final case class NotRegistered(className: String, reason: String) extends SetupStatus
}

object PluginMetadata {

 /**
 * Properties-file keys. Plugin authors write these in
 * `META-INF/sm8/plugin.properties`:
 *
 * {{{
 * groupId=io.sm8.connectors
 * artifactId=in-memory-connector
 * }}}
 */
 val GroupIdKey: String  = "groupId"
 val ArtifactIdKey: String = "artifactId"

 /**
  * ADDITIVE in C10-PR-A: properties-file key for the Plugin's
  * Maven version. Optional in the properties file — missing key
  * falls back to `"0.0.0"` (synthetic) per the `Plugin` trait
  * default impl. Plugin authors SHOULD publish real coordinates
  * in their `META-INF/sm8/plugin.properties`.
  */
 val VersionKey: String = "version"
}

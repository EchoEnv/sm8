/*
 * SM8 SDK — PluginMetadata.
 *
 * Maven-coordinate identifier for a published SM8 Plugin. Plugin authors
 * embed this in `META-INF/sm8/plugin.properties` next to the
 * `META-INF/services/io.sm8.sdk.Plugin` declaration. The Portal uses
 * `coords` to gate which Plugins load (Q6 = C — public ecosystem +
 * Maven-coords allowlist).
 *
 * Per [[scala-data-driven-refactor-mindset]]: data only, no behavior.
 * Loaded from a properties file (data, not code).
 *
 * Per [[scala-jvm-safety-mindset]]: the constructor validates at
 * the boundary — empty groupId / artifactId are rejected (programmer
 * error on the Plugin author's part).
 */
package io.sm8.sdk

/**
 * Maven coordinates for a Plugin. Read from `META-INF/sm8/plugin.properties`
 * during Portal discovery.
 *
 * @param groupId     Maven groupId (e.g., "io.sm8.connectors")
 * @param artifactId Maven artifactId (e.g., "in-memory-connector")
 */
final case class PluginMetadata(groupId: String, artifactId: String) {

  require(groupId.nonEmpty, "sm8: plugin groupId must not be empty")
  require(artifactId.nonEmpty, "sm8: plugin artifactId must not be empty")

  /** The "groupId:artifactId" form used in the allowlist. */
  def coords: String = s"$groupId:$artifactId"
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
  val GroupIdKey: String     = "groupId"
  val ArtifactIdKey: String = "artifactId"
}
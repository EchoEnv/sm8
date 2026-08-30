/*
 * SM8 Spark Connector — engine-identity constants.
 *
 * Single source of truth for the values that appear in every
 * `EngineIdentity` built inside this connector. The registry keys
 * engines by `identity.name`, and the URL parser advertises the
 * same wire-stable name, so a literal drifting between the
 * provider, the descriptor, and the parser silently breaks
 * engine dispatch. Referencing these constants keeps the three
 * sites byte-identical by construction.
 *
 * ==Field meanings==
 *
 * - `WireName`: the wire-stable engine name. Must equal
 *   `EngineUrl.Spark.engineName` and
 *   `SparkEngineUrlParser.engineName` (the routing invariant —
 *   pinned by `SparkEngineIdentityInvariantSpec`).
 * - `DescriptorName`: the descriptor's SPI entry-point name.
 *   Deliberately distinct from `WireName`: the descriptor is the
 *   adapter-version entry point, not a realized engine (see the
 *   `realize` scaladoc on `SparkEngineProviderDescriptor`).
 * - `UnrealizedNativeVersion`: sentinel used before a Spark
 *   session exists (descriptor construction, null-session
 *   provider).
 * - `AdapterVersion`: this adapter's version, informational.
 *
 * ==Why an object and not inline literals==
 *
 * `EngineIdentity` is pure data; the constants it carries are
 * shared across three construction sites per engine. Inline
 * literals drift (they did: the descriptor's object companion
 * used `"unknown"` where the instance used `"<uninitialized>"`).
 * One object per connector module keeps the layer discipline:
 * core carries the shape, each connector carries its own values.
 */
package io.sm8.connectors.spark

/** Identity constants for the Spark connector. */
private[spark] object SparkEngineConstants {

  /** Wire-stable engine name — the registry routing key. */
  val WireName: String = "spark"

  /** Descriptor SPI entry-point name (adapter-version literal). */
  val DescriptorName: String = "spark-3.5"

  /** Native-version sentinel before a SparkSession exists. */
  val UnrealizedNativeVersion: String = "<uninitialized>"

  /** This adapter's version (informational). */
  val AdapterVersion: String = "0.1.0"
}

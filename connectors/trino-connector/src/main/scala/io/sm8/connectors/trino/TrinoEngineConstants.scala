/*
 * SM8 Trino Connector — engine-identity constants.
 *
 * Single source of truth for the values that appear in every
 * `EngineIdentity` built inside this connector. The registry keys
 * engines by `identity.name`, and the URL parser advertises the
 * same wire-stable name, so a literal drifting between the
 * provider, the descriptor, and the parser silently breaks
 * engine dispatch. Referencing these constants keeps the three
 * sites byte-identical by construction.
 *
 * The two `nativeVersion` sentinels distinguish "no URL realized
 * yet" from "URL realized, JDBC client not yet wired" (this
 * connector is a stub until the Trino cluster is provisioned).
 *
 * See `SparkEngineConstants` for the fuller rationale; the shape
 * is mirrored per connector so each connector owns its values
 * (core carries the identity shape only, per the layer
 * discipline).
 */
package io.sm8.connectors.trino

/** Identity constants for the Trino connector. */
private[trino] object TrinoEngineConstants {

  /** Wire-stable engine name — the registry routing key. */
  val WireName: String = "trino"

  /** Native-version sentinel before a URL is realized. */
  val UnrealizedNativeVersion: String = "<uninitialized>"

  /** Native-version value once a URL is realized (client stub). */
  val RealizedStubNativeVersion: String = "client-ready"

  /** Native-version used by the descriptor object companion. */
  val DescriptorNativeVersion: String = UnrealizedNativeVersion

  /** This adapter's version (informational). */
  val AdapterVersion: String = "0.1.0"
}

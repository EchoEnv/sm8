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
 *
 * ==Why the routing invariant is runtime-pinned==
 *
 * The constraint "`WireName` must equal the URL parser's
 * `engineName`" is enforced by the corresponding
 * `TrinoEngineIdentityInvariantSpec` at test time. Scala 2 has
 * no cross-module compile-time literal-comparison mechanism, so
 * a future maintainer who bypasses this object and writes the
 * literal at a new call site will be caught by the test rather
 * than by the compiler. The Scaladoc states the invariant; the
 * spec enforces it.
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

  /** This adapter's version (informational). */
  val AdapterVersion: String = "0.1.0"
}

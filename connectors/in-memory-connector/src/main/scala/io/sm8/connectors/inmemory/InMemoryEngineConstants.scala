/*
 * SM8 In-Memory Connector — engine-identity constants.
 *
 * Single source of truth for the values that appear in every
 * `EngineIdentity` built inside this connector. The registry keys
 * engines by `identity.name`, and the URL parser advertises the
 * same wire-stable name, so a literal drifting between the
 * provider, the descriptor, and the parser silently breaks
 * engine dispatch. Referencing these constants keeps the sites
 * byte-identical by construction.
 *
 * The in-memory engine is embedded: there is no remote to
 * realize, so a single native-version value ("embedded") covers
 * every lifecycle stage — unlike the Spark / Trino connectors,
 * which distinguish realized from unrealized states.
 *
 * See `SparkEngineConstants` for the fuller rationale; the shape
 * is mirrored per connector so each connector owns its values
 * (core carries the identity shape only, per the layer
 * discipline).
 */
package io.sm8.connectors.inmemory

/** Identity constants for the in-memory connector. */
private[inmemory] object InMemoryEngineConstants {

  /** Wire-stable engine name — the registry routing key. */
  val WireName: String = "in-memory"

  /** Native-version value (embedded engine — always the same). */
  val NativeVersion: String = "embedded"

  /** This adapter's version (informational). */
  val AdapterVersion: String = "0.1.0"
}

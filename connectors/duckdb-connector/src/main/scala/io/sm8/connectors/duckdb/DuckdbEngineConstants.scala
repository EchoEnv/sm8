/*
 * SM8 DuckDB Connector — engine-identity constants.
 *
 * Single source of truth for the values that appear in every
 * `EngineIdentity` built inside this connector. Mirrors the shape
 * of `SparkEngineConstants` / `TrinoEngineConstants` so each
 * connector owns its values (core carries the identity shape only,
 * per the layer discipline).
 *
 * DuckDB native-version sentinels distinguish "no URL realized yet"
 * from "URL realized, JDBC connection open".
 */
package io.sm8.connectors.duckdb

/** Identity constants for the DuckDB connector. */
private[duckdb] object DuckdbEngineConstants {

  /** Wire-stable engine name — the registry routing key. */
  val WireName: String = "duckdb"

  /** Native-version sentinel before a URL is realized. */
  val UnrealizedNativeVersion: String = "<uninitialized>"

  /** This adapter's version (informational). */
  val AdapterVersion: String = "0.1.0"

  /** The JDBC URL prefix every DuckDB URL must start with. */
  val UrlPrefix: String = "jdbc:duckdb:"
}

/*
 * SM8 DuckDB Connector — EngineUrlParser (per RFC adapters.md Rule 4).
 *
 * DuckDB URLs: `jdbc:duckdb:` (in-memory database) or
 * `jdbc:duckdb:/path/to/db.duckdb` (file-backed). Both forms keep
 * DuckDB in-process — there is no remote host in the grammar.
 *
 * Registered via SPI:
 *   META-INF/services/io.sm8.core.engine.EngineUrlParser
 */
package io.sm8.connectors.duckdb

import io.sm8.core.engine.{EngineError, EngineUrl, EngineUrlParser}

class DuckdbEngineUrlParser extends EngineUrlParser {

  override def engineName: String = DuckdbEngineConstants.WireName

  override def parse(raw: String): Either[EngineError, EngineUrl] = {
    val trimmed = if (raw == null) "" else raw.trim
    if (trimmed.isEmpty) {
      Left(EngineError.ConnectionFailed(
        engine  = DuckdbEngineConstants.WireName,
        reason  = "blank URL",
        message = "sm8: DuckDB URL must be non-blank (use 'jdbc:duckdb:' for an in-memory database)"
      ))
    } else if (trimmed.startsWith(DuckdbEngineConstants.UrlPrefix)) {
      Right(EngineUrl.DuckDb(jdbcUrl = trimmed))
    } else {
      Left(EngineError.ConnectionFailed(
        engine  = DuckdbEngineConstants.WireName,
        reason  = "URL grammar mismatch",
        message = s"sm8: DuckDB URL must start with '${DuckdbEngineConstants.UrlPrefix}', got '$trimmed'"
      ))
    }
  }
}

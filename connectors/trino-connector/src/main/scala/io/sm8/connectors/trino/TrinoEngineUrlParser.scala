/*
 * SM8 Trino Connector — EngineUrlParser (PR-15, ADR-008-Q §C1).
 *
 * Per RFC `adapters.md` Rule 4: per-connector URL grammar validation.
 * Trino URLs: `jdbc:trino://host:port[/catalog/schema]` (JDBC-style).
 *
 * Registered via SPI:
 *   META-INF/services/io.sm8.core.engine.EngineUrlParser
 */
package io.sm8.connectors.trino

import io.sm8.core.engine.{EngineError, EngineUrl, EngineUrlParser}

class TrinoEngineUrlParser extends EngineUrlParser {
  override def engineName: String = "trino"

  override def parse(raw: String): Either[EngineError, EngineUrl] = {
    val trimmed = raw.trim
    if (trimmed.isEmpty) {
      Left(EngineError.ConnectionFailed(
        engine = "trino",
        reason = "blank URL",
        message = "sm8: Trino URL must be non-blank"
      ))
    } else if (trimmed.startsWith("jdbc:trino://")) {
      Right(EngineUrl.Trino(jdbcUrl = trimmed))
    } else {
      Left(EngineError.ConnectionFailed(
        engine = "trino",
        reason = "URL grammar mismatch",
        message =
          s"sm8: Trino URL must start with 'jdbc:trino://', got '$trimmed'"
      ))
    }
  }
}

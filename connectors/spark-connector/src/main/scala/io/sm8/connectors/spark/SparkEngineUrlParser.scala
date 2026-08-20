/*
 * SM8 Spark Connector — EngineUrlParser (PR-15, ADR-008-Q §C1).
 *
 * Per RFC `adapters.md` Rule 4: per-connector URL grammar validation.
 * Spark URLs: `local[*]` (local mode), `spark://host:port` (standalone),
 * `spark-connect://host:port` (Spark Connect protocol).
 *
 * Registered via SPI:
 * META-INF/services/io.sm8.core.engine.EngineUrlParser
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineError, EngineUrl, EngineUrlParser}

class SparkEngineUrlParser extends EngineUrlParser {
 override def engineName: String = "spark"

 override def parse(raw: String): Either[EngineError, EngineUrl] = {
 val trimmed = raw.trim
 if (trimmed.isEmpty) {
  Left(EngineError.ConnectionFailed(
  engine = "spark",
  reason = "blank URL",
  message = "sm8: Spark URL must be non-blank"
  ))
 } else if (
  trimmed.startsWith("local[") ||
  trimmed.startsWith("spark://") ||
  trimmed.startsWith("spark-connect://")
 ) {
  Right(EngineUrl.Spark(master = trimmed))
 } else {
  Left(EngineError.ConnectionFailed(
  engine = "spark",
  reason = "unsupported URL grammar",
  message =
   s"sm8: Spark URL must start with 'local[', 'spark://', or 'spark-connect://', got '$trimmed'"
  ))
 }
 }
}

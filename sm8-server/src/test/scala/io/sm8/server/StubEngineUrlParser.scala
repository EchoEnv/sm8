/*
 * SM8 Server test fixture: a stub `EngineUrlParser` registered via SPI
 * for the engine name "stub-spark". Accepts any non-blank URL.
 */
package io.sm8.server

import io.sm8.core.engine.{EngineError, EngineUrl, EngineUrlParser}

class StubEngineUrlParser extends EngineUrlParser {
  override val engineName: String = "stub-spark"

  override def parse(raw: String): Either[EngineError, EngineUrl] =
    if (raw.trim.isEmpty)
      Left(EngineError.ConnectionFailed(
        engine = "stub-spark",
        reason = "blank URL",
        message = "sm8: stub parser rejected blank URL"
      ))
    else
      Right(EngineUrl.Spark(master = raw))
}

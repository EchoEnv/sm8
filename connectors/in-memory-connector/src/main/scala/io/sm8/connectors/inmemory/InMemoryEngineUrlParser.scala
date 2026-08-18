/*
 * SM8 In-Memory Connector — EngineUrlParser (PR-15, ADR-008-Q §C1).
 *
 * Per RFC `adapters.md` Rule 4: in-memory has no URL grammar. The
 * parser accepts any non-blank URL (treated as an optional seed hint)
 * OR an empty URL (no seed). Returns `EngineUrl.InMemory(...)`.
 *
 * Registered via SPI:
 *   META-INF/services/io.sm8.core.engine.EngineUrlParser
 */
package io.sm8.connectors.inmemory

import io.sm8.core.engine.{EngineError, EngineUrl, EngineUrlParser}

class InMemoryEngineUrlParser extends EngineUrlParser {
  override def engineName: String = "in-memory"

  override def parse(raw: String): Either[EngineError, EngineUrl] = {
    val trimmed = raw.trim
    // In-memory: empty URL = no seed; any non-blank URL = seed hint.
    // (Per RFC adapters.md Rule 4: per-connector grammar — we accept
    // anything, including blank, since there's no real connection.)
    Right(EngineUrl.InMemory(seed = if (trimmed.isEmpty) None else Some(trimmed)))
  }
}

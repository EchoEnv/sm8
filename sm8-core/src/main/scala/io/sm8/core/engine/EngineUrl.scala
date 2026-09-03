/*
 * SM8 Core — EngineUrl typed URL grammar.
 *
 * The typed carrier for an engine connection URL. Per RFC `adapters.md`
 * Rule 4: "Per-connector `realize()` validates its own URL grammar;
 * the deployment module does NOT validate." This file therefore does NOT
 * contain grammar parsers — it only carries the typed shape. Parsers
 * live in each connector, registered via the `EngineUrlParser` SPI
 * (see `EngineUrlParser.scala`).
 *
 * ==Why a sealed trait (not a Map[String, URLParser])==
 *
 * trait + match". The case-class set is fixed, small, known-at-compile-time;
 * each new reference engine adds one more `case class` next to the
 * existing 4 (Spark, Trino, DuckDb, InMemory; the trait is `sealed`
 * so the compiler enforces exhaustiveness). External connectors do
 * NOT extend the sealed trait — they use the `EngineUrlParser` SPI
 * to validate their grammar and realize against the existing 4
 * cases. (The DuckDB engine was promoted to a dedicated case (not
 * the `InMemory(seed)` slot) because the in-memory case would
 * have lied about the engine name in audit + routing events.)
 *
 * ==Why NOT put the grammar parser in the core companion==
 *
 * Per RFC `adapters.md` Rule 4 (verbatim): "Per-connector `realize()`
 * validates its own URL grammar". Putting grammar parsers here would
 * import connector knowledge into the supposedly engine-portable core,
 * Per the core-boundary rule (no grammar parsers here), external
 * connectors are decoupled from this trait's body.
 *
 * ==Serializable (Spark closure-safety + Restate journal capture)==
 *
 * §1: this trait `extends Serializable` because `EngineRegistry`
 * stores `Map[String, EngineProvider]` and survives `Restate.run`
 * journal capture (PR-C5b-extension). Connectors' parser results
 * (`EngineUrl` instances) may also cross the closure boundary via
 * `TypedRealizationProvider.realizeTyped`.
 *
 * ==Wire-stable shape ==
 *
 * The 4 case classes carry ONLY the typed fields needed for the
 * engine-specific realization (master URL, JDBC URL, or no-arg for
 * embedded). No `String` parser output leaks into core.
 */
package io.sm8.core.engine

/**
 * Typed URL for engine connection. Connector-neutral carrier.
 *
 * is pure data; behavior lives in the connector's `EngineUrlParser`
 * (see `EngineUrlParser.scala`).
 */
sealed trait EngineUrl extends Product with Serializable {
 /** The raw URL string (after connector-side parsing). */
 def raw: String

 /** Wire-stable engine name (e.g. "spark", "trino", "in-memory"). */
 def engineName: String
}

object EngineUrl {

 /** Spark engine URL: a Spark master URL (e.g. `local[*]`,
 * `spark://host:7077`, `spark-connect://host:15002`). */
 final case class Spark(master: String) extends EngineUrl {
 val raw: String  = master
 val engineName: String = "spark"
 }

 /** Trino engine URL: a JDBC-style URL (e.g. `jdbc:trino://host:8080`). */
 final case class Trino(jdbcUrl: String) extends EngineUrl {
 val raw: String  = jdbcUrl
 val engineName: String = "trino"
 }

 /** DuckDB engine URL: a JDBC-style URL (`jdbc:duckdb:` for an
 * in-memory database, or `jdbc:duckdb:/path/to/db.duckdb` for a
 * file-backed one). The `InMemory` seed-slot lies about the engine
 * name ("in-memory" instead of "duckdb"), which corrupts audit
 * events and breaks the conformance base's routing-invariant
 * check; the dedicated case keeps the wire-stable name on the typed
 * carrier. */
 final case class DuckDb(jdbcUrl: String) extends EngineUrl {
 val raw: String  = jdbcUrl
 val engineName: String = "duckdb"
 }

 /** In-memory engine URL: no connection needed (embedded reference
 * engine). The optional seed hint is for connectors that want a
 * deterministic seed. */
 final case class InMemory(seed: Option[String] = None) extends EngineUrl {
 val raw: String  = seed.getOrElse("in-memory")
 val engineName: String = "in-memory"
 }

 /** Engine-name-only factory: dispatches to the engine-specific parser
 * registered via SPI (`EngineUrlParser`).
 *
 * `Either[EngineError, EngineUrl]` (typed error, not silent `None`).
 *
 * once, at the boundary. If the engineName is unknown OR the
 * parser rejects the raw URL, the typed error names the failure
 * mode (e.g. `EngineError.ConnectionFailed(engine = "spark",
 * reason = "URL must start with 'spark://' or 'local['")`).
 *
 * @param engineName wire-stable engine name (e.g. "spark", "trino")
 * @param raw  the raw URL string (parsed by the engine-specific parser)
 * @return   `Right(EngineUrl)` on success; `Left(EngineError)`
 *     on failure
 */
 def parse(engineName: String, raw: String): Either[EngineError, EngineUrl] = {
 if (engineName == null || engineName.trim.isEmpty) {
  Left(EngineError.ConnectionFailed(
  engine = "<unknown>",
  reason = "blank engine name",
  message = "sm8: EngineUrl.parse requires a non-blank engineName"
  ))
 } else if (raw == null) {
  Left(EngineError.ConnectionFailed(
  engine = engineName,
  reason = "null URL",
  message = s"sm8: EngineUrl.parse requires a non-null URL for engine '$engineName'"
  ))
 } else {
  EngineUrlParser.lookup(engineName) match {
  case Right(parser) => parser.parse(raw)
  case Left(parseErr) => Left(parseErr)
  }
 }
 }
}

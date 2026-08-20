/*
 * SM8 Core — EngineUrlParser SPI.
 *
 * Per-connector URL grammar validator. Per RFC `adapters.md` Rule 4
 * (verbatim): "Per-connector `realize()` validates its own URL grammar;
 * the deployment module does NOT validate."
 *
 * Each connector that supports URL-based connection registers its own
 * parser via SPI:
 *
 * META-INF/services/io.sm8.core.engine.EngineUrlParser
 *
 * The file contains the FQN of one or more `EngineUrlParser`
 * implementations (e.g. `io.sm8.connectors.spark.SparkEngineUrlParser`).
 *
 * ==Why a subtrait (NOT a Map[String, EngineUrlParser])==
 *
 * implementations): the trait in core; per-engine impl in connectors.
 * + match, escalate to Map only when the rule set "must change without
 * a deploy". URL grammar validation is fixed at compile time (one parser
 * per engine), so a trait is correct.
 *
 * ==Why a separate file (not in EngineUrl.scala)==
 *
 * concern per file. The `EngineUrl` sealed trait (data) is separate from
 * the `EngineUrlParser` trait (behavior registration).
 */
package io.sm8.core.engine

/**
 * Per-connector URL grammar validator. Discovered via SPI.
 *
 * this is the per-connector parser, not a core data type. The core
 * `EngineUrl.parse(.)` factory delegates to the engine-specific parser
 * looked up by name.
 *
 * for the same reasons as `EngineUrl` (Restate journal capture +
 * Spark closure-safety).
 */
trait EngineUrlParser extends Serializable {
 /** Wire-stable engine name (e.g. "spark", "trino"). Must match the
 * `engineName` field on the resulting `EngineUrl` case class. */
 def engineName: String

 /** Parse the raw URL string into the typed `EngineUrl` for this
 * engine. Returns `Left(EngineError.ConnectionFailed(.))` on
 * invalid URL (per RFC `adapters.md` Rule 4 — typed error). */
 def parse(raw: String): Either[EngineError, EngineUrl]
}

object EngineUrlParser {
 /** SPI lookup: discover all registered parsers + return the one
 * matching `engineName`. Returns `Left(EngineError.EngineUnavailable(.))`
 * if no parser is registered for the engine name.
 *
 * `None`. The caller (sm8-server `EngineLoader.discoverAndRealize`)
 * can surface the missing-parser case as a boot failure (fail-loud
 * per design §4.1).
 *
 * standard discovery mechanism — no reflection on the connector
 * class itself.
 *
 * ServiceLoader is per-classloader; we cache the loaded instances
 * per-call (no static state to leak).
 */
 def lookup(
  engineName: String,
  classLoader: ClassLoader = Thread.currentThread.getContextClassLoader
 ): Either[EngineError, EngineUrlParser] = {
 import scala.jdk.CollectionConverters._
 val allParsers: List[EngineUrlParser] =
  java.util.ServiceLoader.load(classOf[EngineUrlParser], classLoader).iterator().asScala.toList

 allParsers.find(_.engineName == engineName) match {
  case Some(parser) => Right(parser)
  case None  =>
  val available = allParsers.map(_.engineName).sorted
  Left(EngineError.EngineUnavailable(
   engine  = engineName,
   available = available,
   wasDefault = false,
   message = s"sm8: no EngineUrlParser registered for engine '$engineName' " +
      s"(available: [${available.mkString(", ")}])"
  ))
 }
 }
}

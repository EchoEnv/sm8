/*
 * SM8 Core — TypedRealizationProvider subtrait.
 *
 * The typed-error companion to `EngineProvider.realize(url: String):
 * Option[EngineProvider]`. Per ADR-008-Q §C2 (Architect P0-2 binary-compat):
 *
 * "Either keep the new typed method in a new capability subtrait
 * (e.g. `TypedRealizationProvider`) discovered/checked by
 * `EngineLoader`, or explicitly prove the chosen Scala default-method
 * encoding with MiMa against representative separately compiled
 * implementors and document that external providers must recompile
 * before v0.1.0."
 *
 * trait breaks binary compatibility for existing implementors compiled
 * against the old version"): the subtrait is the safe option.
 *
 * implementations): the subtrait is the Protocol; connectors
 * implement it. Per §1 ("errors are
 * data"): the typed `Either[EngineError, EngineProvider]` return type
 * replaces the silent `Option[.]` (which could not distinguish
 * "engine doesn't support URL realization" from "URL is invalid for
 * this engine").
 *
 * ==Default impl (single realization path per ADR-008-Q §C7 / DE P1-B)==
 *
 * The default `realizeTyped(parsedUrl)` impl delegates to the existing
 * `realize(url: String): Option[EngineProvider]`. This guarantees ONE
 * realization path: connectors that don't implement this subtrait
 * continue to use the legacy `realize(url)` (with `None` as a
 * silent failure mode); connectors that DO implement it can return
 * typed errors. Connectors MAY override the default for engine-specific
 * error messages (e.g. wrapping a `SparkException` from
 * `SparkSession.builder().getOrCreate()` into `ConnectionFailed`).
 */
package io.sm8.core.engine

/**
 * Typed realization contract (subtrait of `EngineProvider`).
 *
 * typed-error realization. 
 * ("errors are data"): the return type is `Either[EngineError,
 * EngineProvider]` (typed error), not `Option[.]` (silent failure).
 *
 * Connectors that want typed-error realization implement BOTH
 * `EngineProvider` AND this subtrait. The `EngineLoader` (in sm8-server)
 * discovers the typed realization via `instanceOf TypedRealizationProvider`
 * check.
 */
trait TypedRealizationProvider extends EngineProvider {

 /** Typed realization from a parsed `EngineUrl`. Returns
 * `Right(realizedProvider)` on success; `Left(EngineError)`
 * on failure (typed error per `scala-` §1).
 *
 * Default impl (per ADR-008-Q §C7 / DE P1-B): delegates to the
 * legacy `realize(url: String): Option[EngineProvider]`. Connectors
 * MAY override for engine-specific typed errors.
 *
 * The conversion from `Option` to `Either` is the only place where
 * the legacy silent-failure mode is converted to typed error — and
 * the typed error names the exact failure mode
 * (`ConnectionFailed(engine, reason = "realize(url) returned None for parsed url")`).
 */
 def realizeTyped(parsedUrl: EngineUrl): Either[EngineError, EngineProvider] =
 TypedRealizationProvider.defaultRealizeTyped(this, parsedUrl)
}

object TypedRealizationProvider {

 /** Default `realizeTyped` impl: converts `EngineUrl` → raw string,
 * delegates to `realize(url: String)`, maps `Some(p)` → `Right(p)`,
 * `None` → `Left(ConnectionFailed(.))`.
 *
 * boundary conversion. The legacy `realize(url)` may return `None`
 * for two reasons (URL invalid OR engine doesn't support URL
 * realization); we collapse both into a single typed error here
 * because the connector's specific override (if any) provides the
 * granular error.
 *
 * must close. The realization path is pure construction; no
 * resources are opened here.
 */
 def defaultRealizeTyped(
  self:  EngineProvider,
  parsedUrl: EngineUrl
 ): Either[EngineError, EngineProvider] =
 self.realize(parsedUrl.raw) match {
  case Some(p) => Right(p)
  case None =>
  Left(EngineError.ConnectionFailed(
   engine = parsedUrl.engineName,
   reason = s"realize(url) returned None for parsed url",
   message =
   s"sm8: ${parsedUrl.engineName} engine rejected URL " +
   s"'${parsedUrl.raw}' " +
   s"(realize(url) default returns None for blank/invalid URLs)"
  ))
 }
}

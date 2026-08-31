/*
 * SM8 DuckDB Connector — ServiceLoader descriptor (PR-O4g parity).
 *
 * The descriptor is the ONLY thing registered via SPI for engine
 * discovery (`META-INF/services/io.sm8.core.engine.EngineProvider`).
 * Its `available` flag reflects the connector's intrinsic state —
 * the embedded driver is always available (no remote to fail).
 */
package io.sm8.connectors.duckdb

import io.sm8.core.engine.{EngineContext, EngineError, EngineIdentity, EngineProvider, EngineUrl, TypedRealizationProvider}
import DuckdbEngineConstants._

class DuckdbEngineProviderDescriptor extends TypedRealizationProvider {

  override def identity: EngineIdentity =
    EngineIdentity(
      name                 = WireName,
      nativeVersion        = UnrealizedNativeVersion,
      engineAdapterVersion = AdapterVersion)

  /** The descriptor itself does not hold a connection — it just
    * knows how to construct realized providers. Available = true
    * because the embedded driver is always loadable; a real
    * failure surfaces at `realize(url)` time as typed
    * `EngineError.ConnectionFailed`. */
  override val available: Boolean = true

  /** Realize a typed provider from a raw `jdbc:duckdb:` URL string.
    * The return type narrows to `Option[DuckdbEngineProvider]` (not
    * the generic `Option[EngineProvider]`) so callers can use the
    * concrete type without a cast. */
  override def realize(url: String): Option[DuckdbEngineProvider] =
    if (url == null || url.trim.isEmpty) None
    else if (url.trim.startsWith(UrlPrefix)) Some(new DuckdbEngineProvider(url.trim))
    else None

  override def realizeTyped(parsedUrl: EngineUrl): Either[EngineError, EngineProvider] =
    parsedUrl match {
      case duck: EngineUrl.DuckDb =>
        realize(duck.jdbcUrl) match {
          case Some(p) => Right(p)
          case None    =>
            Left(EngineError.ConnectionFailed(
              engine  = WireName,
              reason  = "realize(url) returned None for parsed url",
              message = s"sm8: duckdb descriptor: unexpected URL for parsed url: '${duck.jdbcUrl}'"
            ))
        }
      case other =>
        Left(EngineError.ConnectionFailed(
          engine  = WireName,
          reason  = "unexpected EngineUrl case for duckdb descriptor",
          message = s"sm8: duckdb descriptor received non-DuckDB EngineUrl: ${other.getClass.getSimpleName}"
        ))
    }

  override def query(model: io.sm8.core.model.Model, request: io.sm8.core.engine.QueryRequest, ctx: io.sm8.core.engine.EngineContext) =
    Left(io.sm8.core.engine.EngineError.FeatureDeferred(
      engine  = WireName,
      feature = "DuckdbEngineProviderDescriptor.query",
      release = "1.0.0",
      message = "Descriptor carries no JDBC connection; call realize(url) first."
    ))

  override def explain(model: io.sm8.core.model.Model, request: io.sm8.core.engine.QueryRequest, ctx: io.sm8.core.engine.EngineContext) =
    Right(s"duckdb.descriptor.explain(${model.name}) — call realize(url) first to run SQL.")
}

object DuckdbEngineProviderDescriptor {
  def identity: EngineIdentity =
    EngineIdentity(
      name                 = WireName,
      nativeVersion        = UnrealizedNativeVersion,
      engineAdapterVersion = AdapterVersion)
}

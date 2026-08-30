/*
 * SM8 Trino Engine Provider — ServiceLoader descriptor (PR-O4g parity, ADR-008-O).
 *
 * Mirrors the spark connector's `SparkEngineProviderDescriptor` pattern
 * (PR-O4g, ADR-008-O) so that:
 *   1. `META-INF/services/io.sm8.core.engine.EngineProvider` declares
 *      THIS descriptor (no trino-client import in the SPI path —
 *      ServiceLoader discoverers don't pull the heavy provider class).
 *   2. `realize(url)` constructs the real `TrinoEngineProvider(url)`
 *      with the parsed JDBC URL.
 *   3. `realizeTyped(parsedUrl)` wraps the realization in typed
 *      `Either[EngineError, EngineProvider]` per ADR-008-Q §C2 /
 *      PR-15, so callers can surface grammar / connection failures
 *      instead of silent `None`.
 *
 * The descriptor carries only identity + realize(); the heavy provider
 * stays in `TrinoEngineProvider(url)`. This matches the SPI shape that
 * the spark + in-memory connectors use (or will use, after PR-195).
 *
 * PR-195 (Round 2 Review B/C): closes the descriptor-pattern gap that
 * `TrinoEngineProvider` was previously registered directly under SPI,
 * bypassing the descriptor indirection. The spark connector followed
 * this pattern from PR-O4g; trino + in-memory had not yet been updated.
 *
 * Trino client wiring remains a stub (no live `TrinoClient` is created
 * here — that lands with the Trino cluster provisioning, same follow-up
 * shape as the spark connector's pre-#41 skeleton).
 */
package io.sm8.connectors.trino

import io.sm8.core.engine.{EngineError, EngineIdentity, EngineProvider, EngineUrl, TypedRealizationProvider}

class TrinoEngineProviderDescriptor extends TypedRealizationProvider {

  override lazy val identity: EngineIdentity =
    EngineIdentity(
      name                 = TrinoEngineConstants.WireName,
      nativeVersion        = TrinoEngineConstants.UnrealizedNativeVersion,
      engineAdapterVersion = TrinoEngineConstants.AdapterVersion
    )

  override val available: Boolean = false

  /** PR-O4g parity: validate the URL grammar before realizing. Returns
    * `None` on blank / non-`jdbc:trino://` URLs (the legacy silent
    * contract). New code should use `realizeTyped` for typed errors. */
  override def realize(url: String): Option[EngineProvider] =
    if (url == null || url.trim.isEmpty) None
    else if (url.trim.startsWith("jdbc:trino://")) Some(new TrinoEngineProvider(url.trim))
    else None

  /** PR-15 (ADR-008-Q §C2): typed realization. Matches the parser at
    * `TrinoEngineUrlParser.parse` — accept `EngineUrl.Trino`, reject
    * all other cases with a typed `ConnectionFailed` error. */
  override def realizeTyped(parsedUrl: EngineUrl): Either[EngineError, EngineProvider] =
    parsedUrl match {
      case trino: EngineUrl.Trino =>
        realize(trino.jdbcUrl) match {
          case Some(p) => Right(p)
          case None    => Left(EngineError.ConnectionFailed(
            engine  = "trino",
            reason  = "realize(url) returned None for parsed url",
            message = s"sm8: trino engine: unexpected URL grammar for parsed url: '${trino.jdbcUrl}'"
          ))
        }
      case other => Left(EngineError.ConnectionFailed(
        engine  = "trino",
        reason  = "unexpected EngineUrl case for trino descriptor",
        message = s"sm8: trino descriptor received non-Trino EngineUrl: ${other.getClass.getSimpleName}"
      ))
    }

  override def query(
    model: io.sm8.core.model.Model,
    request: io.sm8.core.engine.QueryRequest,
    ctx: io.sm8.core.engine.EngineContext
  ): Either[EngineError, io.sm8.core.engine.PortableQueryResult] =
    Left(EngineError.UnsupportedCapability(
      engine     = identity.name,
      capability = "TrinoEngineProviderDescriptor.query",
      message    = "Descriptor carries no TrinoClient; call realize(url) first."
    ))

  override def explain(
    model: io.sm8.core.model.Model,
    request: io.sm8.core.engine.QueryRequest,
    ctx: io.sm8.core.engine.EngineContext
  ): Either[EngineError, String] =
    Right(s"trino.explain(${model.name}): no TrinoClient (descriptor)")
}

object TrinoEngineProviderDescriptor {
  def identity: EngineIdentity =
    EngineIdentity(
      name                 = TrinoEngineConstants.WireName,
      nativeVersion        = TrinoEngineConstants.UnrealizedNativeVersion,
      engineAdapterVersion = TrinoEngineConstants.AdapterVersion)
}
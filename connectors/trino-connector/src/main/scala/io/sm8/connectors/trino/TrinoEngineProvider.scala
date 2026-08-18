/*
 * SM8 Trino Engine Provider — URL-realization stub (PR-B parity).
 *
 * Per RFC `adapters.md` Rule 4: a connector that supports
 * URL-based connection exposes a typed `realize(url)` method.
 * This stub validates the `jdbc:trino://` grammar and returns a
 * provider-shaped stub; the real TrinoClient wiring lands when
 * the Trino cluster is provisioned (same follow-up shape as the
 * Spark connector's pre-#41 skeleton).
 *
 * Per RFC §3: the connector is the ONLY piece that knows about
 * connection strings. The platform + deployment hold only the
 * string.
 */
package io.sm8.connectors.trino

import io.sm8.core.engine.{
  EngineContext, EngineError, EngineIdentity,
  EngineProvider, QueryRequest, PortableQueryResult
}
import io.sm8.core.model.Model

final class TrinoEngineProvider private (
    val jdbcUrl: Option[String]
) extends EngineProvider {

  /** No-arg ctor: contract-gap stub (no URL yet). */
  def this() = this(None)

  /** Typed realization per RFC adapters.md Rule 4. */
  def this(url: String) = this(Some(url))

  override lazy val identity: EngineIdentity =
    EngineIdentity(
      name                 = "trino",
      nativeVersion        = jdbcUrl.map(_ => "client-ready").getOrElse("<uninitialized>"),
      engineAdapterVersion = "0.1.0"
    )

  override val available: Boolean = jdbcUrl.isDefined

  override def realize(url: String): Option[EngineProvider] =
    if (url != null && url.trim.startsWith("jdbc:trino://"))
      Some(new TrinoEngineProvider(url.trim))
    else None

  override def query(
      model: Model, request: QueryRequest, ctx: EngineContext
  ): Either[EngineError, PortableQueryResult] =
    Left(EngineError.FeatureDeferred(
      engine = "trino", feature = "query", release = "post-v1.0.0",
      message = "TrinoEngineProvider runtime lands with the Trino cluster"))

  override def explain(
      model: Model, request: QueryRequest, ctx: EngineContext
  ): Either[EngineError, String] =
    Right(s"trino plan for ${model.name} (stub)")
}

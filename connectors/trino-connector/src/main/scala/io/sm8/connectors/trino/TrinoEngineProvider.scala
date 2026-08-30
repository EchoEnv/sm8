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
 *
 * PR-15 (ADR-008-Q §C2): extends `TypedRealizationProvider` for
 * typed-error realization. The `realizeTyped` override converts
 * the legacy `realize(url): Option` (silent None) into the typed
 * `Either[EngineError, EngineProvider]`.
 *
 * PR-202 (audit follow-up Bundle A4): adds `decideUnsupported`
 * honor-or-reject for `ctx.decisionHints` (ADR-009-d item 13),
 * mirroring the in-memory provider. Trino is a stub that returns
 * `FeatureDeferred` on `query` regardless of the decision — a
 * decided-but-ignored field is a more specific error (typed
 * `UnsupportedCapability` named by the platform meta key) than the
 * generic deferred stub error. The platform's portability claim
 * (any adapter reads the decision) is only sound if a decided-but-
 * ignored field is impossible. Mirroring `InMemoryEngineProvider`
 * keeps the per-adapter behavior deterministic across the 3
 * reference engines (in-memory / spark / trino).
 */
package io.sm8.connectors.trino

import io.sm8.core.engine.{
  DecisionHintsPolicy, EngineContext, EngineError, EngineIdentity, EngineProvider, EngineUrl,
  PortableQueryResult, QueryRequest, TypedRealizationProvider
}
import io.sm8.core.model.Model

final class TrinoEngineProvider private (
    val jdbcUrl: Option[String]
) extends TypedRealizationProvider {

  /** No-arg ctor: contract-gap stub (no URL yet). */
  def this() = this(None)

  /** Typed realization per RFC adapters.md Rule 4. */
  def this(url: String) = this(Some(url))

  override lazy val identity: EngineIdentity =
    EngineIdentity(
      name                 = TrinoEngineConstants.WireName,
      nativeVersion        = jdbcUrl.map(_ => TrinoEngineConstants.RealizedStubNativeVersion)
                                    .getOrElse(TrinoEngineConstants.UnrealizedNativeVersion),
      engineAdapterVersion = TrinoEngineConstants.AdapterVersion
    )

  override val available: Boolean = jdbcUrl.isDefined

  /** Legacy `realize(url): Option[EngineProvider]` (silent on bad URL). */
  override def realize(url: String): Option[EngineProvider] =
    if (url != null && url.trim.startsWith("jdbc:trino://"))
      Some(new TrinoEngineProvider(url.trim))
    else None

  /** PR-15: typed realization. The legacy `realize(url)` parser
    * already enforces the `jdbc:trino://` prefix; this override
    * surfaces a typed `ConnectionFailed` error if a non-Trino
    * `EngineUrl` is passed (defense in depth). */
  override def realizeTyped(parsedUrl: EngineUrl): Either[EngineError, EngineProvider] = parsedUrl match {
    case trino: EngineUrl.Trino =>
      realize(trino.jdbcUrl) match {
        case Some(p) => Right(p)
        case None    => Left(EngineError.ConnectionFailed(
          engine = "trino",
          reason = "realize(url) returned None for parsed url",
          message = s"sm8: trino engine: unexpected URL grammar for parsed url: '${trino.jdbcUrl}'"
        ))
      }
    case other => Left(EngineError.ConnectionFailed(
      engine = "trino",
      reason = "unexpected EngineUrl case for trino provider",
      message = s"sm8: trino provider received non-Trino EngineUrl: ${other.getClass.getSimpleName}"
    ))
  }

  override def query(
      model: Model, request: QueryRequest, ctx: EngineContext
  ): Either[EngineError, PortableQueryResult] =
    decideUnsupported(ctx) match {
      case Some(err) => Left(err)
      case None => Left(EngineError.FeatureDeferred(
        engine = "trino", feature = "query", release = "post-v1.0.0",
        message = "TrinoEngineProvider runtime lands with the Trino cluster"))
    }

  /** PR-204 (refactor): ADR-009-d item 13 honor-or-reject delegated
    * to `DecisionHintsPolicy.honorOrReject` (sm8-core). Mirrors
    * `InMemoryEngineProvider.decideUnsupported` exactly modulo the
    * adapter-specific `engineField`/`engineDisplayName` parameters
    * (per-engine convention; spark honors the decision natively and
    * does NOT use this helper). The empty (no-oracle) fold yields
    * `None` and keeps the prior `FeatureDeferred` behavior.
    *
    * History:
    *   - PR-202: Trino started mirroring InMemory's
    *     `decideUnsupported` 1:1 to close the cross-engine
    *     inconsistency. The duplication was explicitly deferred.
    *   - PR-204 (this refactor): extract the shared logic into
    *     `DecisionHintsPolicy`. Observable behavior unchanged. */
  private def decideUnsupported(ctx: EngineContext): Option[EngineError] =
    DecisionHintsPolicy.honorOrReject(ctx, "trino-connector", "trino engine")

  override def explain(
      model: Model, request: QueryRequest, ctx: EngineContext
  ): Either[EngineError, String] =
    Right(s"trino plan for ${model.name} (stub)")
}

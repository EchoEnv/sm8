/*
 * SM8 in-memory Engine Provider — always-available reference (PR-B parity).
 *
 * Per RFC `adapters.md` Rule 4: in-memory has no URL grammar —
 * `realize()` returns `None` (the default) because the provider
 * is already fully realized at construction. It exists so the
 * registry always has at least one available engine on a bare
 * classpath (test + reference shape).
 *
 * PR-15 (ADR-008-Q §C2): extends `TypedRealizationProvider` for
 * typed-error realization. In-memory has no grammar, so the
 * `realizeTyped` override accepts any `EngineUrl.InMemory` (and
 * returns `this` — in-memory is already realized).
 */
package io.sm8.connectors.inmemory

import io.sm8.core.engine.{
  DecisionHintsPolicy, EngineContext, EngineError, EngineIdentity, EngineProvider, EngineUrl,
  PortableQueryResult, QueryRequest, ResultSchema, TypedRealizationProvider
}
import io.sm8.core.model.Model

final class InMemoryEngineProvider() extends TypedRealizationProvider {

  override val identity: EngineIdentity =
    EngineIdentity(
      name                 = InMemoryEngineConstants.WireName,
      nativeVersion        = InMemoryEngineConstants.NativeVersion,
      engineAdapterVersion = InMemoryEngineConstants.AdapterVersion)

  override val available: Boolean = true

  /** PR-15: in-memory accepts any `EngineUrl.InMemory`. Returns
    * `this` (already realized). Defense in depth: rejects
    * `Spark` / `Trino` URLs. */
  override def realizeTyped(parsedUrl: EngineUrl): Either[EngineError, EngineProvider] = parsedUrl match {
    case _: EngineUrl.InMemory => Right(this)
    case other => Left(EngineError.ConnectionFailed(
      engine = "in-memory",
      reason = "unexpected EngineUrl case for in-memory provider",
      message = s"sm8: in-memory provider received non-InMemory EngineUrl: ${other.getClass.getSimpleName}"
    ))
  }

  override def query(
      model: Model, request: QueryRequest, ctx: EngineContext
  ): Either[EngineError, PortableQueryResult] =
    decideUnsupported(ctx) match {
      case Some(err) => Left(err)
      case None =>
        Right(PortableQueryResult(
          schema = ResultSchema(Nil),
          rows = Vector.empty,
          metadata = Map("engine" -> "in-memory")
        ))
    }

  /** PR-204 (refactor): ADR-009-d item 13 honor-or-reject delegated
    * to `DecisionHintsPolicy.honorOrReject` (sm8-core). See that
    * file for the deterministic order + null short-circuit
    * semantics. Engine field `"in-memory-connector"` + display name
    * `"in-memory engine"` match the per-engine convention.
    *
    * History:
    *   - PR-15: introduce typed `UnsupportedCapability` for the
    *     in-memory engine's broadcast/skew inability.
    *   - PR-200: add falsifiable spec coverage that routed real
    *     EngineContext through `query(...)` (the prior spec passed
    *     `null` for ctx, which short-circuited this branch).
    *   - PR-202: cross-engine parity — Trino started mirroring this
    *     logic (and that's when the duplication became obvious).
    *   - PR-204 (this refactor): extract the shared logic into
    *     `DecisionHintsPolicy`. Observable behavior unchanged. */
  private def decideUnsupported(ctx: EngineContext): Option[EngineError] =
    DecisionHintsPolicy.honorOrReject(ctx, "in-memory-connector", "in-memory engine")

  override def explain(
      model: Model, request: QueryRequest, ctx: EngineContext
  ): Either[EngineError, String] =
    Right(s"in-memory plan for ${model.name}")
}
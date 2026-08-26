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
  DecisionHints, EngineContext, EngineError, EngineIdentity, EngineProvider, EngineUrl,
  PortableQueryResult, QueryRequest, ResultSchema, TypedRealizationProvider
}
import io.sm8.core.model.Model

final class InMemoryEngineProvider() extends TypedRealizationProvider {

  override val identity: EngineIdentity =
    EngineIdentity(name = "in-memory", nativeVersion = "embedded", engineAdapterVersion = "0.1.0")

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

  /** ADR-009-d item 13 honor-or-reject: the platform fold populates
    * `ctx.decisionHints` from any registered broadcast/skew plugin.
    * In-memory has no native broadcast or skew join config, so it
    * cannot consume a decided field; the platform's portability claim
    * (any adapter reads the decision) is only sound if a decided-but-
    * ignored field is impossible. A non-empty decision therefore
    * surfaces as a typed `UnsupportedCapability` naming the first
    * decided field (deterministic order: broadcastArmed,
    * broadcastThresholdBytes, then skewArmed) rather than silently
    * dropping it. Capability strings are the platform meta keys
    * (`sm8.broadcast.arm` / `sm8.broadcast.thresholdBytes` /
    * `sm8.skew.arm`) that produced each field. The empty (no-oracle)
    * fold yields `None` and keeps the prior empty-success behavior. */
  private def decideUnsupported(ctx: EngineContext): Option[EngineError] =
    if (ctx == null) None
    else ctx.decisionHints.flatMap { dh =>
      val key =
        if (dh.broadcastArmed.isDefined) "sm8.broadcast.arm"
        else if (dh.broadcastThresholdBytes.isDefined) "sm8.broadcast.thresholdBytes"
        else if (dh.skewArmed.isDefined) "sm8.skew.arm"
        else ""
      if (key.isEmpty) None
      else Some(EngineError.UnsupportedCapability(
        engine     = "in-memory-connector",
        capability = key,
        message    = s"sm8: in-memory engine cannot honor decided field '$key'; " +
          "route to an engine with a native broadcast/skew config or drop the broadcast/skew plugin"
      ))
    }

  override def explain(
      model: Model, request: QueryRequest, ctx: EngineContext
  ): Either[EngineError, String] =
    Right(s"in-memory plan for ${model.name}")
}
/*
 * SM8 in-memory Engine Provider — always-available reference (PR-B parity).
 *
 * Per RFC `adapters.md` Rule 4: in-memory has no URL grammar —
 * `realize()` returns `None` (the default) because the provider
 * is already fully realized at construction. It exists so the
 * registry always has at least one available engine on a bare
 * classpath (test + reference shape).
 */
package io.sm8.connectors.inmemory

import io.sm8.core.engine.{
  EngineContext, EngineError, EngineIdentity,
  MCPEngineProvider, MCPQueryRequest, PortableQueryResult,
  ResultSchema
}
import io.sm8.core.model.Model

final class InMemoryEngineProvider() extends MCPEngineProvider {

  override val identity: EngineIdentity =
    EngineIdentity(name = "in-memory", nativeVersion = "embedded", engineAdapterVersion = "0.1.0")

  override val available: Boolean = true

  override def query(
      model: Model, request: MCPQueryRequest, ctx: EngineContext
  ): Either[EngineError, PortableQueryResult] =
    Right(PortableQueryResult(
      schema = ResultSchema(Nil),
      rows = Vector.empty,
      metadata = Map("engine" -> "in-memory")
    ))

  override def explain(
      model: Model, request: MCPQueryRequest, ctx: EngineContext
  ): Either[EngineError, String] =
    Right(s"in-memory plan for ${model.name}")
}

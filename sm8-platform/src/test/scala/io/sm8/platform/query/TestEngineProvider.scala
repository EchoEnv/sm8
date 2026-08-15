/*
 * SM8 Platform — test MCPEngineProvider for Main's ServiceLoader path.
 *
 * Registered via
 * `src/test/resources/META-INF/services/io.sm8.core.engine.MCPEngineProvider`
 * so `Main.discoverProviders` finds it exactly like production
 * connector JARs would (per ADR-006: engines arrive from the
 * connector layer via the classpath).
 *
 * Serializable per the closure-safety contract (PR #36): pure
 * fields only, round-trip proven in MainSpec.
 */
package io.sm8.platform.query

import io.sm8.core.engine.{
  EngineContext, EngineError, EngineIdentity,
  MCPEngineProvider, MCPQueryRequest, PortableQueryResult
}
import io.sm8.core.model.Model

final class TestEngineProvider extends MCPEngineProvider with java.io.Serializable {
  override val identity: EngineIdentity =
    EngineIdentity(name = "test-engine", nativeVersion = "1.0", engineAdapterVersion = "1.0")
  override val available: Boolean = true
  override def explain(
      model: Model, request: MCPQueryRequest, ctx: EngineContext
  ): Either[EngineError, String] =
    Right(s"test-engine plan for ${model.name}")
  override def query(
      model: Model, request: MCPQueryRequest, ctx: EngineContext
  ): Either[EngineError, PortableQueryResult] =
    Right(PortableQueryResult(
      schema = io.sm8.core.engine.ResultSchema(Nil),
      rows = Vector.empty,
      metadata = Map.empty,
    ))
}

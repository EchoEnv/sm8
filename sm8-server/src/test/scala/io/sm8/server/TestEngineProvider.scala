/*
 * SM8 Platform — test EngineProvider for Main's ServiceLoader path.
 *
 * Registered via
 * `src/test/resources/META-INF/services/io.sm8.core.engine.EngineProvider`
 * so `Main.discoverProviders` finds it exactly like production
 * connector JARs would (per ADR-006: engines arrive from the
 * connector layer via the classpath).
 *
 * Serializable per the closure-safety contract (PR #36): pure
 * fields only, round-trip proven in MainSpec.
 */
package io.sm8.server

import io.sm8.core.engine.{
  EngineContext, EngineError, EngineIdentity,
  EngineProvider, QueryRequest, PortableQueryResult
}
import io.sm8.core.model.Model

final class TestEngineProvider extends EngineProvider with java.io.Serializable {
  override val identity: EngineIdentity =
    EngineIdentity(name = "test-engine", nativeVersion = "1.0", engineAdapterVersion = "1.0")
  override val available: Boolean = true
  override def explain(
      model: Model, request: QueryRequest, ctx: EngineContext
  ): Either[EngineError, String] =
    Right(s"test-engine plan for ${model.name}")
  override def query(
      model: Model, request: QueryRequest, ctx: EngineContext
  ): Either[EngineError, PortableQueryResult] =
    Right(PortableQueryResult(
      schema = io.sm8.core.engine.ResultSchema(Nil),
      rows = Vector.empty,
      metadata = Map.empty,
    ))
}

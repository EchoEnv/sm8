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
 *
 * PR-15: extends `TypedRealizationProvider` (subtrait of `EngineProvider`)
 * for typed-error realization. The default delegate in
 * `TypedRealizationProvider.defaultRealizeTyped` invokes `self.realize(url)`;
 * `TestEngineProvider` does NOT override `realize(url)` so it inherits the
 * default `Option = None` — which the default delegate maps to
 * `Left(ConnectionFailed(...))`. This exercises the typed-error path
 * end-to-end.
 */
package io.sm8.server

import io.sm8.core.engine.{
  EngineContext, EngineError, EngineIdentity, EngineUrl,
  PortableQueryResult, QueryRequest, TypedRealizationProvider
}
import io.sm8.core.model.Model

final class TestEngineProvider extends TypedRealizationProvider with java.io.Serializable {
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

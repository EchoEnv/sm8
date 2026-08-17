/*
 * SM8 Spark Connector -- ModelRegistry.
 *
 * PR-M3 (per ADR-008-L Appendix GAP 3): the bridge that translates a
 * model-by-name (a `JoinSpec.rightModel` string) into a portable
 * `SourceRef`. Lives in the connector because the registry is
 * engine-portable in shape but the IMPLEMENTATION (which catalog
 * / session-scoped view / file glob holds the model-to-source map)
 * is adapter-specific behavior per RFC SS3.
 *
 * Two reference implementations (this PR):
 *   - `NoopModelRegistry`: returns `UnsupportedCapability` for every
 *     name. Suit deployments that do not support multi-model queries.
 *   - `SessionCatalogModelRegistry`: maps name -> SourceRef.ByName
 *     pointing at the active Spark catalog / session-scoped view.
 *     The natural v1 mapping for single-cluster deployments.
 *
 * Per [[karpathy-guidelines-mindset]]: smallest interface that the
 * 3 callers need (QueryBuilder, the deployment layer, the source
 * resolver). The trait is ONE method.
 *
 * Per [[scala-jvm-safety-mindset]]: zero spark imports; pure trait;
 * Product with Serializable.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.EngineError
import io.sm8.core.model.SourceRef

trait ModelRegistry extends java.io.Serializable {
  /** Look up a model by name. Returns `Right(SourceRef)` when the
    * model is registered; `Left(UnsupportedCapability)` when the
    * registry cannot resolve (the default for the noop impl). */
  def resolveModel(name: String): Either[EngineError, SourceRef]
}

object ModelRegistry {

  /** No-op registry: every model-by-name lookup returns
    * `UnsupportedCapability`. Use when the deployment does not
    * support multi-model queries (single-Model-per-deployment). */
  object NoopModelRegistry extends ModelRegistry {
    override def resolveModel(name: String): Either[EngineError, SourceRef] =
      Left(EngineError.UnsupportedCapability(
        engine     = "spark-3.5",
        capability = "ModelRegistry.resolveModel",
        message    = s"No model registry bound; cannot resolve model '$name'.",
      ))
  }
}

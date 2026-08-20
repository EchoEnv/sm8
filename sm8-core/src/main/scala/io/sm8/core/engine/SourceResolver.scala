/*
 * SM8 Core -- SourceResolver (engine-portable source-resolution trait).
 *
 * PR-L (per ADR-008-L): the boundary step BEFORE the engine sees
 * anything. `QueryBuilder.build(model, resolver, identity)` calls
 * `resolver.resolve(...)` to turn a portable `SourceRef` into a
 * typed `ResolvedSource` (carrying the schema + the provenance),
 * then walks the `Model` to emit a portable `RelOp` tree.
 *
 * behavior in adapters): the resolver trait + its `ResolvedSource`
 * ADT are pure data types here; the IMPLEMENTATION (catalog adapter,
 * file reader, REST fetcher) is engine- or deployment-specific
 * and lives in a connector / deployment module. The SM8 reactor
 * ships a `NoopSourceResolver` (test-only) so `QueryBuilderSpec`
 * has a concrete implementation to exercise.
 *
 * `/tmp/semanticdf/semanticdf-core/src/main/scala/io/semanticdf/core/engine/SourceResolver.scala`
 * with the same shape (`resolve` + `ResolvedSource` ADT with
 * Scan/NotFound/Incompatible/AuthFailed cases).
 *
 * types; Product with Serializable.
 */
package io.sm8.core.engine

import io.sm8.core.model.SourceRef
import io.sm8.core.schema.Field

/** Result of resolving a portable `SourceRef` against a
 * catalog / file system / REST endpoint.
 *
 * (NotFound / Incompatible / AuthFailed) are typed ADT cases
 * -- no silent defaulting, no exceptions-as-flow.
 */
sealed trait ResolvedSource extends Product with Serializable

object ResolvedSource {

 /** Successful resolution. Carries the actual schema (after
 * source resolution) + the original `SourceRef` (provenance).
 *
 * drift verify at the boundary): the engine adapter validates
 * the actual source's schema matches the model's expected
 * fields; a mismatch yields `EngineError.SourceSchemaChanged`.
 */
 final case class Scan(
  source: SourceRef,
  schema: List[Field],
 ) extends ResolvedSource

 /** The source could not be resolved (table not found, file
 * missing, endpoint unreachable). */
 final case class NotFound(
  source: SourceRef,
  reason: String,
 ) extends ResolvedSource

 /** The source was resolved but its shape is incompatible
 * (schema mismatch, type mismatch, partition missing, etc.). */
 final case class Incompatible(
  source: SourceRef,
  reason: String,
 ) extends ResolvedSource

 /** The source could not be resolved because authentication
 * failed (credentials missing, token expired, etc.). */
 final case class AuthFailed(
  source: SourceRef,
  reason: String,
 ) extends ResolvedSource
}

/** The trait a deployment / connector implements to make a
 * portable `SourceRef` resolvable against a concrete backing
 * system (catalog, filesystem, REST endpoint, etc.).
 *
 * `QueryBuilder` calls `resolve(sourceRef, identity)` for the
 * model's primary source; for `model.joins`, the builder calls
 * `resolveModel(rightModel, identity)` to translate the
 * right-model name into a `SourceRef` (which is then resolved).
 */
trait SourceResolver extends java.io.Serializable {

 /** Resolve a `SourceRef` into a `ResolvedSource`. The
 * `identity` carries the engine name + version for error
 * reporting.
 *
 * Returns `Right(ResolvedSource.Scan(schema, source))` on
 * success; `Right(NotFound/Incompatible/AuthFailed)` on
 * typed resolution failure; never throws.
 */
 def resolve(
  source: SourceRef,
  identity: EngineIdentity,
 ): Either[EngineError, ResolvedSource]

 /** Resolve a model-by-name to its `SourceRef`. Used by
 * `QueryBuilder.build` to translate `JoinSpec.rightModel`
 * into a resolvable reference. The default implementation
 * returns `UnsupportedCapability` (no model-name registry);
 * deployments that support multi-model queries supply a
 * registry-backed implementation. */
 def resolveModel(
  name:  String,
  identity: EngineIdentity,
 ): Either[EngineError, SourceRef] =
 Left(EngineError.UnsupportedCapability(
  engine  = identity.name,
  capability = "SourceResolver.resolveModel",
  message = s"Model-by-name resolution not supported by this resolver (name='$name').",
 ))
}

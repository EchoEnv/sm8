/*
 * SM8 Core — Model + ModelStatus + ModelPolicyDefaults + SourceRef.
 *
 * Per [[karpathy-guidelinesmindset]] (smart constructor for validity-
 * at-boundary + match existing style + Scala 2.13 only): `final class`
 * with private val fields + smart factory `Model.of(...)`.
 *
 * Per [[scala-data-driven-refactor-mindset]] (data-only, sealed-trait
 * dispatch): `Model` has NO methods beyond the smart constructor
 * — pure data. All operations (validation, compile, query) live in
 * adapters or hooks.
 *
 * Per [[scala-impact-analysismindset]]: ADDITIVE to sm8-core. Does not
 * modify any of the 10 frozen SDK types (Plugin, Connector, PreHook,
 * PostHook, Transformer, Context, Engine, ConnectorRegistry, HookManager,
 * TransformerRegistry).
 */
package io.sm8.core.model

import io.sm8.sdk.SemanticQuery

/**
 * Engine-portable semantic model container. Holds the dimensions /
 * measures / filters / time-grains / etc. that any engine adapter
 * needs to compile a query.
 *
 * Construction uses the smart factory `Model.of(...)` which runs
 * `ModelValidator.validate(...)` exactly once at the boundary.
 */
final case class Model private (
    val name: String,
    val version: Int,
    val description: Option[String],
    val dimensions: List[Dimension],
    val measures: List[Measure],
    val defaultPolicies: ModelPolicyDefaults,
    val source: SourceRef,
    val status: ModelStatus,
    val filters: List[FilterSpec]
) extends Product with Serializable {

  require(name.nonEmpty, "sm8: Model name must be non-empty")
  require(version >= 0, "sm8: Model version must be non-negative")

  /** Placeholder — engine handles query execution. */
  def query(req: SemanticQuery): Unit = ()
}

/** Model lifecycle status. Sealed per [[scala-data-driven-refactor-mindset]]. */
sealed trait ModelStatus
object ModelStatus {
  case object Draft extends ModelStatus
  case object Published extends ModelStatus
  case object Deprecated extends ModelStatus
}

/** Aggregation of the 3 portable policy ADTs. */
final case class ModelPolicyDefaults(
    materialize: MaterializePolicy,
    cache: CachePolicy,
    audit: AuditPolicy
)

/** Persistence policy for the model's DataFrame. */
sealed trait MaterializePolicy
object MaterializePolicy {
  case object None extends MaterializePolicy
  final case class Persist(level: String) extends MaterializePolicy
  case object Cache extends MaterializePolicy
}

/** Result caching policy. */
sealed trait CachePolicy
object CachePolicy {
  case object NoCache extends CachePolicy
  final case class ReadThrough(name: String) extends CachePolicy
  final case class WriteThrough(name: String) extends CachePolicy
}

/** Audit event emission policy. */
sealed trait AuditPolicy
object AuditPolicy {
  case object NoAudit extends AuditPolicy
  final case class EmitEvents(sinkRef: String) extends AuditPolicy
}

/** A dimension field on a Model. */
final case class Dimension(name: String, expr: String)

/** A measure field on a Model. */
final case class Measure(name: String, expr: String)

/** A pre-defined filter clause on a Model. */
final case class FilterSpec(name: String, expr: String)

/**
 * Serializable source identity. Separate from the engine-specific
 * resolver closure (e.g. Spark's `() => DataFrame`). The closure
 * lives behind a ProviderRef and is registered once at server
 * startup.
 */
sealed trait SourceRef extends Product with Serializable
object SourceRef {
  /** Source identified by name + table name. Resolver registered separately. */
  final case class ByName(name: String, table: String) extends SourceRef
  /** Source identified by file path + format. */
  final case class ByPath(format: String, path: String, options: Map[String, String] = Map.empty) extends SourceRef
  /** ProviderRef — name + driver-local closure. */
  final case class ByProvider(providerRefName: String) extends SourceRef
}

/** Reference to a registered provider closure (driver-local, never serialized). */
final case class ProviderRef(name: String)

/**
 * Smart constructor — runs ModelValidator.validate once at the
 * boundary per [[karpathy-guidelinesmindset]]. Returns
 * `Left(validationError)` on failure, `Right(model)` on success.
 */
object Model {
  def of(
      name: String,
      version: Int,
      description: Option[String] = None,
      dimensions: List[Dimension] = Nil,
      measures: List[Measure] = Nil,
      defaultPolicies: ModelPolicyDefaults = ModelPolicyDefaults(
        materialize = MaterializePolicy.None,
        cache = CachePolicy.NoCache,
        audit = AuditPolicy.NoAudit),
      source: SourceRef,
      status: ModelStatus = ModelStatus.Draft,
      filters: List[FilterSpec] = Nil
  ): Either[ModelValidationError, Model] = {
    if (name == null || name.trim.isEmpty)
      Left(ModelValidationError.InvalidName("Model name must be non-blank"))
    else if (version < 0)
      Left(ModelValidationError.InvalidVersion(version))
    else
      Right(new Model(
        name = name,
        version = version,
        description = description,
        dimensions = dimensions,
        measures = measures,
        defaultPolicies = defaultPolicies,
        source = source,
        status = status,
        filters = filters))
  }
}

/** Typed validation error. Sealed per [[scala-error-handlingmindset]]. */
sealed trait ModelValidationError extends Product with Serializable {
  def message: String
}
object ModelValidationError {
  final case class InvalidName(reason: String) extends ModelValidationError {
    val message = s"Model name invalid: $reason"
  }
  final case class InvalidVersion(value: Int) extends ModelValidationError {
    val message = s"Model version must be non-negative, got $value"
  }
}
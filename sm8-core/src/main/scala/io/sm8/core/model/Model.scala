/*
 * SM8 Core — Model + ModelStatus + ModelPolicyDefaults + SourceRef.
 *  * at-boundary + match existing style + Scala 2.13 only): `final class`
 * with private val fields + smart factory `Model.of(...)`.
 *  * dispatch): `Model` has NO methods beyond the smart constructor
 * — pure data. All operations (validation, compile, query) live in
 * adapters or hooks.
 *  * modify any of the 10 frozen SDK types (Plugin, Connector, PreHook,
 * PostHook, Transformer, Context, Engine, ConnectorRegistry, HookManager,
 * TransformerRegistry).
 */
package io.sm8.core.model

import io.sm8.core.rel.AggregateCall
import io.sm8.sdk.SemanticQuery

/**
 * Engine-portable semantic model container. Holds the dimensions /
 * measures / filters / time-grains / etc. that any engine adapter
 * needs to compile a query.
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
    val filters: List[FilterSpec],
    val calculatedMeasures: List[CalculatedMeasure] = Nil,
    val joins: List[JoinSpec] = Nil
) extends Product with Serializable

// // validity are separate"): NO `require` in the case-class body.
// Validity is enforced exactly once, at the boundary, by the
// `Model.of` smart constructor. The constructor is `private`, so
// `Model.of` is the only instantiation path — the former body
// requires were unreachable duplication. The placeholder
// `def query(...): Unit = ()` no-op (a behavior stub on a pure
// data type) is likewise removed: query execution lives in the
// engine adapters (EngineProvider), never on the data.

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

/** A dimension field on a Model. the current implementation:
  * re-typed `expr: String` -> `Expr` (matching the legacy semanticdf
  * pre-tag audit).   * is the IR, not free-form strings. A String allowed silent typos
  * at engine-compile time (the legacy's stated reason for typed
  * Expr). `dataType: Option[SealedDataType]` carries the optional
  * declared type for model validators; default `None` means
  * "infer from expr".
  * Smart constructor `Dimension.field(name, fieldName)` for the
  * common case (a column reference). The structural constructor is
  * for `Dimension(name, Expr.Add(...))` etc.
  */
final case class Dimension(
    name:     String,
    expr:     io.sm8.core.expr.Expr,
    dataType: Option[io.sm8.core.schema.SealedDataType] = None,
) extends Product with Serializable

object Dimension {
  /** Convenience factory: a typed FieldRef dimension, no declared
    * dataType (inferred from the column). */
  def field(name: String, fieldName: String): Dimension =
    Dimension(name = name, expr = io.sm8.core.expr.Expr.FieldRef(fieldName))
  /** Convenience factory: a typed FieldRef dimension WITH declared
    * dataType. */
  def field(name: String, fieldName: String, dataType: io.sm8.core.schema.SealedDataType): Dimension =
    Dimension(name = name, expr = io.sm8.core.expr.Expr.FieldRef(fieldName), dataType = Some(dataType))
}

/** A measure field on a Model. the current implementation: `expr` is now a `expr` is now a
  * typed `AggregateCall` (was `String`). The typed form forces
  * the model validator + engine adapter to handle every case
  * explicitly; a `String` would allow silent typos at
  * engine-compile time.
  * Smart constructor `Measure.aggregate(name, fn, expr)` covers
  * the common case (`SUM(amount) AS total`). The structural
  * constructor `Measure(name, AggregateCall(...))` is for the
  * less-common case (`COUNT(*)`, `APPROX_PERCENTILE(x, 0.95)`).
  */
final case class Measure(name: String, expr: AggregateCall) extends Product with Serializable

object Measure {

  /** Construct a single-aggregate measure (the common case).
    * `Measure.aggregate("total", AggregateFn.Sum, Expr.FieldRef("amount"))`
    * is equivalent to `Measure(name = "total", expr =
    * AggregateCall(fn = Sum, input = Some(FieldRef("amount")),
    * alias = "total"))`.
    */
  def aggregate(
      name: String,
      fn:    io.sm8.core.rel.AggregateFn,
      expr:  io.sm8.core.expr.Expr,
  ): Measure = Measure(name, AggregateCall(fn, Some(expr), name))
}

/**
 * A pre-defined filter clause on a Model is the typed
 * `FilterSpec(name, predicate: Expr)` defined in
 * `io.sm8.core.model.FilterSpec` (per the legacy semanticdf-core
 * design doc §4.4.1). The raw-SQL version (`expr: String`) that was
 * the legacy adapter Predicate is
 * the canonical one.
 */

/**
 * Serializable source identity. Separate from the engine-specific
 * resolver closure (e.g. Spark's `() => DataFrame`). The closure
 * lives behind a ProviderRef and is registered once at server
 * startup.
 */
sealed trait SourceRef extends Product with Serializable
object SourceRef {
  /** Source identified by catalog / namespace / table.
    * (the design contract): re-port the legacy semanticdf pre-tag shape
    * (Option[String] for catalog + namespace). The wired SM8 was
    * missing these 2 fields -- they were dropped as "smallest
    * correct change" but the data-engineer parity review flagged
    * this as a wire-breaking regression for any YAML/MCP payload
    * referencing the 3-part name. The smart constructors preserve
    * the 2-arg legacy 1.x / 2.x form.
    */
  final case class ByName(
    catalog:   Option[String] = None,
    namespace: Option[String] = None,
    table:     String,
  ) extends SourceRef

  /** Smart constructor: bare `(name, table)` 2-arg form preserved
    * for legacy callers (every existing test) and tests written
    * before the catalog/namespace fields landed. Maps to the legacy
    * semanticdf pre-tag shape (catalog=None, namespace=None). */
  def byName(name: String, table: String): ByName = ByName(
    catalog = None, namespace = None, table = table
  )

  /** Source identified by file path + format. */
  final case class ByPath(format: String, path: String, options: Map[String, String] = Map.empty) extends SourceRef
  /** ProviderRef -- name + driver-local closure. */
  final case class ByProvider(providerRefName: String) extends SourceRef
}

/** Reference to a registered provider closure (driver-local, never serialized). */
final case class ProviderRef(name: String)

/**
 * Smart constructor — runs ModelValidator.validate once at the
 * boundary per [[karpathy-guidelinesmindset]]. Returns
 * `Left(validationError)` on failure, `Right(model)` on success.
 */
import io.sm8.core.schema.SealedDataType

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
      filters: List[FilterSpec] = Nil,
      calculatedMeasures: List[CalculatedMeasure] = Nil,
      joins: List[JoinSpec] = Nil
  ): Either[ModelValidationError, Model] = {
    if (name == null || name.trim.isEmpty)
      Left(ModelValidationError.InvalidName("Model name must be non-blank"))
    else if (version < 0)
      Left(ModelValidationError.InvalidVersion(version))
    else
      // the current implementation: pure model-level
      // cross-reference validation (duplicate-name detection).
      // Schema-level validation lives in
      // `ModelValidator.validateAgainstSchema` (caller-side, after
      // SourceResolver.resolve).
      ModelValidator.validate(
        Model(
          name = name,
          version = version,
          description = description,
          dimensions = dimensions,
          measures = measures,
          defaultPolicies = defaultPolicies,
          source = source,
          status = status,
          filters = filters,
          calculatedMeasures = calculatedMeasures,
          joins = joins)
      ).right.map(_ => new Model(
        name = name,
        version = version,
        description = description,
        dimensions = dimensions,
        measures = measures,
        defaultPolicies = defaultPolicies,
        source = source,
        status = status,
        filters = filters,
        calculatedMeasures = calculatedMeasures,
        joins = joins))
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

  /** aggregated cross-reference validation errors. All
    * collected errors are surfaced at once (per the observable-cause principle
    * SS1) -- never silent partial-validation. */
  final case class SchemaValidation(messages: List[String]) extends ModelValidationError {
    val message = s"Model schema validation failed: ${messages.mkString("; ")}"
  }
}
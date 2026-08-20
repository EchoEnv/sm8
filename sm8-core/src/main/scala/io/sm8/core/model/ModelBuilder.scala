/*
 * SM8 Core — ModelBuilder.
 * Programmatic factory for the engine-portable `io.sm8.core.model.Model`.
 * Sits in core (sm8-core), not in an adapter or plugin. The unit
 * per RFC §3 Core Boundary: the code does not know which database /
 * cache / auth system is in use. It only builds the IR.
 * ==Why a builder (not just Model.of(...))==
 * `Model.of(...)` is the smart constructor: validates at the boundary,
 * returns `Either[ModelValidationError, Model]`. It works for the
 * common case but every test / dynamic model construction site has
 * to spell out every field. The legacy `io.semanticdf` library used
 * `SemanticTable.builder()` extensively; SM8 picks up that pattern
 * but with the sealed-trait dispatch (`SourceRef` is sealed) the
 * builder is fully type-checked at compile time.
 * ==Why in core (not in a plugin)==
 * Per RFC §3 (Core Boundary table):
 *   - Core never imports a specific adapter / plugin / hook.
 *   - Adapters know about a specific data source.
 *   - Plugins bundle adapters + hooks for one purpose.
 *   - Hooks know *when* in the pipeline.
 * ModelBuilder knows *how to construct a Model*. That's core. No
 * data-source knowledge, no pipeline knowledge.
 * ==Why immutable (case class)==
 * no shared mutable state. The builder is a `case class`; `with*`
 * methods return new instances. No `var`, no `clear()`, no static
 * carrier. Same pattern as Scala's stdlib `ListMap` / `Set`.
 * ==Why Either-based build==
 * `build` method returns `Either[ModelValidationError, Model]`
 * — never throws. Validation that requires multi-field reasoning
 * (e.g. "name + version must be unique across models") lands in
 * the future `ModelRegistry` layer; this builder validates the
 * per-call invariants only.
 * ==Spark concerns (per user directive)==
 * - mantras #1, #5: no Spark types captured, no executor-side
 *   closure. The builder is pure data.
 * - mantra #3 (schema-drift verify at boundary): the validation
 *   here is the boundary; field-level invariants are checked once,
 *   not by every consumer.
 * closure, no mutable state. The serialized Model is the
 * `final case class Model(...) extends Product with Serializable`
 * already in this file — case-class derived Serialization is the
 * contract.
 * factory, not a hot path. No per-call allocation concerns.
 * ==Plan alignment==
 * Per agile-kindling-beacon plan line 195: the `manifest/` IR
 * move (YAML loading) is a future PR. ModelBuilder is the
 * companion factory that YAML deserialization will eventually
 * call. This PR pre-empts that work — the builder is the
 * foundation.
 */
package io.sm8.core.model

/**
 * Programmatic factory for `Model`. Use `with*` methods to set
 * each field, then call `build` to produce the validated
 * `Either[ModelValidationError, Model]`.
 * The builder is a typed factory; no behavior, no Spark, no
 * pipeline knowledge.
 * the existing `Model.of(...)` smart constructor stays. The
 * builder is additive — it gives callers a fluent style for
 * the dynamic-construction use case (tests, programmatic
 * model generation) without forcing every field into a
 * positional arg list.
 */
final case class ModelBuilder private (
    name:           Option[String]            = None,
    version:        Option[Int]               = None,
    description:    Option[String]            = None,
    dimensions:     List[Dimension]           = Nil,
    measures:       List[Measure]             = Nil,
    defaultPolicies: ModelPolicyDefaults      = ModelPolicyDefaults(
      materialize = MaterializePolicy.None,
      cache       = CachePolicy.NoCache,
      audit       = AuditPolicy.NoAudit,
    ),
    source:         Option[SourceRef]         = None,
    status:         ModelStatus               = ModelStatus.Draft,
    filters:        List[FilterSpec]          = Nil,
    calculatedMeasures: List[CalculatedMeasure] = Nil,
    joins:          List[JoinSpec]            = Nil,
) {

  def withName(value: String): ModelBuilder =
    copy(name = Option(value))

  def withVersion(value: Int): ModelBuilder =
    copy(version = Option(value))

  def withDescription(value: String): ModelBuilder =
    copy(description = Option(value))

  def withDimension(name: String, expr: io.sm8.core.expr.Expr): ModelBuilder =
    copy(dimensions = dimensions :+ Dimension(name, expr))

  def withDimensions(values: List[Dimension]): ModelBuilder =
    copy(dimensions = values)

  /** `expr` is a typed `AggregateCall`.
    * Use `withMeasureAgg(name, fn, expr)` for the common
    * single-aggregate case, or this method with the structural
    * `AggregateCall(...)` for `COUNT(*)` /
    * `APPROX_PERCENTILE(x, p)` forms. */
  def withMeasure(name: String, expr: io.sm8.core.rel.AggregateCall): ModelBuilder =
    copy(measures = measures :+ Measure(name, expr))

  /** Smart constructor for the common single-aggregate case:
    * `withMeasureAgg("total", AggregateFn.Sum, Expr.FieldRef("amount"))`. */
  def withMeasureAgg(
      name: String,
      fn:    io.sm8.core.rel.AggregateFn,
      expr:  io.sm8.core.expr.Expr,
  ): ModelBuilder =
    copy(measures = measures :+ Measure.aggregate(name, fn, expr))

  def withMeasures(values: List[Measure]): ModelBuilder =
    copy(measures = values)

  /** Add a calculated (derived) measure — any `Expr`. */
  def withCalculatedMeasure(name: String, expr: io.sm8.core.expr.Expr): ModelBuilder =
    copy(calculatedMeasures = calculatedMeasures :+ CalculatedMeasure(name, expr))

  def withCalculatedMeasures(values: List[CalculatedMeasure]): ModelBuilder =
    copy(calculatedMeasures = values)

  /** Add a join to another model. */
  def withJoin(spec: JoinSpec): ModelBuilder =
    copy(joins = joins :+ spec)

  def withJoins(values: List[JoinSpec]): ModelBuilder =
    copy(joins = values)

  def withPolicies(value: ModelPolicyDefaults): ModelBuilder =
    copy(defaultPolicies = value)

  def withSource(value: SourceRef): ModelBuilder =
    copy(source = Option(value))

  def withStatus(value: ModelStatus): ModelBuilder =
    copy(status = value)

  def withFilter(value: FilterSpec): ModelBuilder =
    copy(filters = filters :+ value)

  def withFilters(values: List[FilterSpec]): ModelBuilder =
    copy(filters = values)

  /**
   * Materialize the validated `Either[ModelValidationError, Model]`.
   * returns an `Either`, never throws. The `Model.of(...)` smart
   * constructor (called under the hood) holds the same contract.
   * the smoke test asserts that `ModelBuilder().withName(...).build`
   * equals `Model.of(...)` for the same input — round-trip proof.
   */
  def build: Either[ModelValidationError, Model] = {
    val n: String = name.getOrElse("")
    val v: Int    = version.getOrElse(-1)
    val s: SourceRef = source.getOrElse(
      SourceRef.ByName(table = "unknown")
    )
    Model.of(
      name            = n,
      version         = v,
      description     = description,
      dimensions      = dimensions,
      measures        = measures,
      defaultPolicies = defaultPolicies,
      source          = s,
      status          = status,
      filters         = filters,
      calculatedMeasures = calculatedMeasures,
      joins           = joins,
    )
  }
}

object ModelBuilder {

  /**
   * Empty builder. All fields default; `build` will return `Left(...)`
   * until `withName` and `withVersion` are set.
   */
  def apply(): ModelBuilder = new ModelBuilder()
}

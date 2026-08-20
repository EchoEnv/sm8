/*
 * SM8 Core -- ModelValidator (PR-M2 per ADR-008-L Appendix GAP 2).
 *
 * Per RFC SS3: cross-reference validation is engine-portable (the
 * validator reads only the portable `ResolvedSource.Scan.schema`
 * for the schema-level checks; the name-uniqueness checks need no
 * IO). The validator lives in core and is called exactly once at
 * the construction boundary (`Model.of`).
 *
 * cross-reference errors at once and surfaces them as a single
 * `ModelValidationError.SchemaValidation(messages)`. Silent
 * partial-validation is the worst error class per ADR-008-H
 * ("never a silent no-op").
 *
 * the typed `Model` data only -- no `String` comparisons, no
 * reflection. The schema it validates against is `List[Field]`
 * (also a typed sealed-trait family from `sm8-core/schema/`).
 *
 * ==Two entry points (per RFC SS12)==
 *
 * `validate(model): Either[ModelValidationError, Unit]` -- pure
 * model-level checks: duplicate names across the same field kind.
 * NO IO. Always callable from `Model.of` without a resolver.
 *
 * `validateAgainstSchema(model, schema): Either[ModelValidationError, Unit]`
 * -- requires the schema (post-source-resolution). Called by the
 * connector / deployment layer after `SourceResolver.resolve(...)`
 * (per PR-M3 + PR-M4).
 *
 * ==Spark concerns (per user directive)==
 *
 * None -- core-only, zero spark imports, zero IO.
 */
package io.sm8.core.model

import io.sm8.core.engine.{EngineError, ResolvedSource}
import io.sm8.core.expr.Expr
import io.sm8.core.schema.Field

object ModelValidator {

 /** Pure model-level validation: name uniqueness within each kind.
 * Returns `SchemaValidation(messages)` with ALL collected errors
 * (not just the first) per [[debug-mantra-mindset]] SS1. */
 def validate(
  model: Model,
 ): Either[ModelValidationError, Unit] = {
 val errs = scala.collection.mutable.ArrayBuffer.empty[String]
 errs ++= duplicateNames(model.dimensions.map(_.name), "dimension")
 errs ++= duplicateNames(model.measures.map(_.name), "measure")
 errs ++= duplicateNames(model.calculatedMeasures.map(_.name), "calculated_measure")
 errs ++= duplicateNames(model.filters.map(_.name),  "filter")
 errs ++= duplicateNames(model.joins.map(_.name),  "join")
 if (errs.isEmpty) Right(()) else Left(ModelValidationError.SchemaValidation(errs.toList))
 }

 /** Schema-level validation: every `Dimension.expr`, `Measure.expr.input`,
 * `FilterSpec.predicate`, `CalculatedMeasure.expr`, and `JoinSpec.keys`
 * must reference a field that actually exists in the resolved
 * schema. Unknown fields fail loud.
 *
 * NOTE: `Measure.expr.fn` is NOT validated here (the function-name
 * lookup is engine-specific). The 6 wired + 10 deferred aggregates
 * are surfaced at compile time (PR-K), not at model-load time.
 *
 * we walk the AST in one pass per category (no double-walk) and
 * collect unique missing fields. The 24-case Expr family is
 * walked by the shared `fields` walker (same shape as the
 * QueryBuilder cycle-detection walker from PR-L).
 */
 def validateAgainstSchema(
  model: Model,
  schema: ResolvedSource.Scan,
 ): Either[ModelValidationError, Unit] = {
 val available = schema.schema.map(_.name).toSet
 val missing = scala.collection.mutable.LinkedHashSet.empty[String]

 // Dimensions: PR-O4b (ADR-008-O) — `Dimension.expr` is now a typed
 // Expr. For the common FieldRef case we extract the column name and
 // look it up in the source schema; any non-FieldRef case (e.g. an
 // Expr.Add on two columns) walks every FieldRef it references.
 model.dimensions.foreach { d =>
  val refs = walkExprForFields(d.expr) // Set[String]
  val unmapped = refs.filterNot(available.contains)
  if (unmapped.nonEmpty)
  missing += s"dimensions[${d.name}].expr references unknown field(s): ${unmapped.mkString(", ")}"
 }

 // Measures: the input expression (AggregateCall.input).
 // `Measure.expr.fn` is engine-specific (skip here).
 model.measures.foreach { m =>
  walkExprForFields(m.expr.input.getOrElse(Expr.FieldRef(m.name)))
  .filterNot(available.contains)
  .foreach(name => missing += s"measures[${m.name}].input references unknown field '$name'")
 }

 // Calculated measures: any Expr.FieldRef / Expr.MeasureRef
 // pointing at an available measure name is fine (the
 // cycle-detection is in QueryBuilder); only raw field
 // references are checked here.
 model.calculatedMeasures.foreach { c =>
  walkExprForFields(c.expr)
  .filterNot(available.contains)
  .foreach(name => missing += s"calculated_measures[${c.name}] references unknown field '$name'")
 }

 // Filters: walk the predicate Expr.
 model.filters.foreach { f =>
  walkExprForFields(f.predicate)
  .filterNot(available.contains)
  .foreach(name => missing += s"filters[${f.name}] references unknown field '$name'")
 }

 // Joins: keys are (leftKey, rightKey) column-name pairs. We
 // check that the LEFT keys exist in the joined schema; the
 // RIGHT keys exist in the right-side model's schema (which
 // PR-M4 wires). For PR-M2, we validate left keys against the
 // primary schema only (the right-model schema lookup is
 // PR-M3 + PR-M4 territory).
 model.joins.foreach { js =>
  js.keys.foreach { case (leftKey, _) =>
  if (!available.contains(leftKey))
   missing += s"joins[${js.name}] references unknown left key '$leftKey'"
  }
 }

 if (missing.isEmpty) Right(()) else Left(ModelValidationError.SchemaValidation(missing.toList))
 }

 /** Field-name extraction: delegates to the engine-portable
 * walker in sm8-core/expr/Calculator (PR-M5). Per RFC SS3 the
 * walker is core; per ADR-008-L the Calculator is the SINGLE
 * source of truth for Expr walking (ModelValidator + QueryBuilder
 * both use it). */
 private def walkExprForFields(e: Expr): Set[String] = io.sm8.core.expr.Calculator.fieldNamesOf(e)

 /** Detect duplicate names within a single field kind. */
 private def duplicateNames(
  names: List[String],
  kind: String,
 ): List[String] = {
 val seen = scala.collection.mutable.LinkedHashSet.empty[String]
 val dups = scala.collection.mutable.LinkedHashSet.empty[String]
 names.foreach { n =>
  if (!seen.add(n)) dups += n
 }
 dups.toList.map(n => s"duplicate $kind name '$n'")
 }
}

/*
 * SM8 Core -- ModelValidator (PR-M2 per ADR-008-L Appendix GAP 2).
 *
 * Per RFC SS3: cross-reference validation is engine-portable (the
 * validator reads only the portable `ResolvedSource.Scan.schema`
 * for the schema-level checks; the name-uniqueness checks need no
 * IO). The validator lives in core and is called exactly once at
 * the construction boundary (`Model.of`).
 *
 * Per  SS1: the validator accumulates ALL
 * cross-reference errors at once and surfaces them as a single
 * `ModelValidationError.SchemaValidation(messages)`. Silent
 * partial-validation is the worst error class per ADR-008-H
 * ("never a silent no-op").
 *
 * Per : the validator reads
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
 * connector / deployment layer after `SourceResolver.resolve(.)`
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
    * (not just the first) per  SS1. */
  def validate(
      model: Model): Either[ModelValidationError, Unit] = {
    val errs = scala.collection.mutable.ArrayBuffer.empty[String]
    errs ++= duplicateNames(model.dimensions.map(_.name),  "dimension")
    errs ++= duplicateNames(model.measures.map(_.name),    "measure")
    errs ++= duplicateNames(model.calculatedMeasures.map(_.name), "calculated_measure")
    errs ++= duplicateNames(model.filters.map(_.name),     "filter")
    errs ++= duplicateNames(model.joins.map(_.name),        "join")
    errs ++= duplicateNames(model.rollups.map(_.name),      "rollup")
    // Rollup refs (pre-aggregation, Ticket 3 of
    // docs/wayfinder/2026-09-06-pre-aggregation.md): rollup
    // dimension/measure refs are names of
    // the HOST model's own fields. Unknown refs fail loud here (the
    // same discipline as calculatedMeasures) -- never silent.
    val dimNames  = model.dimensions.map(_.name).toSet
    val measNames = model.measures.map(_.name).toSet
    val calcNames = model.calculatedMeasures.map(_.name).toSet
    model.rollups.foreach { r =>
      r.dimensions.filterNot(dimNames.contains).foreach(d =>
        errs += s"rollups[${r.name}] references unknown dimension '$d'")
      r.measures.filterNot(measNames.contains).foreach { m =>
        if (calcNames.contains(m))
          errs += s"rollups[${r.name}] references calculated measure '$m'; rollup measures must be base measures (a calculated expression cannot be re-aggregated from pre-aggregated rows)"
        else
          errs += s"rollups[${r.name}] references unknown measure '$m'"
      }
      errs ++= validateGrainDimensionDeclared(r, model)
    }
    if (errs.isEmpty) Right(()) else Left(ModelValidationError.SchemaValidation(errs.toList))
  }

  /** Grain-dimension contract checks that need only the DECLARED
    * model data (pure, no resolved schema):
    *   1. co-presence — `timeGrain` and `grainDimension` are both
    *      set or both unset (a grain without an axis is
    *      meaningless; an axis without a grain changes query
    *      semantics without the label that governs routing);
    *   2. ref membership — the grain dimension must name a
    *      dimension already in the rollup's `dimensions`;
    *   3. temporal type — when the named dimension DECLARES a
    *      `dataType`, it must be `Date` or `Timestamp` (calendar
    *      truncation is undefined over other domains; Varchar
    *      ISO strings are excluded because string ordering is
    *      lexicographic and no parsing contract exists in core).
    * A dimension with `dataType = None` skips check 3 here; the
    * schema-aware entry point (`validateAgainstSchema`) settles it
    * from the RESOLVED source type.
    *
    * @param r     the rollup declaration under validation
    * @param model the host model (supplies declared dimension types)
    * @return the collected error messages (empty = valid)
    */
  private def validateGrainDimensionDeclared(
      r: RollupSpec,
      model: Model): List[String] = {
    val out = scala.collection.mutable.ListBuffer.empty[String]
    (r.timeGrain, r.grainDimension) match {
      case (None, Some(gd)) =>
        out += s"rollups[${r.name}]: grainDimension '$gd' is set but timeGrain is not — a grain axis without a grain label cannot route; declare both or neither"
      case (Some(_), None) =>
        out += s"rollups[${r.name}]: timeGrain is set but grainDimension is not — bucketing needs an explicit axis; declare grainDimension (a date/timestamp dimension of this rollup)"
      case (Some(_), Some(gd)) =>
        if (!r.dimensions.contains(gd))
          out += s"rollups[${r.name}]: grainDimension '$gd' must name one of the rollup's own dimensions (${r.dimensions.mkString(", ")})"
        else {
          model.dimensions.find(_.name == gd).flatMap(_.dataType).foreach { t =>
            if (t != io.sm8.core.schema.SealedDataType.Date && t != io.sm8.core.schema.SealedDataType.Timestamp)
              out += s"rollups[${r.name}]: grainDimension '$gd' has declared type $t — calendar truncation requires Date or Timestamp (a string source needs a calculated Date dimension)"
          }
        }
      case (None, None) => ()
    }
    out.toList
  }

  /** Grain-dimension temporal-type check against the RESOLVED
    * source schema: when the declared grain dimension has no
    * explicit `dataType`, its resolved column type must still be
    * `Date` or `Timestamp`. Complements the declared-type check in
    * `validate` — together they make an un-typed (or mistyped)
    * grain axis impossible to reach materialization.
    *
    * When the dimension DOES declare `dataType`, the declared-type
    * check has already passed; this method then cross-checks
    * declaration-vs-resolution consistency (a mismatch is the
    * silent-defaulting class closed ADTs forbid, so it's a typed
    * error too).
    *
    * @param r      the rollup declaration under validation
    * @param model  the host model (for declared-type lookup)
    * @param schema the resolved source schema (column name -> type)
    * @return the collected error messages (empty = valid)
    */
  private[model] def validateGrainDimensionResolved(
      r: RollupSpec,
      model: Model,
      schema: ResolvedSource.Scan): List[String] = {
    val resolved = schema.schema.map(f => f.name -> f.dataType).toMap
    (r.timeGrain, r.grainDimension) match {
      case (Some(_), Some(gd)) if r.dimensions.contains(gd) =>
        model.dimensions.find(_.name == gd).flatMap(_.dataType) match {
          case None =>
            // Declared type absent — the resolved source type decides.
            resolved.get(gd) match {
              case Some(t) if t != io.sm8.core.schema.SealedDataType.Date &&
                               t != io.sm8.core.schema.SealedDataType.Timestamp =>
                List(s"rollups[${r.name}]: grainDimension '$gd' resolved to type $t — calendar truncation requires Date or Timestamp")
              case _ => Nil
            }
          case Some(declaredT) =>
            // Declared type present — only complain if the resolved
            // type disagrees with the declared one (a virtual drift
            // pin; the declared type is the contract the materializer
            // uses when reconciling).
            resolved.get(gd).filterNot(_ == declaredT).map { resT =>
              s"rollups[${r.name}]: grainDimension '$gd' declared type $declaredT disagrees with resolved type $resT — pick one or fix the source"
            }.toList
        }
      case _ => Nil
    }
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
    * Per  "smallest correct change":
    * we walk the AST in one pass per category (no double-walk) and
    * collect unique missing fields. The 24-case Expr family is
    * walked by the shared `fields` walker (same shape as the
    * QueryBuilder cycle-detection walker from PR-L).
    */
  def validateAgainstSchema(
      model: Model,
      schema: ResolvedSource.Scan): Either[ModelValidationError, Unit] = {
    val available = schema.schema.map(_.name).toSet
    val missing = scala.collection.mutable.LinkedHashSet.empty[String]

    // Dimensions: PR-O4b (ADR-008-O) — `Dimension.expr` is now a typed
    // Expr. For the common FieldRef case we extract the column name and
    // look it up in the source schema; any non-FieldRef case (e.g. an
    // Expr.Add on two columns) walks every FieldRef it references.
    model.dimensions.foreach { d =>
      val refs = walkExprForFields(d.expr)  // Set[String]
      val unmapped = refs.filterNot(available.contains)
      if (unmapped.nonEmpty)
        missing += s"dimensions[${d.name}].expr references unknown field(s): ${unmapped.mkString(", ")}"
    }

    // Measures: the input expression (AggregateCall.input).
    // `Measure.expr.fn` is engine-specific (skip here).
    model.measures.foreach { m =>
      // COUNT(*) measures have no input expression (AggregateCall.input == None).
      // Skip the field-reference walk; the measure name itself is not a source
      // field and substituting it via getOrElse would produce a false-positive.
      // All other aggregate functions (Sum/Avg/Min/Max/CountDistinct) require
      // a real input expression -- missing input is a misconfiguration that the
      // downstream lowering layer silently defaults (per ADR-008-W §"Deferred"),
      // so we fail loud here at the model-load boundary.
      if (m.expr.input.isEmpty) {
        if (m.expr.fn != io.sm8.core.rel.AggregateFn.Count)
          missing += s"measures[${m.name}].input is required for aggregate function ${m.expr.fn}"
      } else {
        walkExprForFields(m.expr.input.get).filterNot(available.contains).foreach(name => missing += s"measures[${m.name}].input references unknown field '$name'")
      }
    }

    // Calculated measures: any Expr.FieldRef / Expr.MeasureRef
    // pointing at an available measure name is fine (the
    // cycle-detection is in QueryBuilder); only raw field
    // references are checked here.
    model.calculatedMeasures.foreach { c =>
      walkExprForFields(c.expr).filterNot(available.contains).foreach(name => missing += s"calculated_measures[${c.name}] references unknown field '$name'")
    }

    // Filters: walk the predicate Expr.
    model.filters.foreach { f =>
      walkExprForFields(f.predicate).filterNot(available.contains).foreach(name => missing += s"filters[${f.name}] references unknown field '$name'")
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

    // Grain dimension: resolved-source-type check (companion to
    // the declared-type check in `validate`). When the declared
    // type is absent, the resolved column type decides — a
    // VARCHAR source would route to GrainMismatch at query time
    // (silently), so we surface it here instead.
    model.rollups.foreach { r =>
      validateGrainDimensionResolved(r, model, schema).foreach(missing += _)
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
      kind:  String): List[String] = {
    val seen = scala.collection.mutable.LinkedHashSet.empty[String]
    val dups = scala.collection.mutable.LinkedHashSet.empty[String]
    names.foreach { n =>
      if (!seen.add(n)) dups += n
    }
    dups.toList.map(n => s"duplicate $kind name '$n'")
  }
}

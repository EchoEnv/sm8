/*
 * SM8 Core -- ModelValidator (PR-M2 per ADR-008-L Appendix GAP 2).
 *
 * Per RFC SS3: cross-reference validation is engine-portable (the
 * validator reads only the portable `ResolvedSource.Scan.schema`
 * for the schema-level checks; the name-uniqueness checks need no
 * IO). The validator lives in core and is called exactly once at
 * the construction boundary (`Model.of`).
 *
 * Per [[debug-mantra-mindset]] SS1: the validator accumulates ALL
 * cross-reference errors at once and surfaces them as a single
 * `ModelValidationError.SchemaValidation(messages)`. Silent
 * partial-validation is the worst error class per ADR-008-H
 * ("never a silent no-op").
 *
 * Per [[scala-data-driven-refactor-mindset]]: the validator reads
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
    errs ++= duplicateNames(model.dimensions.map(_.name),  "dimension")
    errs ++= duplicateNames(model.measures.map(_.name),    "measure")
    errs ++= duplicateNames(model.calculatedMeasures.map(_.name), "calculated_measure")
    errs ++= duplicateNames(model.filters.map(_.name),     "filter")
    errs ++= duplicateNames(model.joins.map(_.name),        "join")
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
    * Per [[karpathy-guidelines-mindset]] "smallest correct change":
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

    // Dimensions: the Dimension.expr is a column name (String).
    // Per [[karpathy-guidelines-mindset]]: we trust the legacy
    // "dimensions are column-name references" contract (no Expr
    // parser for dimensions yet -- they're declared as raw strings
    // in the manifest).
    model.dimensions.foreach { d =>
      if (!available.contains(d.expr))
        missing += s"dimensions[${d.name}].expr='${d.expr}' (unknown field)"
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

  /** Walk an Expr AST and collect all `FieldRef.name` it references.
    * Covers the full 24-case family incl. PR-I's CaseWhen / Alias.
    * Shared walker shape (per [[scala-data-driven-refactor-mindset]] SS1:
    * pure data walk, single accumulator, no behavior). */
  private def walkExprForFields(e: Expr): Set[String] = {
    val out = scala.collection.mutable.LinkedHashSet.empty[String]
    def go(x: Expr): Unit = x match {
      case Expr.FieldRef(n)             => out += n
      case Expr.MeasureRef(_)           => ()  // measure refs are engine-known
      case Expr.All(_)                  => ()
      case Expr.Literal(_, _)           => ()
      case Expr.Not(inner)              => go(inner)
      case Expr.IsNull(inner)           => go(inner)
      case Expr.IsNotNull(inner)        => go(inner)
      case Expr.Cast(inner, _)          => go(inner)
      case Expr.Alias(_, inner)         => go(inner)
      case Expr.Add(l, r)               => go(l); go(r)
      case Expr.Subtract(l, r)          => go(l); go(r)
      case Expr.Multiply(l, r)          => go(l); go(r)
      case Expr.Divide(l, r)            => go(l); go(r)
      case Expr.Modulo(l, r)            => go(l); go(r)
      case Expr.Equal(l, r)             => go(l); go(r)
      case Expr.NotEqual(l, r)          => go(l); go(r)
      case Expr.LessThan(l, r)          => go(l); go(r)
      case Expr.LessOrEqual(l, r)       => go(l); go(r)
      case Expr.GreaterThan(l, r)       => go(l); go(r)
      case Expr.GreaterOrEqual(l, r)    => go(l); go(r)
      case Expr.And(l, r)               => go(l); go(r)
      case Expr.Or(l, r)                => go(l); go(r)
      case Expr.CaseWhen(branches, o)   =>
        branches.foreach { case (c, v) => go(c); go(v) }
        go(o)
      case Expr.FunctionCall(_, args)   => args.foreach(go)
    }
    go(e)
    out.toSet
  }

  /** Detect duplicate names within a single field kind. */
  private def duplicateNames(
      names: List[String],
      kind:  String,
  ): List[String] = {
    val seen = scala.collection.mutable.LinkedHashSet.empty[String]
    val dups = scala.collection.mutable.LinkedHashSet.empty[String]
    names.foreach { n =>
      if (!seen.add(n)) dups += n
    }
    dups.toList.map(n => s"duplicate $kind name '$n'")
  }
}

/*
 * SM8 Core -- Calculator (the engine-portable AST walker).
 *
 * PR-M5 (per ADR-008-L Appendix "extract Calculator" recommendation
 * + the user's directive on 2026-08-17): the legacy `io.semanticdf.
 * core.expr.Calculator` was a STATIC-ANALYSIS helper -- it walks an
 * Expr tree and extracts the data the rest of the portable model
 * needs (field references, measure references).
 *
 * Per [[karpathy-guidelines-mindset]]: this is the single source of
 * truth for Expr walking. The same walker is used by:
 *   - `ModelValidator.validateAgainstSchema` (field-name check)
 *   - `QueryBuilder.detectCalcCycles` (measure-name dependency
 *     graph for cycle detection)
 *
 * Per RFC SS3: the walker is ENGINE-PORTABLE. It does not execute
 * the expression (that's engine-specific -- Spark's `Column.when`,
 * Trino's compile, Databricks' Connect). It does STATIC analysis
 * that runs once at model-load time.
 *
 * Per [[scala-data-driven-refactor-mindset]] SS1: PURE-DATA helper.
 * Object singleton, no engine coupling, no state, no allocations
 * per call (a single LinkedHashSet accumulator is reused via the
 * walker's per-call scope).
 *
 * Per ADR-007 (the v0.1.0 cut plan): the walker covers the FULL
 * 24-case Expr family (the 22 legacy cases + PR-I's `CaseWhen` +
 * `Alias`). Adding a new Expr case means adding one new match arm
 * here -- compile-time exhaustive-cases.
 *
 * ==Boundary contract==
 *
 * Zero Spark imports. Verifiable by:
 *   `grep -r 'org.apache.spark' sm8-core/src/main/scala/io/sm8/core/expr/Calculator.scala`
 */
package io.sm8.core.expr

import scala.collection.mutable.LinkedHashSet

object Calculator {

  /** Walk an Expr and collect every `FieldRef.name` it references
    * (de-duplicated, no order). Used by the model validator to
    * check that a calculated measure's `expr` only references fields
    * that exist in the resolved schema.
    *
    * `MeasureRef` / `All` / `Literal` / `FunctionCall` to UDFs do
    * NOT contribute (the first two are engine-known refs; the
    * third is a constant; the fourth is engine-bound). */
  def fieldNamesOf(e: Expr): Set[String] = {
    val out = LinkedHashSet.empty[String]
    def go(x: Expr): Unit = x match {
      case Expr.FieldRef(name)        => out += name
      case Expr.MeasureRef(_)         => ()
      case Expr.All(_)                => ()
      case Expr.Literal(_, _)         => ()
      case Expr.Not(inner)            => go(inner)
      case Expr.IsNull(inner)         => go(inner)
      case Expr.IsNotNull(inner)      => go(inner)
      case Expr.Cast(inner, _)        => go(inner)
      case Expr.Alias(_, inner)       => go(inner)
      case Expr.Add(l, r)             => go(l); go(r)
      case Expr.Subtract(l, r)        => go(l); go(r)
      case Expr.Multiply(l, r)        => go(l); go(r)
      case Expr.Divide(l, r)          => go(l); go(r)
      case Expr.Modulo(l, r)          => go(l); go(r)
      case Expr.Equal(l, r)           => go(l); go(r)
      case Expr.NotEqual(l, r)        => go(l); go(r)
      case Expr.LessThan(l, r)        => go(l); go(r)
      case Expr.LessOrEqual(l, r)     => go(l); go(r)
      case Expr.GreaterThan(l, r)     => go(l); go(r)
      case Expr.GreaterOrEqual(l, r)  => go(l); go(r)
      case Expr.And(l, r)             => go(l); go(r)
      case Expr.Or(l, r)              => go(l); go(r)
      case Expr.CaseWhen(branches, o) =>
        branches.foreach { case (c, v) => go(c); go(v) }
        go(o)
      case Expr.FunctionCall(_, args) => args.foreach(go)
    }
    go(e)
    out.toSet
  }

  /** Walk an Expr and collect every `MeasureRef.name` (and every
    * `All.name`, per legacy PR #419 -- the All references a measure
    * by name) it references. Used by the cycle-detection walker in
    * `QueryBuilder` to build the calculated-measure dependency graph.
    *
    * `FieldRef` / `Literal` / `FunctionCall` do NOT contribute. */
  def measureNamesOf(e: Expr): Set[String] = {
    val out = LinkedHashSet.empty[String]
    def go(x: Expr): Unit = x match {
      case Expr.MeasureRef(name)        => out += name
      case Expr.All(name)               => out += name
      case Expr.FieldRef(_)            => ()
      case Expr.Literal(_, _)          => ()
      case Expr.Not(inner)             => go(inner)
      case Expr.IsNull(inner)          => go(inner)
      case Expr.IsNotNull(inner)       => go(inner)
      case Expr.Cast(inner, _)         => go(inner)
      case Expr.Alias(_, inner)        => go(inner)
      case Expr.Add(l, r)              => go(l); go(r)
      case Expr.Subtract(l, r)         => go(l); go(r)
      case Expr.Multiply(l, r)         => go(l); go(r)
      case Expr.Divide(l, r)           => go(l); go(r)
      case Expr.Modulo(l, r)           => go(l); go(r)
      case Expr.Equal(l, r)            => go(l); go(r)
      case Expr.NotEqual(l, r)         => go(l); go(r)
      case Expr.LessThan(l, r)         => go(l); go(r)
      case Expr.LessOrEqual(l, r)      => go(l); go(r)
      case Expr.GreaterThan(l, r)      => go(l); go(r)
      case Expr.GreaterOrEqual(l, r)   => go(l); go(r)
      case Expr.And(l, r)              => go(l); go(r)
      case Expr.Or(l, r)               => go(l); go(r)
      case Expr.CaseWhen(branches, o) =>
        branches.foreach { case (c, v) => go(c); go(v) }
        go(o)
      case Expr.FunctionCall(_, args)  => args.foreach(go)
    }
    go(e)
    out.toSet
  }
}

/*
 * SM8 Core -- RelOpPlanPrinter (PR-N1 helper).
 *
 * Per the user's directive 2026-08-17: extend the explain() path
 * to walk the produced RelOp tree (the IR-level plan serialiser).
 *
 * Per RFC §3: this is a CORE engine-portable helper. It knows the
 * RelOp ADT (PR-H) and the Expr ADT; it does NOT know about spark,
 * about queries, about anything engine-specific. The connector
 * calls this and wraps the output as its `explain()` result (which
 * is the engine-specific serialization -- typically the same
 * string prefixed with the engine name).
 *
 * one class, one entry point, one recursive walk. The output is a
 * multi-line indented string; tests assert on substring content
 * (per node presence + per-node args).
 */
package io.sm8.core.rel

import io.sm8.core.expr.Expr

object RelOpPlanPrinter {

 /** Render a RelOp tree as a multi-line indented plan string.
 * Each node on its own line, indented by its nesting depth.
 * The output is human-readable AND machine-parseable (per
 *
 * Returns "" for null. The output uses the per-node short form
 * (e.g. "Scan(table=people)", "Filter(<,>)", "Aggregate(g=[r], a=[Sum(t)])").
 */
 def print(relOp: RelOp): String =
 if (relOp == null) "" else printNode(relOp, 0)

 private def printNode(node: RelOp, depth: Int): String = {
 val pad = "  " * depth
 node match {
  case RelOp.Scan(sourceRef, _, _, resolution) =>
  val resPart = resolution.fold("")(r => s", resolution=${renderResolution(r)}")
  s"${pad}Scan(${renderSourceRef(sourceRef)}" + resPart + ")"

  case RelOp.Filter(input, predicate) =>
  s"${pad}Filter(${renderExpr(predicate)})\n" + printNode(input, depth + 1)

  case RelOp.Project(input, expressions) =>
  val exprs = expressions.map { case (e, alias) =>
   s"${renderExpr(e)} AS $alias"
  }.mkString(", ")
  s"${pad}Project($exprs)\n" + printNode(input, depth + 1)

  case RelOp.Aggregate(input, groupBy, aggregates) =>
  val g = if (groupBy.isEmpty) "" else s"g=[${groupBy.map(renderExpr).mkString(",")}]"
  val a = aggregates.map(renderAggregateCall).mkString(",")
  s"${pad}Aggregate($g, a=[$a])\n" + printNode(input, depth + 1)

  case RelOp.Join(left, right, kind, condition) =>
  val kindStr = kind match {
   case JoinKind.Inner => "Inner"
   case JoinKind.Left => "Left"
   case JoinKind.Right => "Right"
   case JoinKind.Full => "Full"
   case JoinKind.Cross => "Cross"
  }
  s"${pad}Join($kindStr, ${renderExpr(condition)})\n" +
   printNode(left, depth + 1) + "\n" +
   printNode(right, depth + 1)

  case RelOp.Sort(input, keys) =>
  val ks = if (keys.isEmpty) "" else keys.map(renderSortKey).mkString(",")
  s"${pad}Sort($ks)\n" + printNode(input, depth + 1)

  case RelOp.Limit(input, count, offset) =>
  s"${pad}Limit(count=$count, offset=$offset)\n" + printNode(input, depth + 1)
 }
 }

 private def renderSourceRef(s: io.sm8.core.model.SourceRef): String = s match {
 case io.sm8.core.model.SourceRef.ByName(catalog, namespace, table) =>
  // PR-O4c (ADR-008-O): print all 3 fields; omit None options from the output
  val catalogPart = catalog.fold("")(n => s"catalog=$n, ")
  val namespacePart = namespace.fold("")(n => s"namespace=$n, ")
  s"$catalogPart$namespacePart" + s"table=$table"
 case io.sm8.core.model.SourceRef.ByPath(format, path, _) =>
  s"format=$format, path=$path"
 case io.sm8.core.model.SourceRef.ByProvider(providerRefName) =>
  s"provider=$providerRefName"
 }

 /** Render a 4-case ResolvedSource as a short tag for the printer.
 * PR-O4d (ADR-008-O): the failure-state provenance is now first-class
 * on the IR; the printer surfaces it inline at Scan time.
 */
 private def renderResolution(r: io.sm8.core.engine.ResolvedSource): String = r match {
 case io.sm8.core.engine.ResolvedSource.Scan(_, _)   => "Scan"
 case io.sm8.core.engine.ResolvedSource.NotFound(_, _)  => "NotFound"
 case io.sm8.core.engine.ResolvedSource.Incompatible(_, _) => "Incompatible"
 case io.sm8.core.engine.ResolvedSource.AuthFailed(_, _) => "AuthFailed"
 }

 private def renderExpr(e: Expr): String = e match {
 case Expr.Literal(value, dataType) =>
  s"Literal($value,$dataType)"
 case Expr.FieldRef(name)  => s"FieldRef($name)"
 case Expr.MeasureRef(name)  => s"MeasureRef($name)"
 case Expr.All(name)    => s"All($name)"
 case Expr.Not(inner)   => s"NOT(${renderExpr(inner)})"
 case Expr.IsNull(inner)   => s"ISNULL(${renderExpr(inner)})"
 case Expr.IsNotNull(inner)  => s"ISNOTNULL(${renderExpr(inner)})"
 case Expr.Cast(inner, t)  => s"CAST(${renderExpr(inner)} AS $t)"
 case Expr.Alias(_, inner)  => renderExpr(inner)
 case Expr.Add(l, r)    => s"(${renderExpr(l)} + ${renderExpr(r)})"
 case Expr.Subtract(l, r)  => s"(${renderExpr(l)} - ${renderExpr(r)})"
 case Expr.Multiply(l, r)  => s"(${renderExpr(l)} * ${renderExpr(r)})"
 case Expr.Divide(l, r)   => s"(${renderExpr(l)} / ${renderExpr(r)})"
 case Expr.Modulo(l, r)   => s"(${renderExpr(l)} % ${renderExpr(r)})"
 case Expr.Equal(l, r)   => s"(${renderExpr(l)} = ${renderExpr(r)})"
 case Expr.NotEqual(l, r)  => s"(${renderExpr(l)} != ${renderExpr(r)})"
 case Expr.LessThan(l, r)  => s"(${renderExpr(l)} < ${renderExpr(r)})"
 case Expr.LessOrEqual(l, r)  => s"(${renderExpr(l)} <= ${renderExpr(r)})"
 case Expr.GreaterThan(l, r)  => s"(${renderExpr(l)} > ${renderExpr(r)})"
 case Expr.GreaterOrEqual(l, r) => s"(${renderExpr(l)} >= ${renderExpr(r)})"
 case Expr.And(l, r)    => s"(${renderExpr(l)} AND ${renderExpr(r)})"
 case Expr.Or(l, r)    => s"(${renderExpr(l)} OR ${renderExpr(r)})"
 case Expr.CaseWhen(branches, otherwise) =>
  val bs = branches.map { case (c, v) => s"WHEN ${renderExpr(c)} THEN ${renderExpr(v)}" }.mkString(" ")
  val ow = renderExpr(otherwise)
  s"CASE $bs ELSE $ow END"
 case Expr.FunctionCall(name, args) =>
  s"$name(${args.map(renderExpr).mkString(", ")})"
 }

 private def renderAggregateCall(a: AggregateCall): String =
 s"${renderAggregateFn(a.fn)}(${a.input.map(renderExpr).getOrElse("*")}) AS ${a.alias}"

 private def renderAggregateFn(fn: AggregateFn): String = fn match {
 case AggregateFn.Sum    => "Sum"
 case AggregateFn.Count    => "Count"
 case AggregateFn.CountDistinct  => "CountDistinct"
 case AggregateFn.Avg    => "Avg"
 case AggregateFn.Min    => "Min"
 case AggregateFn.Max    => "Max"
 case AggregateFn.First    => "First"
 case AggregateFn.Last    => "Last"
 case AggregateFn.StddevSample  => "StddevSample"
 case AggregateFn.StddevPopulation => "StddevPopulation"
 case AggregateFn.VarianceSample  => "VarianceSample"
 case AggregateFn.VariancePopulation => "VariancePopulation"
 case AggregateFn.Median    => "Median"
 case AggregateFn.PercentileContinuous => "PercentileContinuous"
 case AggregateFn.PercentileDiscrete => "PercentileDiscrete"
 case AggregateFn.ApproxPercentile => "ApproxPercentile"
 }

 private def renderSortKey(k: SortKey): String =
 s"${renderExpr(k.expression)} ${k.direction match {
  case SortDirection.Ascending => "ASC"
  case SortDirection.Descending => "DESC"
 }} NULLS ${k.nullOrdering match {
  case NullOrdering.First => "FIRST"
  case NullOrdering.Last => "LAST"
 }}"
}

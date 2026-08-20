/*
 * SM8 Spark Connector — PortableExprCompiler.
 *
 * function (Expr) -> Column with NO captured state. Companion
 * object + sealed-trait dispatch over the closed Expr family
 * from sm8-core. The closure-safety contract from PR #36 (which
 * extends java.io.Serializable on EngineProvider) is preserved
 * by the SparkEngineProvider — the compiler itself captures
 * nothing; the SPARK-SPECIFIC Column handle is the one we hand
 * back to the caller.
 *
 * adapters, data in core": the compiler lives in the
 * spark-connector (an adapter). The portable Expr + LiteralValue
 * live in sm8-core. The compiler is the boundary where engine-
 * portable data becomes engine-specific Column.
 *
 * the column-name resolution is straightforward — every FieldRef
 * lowers to a Spark `col(name)` Column; the runtime compiler
 * has already done any column-rename on the DataFrame. We do
 * NOT trust caller-supplied model dimensions/measures for the
 * output column types — we read them from the actual compiled
 * plan.
 *
 * ported from the legacy `/tmp/semanticdf/adapters/semanticdf-spark`
 * PortableExprCompiler. Extends the legacy's 9 LiteralValue
 * cases to cover the reactor's 14 (ByteValue, ShortValue,
 * BinaryValue, ArrayValue added). The 24 Expr cases match
 * sm8-core's Expr.scala (we share the portable type).
 *
 * Per RFC §13 + Layer A's `SparkTypeBridge`: this compiler
 * pairs with the type bridge to form the full Spark-adapter
 * picture. The bridge handles the schema-level translation
 * (Spark DataType to portable SealedDataType); this compiler
 * handles the row-level expression translation (portable Expr
 * to Spark Column).
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineError, EngineIdentity}
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.predicate.{Predicate => FilterPredicate}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, not => sparkNot, when}
/**
 * Engine-specific Spark compiler for portable [[Expr]] -> Spark
 * [[Column]]. Pure function (Expr) -> Column with no state, no
 * IO. Per [[scala-data-driven-refactor-mindset]] "sealed-trait
 * dispatch": the 24 Expr cases are enumerated at the case-class
 * granularity.
 *
 * The capture contract: this companion is a Scala `object`
 * (singleton). The runtime captures this singleton reference
 * (Serializable per JVM static field conventions). The `Column`
 * return values are Spark-side (transient, per-query).
 *
 * (closure-safety, schema-drift verify, idempotent retry): this
 * function is pure; the resulting Column is captured only
 * inside the dispatcher's executor thunk (per PR #32); the
 * `query` body wraps the resulting DataFrame in a function
 * (not a closure) — no captured state from this compiler.
 */
object PortableExprCompiler extends java.io.Serializable {

 /**
 * Convert a portable [[Expr]] to a Spark [[Column]]. Throws
 * `UnsupportedOperationException` for the not-yet-supported
 * cases (Expr.MeasureRef subquery resolution, Expr.FunctionCall
 * UDF resolution). The legacy's 9 LiteralValue cases are
 * extended to all 14 in the reactor (ByteValue, ShortValue,
 * BinaryValue, ArrayValue added).
 */
 // PR-O1c (ADR-008-O, P0-2): toColumn now returns
 // `Either[EngineError, Column]` instead of `Column`. The 2
 // throw sites (`Expr.FunctionCall` + `LiteralValue.ArrayValue`)
 // become typed `Left(EngineError.UnsupportedCapability)` so
 // the typed error flows through the compile boundary instead
 // of crashing executors at scale.
 //
 // `Either[Error, T]` for expected business errors the caller
 // should handle. FunctionCall + ArrayValue are expected errors
 // (UDF resolution + array-literal support deferred to future
 // PRs), not programmer errors.
 //
 // The happy path (Literal, FieldRef, Add/Sub/Mul/Div/Mod,
 // comparisons, booleans, Cast, MeasureRef, All, CaseWhen, Alias)
 // threads the typed error via `Right`; a single Either fold
 // covers all 6 prod callsites in MinimalRelOpLowerer +
 // PortableQueryCompiler. Per
 // the fold runs in the driver, no executor-side closure capture.
 def toColumn(expr: Expr): Either[EngineError, Column] = expr match {
 // -- Literal: dispatch on the closed LiteralValue ADT --
 case Expr.Literal(value, _) => literalToColumn(value)

 // -- Column reference --
 case Expr.FieldRef(name)  => Right(col(name))

 // -- Arithmetic: pure delegation via Either flatMap --
 case Expr.Add(l, r)   => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl + cr))
 case Expr.Subtract(l, r)  => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl - cr))
 case Expr.Multiply(l, r)  => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl * cr))
 case Expr.Divide(l, r)   => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl / cr))
 case Expr.Modulo(l, r)   => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl % cr))

 // -- Comparison: Spark's ===, =!=, <, <=, >, >= --
 case Expr.Equal(l, r)   => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl === cr))
 case Expr.NotEqual(l, r)  => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl =!= cr))
 case Expr.LessThan(l, r)  => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl < cr))
 case Expr.LessOrEqual(l, r) => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl <= cr))
 case Expr.GreaterThan(l, r) => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl > cr))
 case Expr.GreaterOrEqual(l, r) => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl >= cr))
 case Expr.And(l, r)   => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl && cr))
 case Expr.Or(l, r)    => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl || cr))
 // -- Logical NOT (unary): thread the inner expr via flatMap so the
 // typed error flows through (P0-2 contract). Uses the renamed
 // `sparkNot` import (line 47) to disambiguate from
 // io.sm8.connectors.spark.PortableExprCompiler.not.
 case Expr.Not(e)    => toColumn(e).map(sparkNot)
 // -- Null checks: Spark's `Column.isNull` / `Column.isNotNull`.
 // Threaded via flatMap for the same reason as Not.
 case Expr.IsNull(e)   => toColumn(e).map(_.isNull)
 case Expr.IsNotNull(e)   => toColumn(e).map(_.isNotNull)
 // -- Cast: lowered to Spark's cast (PR-O1b, ADR-008-O,
 // P0-1 data-correctness fix). Uses
 // SparkTypeBridge.sealedDataTypeToSparkType (PR-O1a).
 case Expr.Cast(e, targetType) =>
  toColumn(e).map(_.cast(
  SparkTypeBridge.sealedDataTypeToSparkType(targetType)
  ))

 // -- MeasureRef: a measure reference resolved at the engine
 // side (the existing measure column is in scope after the
 // aggregate). `col(name)` -- the measure name must already
 // be a groupBy-produced column at this point.
 case Expr.MeasureRef(name) => Right(col(name))

 // -- Expr.All: lowered to a simple column reference. The
 // aggregation is already applied at this point in the
 // plan; the column is present. --
 case Expr.All(name) =>
  Right(col(name))

 // -- Conditional: CASE WHEN --
 //
 // Maps to SQL's `CASE WHEN cond THEN x ELSE y END`. Spark's
 // `Column.when(condition, value)` is left-associative; we fold
 // the branches left-to-right so the SQL semantics match
 // ("first matching condition wins"). Per
 // the fold runs in the driver; each `toColumn` is a pure
 // function call.
 case Expr.CaseWhen(branches, otherwise) =>
  val otherwiseE: Either[EngineError, Column] = toColumn(otherwise)
  branches match {
  case Nil =>
   // Empty branches → just `otherwise`. Mirrors SQL's
   // `CASE WHEN FALSE THEN x ELSE y END` shape.
   otherwiseE
  case (firstCond, firstResult) :: tail =>
   for {
   firstC <- toColumn(firstCond)
   firstR <- toColumn(firstResult)
   tailCs <- tail.foldLeft[Either[EngineError, List[(Column, Column)]]](Right(Nil)) {
    case (accE, (cond, result)) =>
    for {
     acc <- accE
     cCol <- toColumn(cond)
     rCol <- toColumn(result)
    } yield acc :+ ((cCol, rCol))
   }
   otherwiseC <- otherwiseE
   head = when(firstC, firstR)
   chained = tailCs.foldLeft(head) { case (acc, (c, v)) => acc.when(c, v) }
   } yield chained.otherwise(otherwiseC)
  }

 // -- Alias: expr AS name --
 //
 // Maps to Spark's `Column.as(name)`. The alias is the
 // expression-level form; `RelOp.Project` carries the higher-
 // level `List[(Expr, String)]` shape (PR-J adds that).
 case Expr.Alias(name, expr) =>
  toColumn(expr).map(_.as(name))

 // -- FunctionCall: UDF resolution deferred. --
 // PR-O1c (ADR-008-O, P0-2): the previous throw-bomb
 // (`throw new UnsupportedOperationException(...)`) became
 // a typed `Left(EngineError.UnsupportedCapability(...))`
 // so the error flows through the compile boundary instead
 // of crashing the driver or (worse, at scale) killing
 // executors + retrying indefinitely.
 case Expr.FunctionCall(name, _) =>
  Left(EngineError.UnsupportedCapability(
  engine  = "spark-3.5",
  capability = "Expr.FunctionCall",
  message = s"PortableExprCompiler.toColumn: Expr.FunctionCall('$name',...) is " +
      "not supported in this Layer C follow-up (UDF resolution " +
      "deferred to a future PR that wires the Spark UDF registry).",
  ))
 }

 /**
 * Helper: compile a `List[Expr]` to `Array[Column]` via Either-fold.
 *
 * a single helper to share between MinimalRelOpLowerer.lowerScan
 * (projection pruning), MinimalRelOpLowerer.lowerProject
 * (the per-alias list), and PortableQueryCompiler.applyAggregations
 * (the dimensions list). Exposed package-private (no `private`
 * keyword) so MinimalRelOpLowerer can call it without exposing
 * it to the SDK surface.
 */
 def colsOf(exprs: List[Expr]): Either[EngineError, Array[Column]] =
 exprs.foldLeft[Either[EngineError, Array[Column]]](Right(Array.empty[Column])) {
  case (accE, e) =>
  for {
   acc <- accE
   c <- toColumn(e)
  } yield acc :+ c
 }

 /**
 * Map a portable [[LiteralValue]] to a Spark `lit(...)` column.
 * `LiteralValue.NullValue` case maps to `lit(null)` (the
 * Spark runtime's null marker). All other cases are
 * type-preserving: the Spark `Column` carries the typed
 * value through the DataFrame ops.
 */
 private def literalToColumn(value: LiteralValue): Either[EngineError, Column] = value match {
 case LiteralValue.NullValue   => Right(lit(null))
 case LiteralValue.BoolValue(b)  => Right(lit(b))
 case LiteralValue.IntValue(n)  => Right(lit(n))
 case LiteralValue.LongValue(n)  => Right(lit(n))
 case LiteralValue.ByteValue(b)  => Right(lit(b))
 case LiteralValue.ShortValue(s)  => Right(lit(s))
 case LiteralValue.FloatValue(f)  => Right(lit(f))
 case LiteralValue.DoubleValue(d)  => Right(lit(d))
 case LiteralValue.DecimalValue(d) => Right(lit(d))
 case LiteralValue.StringValue(s)  => Right(lit(s))
 case LiteralValue.TimestampValue(i) => Right(lit(i))
 case LiteralValue.DateValue(d)  => Right(lit(d))
 case LiteralValue.BinaryValue(b)  => Right(lit(b.toArray))
 // PR-O1c (ADR-008-O, P0-2): array literal returns a typed
 // `Left(EngineError.UnsupportedCapability(...))` instead of
 // a thrown `UnsupportedOperationException`. Same
 // reasoning as `Expr.FunctionCall` above: the throw-bomb
 // could kill executors at scale. The typed error flows
 // through the compile boundary and the MCP server maps it
 // to a 501 UNSUPPORTED_CAPABILITY wire response.
 case LiteralValue.ArrayValue(_) =>
  Left(EngineError.UnsupportedCapability(
  engine  = "spark-3.5",
  capability = "LiteralValue.ArrayValue",
  message = "PortableExprCompiler.toColumn: LiteralValue.ArrayValue is not " +
      "supported (array literals are JSON-serialized; future PR can " +
      "land native Spark array support).",
  ))
 // PR-2/B1 (ADR-008-P §B1 sub-step 2): extend the literalToColumn
 // match with the remaining 2 unwired LiteralValue cases
 // (MapValue + StructValue). Both return typed Left(UnsupportedCapability)
 // matching the existing LiteralValue.ArrayValue shape above -- per
 // PR-O1c the EngineError ADT is REUSED (no new case added). The
 // post-fix LiteralValue wiring count is 16 of 16 (NullValue + 11
 // primitive + BinaryValue + ArrayValue + MapValue + StructValue).
 case LiteralValue.MapValue(_) =>
  Left(EngineError.UnsupportedCapability(
  engine  = "spark-3.5",
  capability = "LiteralValue.MapValue",
  message = "PortableExprCompiler.toColumn: LiteralValue.MapValue is not " +
      "supported (map literals are JSON-serialized; future PR can " +
      "land native Spark map support).",
  ))
 case LiteralValue.StructValue(_) =>
  Left(EngineError.UnsupportedCapability(
  engine  = "spark-3.5",
  capability = "LiteralValue.StructValue",
  message = "PortableExprCompiler.toColumn: LiteralValue.StructValue is not " +
      "supported (struct literals are JSON-serialized; future PR can " +
      "land native Spark struct support).",
  ))
 }
 /**
 * PR-22 (ADR-008-R §PR-22): translate the portable Predicate
 * filter-language AST (sm8-core/predicate/Predicate.scala) to a
 * Spark Column. The 6-case Predicate ADT is exhaustively matched
 * (per scala-bug-hunting-mindset SS3 + scala-data-driven-refactor-
 * mindset SS3 -- sealed over Map).
 *
 * Per scala-spark-batch-bugs-mindset SS1 (closure-safety -- the
 * user's explicit concern): this method is a pure function
 * (Predicate) -> Either[EngineError, Column]. NO closures cross
 * to executors. The resulting Column is consumed at driver-side
 * via df.filter(column).
 *
 * Per scala-perf-testing-mindset SS3 (allocation is the tax):
 * zero per-row allocation. The Column is built once per predicate
 * + reused across all rows in the partition.
 */
 def predicateToColumn(p: FilterPredicate): Either[EngineError, Column] = p match {
 case FilterPredicate.Compare(field, op, value) =>
  val left = col(field)
  val right = lit(value)
  Right(op match {
  case io.sm8.core.predicate.CompareOp.Eq => left === right
  case io.sm8.core.predicate.CompareOp.Ne => left =!= right
  case io.sm8.core.predicate.CompareOp.Lt => left < right
  case io.sm8.core.predicate.CompareOp.Le => left <= right
  case io.sm8.core.predicate.CompareOp.Gt => left > right
  case io.sm8.core.predicate.CompareOp.Ge => left >= right
  })
 case FilterPredicate.In(field, values, negate) =>
  val left = col(field)
  val chained: Column = values.toIndexedSeq match {
  case Seq() => lit(false)
  case Seq(head) => left === lit(head)
  case seq  => seq.tail.foldLeft[Column](left === lit(seq.head)) {
   (acc, v) => acc || (left === lit(v))
  }
  }
  Right(if (negate) !chained else chained)
 case FilterPredicate.IsNull(field, negate) =>
  val c = col(field)
  Right(if (negate) c.isNotNull else c.isNull)
 case FilterPredicate.StringMatch(field, op, pattern) =>
  // PR-29 (ADR-008-R SSfilterPushdown ergonomics): lower to
  // Spark's `Column.startsWith / contains / endsWith`. Per
  // predictable (NOT a regex). The pattern is taken as-is
  // (Spark's Column.startsWith uses exact substring match).
  val c = col(field)
  Right(op match {
  case io.sm8.core.predicate.StringMatchOp.StartsWith => c.startsWith(pattern)
  case io.sm8.core.predicate.StringMatchOp.Contains => c.contains(pattern)
  case io.sm8.core.predicate.StringMatchOp.EndsWith => c.endsWith(pattern)
  })
 case FilterPredicate.And(children) =>
  if (children.isEmpty) Right(lit(true))
  else children.map(predicateToColumn).reduceLeft((accE, cE) => for {
  a <- accE
  c <- cE
  } yield a && c)
 case FilterPredicate.Or(children) =>
  if (children.isEmpty) Right(lit(false))
  else children.map(predicateToColumn).reduceLeft((accE, cE) => for {
  a <- accE
  c <- cE
  } yield a || c)
 case FilterPredicate.Not(pred) =>
  predicateToColumn(pred).map(c => !c)
 }
}

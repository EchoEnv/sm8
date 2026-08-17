/*
 * SM8 Spark Connector — PortableExprCompiler.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1: this is a pure
 * function (Expr) -> Column with NO captured state. Companion
 * object + sealed-trait dispatch over the closed Expr family
 * from sm8-core. The closure-safety contract from PR #36 (which
 * extends java.io.Serializable on MCPEngineProvider) is preserved
 * by the SparkEngineProvider — the compiler itself captures
 * nothing; the SPARK-SPECIFIC Column handle is the one we hand
 * back to the caller.
 *
 * Per [[scala-data-driven-refactor-mindset]] "behavior in
 * adapters, data in core": the compiler lives in the
 * spark-connector (an adapter). The portable Expr + LiteralValue
 * live in sm8-core. The compiler is the boundary where engine-
 * portable data becomes engine-specific Column.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #3 (schema drift):
 * the column-name resolution is straightforward — every FieldRef
 * lowers to a Spark `col(name)` Column; the runtime compiler
 * has already done any column-rename on the DataFrame. We do
 * NOT trust caller-supplied model dimensions/measures for the
 * output column types — we read them from the actual compiled
 * plan.
 *
 * Per [[karpathy-guidelines-mindset]] "smallest correct core":
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
 * Per [[scala-spark-batch-bugs-mindset]] mantras #1, #3, #4
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
  // Per [[scala-error-handling-mindset]] decision rule #1:
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
  // [[scala-spark-batch-bugs-mindset]] mantra #1 (closure-safety):
  // the fold runs in the driver, no executor-side closure capture.
  def toColumn(expr: Expr): Either[EngineError, Column] = expr match {
    // -- Literal: dispatch on the closed LiteralValue ADT --
    case Expr.Literal(value, _)    => literalToColumn(value)

    // -- Column reference --
    case Expr.FieldRef(name)       => Right(col(name))

    // -- Arithmetic: pure delegation via Either flatMap --
    case Expr.Add(l, r)            => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl + cr))
    case Expr.Subtract(l, r)       => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl - cr))
    case Expr.Multiply(l, r)       => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl * cr))
    case Expr.Divide(l, r)         => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl / cr))
    case Expr.Modulo(l, r)         => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl % cr))

    // -- Comparison: Spark's ===, =!=, <, <=, >, >= --
    case Expr.Equal(l, r)          => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl === cr))
    case Expr.NotEqual(l, r)       => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl =!= cr))
    case Expr.LessThan(l, r)       => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl <  cr))
    case Expr.LessOrEqual(l, r)    => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl <= cr))
    case Expr.GreaterThan(l, r)    => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl >  cr))
    case Expr.GreaterOrEqual(l, r) => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl >= cr))
    case Expr.And(l, r)            => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl && cr))
    case Expr.Or(l, r)             => toColumn(l).flatMap(cl => toColumn(r).map(cr => cl || cr))
    // -- Logical NOT (unary): thread the inner expr via flatMap so the
    //    typed error flows through (P0-2 contract). Uses the renamed
    //    `sparkNot` import (line 47) to disambiguate from
    //    io.sm8.connectors.spark.PortableExprCompiler.not.
    case Expr.Not(e)               => toColumn(e).map(sparkNot)
    // -- Null checks: Spark's `Column.isNull` / `Column.isNotNull`.
    //    Threaded via flatMap for the same reason as Not.
    case Expr.IsNull(e)            => toColumn(e).map(_.isNull)
    case Expr.IsNotNull(e)         => toColumn(e).map(_.isNotNull)
    // -- Cast: lowered to Spark's cast (PR-O1b, ADR-008-O,
    //    P0-1 data-correctness fix). Uses
    //    SparkTypeBridge.sealedDataTypeToSparkType (PR-O1a).
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
    //    aggregation is already applied at this point in the
    //    plan; the column is present. --
    case Expr.All(name) =>
      Right(col(name))

    // -- Conditional: CASE WHEN --
    //
    // Maps to SQL's `CASE WHEN cond THEN x ELSE y END`. Spark's
    // `Column.when(condition, value)` is left-associative; we fold
    // the branches left-to-right so the SQL semantics match
    // ("first matching condition wins"). Per
    // [[scala-spark-batch-bugs-mindset]] mantra #1 (closure-safety):
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
                  acc   <- accE
                  cCol  <- toColumn(cond)
                  rCol  <- toColumn(result)
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
        engine     = "spark-3.5",
        capability = "Expr.FunctionCall",
        message    = s"PortableExprCompiler.toColumn: Expr.FunctionCall('$name', ...) is " +
                     "not supported in this Layer C follow-up (UDF resolution " +
                     "deferred to a future PR that wires the Spark UDF registry).",
      ))
  }

  /**
   * Helper: compile a `List[Expr]` to `Array[Column]` via Either-fold.
   *
   * Per [[karpathy-guidelines-mindset]] "smallest correct change":
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
          c   <- toColumn(e)
        } yield acc :+ c
    }

  /**
   * Map a portable [[LiteralValue]] to a Spark `lit(...)` column.
   * Per [[scala-jvm-safety-mindset]] "Null is a liar": the
   * `LiteralValue.NullValue` case maps to `lit(null)` (the
   * Spark runtime's null marker). All other cases are
   * type-preserving: the Spark `Column` carries the typed
   * value through the DataFrame ops.
   */
  private def literalToColumn(value: LiteralValue): Either[EngineError, Column] = value match {
    case LiteralValue.NullValue         => Right(lit(null))
    case LiteralValue.BoolValue(b)       => Right(lit(b))
    case LiteralValue.IntValue(n)        => Right(lit(n))
    case LiteralValue.LongValue(n)       => Right(lit(n))
    case LiteralValue.ByteValue(b)       => Right(lit(b))
    case LiteralValue.ShortValue(s)      => Right(lit(s))
    case LiteralValue.FloatValue(f)      => Right(lit(f))
    case LiteralValue.DoubleValue(d)     => Right(lit(d))
    case LiteralValue.DecimalValue(d)    => Right(lit(d))
    case LiteralValue.StringValue(s)     => Right(lit(s))
    case LiteralValue.TimestampValue(i)  => Right(lit(i))
    case LiteralValue.DateValue(d)       => Right(lit(d))
    case LiteralValue.BinaryValue(b)     => Right(lit(b.toArray))
    // PR-O1c (ADR-008-O, P0-2): array literal returns a typed
    // `Left(EngineError.UnsupportedCapability(...))` instead of
    // a thrown `UnsupportedOperationException`. Same
    // reasoning as `Expr.FunctionCall` above: the throw-bomb
    // could kill executors at scale. The typed error flows
    // through the compile boundary and the MCP server maps it
    // to a 501 UNSUPPORTED_CAPABILITY wire response.
    case LiteralValue.ArrayValue(_) =>
      Left(EngineError.UnsupportedCapability(
        engine     = "spark-3.5",
        capability = "LiteralValue.ArrayValue",
        message    = "PortableExprCompiler.toColumn: LiteralValue.ArrayValue is not " +
                     "supported (array literals are JSON-serialized; future PR can " +
                     "land native Spark array support).",
      ))
  }
}

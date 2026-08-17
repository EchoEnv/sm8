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
  def toColumn(expr: Expr): Column = expr match {
    // -- Literal: dispatch on the closed LiteralValue ADT --
    case Expr.Literal(value, _)    => literalToColumn(value)

    // -- Column reference --
    case Expr.FieldRef(name)       => col(name)

    // -- Arithmetic: pure delegation --
    case Expr.Add(l, r)            => toColumn(l) + toColumn(r)
    case Expr.Subtract(l, r)       => toColumn(l) - toColumn(r)
    case Expr.Multiply(l, r)       => toColumn(l) * toColumn(r)
    case Expr.Divide(l, r)         => toColumn(l) / toColumn(r)
    case Expr.Modulo(l, r)         => toColumn(l) % toColumn(r)

    // -- Comparison: Spark's ===, =!=, <, <=, >, >= --
    case Expr.Equal(l, r)          => toColumn(l) === toColumn(r)
    case Expr.NotEqual(l, r)       => toColumn(l) =!= toColumn(r)
    case Expr.LessThan(l, r)       => toColumn(l) <  toColumn(r)
    case Expr.LessOrEqual(l, r)    => toColumn(l) <= toColumn(r)
    case Expr.GreaterThan(l, r)    => toColumn(l) >  toColumn(r)
    case Expr.GreaterOrEqual(l, r) => toColumn(l) >= toColumn(r)

    // -- Boolean: Spark's &&, ||, unary ! --
    case Expr.And(l, r)            => toColumn(l) && toColumn(r)
    case Expr.Or(l, r)             => toColumn(l) || toColumn(r)
    case Expr.Not(e)               => !toColumn(e)
    case Expr.IsNull(e)            => toColumn(e).isNull
    case Expr.IsNotNull(e)         => toColumn(e).isNotNull

    // -- Cast: lowered to Spark's cast (PR-O1b, ADR-008-O,
    //    P0-1 data-correctness fix). Previously this rendered
    //    `cast("string")` regardless of `targetType` -- the
    //    wire-schema lied and the row-cell decoder fell through
    //    to ResultValue.StringV(cell.toString). Now the cast
    //    uses the portable SealedDataType via
    //    SparkTypeBridge.sealedDataTypeToSparkType (PR-O1a).
    case Expr.Cast(e, targetType) =>
      toColumn(e).cast(
        SparkTypeBridge.sealedDataTypeToSparkType(targetType)
      )

    // -- MeasureRef: a measure reference resolved at the engine
    // side (the existing measure column is in scope after the
    // aggregate). For the GAP-5/7 minimum: `col(name)` -- the measure
    // name must already be a groupBy-produced column at this point.
    // The PR-M2 ModelValidator walker skips MeasureRef as engine-known
    // (schema validation does not require it to be a schema field).
    case Expr.MeasureRef(name) => col(name)

    // -- Expr.All: lowered to a simple column reference. The
    //    aggregation is already applied at this point in the
    //    plan; the column is present. --
    case Expr.All(name) =>
      col(name)

    // -- Conditional: CASE WHEN --
    //
    // Maps to SQL's `CASE WHEN cond THEN x ELSE y END`. Spark's
    // `Column.when(condition, value)` is left-associative; we fold
    // the branches left-to-right so the SQL semantics match
    // ("first matching condition wins"):
    //   branches = [(c1, x1), (c2, x2)] ->
    //     when(c1, x1).when(c2, x2).otherwise(otherwise)
    //
    // Per [[scala-spark-batch-bugs-mindset]] mantra #1 (closure-safety):
    // the foldLeft runs in the driver, no executor-side closure
    // capture. Each `toColumn` is a pure function call.
    case Expr.CaseWhen(branches, otherwise) =>
      // Spark's `Column.when(c, v)` can ONLY be chained on a Column
      // that came from a previous `.when()` (Spark 3.5 contract:
      // "when() can only be applied on a Column previously generated
      // by when() function"). The free function `functions.when(c, v)`
      // creates a fresh CASE-WHEN Column — that's the entry point.
      // Then chain with `.when(...)` for subsequent branches, and
      // terminate with `.otherwise(...)`.
      //
      // Per [[scala-spark-batch-bugs-mindset]] mantra #1 (closure-safety):
      // the fold runs in the driver; each `toColumn` is a pure
      // function call. No executor-side closure capture.
      branches match {
        case Nil =>
          // Empty branches → just `otherwise`. Mirrors SQL's
          // `CASE WHEN FALSE THEN x ELSE y END` shape.
          toColumn(otherwise)
        case (firstCond, firstResult) :: tail =>
          val head = when(toColumn(firstCond), toColumn(firstResult))
          tail.foldLeft(head) { (acc, branch) =>
            val (cond, result) = branch
            acc.when(toColumn(cond), toColumn(result))
          }.otherwise(toColumn(otherwise))
      }

    // -- Alias: expr AS name --
    //
    // Maps to Spark's `Column.as(name)`. The alias is the
    // expression-level form; `RelOp.Project` carries the higher-
    // level `List[(Expr, String)]` shape (PR-J adds that).
    case Expr.Alias(name, expr) =>
      toColumn(expr).as(name)

    // -- FunctionCall: UDF resolution deferred. --
    case Expr.FunctionCall(name, _) =>
      throw new UnsupportedOperationException(
        s"PortableExprCompiler.toColumn: Expr.FunctionCall('$name', ...) is " +
        "not supported in this Layer C follow-up (UDF resolution " +
        "deferred to a future PR that wires the Spark UDF registry).",
      )
  }

  /**
   * Map a portable [[LiteralValue]] to a Spark `lit(...)` column.
   * Per [[scala-jvm-safety-mindset]] "Null is a liar": the
   * `LiteralValue.NullValue` case maps to `lit(null)` (the
   * Spark runtime's null marker). All other cases are
   * type-preserving: the Spark `Column` carries the typed
   * value through the DataFrame ops.
   */
  private def literalToColumn(value: LiteralValue): Column = value match {
    case LiteralValue.NullValue         => lit(null)
    case LiteralValue.BoolValue(b)       => lit(b)
    case LiteralValue.IntValue(n)        => lit(n)
    case LiteralValue.LongValue(n)       => lit(n)
    case LiteralValue.ByteValue(b)       => lit(b)
    case LiteralValue.ShortValue(s)      => lit(s)
    case LiteralValue.FloatValue(f)      => lit(f)
    case LiteralValue.DoubleValue(d)     => lit(d)
    case LiteralValue.DecimalValue(d)    => lit(d)
    case LiteralValue.StringValue(s)     => lit(s)
    case LiteralValue.TimestampValue(i)  => lit(i)
    case LiteralValue.DateValue(d)       => lit(d)
    case LiteralValue.BinaryValue(b)     => lit(b.toArray)
    case LiteralValue.ArrayValue(_)      =>
      // Per the v0.3.0 design review: array literals are JSON-
      // serialized into a single string column. The legacy does
      // not support array literal value expressions; we follow
      // the same path. A future PR can land array-literal
      // support via Spark's `array(...)` function.
      throw new UnsupportedOperationException(
        "PortableExprCompiler.toColumn: LiteralValue.ArrayValue is not " +
        "supported (array literals are JSON-serialized; future PR can " +
        "land native Spark array support).",
      )
  }
}

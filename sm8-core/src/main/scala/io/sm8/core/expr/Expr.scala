package io.sm8.core.expr

import io.sm8.core.schema.{Field, SealedDataType}

/** Engine-portable expression ADT — the engine-portable contract. Mirrors
 * the design doc §4.5.2 "Portable Expr" (21 cases total: 1 literal
 * + 2 references + 5 arithmetic + 6 comparison + 3 boolean + 2 null
 * checks + 1 cast + 1 function call).
 * An `Expr` is the engine-portable shape of a scalar or calculated
 * expression. It flows through the portable model (as the
 * `calculated_measures.expr:` field, the `filters.expr:` field, the
 * `transforms.expr:` field, etc.), through the MCP wire format
 * (`describe_model.data.measures[i].expr`), and through every
 * engine adapter's expression-compile step.
 * ==Why a sealed ADT (not a String)==
 * The design's "Capabilities describe what an engine supports"
 * principle applies: `Expr` is the engine-portable expression
 * shape. A free-form `expr: String` field would let engines
 * invent new expression forms that the validator and compiler
 * couldn't classify. A closed ADT forces every component to
 * handle the closed set of expression forms.
 * ==Why a separate type from `SealedDataType`==
 * `SealedDataType` is the TYPE (`Int`, `BigInt`, etc.). `Expr` is
 * an EXPRESSION that produces a value of that type. The same
 * type (e.g. `BigInt`) can be produced by many expressions
 * (literal, field ref, arithmetic, function call). The type is
 * engine-portable; the expression is engine-portable.
 * ==Why a separate type from `LiteralValue`==
 * `LiteralValue` is the runtime VALUE (the actual `42`, the actual
 * `"hello"`). `Expr` is the EXPRESSION that produces a value
 * (e.g. `Literal(42)` produces an `Int` value; `Add(Literal(1), Literal(2))`
 * produces an `Int` value).
 * ==Why a separate type from `Field`==
 * `Field` is a column-level declaration (name + type + nullability,
 * as resolved from source). `Expr` is an EXPRESSION that can
 * reference a field (e.g. `FieldRef("amount")` references the
 * `Field` named "amount" of the source).
 * ==Why a separate type from `Predicate`==
 * `Predicate` is a boolean expression (where/having filters).
 * `Expr` is a typed scalar expression (calculations). They overlap
 * (booleans are values too) but `Predicate` is the filter
 * vocabulary, `Expr` is the calculation vocabulary. The design's
 * risk #4: "predicates are typed (boolean only)" — `Predicate`
 * is closed, `Expr` is closed, they don't share cases.
 * ==Why core (engine-portable)==
 * Expressions are universal across query engines. The engine
 * adapter compiles each case to its native SQL or computation.
 * Per scala-data-driven-refactor, data (the expression) lives in
 * core; behavior (the compile to SQL) lives in the engine
 * adapter.
 * ==Data-driven mantra compliance==
 * - Pure data: sealed trait + case classes (no behavior)
 * - Equality auto-derived
 * - `Product with Serializable`
 * ==Boundary contract==
 * Zero Spark imports. Verifiable by:
 * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/expr/Expr.scala`
 */
sealed trait Expr extends Product with Serializable

object Expr {

 // -- Literals --

 /** A literal value of a given type. Maps to Spark's `Literal(value,
 * dataType)`, Trino's parameter binding, Databricks' value. */
 final case class Literal(value: LiteralValue, dataType: SealedDataType)
  extends Expr

 // -- References --

 /** A field reference by name. The field must be in the resolved
 * schema (from `ResolvedScan.fields`). Maps to Spark's
 * `UnresolvedAttribute(name)`, Trino's column reference. */
 final case class FieldRef(name: String) extends Expr

 /** A measure reference by name. Only valid in a calculated
 * measure context. Maps to Spark's `UnresolvedAttribute(name)`
 * within a `MeasureScope`, Trino's sibling-measure reference. */
 final case class MeasureRef(name: String) extends Expr

 /** Percent-of-total reference (the legacy `t.all(name)` /
 * `SemanticScope.all(name)` form). Resolves to the value of the
 * named measure at the OUTER aggregation scope (sum across all
 * rows of the current group).
 * Per scala-data-driven-refacer §1: the data is "the total of
 * measure `measureName` across the current grouping". The
 * engine-specific lowerer (window function in Spark, `OVER ()`
 * in Trino + DuckDB) lives in the engine adapter. The portable
 * `Model.dimensions` carry the grouping columns; the engine
 * threads them into the window partition at compile time.
 * Added in v0.3.1 
 * gap. Closes the pct-of-total gap; the legacy library had the
 * surface but not the implementation (`SemanticScope.all` throws
 * `UnsupportedOperationException`). */
 final case class All(measureName: String) extends Expr

 // -- Arithmetic (5) --

 /** Addition. Both operands must be numeric. */
 final case class Add(left: Expr, right: Expr) extends Expr

 /** Subtraction. Both operands must be numeric. */
 final case class Subtract(left: Expr, right: Expr) extends Expr

 /** Multiplication. Both operands must be numeric. */
 final case class Multiply(left: Expr, right: Expr) extends Expr

 /** Division. Both operands must be numeric. Per the design's risk
 * #9: "Decimal scale/overflow differs by engine" — the engine
 * adapter reports `EngineError.DecimalOverflow` when the result
 * would exceed the declared precision. */
 final case class Divide(left: Expr, right: Expr) extends Expr

 /** Modulo (remainder). Both operands must be integer. */
 final case class Modulo(left: Expr, right: Expr) extends Expr

 // -- Comparison (6) --

 /** Equality. Both operands must be the same type. */
 final case class Equal(left: Expr, right: Expr) extends Expr

 /** Inequality. Both operands must be the same type. */
 final case class NotEqual(left: Expr, right: Expr) extends Expr

 /** Less than. Both operands must be the same numeric type. */
 final case class LessThan(left: Expr, right: Expr) extends Expr

 /** Less or equal. Both operands must be the same numeric type. */
 final case class LessOrEqual(left: Expr, right: Expr) extends Expr

 /** Greater than. Both operands must be the same numeric type. */
 final case class GreaterThan(left: Expr, right: Expr) extends Expr

 /** Greater or equal. Both operands must be the same numeric type. */
 final case class GreaterOrEqual(left: Expr, right: Expr) extends Expr

 // -- Boolean (3) --

 /** Logical AND. Both operands must be boolean. */
 final case class And(left: Expr, right: Expr) extends Expr

 /** Logical OR. Both operands must be boolean. */
 final case class Or(left: Expr, right: Expr) extends Expr

 /** Logical NOT. Operand must be boolean. */
 final case class Not(expr: Expr) extends Expr

 // -- Null checks (2) --

 /** IS NULL. Operand is any type. */
 final case class IsNull(expr: Expr) extends Expr

 /** IS NOT NULL. Operand is any type. */
 final case class IsNotNull(expr: Expr) extends Expr

 // -- Cast (1) --

 /** Cast an expression to a different type. Maps to Spark's
 * `Cast(child, dataType)`, Trino's `CAST(expr AS type)`, Databricks'
 * value cast. */
 final case class Cast(expr: Expr, targetType: SealedDataType) extends Expr

 // -- Function call (1) --

 /** Engine-specific function call. The `name` is the function name
 * (e.g. `LOWER`, `UPPER`, `LENGTH`, `ABS`); `args` are the arguments.
 * The engine adapter validates that the function is supported
 * in its engine (e.g. `LOWER` works in Trino, Snowflake, Databricks
 * but not all engines support every string function).
 * Maps to Spark's `Call(functionName, args)`, Trino's function
 * call. */
 final case class FunctionCall(name: String, args: Seq[Expr]) extends Expr

 // -- Case When (1) --

 /** CASE WHEN conditional expression. Maps to SQL's
 * `CASE WHEN cond THEN x ELSE y END`, Spark's `Column.when(.)`
 * foldLeft chain, Trino's `CASE WHEN. THEN. ELSE. END`.
 * The `branches` are evaluated in order; the first matching
 * `condition` produces the corresponding `result`. If no branch
 * matches, the `otherwise` expression is returned.
 * Empty `branches` list is allowed — it degenerates to
 * `otherwise`. This mirrors SQL's `CASE WHEN FALSE THEN x ELSE y END`
 * shape and is useful for derived measures whose condition
 * structure is computed dynamically.
 *  * Expr)]` is the portable shape; the engine adapter folds it
 * via `Column.when(.).when(.).otherwise(.)`. A free-form
 * `when: Map[Expr, Expr]` would be a downgrade (silent
 * defaulting on lookup miss) — and the order of branches
 * matters in SQL `CASE WHEN`, so `List` (ordered) is correct.
 *  * that doesn't support CASE WHEN surfaces a typed
 * `EngineError.UnsupportedCapability` — never a silent no-op.
 *  * close the "transform: CASE WHEN" gap identified in the
 * user's 2026-08-16 directive ("column like case when").
 */
 final case class CaseWhen(
  branches: List[(Expr, Expr)],
  otherwise: Expr) extends Expr

 // -- Alias (1) --

 /** Column alias — `expr AS name`. Maps to Spark's
 * `Column.as(name)`, Trino's `SELECT expr AS name`, DuckDB's
 * `SELECT expr AS name`.
 * The `name` is the alias used in the result schema (the
 * `Field.name` of the resulting column). `Alias` is the
 * expression-level form; the higher-level `RelOp.Project`
 * carries `List[(Expr, String)]` (an expression + an alias),
 * but when a model validator wants to express "this dimension
 * is `a + b` named `total`" inline in `Expr`, `Alias` is the
 * portable shape.
 *  * class from `FieldRef(name)` — `FieldRef` is a reference to
 * an existing column; `Alias(name, expr)` is a NEW column
 * derived from an expression with a given name. The two are
 * semantically different shapes.
 *  * to close the "transform: alias" gap identified in the
 * user's 2026-08-16 directive ("or alias, etc.").
 */
 final case class Alias(
  name: String,
  expr: Expr) extends Expr
}

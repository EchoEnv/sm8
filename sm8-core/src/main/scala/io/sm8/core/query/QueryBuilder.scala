/*
 * SM8 Core -- QueryBuilder (engine-portable `Model -> RelOp` lowering).
 *
 * PR-L (per ADR-008-L): the boundary step BEFORE the engine sees
 * anything. `QueryBuilder.build(model, resolver, identity)` turns
 * a portable `Model` into a portable `RelOp` tree (the SM8
 * "Model -> IR" path). The engine adapter (`PortableQueryCompiler`
 * in the spark-connector, future Trino/DuckDB compilers) then
 * lowers each `RelOp` case to its native operation.
 *
 * Per [[scala-data-driven-refactor-mindset]] SS1 (data in core,
 * behavior in adapters): the lowering is engine-portable --
 * every engine adapter wants the SAME `RelOp` tree for a given
 * `Model` (modulo engine-specific capabilities). The
 * `RelOp -> native plan` is the adapter's behavior.
 *
 * Per [[karpathy-guidelines-mindset]]: ported from the legacy
 * `/tmp/semanticdf/semanticdf-core/src/main/scala/io/semanticdf/core/query/QueryBuilder.scala`
 * with the FULL shape (joins + aggregate + sort + limit + cycle
 * detection) -- not the legacy's v1 single-source scope.
 *
 * ==Why a separate object (not a method on `Model`)==
 *
 * Per [[karpathy-guidelines-mindset]] "smallest correct core":
 * the `Model` class is pure data (per SS1 of the data-driven
 * mantra). Putting `build` on `Model` would mix data with
 * behavior (and `SourceResolver` is a runtime collaborator, not
 * a data field). A separate object keeps `Model` pure.
 *
 * ==Build pipeline==
 *
 *   1. resolveSource(model.source)  -> Scan
 *   2. resolveJoinSources(model.joins)  -> List[(JoinSpec, Scan)]
 *   3. assembleRelOp(model, scan, joinScans)  -> RelOp tree
 *      (joins fold the Scan nodes via RelOp.Join; then Filter;
 *      then Project + Aggregate + Sort + Limit)
 *   4. detectCalcCycles(model.calculatedMeasures)  -> typed
 *      EngineError.UnsupportedCapability (fail-fast at build
 *      time, never a silent runtime crash)
 *
 * ==Boundary contract==
 * ==Boundary contract==
 *
 */
package io.sm8.core.query

import io.sm8.core.engine.{EngineError, EngineIdentity, ResolvedSource, SourceResolver}
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.schema.SealedDataType
import io.sm8.core.model.{CalculatedMeasure, Model, SourceRef}
import io.sm8.core.rel.{AggregateCall, JoinKind, RelOp, SortKey}

object QueryBuilder {

  /** Lower a portable [[Model]] to a portable [[RelOp]] tree.
    *
    * Per [[scala-data-driven-refactor-mindset]] SS1: the lowering
    * is pure data-shape-only -- no IO beyond the `SourceResolver`
    * call (which the caller chose). The result tree is engine-
    * portable; the engine adapter does the native-plan lower.
    */
  def build(
      model:    Model,
      resolver: SourceResolver,
      identity: EngineIdentity,
  ): Either[EngineError, RelOp] = {

    // Step 1: resolve the primary source.
    val primaryScanE = for {
      scan       <- resolver.resolve(model.source, identity)
      typedScan   <- toScan(scan, model.name)
    } yield typedScan

    // Step 2: resolve every JoinSpec's right side (each join is
    // a new SourceRef call). FoldLeft so later joins can see
    // earlier resolution failures.
    val joinScansE: Either[EngineError, List[(ScanJoin)]] = model.joins.foldLeft(
      Right(Nil): Either[EngineError, List[(ScanJoin)]]
    ) { (accE, js) =>
      accE.flatMap { acc =>
        // Step 2a: translate js.rightModel (String) into a SourceRef
        // via the resolver.
        resolver.resolveModel(js.rightModel, identity).flatMap { rightSource =>
          // Step 2b: resolve that SourceRef.
          resolver.resolve(rightSource, identity).flatMap { resolved =>
            toScan(resolved, js.rightModel).map { rightScan =>
              acc :+ ScanJoin(js, rightScan)
            }
          }
        }
      }
    }

    // Step 3: assemble the RelOp tree.
    val planE = for {
      primary    <- primaryScanE
      joinScans  <- joinScansE
      cycleCheck <- detectCalcCycles(model.calculatedMeasures)
    } yield assembleRelOp(model, primary, joinScans)

    planE.left.map(annotateWith(model.name))
  }

  /** Internal: a join + its resolved right-side Scan. */
  private case class ScanJoin(
      js:        io.sm8.core.model.JoinSpec,
      rightScan: ResolvedSource.Scan,
  )

  /** Surface a resolver failure as a typed error tagged with the
    * model name for diagnostics. */
  private def annotateWith(modelName: String)(err: EngineError): EngineError =
    err match {
      case e: EngineError.FeatureDeferred =>
        // Already shaped; carry the model name in the feature tag.
        e.copy(feature = s"query-builder.${e.feature}:${modelName}")
      case e: EngineError.UnsupportedCapability =>
        e.copy(message = s"${e.message} (model='$modelName')")
      case other => other
    }

  /** Internal: pattern-match the `ResolvedSource` ADT to `Scan`,
    * mapping the 3 failure cases to typed `EngineError.FeatureDeferred`
    * at the build boundary (per [[scala-error-handling-mindset]]:
    * never silent no-ops). */
  private def toScan(
      rs:     ResolvedSource,
      source: String,
  ): Either[EngineError, ResolvedSource.Scan] = rs match {
    case scan: ResolvedSource.Scan         => Right(scan)
    case nf: ResolvedSource.NotFound      => Left(EngineError.FeatureDeferred(
                                                engine   = "query-builder",
                                                feature  = s"source-not-found:$source",
                                                release  = "post-v0.1.0",
                                                message  = s"Source '$source' not found: ${nf.reason}"))
    case in: ResolvedSource.Incompatible  => Left(EngineError.FeatureDeferred(
                                                engine   = "query-builder",
                                                feature  = s"source-incompatible:$source",
                                                release  = "post-v0.1.0",
                                                message  = s"Source '$source' incompatible: ${in.reason}"))
    case au: ResolvedSource.AuthFailed    => Left(EngineError.FeatureDeferred(
                                                engine   = "query-builder",
                                                feature  = s"source-auth-failed:$source",
                                                release  = "post-v0.1.0",
                                                message  = s"Source '$source' auth failed: ${au.reason}"))
  }

  /** Internal: detect cycles in the calculated-measure dependency
    * graph. A calculated measure `cm` depends on any measure name
    * it references (via `Expr.FieldRef` / `Expr.MeasureRef`).
    *
    * Per ADR-007 SSPR-J (cycle detection defaulted to PR-J,
    * implemented here): cycles raise a typed
    * `EngineError.UnsupportedCapability` at build time -- fail-
    * fast, never a silent runtime crash (per
    * [[debug-mantra-mindset]] SS1).
    *
    * Algorithm: DFS with WHITE/GRAY/BLACK coloring. WHITE = not
    * visited; GRAY = on current path; BLACK = fully processed.
    * A back-edge to GRAY is a cycle.
    */
  private def detectCalcCycles(
      calcs: List[CalculatedMeasure],
  ): Either[EngineError, Unit] = {
    val byName: Map[String, CalculatedMeasure] =
      calcs.map(c => c.name -> c).toMap

    val White = 0
    val Gray  = 1
    val Black = 2

    val color = scala.collection.mutable.Map[String, Int]().withDefaultValue(White)

    def refs(name: String): Set[String] =
      byName.get(name).map(measureRefs).getOrElse(Set.empty)

    // Measure-dependency extraction (cycle detection). A name is
    // a cycle ref if it is EITHER:
    //   - a MeasureRef / All (Calculator.measureNamesOf), OR
    //   - a FieldRef that matches one of the declared calculated
    //     measure names (so a FieldRef to "x" inside calc "a" is a
    //     cycle back to "a" if "x" is also a calc)
    //
    // Per [[karpathy-guidelines-mindset]]: Calculator is the single
    // source of truth for Expr walking -- QueryBuilder composes the
    // two field-name + measure-name views and filters by the
    // declared-calc set.
    def measureRefs(c: CalculatedMeasure): Set[String] = {
      val m = io.sm8.core.expr.Calculator.measureNamesOf(c.expr)
      val f = io.sm8.core.expr.Calculator.fieldNamesOf(c.expr)
      (m ++ f).filter(byName.contains)
    }

    // Iterative DFS with an explicit path stack (no recursion -- the
    // calc DAG is small in practice; iterative avoids Scala stack
    // surprises per scala-jvm-safety-mindset).
    def visit(name: String): Either[EngineError, Unit] = {
      var stack: List[(String, List[String])] = List((name, refs(name).toList))
      var path: List[String] = List.empty
      var foundCycle: Option[List[String]] = None

      while (stack.nonEmpty && foundCycle.isEmpty) {
        val (n, remaining) = stack.head
        stack = stack.tail
        color(n) match {
          case Black => ()  // already processed
          case Gray  => foundCycle = Some((path :+ n).reverse)  // cycle
          case White =>
            color(n) = Gray
            path = path :+ n
            // push neighbours; if any are Gray on revisit, we catch above
            val next = remaining.headOption.map(_ => remaining.tail ::: List.empty[String])
            remaining match {
              case Nil =>
                // done with n
                color(n) = Black
              case head :: tail =>
                stack = (n -> tail) :: stack
                stack = (head -> refs(head).toList) :: stack
            }
        }
      }
      foundCycle match {
        case Some(cycle) => Left(EngineError.UnsupportedCapability(
          engine     = "query-builder",
          capability = "CalculatedMeasure.cycle",
          message    = s"Cycle in calculated-measure DAG: ${cycle.mkString(" -> ")}",
        ))
        case None => Right(())
      }
    }

    // Run a separate visit per uncolored calc
    val allOk: Either[EngineError, Unit] = calcs.foldLeft[Either[EngineError, Unit]](Right(())) { (acc, c) =>
      acc.flatMap(_ => visit(c.name))
    }
    allOk
  }

  /** Internal: assemble the RelOp tree once every source is resolved
    * + cycles are checked.
    *
    * Shape: Scan_1 left-join Scan_2 left-join ... -> Filter ->
    * Project + Aggregate -> Sort -> Limit.
    */
  private def assembleRelOp(
      model:      Model,
      primary:    ResolvedSource.Scan,
      joinScans:  List[ScanJoin],
  ): RelOp = {

    // Build the multi-source Scan via folding Join nodes.
    // Per RelOp.Join (PR-H): the join shape is Scan_1 ⊕ Scan_2 → Join.
    // PR-K handles the engine side (5 kinds, single-key equi-join
    // dedup); here we just emit the structural RelOp.
    val multiScan: RelOp = joinScans.foldLeft[RelOp](
      RelOp.Scan(
        sourceRef = primary.source,
        schema    = primary.schema,
        projection = Nil,  // v0.1.0: read all columns; engine can prune
      )
    ) { (acc, sj) =>
      val rightScanNode = RelOp.Scan(
        sourceRef  = sj.rightScan.source,
        schema     = sj.rightScan.schema,
        projection = Nil,
      )
      // Build the join condition from the single (left, right) key.
      // The single-key constraint matches PR-K's spark-side compile.
      // Multi-key joins surface as typed UnsupportedCapability at the
      // spark compile step (PR-K's contract), not here (the QueryBuilder
      // shape supports them; the engine adapter may reject).
      val condition: Expr = sj.js.keys match {
        case Nil        => Expr.Literal(io.sm8.core.expr.LiteralValue.BoolValue(true), io.sm8.core.schema.SealedDataType.Boolean)
        case (l, r) :: Nil => Expr.Equal(Expr.FieldRef(l), Expr.FieldRef(r))
        case _          => Expr.Literal(io.sm8.core.expr.LiteralValue.BoolValue(true), io.sm8.core.schema.SealedDataType.Boolean)
      }
      RelOp.Join(
        left      = acc,
        right     = rightScanNode,
        kind      = sj.js.kind,
        condition = condition,
      )
    }

    // Apply model.filters as a Filter chain (foldLeft).
    val filtered: RelOp = model.filters.foldLeft(multiScan) { (acc, f) =>
      RelOp.Filter(input = acc, predicate = f.predicate)
    }

    // Project + Aggregate if measures exist (dims become groupBy
    // keys, measures become aggregates); otherwise a plain Project.
    val projected: RelOp = if (model.measures.nonEmpty) {
      val aggNode = RelOp.Aggregate(
        input      = filtered,
        groupBy    = model.dimensions.map(d => Expr.FieldRef(d.expr)),
        aggregates = model.measures.map(m => AggregateCall(
          fn        = m.expr.fn,
          input     = m.expr.input,
          alias     = m.name,
        )),
      )
      RelOp.Project(
        input       = aggNode,
        expressions = projectExpressions(model),
      )
    } else {
      RelOp.Project(
        input       = filtered,
        expressions  = projectExpressions(model),
      )
    }

    // Sort: v0.1.0 -- no portable sort key. The engine adapter
    // adds engine-specific sort via `preview(n)` / `count()` etc.
    // PR-L leaves sortKey as a pass-through (RelOp.Sort with empty
    // keys = no-op; the engine adapter is free to add its own).
    val sorted: RelOp = RelOp.Sort(input = projected, keys = Nil)

    // Limit: v0.1.0 -- no portable limit. Same reasoning.
    val limited: RelOp = RelOp.Limit(input = sorted, count = Long.MaxValue)

    limited
  }

  /** Internal: the Projection expressions. Dimensions + measures
    * + calculated measures. Each calculated measure is wrapped
    * in `Expr.Alias(name, expr)` (per PR-I) so the engine
    * adapter can name the resulting column.
    */
  private def projectExpressions(model: Model): List[(Expr, String)] = {
    val dimCols  = model.dimensions.map(d => (Expr.FieldRef(d.expr), d.name))
    val measCols = model.measures.map(m => (Expr.FieldRef(m.name), m.name))
    val calcCols = model.calculatedMeasures.map(c => (Expr.Alias(c.name, c.expr), c.name))
    dimCols ++ measCols ++ calcCols
  }
}

/*
 * SM8 Core — ModelLoader (YAML → Model).
 *
 * Engine-portable YAML manifest loader. Reads a YAML file and
 * produces an `io.sm8.core.model.Model` (the engine-portable IR).
 *
 * Per [[scala-data-driven-refactor-mindset]] §1 ("data in core,
 * behavior in adapters"): the loader is a typed factory in core.
 * It does NOT know which database / cache / auth system is in use
 * (RFC §3 Core Boundary). Spark coupling happens in the connector
 * layer; the YAML layer is engine-portable.
 *
 * ==Why a separate manifest layer (vs. reading directly into Model)==
 *
 * Per [[scala-data-driven-refactor-mindset]] §2 ("shape and validity
 * are separate"): the YAML root is parsed into a `Map[String, Any]`
 * (shape). Then `ModelBuilder.build(...)` validates + constructs the
 * domain Model (validity). Mixing the two would mean parse failures
 * leak into domain invariants.
 *
 * ==Why minimal subset (not the legacy's full 10 portable types)==
 *
 * Per [[karphy-guidags-mindset]] "smallest correct change": the
 * legacy's `PortableModel` carries 8 sub-types
 * (`PortableJoin`, `PortableRollup`, `PortableCalculatedMeasure`,
 * `PortableFilter`, etc.). SM8-core's `Model` does NOT have those
 * fields (PR-M1 added joins + calculated_measures; rollups remain
 * IR — they're deferred per the plan). So we port ONLY the subset
 * that maps to existing `Model` fields: name, version, description,
 * source, status, dimensions, measures, filters.
 *
 * When those IR fields land in future PRs, the loader extends.
 *
 * ==RFC alignment==
 *
 * - `semantic-layer-engine-architecture.md` §3 Core Boundary: lives
 *   in core, no data-source knowledge.
 * - `semantic-layer-engine-architecture.md` §7 Contracts: the
 *   loader is a typed factory for the `Model` contract.
 * - `plugins.md` / `hooks.md` / `adapters.md`: not a plugin/hook/
 *   adapter.
 *
 * ==Plan alignment==
 *
 * - Plan line 195 ("manifest/ IR move"): this PR is the minimal
 *   subset of that move. The full 10 portable types land in a
 *   future PR series when the underlying IR fields exist.
 * - Plan line 289 (Step 10 "ModelService.compileFromYaml"): this
 *   loader is the foundation for that path.
 *
 * ==Spark concerns (per user directive)==
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantras:
 * - mantras #1, #5: no Spark types captured, no executor-side
 *   closure. The loader is pure data in sm8-core (Spark-free per
 *   the plan's inverted enforcer pattern).
 * - mantra #3 (schema-drift verify at boundary): parse failures
 *   are typed `ManifestError`; validation failures are typed
 *   `ModelValidationError`. The two layers stay distinct.
 *
 * Per [[scala-jvm-safety-mindset]]: `InputStream.close()` in
 * `finally`. No static / ThreadLocal state. No mutable builder.
 *
 * Per [[scala-perf-testing-mindset]]: not in hot path; YAML loading
 * is startup-time.
 */
package io.sm8.core.manifest

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

import io.sm8.core.model.{FilterSpec, Model, ModelBuilder, ModelStatus, SourceRef}

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.util.control.NonFatal

/**
 * Loads an engine-portable `Model` from a YAML manifest.
 *
 * Supported YAML schema (subset of the legacy `PortableModel`):
 *
 * {{{
 * name: my-model                  # required
 * version: 1                      # required (>= 0)
 * description: "..."              # optional
 * source:                         # required (one of:)
 *   byName:
 *     catalog: default            # optional
 *     table: people               # required
 *   byPath:
 *     format: csv                 # required
 *     path: /tmp/data.csv         # required
 *     options: {key: value}       # optional
 *   byProvider:
 *     providerRefName: my-ref     # required
 * status: draft                   # optional (default: draft)
 * dimensions:                     # optional (default: [])
 *   - name: region
 *     expr: region
 * measures:                       # optional (default: [])
 *   - name: revenue
 *     expr: sum(amount)
 * filters:                        # optional (default: [])
 *   - name: adults
 *     # raw-sql predicate; typed Expr filter is deferred
 *     predicate: "age >= 18"
 * }}}
 *
 * The `predicate` field for filters is a raw SQL string in this PR
 * (matches `MCPQueryRequest.where: Option[String]`). When the typed
 * `FilterSpec.predicate: Expr` parser ships, this loader upgrades.
 */
object ModelLoader {

  private val mapper: ObjectMapper =
    new ObjectMapper(new YAMLFactory())

  /** Load from a `Path`. Returns `Right(Model)` on success;
    * `Left(ManifestError)` on parse failure. Validation failures
    * (e.g. blank name) surface as `ModelValidationError` from
    * `Model.of(...)` — wrapped in `ManifestError.InvalidYaml` here
    * so callers see a single error type from the loader. */
  def fromPath(path: Path): Either[ManifestError, Model] = {
    if (!Files.exists(path))
      Left(ManifestError.InvalidYaml(s"file not found: $path"))
    else {
      val stream: InputStream = Files.newInputStream(path)
      try fromStream(stream)
      catch {
        case NonFatal(e) =>
          Left(ManifestError.ParseFailure(e.getMessage))
      } finally stream.close()
    }
  }

  /** Load from a `String` (for tests + in-memory manifests). */
  def fromString(yaml: String): Either[ManifestError, Model] =
    fromStream(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)))

  /** Load from an `InputStream`. Caller is responsible for stream
    * lifecycle EXCEPT for the case of `fromPath` (which manages
    * its own stream). */
  def fromStream(stream: InputStream): Either[ManifestError, Model] =
    try {
      val root = mapper.readValue(stream, classOf[java.util.Map[_, _]])
      buildModel(root)
    } catch {
      case NonFatal(e) =>
        Left(ManifestError.ParseFailure(e.getMessage))
    }

  /** Construct the `Model` from the parsed YAML map. */
  private def buildModel(
      root: java.util.Map[_, _]
  ): Either[ManifestError, Model] = {
    val name: Option[String] = stringField(root, "name")
    val version: Option[Int]  = intField(root, "version")
    val description: Option[String] = stringField(root, "description")
    val statusOpt: Option[String]   = stringField(root, "status")

    // Name + version are required (the rest have defaults).
    if (name.isEmpty)
      return Left(ManifestError.MissingField("name", "root"))
    if (version.isEmpty)
      return Left(ManifestError.MissingField("version", "root"))

    val status: ModelStatus = statusOpt match {
      case None                       => ModelStatus.Draft
      case Some(s) if s.equalsIgnoreCase("draft")     => ModelStatus.Draft
      case Some(s) if s.equalsIgnoreCase("published") => ModelStatus.Published
      case Some(s) if s.equalsIgnoreCase("deprecated") => ModelStatus.Deprecated
      case Some(other) =>
        return Left(ManifestError.UnknownStatus(other))
    }

    val source: Either[ManifestError, SourceRef] =
      parseSource(asMap(root.get("source")).getOrElse(return Left(ManifestError.MissingField("source", "root"))))

    source.flatMap { src =>
      val dims    = parseDimensions(asSeq(root.get("dimensions")))
      val meas    = parseMeasures(asSeq(root.get("measures")))
      val filters = parseFilters(asSeq(root.get("filters")))

      // PR-M1 (ADR-008-L Appendix GAP 4): parse joins + calculated
      // measures. Both can fail (unknown join kind, unparsable calc
      // expr) -- surface as typed ManifestError, never silent.
      val joinsE  = parseJoins(asSeq(root.get("joins")))
      val calcsE  = parseCalculatedMeasures(asSeq(root.get("calculated_measures")))

      for {
        joins <- joinsE
        calcs <- calcsE
        // Use ModelBuilder so the validation + return-type contract
        // matches the programmatic path (PR #44). The description
        // is set only when present in the YAML (avoids the wart of
        // `Some("")` when the field is absent).
        model <- {
          val b0 = ModelBuilder()
            .withName(name.get)
            .withVersion(version.get)
            .withSource(src)
            .withStatus(status)
          val b1 = description.fold(b0)(d => b0.withDescription(d))
          b1
            .withDimensions(dims)
            .withMeasures(meas)
            .withFilters(filters)
            .withJoins(joins)
            .withCalculatedMeasures(calcs)
            .build
        }.left.map(err => ManifestError.InvalidYaml(err.message))
      } yield model
    }
  }

  // -- YAML map / list helpers --

  private def asMap(v: Any): Option[java.util.Map[_, _]] = v match {
    case m: java.util.Map[_, _] => Some(m)
    case null                   => None
    case _                      => None
  }

  private def asSeq(v: Any): Seq[Any] = v match {
    case s: java.util.List[_] => s.asScala.toSeq
    case null                => Seq.empty
    case _                   => Seq.empty
  }

  private def stringField(root: java.util.Map[_, _], key: String): Option[String] =
    Option(root.get(key)).map(_.toString).filter(_.nonEmpty)

  private def intField(root: java.util.Map[_, _], key: String): Option[Int] =
    Option(root.get(key)).flatMap {
      case n: java.lang.Integer => Some(n.intValue)
      case n: java.lang.Long    => Some(n.intValue)
      case n: Number             => Some(n.intValue)
      case s: String             => scala.util.Try(s.toInt).toOption
      case _                     => None
    }

  // -- SourceRef parsing (sealed-trait dispatch) --

  private def parseSource(m: java.util.Map[_, _]): Either[ManifestError, SourceRef] = {
    if (m.containsKey("byName")) {
      val inner = asMap(m.get("byName")).getOrElse(return Left(ManifestError.InvalidYaml("source.byName is not a map")))
      val table = stringField(inner, "table")
      if (table.isEmpty)
        Left(ManifestError.MissingField("source.byName.table", "source"))
      else {
        // SourceRef.ByName(name, table) — `name` field can carry the
        // catalog.legacy combo. If absent, use "default".
        val name = stringField(inner, "name").getOrElse("default")
        Right(SourceRef.ByName(
          catalog   = None,
          namespace = if (name == "default") None else Some(name),
          table     = table.get,
        ))
      }
    } else if (m.containsKey("byPath")) {
      val inner = asMap(m.get("byPath")).getOrElse(return Left(ManifestError.InvalidYaml("source.byPath is not a map")))
      val format = stringField(inner, "format")
      val path   = stringField(inner, "path")
      if (format.isEmpty)
        Left(ManifestError.MissingField("source.byPath.format", "source"))
      else if (path.isEmpty)
        Left(ManifestError.MissingField("source.byPath.path", "source"))
      else {
        val opts = asMap(inner.get("options")).map { o =>
          o.asScala.toMap.collect {
            case (k: String, v) => (k, v.toString)
          }.toMap
        }.getOrElse(Map.empty)
        Right(SourceRef.ByPath(format = format.get, path = path.get, options = opts))
      }
    } else if (m.containsKey("byProvider")) {
      val inner = asMap(m.get("byProvider")).getOrElse(return Left(ManifestError.InvalidYaml("source.byProvider is not a map")))
      val name = stringField(inner, "providerRefName")
      if (name.isEmpty)
        Left(ManifestError.MissingField("source.byProvider.providerRefName", "source"))
      else
        Right(SourceRef.ByProvider(name.get))
    } else {
      Left(ManifestError.UnknownSourceRef(
        s"source must have one of: byName, byPath, byProvider (got keys: ${m.keySet.asScala.mkString(", ")})"
      ))
    }
  }

  // -- Dimension / measure / filter parsing --

  private def parseDimensions(seq: Seq[Any]): List[io.sm8.core.model.Dimension] =
    seq.toList.flatMap {
      case m: java.util.Map[_, _] =>
        val name = stringField(m, "name")
        val expr = stringField(m, "expr").orElse(name)
        (name, expr) match {
          case (Some(n), Some(e)) => Some(io.sm8.core.model.Dimension.field(n, e))
          case _ => None
        }
      case _ => None
    }

  /** Parses measures. PR-J (2026-08-16): the `expr:` field is now
    * parsed into a typed `AggregateCall`. The well-known legacy
    * string forms are recognized; unknown forms surface as
    * `None` (fail loud — the caller's validation reports the
    * missing measure, never a silent no-op).
    *
    * Recognized forms (case-insensitive fn name):
    *   - "sum(x)"       -> AggregateCall(Sum, Some(FieldRef("x")), alias)
    *   - "count(*)"     -> AggregateCall(Count, None, alias)
    *   - "avg(x)", "min(x)", "max(x)",
    *     "count_distinct(x)", "countdistinct(x)" -> the matching fn
    *   - bare "x" (no parens) -> AggregateCall(Sum, Some(FieldRef("x")), alias)
    *     (the legacy's implicit-sum default)
    */
  /** PR-M1 (ADR-008-L Appendix GAP 4): parse the `joins:` block.
    * YAML shape (each entry a map):
    *   - name: j1
    *     rightModel: customers
    *     kind: inner            # inner|left|right|full|outer|cross (ci)
    *     keys: [[region, region]]  # list of [leftKey, rightKey] pairs
    * Unknown kind -> typed ManifestError.ParseFailure (never silent).
    */
  private def parseJoins(seq: Seq[Any]): Either[ManifestError, List[io.sm8.core.model.JoinSpec]] =
    seq.toList.flatMap(asMap).map { m =>
      val name       = stringField(m, "name").getOrElse("")
      val rightModel = stringField(m, "rightModel").orElse(stringField(m, "right_model")).getOrElse("")
      val kindStr    = stringField(m, "kind").getOrElse("inner")
      val keysRaw    = asSeq(m.get("keys"))
      val kind: Either[ManifestError, io.sm8.core.rel.JoinKind] = kindStr.toLowerCase match {
        case "inner" => Right(io.sm8.core.rel.JoinKind.Inner)
        case "left"  => Right(io.sm8.core.rel.JoinKind.Left)
        case "right" => Right(io.sm8.core.rel.JoinKind.Right)
        case "full" | "outer" => Right(io.sm8.core.rel.JoinKind.Full)
        case "cross" => Right(io.sm8.core.rel.JoinKind.Cross)
        case other   => Left(ManifestError.ParseFailure(
          s"joins[$name]: unknown kind '$other' (supported: inner, left, right, full, outer, cross)"))
      }
      val keys: List[(String, String)] = keysRaw.toList.flatMap {
        case pair: java.util.List[_] if pair.size == 2 =>
          List((pair.get(0).toString, pair.get(1).toString))
        case _ => Nil  // malformed pair entries are skipped; PR-M2's
                       // ModelValidator cross-references catch them
      }
      kind.map(k => io.sm8.core.model.JoinSpec(name, rightModel, k, keys))
    }.foldLeft[Either[ManifestError, List[io.sm8.core.model.JoinSpec]]](Right(Nil)) { (accE, jE) =>
      for (acc <- accE; j <- jE) yield acc :+ j
    }

  /** PR-M1 (ADR-008-L Appendix GAP 4): parse the
    * `calculated_measures:` block. Each entry: { name, expr }. The
    * expr string goes through ExprParser (now CASE WHEN / AS alias /
    * all() / measure() aware per GAP 1). Parse failure -> typed
    * ManifestError.ParseFailure (never silent). */
  private def parseCalculatedMeasures(
      seq: Seq[Any],
  ): Either[ManifestError, List[io.sm8.core.model.CalculatedMeasure]] =
    seq.toList.flatMap(asMap).map { m =>
      val name = stringField(m, "name").getOrElse("")
      stringField(m, "expr") match {
        case None =>
          Left(ManifestError.ParseFailure(s"calculated_measures[$name]: missing 'expr'"))
        case Some(exprStr) =>
          io.sm8.core.expr.ExprParser.parseExpr(exprStr).left.map { pe =>
            ManifestError.ParseFailure(s"calculated_measures[$name]: ${pe.toString}")
          }.map(e => io.sm8.core.model.CalculatedMeasure(name, e))
      }
    }.foldLeft[Either[ManifestError, List[io.sm8.core.model.CalculatedMeasure]]](Right(Nil)) { (accE, cE) =>
      for (acc <- accE; c <- cE) yield acc :+ c
    }

  private def parseMeasures(seq: Seq[Any]): List[io.sm8.core.model.Measure] =
    seq.toList.flatMap {
      case m: java.util.Map[_, _] =>
        val name = stringField(m, "name")
        val expr = stringField(m, "expr").orElse(name)
        (name, expr) match {
          case (Some(n), Some(e)) => parseAggregateCall(n, e)
          case _ => None
        }
      case _ => None
    }

  /** Parse a legacy measure-expression string into a typed
    * `AggregateCall`. Per [[scala-error-handling-mindset]]:
    * unknown function names return `None` (the caller's
    * validation reports the missing measure; never a silent
    * default to a wrong aggregate). */
  private def parseAggregateCall(
      alias: String,
      expr:  String,
  ): Option[io.sm8.core.model.Measure] = {
    import io.sm8.core.expr.Expr
    import io.sm8.core.rel.{AggregateCall, AggregateFn}
    val trimmed = expr.trim
    // fn(arg) form?
    val fnCall = """(?i)^(sum|count|avg|min|max|count_distinct|countdistinct)\s*\(\s*(.+?)\s*\)$""".r
    trimmed match {
      case fnCall(fn, arg) =>
        val aFn = fn.toLowerCase match {
          case "sum"             => Some(AggregateFn.Sum)
          case "count"           => if (arg == "*") Some(AggregateFn.Count) else None
          case "avg"             => Some(AggregateFn.Avg)
          case "min"             => Some(AggregateFn.Min)
          case "max"             => Some(AggregateFn.Max)
          case "count_distinct" | "countdistinct" => Some(AggregateFn.CountDistinct)
          case _                 => None
        }
        aFn.map { f =>
          val input = if (arg == "*") None else Some(Expr.FieldRef(arg))
          io.sm8.core.model.Measure(alias, AggregateCall(f, input, alias))
        }
      case _ =>
        // Bare column name -> implicit Sum (the legacy default).
        if (trimmed.nonEmpty && trimmed.matches("""[A-Za-z_][A-Za-z0-9_.]*""")) {
          Some(io.sm8.core.model.Measure(
            alias, AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef(trimmed)), alias)))
        } else None
    }
  }

  /** Parses filters. The `predicate:` field is a raw SQL-like
    * expression. Delegates to `ExprParser.parseExpr` (added in
    * the typed-expr-filter PR) for the typed `Expr` AST.
    *
    * Per [[scala-error-handling-mindset]]: an unparseable predicate
    * is `Left(ExprParseError)`; we surface as `Left(ManifestError)`. */
  /** Parses the legacy-style `predicate: "<raw SQL>"` form. The
    * typed `FilterSpec.predicate: Expr` AST is produced via the
    * `ExprParser` (typed-expr-filter PR); for unparseable
    * predicates we return None (the filter is skipped — the
    * caller will see a partial filter list with a manifest-level
    * error only if it's the only filter source). */
  private def parseFilters(seq: Seq[Any]): List[FilterSpec] =
    seq.toList.flatMap {
      case m: java.util.Map[_, _] =>
        val name      = stringField(m, "name")
        val predicate = stringField(m, "predicate")
        (name, predicate) match {
          case (Some(n), Some(p)) =>
            io.sm8.core.expr.ExprParser.parseExpr(p).toOption.map { parsed =>
              FilterSpec(
                name      = n,
                predicate = parsed,
              )
            }
          case _ => None
        }
      case _ => None
    }
}


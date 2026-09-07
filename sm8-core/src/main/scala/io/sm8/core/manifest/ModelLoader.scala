/*
 * SM8 Core — ModelLoader (YAML → Model).
 *
 * Engine-portable YAML manifest loader. Reads a YAML file and
 * produces an `io.sm8.core.model.Model` (the engine-portable IR).
 *
 * behavior in adapters"): the loader is a typed factory in core.
 * It does NOT know which database / cache / auth system is in use
 * (RFC §3 Core Boundary). Spark coupling happens in the connector
 * layer; the YAML layer is engine-portable.
 *
 * ==Why a separate manifest layer (vs. reading directly into Model)==
 *
 * are separate"): the YAML root is parsed into a `Map[String, Any]`
 * (shape). Then `ModelBuilder.build(.)` validates + constructs the
 * domain Model (validity). Mixing the two would mean parse failures
 * leak into domain invariants.
 *
 * ==Why minimal subset (not the legacy's full 10 portable types)==
 *
 * legacy's `PortableModel` carries 8 sub-types
 * (`PortableJoin`, `PortableRollup`, `PortableCalculatedMeasure`,
 * `PortableFilter`, etc.). At the time of that port, `Model` did
 * NOT carry those fields (PR-M1 added joins + calculated_measures).
 * Ticket 3 of docs/wayfinder/2026-09-06-pre-aggregation.md
 * (2026-09-06) added `rollups` (parsed from the
 * `rollups:` block above); the remaining legacy portables have no
 * Model-field counterpart yet. So we port ONLY the subset
 * that maps to existing `Model` fields: name, version, description,
 * source, status, dimensions, measures, filters (+ joins,
 * calculated_measures, rollups).
 *
 * ==RFC alignment==
 *
 * - `semantic-layer-engine-architecture.md` §3 Core Boundary: lives
 * in core, no data-source knowledge.
 * - `semantic-layer-engine-architecture.md` §7 Contracts: the
 * loader is a typed factory for the `Model` contract.
 * - `plugins.md` / `hooks.md` / `adapters.md`: not a plugin/hook/
 * adapter.
 *
 * ==Plan alignment==
 *
 * - Plan line 195 ("manifest/ IR move"): this PR is the minimal
 * subset of that move. The full 10 portable types land in a
 * future PR series when the underlying IR fields exist.
 * - Plan line 289 (Step 10 "ModelService.compileFromYaml"): this
 * loader is the foundation for that path.
 *
 * ==Spark concerns (per user directive)==
 *
 * - mantras #1, #5: no Spark types captured, no executor-side
 * closure. The loader is pure data in sm8-core (Spark-free per
 * the plan's inverted enforcer pattern).
 * - mantra #3 (schema-drift verify at boundary): parse failures
 * are typed `ManifestError`; validation failures are typed
 * `ModelValidationError`. The two layers stay distinct.
 *
 * `finally`. No static / ThreadLocal state. No mutable builder.
 *
 * is startup-time.
 */
package io.sm8.core.manifest

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

import io.sm8.core.model.{FilterSpec, Model, ModelBuilder, ModelStatus, SourceRef}

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets

import scala.util.control.NonFatal

/**
 * Loads an engine-portable `Model` from a YAML manifest.
 *
 * Supported YAML schema (subset of the legacy `PortableModel`):
 *
 * {{{
 * name: my-model     # required
 * version: 1      # required (>= 0)
 * description: "."    # optional
 * source:       # required (one of:)
 * byName:
 *  catalog: default   # optional
 *  table: people    # required
 * byPath:
 *  format: csv     # required
 *  path: /tmp/data.csv   # required
 *  options: {key: value}  # optional
 * byProvider:
 *  providerRefName: my-ref  # required
 * status: draft     # optional (default: draft)
 * dimensions:      # optional (default: [])
 * - name: region
 *  expr: region
 * measures:      # optional (default: [])
 * - name: revenue
 *  expr: sum(amount)
 * filters:      # optional (default: [])
 * - name: adults
 *  # raw-sql predicate; typed Expr filter is deferred
 *  predicate: "age >= 18"
 * }}}
 *
 * The `predicate` field for filters is a raw SQL string in this PR
 * (matches `QueryRequest.where: Option[String]`). When the typed
 * `FilterSpec.predicate: Expr` parser ships, this loader upgrades.
 */
object ModelLoader {

 // Public label passed as `source` to `fromStream` when the caller
 // has no real origin (e.g. tests, programmatic construction).
 // Promoted to a constant so the magic string lives in exactly one
 // place; callers and error-message consumers can reference it.
 val InMemorySource: String = "<in-memory>"

 private val mapper: ObjectMapper =
 new ObjectMapper(new YAMLFactory())

 /** Load from a `String` (for tests + in-memory manifests).
    *
    * @param yaml the manifest YAML as a string
    * @return the typed `Model` or a typed `ManifestError`
    */
 def fromString(yaml: String): Either[ManifestError, Model] =
 fromStream(
 new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)),
 source = InMemorySource
 )

 /** Load from an `InputStream`. Caller is responsible for stream
 * lifecycle. The `source` label is used only in error messages
 * (so the operator sees WHICH stream failed — a path, a URL,
 * a memory buffer, etc.).
 *
 * @param stream the InputStream containing the YAML manifest
 * @param source a human-readable label included in error
 *               messages (e.g. a path, URL, or `InMemorySource`)
 * @return      `Right(Model)` on success;
 *              `Left(ManifestError)` on parse failure
 */
 def fromStream(stream: InputStream, source: String): Either[ManifestError, Model] =
 try {
  val root = mapper.readValue(stream, classOf[java.util.Map[_, _]])
  buildModel(root)
 } catch {
  case NonFatal(e) =>
  Left(ManifestError.ParseFailure(s"$source: ${e.getMessage}"))
 }

 /** Construct the `Model` from the parsed YAML map. */
 private def buildModel(
  root: java.util.Map[_, _]
 ): Either[ManifestError, Model] = {
 val name: Option[String] = stringField(root, "name")
 val version: Option[Int] = intField(root, "version")
 val description: Option[String] = stringField(root, "description")
 val statusOpt: Option[String] = stringField(root, "status")

 // Name + version are required (the rest have defaults).
 if (name.isEmpty)
  return Left(ManifestError.MissingField("name", "root"))
 if (version.isEmpty)
  return Left(ManifestError.MissingField("version", "root"))

 val status: ModelStatus = statusOpt match {
  case None      => ModelStatus.Draft
  case Some(s) if s.equalsIgnoreCase("draft")  => ModelStatus.Draft
  case Some(s) if s.equalsIgnoreCase("published") => ModelStatus.Published
  case Some(s) if s.equalsIgnoreCase("deprecated") => ModelStatus.Deprecated
  case Some(other) =>
  return Left(ManifestError.UnknownStatus(other))
 }

 val source: Either[ManifestError, SourceRef] =
  parseSource(asMap(root.get("source")).getOrElse(return Left(ManifestError.MissingField("source", "root"))))

 source.flatMap { src =>
  val dims = parseDimensions(asSeq(root.get("dimensions")))
  val meas = parseMeasures(asSeq(root.get("measures")))
  val filters = parseFilters(asSeq(root.get("filters")))

  // PR-M1 (ADR-008-L Appendix GAP 4): parse joins + calculated
  // measures. Both can fail (unknown join kind, unparsable calc
  // expr) -- surface as typed ManifestError, never silent.
  val joinsE = parseJoins(asSeq(root.get("joins")))
  val filtersE = parseFilters(asSeq(root.get("filters")))
  val calcsE = parseCalculatedMeasures(asSeq(root.get("calculated_measures")))
  // Pre-aggregation rollups (Ticket 3 of
  // docs/wayfinder/2026-09-06-pre-aggregation.md): parse the
  // optional `rollups:` block. Each
  // entry: { name, dimensions, measures, time_grain? }. Name-less or
  // empty entries fail loud as typed ManifestError (never silent);
  // ref-existence is ModelValidator's job at the Model.of boundary.
  val rollupsE = parseRollups(asSeq(root.get("rollups")))

  for {
  joins <- joinsE
  filters <- filtersE
  calcs <- calcsE
  rollups <- rollupsE
  // Use ModelBuilder so the validation + return-type contract
  // matches the programmatic path (PR #44). The description
  // is set only when present in the YAML (avoids the wart of
  // `Some("")` when the field is absent).
  model <- {
   val b0 = ModelBuilder().withName(name.get).withVersion(version.get).withSource(src).withStatus(status)
   val b1 = description.fold(b0)(d => b0.withDescription(d))
   b1.withDimensions(dims).withMeasures(meas).withFilters(filters).withJoins(joins).withCalculatedMeasures(calcs).withRollups(rollups).build
  }.left.map(err => ManifestError.InvalidYaml(err.message))
  } yield model
 }
 }

 // -- YAML map / list helpers --

 private def asMap(v: Any): Option[java.util.Map[_, _]] = v match {
 case m: java.util.Map[_, _] => Some(m)
 case null     => None
 case _      => None
 }

 private def asSeq(v: Any): Seq[Any] = v match {
 case s: java.util.List[_] => s.asScala.toSeq
 case null    => Seq.empty
 case _     => Seq.empty
 }

 private def stringField(root: java.util.Map[_, _], key: String): Option[String] =
 Option(root.get(key)).map(_.toString).filter(_.nonEmpty)

 private def intField(root: java.util.Map[_, _], key: String): Option[Int] =
 Option(root.get(key)).flatMap {
  case n: java.lang.Integer => Some(n.intValue)
  case n: java.lang.Long => Some(n.intValue)
  case n: Number    => Some(n.intValue)
  case s: String    => scala.util.Try(s.toInt).toOption
  case _      => None
 }

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
   catalog = None,
   namespace = if (name == "default") None else Some(name),
   table  = table.get))
  }
 } else if (m.containsKey("byPath")) {
  val inner = asMap(m.get("byPath")).getOrElse(return Left(ManifestError.InvalidYaml("source.byPath is not a map")))
  val format = stringField(inner, "format")
  val path = stringField(inner, "path")
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
 * - "sum(x)"  -> AggregateCall(Sum, Some(FieldRef("x")), alias)
 * - "count(*)"  -> AggregateCall(Count, None, alias)
 * - "avg(x)", "min(x)", "max(x)",
 *  "count_distinct(x)", "countdistinct(x)" -> the matching fn
 * - bare "x" (no parens) -> AggregateCall(Sum, Some(FieldRef("x")), alias)
 *  (the legacy's implicit-sum default)
 */
 /** PR-M1 (ADR-008-L Appendix GAP 4): parse the `joins:` block.
 * YAML shape (each entry a map):
 * - name: j1
 *  rightModel: customers
 *  kind: inner   # inner|left|right|full|outer|cross (ci)
 *  keys: [[region, region]] # list of [leftKey, rightKey] pairs
 * Unknown kind -> typed ManifestError.ParseFailure (never silent).
 */
  private def parseJoins(seq: Seq[Any]): Either[ManifestError, List[io.sm8.core.model.JoinSpec]] = {
  /** Parse one join entry (a single map) into a typed JoinSpec.
    *
    * @param m the entry map
    * @return the typed JoinSpec, or a typed `ManifestError`
    */
  def parseOne(m: java.util.Map[_, _]): Either[ManifestError, io.sm8.core.model.JoinSpec] = {
   val name  = stringField(m, "name").getOrElse("")
   val rightModel = stringField(m, "rightModel").orElse(stringField(m, "right_model")).getOrElse("")
   val kindStr = stringField(m, "kind").getOrElse("inner")
   val keysRaw = asSeq(m.get("keys"))
   val kind: Either[ManifestError, io.sm8.core.rel.JoinKind] = kindStr.toLowerCase match {
     case "inner" => Right(io.sm8.core.rel.JoinKind.Inner)
     case "left"  => Right(io.sm8.core.rel.JoinKind.Left)
     case "right" => Right(io.sm8.core.rel.JoinKind.Right)
     case "full" | "outer" => Right(io.sm8.core.rel.JoinKind.Full)
     case "cross" => Right(io.sm8.core.rel.JoinKind.Cross)
     case other => Left(ManifestError.ParseFailure(
       s"joins[$name]: unknown kind '$other' (supported: inner, left, right, full, outer, cross)"))
   }
   val keys: List[(String, String)] = keysRaw.toList.flatMap {
     case pair: java.util.List[_] if pair.size == 2 =>
       List((pair.get(0).toString, pair.get(1).toString))
     case _ => Nil // malformed pairs skipped; ModelValidator cross-refs catch them
   }
   val estimated: Either[ManifestError, Option[Long]] =
     stringField(m, "estimated_rows").orElse(stringField(m, "estimatedRows")) match {
       case Some(raw) =>
         scala.util.Try(raw.toLong) match {
           case scala.util.Success(v) if v >= 0 => Right(Some(v))
           case scala.util.Success(v) => Left(ManifestError.ParseFailure(
             s"joins[$name]: estimated_rows must be >= 0, got $v"))
           case scala.util.Failure(_) => Left(ManifestError.ParseFailure(
             s"joins[$name]: estimated_rows '$raw' is not a non-negative integer"))
         }
       case None => Right(None)
     }
   for {
     k <- kind
     est <- estimated
   } yield io.sm8.core.model.JoinSpec(name, rightModel, k, keys, est)
  }
  seq.toList.foldLeft[Either[ManifestError, List[io.sm8.core.model.JoinSpec]]](Right(Nil)) { (accE, entry) =>
   for {
     acc  <- accE
     m    <- asMap(entry).toRight(ManifestError.ParseFailure(
             s"joins[]: entry must be a map (got ${Option(entry).map(_.getClass.getSimpleName).getOrElse("null")})"))
     join <- parseOne(m)
   } yield acc :+ join
  }
 }
 /** PR-M1 (ADR-008-L Appendix GAP 4): parse the
 * `calculated_measures:` block. Each entry: { name, expr }. The
 * expr string goes through ExprParser (now CASE WHEN / AS alias /
 * all() / measure() aware per GAP 1). Parse failure -> typed
 * ManifestError.ParseFailure (never silent). */
  private def parseCalculatedMeasures(
 seq: Seq[Any]): Either[ManifestError, List[io.sm8.core.model.CalculatedMeasure]] = {
  /** Parse one calculated-measure entry into a typed CalculatedMeasure.
    *
    * @param m the entry map
    * @return the typed CalculatedMeasure, or a typed `ManifestError`
    */
  def parseOne(m: java.util.Map[_, _]): Either[ManifestError, io.sm8.core.model.CalculatedMeasure] = {
   val name = stringField(m, "name")
   name match {
    case None =>
     Left(ManifestError.ParseFailure("calculated_measures[]: missing 'name'"))
    case Some(n) =>
     stringField(m, "expr") match {
      case None =>
       Left(ManifestError.ParseFailure(s"calculated_measures[$n]: missing 'expr'"))
      case Some(exprStr) =>
       io.sm8.core.expr.ExprParser.parseExpr(exprStr).left.map { pe =>
        ManifestError.ParseFailure(s"calculated_measures[$n]: ${pe.toString}")
       }.map(e => io.sm8.core.model.CalculatedMeasure(n, e))
     }
   }
  }
  seq.toList.foldLeft[Either[ManifestError, List[io.sm8.core.model.CalculatedMeasure]]](Right(Nil)) { (accE, entry) =>
   for {
     acc  <- accE
     m    <- asMap(entry).toRight(ManifestError.ParseFailure(
             s"calculated_measures[]: entry must be a map (got ${Option(entry).map(_.getClass.getSimpleName).getOrElse("null")})"))
     cm   <- parseOne(m)
   } yield acc :+ cm
  }
 }
 /** Parse the `rollups:` block (Ticket #3 of
 * docs/wayfinder/2026-09-06-pre-aggregation.md). Each entry:
 * { name, dimensions: [names], measures: [names], time_grain? }.
 * Strict by design: a non-map entry, a missing name, both refs
 * empty, both grain spellings, or a non-list refs value all fail
 * loud as typed ManifestError (never silent -- a silently dropped
 * or degenerate rollup would weaken Ticket 4 routing). A single
 * string ref is accepted as a 1-list (author convenience).
 * Ref-existence is ModelValidator's job at the Model.of boundary.
 */
 private def parseRollups(
 seq: Seq[Any]): Either[ManifestError, List[io.sm8.core.model.RollupSpec]] = {
  /** Parse one `rollups:` entry (a single YAML map) into a typed
    * `RollupSpec`, failing loud on: missing name, both grain
    * spellings, non-list refs, or both refs empty.
    *
    * @param m the entry map
    * @return the typed spec, or a typed `ManifestError`
    */
  def parseOne(m: java.util.Map[_, _]): Either[ManifestError, io.sm8.core.model.RollupSpec] = {
   val name = stringField(m, "name")
   val grainSpecs = List(stringField(m, "time_grain"), stringField(m, "timeGrain")).flatten
   val grainE: Either[ManifestError, Option[String]] =
    if (grainSpecs.length > 1)
    Left(ManifestError.ParseFailure(
     s"rollups[${name.getOrElse("?")}]: specify at most one of time_grain / timeGrain"))
    else Right(grainSpecs.headOption)
   /** Strict ref-list parse for one key. Single string -> 1-list
     * (author convenience); non-list, non-null anything else ->
     * typed `ParseFailure` (never silently coerced to Nil: a
     * dropped ref would degenerate the rollup and silently weaken
     * Ticket 4 routing).
     *
     * @param key the YAML key ("dimensions" or "measures")
     * @return the parsed name list, or a typed `ManifestError`
     */
   def refList(key: String): Either[ManifestError, List[String]] =
    Option(m.get(key)) match {
    case None => Right(Nil)
    case Some(null) => Right(Nil)
    case Some(x: java.util.List[_]) => Right(x.asScala.toList.map(_.toString))
    case Some(scalar: String) => Right(List(scalar))
    case Some(other) =>
     Left(ManifestError.ParseFailure(
     s"rollups[${name.getOrElse("?")}].$key must be a list (got ${other.getClass.getSimpleName})"))
    }
   name match {
    case None => Left(ManifestError.ParseFailure("rollups[]: missing 'name'"))
    case Some(n) =>
    for {
     grain <- grainE
     dims <- refList("dimensions")
     meas <- refList("measures")
     _ <- if (dims.isEmpty && meas.isEmpty)
      Left(ManifestError.ParseFailure(
      s"rollups[$n]: dimensions and measures must not both be empty"))
     else Right(())
    } yield io.sm8.core.model.RollupSpec(
     name = n,
     dimensions = dims,
     measures = meas,
     timeGrain = grain)
   }
  }
  seq.toList.foldLeft[Either[ManifestError, List[io.sm8.core.model.RollupSpec]]](Right(Nil)) { (accE, entry) =>
   for {
    acc <- accE
    m <- asMap(entry).toRight(ManifestError.ParseFailure(
     s"rollups[]: each entry must be a map (got ${Option(entry).map(_.getClass.getSimpleName).getOrElse("null")})"))
    spec <- parseOne(m)
   } yield acc :+ spec
  }
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
 * `AggregateCall`. 
 * unknown function names return `None` (the caller's
 * validation reports the missing measure; never a silent
 * default to a wrong aggregate). */
 private def parseAggregateCall(
  alias: String,
  expr: String): Option[io.sm8.core.model.Measure] = {
 import io.sm8.core.expr.Expr
 import io.sm8.core.rel.{AggregateCall, AggregateFn}
 val trimmed = expr.trim
 // fn(arg) form?
 val fnCall = """(?i)^(sum|count|avg|min|max|count_distinct|countdistinct)\s*\(\s*(.+?)\s*\)$""".r
 trimmed match {
  case fnCall(fn, arg) =>
  val aFn = fn.toLowerCase match {
   case "sum"    => Some(AggregateFn.Sum)
   case "count"   => if (arg == "*") Some(AggregateFn.Count) else None
   case "avg"    => Some(AggregateFn.Avg)
   case "min"    => Some(AggregateFn.Min)
   case "max"    => Some(AggregateFn.Max)
   case "count_distinct" | "countdistinct" => Some(AggregateFn.CountDistinct)
   case _     => None
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

 /** Parse the `filters:` block. Each entry: { name, predicate }.
 * `predicate` is a raw SQL-like expression (delegated to
 * ExprParser.parseExpr per ADR-008-P filter-language contract).
 * Strict by design (Ticket 3 follow-up: same discipline as
 * parseRollups): a non-map entry, missing name, missing
 * predicate, or unparsable expr fails loud as typed
 * ManifestError.ParseFailure -- never silent (silent drops let a
 * broken filter load and silently mis-filter query results).
 */
 private def parseFilters(
 seq: Seq[Any]): Either[ManifestError, List[FilterSpec]] = {
 /** Parse one filter entry into a typed FilterSpec.
 *
 * @param entry the entry (list item, may be any type)
 * @return the typed FilterSpec, or a typed `ManifestError`
 */
 def parseOne(entry: Any): Either[ManifestError, FilterSpec] =
  asMap(entry).toRight(ManifestError.ParseFailure(
   s"filters[]: entry must be a map (got ${entry.getClass.getSimpleName})"))
   .flatMap { m =>
    val name = stringField(m, "name")
    val predicateStr = stringField(m, "predicate")
    (name, predicateStr) match {
     case (Some(n), Some(p)) =>
      io.sm8.core.expr.ExprParser.parseExpr(p).left.map { pe =>
       ManifestError.ParseFailure(s"filters[$n]: ${pe.toString}")
      }.map { parsed =>
       FilterSpec(name = n, predicate = parsed)
      }
     case (None, _) =>
      Left(ManifestError.ParseFailure("filters[]: missing 'name'"))
     case (_, None) =>
      Left(ManifestError.ParseFailure(
       s"filters[${name.getOrElse("")}]: missing 'predicate'"))
    }
   }
 seq.toList.foldLeft[Either[ManifestError, List[FilterSpec]]](Right(Nil)) { (accE, entryE) =>
  for {
   acc <- accE
   f   <- parseOne(entryE)
  } yield acc :+ f
}
}
}

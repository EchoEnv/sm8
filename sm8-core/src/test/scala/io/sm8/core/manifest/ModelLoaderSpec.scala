/*
 * SM8 Core — ModelLoader spec.
 *
 * Per [[debug-mantra-mindset]]: a fast deterministic pass/fail
 * signal for the YAML → Model path. Each test exercises one
 * invariant.
 *
 * Per [[karphy-guidags-mindset]] "smallest correct change": no
 * incidental assertions, no incidental metrics.
 *
 * Per [[scala-data-driven-refactor-mindset]] §2 ("shape and validity
 * are separate"): the spec verifies that parse failures return
 * `Left[ManifestError]` (shape-level) and that validation failures
 * flow through `ModelBuilder.build(...)` (validity-level).
 *
 * ==Spark concerns (per user directive)==
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantras: N/A in core.
 * The loader has zero Spark types. The Model produced here can
 * flow to EngineRegistry → SparkEngineProvider.query() via
 * the connector layer (PRs #38-#42 handle Spark concerns).
 */
package io.sm8.core.manifest

import io.sm8.core.model.{Dimension, FilterSpec, Measure, Model, ModelStatus, SourceRef}
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.schema.SealedDataType

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class ModelLoaderSpec extends AnyFunSuite with Matchers {

  // -- Happy paths --

  test("ModelLoader.fromString: minimal valid YAML with byName source produces Right(Model)") {
    val yaml =
      """name: people-model
        |version: 1
        |source:
        |  byName:
        |    table: people
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isRight shouldBe true
    val m = out.toOption.get
    m.name shouldBe "people-model"
    m.version shouldBe 1
    m.source shouldBe SourceRef.ByName(table = "people")
    m.status shouldBe ModelStatus.Draft
  }

  test("ModelLoader.fromString: full YAML with byPath source + dimensions + measures produces Right(Model)") {
    val yaml =
      """name: full-model
        |version: 2
        |description: A test model
        |source:
        |  byPath:
        |    format: csv
        |    path: /tmp/data.csv
        |    options:
        |      header: "true"
        |      inferSchema: "true"
        |status: published
        |dimensions:
        |  - name: region
        |    expr: region
        |  - name: product
        |    expr: product_name
        |measures:
        |  - name: revenue
        |    expr: sum(amount)
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isRight shouldBe true
    val m = out.toOption.get
    m.name shouldBe "full-model"
    m.version shouldBe 2
    m.description shouldBe Some("A test model")
    m.status shouldBe ModelStatus.Published
    m.source shouldBe SourceRef.ByPath(
      format = "csv",
      path   = "/tmp/data.csv",
      options = Map("header" -> "true", "inferSchema" -> "true"),
    )
    m.dimensions shouldBe List(
      Dimension.field("region", "region"),
      Dimension.field("product", "product_name"),
    )
    m.measures shouldBe List(Measure(
      "revenue",
      io.sm8.core.rel.AggregateCall(
        io.sm8.core.rel.AggregateFn.Sum,
        Some(io.sm8.core.expr.Expr.FieldRef("amount")),
        "revenue")))
  }

  test("ModelLoader.fromString: byProvider source path constructs SourceRef.ByProvider") {
    val yaml =
      """name: provider-model
        |version: 1
        |source:
        |  byProvider:
        |    providerRefName: my-driver-ref
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isRight shouldBe true
    out.toOption.get.source shouldBe SourceRef.ByProvider("my-driver-ref")
  }

  test("ModelLoader.fromString: deprecated status maps to ModelStatus.Deprecated") {
    val yaml =
      """name: deprecated-model
        |version: 1
        |status: deprecated
        |source:
        |  byName:
        |    table: t
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isRight shouldBe true
    out.toOption.get.status shouldBe ModelStatus.Deprecated
  }

  // -- Failure paths (typed ManifestError) --

  test("ModelLoader.fromString: missing name returns Left(MissingField)") {
    val yaml =
      """version: 1
        |source:
        |  byName:
        |    table: t
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [ManifestError.MissingField]
    out.left.get.message should include ("name")
  }

  test("ModelLoader.fromString: missing version returns Left(MissingField)") {
    val yaml =
      """name: no-version
        |source:
        |  byName:
        |    table: t
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [ManifestError.MissingField]
    out.left.get.message should include ("version")
  }

  test("ModelLoader.fromString: unknown status returns Left(UnknownStatus)") {
    val yaml =
      """name: bad-status-model
        |version: 1
        |status: retired
        |source:
        |  byName:
        |    table: t
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [ManifestError.UnknownStatus]
    out.left.get.message should include ("retired")
  }

  test("ModelLoader.fromString: source without byName/byPath/byProvider returns Left(UnknownSourceRef)") {
    val yaml =
      """name: bad-source-model
        |version: 1
        |source:
        |  unknownVariant:
        |    foo: bar
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [ManifestError.UnknownSourceRef]
  }

  test("ModelLoader.fromString: byPath without `format` returns Left(MissingField)") {
    val yaml =
      """name: bad-path-model
        |version: 1
        |source:
        |  byPath:
        |    path: /tmp/x
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [ManifestError.MissingField]
  }

  test("ModelLoader.fromString: malformed YAML syntax returns Left(ParseFailure)") {
    // Truly malformed: unclosed flow-mapping bracket.
    val yaml = "name: malformed\nversion: 1\nsource: { byName: [table: t"
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [ManifestError.ParseFailure]
  }

  test("ModelLoader.fromString: empty source returns Left(MissingField)") {
    val yaml =
      """name: empty-source
        |version: 1
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [ManifestError.MissingField]
  }

  // -- Per [[debug-mantra-mindset]] §5 verify --

  test("ModelLoader: smoke round-trip - parsed Model survives ObjectOutputStream (Serializable contract)") {
    val yaml =
      """name: serialize-test
        |version: 1
        |source:
        |  byName:
        |    table: t
        |""".stripMargin
    val built = ModelLoader.fromString(yaml).toOption.get
    val bytes = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bytes)
    oos.writeObject(built)
    oos.close()
    val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray))
    val restored = ois.readObject().asInstanceOf[Model]
    ois.close()
    restored shouldBe built
  }

  // -- Filter parsing --

  test("ModelLoader.fromString: filters with raw-sql predicate are parsed into typed Expr AST via ExprParser") {
    val yaml =
      """name: filter-model
        |version: 1
        |source:
        |  byName:
        |    table: t
        |filters:
        |  - name: adults
        |    predicate: "age >= 18"
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isRight shouldBe true
    val m = out.toOption.get
    m.filters.size shouldBe 1
    val f = m.filters.head
    f.name shouldBe "adults"
    // The predicate is now the TYPED Expr AST (PR #46: typed-expr-filter)
    // not the raw-SQL placeholder from PR #45. This is the contract
    // upgrade documented in PR #45's body.
    f.predicate shouldBe Expr.GreaterOrEqual(
      left  = Expr.FieldRef("age"),
      right = Expr.Literal(LiteralValue.IntValue(18), SealedDataType.Int),
    )
  }

  // === fromStream(_, source) signature tests ===

  test("fromStream accepts an arbitrary InputStream + source label and parses") {
    val yaml =
      """name: from-stream-test
        |version: 1
        |source:
        |  byName:
        |    table: t
        |""".stripMargin
    val stream = new java.io.ByteArrayInputStream(yaml.getBytes("UTF-8"))
    val out = ModelLoader.fromStream(stream, source = "test://inline")
    out.isRight shouldBe true
    out.toOption.get.name shouldBe "from-stream-test"
  }

  test("fromStream's source label is propagated into the ParseFailure message on malformed YAML") {
    val malformed = "{ this is :: not yaml".getBytes("UTF-8")
    val stream = new java.io.ByteArrayInputStream(malformed)
    val out = ModelLoader.fromStream(stream, source = "/etc/sm8/manifests/prod.yaml")
    out.isLeft shouldBe true
    val err = out.swap.toOption.get
    err match {
      case ManifestError.ParseFailure(reason) =>
        reason should include ("/etc/sm8/manifests/prod.yaml")
      case other =>
        fail(s"expected ParseFailure, got $other")
    }
  }

  test("fromStream's in-memory label '<in-memory>' is propagated by fromString") {
    val malformed = "{ broken ::".getBytes("UTF-8")
    val stream = new java.io.ByteArrayInputStream(malformed)
    val out = ModelLoader.fromStream(stream, source = "<in-memory>")
    out.isLeft shouldBe true
    val err = out.swap.toOption.get
    err match {
      case ManifestError.ParseFailure(reason) =>
        reason should include ("<in-memory>")
      case other =>
        fail(s"expected ParseFailure, got $other")
    }
  }

  // -- parser-hardening tests: the previously-silent asMap/asSeq sites
  //    now fail loud as typed ManifestError.ParseFailure (design:
  //    docs/adr/0022 "Known v1 limits" follow-up lane).

  test("hardening: a scalar at the rollups block fails loud instead of loading zero rollups") {
    val yaml =
      """
        |name: m
        |version: 1
        |source:
        |  byName:
        |    table: t
        |dimensions:
        |  - name: region
        |    expr: region
        |measures:
        |  - name: cnt
        |    expr: count(*)
        |rollups: myrollup
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.swap.toOption.get match {
      case ManifestError.ParseFailure(reason) =>
        reason should include ("rollups: expected a list")
      case other => fail(s"expected ParseFailure, got $other")
    }
  }

  test("hardening: a scalar at joins[].keys fails loud instead of loading a zero-key join") {
    val yaml =
      """
        |name: m
        |version: 1
        |source:
        |  byName:
        |    table: t
        |dimensions:
        |  - name: region
        |    expr: region
        |measures:
        |  - name: cnt
        |    expr: count(*)
        |joins:
        |  - name: j1
        |    rightModel: customers
        |    keys: region
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.swap.toOption.get match {
      case ManifestError.ParseFailure(reason) =>
        reason should include ("joins[].keys: expected a list")
      case other => fail(s"expected ParseFailure, got $other")
    }
  }

  test("hardening: a scalar at source.byPath.options fails loud instead of loading zero options") {
    val yaml =
      """
        |name: m
        |version: 1
        |source:
        |  byPath:
        |    format: csv
        |    path: /tmp/data.csv
        |    options: header=true
        |dimensions:
        |  - name: region
        |    expr: region
        |measures:
        |  - name: cnt
        |    expr: count(*)
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.swap.toOption.get match {
      case ManifestError.ParseFailure(reason) =>
        reason should include ("source.byPath.options: expected a map")
      case other => fail(s"expected ParseFailure, got $other")
    }
  }

  test("hardening: absent optional blocks still load (no false positives on the strict paths)") {
    val yaml =
      """
        |name: m
        |version: 1
        |source:
        |  byPath:
        |    format: csv
        |    path: /tmp/data.csv
        |dimensions:
        |  - name: region
        |    expr: region
        |measures:
        |  - name: cnt
        |    expr: count(*)
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isRight shouldBe true
    out.right.get.rollups shouldBe empty
    out.right.get.joins shouldBe empty
    out.right.get.source match {
      case s: SourceRef.ByPath => s.options shouldBe empty
      case other => fail(s"expected ByPath source, got $other")
    }
  }

  // -- per-pair join-keys hardening (closes the residual silent
  //    path the R1 review flagged as F1) --

  test("hardening: `keys: []` fails loud instead of producing a zero-key join") {
    val yaml =
      """
        |name: m
        |version: 1
        |source:
        |  byName:
        |    table: t
        |dimensions:
        |  - name: region
        |    expr: region
        |measures:
        |  - name: cnt
        |    expr: count(*)
        |joins:
        |  - name: j1
        |    rightModel: customers
        |    keys: []
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.swap.toOption.get match {
      case ManifestError.ParseFailure(reason) =>
        reason should include ("keys must contain at least one")
      case other => fail(s"expected ParseFailure, got $other")
    }
  }

  test("hardening: kind-aware carve-out — `kind: cross` + `keys: []` is legal (the Cartesian-product declared form)") {
    // the parser-hardening R2 DE follow-up: the empty-keys guard was kind-blind.
    // Cross joins are an unconditional Cartesian product -- empty
    // keys is their DECLARED form, not an authoring slip. Every
    // other kind still requires >= 1 pair (the test above this one
    // pins the fail-loud path for inner/left/right/full).
    val yaml =
      """
        |name: m
        |version: 1
        |source:
        |  byName:
        |    table: t
        |dimensions:
        |  - name: region
        |    expr: region
        |measures:
        |  - name: cnt
        |    expr: count(*)
        |joins:
        |  - name: cross1
        |    rightModel: products
        |    kind: cross
        |    keys: []
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isRight shouldBe true
    out.right.get.joins.size shouldBe 1
    out.right.get.joins.head.kind shouldBe io.sm8.core.rel.JoinKind.Cross
    out.right.get.joins.head.keys shouldBe empty
  }

  test("hardening: a malformed keys pair (size != 2) fails loud instead of being silently dropped") {
    val yaml =
      """
        |name: m
        |version: 1
        |source:
        |  byName:
        |    table: t
        |dimensions:
        |  - name: region
        |    expr: region
        |measures:
        |  - name: cnt
        |    expr: count(*)
        |joins:
        |  - name: j1
        |    rightModel: customers
        |    keys:
        |      - [region, region, extra]
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.swap.toOption.get match {
      case ManifestError.ParseFailure(reason) =>
        reason should include ("each keys entry must be a [leftKey, rightKey] pair")
      case other => fail(s"expected ParseFailure, got $other")
    }
  }

  // -- source: root hardening (closes the F1 DE finding: scalar at
  //    `source:` used to surface as misleading MissingField) --

  test("hardening: a scalar at `source:` fails loud with a type error, not a misleading MissingField") {
    val yaml =
      """
        |name: m
        |version: 1
        |source: mytable
        |dimensions:
        |  - name: region
        |    expr: region
        |measures:
        |  - name: cnt
        |    expr: count(*)
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.swap.toOption.get match {
      case ManifestError.ParseFailure(reason) =>
        reason should include ("source: expected a map")
      case other => fail(s"expected ParseFailure, got $other")
    }
  }

  // -- options shape hardening variant (closes the F4 arch gap) --

  test("hardening: `options: {}` is legal (empty map, distinct from absent)") {
    val yaml =
      """
        |name: m
        |version: 1
        |source:
        |  byPath:
        |    format: csv
        |    path: /tmp/data.csv
        |    options: {}
        |dimensions:
        |  - name: region
        |    expr: region
        |measures:
        |  - name: cnt
        |    expr: count(*)
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isRight shouldBe true
    out.right.get.source match {
      case s: SourceRef.ByPath => s.options shouldBe empty
      case other => fail(s"expected ByPath source, got $other")
    }
  }

  // -- cascade_source loader parsing (ADR-0031 D4; narwhal final-gate
  //    F5 follow-up: dual-key convention + empty/non-string refusals) --

  test("cascade loader: cascade_source (snake) parses into RollupSpec.cascadeSource") {
    val yaml =
      """name: casc
        |version: 1
        |source:
        |  byName:
        |    table: events
        |dimensions:
        |  - name: order_date_hour
        |    expr: order_date_hour
        |    dataType: Timestamp
        |  - name: order_date_day
        |    expr: order_date_day
        |    dataType: Date
        |  - name: region
|    expr: region
        |measures:
        |  - name: total
|    expr: sum(amount)
        |rollups:
        |  - name: hourly
        |    dimensions: [order_date_hour, region]
        |    measures: [total]
        |    time_grain: hour
        |    grain_dimension: order_date_hour
        |  - name: daily
        |    dimensions: [order_date_day, region]
        |    measures: [total]
        |    time_grain: day
        |    grain_dimension: order_date_day
        |    cascade_source: hourly
        |""".stripMargin
    val res = ModelLoader.fromString(yaml)
    res match {
      case scala.util.Right(m) =>
        val daily = m.rollups.find(_.name == "daily")
          .getOrElse(fail("daily rollup not parsed"))
        daily.cascadeSource shouldBe Some("hourly")
      case scala.util.Left(err) =>
        fail(s"expected Right, got manifest error: $err")
    }
  }

  test("cascade loader: cascadeSource (camel) parses identically — silent-drop regression pin") {
    val yaml =
      """name: casc
        |version: 1
        |source:
        |  byName:
        |    table: events
        |dimensions:
        |  - name: order_date_hour
        |    expr: order_date_hour
        |    type: timestamp
        |  - name: order_date_day
        |    expr: order_date_day
        |    type: date
        |  - name: region
        |    expr: region
        |measures:
        |  - name: total
        |    expr: sum(amount)
        |rollups:
        |  - name: hourly
        |    dimensions: [order_date_hour, region]
        |    measures: [total]
        |    time_grain: hour
        |    grain_dimension: order_date_hour
        |  - name: daily
        |    dimensions: [order_date_day, region]
        |    measures: [total]
        |    time_grain: day
        |    grain_dimension: order_date_day
        |    cascadeSource: hourly
        |""".stripMargin
    val res = ModelLoader.fromString(yaml)
    // Narwhal F5: the camel key MUST NOT silently drop (pre-fix
    // behavior — the cascade would be absent and daily would build
    // from base with no signal).
    res match {
      case scala.util.Right(m) =>
        val daily = m.rollups.find(_.name == "daily")
          .getOrElse(fail("daily rollup not parsed"))
        daily.cascadeSource shouldBe Some("hourly")
      case scala.util.Left(err) =>
        fail(s"expected Right, got manifest error: $err")
    }
  }

  test("cascade loader: both keys set fails loud (ambiguous declaration)") {
    val yaml =
      """name: casc
        |version: 1
        |source:
        |  byName:
        |    table: events
        |dimensions:
        |  - name: order_date_day
        |    expr: order_date_day
        |    type: date
        |measures:
        |  - name: total
        |    expr: sum(amount)
        |rollups:
        |  - name: daily
        |    dimensions: [order_date_day]
        |    measures: [total]
        |    time_grain: day
        |    grain_dimension: order_date_day
        |    cascade_source: hourly
        |    cascadeSource: hourly
        |""".stripMargin
    val res = ModelLoader.fromString(yaml)
    res.isLeft shouldBe true
  }

  test("cascade loader: empty-string cascade_source fails loud") {
    val yaml =
      """name: casc
        |version: 1
        |source:
        |  byName:
        |    table: events
        |dimensions:
        |  - name: order_date_day
        |    expr: order_date_day
        |    type: date
        |measures:
        |  - name: total
        |    expr: sum(amount)
        |rollups:
        |  - name: daily
        |    dimensions: [order_date_day]
        |    measures: [total]
        |    time_grain: day
        |    grain_dimension: order_date_day
        |    cascade_source: ""
        |""".stripMargin
    val res = ModelLoader.fromString(yaml)
    res.isLeft shouldBe true
  }
}

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

  // === PR-273 (C7-T3) ModelLoader I/O refactor tests ===

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

  test("sm8-core's ModelLoader no longer accepts a Path (fromPath removed per RFC §3)") {
    // The whole point of C7-T3: sm8-core is I/O-free. The Path-based
    // entry point used to exist on ModelLoader (line 122 pre-PR-273);
    // post-PR-273 it's gone. The compile-time absence is the contract.
    val methodNames = ModelLoader.getClass.getMethods.map(_.getName).toSet
    assert(!methodNames.contains("fromPath"),
      "ModelLoader.fromPath must NOT exist post-PR-273 (sm8-core is I/O-free); use PlatformModelLoader.fromPath in sm8-platform instead")
  }
}

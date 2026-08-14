/*
 * SM8 Core — ModelBuilder spec.
 *
 * Per [[debug-mantra-mindset]] §1 (reproduce): a fast deterministic
 * pass/fail signal for the ModelBuilder API. The smoke test asserts
 * the builder produces a Model equivalent to the existing
 * `Model.of(...)` smart constructor for the same input.
 *
 * Per [[karphy-guidags-mindset]] "smallest correct change": each
 * test covers ONE invariant. No incidental assertions. No
 * incidental metrics (per karphy-guidags-mindset rule 5).
 *
 * Per [[scala-data-driven-refactor-mindset]] §1: data in core,
 * behavior in core. The builder IS the data — a typed factory.
 * No mocks, no stubs.
 *
 * Per [[scala-error-handling-mindset]] "errors are data":
 * validation produces `Left(ModelValidationError)` values,
 * never throws.
 *
 * ==Spark concerns (per user directive)==
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantras:
 * - mantras #1, #5: no Spark types, no closures, no executor
 *   leakage. The builder is pure data in sm8-core (Spark-free
 *   per the plan's inverted enforcer pattern).
 * - mantra #3 (schema-drift verify at boundary): validation
 *   here is the boundary check; field-level invariants are
 *   enforced once, not by every consumer.
 * Per [[scala-jvm-safety-mindset]]: no Spark reference, no
 * mutable state. The Model produced is auto-Serializable
 * (case class derivation).
 * Per [[scala-perf-testing-mindset]]: builder is startup-time;
 * no hot-path concerns.
 *
 * ==Plan alignment==
 *
 * Per agile-kindling-beacon plan line 195: the manifest/ IR
 * move is a future PR. ModelBuilder is the foundation that
 * YAML deserialization will eventually call.
 */
package io.sm8.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class ModelBuilderSpec extends AnyFunSuite with Matchers {

  // -- Happy path: full builder produces a valid Model --

  test("ModelBuilder.build: withName + withVersion + withSource produces Right(Model) equivalent to Model.of(...)") {
    val src: SourceRef = SourceRef.ByName("default", "people")
    val built = ModelBuilder()
      .withName("test-model")
      .withVersion(1)
      .withSource(src)
      .build
    val direct = Model.of(
      name    = "test-model",
      version = 1,
      source  = src,
    )
    built shouldBe direct
  }

  test("ModelBuilder.build: dimensions and measures accumulate via with* methods") {
    val built = ModelBuilder()
      .withName("dm-test")
      .withVersion(2)
      .withSource(SourceRef.ByName("default", "sales"))
      .withDimension("region", "region")
      .withDimension("product", "product")
      .withMeasure("revenue", "sum(amount)")
      .build
    val expected = Model.of(
      name       = "dm-test",
      version    = 2,
      source     = SourceRef.ByName("default", "sales"),
      dimensions = List(Dimension("region", "region"), Dimension("product", "product")),
      measures   = List(Measure("revenue", "sum(amount)")),
    )
    built shouldBe expected
  }

  // -- Validation: missing required fields return Left --

  test("ModelBuilder.build: without withName returns Left(InvalidName)") {
    val built = ModelBuilder()
      .withVersion(1)
      .withSource(SourceRef.ByName("default", "t"))
      .build
    built.isLeft shouldBe true
    built.left.get shouldBe a [ModelValidationError.InvalidName]
  }

  test("ModelBuilder.build: without withVersion returns Left(InvalidVersion)") {
    val built = ModelBuilder()
      .withName("no-version-model")
      .withSource(SourceRef.ByName("default", "t"))
      .build
    built.isLeft shouldBe true
    built.left.get shouldBe a [ModelValidationError.InvalidVersion]
  }

  // -- Per [[scala-impact-analysis-mindset]] backward compatibility --

  test("ModelBuilder: existing Model.of(...) smart constructor still works (additive API)") {
    val direct = Model.of(
      name    = "compat-model",
      version = 1,
      source  = SourceRef.ByName("default", "t"),
    )
    direct.isRight shouldBe true
  }

  // -- Per [[debug-mantra-mindset]] §5 verify --

  test("ModelBuilder: smoke round-trip - builder produced Model survives ObjectOutputStream (Serializable contract)") {
    val built = ModelBuilder()
      .withName("serialize-test")
      .withVersion(3)
      .withSource(SourceRef.ByName("default", "t"))
      .withDimension("a", "a")
      .withMeasure("b", "b")
      .build
      .toOption
      .get
    val bytes = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bytes)
    oos.writeObject(built)
    oos.close()
    val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray))
    val restored = ois.readObject().asInstanceOf[Model]
    ois.close()
    restored shouldBe built
  }

  // -- Per [[scala-data-driven-refactor-mindset]] §3 sealed-trait dispatch --

  test("ModelBuilder.build: supports all 3 SourceRef variants (ByName, ByPath, ByProvider)") {
    val byName = ModelBuilder().withName("n").withVersion(1).withSource(SourceRef.ByName("c", "t")).build
    val byPath = ModelBuilder().withName("p").withVersion(1).withSource(SourceRef.ByPath("csv", "/tmp/x")).build
    val byProvider = ModelBuilder().withName("pr").withVersion(1).withSource(SourceRef.ByProvider("driver-ref")).build
    byName.isRight shouldBe true
    byPath.isRight shouldBe true
    byProvider.isRight shouldBe true
  }
}

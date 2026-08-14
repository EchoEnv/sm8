/*
 * SM8 Core — ManifestValidator spec.
 *
 * Per [[debug-mantra-mindset]]: fast deterministic pass/fail tests.
 * Each test exercises one invariant of the validator.
 *
 * Per [[karphyaguids-mindset]]: no incidental assertions, no
 * incidental metrics. Pure functions; no mocks.
 *
 * Per [[scala-data-driven-refactor-mindset]] §1: data in core.
 * The validator is a typed factory — no behavior beyond
 * String → Either[ManifestError, JsonNode].
 *
 * ==Spark concerns (per user directive)==
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantras: N/A in core.
 * The validator has zero Spark types. The validated JsonNode
 * flows to ModelLoader → MCPEngineProvider → spark-connector
 * layer (PRs #38-#42 handle Spark concerns).
 */
package io.sm8.core.manifest

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class ManifestValidatorSpec extends AnyFunSuite with Matchers {

  // -- Happy paths --

  test("ManifestValidator: minimal valid manifest returns Right(JsonNode)") {
    val yaml =
      """name: people-model
        |version: 1
        |source:
        |  byName:
        |    table: people
        |""".stripMargin
    val out = ManifestValidator.validate(yaml)
    out.isRight shouldBe true
  }

  test("ManifestValidator: full manifest (all fields) returns Right(JsonNode)") {
    val yaml =
      """name: full-model
        |version: 2
        |description: A complete test
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
        |measures:
        |  - name: revenue
        |    expr: sum(amount)
        |filters:
        |  - name: adults
        |    predicate: "age >= 18"
        |""".stripMargin
    val out = ManifestValidator.validate(yaml)
    out.isRight shouldBe true
  }

  // -- Failure paths (schema-layer) --

  test("ManifestValidator: missing required `name` returns Left(SchemaValidation)") {
    val yaml = "version: 1\nsource:\n  byName:\n    table: t\n"
    val out = ManifestValidator.validate(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [ManifestError.SchemaValidation]
    val msgs = out.left.get.asInstanceOf[ManifestError.SchemaValidation].messages
    msgs should not be empty
    msgs.head should include ("name")
  }

  test("ManifestValidator: missing required `version` returns Left(SchemaValidation)") {
    val yaml = "name: no-version\nsource:\n  byName:\n    table: t\n"
    val out = ManifestValidator.validate(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [ManifestError.SchemaValidation]
  }

  test("ManifestValidator: missing `source` returns Left(SchemaValidation)") {
    val yaml = "name: x\nversion: 1\n"
    val out = ManifestValidator.validate(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [ManifestError.SchemaValidation]
  }

  test("ManifestValidator: negative `version` (below min 0) returns Left(SchemaValidation)") {
    val yaml =
      """name: neg-version
        |version: -1
        |source:
        |  byName:
        |    table: t
        |""".stripMargin
    val out = ManifestValidator.validate(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [ManifestError.SchemaValidation]
  }

  test("ManifestValidator: invalid source (none of byName/byPath/byProvider) returns Left(SchemaValidation)") {
    val yaml =
      """name: bad-source
        |version: 1
        |source:
        |  bogus:
        |    foo: bar
        |""".stripMargin
    val out = ManifestValidator.validate(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [ManifestError.SchemaValidation]
  }

  test("ManifestValidator: invalid status (not in enum) returns Left(SchemaValidation)") {
    val yaml =
      """name: bad-status
        |version: 1
        |status: retired
        |source:
        |  byName:
        |    table: t
        |""".stripMargin
    val out = ManifestValidator.validate(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [ManifestError.SchemaValidation]
  }

  test("ManifestValidator: extra unknown top-level field returns Left(SchemaValidation) (additionalProperties: false)") {
    val yaml =
      """name: extra-field
        |version: 1
        |source:
        |  byName:
        |    table: t
        |unknownField: forbidden
        |""".stripMargin
    val out = ManifestValidator.validate(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [ManifestError.SchemaValidation]
  }

  // -- Failure path (parse-layer) --

  test("ManifestValidator: malformed YAML syntax returns Left(ParseFailure)") {
    val yaml = "name: x\nversion: 1\nsource: { byName: [table: t"
    val out = ManifestValidator.validate(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [ManifestError]
  }

  test("ManifestValidator: empty input returns Left(ParseFailure)") {
    val out = ManifestValidator.validate("")
    out.isLeft shouldBe true
    out.left.get shouldBe a [ManifestError]
  }

  // -- Smoke round-trip (Serializability is Jackson-owned) --

  test("ManifestValidator: validated JsonNode can be (de)serialized via Jackson") {
    val yaml =
      """name: rt-test
        |version: 1
        |source:
        |  byName:
        |    table: t
        |""".stripMargin
    val node = ManifestValidator.validate(yaml).toOption.get
    // Round-trip via writeValueAsString + readTree
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val asString = mapper.writeValueAsString(node)
    val reparsed = mapper.readTree(asString)
    reparsed shouldBe node
  }
}

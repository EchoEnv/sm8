/*
 * SM8 Platform — PlatformModelLoader spec.
 *
 * Per [[debug-mantra-mindset]]: a deterministic pass/fail signal.
 * Each test exercises one invariant of the adapter.
 *
 * Per [[karphyaguids-mindset]]: no incidental assertions, no
 * incidental metrics. Pure functions; the only side effects are
 * file-system (the `fromPath` test uses a temp file).
 *
 * Per [[scala-data-driven-refactor-mindset]] §1: data in core.
 * The adapter is a typed factory — no behavior beyond
 * Path/String → Model conversion.
 *
 * ==Spark concerns (per user directive)==
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantras: N/A in this
 * spec (no Spark types). The Model produced can flow to
 * EngineRegistry → SparkEngineProvider.query() via the
 * connector layer (PRs #38-#42 handle Spark concerns).
 */
package io.sm8.platform.query

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import io.sm8.core.model.{Model, ModelStatus, SourceRef}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class PlatformModelLoaderSpec extends AnyFunSuite with Matchers {

  /** Helper: write a YAML string to a temp file, return the path.
    * Cleans up after the test via `after` hook (or in `finally`). */
  private def writeYaml(content: String): Path = {
    val tmp = Files.createTempFile("sm8-model-", ".yml")
    Files.write(tmp, content.getBytes(StandardCharsets.UTF_8))
    tmp
  }

  // -- Happy path: fromString returns Right(Model) --

  test("PlatformModelLoader.fromString: minimal YAML with byName source returns Right(Model)") {
    val yaml =
      """name: people-model
        |version: 1
        |source:
        |  byName:
        |    table: people
        |""".stripMargin
    val out = PlatformModelLoader.fromString(yaml)
    out.isRight shouldBe true
    val m = out.toOption.get
    m.name shouldBe "people-model"
    m.version shouldBe 1
    m.source shouldBe SourceRef.ByName(table = "people")
    m.status shouldBe ModelStatus.Draft
  }

  // -- Happy path: fromPath round-trip --

  test("PlatformModelLoader.fromPath: real file on disk survives and matches fromString outcome") {
    // Per [[debug-mantra-mindset]] §5 verify: the fix is real.
    // We round-trip through the actual filesystem path to prove
    // that fromPath actually reads files.
    val yaml =
      """name: file-roundtrip-model
        |version: 2
        |source:
        |  byName:
        |    table: t
        |""".stripMargin
    val path = writeYaml(yaml)
    try {
      val fromPathOut  = PlatformModelLoader.fromPath(path).toOption.get
      val fromStringOut = PlatformModelLoader.fromString(yaml).toOption.get
      fromPathOut shouldBe fromStringOut
    } finally {
      Files.deleteIfExists(path)
    }
  }

  test("PlatformModelLoader.fromPath: missing file returns Left(PlatformModelError.InvalidYaml)") {
    // Per [[debug-mantra-mindset]] §1 reproduce: missing file
    // must be reproducible. We point at /nonexistent/...
    val bogus = Path.of("/nonexistent/sm8-test-yml--does-not-exist.yml")
    val out = PlatformModelLoader.fromPath(bogus)
    out.isLeft shouldBe true
    out.left.get shouldBe a [PlatformModelError]
  }


  // -- PR #49: schema-validation pipeline runs BEFORE ModelLoader parse --

  test("PlatformModelLoader.fromString: extra unknown top-level field returns Left(SchemaValidation) — additionalProperties: false catches it") {
    val yaml =
      """name: extra
        |version: 1
        |source:
        |  byName:
        |    table: t
        |unknown_field: forbidden
        |""".stripMargin
    val out = PlatformModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [PlatformModelError.SchemaValidation]
  }

  // -- Failure paths: typed PlatformModelError --

  test("PlatformModelLoader.fromString: missing name returns Left(PlatformModelError.SchemaValidation) (schema-layer catches it first)") {
    // Per PR #49: the schema-validation layer catches missing-field
    // errors BEFORE ModelLoader's post-parse MissingField check
    // runs. The typed PlatformModelError.SchemaValidation case
    // carries the validation messages so callers can debug.
    val yaml = "version: 1\nsource:\n  byName:\n    table: t\n"
    val out = PlatformModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    val err = out.left.get
    err shouldBe a [PlatformModelError.SchemaValidation]
    err.message should include ("name")
  }

  test("PlatformModelLoader.fromString: missing version returns Left(PlatformModelError.SchemaValidation)") {
    val yaml = "name: no-version\nsource:\n  byName:\n    table: t\n"
    val out = PlatformModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [PlatformModelError.SchemaValidation]
    out.left.get.message should include ("version")
  }

  test("PlatformModelLoader.fromString: malformed YAML returns Left(PlatformModelError.ParseFailure)") {
    // Truly malformed: unclosed flow-mapping.
    val yaml = "name: x\nversion: 1\nsource: { byName: [table: t"
    val out = PlatformModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [PlatformModelError.ParseFailure]
  }

  test("PlatformModelLoader.fromString: unknown status returns Left(PlatformModelError.UnknownStatus)") {
    val yaml =
      """name: bad-status
        |version: 1
        |status: retired
        |source:
        |  byName:
        |    table: t
        |""".stripMargin
    val out = PlatformModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [PlatformModelError.SchemaValidation]  // PR #49: caught by schema enum
  }

  test("PlatformModelLoader.fromString: unknown SourceRef variant returns Left(PlatformModelError.UnknownSourceRef)") {
    val yaml =
      """name: bad-source
        |version: 1
        |source:
        |  mystery:
        |    foo: bar
        |""".stripMargin
    val out = PlatformModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a [PlatformModelError.SchemaValidation]  // PR #49: caught by schema oneOf
  }

  // -- PlatformModelError carries the underlying CoreManifestError --

  test("PlatformModelError: each case carries the underlying CoreManifestError for traceability") {
    // Per [[karphyaguids-mindset]]: a tester can pull the inner
    // CoreManifestError to debug.
    val yaml = "version: 1\nsource:\n  byName:\n    table: t\n"
    val err = PlatformModelLoader.fromString(yaml).left.get
    val inner = err match {
      case m: PlatformModelError.SchemaValidation => m.coreError
      case _ => fail(s"expected SchemaValidation, got $err")
    }
    inner.message should include ("name")
  }
}

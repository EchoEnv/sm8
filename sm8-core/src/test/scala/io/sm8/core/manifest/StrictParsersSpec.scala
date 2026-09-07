/*
 * SM8 Core — strict-parsers spec (Ticket 2 of
 * docs/wayfinder/2026-09-07-parser-hardening.md).
 *
 * Pins the "never silent" discipline Ticket 3's parseRollups
 * established, now applied to the OLDER parsers (parseJoins,
 * parseCalculatedMeasures, parseFilters):
 *   - a non-map entry in a list block fails loud as typed
 *     ManifestError.ParseFailure (previously: silently dropped by
 *     flatMap(asMap));
 *   - a missing 'name' fails loud (previously: silently skipped in
 *     parseFilters's Option-map);
 *   - a missing 'predicate'/'expr' fails loud (previously: silently
 *     skipped or dropped);
 *   - an unparsable predicate fails loud (previously: silently
 *     dropped by toOption).
 * All previously-VALID fixtures continue to load (ModelLoaderSpec
 * + ModelLoaderM1Spec + EndToEndPipelineSpec already pin that;
 * this spec adds the negative cases).
 */
package io.sm8.core.manifest

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StrictParsersSpec extends AnyFunSuite with Matchers {

  private val base =
    """name: m
      |version: 1
      |source:
      |  byName:
      |    table: t
      |""".stripMargin

  // ===== parseFilters: silent paths now loud =====

  test("filters: non-map entry fails loud (previously silently dropped)") {
    val yaml = base +
      """
        |filters:
        |  - name: adults
        |    predicate: "age >= 18"
        |  - "oops-a-scalar"
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a[ManifestError.ParseFailure]
    out.left.get.toString should include("filters[]: entry must be a map")
  }

  test("filters: missing 'name' fails loud (previously silently skipped)") {
    val yaml = base +
      """
        |filters:
        |  - predicate: "age >= 18"
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a[ManifestError.ParseFailure]
    out.left.get.toString should include("missing 'name'")
  }

  test("filters: missing 'predicate' fails loud (previously silently skipped)") {
    val yaml = base +
      """
        |filters:
        |  - name: adults
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a[ManifestError.ParseFailure]
    out.left.get.toString should include("missing 'predicate'")
  }

  test("filters: unparsable predicate fails loud (previously silently dropped by toOption)") {
    val yaml = base +
      """
        |filters:
        |  - name: broken
        |    predicate: "age >=> 18 AND"
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a[ManifestError.ParseFailure]
    out.left.get.toString should include("filters[broken]")
  }

  test("filters: valid entry still loads (no regression)") {
    val yaml = base +
      """
        |filters:
        |  - name: adults
        |    predicate: "age >= 18"
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isRight shouldBe true
    out.toOption.get.filters.size shouldBe 1
  }

  // ===== parseCalculatedMeasures: non-map entry now loud =====

  test("calculated_measures: non-map entry fails loud (previously silently dropped by flatMap(asMap))") {
    val yaml = base +
      """
        |calculated_measures:
        |  - name: cm1
        |    expr: "a + b"
        |  - 42
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a[ManifestError.ParseFailure]
    out.left.get.toString should include("calculated_measures[]: entry must be a map")
  }

  test("calculated_measures: valid entry still loads (no regression)") {
    val yaml = base +
      """
        |calculated_measures:
        |  - name: cm1
        |    expr: "a + b"
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isRight shouldBe true
    out.toOption.get.calculatedMeasures.size shouldBe 1
  }

  test("calculated_measures: missing 'name' fails loud (previously silently dropped by getOrElse(''))") {
    val yaml = base +
      """
        |calculated_measures:
        |  - expr: "a + b"
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a[ManifestError.ParseFailure]
    out.left.get.toString should include("calculated_measures[]: missing 'name'")
  }

  // ===== parseJoins: non-map entry now loud =====

  test("joins: non-map entry fails loud (previously silently dropped by flatMap(asMap))") {
    val yaml = base +
      """
        |joins:
        |  - name: j1
        |    rightModel: c
        |    kind: inner
        |    keys: [[a, b]]
        |  - "not-a-map"
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a[ManifestError.ParseFailure]
    out.left.get.toString should include("joins[]: entry must be a map")
  }

  test("joins: missing 'name' fails loud (previously silently used getOrElse(''))") {
    val yaml = base +
      """
        |joins:
        |  - rightModel: c
        |    kind: inner
        |    keys: [[a, b]]
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a[ManifestError.ParseFailure]
    out.left.get.toString should include("joins[]: missing 'name'")
  }

  test("joins: valid entry still loads (no regression)") {
    val yaml = base +
      """
        |joins:
        |  - name: j1
        |    rightModel: c
        |    kind: inner
        |    keys: [[a, b]]
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isRight shouldBe true
    out.toOption.get.joins.size shouldBe 1
  }

  // ===== accumulated errors still surface together =====

  test("multiple silent-path violations across blocks: first failure surfaces (fold order)") {
    val yaml = base +
      """
        |calculated_measures:
        |  - 42
        |filters:
        |  - "also-bad"
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    // Both blocks have issues; the one processed first wins (the
    // for-comp processes joins -> filters -> calculated_measures).
    // Either error type is acceptable — the point is that it fails
    // LOUD, never silently.
    out.left.get shouldBe a[ManifestError]
  }
}

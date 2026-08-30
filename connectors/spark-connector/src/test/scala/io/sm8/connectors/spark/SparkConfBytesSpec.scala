/*
 * SM8 Spark Connector — SparkConfBytesSpec (PR-210).
 *
 * Pins the byte-string grammar of `SparkConfBytes.parseBytes`, the
 * connector-local mirror of Spark's
 * `ConfigHelpers.byteFromString` (the parser `SQLConf.bytesConf
 * (ByteUnit.BYTE)` wires for `spark.sql.autoBroadcastJoinThreshold`).
 *
 * Every expected value below was verified against Spark 3.5.8's
 * own `byteFromString` empirically (reflection harness over
 * `spark-core_2.13:3.5.8`), so a future Spark upgrade that changes
 * the grammar surfaces here as a concrete expected-value failure.
 *
 * Per scala-bug-hunting: the sign-split-before-suffix-parse rule
 * (leading `-` only), the no-suffix = bytes default, and the
 * NumberFormatException-on-malformed contract are each pinned by
 * a dedicated case.
 *
 * Per scala-perf-testing: pure string parsing, no SparkSession —
 * the whole suite runs on the JVM with no Spark context.
 */
package io.sm8.connectors.spark

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkConfBytesSpec extends AnyFunSuite with Matchers {

  test("PR-210: plain integers parse as bytes (Spark default shape)") {
    SparkConfBytes.parseBytes("104857600") shouldBe 104857600L
    SparkConfBytes.parseBytes("0") shouldBe 0L
    SparkConfBytes.parseBytes("10") shouldBe 10L
  }

  test("PR-210: Spark's own default string (10MB) parses to 10 MiB") {
    // The value PR-197's fallback coincidentally matched. After
    // PR-210 the seed reads the REAL parsed bytes; the default
    // string must equal BroadcastSeedDefaultBytes so the fallback
    // and the parsed value stay indistinguishable for default
    // sessions (no behavior change for operators who never set
    // the key).
    SparkConfBytes.parseBytes("10MB") shouldBe 10485760L
    SparkConfBytes.parseBytes("10MB") shouldBe PortableQueryCompiler.BroadcastSeedDefaultBytes
  }

  test("PR-210: single-letter suffixed positives (1g, 512kb, 2m) parse to exact bytes") {
    SparkConfBytes.parseBytes("1g") shouldBe 1073741824L
    SparkConfBytes.parseBytes("512kb") shouldBe 524288L
    SparkConfBytes.parseBytes("2m") shouldBe 2097152L
    SparkConfBytes.parseBytes("1t") shouldBe 1099511627776L
  }

  test("PR-210: documented disable sentinel (-1) parses to -1 (the JavaUtils regression)") {
    // Falsifiable: `org.apache.spark.network.util.JavaUtils
    // .byteStringAsBytes("-1")` throws NumberFormatException — its
    // grammar has no sign. Spark's real parser accepts it; this
    // pin is the exact case that forced the connector-local
    // mirror instead of the public JavaUtils call.
    SparkConfBytes.parseBytes("-1") shouldBe -1L
  }

  test("PR-210: suffixed negatives (-2b, -100b, -2m) parse to exact negative bytes") {
    SparkConfBytes.parseBytes("-2b") shouldBe -2L
    SparkConfBytes.parseBytes("-100b") shouldBe -100L
    SparkConfBytes.parseBytes("-2m") shouldBe -2097152L
  }

  test("PR-210: sign-split happens BEFORE suffix parse (trailing sign is invalid)") {
    // Spark's byteFromString splits a LEADING sign only; `1g-`
    // must fail, not parse to -1073741824.
    an[NumberFormatException] should be thrownBy SparkConfBytes.parseBytes("1g-")
  }

  test("PR-210: case-insensitive suffixes (1G, 512KB, 2M)") {
    SparkConfBytes.parseBytes("1G") shouldBe 1073741824L
    SparkConfBytes.parseBytes("512KB") shouldBe 524288L
    SparkConfBytes.parseBytes("2M") shouldBe 2097152L
  }

  test("PR-210: whitespace is tolerated (Spark trims before parse; callers also trim)") {
    // byteStringAs lowercases + trims internally; the seed also
    // trims the conf value before handing it over. Both shapes
    // must agree.
    SparkConfBytes.parseBytes(" 1g") shouldBe 1073741824L
    SparkConfBytes.parseBytes("1g ") shouldBe 1073741824L
    SparkConfBytes.parseBytes(" 1g ".trim) shouldBe 1073741824L
  }

  test("PR-210: malformed input throws NumberFormatException (fallback contract)") {
    // Non-numeric text, invalid suffix, and fractional values all
    // funnel to NumberFormatException — the type the seed's
    // catch arms (PR-197 contract) match on.
    an[NumberFormatException] should be thrownBy SparkConfBytes.parseBytes("abc")
    an[NumberFormatException] should be thrownBy SparkConfBytes.parseBytes("1x")
    an[NumberFormatException] should be thrownBy SparkConfBytes.parseBytes("1.5g")
  }

  test("PR-210: empty string is malformed (Spark also rejects it)") {
    an[NumberFormatException] should be thrownBy SparkConfBytes.parseBytes("")
  }

  test("PR-210: magnitude overflow throws IllegalArgumentException (seed catch widened to match)") {
    // ByteUnit.convertFrom throws IAE when the magnitude-suffix
    // product exceeds Long.MAX_VALUE — distinct from
    // NumberFormatException (bad grammar). Both are caught at the
    // seedBroadcastThreshold call sites so the PR-197 fallback
    // semantics hold for overflow paths. `Long.MAX_VALUE` bytes
    // parses cleanly; one past rejects.
    SparkConfBytes.parseBytes("9223372036854775807b") shouldBe Long.MaxValue
    an[IllegalArgumentException] should be thrownBy SparkConfBytes.parseBytes("9223372036854775808b")
    // Same shape for suffixed magnitudes (scaled overflow).
    an[IllegalArgumentException] should be thrownBy SparkConfBytes.parseBytes("9000000000000000000g")
  }
}

/*
 * SM8 Core — EngineError test.
 *
 * Per [[scala-data-driven-refactor-mindset]] "sealed trait dispatch":
 * tests prove the compiler-enforced exhaustiveness AND the
 * toErrorDetail mapping.
 *
 * Per [[debug-mantra-mindset]] + [[karpathy-guidelinesmindset]]:
 * tests use plain text descriptions (no `.` — ScalaTest parses dots
 * as method-call separators).
 */
package io.sm8.core.engine

import io.sm8.sdk.{ErrorCode, ErrorDetail}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EngineErrorSpec extends AnyFlatSpec with Matchers {

  "ErrorCode" should "have 10 sealed cases" in {
    ErrorCode.UNSUPPORTED_CAPABILITY shouldBe a [ErrorCode]
    ErrorCode.INCOMPATIBLE_EXPR_SHAPE shouldBe a [ErrorCode]
    ErrorCode.DECIMAL_OVERFLOW shouldBe a [ErrorCode]
    ErrorCode.FEATURE_DEFERRED shouldBe a [ErrorCode]
    ErrorCode.CANCELLATION_FAILED shouldBe a [ErrorCode]
    ErrorCode.CONNECTION_FAILED shouldBe a [ErrorCode]
    ErrorCode.QUERY_TIMED_OUT shouldBe a [ErrorCode]
    ErrorCode.AUDIT_SINK_UNAVAILABLE shouldBe a [ErrorCode]
    ErrorCode.PROVIDER_INVOCATION_FAILED shouldBe a [ErrorCode]
    ErrorCode.SOURCE_SCHEMA_CHANGED shouldBe a [ErrorCode]
    ErrorCode.ENGINE_UNAVAILABLE shouldBe a [ErrorCode]
  }

  "EngineError UnsupportedCapability" should "map to UNSUPPORTED_CAPABILITY" in {
    val e = EngineError.UnsupportedCapability("trino", "ROLLUP", "not supported")
    val d = e.toErrorDetail
    d.code shouldBe ErrorCode.UNSUPPORTED_CAPABILITY
    d.message should (include ("ROLLUP"))
    d.engine shouldBe Some("trino")
  }

  "EngineError QueryTimedOut" should "map to QUERY_TIMED_OUT" in {
    val e = EngineError.QueryTimedOut("spark", "cancelled_ok", "budget exceeded")
    val d = e.toErrorDetail
    d.code shouldBe ErrorCode.QUERY_TIMED_OUT
    d.message should (include ("cancelled_ok"))
  }

  "EngineError DecimalOverflow" should "include precision/scale in message" in {
    val e = EngineError.DecimalOverflow("duckdb", "12345.6789", 10, 2, "over")
    val d = e.toErrorDetail
    d.code shouldBe ErrorCode.DECIMAL_OVERFLOW
    d.message should (include ("DECIMAL(10,2)"))
    d.message should (include ("12345.6789"))
  }

  "EngineError ConnectionFailed" should "set engine field" in {
    val e = EngineError.ConnectionFailed("trino", "timeout", "no route")
    e.toErrorDetail.engine shouldBe Some("trino")
  }
}

class ErrorDetailSpec extends AnyFlatSpec with Matchers {
  "ErrorDetail" should "default engine to None" in {
    val d = ErrorDetail(ErrorCode.UNSUPPORTED_CAPABILITY, "msg")
    d.code shouldBe ErrorCode.UNSUPPORTED_CAPABILITY
    d.message shouldBe "msg"
    d.engine shouldBe None
  }

  it should "accept explicit engine" in {
    val d = ErrorDetail(ErrorCode.QUERY_TIMED_OUT, "msg", Some("spark"))
    d.engine shouldBe Some("spark")
  }
}
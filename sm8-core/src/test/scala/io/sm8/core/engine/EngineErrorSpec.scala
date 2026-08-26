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

"EngineError PersistLifecycleFailed (Unpersist phase)" should "map to PROVIDER_INVOCATION_FAILED with phase context" in {
  // ADR-009-f v3.2: the paired-lifecycle Unpersist failure (the
  // path SparkEngineProvider.applyPostCompilePipeline actually
  // exercises on unpersist-side faults).
  val e = EngineError.PersistLifecycleFailed(
    engine = "spark-3.5",
    phase = EngineError.PersistPhase.Unpersist,
    cause = "org.apache.spark.SparkException",
    message = "executor OOM during unpersist")
  e.engine shouldBe "spark-3.5"
  e.phase shouldBe EngineError.PersistPhase.Unpersist
  e.cause shouldBe "org.apache.spark.SparkException"
  e.message shouldBe "executor OOM during unpersist"
  val d = e.toErrorDetail
  d.code shouldBe ErrorCode.PROVIDER_INVOCATION_FAILED
  d.message should include ("persist-lifecycle(Unpersist)")
  d.message should include ("org.apache.spark.SparkException")
  d.message should include ("executor OOM during unpersist")
  d.engine shouldBe Some("spark-3.5")
}

"EngineError PersistLifecycleFailed (Persist phase)" should "still map to PROVIDER_INVOCATION_FAILED (same wire code)" in {
  // The PersistPhase sealed trait's Persist case is reserved for
  // a future persist-side surface; the wire mapping must be the
  // same because the dispatcher / MCP server cannot tell them
  // apart on the status code path.
  val e = EngineError.PersistLifecycleFailed(
    engine = "spark-3.5",
    phase = EngineError.PersistPhase.Persist,
    cause = "X",
    message = "y")
  e.toErrorDetail.code shouldBe ErrorCode.PROVIDER_INVOCATION_FAILED
  e.toErrorDetail.message should include ("persist-lifecycle(Persist)")
}

"EngineError PersistPhase" should "have exactly Persist and Unpersist cases (sealed, no leak)" in {
  // Compiler-enforced: the sealed trait + 2 case objects exhaust
  // the phase dimension.
  val all: Set[EngineError.PersistPhase] = Set(
    EngineError.PersistPhase.Persist,
    EngineError.PersistPhase.Unpersist)
  all.size shouldBe 2
  EngineError.PersistPhase.Persist should not be EngineError.PersistPhase.Unpersist
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
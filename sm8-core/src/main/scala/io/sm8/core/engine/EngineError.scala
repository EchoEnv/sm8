/*
 * SM8 Core — EngineError ADT.
 *
 * Engine-portable typed failure sealed ADT. Every engine adapter returns
 * EngineError from compile/execute — the MCP server maps it to MCP error
 * envelopes via the exhaustive `toErrorDetail` mapping.
 *
 * data-only, smart constructors at boundary): pure data, sealed-trait +
 * case-class pattern. Compiler enforces exhaustiveness via pattern match.
 *
 * existing style): Scala 2.13 idiom — `sealed trait + final case
 * classes + require(.)`. NO Scala-3-only `enum`.
 *
 * The 7 frozen SDK types are unchanged. PR-B handlers consume this.
 */
package io.sm8.core.engine

import io.sm8.sdk.{ErrorCode, ErrorDetail}

/**
 * Closed set of wire-stable failure modes. Universal across engines
 * (Spark, Trino, DuckDB,.) per the multi-engine design. Each
 * case carries ONLY the data needed to identify the failure; the
 * `engine` field tells the consumer which engine produced it.
 */
sealed trait EngineError extends Product with Serializable {
 /** Which engine produced the error (e.g. "trino", "spark"). */
 def engine: String

 /** Human-readable detail. */
 def message: String

 /** Map this error to the engine-portable ErrorDetail wire shape. */
 def toErrorDetail: ErrorDetail
}

object EngineError {

 final case class UnsupportedCapability(engine: String, capability: String, message: String)
  extends EngineError {
 override def toErrorDetail: ErrorDetail =
  ErrorDetail(ErrorCode.UNSUPPORTED_CAPABILITY, s"$capability: $message", Some(engine))
 }

 final case class IncompatibleExprShape(engine: String, shape: String, message: String)
  extends EngineError {
 override def toErrorDetail: ErrorDetail =
  ErrorDetail(ErrorCode.INCOMPATIBLE_EXPR_SHAPE, s"$shape [engine=$engine]", Some(engine))
 }

 final case class DecimalOverflow(
  engine: String,
  value: String,
  precision: Int,
  scale: Int,
  message: String
 ) extends EngineError {
 override def toErrorDetail: ErrorDetail =
  ErrorDetail(
  ErrorCode.DECIMAL_OVERFLOW,
  s"$value does not fit DECIMAL($precision,$scale)",
  Some(engine))
 }

 final case class FeatureDeferred(
  engine: String,
  feature: String,
  release: String,
  message: String
 ) extends EngineError {
 override def toErrorDetail: ErrorDetail =
  ErrorDetail(ErrorCode.FEATURE_DEFERRED, s"$feature deferred to $release", Some(engine))
 }

 final case class CancellationFailed(engine: String, reason: String, message: String)
  extends EngineError {
 override def toErrorDetail: ErrorDetail =
  ErrorDetail(ErrorCode.CANCELLATION_FAILED, reason, Some(engine))
 }

 final case class ConnectionFailed(engine: String, reason: String, message: String)
  extends EngineError {
 override def toErrorDetail: ErrorDetail =
  ErrorDetail(ErrorCode.CONNECTION_FAILED, reason, Some(engine))
 }

 final case class QueryTimedOut(engine: String, cancelStatus: String, message: String)
  extends EngineError {
 override def toErrorDetail: ErrorDetail =
  ErrorDetail(ErrorCode.QUERY_TIMED_OUT, cancelStatus, Some(engine))
 }

 final case class AuditSinkUnavailable(engine: String, name: String, message: String)
  extends EngineError {
 override def toErrorDetail: ErrorDetail =
  ErrorDetail(ErrorCode.AUDIT_SINK_UNAVAILABLE, name, Some(engine))
 }

 final case class ProviderInvocationFailed(
  engine: String,
  name: String,
  reason: String,
  message: String
 ) extends EngineError {
 override def toErrorDetail: ErrorDetail =
  ErrorDetail(ErrorCode.PROVIDER_INVOCATION_FAILED, s"$name: $reason", Some(engine))
 }

 final case class SourceSchemaChanged(engine: String, source: String, message: String)
  extends EngineError {
 override def toErrorDetail: ErrorDetail =
  ErrorDetail(ErrorCode.SOURCE_SCHEMA_CHANGED, source, Some(engine))
 }

 final case class EngineUnavailable(
  engine: String,
  available: List[String],
  wasDefault: Boolean,
  message: String
 ) extends EngineError {
 override def toErrorDetail: ErrorDetail =
  ErrorDetail(
  ErrorCode.ENGINE_UNAVAILABLE,
  s"$engine (wasDefault=$wasDefault)",
  Some(engine))
 }
}
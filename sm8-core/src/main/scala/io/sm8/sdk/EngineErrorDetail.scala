/*
 * SM8 SDK — EngineError wire shape + ErrorCode enum.
 *
 * Per [[scala-impact-analysismindset]]: ADDITIVE to sm8-sdk. New
 * type. Does not modify any of the 7 frozen SDK types.
 *
 * Per [[scala-data-driven-refactor-mindset]] (sealed trait dispatch
 * over Map[String, Any]): closed set of stable wire codes. Plugins
 * reference codes by name (engine-portable, no exceptions thrown
 * for control flow).
 *
 * Per [[karpathy-guidelinesmindset]] (smallest correct core + Scala
 * 2.13 idiom): sealed trait + case objects. NOT Scala 3 `enum`.
 */
package io.sm8.sdk

/**
 * Engine-portable ErrorDetail wire shape. Returned to MCP server
 * (or any transport) verbatim. The mapping from EngineError →
 * ErrorDetail is total (see `EngineError.toErrorDetail` in core).
 */
final case class ErrorDetail(
    code: ErrorCode,
    message: String,
    engine: Option[String] = None
)

/**
 * Closed set of stable wire error codes. Per karpathy: sealed
 * trait + case objects = exhaustive pattern match.
 */
sealed trait ErrorCode
object ErrorCode {
  case object UNSUPPORTED_CAPABILITY extends ErrorCode
  case object INCOMPATIBLE_EXPR_SHAPE extends ErrorCode
  case object DECIMAL_OVERFLOW extends ErrorCode
  case object FEATURE_DEFERRED extends ErrorCode
  case object CANCELLATION_FAILED extends ErrorCode
  case object CONNECTION_FAILED extends ErrorCode
  case object QUERY_TIMED_OUT extends ErrorCode
  case object AUDIT_SINK_UNAVAILABLE extends ErrorCode
  case object PROVIDER_INVOCATION_FAILED extends ErrorCode
  case object SOURCE_SCHEMA_CHANGED extends ErrorCode
  case object ENGINE_UNAVAILABLE extends ErrorCode
}
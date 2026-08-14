/*
 * SM8 Core — ManifestError.
 *
 * Engine-portable, typed error for the YAML manifest layer.
 * Per [[scala-error-handling-mindset]]: errors are data. Parsing
 * failures return `Left[ManifestError]`, never throw.
 *
 * Per [[scala-data-driven-refactor-mindset]] §2 ("shape and validity
 * are separate"): ManifestError covers PARSE failures (shape-level:
 * missing fields, type mismatches, unknown SourceRef variants,
 * schema validation failures).  The downstream `Model.of(...)`
 * smart constructor covers VALIDITY failures (domain-level: name
 * not blank, version non-negative).  The two stay distinct so
 * callers can tell "the YAML is malformed" from "the YAML parsed
 * but the domain rules reject it".
 *
 * Per [[scala-impact-analysis-mindset]] mantra 4 (refuse to stop
 * until every affected caller is named): adding a new ManifestError
 * case is a BREAKING change to the sealed-trait family.  Exhaustive
 * patterns live in:
 *   - io.sm8.core.manifest.ManifestValidator.fromCore  (exhaustive)
 *   - io.sm8.platform.query.PlatformModelLoader.fromCore  (mirror)
 * Both MUST be updated when adding a case.
 *
 * ==Spark concerns (per user directive)==
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantras: N/A in core.
 * sm8-core is Spark-free per the plan's inverted enforcer pattern.
 * The ManifestError type has zero Spark references.
 *
 * Per [[scala-jvm-safety-mindset]]: no mutable state. Pure data.
 *
 * Per [[scala-perf-testing-mindset]]: not in hot path.
 */
package io.sm8.core.manifest

/** Typed error from YAML manifest parsing. */
sealed trait ManifestError extends Product with Serializable {
  def message: String
}

object ManifestError {

  /** The YAML root is missing or malformed (not a Map, empty, etc.). */
  final case class InvalidYaml(reason: String) extends ManifestError {
    val message: String = s"Invalid YAML manifest: $reason"
  }

  /** A required field is missing (e.g. `name:`, `version:`, `source:`). */
  final case class MissingField(field: String, where: String) extends ManifestError {
    val message: String = s"Missing required field '$field' in $where"
  }

  /** A `source:` entry references an unknown SourceRef variant. */
  final case class UnknownSourceRef(reason: String) extends ManifestError {
    val message: String = s"Unknown SourceRef variant: $reason"
  }

  /** A `status:` entry is not one of the known `ModelStatus` values. */
  final case class UnknownStatus(value: String) extends ManifestError {
    val message: String = s"Unknown status: '$value' (expected: draft, published, deprecated)"
  }

  /** Catch-all for Jackson parse failures (malformed YAML syntax). */
  final case class ParseFailure(reason: String) extends ManifestError {
    val message: String = s"YAML parse failure: $reason"
  }

  /** Schema validation failure: the JSON tree does not satisfy the
    * expected manifest schema (missing required field, wrong type,
    * unknown enum value, etc.).  Carries ALL validation messages
    * from `com.networknt:json-schema-validator` so callers can
    * surface a complete error report, not just the first failure.
    *
    * Per [[debug-mantra-mindset]] reproduce: this case is reached
    * only after `ManifestValidator.validate(yaml)` returns
    * `Left(SchemaValidation(messages))` — strictly for STRUCTURAL
    * failures (before `Model.of(...)` is called). */
  final case class SchemaValidation(messages: List[String]) extends ManifestError {
    val message: String =
      if (messages.size == 1) s"Schema validation failed: ${messages.head}"
      else s"Schema validation failed (${messages.size} errors):\n  - " + messages.mkString("\n  - ")
  }
}

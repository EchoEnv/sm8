/*
 * SM8 Core — ManifestError.
 *
 * Engine-portable, typed error for the YAML manifest layer.
 * Per [[scala-error-handling-mindset]]: errors are data. Parsing
 * failures return `Left[ManifestError]`, never throw.
 *
 * Per [[scala-data-driven-refactor-mindset]] §2 ("shape and validity
 * are separate"): ManifestError covers PARSE failures (shape-level:
 * missing fields, type mismatches, unknown SourceRef variants).
 * The downstream `Model.of(...)` smart constructor covers VALIDATION
 * failures (domain-level: name not blank, version non-negative).
 * The two stay distinct so callers can tell "the YAML is malformed"
 * from "the YAML parsed but the domain rules reject it".
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
    val message: String = s"Unknown status: '$value' (expected: draft, published, archived)"
  }

  /** Catch-all for Jackson parse failures (malformed YAML syntax). */
  final case class ParseFailure(reason: String) extends ManifestError {
    val message: String = s"YAML parse failure: $reason"
  }
}

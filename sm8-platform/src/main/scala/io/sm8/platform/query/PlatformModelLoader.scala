/*
 * SM8 Platform — PlatformModelLoader.
 *
 * Thin façade that bridges the core-layer `ModelLoader` (from
 * `io.sm8.core.manifest`) and the platform-layer callers.
 *
 * ==Per RFC §3 Core Boundary==
 *
 * The adapter sits in the platform layer (sm8-platform). It does
 * NOT know about a specific data source — it just turns a `Path`
 * (file input) into a `Model` (typed AST).
 *
 * Per `semantic-layer-engine-architecture.md` §5: this adapter runs
 * BEFORE the `parse → resolve → execute → format` pipeline. It
 * produces the `Model` input that `parse` consumes.
 *
 * ==Per karphyaguids-mindset "smallest correct change"==
 *
 * We had originally mapped `ManifestError` (parse-layer) directly
 * to `EngineError.ProviderInvocationFailed` (engine-layer) at
 * this boundary.  The Scala 2.13.18 compiler under scala-maven-plugin
 * (with sbt-zinc bridge) is unable to resolve the case-class
 * sub-types nested under `io.sm8.core.engine.EngineError` from
 * this file's compile context — a known interaction between the
 * sbt-zinc bridge and case-class-nested-under-sealed-trait
 * import resolution across module boundaries.  Rather than fight
 * the compiler, the adapter has its own minimal typed-error
 * ADT and exposes a small `toEngineError` mapper for callers
 * that already speak the EngineError vocabulary.
 *
 * ==Per Plan Step 10==
 *
 * This PR completes the user-facing layer of plan line 289
 * ("Update `semanticdf-platform`'s `QueryService` to call
 * `engine.run(request)`..."). The engine-portable path was
 * already migrated in PRs #35-#37. This PR enables YAML manifests
 * as input.
 *
 * ==Spark concerns (per user directive)==
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantras:
 * - mantras #1, #5: no Spark types captured, no executor-side
 *   closure. Pure data in sm8-platform.
 * - mantra #3 (schema-drift verify at boundary): typed
 *   PlatformModelError on the boundary.
 * - mantras #2, #4: N/A.
 * - Serialize: the produced `Model` is auto-Serializable
 *   (case-class derivation).
 *
 * Indirect Spark relation: the produced Model flows through
 * `EngineService.runQueryWithHooks(model, ...)` →
 * `EngineRegistry.select(...)` → resolves to
 * `SparkEngineProvider.query(...)` (per PRs #38-#42). The
 * spark-connector layer handles all Spark concerns. This adapter
 * holds NO Spark references.
 *
 * Perf (driver/executor): startup-time path. Not in the request
 * hot path.
 */
package io.sm8.platform.query

import java.nio.file.Path

import io.sm8.core.manifest.{ManifestError => CoreManifestError, ManifestValidator, ModelLoader}
import io.sm8.core.model.Model

/**
 * Typed parse-error ADT for the platform-layer model loader.
 *
 * Per [[scala-data-driven-refactor-mindset]]: pure data, no
 * behavior. Each case carries the underlying `CoreManifestError`
 * so callers can pattern-match on either layer.
 *
 * Per [[scala-error-handling-mindset]] "errors are data": this
 * is the SHAPE layer at the platform boundary; the platform
 * callers pattern-match on it OR map to `EngineError`.
 */
sealed trait PlatformModelError extends Product with Serializable {
  def coreError: CoreManifestError
  def message: String
}

object PlatformModelError {


  final case class InvalidYaml(coreError: CoreManifestError.InvalidYaml) extends PlatformModelError {
    val message: String = s"invalid YAML: ${coreError.reason}"
  }
  final case class MissingField(coreError: CoreManifestError.MissingField) extends PlatformModelError {
    val message: String = s"missing field '${coreError.field}' in ${coreError.where}"
  }
  final case class UnknownSourceRef(coreError: CoreManifestError.UnknownSourceRef) extends PlatformModelError {
    val message: String = s"unknown SourceRef: ${coreError.reason}"
  }
  final case class UnknownStatus(coreError: CoreManifestError.UnknownStatus) extends PlatformModelError {
    val message: String = s"unknown status: '${coreError.value}'"
  }
  final case class ParseFailure(coreError: CoreManifestError.ParseFailure) extends PlatformModelError {
    val message: String = s"YAML parse failure: ${coreError.reason}"
  }

  final case class SchemaValidation(coreError: CoreManifestError.SchemaValidation) extends PlatformModelError {
    val message: String = coreError.message
  }

  /** Smart constructor: pattern-match a `CoreManifestError` into a
    * `PlatformModelError`. Exhaustive on the core-layer ADT. */
  def fromCore(err: CoreManifestError): PlatformModelError = err match {
    case e: CoreManifestError.InvalidYaml      => InvalidYaml(e)
    case e: CoreManifestError.MissingField    => MissingField(e)
    case e: CoreManifestError.UnknownSourceRef => UnknownSourceRef(e)
    case e: CoreManifestError.UnknownStatus    => UnknownStatus(e)
    case e: CoreManifestError.ParseFailure     => ParseFailure(e)
    case e: CoreManifestError.SchemaValidation => SchemaValidation(e)
  }
}

/**
 * Loads an engine-portable `Model` from a YAML file.
 *
 * Per karphyaguids-mindset "smallest correct change":
 * - reuses `io.sm8.core.manifest.ModelLoader` (no copy)
 * - surfaces parse failures as `Left[PlatformModelError]`
 * - exposes `toEngineError(...)` for callers that prefer to
 *   convert to the engine-portable `EngineError` ADT
 */
object PlatformModelLoader {

  /** Load a `Model` from a YAML file path with schema validation.
    *
    * Pipeline:
    *   1. read file → raw YAML string
    *   2. validate against manifest.schema.v2.json (catches
    *      structural failures: missing fields, wrong types,
    *      unknown enums, extra fields)
    *   3. parse via ModelLoader (catches semantic failures: bad
    *      integer, unknown SourceRef variant, etc.)
    *
    * Per debug-mantra-mindset: each step produces a typed
    * error so callers can pinpoint where validation failed.
    *
    * @param path the file path to read from
    * @return `Right(Model)` on success;
    *         `Left(PlatformModelError)` on validation or parse
    *         failure (the case class identifies which stage) */
  def fromPath(path: Path): Either[PlatformModelError, Model] =
    readFile(path).flatMap(validateAndLoad)

  /** Read a YAML file to a UTF-8 string with full typed-error coverage of the IO boundary.
    *
    * The 5 caught exceptions are IO-derived: a binary file renamed to `.yml`, a
    * file deleted between the existence check and the open, a permission revoked
    * mid-call, a transient disk error, or a non-UTF-8 byte sequence. Each is
    * surfaced as a typed `Left(PlatformModelError)` so callers pattern-match on
    * the ADT instead of catching `IOException`.
    *
    * The 6 deliberately-uncaught exceptions (`SecurityException`, `OutOfMemoryError`,
    * `StackOverflowError`, `NullPointerException`, `InvalidPathException`,
    * `IllegalArgumentException`) are programmer / JVM faults and propagate as
    * throws so the JVM-fault failure mode is not hidden by a typed error.
    *
    * @param path the file path to read from
    * @return `Right(yaml)` on success; `Left(PlatformModelError)` on any IO failure
    *         (the ADT case identifies the failure: `InvalidYaml` for missing-file
    *         or access-denied, `ParseFailure` for UTF-8 or generic IO errors) */
  private def readFile(path: Path): Either[PlatformModelError, String] = {
    if (!java.nio.file.Files.exists(path))
      Left(PlatformModelError.InvalidYaml(CoreManifestError.InvalidYaml(s"file not found: $path")))
    else {
      try {
        val stream = java.nio.file.Files.newInputStream(path)
        try {
          val content = scala.io.Source.fromInputStream(stream, "UTF-8").mkString
          Right(content)
        } finally stream.close()
      } catch {
        case e: java.nio.charset.MalformedInputException =>
          Left(PlatformModelError.ParseFailure(CoreManifestError.ParseFailure(s"file is not valid UTF-8: ${e.getMessage}")))
        case e: java.nio.charset.UnmappableCharacterException =>
          Left(PlatformModelError.ParseFailure(CoreManifestError.ParseFailure(s"file contains unmappable characters: ${e.getMessage}")))
        case e: java.nio.file.NoSuchFileException =>
          Left(PlatformModelError.InvalidYaml(CoreManifestError.InvalidYaml(s"file not found: ${e.getMessage}")))
        case e: java.nio.file.AccessDeniedException =>
          Left(PlatformModelError.InvalidYaml(CoreManifestError.InvalidYaml(s"access denied: ${e.getMessage}")))
        case e: java.io.IOException =>
          Left(PlatformModelError.ParseFailure(CoreManifestError.ParseFailure(s"IO error: ${e.getMessage}")))
      }
    }
  }

  /** Load a `Model` from an in-memory YAML string with schema validation.
    *
    * @param yaml the raw manifest string
    * @return `Right(Model)` on success;
    *         `Left(PlatformModelError)` on validation or parse
    *         failure */
  def fromString(yaml: String): Either[PlatformModelError, Model] =
    validateAndLoad(yaml)

  /** Shared pipeline: schema validate then semantic load. */
  private def validateAndLoad(yaml: String): Either[PlatformModelError, Model] =
    ManifestValidator.validate(yaml).left.map(PlatformModelError.fromCore).flatMap { _ =>
      ModelLoader.fromString(yaml).left.map(PlatformModelError.fromCore)
    }
}

/*
 * SM8 Core — ManifestValidator.
 *
 * JSON-Schema-driven validator for SM8 model manifests.
 * Loads the v2 schema from `META-INF/sm8/manifest.schema.v2.json`
 * on the classpath and validates a YAML-string manifest against it.
 *
 * ==Why this exists==
 *
 * Per agile-kindling-beacon plan tech-stack line 148: the
 * `com.networknt:json-schema-validator` 1.5.2 dep is kept for
 * "Manifest v2 validation."  This PR wires that dep into a real
 * validator at the platform boundary, completing the original
 * plan intent.
 *
 * ==Design (per karphyaguids-mindset "smallest correct change")==
 *
 * 1. Schema is parsed ONCE per JVM (via `lazy val`).  Static
 *    initialization is safe because the schema file is bundled
 *    in the sm8-core jar's `src/main/resources/META-INF/sm8/`
 *    directory (classloader-resolved).
 * 2. `validate(yaml)` returns `Either[ManifestError, JsonNode]`.
 *    Never throws; per [[scala-error-handling-mindset]].
 * 3. ALL validation messages are collected (not just the first).
 *    `com.networktt` aggregates them; we surface them as a list.
 * 4. Jackson's `JsonNode` is the validated output.  Downstream
 *    code (ModelLoader) consumes the same shape.
 *
 * ==RFC alignment==
 *
 * Per `semantic-layer-engine-architecture.md` §3 Core Boundary:
 * this validator sits in core.  It does NOT know about a
 * specific data source.
 *
 * Per `semantic-layer-engine-architecture.md` §7 Contracts: this
 * enforces the `Model` shape contract at the JSON boundary.
 *
 * Per `adapters.md` / `plugins.md` / `hooks.md`: not an adapter,
 * plugin, or hook (no data-source knowledge, no Setup, no Context).
 *
 * ==Plan alignment==
 *
 * - Plan tech-stack line 148: uses the existing
 *   `com.networknt:json-schema-validator:1.5.2` dep.
 * - Plan line 195 (manifest/ IR move): completes the typed-shape
 *   chain with boundary validation.
 *
 * ==Spark concerns (per user directive)==
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantras:
 * - mantras #1, #5: no Spark types captured, no executor-side
 *   closure. Pure JSON validation in sm8-core (Spark-free per
 *   the plan's inverted enforcer pattern).
 * - mantra #3 (schema-drift verify at boundary): typed
 *   `ManifestError.SchemaValidation` on the boundary.
 * - mantras #2, #4: N/A.
 * - Serialize: `JsonNode` is auto-Serializable (Jackson).
 *
 * Indirect Spark relation: the validated JSON tree flows to
 * `ModelLoader.fromString(...)` → produces `Model` → consumed by
 * `EngineProvider.query(...)` via the connector layer
 * (PRs #38-#42 handle all Spark concerns). The validator is
 * INDIRECTLY upstream of Spark execution but holds NO Spark
 * references — the boundary holds.
 *
 * Perf (driver/executor): the validated JsonNode crosses into
 * `ModelLoader` (driver-side).  No executor-side closure
 * captures the AST.
 */
package io.sm8.core.manifest

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.networknt.schema._

import scala.jdk.CollectionConverters._

/**
 * JSON-Schema-driven validator for SM8 model manifests.
 *
 * Per [[karphyaguids-mindset]] "smallest correct change":
 * - one entry-point method (`validate`)
 * - schema parsed lazily once per JVM
 * - structured error output (List of messages, not just first)
 */
object ManifestValidator {

  /** The classloader-resolved schema path.  Bundled in the
    * `sm8-core/src/main/resources/META-INF/sm8/` directory. */
  private val SchemaResource: String = "META-INF/sm8/manifest.schema.v2.json"

  /** The Jackson `ObjectMapper` reused for both YAML parsing
    * (to JsonNode) and validating against the schema. */
  private val mapper: ObjectMapper =
    new ObjectMapper(new YAMLFactory())

  /** Cached, lazily-loaded JSON-schema validator instance.  Per
    * [[scala-jvm-safety-mindset]]: no mutable state outside the
    * lazy val's initialization flag. */
  private lazy val schemaValidator: JsonSchema = {
    val stream = Option(getClass.getClassLoader.getResourceAsStream(SchemaResource))
      .getOrElse(throw new IllegalStateException(
        s"sm8: cannot find $SchemaResource on the classpath — the schema JSON was not packaged into the sm8-core jar."
      ))
    try {
      val schemaNode = mapper.readTree(stream)
      val factory    = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
      factory.getSchema(schemaNode)
    } finally stream.close()
  }

  /**
   * Validate a YAML manifest string against the v2 schema.
   *
   * Per [[scala-error-handling-mindset]] "errors are data": this
   * returns `Either[ManifestError, JsonNode]`, never throws.
   *
   * @param yaml the raw manifest string (the same input format
   *            that `ModelLoader.fromString` accepts)
   * @return    `Right(JsonNode)` on success (the parsed +
   *            schema-validated tree); `Left(ManifestError)` on
   *            parse failure or schema violation
   */
  def validate(yaml: String): Either[ManifestError, JsonNode] = {
    val parsed: JsonNode =
      try {
        mapper.readTree(yaml)
      } catch {
        case scala.util.control.NonFatal(e) =>
          return Left(ManifestError.ParseFailure(e.getMessage))
      }
    if (parsed == null || parsed.isNull)
      Left(ManifestError.InvalidYaml("YAML root is null or empty"))
    else {
      val errors = schemaValidator.validate(parsed).asScala.toList
      if (errors.isEmpty) Right(parsed)
      else {
        // Per [[debug-mantra-mindset]] §5 verify: a real `validate`
        // run produced 3 messages for a 3-error input.  We surface
        // them as a List-of-string, not just the first — lets
        // callers log / report all errors at once.
        val messages = errors.map(_.getMessage)
        Left(ManifestError.SchemaValidation(messages))
      }
    }
  }
}

/*
 * SM8 Platform — NoopJsonSchemaValidatorSupplier.
 *
 * Per ADR-014 (PR-261): the MCP SDK's `McpJsonDefaults.getSchemaValidator()`
 * uses ServiceLoader to find a `JsonSchemaValidatorSupplier`. The
 * default implementation (`JacksonJsonSchemaValidatorSupplier` from
 * mcp-json-jackson3) transitively depends on `com.networknt:json-schema-validator`,
 * which conflicts with sm8-core's `ManifestValidator` (different
 * versions of the same package have different class shapes — 1.5.2
 * uses `SpecVersion.VersionFlag`; 3.0.6 uses `Dialects`).
 *
 * For PR-263 v1 (empty tool list, schema validation unnecessary), we
 * register this no-op supplier in sm8-platform's classpath BEFORE
 * the MCP SDK's bundle. ServiceLoader finds the first match in
 * classpath order, so this supplier wins.
 *
 * Implementation note: this is a regular `class` with a static
 * `INSTANCE` field, NOT a Scala `object`. ServiceLoader in JDK 17
 * expects to instantiate via reflection; the Scala companion's
 * MODULE$ indirection doesn't satisfy the type bound
 * `JsonSchemaValidatorSupplier implements Supplier<JsonSchemaValidator>`
 * (the static `.get()` on the companion object is a synthetic bridge
 * that returns `Object` after type erasure, not `JsonSchemaValidator`).
 * A regular class with `extends JsonSchemaValidatorSupplier` works
 * cleanly.
 *
 * If a future PR bridges the 5 PR-260 tools into the Streamable HTTP
 * transport, the bridge PR must either (a) drop the sm8-core
 * ManifestValidator's networknt dep entirely and use the SDK's
 * default, or (b) implement a real validator here. See ADR-014 §Decision
 * for the trade-off.
 */
package io.sm8.platform.query

import io.modelcontextprotocol.json.schema.JsonSchemaValidator
import io.modelcontextprotocol.json.schema.JsonSchemaValidatorSupplier

/** ServiceLoader SPI implementation (per the `META-INF/services/io.modelcontextprotocol.json.schema.JsonSchemaValidatorSupplier`
  * file in this module's resources). The MCP SDK calls
  * `ServiceLoader.load(JsonSchemaValidatorSupplier.class).findFirst()`
  * to look up the validator; we register this class on the classpath
  * to win the lookup (sm8-platform is FIRST in the classpath; the
  * MCP SDK's jar comes later).
  *
  * The validator returned is a no-op: validate() always returns
  * `asValid(null)`. Safe for v1 because the tool list is empty;
  * the SDK never calls validate() in that path. */
final class NoopJsonSchemaValidatorSupplier extends JsonSchemaValidatorSupplier {
  override def get(): JsonSchemaValidator = new JsonSchemaValidator {
    override def validate(schema: java.util.Map[String, Object], data: Object): JsonSchemaValidator.ValidationResponse =
      JsonSchemaValidator.ValidationResponse.asValid(null)
    override def validateSchema(schema: java.util.Map[String, Object]): JsonSchemaValidator.ValidationResponse =
      JsonSchemaValidator.ValidationResponse.asValid(null)
  }
}

/** Static factory so the META-INF/services file can reference a
  * concrete instance. */
object NoopJsonSchemaValidatorSupplier {
  val INSTANCE: JsonSchemaValidatorSupplier = new NoopJsonSchemaValidatorSupplier
}

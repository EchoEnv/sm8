package io.sm8.core.engine

/** Engine-portable engine-identity ADT — Phase 2 contract. Mirrors
 * the design doc §4.5 "EngineIdentity".
 *
 * An 
 * adapter. It's surfaced in MCP `describe_model`, OKF generation,
 * and audit events. Three fields:
 *
 * - `name`: the engine's wire-stable label (e.g. "trino",
 *  "spark", "databricks"). Renaming is a BREAKING change to
 *  MCP clients.
 * - `nativeVersion`: the engine's native version (Trino 0.286,
 *  Spark 3.5.8, etc.) — informational, for diagnostics.
 * - `engineAdapterVersion`: the version of THIS adapter
 *  (semanticdf-trino 0.2.4, etc.) — informational, for
 *  diagnostics and reproducibility.
 *
 * ==Why a case class (vs. just a String)==
 *
 * The design uses 3 fields to distinguish the engine's NATIVE
 * version (informational) from the ADAPTER's version (which can
 * matter for reproducibility — same model + same adapter version
 * produces the same SQL). Per [[scala-data-driven-refactor-mindset]] §1
 * ("data is data, behavior lives elsewhere"): the identity is
 * pure data; no behavior on the case class.
 *
 * ==Why core (engine-portable)==
 *
 * The identity SHAPE (3 fields, all String) is universal across
 * engines. The VALUES are engine-specific (each adapter sets its
 * own name/version at construction).
 *
 * ==Data-driven mantra compliance==
 *
 * - Pure data: case class (no behavior)
 * - Equality auto-derived
 * - `Product with Serializable` for Java-serialization round-trip
 *
 * ==Boundary contract==
 *
 * Zero Spark imports. Verifiable by:
 * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/engine/EngineIdentity.scala`
 */
final case class EngineIdentity(
 name:    String,
 nativeVersion:  String,
 engineAdapterVersion: String) extends Product with Serializable
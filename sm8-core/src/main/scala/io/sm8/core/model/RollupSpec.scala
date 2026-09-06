/*
 * SM8 Core — RollupSpec (Ticket #3 of
 * docs/wayfinder/2026-09-06-pre-aggregation.md; design in
 * docs/adr/0022-pre-aggregation-sm8-native.md).
 *
 * A user-declared pre-aggregation on a Model: which (dimensions,
 * measures) shape is materialized (by a later connector-side
 * materializer, Ticket 4 of the map), optionally at a time grain.
 *
 * ==Pure data==
 * Per the Model header contract: `Model` carries NO methods beyond
 * the smart constructor; likewise `RollupSpec` is pure data — the
 * refs are names of the HOST model's own dimensions/measures, and
 * validation (unknown refs, duplicate rollup names) lives in
 * `ModelValidator.validate`, called once by `Model.of`.
 *
 * ==Why name-strings, not object refs==
 * The declaration is author-facing YAML (ModelLoader parses it).
 * Name-strings match how joins reference right models and keep the
 * YAML round-trip trivial. The validator fails loud on unknown
 * names at the boundary (same discipline as calculatedMeasures —
 * never a silent no-op).
 *
 * ==Why timeGrain: Option[String]==
 * Matches the existing engine-portable precedent:
 * `io.sm8.core.engine.QueryRequest.timeGrain: Option[String]`
 * (sm8-core/EngineProvider.scala). The grain vocabulary
 * (day/hour/month, ...) is engine-facing; core treats it as an
 * opaque label in v1 — raw string equality is fail-safe (a
 * vocabulary mismatch routes to the base path, a perf loss, never
 * wrong results). Ticket 4's matcher and Ticket 5's materializer
 * must share one normalization; pinned there. No IO, no Spark
 * types.
 */
package io.sm8.core.model

/** Declared pre-aggregation on a Model.
  *
  * @param name       unique rollup name within the model
  *                   (uniqueness enforced by `ModelValidator`)
  * @param dimensions names of the HOST model's dimensions this
  *                   rollup groups by (each must exist on the model;
  *                   validated at the boundary). No default: the
  *                   declaration site must state its intent — a
  *                   ref-less rollup would be a silent no-op.
  * @param measures   names of the HOST model's measures this rollup
  *                   pre-aggregates (BASE measures only — a
  *                   calculated-measure ref is a distinct validation
  *                   error; validated at the boundary). No default
  *                   for the same reason.
  * @param timeGrain  optional opaque grain label (e.g. "day"); None
  *                   = grain-agnostic rollup
  *
  * @note No defaults on `dimensions`/`measures`: a ref-less rollup
  * is a degenerate declaration (it would vacuously match by
  * subsumption in Ticket 4's router), so the declaration site must
  * state its intent explicitly. `timeGrain` defaults to None
  * (grain-agnostic).
  */
final case class RollupSpec(
    name: String,
    dimensions: List[String],
    measures: List[String],
    timeGrain: Option[String] = None
) extends Product with Serializable

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
  * @param grainDimension when `timeGrain` is defined, the SINGLE
  *                   dimension in `dimensions` whose values are
  *                   bucket-truncated at `timeGrain`. Must name a
  *                   dimension already in `dimensions` whose
  *                   declared (or resolved) data type is `Date` or
  *                   `Timestamp` — validated in `ModelValidator`.
  *                   Co-presence with `timeGrain` is itself
  *                   validated: a grain without an axis and an axis
  *                   without a grain are each a distinct validation
  *                   error. Inference is deliberately unsupported
  *                   (a value-domain guess silently picks a column
  *                   and breaks when two date dims exist — order
  *                   date vs ship date); the axis is stated at the
  *                   declaration site.
  * @param freshness optional freshness policy (ADR-0030 D3,
  *                   Tier 2): `None` (default) = route normally
  *                   (pre-Tier-2 behavior); `Some(FinalRequired)` =
  *                   the connector's resolution layer refuses
  *                   non-final buckets (`RollupBucketStale`).
  *                   Requires `timeGrain` + `grainDimension`
  *                   (validated); core never looks up watermarks
  *                   (RFC §3 — the watermark table is connector
  *                   side).
  * @param cascadeSource optional cascade declaration (ADR-0031 D4,
  *                   Tier 2): the NAME of the finer-grained rollup
  *                   (on the SAME model) this rollup is built from
  *                   (e.g. daily declares `cascadeSource: hourly`).
  *                   A name-string, not an object ref: same
  *                   name-string discipline as `dimensions`/
  *                   `measures` (YAML round-trip; existence +
  *                   eligibility validated by
  *                   `ModelValidator.validateCascadeDag`, which
  *                   walks the declaration graph and refuses
  *                   cycles/self-cycles/unknown names at
  *                   deployment time — never at refresh time).
  *                   `None` = build from base (pre-cascade
  *                   behavior unchanged). The eligibility predicate
  *                   itself lives in `CascadeContract.eligibility`
  *                   (pure, ADR-0031 D1).
  *
  * @note No defaults on `dimensions`/`measures`: a ref-less rollup
  * is a degenerate declaration (it would vacuously match by
  * subsumption in Ticket 4's router), so the declaration site must
  * state its intent explicitly. `timeGrain`/`grainDimension`
  * default to None (grain-agnostic); the co-presence rule makes
  * the mixed half-declared state unreachable.
  */
final case class RollupSpec(
    name: String,
    dimensions: List[String],
    measures: List[String],
    timeGrain: Option[String] = None,
    grainDimension: Option[String] = None,
    freshness: Option[FreshnessPolicy] = None,
    cascadeSource: Option[String] = None
) extends Product with Serializable

// SM8 Core — DecisionHintsPolicy.
//
// ADR-009-d item 13 honor-or-reject helper, shared across engine
// adapters that cannot consume a per-query DecisionHints. The
// platform's portability claim ("any adapter reads the decision") is
// only sound if a decided-but-ignored field surfaces as a typed
// `UnsupportedCapability` named by the producing platform meta key.
//
// Per scala-data-driven-refactor §3, the duplicated decision-logic
// between InMemoryEngineProvider and TrinoEngineProvider is a known
// refactor target. The pre-PR-202 implementation had this logic
// duplicated 1:1 in each adapter; PR-202 closed the cross-engine
// inconsistency (Trino now mirrors InMemory) but explicitly
// deferred the extraction to a follow-up. This file IS that
// follow-up — the smallest refactor that eliminates the
// duplication without changing observable behavior.
//
// Deterministic ordering (broadcastArmed → broadcastThresholdBytes →
// skewArmed) is the platform contract — see DecisionHints.scala:96
// and ADR-009-d v0.3. Different orderings across adapters would be a
// silent semantic shift. The order is locked in by:
//   - InMemoryEngineProviderSpec.scala (PR-200 + prior)
//   - TrinoEngineProviderSpec.scala (PR-202)
//   - DecisionHintsPolicySpec.scala (this PR, new)
//
// Engine-field and display-name conventions are ADAPTER-SPECIFIC:
//   - engine field: `<connector-artifactId>` (e.g. "in-memory-connector")
//   - display name: bare engine name in the message body
//     (e.g. "in-memory engine")
// Matches the pattern observed across all 3 reference connectors.
//
// Boundary contract: zero Spark imports, zero plugin imports. The
// helper is in `core` so any adapter (in-memory / spark / trino /
// future) can reference it without a layer leak. The platform fold
// (`EngineService.runQueryWithHooks`) extracts the meta keys and
// builds the DecisionHints ADT; adapters never read `Context.meta`
// directly.
package io.sm8.core.engine

/**
 * ADR-009-d item 13 honor-or-reject helper for engine adapters
 * that cannot consume a per-query DecisionHints. Different adapters
 * have different capabilities:
 *   - in-memory: cannot broadcast or skew (always-empty stub)
 *   - trino: stub (no native broadcast/skew config)
 *   - spark: HONORS the decision (consumes broadcastArmed /
 *     broadcastThresholdBytes / skewArmed directly via `eCtx`)
 * Adapters in the first two categories use this helper to surface
 * a uniform typed error.
 */
object DecisionHintsPolicy {

  /** Return the platform meta key of the first decided field, or
    * `None` if no field is decided. The deterministic order
    * (broadcastArmed → broadcastThresholdBytes → skewArmed) is the
    * platform contract — see DecisionHints.scala:96. Caller uses
    * `None` to fall through to its empty-fold behavior.
    *
    * Field semantics (mirror DecisionHints.scala:43-65):
    *   - `broadcastArmed.isDefined` matches BOTH `Some(true)` AND
    *     `Some(false)`. Per DecisionHints.scala:8, `Some(false)` is
    *     "oracle disarmed" — a real plugin decision, not a no-op.
    *     Honoring it (rejecting it on a non-consumer adapter) is the
    *     correct move. PR-200 review MEDIUM-2 tracked the
    *     `.contains(true)` alternative as a separate follow-up —
    *     that alternative would have treated `Some(false)` disarm as
    *     a no-op, silently violating the DecisionHints.scala:8
    *     contract.
    */
  def firstDecidedCapabilityKey(dh: DecisionHints): Option[String] =
    if (dh.broadcastArmed.isDefined) Some("sm8.broadcast.arm")
    else if (dh.broadcastThresholdBytes.isDefined) Some("sm8.broadcast.thresholdBytes")
    else if (dh.skewArmed.isDefined) Some("sm8.skew.arm")
    else None

  /** Honor-or-reject: return a typed `UnsupportedCapability` if the
    * context has a decided field this adapter cannot honor.
    *
    * @param ctx the per-query engine context (may be `null` for
    *            legacy smoke-test paths that bypass the platform fold;
    *            those return `None` so the adapter falls through to its
    *            empty-fold behavior — preserves the pre-refactor
    *            `if (ctx == null) None` short-circuit)
    * @param engineField the typed `engine` value used by this adapter
    *                    (e.g. `"in-memory-connector"`,
    *                    `"trino-connector"`)
    * @param engineDisplayName human-readable engine name embedded in
    *                          the message body (e.g.
    *                          `"in-memory engine"`, `"trino engine"`)
    * @return `None` when ctx is null OR `ctx.decisionHints` is None OR
    *         all DecisionHints fields are None (empty fold). Returns
    *         `Some(UnsupportedCapability)` when at least one field is
    *         decided — the deterministic ordering picks the first one.
    */
  def honorOrReject(
      ctx:             EngineContext,
      engineField:     String,
      engineDisplayName: String
  ): Option[EngineError.UnsupportedCapability] =
    if (ctx == null) None
    else ctx.decisionHints.flatMap { dh =>
      firstDecidedCapabilityKey(dh).map { key =>
        EngineError.UnsupportedCapability(
          engine     = engineField,
          capability = key,
          message    = s"sm8: $engineDisplayName cannot honor decided field '$key'; " +
            "route to an engine with a native broadcast/skew config or drop the broadcast/skew plugin"
        )
      }
    }
}

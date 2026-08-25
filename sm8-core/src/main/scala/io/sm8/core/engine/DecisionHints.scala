// SM8 Core — DecisionHints.
//
// Per-query broadcast + skew arm decisions from any registered
// plugin's PreExecute hook. Both arm fields are Optional[Boolean]:
// None means "no oracle registered for this decision; the adapter
// may use its inline fallback". Some(true) means "oracle armed".
// Some(false) means "oracle disarmed" — the adapter must NOT arm,
// even if its inline rule would.
//
// The broadcast threshold bytes (Optional[Long]) cross the boundary
// too: when Some, the adapter uses it as the byte-budget value
// directly (instead of reading spark.sql.autoBroadcastJoinThreshold
// from the session). When None, the adapter falls back to the
// session default.
//
// Per-query session design: typed transport. The decision lives
// in plugins/*; only this typed value crosses the boundary into
// the spark connector. Zero SDK surface (Context, HookManager,
// Plugin, PreHook all frozen).
//
// Why a closed ADT, not a free-form map: same data-driven discipline
// as EngineContext + its sub-ADTs. A closed shape forces every
// adapter to handle the same set of decisions. A free-form
// Map[String, Any] would let adapters accidentally invent decision
// names that the plugin cannot classify.
//
// Why core (engine-portable): the decision SHAPE is engine-portable;
// the byte value is a numeric contract (Long); the boolean arm is
// universal. The engine adapter applies it; the SHAPE is not.
//
// Boundary contract: zero Spark imports. The decision is delivered
// as a typed value on EngineContext; no Context.meta string lookups,
// no engine internals leak.
package io.sm8.core.engine

/**
 * Per-query broadcast + skew arm decisions from any registered
 * plugin's PreExecute hook. The fields are independently Optional;
 * a plugin may register only the broadcast decision (skipping the
 * skew decision) and the adapter sees None for skew (inline
 * fallback fires).
 *
 * Per-decision oracle semantics:
 *
 * - `broadcastArmed` is the FULL arm decision. When `Some(b)`, the
 *   spark connector arms or disarms the broadcast byte-gate
 *   according to `b`, regardless of any inline presence rule. When
 *   `None`, the inline presence rule (`model.joins.exists(_.estimatedRows.isDefined)`)
 *   is the only arm gate.
 * - `skewArmed` is the PARTIAL arm decision. When `Some(true)`,
 *   the spark connector arms the skew seed only if the model ALSO
 *   declares at least one join with `estimatedRows` — a
 *   `Some(true)` on a model with no estimated joins is dropped,
 *   because the AQE skew factor has no purpose without a
 *   declared large-row join. When `Some(false)`, the spark
 *   connector disarms the skew seed (a no-`Some(f)`-required
 *   disarming). When `None`, the inline presence rule plus
 *   `JoinHints.skewFactor = Some(f)` is the gate.
 * - `broadcastThresholdBytes` is the byte budget the plugin chose
 *   for the broadcast seed. When `Some(bytes)`, the spark
 *   connector uses `bytes` as the byte-gate budget directly
 *   (instead of reading `spark.sql.autoBroadcastJoinThreshold`
 *   from the session). When `None`, the session default applies.
 *   The unit is BYTES; a row-count value would be misinterpreted
 *   as a byte budget and disarm most real joins.
 *
 * Asymmetry rationale: broadcast and skew arm on different
 * evidence. Broadcast is a structural choice (small side fits
 * the byte gate); a model with no joins can still meaningfully
 * broadcast nothing, so the oracle can arm unconditionally.
 * Skew is a probabilistic choice (large side has skewed keys);
 * without an estimated large-row join, a skew factor has no
 * observable effect, so the connector refuses to write one.
 * Plugins that want unconditional skew arm must register a
 * model with at least one `estimatedRows` declaration.
 *
 * Typed transport: the decision crosses the adapter boundary
 * as this typed value on `EngineContext.decisionHints`. The SDK
 * `Context.meta` map (which plugins write into) stays in the
 * platform; the platform fold (`EngineService.runQueryWithHooks`)
 * extracts the meta keys and builds this ADT. Adapters never
 * read `Context.meta` directly.
 *
 * @param broadcastArmed the broadcast arm decision from the plugin
 *                       (None = no oracle; Some(true) = arm; Some(false) = disarm)
 * @param skewArmed the skew arm decision from the plugin
 *                  (None = no oracle; Some(true) = arm but still
 *                  requires a model with an `estimatedRows`-bearing
 *                  join; Some(false) = disarm)
 * @param broadcastThresholdBytes the byte-gate threshold the
 *                                plugin chose for the broadcast
 *                                seed, in BYTES (None = use the
 *                                session default; Some(bytes) = use
 *                                this byte value directly)
 */
final case class DecisionHints(
 broadcastArmed: Option[Boolean] = None,
 skewArmed: Option[Boolean] = None,
 broadcastThresholdBytes: Option[Long] = None)
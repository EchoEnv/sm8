package io.sm8.core.engine

// ADR-009-g Fix 2 + Fix 3: single-source CachePolicy ADT. The engine-side
// ADT was deleted; EngineContext.cachePolicy now carries the model-side
// io.sm8.core.model.CachePolicy. The fold (EngineService.runQueryWithHooks
// initialCtx.meta construction) propagates model.defaultPolicies.cache to
// the cache-plugin hooks via this imported type.
import io.sm8.core.model.CachePolicy

/** Engine-portable typed policies for query execution —
 * the engine-portable contract. Per the design doc §4
 * "Engine contract", `EngineContext` carries the typed policies
 * + hints the caller asks the engine to apply for a single query:
 * `cachePolicy` (cache mode), `joinHints` (broadcast / skew /
 * strategy), `decisionHints` (per-query plugin oracle), and
 * `cacheKey` (canonical cache key). The engine adapter adapts each
 * policy to its supported form (per the request-policy matrix in
 * §4.5.3).
 *
 * ==Field lifecycle (PR-199 cleanup)==
 *
 * Pre-PR-199 carried 7 fields including `auditPolicy`,
 * `timeout`, `cancellation` — those were dead in production
 * (zero consumers across connectors/, plugins/, sm8-platform/,
 * sm8-core/) and have been removed per karpathy-guidelines
 * "dead code is a smell". The cancelled `engine-side
 * AuditPolicy` + `CancellationCapability` ADTs are removed
 * alongside.
 *
 * ==Why a typed ADT (not a String map)==
 * The design doc says: "These questions must not be answered by
 * string parameters in `EngineContext`." A closed ADT forces every
 * engine adapter to handle the closed set of policies. Free-form
 * strings would let adapters accidentally invent policy names that
 * the consumer can't classify.
 * ==Why core (engine-portable)==
 * The remaining fields are universal across query engines (every
 * engine has the notion of cache mode + join hints). The engine
 * adapter adapts them; the SHAPE is engine-portable.
 * ==Data-driven mantra compliance==
 * - Pure data: sealed traits + final case classes (no behavior)
 * - Equality auto-derived
 * - `Product with Serializable` for Java-serialization round-trip
 * ==Boundary contract==
 * Zero Spark imports. Verifiable by:
 * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/engine/EngineContext.scala`
 * ==Consolidation status==
 * The follow-up implementation will add:
 * - `PortableModel` (the full portable model type) to replace the
 *  `Any` placeholder in `Engine.compile(model: Any,.)`
 * - `PortableExpr` / `RelOp` (the portable IR) to replace the
 *  `Any` placeholder in `Engine.execute(plan: Any,.)`
 */
final case class EngineContext(
 cachePolicy:  CachePolicy,
 joinHints:   JoinHints,
 // Per-query decision oracle: populated by the platform
 // engineExecutor from the post-PreExecute Context.meta; None means
 // no oracle (adapter uses its inline fallback). Typed transport
 // for the plugin's decision (the decision LOGIC stays in
 // plugins/*; only this typed value crosses the boundary). NOT in
 // the SDK (Context/HookManager/Plugin are frozen).
 decisionHints: Option[DecisionHints] = None,
 // PR-197 (Round 1 audit HIGH-3): optional platform-computed cache
 // key, propagated from `EngineService.runQueryWithHooks` via
 // `CacheBridge.platformCacheKey(...)`. When `Some(key)` the engine
 // adapter MUST use this key for its internal cache key derivation
 // (instead of computing its own local placeholder), so the
 // adapter-computed key matches the platform canonical key that
 // `CachePlugin`'s `EngineHookRequest.cacheKey` consults. When
 // `None` (the default for legacy / bare-deploy paths that bypass
 // `EngineService`), the adapter falls back to its local
 // smoke-test derivation. Per scala-data-driven-refactor-mindset
 // "data is data": the canonical key is a bijection between
 // request-shape and key, length-prefixed to avoid collisions.
 cacheKey: Option[String] = None) extends Product with Serializable

object EngineContext {

 /**
 * Default context for queries that don't specify any policies. The
 * `cachePolicy = NoCache` default matches `model.defaultPolicies`
 * in the absence of a model-supplied policy — the platform fold
 * (EngineService.runQueryWithHooks) overrides this with the
 * model's actual policy before the engine sees it.
 */
 val defaultContext: EngineContext = EngineContext(
 cachePolicy  = CachePolicy.NoCache,
 joinHints   = JoinHints())
}

// ADR-009-g Fix 2: the engine-side CachePolicy ADT (previously declared
// here as a 4-case sealed trait: NoCache, ReadThrough, WriteThrough,
// ReadOnly) is DELETED. The single-source ADT lives in
// io.sm8.core.model.CachePolicy (3 cases: NoCache, ReadThrough(name),
// WriteThrough(name)). All callers reference the model-side type.

// PR-199 (Round 1 audit pre-existing HIGH cleanup): the engine-side
// `AuditPolicy` sealed trait + the `CancellationCapability` sealed
// trait (4 cases each) were DEAD — zero production consumers
// anywhere in the codebase. The `EngineContext` fields that
// nominally carried them were never read; the runtime gate uses
// the SDK's Restate journal retry, NonFatal discipline (PR-176),
// and the platform's hook runner. Per karpathy-guidelines
// "dead code is a smell", both ADTs are removed.

// -- JoinHints: per-query optimization hints ---

/** Engine-portable join-hint ADT. Pure data — case class with
 * optional fields. The engine adapter maps each Option to its
 * engine's native form (e.g. `broadcastRightBelowBytes` → Spark's
 * `autoBroadcastJoinThreshold`).
 * All fields are Optional so the caller can leave any unset. The
 * adapter interprets `None` as "no preference" for that hint. */
final case class JoinHints(
 broadcastRightBelowBytes: Option[Long] = None,
 skewFactor:    Option[Int] = None,
 preferredStrategy:  Option[JoinStrategy] = None) extends Product with Serializable

/** Engine-portable join strategy preference. The engine picks the
 * best match (or rejects if the strategy is unsupported). */
sealed trait JoinStrategy extends Product with Serializable

object JoinStrategy {
 /** Broadcast join — push the smaller side to all executors.
 * Spark: `broadcast()`. Trino: `BROADCAST`. */
 case object Broadcast extends JoinStrategy

 /** Shuffle-hash join. Most common default. */
 case object ShuffleHash extends JoinStrategy

 /** Sort-merge join. Required for range joins on some engines
 * (e.g. Trino). */
 case object SortMerge extends JoinStrategy
}


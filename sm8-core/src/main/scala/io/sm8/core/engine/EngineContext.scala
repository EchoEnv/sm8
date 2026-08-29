package io.sm8.core.engine

import scala.concurrent.duration.Duration

// ADR-009-g Fix 2 + Fix 3: single-source CachePolicy ADT. The engine-side
// ADT was deleted; EngineContext.cachePolicy now carries the model-side
// io.sm8.core.model.CachePolicy. The fold (EngineService.runQueryWithHooks
// initialCtx.meta construction) propagates model.defaultPolicies.cache to
// the cache-plugin hooks via this imported type.
import io.sm8.core.model.CachePolicy

/** Engine-portable typed policies for query execution —
 * the engine-portable contract. Mirrors the design doc §4 "Engine contract".
 * The `EngineContext` carries the typed policies the caller asks the
 * engine to apply for a single query: materialize (persist
 * intermediate results), cache (result-cache mode), audit (where
 * audit events go), join hints (broadcast / skew), timeout, and
 * cancellation mechanism. The engine adapter adapts each policy
 * to its supported form (per the request-policy matrix in §4.5.3).
 * ==Why a typed ADT (not a String map)==
 * The design doc says: "These questions must not be answered by
 * string parameters in `EngineContext`." A closed ADT forces every
 * engine adapter to handle the closed set of policies. Free-form
 * strings would let adapters accidentally invent policy names that
 * the consumer can't classify.
 * ==Why core (engine-portable)==
 * The policies are universal across query engines (every engine has
 * the notion of "cache mode" or "join hint"). The engine adapter
 * adapts them; the SHAPE is engine-portable.
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
 auditPolicy:  AuditPolicy,
 joinHints:   JoinHints,
 timeout:   Duration,
 cancellation:  CancellationCapability,
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

 /** Default context for queries that don't specify any policies. Each
 * engine adapter is responsible for choosing sensible defaults
 * for each policy (e.g. "no materialize", "no cache", "best-effort
 * audit"). The defaults here are placeholders that the caller
 * would typically override. */
 val defaultContext: EngineContext = EngineContext(
 cachePolicy  = CachePolicy.NoCache,
 auditPolicy  = AuditPolicy.NoAudit,
 joinHints   = JoinHints(),
 timeout   = Duration.Inf,
 cancellation  = CancellationCapability.Unsupported)
}

// ADR-009-g Fix 2: the engine-side CachePolicy ADT (previously declared
// here as a 4-case sealed trait: NoCache, ReadThrough, WriteThrough,
// ReadOnly) is DELETED. The single-source ADT lives in
// io.sm8.core.model.CachePolicy (3 cases: NoCache, ReadThrough(name),
// WriteThrough(name)). All callers reference the model-side type.


// -- AuditPolicy: where audit events go ---

/** Engine-portable audit-event destination policy. */
sealed trait AuditPolicy extends Product with Serializable

object AuditPolicy {
 /** No audit. Adapter skips audit emission. */
 case object NoAudit extends AuditPolicy

 /** Audit to the engine's default sink (typically an in-memory
 * ring buffer for the duration of the request, or a
 * server-configured Restate journal entry). */
 case object EngineDefault extends AuditPolicy
}

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

// -- CancellationCapability: how the engine should be cancelled ---

/** Engine-portable cancellation-mechanism ADT. The caller picks
 * one; the engine adapter uses it if it supports the requested
 * mechanism, or returns `EngineError.CancellationFailed` if not. */
sealed trait CancellationCapability extends Product with Serializable

object CancellationCapability {
 /** Cooperative cancellation: engine checks a flag between
 * pipeline stages. Universal support. */
 final case class Cooperative(requestId: String) extends CancellationCapability

 /** Spark job-tag cancellation: engine attaches the `requestId`
 * as a Spark job tag; the runtime cancels jobs with that tag.
 * Spark-specific but listed for parity. */
 final case class SparkJobTag(requestId: String) extends CancellationCapability

 /** Remote-statement cancellation: engine issues an async
 * cancel against the backend's statement handle. Trino uses
 * this for `io.trino.client.StatementClient.cancel()`. */
 final case class RemoteStatement(requestId: String) extends CancellationCapability

 /** Engine doesn't support cancellation for this request. The
 * caller is expected to bound the query with a finite timeout. */
 case object Unsupported extends CancellationCapability
}
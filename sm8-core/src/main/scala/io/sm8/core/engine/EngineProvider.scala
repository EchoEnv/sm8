package io.sm8.core.engine

import io.sm8.core.model.Model

/** Engine-portable engine-provider trait — the engine-portable contract.
 * Mirrors the design doc §6.4 "EngineProvider".
 * ==Why a trait (not a concrete class)==
 * Per scala-data-driven-refactor \u00a71: data in core, behavior in
 * adapters. The PROVIDER trait is the data shape (the contract
 * MCP needs); the IMPLEMENTATIONS (SparkEngineProvider,
 * TrinoEngineProvider) are the engine-specific behavior. The trait
 * lives in core; the implementations live in each engine adapter.
 * ==Why `available: Boolean` (not just `identity: EngineIdentity`)==
 * Per the design's `EngineRegistry`: "the registry's `select`
 * filters availability". A provider can be registered but
 * unavailable (e.g. Spark Connect URL not configured; Trino cluster
 * not reachable). `available` is a runtime check, not a config
 * check.
 * ==Why `query` returns `Either[EngineError, PortableQueryResult]`==
 * Per the design \u00a76.4: every engine adapter's execute shape.
 * MCP consumers get a uniform `PortableQueryResult` shape (from PR
 * #400). `Either[EngineError,.]` lets the registry surface typed
 * errors uniformly \u2014 not exceptions.
 * ==Why `model: Model` (not `SemanticTable`)==
 * The MCP is engine-portable. The `Model` is the engine-portable
 * shape (from core). The `SemanticTable` is the Spark-specific
 * shape (from the spark adapter). The provider receives a `Model`
 * and translates it to its engine's native shape internally.
 * Extends `Serializable` so that `EngineRegistry` (which stores
 * `Map[String, EngineProvider]`) can be safely serialized for
 * `Restate.run` journal capture (forward-looking). Concrete
 * providers must therefore be Serializable themselves — enforced
 * at compile time by this trait. */
trait EngineProvider extends Serializable {

 /** Wire-stable engine label. */
 def identity: EngineIdentity

 /** Runtime availability check. Per the design: "the registry's
 * `select` filters availability". `true` iff the provider is
 * configured AND can serve queries right now. */
 def available: Boolean

 /** Lifecycle hook for resource cleanup at shutdown. the current implementation: the sm8-server runtime installs this in a
 * `sys.addShutdownHook` so realized SparkSessions stop on exit.
 * The default is a no-op (in-memory connectors have nothing to
 * release). Safe to call multiple times.
 */
 def close(): Unit = ()

 /** Execute a query against this engine. Returns the
 * engine-portable `PortableQueryResult` (not the engine-native
 * shape).
 * @param model the portable model to query
 * @param request the MCP query request shape (dimensions,
 *     measures, where, having, orderBy, limit, etc.)
 * @param ctx  the engine context (timeout, cancellation,
 *     audit policy, etc.)
 * @return   either a `PortableQueryResult` or a typed
 *     `EngineError` (e.g. `EngineUnavailable`,
 *     `ConnectionFailed`, `QueryTimedOut`) */
 def query(
  model: Model,
  request: io.sm8.core.engine.QueryRequest,
  ctx:  EngineContext): Either[EngineError, PortableQueryResult]

 /** Return a human-readable plan description (no execution).
 * Mirrors `Engine.explain`. Used by the `explain` tool. */
 def explain(
  model: Model,
  request: io.sm8.core.engine.QueryRequest,
  ctx:  EngineContext): Either[EngineError, String]

 /** Typed URL realization (added 2026-08-15 (typed URL realization per RFC `adapters.md` Rule 4)).
 * A connector that supports URL-based connection (Spark master
 * URL, Trino JDBC URL, DuckDB path, HTTP endpoint, etc.)
 * implements this method to build its concrete client/session
 * from a plain string. The deployment module calls this typed
 * method — it does NOT reflect over the class to find a
 * `(String)` ctor. Per-connector `realize()` validates its own
 * URL grammar; the deployment module does NOT validate.
 * Default: `None` — this provider does not support URL-based
 * realization (e.g. an embedded/test provider). The deployment
 * keeps the stub as-is; `wire()` fails loud if no available
 * provider remains.
 * Per RFC §3: the connector is the ONLY piece that knows about
 * connection strings, drivers, or sessions. The platform and
 * the deployment module hold only the string.
 * @param url the connection URL (e.g. `spark://host:7077`,
 *   `spark-connect://host:15002`, `local[*]`,
 *   `jdbc:trino://host:8080`)
 * @return `Some(realizedProvider)` on success;
 *   `None` if this provider does not support URL
 *   realization or the URL is not valid for it
 */
 def realize(url: String): Option[EngineProvider] = None
}

/** Engine-portable query-request shape. The shape is
 * engine-portable (no Spark, no Trino types). The provider
 * translates to its engine's native shape.
 * ==Why no `where` / `having` / `orderBy` for v1==
 * The `core.predicate.Predicate` and `predicate.Predicate`
 * (spark-adapter) are TWO different types per the v0.3.0
 * DE review (Predicate type duplication). The MCP server
 * currently uses the spark-adapter Predicate for filter
 * translation. For PR 5, the engine-portable `QueryRequest`
 * deliberately OMITS `where` / `having` — the MCP Query
 * handler in `semanticdf-mcp` keeps its own filter logic on
 * the legacy path. A future PR aligns the predicate types
 * (per the design's "Predicate consolidation" plan in
 * design §6.2).
 * ==Why v0.3.2 adds `where` (raw SQL)==
 * The Platform's `QueryService` (PR #443's design doc) needs
 * to pass a raw-SQL `where` from its wire DTO through the
 * engine registry. the current implementation introduces this field as
 * an engine-specific raw filter:
 * - Spark: applies via `df.filter(where)`.
 * - Other engines (Trino / DuckDB / PG / Hera / UC / HMS):
 *  may convert raw SQL to their native syntax, apply as-is
 *  if compatible, or return `EngineError.FeatureDeferred`
 *  if they don't support the syntax. The field is
 *  deliberately named `where` to match the platform's
 *  wire DTO; it is NOT promoted to a fully-portable
 *  typed `FilterSpec` yet (that's deferred to a follow-up
 *  the design contract).
 * ==Semantic caveat==
 * Adding raw SQL to an otherwise-typed request shape is a
 * pragmatic compromise for the current implementation. The long-term shape
 * (typed FilterSpec) is documented as future work in
 * `docs/design/v0.3.2-platform-core-model-design.md` §6. */
final case class QueryRequest(
 model:  String,
 dimensions: Seq[String] = Seq.empty,
 measures: Seq[String] = Seq.empty,
 limit:  Option[Long] = None,
 timeGrain: Option[String] = None,
 timeRange: Option[(String, String)] = None,
 /** Raw SQL filter, applied after compile + before limit
  * (matches the legacy `CacheBridge.executeQuery` order).
  * `None` = no additional filter beyond the model's typed
  * `filters`. Some `Seq[String]` — applies the raw SQL
  * via `df.filter(where)` on Spark; engines decide how to
  * handle it (see trait doc above). */
 where:  Option[String] = None,
 /** Engine-portable typed filters, applied AFTER the model's
  * compile-time filters and BEFORE where (the raw SQL
  * field). Each FilterSpec carries a name (for diagnostics)
  * and an Expr predicate. Per scala-data-driven-refacer:
  * Expr is pure data; the engine-specific compile is in the
  * adapter. Per scala-error-handling: at the IO boundary,
  * unsupported filter shapes surface as typed
  * EngineError.UnsupportedCapability. */
 filters: List[io.sm8.core.model.FilterSpec] = Nil,
 /** the current implementation (the design contract current implementation): typed aggregate measures. The
  * phantom `[Nothing]` is the "top" phantom (per the typed
  * at the consumer's `object Refs {. }` site. The phantom
  * `Nothing` is `extends Nothing` (the bottom type) so any
  * typed measure `TypedAggregateCall[M]` is a subtype of
  * `TypedAggregateCall[Nothing]` — variance-safe per
  *  §1.
  * Per karpathy-guidelines §3 (surgical): default = Nil (no
  * behavior change for existing 19 callers). */
 aggregateMeasures: Seq[io.sm8.core.rel.TypedAggregateCall[Nothing]] = Nil,
 /** the current implementation: typed having predicates (per the design contract). */
 having:  Seq[io.sm8.core.rel.Having[Nothing]]    = Nil,
 /** the current implementation: typed partition hints (best-effort; AQE may override
  * per scala-spark-batch-bugs §2). */
 partitionBy: Seq[io.sm8.core.rel.PartitionBy[Nothing]]  = Nil,
 /** the current implementation: typed window specs (rank-only minimal; the design contract). */
 window:  Seq[io.sm8.core.rel.TypedWindow[Nothing, Nothing]] = Nil,
 /** the current implementation: typed order-by columns (used by window + sort). */
 orderBy:   Seq[io.sm8.core.model.TypedDimension[Nothing]]  = Nil,
 /** the current implementation (the design contract current implementation): typed predicate filters (DSL
  * shape -- parallels `where: Option[String]` raw SQL filter).
  * Per scala- §3 (binary compat): the
  * existing `filters: List[FilterSpec]` field (line 179) is the
  * legacy `Model.filters` shape (consumed by
  * `PortableQueryCompiler.applyFilters`); this NEW field is the
  * typed-DSL shape (consumed by `TypedQueryCompiler`). Both
  * default to Nil (zero behavior change for 19 callers).
  * Per karpathy-guidelines §3 (surgical): default = Nil. */
 whereFilters:  Seq[io.sm8.core.rel.TypedPredicate[Nothing]] = Nil,
 sortDirections: Seq[io.sm8.core.rel.SortDirection]    = Nil) extends Product with Serializable

object QueryRequest {

 /** Empty query \u2014 the canonical "zero filters" shape. */
 val empty: QueryRequest = QueryRequest(model = "")
}
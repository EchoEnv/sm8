package io.sm8.core.engine

import io.sm8.core.model.Model

/** MCPEngine-portable MCP engine-provider trait \u2014 Phase 2 contract.
  * Mirrors the design doc \u00a76.4 "MCPEngineProvider".
  *
  * ==Why a trait (not a concrete class)==
  *
  * Per scala-data-driven-refactor \u00a71: data in core, behavior in
  * adapters. The PROVIDER trait is the data shape (the contract
  * MCP needs); the IMPLEMENTATIONS (SparkEngineProvider,
  * TrinoEngineProvider) are the engine-specific behavior. The trait
  * lives in core; the implementations live in each engine adapter.
  *
  * ==Why `available: Boolean` (not just `identity: EngineIdentity`)==
  *
  * Per the design's `MCPEngineRegistry`: "the registry's `select`
  * filters availability". A provider can be registered but
  * unavailable (e.g. Spark Connect URL not configured; Trino cluster
  * not reachable). `available` is a runtime check, not a config
  * check.
  *
  * ==Why `query` returns `Either[EngineError, PortableQueryResult]`==
  *
  * Per the design \u00a76.4: every engine adapter's execute shape.
  * MCP consumers get a uniform `PortableQueryResult` shape (from PR
  * #400). `Either[EngineError, ...]` lets the registry surface typed
  * errors uniformly \u2014 not exceptions.
  *
  * ==Why `model: Model` (not `SemanticTable`)==
  *
  * The MCP is engine-portable. The `Model` is the engine-portable
  * shape (from core). The `SemanticTable` is the Spark-specific
  * shape (from the spark adapter). The provider receives a `Model`
  * and translates it to its engine's native shape internally.
  *
  * Extends `Serializable` so that `MCPEngineRegistry` (which stores
  * `Map[String, MCPEngineProvider]`) can be safely serialized for
  * `Restate.run` journal capture (PR-C5b-extension). Concrete
  * providers must therefore be Serializable themselves — enforced
  * at compile time by this trait. */
trait MCPEngineProvider extends Serializable {

  /** Wire-stable engine label. */
  def identity: EngineIdentity

  /** Runtime availability check. Per the design: "the registry's
    * `select` filters availability". `true` iff the provider is
    * configured AND can serve queries right now. */
  def available: Boolean

  /** Execute a query against this engine. Returns the
    * engine-portable `PortableQueryResult` (not the engine-native
    * shape).
    *
    * @param model    the portable model to query
    * @param request  the MCP query request shape (dimensions,
    *                 measures, where, having, orderBy, limit, etc.)
    * @param ctx      the engine context (timeout, cancellation,
    *                 audit policy, etc.)
    * @return          either a `PortableQueryResult` or a typed
    *                 `EngineError` (e.g. `EngineUnavailable`,
    *                 `ConnectionFailed`, `QueryTimedOut`) */
  def query(
      model:   Model,
      request: io.sm8.core.engine.MCPQueryRequest,
      ctx:     EngineContext,
  ): Either[EngineError, PortableQueryResult]

  /** Return a human-readable plan description (no execution).
    * Mirrors `MCPEngine.explain`. Used by MCP's `explain` tool. */
  def explain(
      model:   Model,
      request: io.sm8.core.engine.MCPQueryRequest,
      ctx:     EngineContext,
  ): Either[EngineError, String]
}

/** MCPEngine-portable MCP query-request shape. The shape is
  * engine-portable (no Spark, no Trino types). The provider
  * translates to its engine's native shape.
  *
  * ==Why no `where` / `having` / `orderBy` for v1==
  *
  * The `core.predicate.Predicate` and `predicate.Predicate`
  * (spark-adapter) are TWO different types per the v0.3.0
  * DE review (Predicate type duplication). The MCP server
  * currently uses the spark-adapter Predicate for filter
  * translation. For PR 5, the engine-portable `MCPQueryRequest`
  * deliberately OMITS `where` / `having` — the MCP Query
  * handler in `semanticdf-mcp` keeps its own filter logic on
  * the legacy path. A future PR aligns the predicate types
  * (per the design's "Predicate consolidation" plan in
  * design §6.2).
  *
  * ==Why v0.3.2 adds `where` (raw SQL)==
  *
  * The Platform's `QueryService` (PR #443's design doc) needs
  * to pass a raw-SQL `where` from its wire DTO through the
  * engine registry. v0.3.2 Phase 1 introduces this field as
  * an engine-specific raw filter:
  *
  *   - Spark: applies via `df.filter(where)`.
  *   - Other engines (Trino / DuckDB / PG / Hera / UC / HMS):
  *     may convert raw SQL to their native syntax, apply as-is
  *     if compatible, or return `EngineError.FeatureDeferred`
  *     if they don't support the syntax. The field is
  *     deliberately named `where` to match the platform's
  *     wire DTO; it is NOT promoted to a fully-portable
  *     typed `FilterSpec` yet (that's deferred to a follow-up
  *     PR per the design doc).
  *
  * ==Semantic caveat==
  *
  * Adding raw SQL to an otherwise-typed request shape is a
  * pragmatic compromise for Phase 1. The long-term shape
  * (typed FilterSpec) is documented as future work in
  * `docs/design/v0.3.2-platform-core-model-design.md` §6. */
final case class MCPQueryRequest(
    model:      String,
    dimensions: Seq[String] = Seq.empty,
    measures:   Seq[String] = Seq.empty,
    limit:      Option[Long] = None,
    timeGrain:  Option[String] = None,
    timeRange:  Option[(String, String)] = None,
    /** Raw SQL filter, applied after compile + before limit
      * (matches the legacy `CacheBridge.executeQuery` order).
      * `None` = no additional filter beyond the model's typed
      * `filters`. Some `Seq[String]` — applies the raw SQL
      * via `df.filter(where)` on Spark; engines decide how to
      * handle it (see trait doc above). */
    where:      Option[String] = None,
    /** MCPEngine-portable typed filters, applied AFTER the model's
      * compile-time filters and BEFORE where (the raw SQL
      * field). Each FilterSpec carries a name (for diagnostics)
      * and an Expr predicate. Per scala-data-driven-refacer:
      * Expr is pure data; the engine-specific compile is in the
      * adapter. Per scala-error-handling: at the IO boundary,
      * unsupported filter shapes surface as typed
      * EngineError.UnsupportedCapability. */
    filters:    List[io.sm8.core.model.FilterSpec] = Nil,
) extends Product with Serializable

object MCPQueryRequest {

  /** Empty query \u2014 the canonical "zero filters" shape. */
  val empty: MCPQueryRequest = MCPQueryRequest(model = "")
}
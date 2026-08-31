/*
 * SM8 DuckDB Connector — EngineProvider + ServiceLoader descriptor.
 *
 * The provider executes real SQL against an in-process DuckDB
 * database over JDBC. Layer discipline (RFC §3): DuckDB types
 * (`org.duckdb.*`, `java.sql.*`) live ONLY here — no other module
 * sees them.
 *
 * ==Resource lifecycle==
 *
 * The JDBC `Connection` is opened in `realize`/`realizeTyped` (the
 * realization boundary), stored on the provider, and closed in
 * `close()` (idempotent — the trait contract allows repeated calls).
 * Statement/ResultSet resources are scoped per-query with
 * try/finally so a failed query cannot leak them.
 *
 * ==Memory==
 *
 * The `jdbc:duckdb:` URL (the default in-memory form) loads ALL data
 * into the JVM heap. This is the right pick for tests and small
 * reference datasets; for production-scale data use a FILE-BACKED
 * URL (`jdbc:duckdb:/path/to/db.duckdb`) so DuckDB can page data
 * to disk. A future contributor reading "in-process" should not
 * assume unlimited memory.
 *
 * ==Closure safety==
 *
 * `EngineProvider extends Serializable`; the captured `Connection`
 * is `java.sql.Connection` (not serializable at the type level) —
 * so the provider marks it `@transient` and re-derives a fresh
 * connection from the stored JDBC URL on first use after
 * deserialization (`connectionOrNull`). This mirrors the
 * `InMemoryResultCache` `@transient inflight` + lazy re-init
 * pattern (drop the cached reference, rebuild on first use) used
 * by other in-memory state caches in the repo.
 *
 * ==Concurrency==
 *
 * `connectionOrNull` uses double-checked locking so two threads
 * racing on first use do not each open a fresh `jdbc:duckdb:`
 * connection (which would produce two disconnected in-memory
 * databases — the second thread's seed wouldn't be visible to the
 * first).
 *
 * ==SQL==
 *
 * The query is `SELECT * FROM <table>` where `<table>` comes from
 * the model's `SourceRef.ByName.table` (connector-controlled via
 * Model manifests, not user SQL input). The boundary is
 * internal-trust — no SQL injection surface. */
package io.sm8.connectors.duckdb

import io.sm8.core.engine.{EngineContext, EngineError, EngineIdentity, EngineProvider, EngineUrl, PortableQueryResult, QueryRequest, ResultRow, ResultSchema, ResultValue, TypedRealizationProvider}
import io.sm8.core.model.Model
import io.sm8.core.schema.{Field, SealedDataType}

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/**
 * Realized DuckDB provider: holds an open JDBC connection to the
 * realized database and executes semantic-model queries against it.
 *
 * @param jdbcUrl the realized `jdbc:duckdb:` URL
 */
final class DuckdbEngineProvider private[duckdb] (
  private val jdbcUrl: String
) extends TypedRealizationProvider {

  import DuckdbEngineConstants._

  @transient private var conn: Connection = null

  override val identity: EngineIdentity =
    EngineIdentity(
      name                 = WireName,
      nativeVersion        = s"duckdb-jdbc-1.5.5.1",
      engineAdapterVersion = AdapterVersion)

  /** A live DuckDB connection is always usable — the driver is
    * embedded, there is no remote to reach. */
  override val available: Boolean = true

  /** The connection, (re)established lazily. `@transient conn` is
    * null after Java-serialization round-trip; the URL survives
    * (it is a plain String field) so the connection re-derives
    * deterministically.
    *
    * Double-checked locking protects against two threads racing
    * on first use — without it, both threads could open a fresh
    * `jdbc:duckdb:` connection (each is a private in-memory DB,
    * so any state one thread seeds is invisible to the other).
    * For file-backed URLs the race is wasteful but safe; for
    * in-memory the race would produce two disconnected databases. */
  private def connectionOrNull: Connection = {
    if (conn == null) {
      synchronized {
        if (conn == null) {
          Class.forName("org.duckdb.DuckDBDriver")
          conn = DriverManager.getConnection(jdbcUrl)
        }
      }
    }
    conn
  }

  /** Test seam: exposes the live connection so the conformance
    * fixture can create tables before the query contract runs.
    * Not part of the `EngineProvider` contract — test-scope use
    * only. */
  private[duckdb] def testConnection: Connection = connectionOrNull

  /** Close the JDBC connection (idempotent). */
  override def close(): Unit = {
    if (conn != null) {
      try conn.close()
      finally conn = null
    }
  }

  /**
   * Realize a provider from a raw `jdbc:duckdb:` URL string.
   *
   * @param url a non-blank `jdbc:duckdb:` URL
   * @return a new provider on grammar match; `None` otherwise
   */
  override def realize(url: String): Option[EngineProvider] =
    if (url == null || url.trim.isEmpty) None
    else if (url.trim.startsWith(UrlPrefix)) Some(new DuckdbEngineProvider(url.trim))
    else None

  /**
   * Typed realization: accept only the `EngineUrl.DuckDb` case.
   *
   * @param parsedUrl the typed URL
   * @return `Right(provider)` on success; `Left(EngineError.ConnectionFailed)` on
   *   foreign case or realization failure
   */
  override def realizeTyped(parsedUrl: EngineUrl): Either[EngineError, EngineProvider] =
    parsedUrl match {
      case duck: EngineUrl.DuckDb =>
        realize(duck.jdbcUrl) match {
          case Some(p) => Right(p)
          case None    =>
            Left(EngineError.ConnectionFailed(
              engine  = WireName,
              reason  = "realize(url) returned None for parsed url",
              message = s"sm8: duckdb engine: unexpected URL for parsed url: '${duck.jdbcUrl}'"
            ))
        }
      case other =>
        Left(EngineError.ConnectionFailed(
          engine  = WireName,
          reason  = "unexpected EngineUrl case for duckdb descriptor",
          message = s"sm8: duckdb descriptor received non-DuckDB EngineUrl: ${other.getClass.getSimpleName}"
        ))
    }

  /**
   * Execute the semantic query as SQL against DuckDB and port the
   * ResultSet into a `PortableQueryResult`.
   *
   * Statement/ResultSet are scoped try/finally per-query (resource
   * safety on every exit path). Any NonFatal JDBC failure surfaces
   * as a typed `EngineError.ConnectionFailed` — never a silent
   * partial result (typed-error: the caller must pattern-match the
   * typed `EngineError` variant returned, NOT a generic throwable).
   *
   * @param model   the portable model (named the queried table)
   * @param request the query shape (dimensions/measures/limit)
   * @param ctx     the engine context (unused for an embedded engine)
   * @return `Right(PortableQueryResult)` with schema + rows, or a
   *   typed `EngineError`
   */
  override def query(
      model: Model,
      request: QueryRequest,
      ctx: EngineContext
  ): Either[EngineError, PortableQueryResult] = {
    // The conformance + smoke tests pass a model with no dimensions
    // or measures; a SELECT * over the model's table is the smallest
    // honest query shape. Semantic-layer column projection lands
    // with the DuckDB semantic-bridge follow-up.
    val table = model.source match {
      case byName: io.sm8.core.model.SourceRef.ByName => byName.table
      case _ =>
        return Left(EngineError.UnsupportedCapability(
          engine     = WireName,
          capability = "DuckdbEngineProvider.query:non-ByName-source",
          message    = s"sm8: duckdb engine: model '${model.name}' must reference a ByName table source"
        ))
    }

    // NPE guard: request.model may disagree with model.name on a
    // malformed call — the queried table is model.source's table,
    // not request.model's string; both stay as-is and no
    // cross-check is enforced here (mirror of the
    // other reference engines).
    try {
      val c = connectionOrNull
      var stmt: Statement = null
      var rs: ResultSet = null
      try {
        stmt = c.createStatement()
        rs = stmt.executeQuery(s"SELECT * FROM $table")

        val meta = rs.getMetaData
        val colCount = meta.getColumnCount
        val fields = (1 to colCount).map { i =>
          Field(
            name     = meta.getColumnLabel(i),
            dataType = sqlTypeToSealed(meta.getColumnType(i)),
            nullable = meta.isNullable(i) != java.sql.ResultSetMetaData.columnNoNulls
          )
        }.toList

        val schema = ResultSchema(fields)
        val rowsBuilder = Vector.newBuilder[ResultRow]
        while (rs.next()) {
          val values = (1 to colCount).map { i =>
            val raw = rs.getObject(i)
            if (raw == null) ResultValue.NullV
            else ResultValue.StringV(String.valueOf(raw)) // portable decode: everything as string for v1
          }.toList
          rowsBuilder += ResultRow(values = values, schema = schema)
        }

        Right(PortableQueryResult(
          schema   = schema,
          rows     = rowsBuilder.result(),
          metadata = Map("engine" -> WireName)
        ))
      } finally {
        if (rs != null) rs.close()
        if (stmt != null) stmt.close()
      }
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        Left(EngineError.ConnectionFailed(
          engine  = WireName,
          reason  = "interrupted during query execution",
          message = s"sm8: duckdb query interrupted: ${e.getMessage}"
        ))
      case NonFatal(e) =>
        Left(EngineError.ConnectionFailed(
          engine  = WireName,
          reason  = "DuckDB query failed",
          message = s"sm8: duckdb query failed: ${e.getClass.getSimpleName}: ${e.getMessage}"
        ))
    }
  }

  /**
   * Human-readable plan (no execution).
   *
   * @param model   the portable model
   * @param request the query request
   * @param ctx     the engine context
   * @return `Right(plan description)` always (this stub does not fail)
   */
  override def explain(
      model: Model,
      request: QueryRequest,
      ctx: EngineContext
  ): Either[EngineError, String] =
    Right(s"duckdb plan for ${model.name} (SELECT * FROM ${tableNameOf(model)})")

  private def tableNameOf(model: Model): String = model.source match {
    case byName: io.sm8.core.model.SourceRef.ByName => byName.table
    case _                                          => "<non-ByName source>"
  }

  /** Map a `java.sql.Types` int to the portable `SealedDataType`.
    * Coarse for v1: numeric → Int, temporal → Timestamp/Date,
    * boolean → Boolean, everything else → Varchar. */
  private def sqlTypeToSealed(sqlType: Int): SealedDataType = {
    import java.sql.Types._
    sqlType match {
      case BOOLEAN                            => SealedDataType.Boolean
      case DATE                              => SealedDataType.Date
      case TIME | TIMESTAMP | TIMESTAMP_WITH_TIMEZONE => SealedDataType.Timestamp
      case TINYINT | SMALLINT | INTEGER       => SealedDataType.Int
      case BIGINT                            => SealedDataType.BigInt
      case FLOAT | REAL | DOUBLE              => SealedDataType.Double
      case NUMERIC | DECIMAL                  => SealedDataType.Decimal(precision = 38, scale = 0)
      case _                                  => SealedDataType.Varchar
    }
  }
}

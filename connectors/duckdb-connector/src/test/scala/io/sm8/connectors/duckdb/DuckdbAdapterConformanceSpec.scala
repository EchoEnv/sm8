/*
 * SM8 DuckDB connector — shared adapter conformance suite.
 *
 * Extends the unified `AdapterConformanceSpec` from sm8-core's
 * test-jar. DuckDB has a real URL grammar (`jdbc:duckdb:` prefix);
 * the spec exercises blank/null rejection, grammar-invalid
 * rejection (non-`jdbc:duckdb:` URLs), foreign-EngineUrl typed
 * rejection (`EngineUrl.Spark` → typed `EngineError.ConnectionFailed`),
 * and the query contract on a LIVE DuckDB database.
 *
 * The conformance base calls `descriptor.realize(validUrl).get` in
 * its determinism test, which creates a NEW in-memory DuckDB per
 * realization. To make `wellFormedQuery`'s table visible to the
 * base's separately-realized provider, both must share the same DB.
 * DuckDB's `jdbc:duckdb:` opens a process-private in-memory DB per
 * `DriverManager.getConnection` call (cheap, ephemeral), so this
 * spec seeds the conformance table via a SHARED file-backed DB
 * `jdbc:duckdb:<tmpfile>` and the base's separate realization
 * re-opens the same file. After the spec suite, every realized
 * provider is closed (resource lifecycle, scala-jvm-safety §2) and
 * the temp file is deleted.
 *
 * `querySucceeds = true` for DuckDB — the provider executes real
 * SQL and the well-formed query reads a table created via
 * `CREATE OR REPLACE TABLE` (idempotent on every realization).
 */
package io.sm8.connectors.duckdb

import io.sm8.core.engine.{EngineContext, EngineProvider, EngineUrl, QueryRequest, TypedRealizationProvider}
import io.sm8.core.model.{Model, ModelStatus, SourceRef}
import io.sm8.sdk.contract.AdapterConformanceSpec

import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.nio.file.{Files, Path}

class DuckdbAdapterConformanceSpec extends AdapterConformanceSpec with BeforeAndAfterAll {

  /** Shared file-backed DB so every realized provider sees the same
    * `conformance_people` table. `jdbc:duckdb:<file>` opens a private
    * persistent DB at that path; the test creates the file (per
    * spec-run) and the JVM cleans it up on `afterAll`. */
  private val sharedFile: Path = {
    val f = Files.createTempFile("duckdb-conformance-", ".duckdb").toAbsolutePath
    // DuckDB refuses to open a non-empty file as a DB; create-empty
    // first, then delete the empty file so the JDBC URL creates it
    // fresh on first connection (this matches the existing
    // `createDataFrame` pattern in SparkConnectorBigDataScaleSpec).
    Files.deleteIfExists(f)
    f
  }
  private val sharedUrl: String = "jdbc:duckdb:" + sharedFile.toString

  /** Providers realized during the suite (the conformance base calls
    * `descriptor.realize(validUrl)` in its routing-invariant +
    * determinism tests, and our `wellFormedQuery` calls it again
    * — all three yield fresh DuckDB connections). The
    * `wellFormedQuery`-triggered realization is the one we track
    * in the `realizedProviders` buffer and close in `afterAll`; the
    * base's two realizations remain open until JVM exit (this is
    * a known small leak under the shared-file design — see
    * cross-engine-conformance-matrix.md Note 1 for the full
    * rationale). Tracking the `wellFormedQuery` realization here
    * ensures the conformance-table resource is released. */
  private val realizedProviders = scala.collection.mutable.ListBuffer.empty[DuckdbEngineProvider]

  /** The descriptor under test (URL-grammar validating; narrow
    * return type drops the test-side `asInstanceOf`). */
  override def descriptor: TypedRealizationProvider = new DuckdbEngineProviderDescriptor()

  /** Wire-stable name matching [[DuckdbEngineConstants]]. */
  override def wireName: String = "duckdb"

  /** The shared file-backed DuckDB URL. */
  override def validUrl: String = sharedUrl

  /** Non-`jdbc:duckdb:` URLs the grammar rejects. */
  override def invalidUrls: Seq[String] = Seq(
    "http://not-a-jdbc-url",
    "jdbc:mysql://wrong-engine",
    "jdbc:trino://localhost:8080",
    "jdbc:duckdb"  // scheme prefix without the colon
  )

  /** Spark URL is foreign to this engine. */
  override def foreignEngineUrl: EngineUrl = EngineUrl.Spark("local[*]")

  /** A (model, request) pair whose source table is created in the
    * shared file-backed database. Each call realizes a fresh
    * connection (tracked for afterAll cleanup) and seeds the
    * `conformance_people` table idempotently. */
  override def wellFormedQuery: (Model, QueryRequest) = {
    val provider = realizedProviders.synchronized {
      val p = descriptor.realize(sharedUrl).get.asInstanceOf[DuckdbEngineProvider]
      realizedProviders += p
      p
    }
    val conn = provider.testConnection
    val stmt = conn.createStatement()
    try {
      stmt.execute("CREATE OR REPLACE TABLE conformance_people (id INTEGER, name VARCHAR)")
      stmt.execute("INSERT INTO conformance_people VALUES (1, 'alice'), (2, 'bob')")
    } finally stmt.close()

    val model = Model.of(
      name    = "conformance-model",
      version = 1,
      source  = SourceRef.ByName(table = "conformance_people"),
      status  = ModelStatus.Draft,
      dimensions = Nil,
      measures   = Nil
    ).toOption.get

    (model, QueryRequest(model = model.name))
  }

  /** Default engine context. */
  override def queryContext: EngineContext = EngineContext.defaultContext

  override def afterAll(): Unit = {
    // Close every realized provider (resource lifecycle) then
    // delete the temp file.
    realizedProviders.synchronized {
      realizedProviders.foreach(_.close())
      realizedProviders.clear()
    }
    try Files.deleteIfExists(sharedFile)
    catch { case _: java.io.IOException => /* best effort */ }
    super.afterAll()
  }
}

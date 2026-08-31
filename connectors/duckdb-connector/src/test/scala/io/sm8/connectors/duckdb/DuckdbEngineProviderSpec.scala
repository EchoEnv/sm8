/*
 * SM8 DuckDB connector — engine-specific deep spec.
 *
 * Goes beyond the shared conformance suite with DuckDB-specific
 * behaviors: real SQL round-trip (CREATE/INSERT/SELECT with typed
 * columns), typed-row decoding, determinism on real data, URL
 * parser grammar, and the close() lifecycle idempotence contract.
 *
 * The engine is fully in-process — `jdbc:duckdb:` opens an
 * ephemeral in-memory database that vanishes when the connection
 * closes. No disk, no network, no flake surface.
 */
package io.sm8.connectors.duckdb

import io.sm8.core.engine.{EngineContext, EngineError, EngineUrl, QueryRequest}
import io.sm8.core.model.{Model, ModelStatus, SourceRef}
import io.sm8.core.schema.SealedDataType

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DuckdbEngineProviderSpec extends AnyFunSuite with Matchers {

  private def provider(): DuckdbEngineProvider =
    new DuckdbEngineProviderDescriptor().realize("jdbc:duckdb:").get.asInstanceOf[DuckdbEngineProvider]

  private def model(tableName: String): Model =
    Model.of(
      name    = "spec-model",
      version = 1,
      source  = SourceRef.ByName(table = tableName),
      status  = ModelStatus.Draft
    ).toOption.get

  private def request(modelName: String): QueryRequest = QueryRequest(model = modelName)

  test("realize: blank/null URLs → None (grammar: non-blank required)") {
    val d = new DuckdbEngineProviderDescriptor()
    d.realize("") shouldBe None
    d.realize("   ") shouldBe None
    d.realize(null) shouldBe None
  }

  test("realize: non-duckdb URLs → None (grammar mismatch)") {
    val d = new DuckdbEngineProviderDescriptor()
    d.realize("http://not-a-jdbc-url") shouldBe None
    d.realize("jdbc:mysql://wrong") shouldBe None
  }

  test("parser: validated URL carried as EngineUrl.DuckDb + wire name") {
    val parser = new DuckdbEngineUrlParser()
    val parsed = parser.parse("jdbc:duckdb:/tmp/x.duckdb")
    parsed.isRight shouldBe true
    val url = parsed.toOption.get
    url.engineName shouldBe "duckdb"
    url.raw shouldBe "jdbc:duckdb:/tmp/x.duckdb"
    url shouldBe an[EngineUrl.DuckDb]
    url.asInstanceOf[EngineUrl.DuckDb].jdbcUrl shouldBe "jdbc:duckdb:/tmp/x.duckdb"
  }

  test("parser: blank + grammar-invalid URLs → typed ConnectionFailed") {
    val parser = new DuckdbEngineUrlParser()
    parser.parse("").isLeft shouldBe true
    parser.parse(null).isLeft shouldBe true
    val bad = parser.parse("http://wrong")
    bad.isLeft shouldBe true
    bad.swap.toOption.get.engine shouldBe "duckdb"
  }

  test("query: real SQL round-trip — CREATE + INSERT + SELECT with typed columns") {
    val p = provider()
    val stmt = p.testConnection.createStatement()
    try {
      stmt.execute("CREATE OR REPLACE TABLE spec_people (id INTEGER, name VARCHAR)")
      stmt.execute("INSERT INTO spec_people VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
    } finally stmt.close()

    val out = p.query(model("spec_people"), request("spec-model"), EngineContext.defaultContext)
    out.isRight shouldBe true
    val res = out.toOption.get

    res.schema.fields.map(_.name) shouldBe Seq("id", "name")
    res.schema.fields.map(_.dataType) shouldBe Seq(SealedDataType.Int, SealedDataType.Varchar)
    res.rowCount shouldBe 3
    res.isWellFormed shouldBe true
    res.metadata("engine") shouldBe "duckdb"
  }

  test("query: same input twice is deterministic (schema + rows + metadata)") {
    val p = provider()
    val stmt = p.testConnection.createStatement()
    try {
      stmt.execute("CREATE OR REPLACE TABLE det_people (id INTEGER)")
      stmt.execute("INSERT INTO det_people VALUES (10), (20)")
    } finally stmt.close()

    val m = model("det_people")
    val r1 = p.query(m, request("spec-model"), EngineContext.defaultContext)
    val r2 = p.query(m, request("spec-model"), EngineContext.defaultContext)
    r1 shouldBe r2
  }

  test("query: a missing table surfaces a typed ConnectionFailed (no silent partial data)") {
    val p = provider()
    val out = p.query(model("table_that_does_not_exist"), request("spec-model"), EngineContext.defaultContext)
    out.isLeft shouldBe true
    out.swap.toOption.get shouldBe an[EngineError.ConnectionFailed]
  }

  test("query: non-ByName source surfaces UnsupportedCapability (documented v1 limit)") {
    val p = provider()
    val m = Model.of(
      name    = "spec-model",
      version = 1,
      source  = SourceRef.ByProvider("some-provider"),
      status  = ModelStatus.Draft
    ).toOption.get
    val out = p.query(m, request("spec-model"), EngineContext.defaultContext)
    out.isLeft shouldBe true
    out.swap.toOption.get shouldBe an[EngineError.UnsupportedCapability]
  }

  test("close: idempotent — double close does not throw; provider is reusable after close via lazy re-derive") {
    // File-backed DB so re-derive opens the SAME file (not a fresh
    // in-memory DB where the table wouldn't be visible).
    val tmp = java.nio.file.Files.createTempFile("duckdb-reuse-", ".duckdb").toAbsolutePath
    java.nio.file.Files.deleteIfExists(tmp)
    val url = "jdbc:duckdb:" + tmp.toString
    val p = new DuckdbEngineProviderDescriptor().realize(url).get

    val seedStmt = p.testConnection.createStatement()
    try {
      seedStmt.execute("CREATE TABLE reuse_people (id INTEGER)")
      seedStmt.execute("INSERT INTO reuse_people VALUES (42)")
    } finally seedStmt.close()

    // Close the connection (test the idempotent lifecycle).
    p.close()
    p.close() // second close is a no-op (trait contract)

    // After close, the next query must re-derive a fresh connection
    // from the stored URL — and the same table + row must still be
    // visible (proving the provider is genuinely reusable, not just
    // "lazy re-derive doesn't throw"). Values are StringV under the
    // v1 decode (see DuckdbEngineProvider.query Scaladoc).
    val m = Model.of(
      name    = "spec-model",
      version = 1,
      source  = SourceRef.ByName(table = "reuse_people"),
      status  = ModelStatus.Draft
    ).toOption.get
    val out = p.query(m, request("spec-model"), EngineContext.defaultContext)
    out.isRight shouldBe true
    val rows = out.toOption.get.rows
    rows.size shouldBe 1
    rows.head.values shouldBe Seq(io.sm8.core.engine.ResultValue.StringV("42"))

    try java.nio.file.Files.deleteIfExists(tmp)
    catch { case _: java.io.IOException => /* best effort */ }
  }

  test("realized provider survives Java-serialization round-trip (closure-safety)") {
    import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
    val p = provider()
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(p); oos.close(); bos.toByteArray
    val ois = new ObjectInputStream(new java.io.ByteArrayInputStream(bos.toByteArray))
    val back = ois.readObject().asInstanceOf[DuckdbEngineProvider]
    // The transient connection is gone; the identity survives.
    back.identity.name shouldBe "duckdb"
    back.available shouldBe true
  }
}

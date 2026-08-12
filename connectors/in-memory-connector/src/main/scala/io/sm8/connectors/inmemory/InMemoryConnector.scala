/*
 * SM8 in-memory Connector — the built-in reference.
 *
 * Per [[scala-data-driven-refactor-mindset]]:
 *   - Data and behavior are separated: data lives in `InMemoryConfig`
 *     and `Table` case classes; the Connector's `query`/`schema`
 *     methods are pure functions over that data.
 *   - Tables are a sealed trait (`Table`) with typed case classes,
 *     not a generic `Map[String, Any]`. `query` dispatches via
 *     pattern-match on `Table`.
 *   - The Connector is immutable: seed is supplied at construction
 *     (case class field). `connect(config)` validates the config
 *     type — it does NOT mutate state.
 *
 * Per [[scala-error-handling-mindset]]:
 *   - `query` on an unknown table raises (RFC adapters.md Rule 1:
 *     errors propagate, never swallowed). Step 3 throws because
 *     the typed `Either[EngineError, ResultRows]` shape lands in
 *     Step 0 when EngineError moves from semanticdf-core.
 *
 * Per [[scala-jvm-safety-mindset]]:
 *   - No resource lifecycle. InMemoryConnector has no JDBC
 *     connection, file handle, or network socket — just a
 *     `Map[String, Table]` held in memory.
 */
package io.sm8.connectors.inmemory

import io.sm8.sdk._

// ============================================================================
// Data types (case classes, no behavior)
// ============================================================================

/**
 * Sealed trait for table data held by the in-memory Connector. Each
 * table is a typed value with a known column list and a known row
 * shape. Dispatch via `match` in `InMemoryConnector.query`.
 *
 * Adding a new table type is a case-class addition here + a new
 * case in `query`'s match. The compiler enforces exhaustiveness
 * (per [[scala-data-driven-refactor-mindset]] step 3).
 */
sealed trait Table {
  /** The columns of this table, in declaration order. */
  def columns: List[String]
  /** Convert this table to the portable ResultRows shape. */
  def toResultRows: ResultRows
}

/**
 * Generic table: column list + rows of `Map<String, Any>`. For
 * Step 3 this is the only Table case; typed tables (e.g.
 * `FlightsTable`) land in later steps as concrete plugins need them.
 */
final case class GenericTable(columns: List[String], rows: Vector[Map[String, Any]])
    extends Table {
  override def toResultRows: ResultRows = ResultRows(rows)
}

/**
 * Config: the seed tables for this Connector. The Connector is
 * constructed with this seed; `connect(config)` validates the
 * config type (and may verify the seed matches expectations).
 *
 * Not a `case class` with required seed — defaults to empty so the
 * no-arg constructor `InMemoryConnector()` is valid for tests
 * that don't need data.
 */
final case class InMemoryConfig(seed: Map[String, Table] = Map.empty)
    extends ConnectorConfig

// ============================================================================
// Query types (sealed trait, pattern-matched in query)
// ============================================================================

/** Query: list all tables, or select one table by name. */
sealed trait InMemoryQuery extends SemanticQuery
case object ListTables                              extends InMemoryQuery
final case class SelectTable(name: String)          extends InMemoryQuery
/** Sentinels for the conformance contract's "malformed" path. */
case object MalformedQuery                          extends InMemoryQuery

// ============================================================================
// The Connector itself — immutable, case class, data-driven
// ============================================================================

/**
 * The in-memory Connector. Holds `tables: Map[String, Table]`
 * (immutable, set at construction). All read methods (`query`,
 * `schema`) are pure functions over `tables`.
 *
 * `connect(config)` validates the config type. It does not mutate
 * the Connector — the seed is fixed at construction. This is a
 * deliberate simplification for a test/reference Connector;
 * real Connectors (Step 8) will use `connect()` to actually
 * establish connections (JDBC, Thrift, REST, etc.).
 *
 * Construction:
 * {{{
 *   val c = InMemoryConnector(Map("flights" -> GenericTable(
 *     List("carrier", "flight_count"),
 *     Vector(Map("carrier" -> "AA", "flight_count" -> 5)))))
 *   engine.connectors.register(c)
 *   engine.run(ConnectorRequest("in-memory", SelectTable("flights")))
 * }}}
 */
final case class InMemoryConnector(tables: Map[String, Table] = Map.empty)
    extends Connector {

  override def name: String = "in-memory"

  override def connect(config: ConnectorConfig): Unit = config match {
    case _: InMemoryConfig => ()  // accept; seed is fixed at construction
    case other             =>
      throw new IllegalArgumentException(
        s"in-memory: expected InMemoryConfig, got ${other.getClass.getSimpleName}")
  }

  /**
   * Data-driven dispatch via pattern-match on Table cases (per
   * [[scala-data-driven-refactor-mindset]] step 3). Adding a new
   * Table case forces the compiler to flag this match as
   * non-exhaustive — a useful "did you update the Connector?"
   * reminder.
   */
  override def query(request: SemanticQuery): ResultRows = request match {
    case ListTables =>
      // One row per table — minimal "what tables exist?" answer.
      ResultRows(tables.keys.toVector.sorted.map(t => Map("table_name" -> t)))

    case SelectTable(name) =>
      tables.get(name) match {
        case Some(t) => t.toResultRows
        case None    =>
          throw new IllegalArgumentException(s"in-memory: unknown table '$name'")
      }

    case MalformedQuery =>
      throw new IllegalArgumentException("in-memory: malformed query")

    case other =>
      throw new IllegalArgumentException(
        s"in-memory: expected InMemoryQuery, got ${other.getClass.getSimpleName}")
  }

  override def schema(): ConnectorSchema = {
    // Union of all column names across all tables. Per-table
    // schema lands in Step 0 with the IR.
    val cols = tables.values.toList.flatMap(_.columns).distinct.sorted
    ConnectorSchema(cols)
  }
}
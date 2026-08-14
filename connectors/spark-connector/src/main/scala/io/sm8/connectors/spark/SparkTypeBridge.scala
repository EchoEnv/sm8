/*
 * SM8 Spark Connector — Spark `DataType` → portable `SealedDataType`
 * bridge.
 *
 * Per [[scala-data-driven-refactor-mindset]] (sealed-trait dispatch):
 * pure function — no state, no side effects. The match is
 * exhaustive over Spark's `DataType` family at the case-class
 * granularity that we care about (the portable shape). Every
 * Spark `DataType` that maps to one of our 13 `SealedDataType`
 * cases is enumerated; anything else falls back to `Json`.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #3 (schema drift):
 * "verify, don't assume". The Spark runtime's `df.schema` is the
 * source of truth — we do NOT trust the caller-supplied model
 * dimensions/measures for the output column types; we read them
 * from the actual compiled plan via `df.schema`. This bridge is
 * the BOUNDARY where that verification happens. Unsupported Spark
 * types fall back to `Json` so the row data still serializes
 * (the actual value is a JSON string). The fallback is logged
 * via the PluginSerializationSpec conformance test that follows.
 *
 * Per [[karpathy-guidelines-mindset]] 'smallest correct core':
 * no state captured — pure function. The return type
 * `io.sm8.core.schema.SealedDataType` extends `Product with
 * Serializable` (PR-C0). The function itself is `extends
 * java.io.Serializable` so the surrounding `MCPEngineProvider`
 * trait (also Serializable per the engine-portable contract) round-
 * trips through `ObjectOutputStream`.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1 (closure-safety):
 * `extends java.io.Serializable` is declared on the companion +
 * the function's return type is a sealed ADT. No `SparkSession`,
 * no `DataFrame`, no `Iterator`, no `Connection` is closed over.
 * The PluginSerializationSpec + PluginClosureSafetyConformanceSpec
 * (PR #36) round-trip this object through `ObjectOutputStream`
 * and catch any future regression.
 *
 * The legacy `/tmp/semanticdf/adapters/semanticdf-spark` has this
 * same logic as a private method on `SparkEngineProvider`. Per
 * [[scala-data-driven-refactor-mindset]] "behavior in adapters,
 * data in core" — we extract the bridge to its own type so the
 * dispatch is testable in isolation AND can be shared across
 * future Spark-shaped Connectors (per agile-kindling-beacon plan
 * line 287: duckdb, unity-catalog, postgresql all need similar
 * type bridges when they ship).
 */
package io.sm8.connectors.spark

import io.sm8.core.schema.SealedDataType

import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  BooleanType,
  DataType,
  DateType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  MapType,
  StringType,
  StructType,
  TimestampType
}

/**
 * Spark `DataType` → portable `SealedDataType` bridge.
 *
 * Companion object + pure function. No instance state. The function
 * is the canonical entry point; the companion object exists so the
 * function is reachable without instantiation (preserves the
 * "data in core, behavior in adapters" separation — the bridge is
 * pure dispatch, not behavior).
 *
 * Per [[scala-impact-analysis-mindset]] mantra 4 (name every caller):
 * today the only caller is `SparkConnector.query()` body (lands in
 * Layer C of the Step 8 follow-up). The `SparkTypeBridgeSpec` tests
 * the dispatch in isolation NOW so that caller can use it without
 * re-asserting the mapping.
 */
object SparkTypeBridge extends java.io.Serializable {

  /**
   * Map a Spark `DataType` to the portable `SealedDataType`.
   *
   * Return shape mirrors the legacy's mapping
   * (`/tmp/semanticdf/adapters/semanticdf-spark/SparkEngineProvider.scala:255`)
   * adapted to our reactor's 13-case `SealedDataType` ADT.
   *
   * Unsupported Spark types fall back to `Json` so the row data
   * still serializes. Per [[scala-spark-batch-bugs-mindset]] mantra
   * #3: never assume; verify at the boundary. The fallback is the
   * explicit "I don't know what this is" signal.
   *
   * @param dt  a Spark `DataType` (typically `df.schema.fields(i).dataType`)
   * @return    the engine-portable `SealedDataType` for the same column
   */
  def sparkTypeToSealedDataType(dt: DataType): SealedDataType = dt match {
    // Primitives — direct one-to-one mapping.
    case _: StringType     => SealedDataType.Varchar
    case _: IntegerType    => SealedDataType.Int
    case _: LongType       => SealedDataType.Int   // portable has no Long; widen semantics
    case _: FloatType      => SealedDataType.Double
    case _: DoubleType     => SealedDataType.Double
    case _: BooleanType    => SealedDataType.Boolean
    case _: TimestampType  => SealedDataType.Timestamp
    case _: DateType       => SealedDataType.Date
    // Decimal — Spark's DecimalType has precision + scale. We use
    // the same default (38, 18) as the legacy; a future PR can wire
    // through the actual values when the connector accepts config
    // shape overrides.
    case _: DecimalType    => SealedDataType.Decimal(38, 18)

    // Nested — fall back to Json. The portable `SealedDataType`
    // supports `Array` / `Map` / `Row` (with inner types) — but for
    // a faithful round-trip with Spark we'd need to decode the inner
    // element types recursively. That recursive descent belongs to
    // Layer C (the real runtime). Layer A (this bridge) conservatively
    // emits `Json` so the schema field shape is well-typed but the
    // value-side handling is left as future work.
    case _: ArrayType  => SealedDataType.Json
    case _: MapType    => SealedDataType.Json
    case _: StructType => SealedDataType.Json
    case _: BinaryType => SealedDataType.Json

    // Unknown — Json fallback (matches legacy's last `case _` arm).
    case _             => SealedDataType.Json
  }
}

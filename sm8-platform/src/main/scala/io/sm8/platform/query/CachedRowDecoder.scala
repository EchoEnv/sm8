/*
 * SM8 Platform — Row-level decoder for the engine-portable
 * cached-row wire format.
 *
 * Per [[scala-data-driven-refactor-mindset]] (sealed-trait dispatch
 * over `switch (simpleName)`): replaces the Java
 * `QueryService.fromRestateCachedRow` (semanticdf-platform lines
 * 940-973) with a Scala 2.13 row-level decoder. The decoder walks
 * the `RestateCachedRow.rows` List, applying `PortableCellCodec.decodeCell`
 * (from PR-C4a) to each cell using the corresponding tag in
 * `RestateCachedRow.fieldTypes`.
 *
 * Per [[karpathy-guidelines-mindset]] (smallest correct core + match
 * existing style + Scala 2.13 idiom): pure function. Returns the
 * decoded rows as `List[List[Object]]` — the legacy Java code
 * returned a `CachedResult` (which carries a Spark `Array[Row]` +
 * `StructType`). The Scala version drops the Spark-specific
 * `StructType` (documented landmine in the legacy code — the
 * rebuilt schema is all-`StringType`, losing the original column
 * types). The wire contract: `RestateCachedRow.fieldNames` carries
 * the column names; the typed cells carry the column values.
 *
 * Per [[scala-jvm-safety-mindset]]: null row entries (per the
 * `RestateCachedRow` wire contract) are converted to empty
 * `List[Object]` — matching the legacy Java behavior
 * (`RowFactory.create(new Object[0])` produces an empty Row).
 * Null at this boundary is a documented wire contract, not a
 * JVM-safety violation.
 *
 * Per [[scala-impact-analysis-mindset]]: 0 callers in our reactor
 * (the Java `QueryService.fromRestateCachedRow` stays in
 * `/tmp/semanticdf` for later migration PRs). Pure function,
 * dormant until PR-C4c (`toQueryResultFromJournaled`) + PR-C5+
 * (`runQueryViaEngineRegistry`) consume it.
 *
 * ==Behavior change vs Java (intentional)==
 *
 * The Java code returned a `CachedResult` with a `StructField[]
 * schema` (all-`StringType`, suppressing the original column types).
 * The Scala version returns `List[List[Object]]` — no fake schema.
 * Consumers read the field names from `RestateCachedRow.fieldNames`
 * (the same source-of-truth the legacy code used) and the typed
 * cells from the returned list. This is engine-portable: no Spark
 * `DataType` in the wire contract.
 */
package io.sm8.platform.query

/**
 * Row-level decoder for the engine-portable cached-row wire format.
 *
 * Inverse of `toRestateCachedRowFromPortable` (which lives in
 * `semanticdf-platform/.../QueryService.java` lines 711-740 and is
 * the encode-from-`PortableQueryResult` counterpart).
 */
object CachedRowDecoder {

  /**
   * Decode an entire `RestateCachedRow` to its typed rows.
   *
   * Per the [[io.sm8.platform.query.RestateCachedRow]] wire contract:
   *   - `fieldNames.size` must equal `fieldTypes.size` (enforced by
   *     `RestateCachedRow`'s smart constructor)
   *   - Each row's cells must have exactly `fieldNames.size` cells,
   *     or be null (enforced by `RestateCachedRow`'s smart constructor)
   *
   * Per-row behavior:
   *   - Non-null row → `List[Object]` of decoded cells (one per column)
   *   - Null row → `List.empty[Object]` (matches the legacy Java
   *     `RowFactory.create(new Object[0])` behavior)
   *
   * The field names are NOT in the returned list — they live in
   * `row.fieldNames` (the same source-of-truth the legacy Java code
   * used). Consumers compose the two as needed.
   *
   * @param row the cached row to decode
   * @return    the decoded rows (one `List[Object]` per input row)
   */
  def fromRestateCachedRow(row: RestateCachedRow): List[List[Object]] = {
    val fieldTypes = row.fieldTypes
    row.rows.map { cells =>
      if (cells == null) List.empty[Object]
      else cells.toList.zip(fieldTypes).map { case (encoded, tag) =>
        PortableCellCodec.decodeCell(tag, encoded)
      }
    }
  }
}
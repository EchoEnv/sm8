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

import io.sm8.core.engine.{PortableQueryResult, ResultRow, ResultSchema, ResultValue}

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
    row.rows.toList.map { cells =>
      if (cells == null) List.empty[Object]
      else cells.toList.zip(fieldTypes).map { case (encoded, tag) =>
        if (encoded == null) null
        else PortableCellCodec.decodeCell(tag, encoded)
      }
    }
  }

  /**
   * Convert a cached `RestateCachedRow` to a `PortableQueryResult`
   * (the engine-portable result shape). Used by the cache-hit
   * branch of `EngineService.runQuery` so both the HIT and MISS
   * paths can yield a `PortableQueryResult` for uniform
   * for-comprehension dispatch.
   *
   * Builds the schema from the cached row's `fieldNames` +
   * `fieldTypes`. The field's `dataType` is set to a
   * `SealedDataType.Varchar` (a safe default — the cache journal
   * doesn't preserve the full type info; `toQueryResultFromPortable`
   * uses the same approximation when adapting for `QueryResult`).
   *
   * @param row the cached row to convert
   * @return    the equivalent `PortableQueryResult`
   */
  /**
   * Inverse of `encodeCell`: wrap the JVM-erased decoded value
   * (a `java.lang.Object`) into the corresponding `ResultValue`
   * case. Sealed-trait dispatch on the tag determines the case
   * (per [[scala-data-driven-refactor-mindset]] "sealed-trait
   * dispatch over Map" — no Map-based rule tables).
   *
   * Inverse of `encodeCell` in [[PortableCellCodec]]. The wire
   * format is JVM-typed (the cache journal preserves the JVM
   * form for `Restate.run` compatibility); this helper rebuilds
   * the typed `ResultValue` on the read side.
   */
  private def toResultValue(tag: String, v: Object): ResultValue = tag match {
    case RestateCachedRow.T_STRING    => Option(v) match {
      case Some(s: String) => ResultValue.StringV(s)
      case _               => ResultValue.NullV
    }
    case RestateCachedRow.T_LONG      => Option(v) match {
      case Some(n: java.lang.Long)   => ResultValue.IntV(n)
      // decodeCell returns `java.lang.Long`; the legacy `decode`
      // returned a long primitive. The conversion is fine.
      case Some(n: java.lang.Integer) => ResultValue.IntV(n.toLong)
      case _                          => ResultValue.NullV
    }
    case RestateCachedRow.T_DOUBLE    => Option(v) match {
      case Some(d: java.lang.Double) => ResultValue.DoubleV(d)
      case _                         => ResultValue.NullV
    }
    case RestateCachedRow.T_BOOLEAN   => Option(v) match {
      case Some(b: java.lang.Boolean) => ResultValue.BoolV(b)
      case _                          => ResultValue.NullV
    }
    case RestateCachedRow.T_DECIMAL   => Option(v) match {
      // decodeCell returns `java.math.BigDecimal` (the JDK
      // BigDecimal). Convert to Scala BigDecimal via toPlainString
      // for stable repr (preserves scale per the legacy convention).
      case Some(bd: java.math.BigDecimal) => ResultValue.DecimalV(BigDecimal(bd.toPlainString))
      case _                              => ResultValue.NullV
    }
    case RestateCachedRow.T_TIMESTAMP => Option(v) match {
      // decodeCell returns `java.sql.Timestamp` (NOT
      // `java.time.Instant` as the previous implementation
      // assumed). The `java.sql.Timestamp` carries the
      // millis-since-epoch in JVM-default-zone-independent
      // representation. Convert via `.toInstant` which uses
      // the underlying UTC instant.
      case Some(ts: java.sql.Timestamp) => ResultValue.TimestampV(ts.toInstant)
      case _                            => ResultValue.NullV
    }
    case RestateCachedRow.T_DATE      => Option(v) match {
      // decodeCell returns `java.sql.Date` (NOT
      // `java.time.LocalDate`). Per the legacy convention,
      // the Date is anchored at UTC midnight of the date.
      // Convert via millis-since-epoch in UTC.
      case Some(d: java.sql.Date) => ResultValue.DateV(
          java.time.LocalDate.ofEpochDay(d.getTime / 86_400_000L)
        )
      case _                       => ResultValue.NullV
    }
    case RestateCachedRow.T_BINARY    => Option(v) match {
      case Some(b: Array[Byte]) => ResultValue.BinaryV(b)
      case _                    => ResultValue.NullV
    }
    case _                            => ResultValue.NullV
  }

  /**
   * Map a wire-format tag (`RestateCachedRow.T_*` constant) back
   * to the engine-portable `SealedDataType`. Used by
   * `fromRestateCachedRowAsPortable` to preserve the original
   * schema in the cache-HIT path. Matches the dispatch in
   * `EngineTypeTags` (PR-C1, sm8-platform).
   */
  private def tagToSealedDataType(tag: String): io.sm8.core.schema.SealedDataType = tag match {
    case RestateCachedRow.T_STRING    => io.sm8.core.schema.SealedDataType.Varchar
    case RestateCachedRow.T_LONG      => io.sm8.core.schema.SealedDataType.BigInt
    case RestateCachedRow.T_DOUBLE    => io.sm8.core.schema.SealedDataType.Double
    case RestateCachedRow.T_BOOLEAN   => io.sm8.core.schema.SealedDataType.Boolean
    case RestateCachedRow.T_DECIMAL   => io.sm8.core.schema.SealedDataType.Decimal(0, 0)
    case RestateCachedRow.T_TIMESTAMP => io.sm8.core.schema.SealedDataType.Timestamp
    case RestateCachedRow.T_DATE      => io.sm8.core.schema.SealedDataType.Date
    case RestateCachedRow.T_BINARY    => io.sm8.core.schema.SealedDataType.Varchar
    case _                            => io.sm8.core.schema.SealedDataType.Varchar
  }

  def fromRestateCachedRowAsPortable(row: RestateCachedRow): PortableQueryResult = {
    val fields: List[io.sm8.core.schema.Field] =
      row.fieldNames.zip(row.fieldTypes).map { case (name, tag) =>
        io.sm8.core.schema.Field.nonNull(name, tagToSealedDataType(tag))
      }
    PortableQueryResult(
      rows   = row.rows.toVector.map { cells =>
        ResultRow(
          values = if (cells == null) Nil
                   else cells.toList.zip(row.fieldTypes).map { case (encoded, tag) =>
                     // Null cells encode to a null `encoded`
                     // (per `encodeCell` for `ResultValue.NullV`).
                     if (encoded == null) ResultValue.NullV
                     else toResultValue(tag, PortableCellCodec.decodeCell(tag, encoded))
                   },
          schema = ResultSchema(Nil)
        )
      },
      schema = ResultSchema(fields)
    )
  }

  /**
   * Encode a `PortableQueryResult` to a `RestateCachedRow` (the wire
   * format used by the cache journal and `Restate.run`).
   *
   * Replaces the Java `QueryService.toRestateCachedRowFromPortable`
   * (semanticdf-platform lines 711-740). Iterates the portable's
   * rows + schema, encoding each `ResultValue` to its wire-form
   * string via `PortableCellCodec.encodeCell` (from PR-C2).
   *
   * ==Behavior preservation vs Java==
   *
   * The legacy Java code used `JavaConverters.asJavaIterable(...)`
   * to walk the portable's Scala `Vector` fields. The Scala version
   * iterates the `Vector` directly — no Java interop needed.
   *
   * @param portable the engine-portable result
   * @return         the cache-journal wire format
   */
  /**
   * Map an engine-portable `SealedDataType` to the wire-format
   * tag constant. Inverse of `tagToSealedDataType`. Matches the
   * dispatch in `EngineTypeTags`.
   */
  private def resultValueToTag(dt: io.sm8.core.schema.SealedDataType): String = dt match {
    case io.sm8.core.schema.SealedDataType.Varchar   => RestateCachedRow.T_STRING
    case io.sm8.core.schema.SealedDataType.Int      => RestateCachedRow.T_LONG
    case io.sm8.core.schema.SealedDataType.BigInt   => RestateCachedRow.T_LONG
    case io.sm8.core.schema.SealedDataType.Double   => RestateCachedRow.T_DOUBLE
    case io.sm8.core.schema.SealedDataType.Boolean  => RestateCachedRow.T_BOOLEAN
    case io.sm8.core.schema.SealedDataType.Timestamp => RestateCachedRow.T_TIMESTAMP
    case io.sm8.core.schema.SealedDataType.Date     => RestateCachedRow.T_DATE
    case _: io.sm8.core.schema.SealedDataType.Decimal => RestateCachedRow.T_DECIMAL
    case _                                           => RestateCachedRow.T_STRING
  }

  def toRestateCachedRowFromPortable(
      portable: PortableQueryResult
  ): RestateCachedRow = {
    val fieldNames: List[String] = portable.schema.fields.map(_.name).toList
    val fieldTypes: List[String] = portable.schema.fields.map(_.dataType match {
      case dt => resultValueToTag(dt)
    }).toList
    val rows: List[Array[String]] = portable.rows.toList.map { row =>
      row.values.toList.map(PortableCellCodec.encodeCell).toArray
    }
    RestateCachedRow(
      fieldNames = fieldNames,
      fieldTypes = fieldTypes,
      rows       = rows
    )
  }

  /**
   * Decode a `RestateCachedRow` to a [[QueryResult]] (the platform's
   * MCP wire response shape).
   *
   * Replaces the Java `QueryService.toQueryResultFromJournaled`
   * (semanticdf-platform lines 625-650).
   *
   * ==Behavior change vs Java (intentional)==
   *
   * The legacy Java code NPEs on null row entries (line 634:
   * `int cols = cells.length;` — `cells.length` throws if `cells`
   * is null). The Scala version handles null entries gracefully
   * (matching `fromRestateCachedRow`'s PR-C4b behavior: null
   * cells → `List.empty[Object]`). This creates internal
   * consistency between the two decoders and avoids NPEs at the
   * boundary. 0 callers in our reactor today, so the change is
   * dormant until the engine-portable path migration (PR-C5+).
   *
   * @param modelName  the model name (or `null` for "unknown")
   * @param journaled  the cached row to decode
   * @param maxRows    the truncation cap. Defaults to `100_000`
   *                   matching the legacy `CacheBridge.DefaultMaxRows`.
   *                   The env-var-aware `CacheBridge.effectiveMaxRows()`
   *                   value is the caller's responsibility (it
   *                   reaches this decode path once we wire it up
   *                   in PR-C5+).
   * @return           the decoded [[QueryResult]]
   */
  def toQueryResultFromJournaled(
      modelName: String,
      journaled: RestateCachedRow,
      maxRows: Int = 100_000
  ): QueryResult = {
    val rows = fromRestateCachedRow(journaled)
    QueryResult(
      model     = if (modelName == null) "unknown" else modelName,
      measures  = journaled.fieldNames,
      rows      = rows,
      truncated = rows.size >= maxRows,
      rowCount  = rows.size.toLong
    )
  }
}
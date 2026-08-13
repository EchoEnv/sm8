/*
 * SM8 Platform — Cell-level encoder/decoder for the engine-portable
 * cached-row wire format.
 *
 * Replaces the Java helpers in `semanticdf-platform/.../QueryService.java`:
 *   - `encodePortableCell` (lines 781-805)
 *   - `toJavaValue` (lines 346-370)
 *
 * Per [[scala-data-driven-refactor-mindset]] (sealed-trait dispatch
 * over `switch (simpleName)`): replaces `switch` on Java's
 * reflection-based simpleName with a Scala 2.13 pattern match
 * over the closed `ResultValue` ADT. Compiler enforces
 * exhaustiveness — adding a new `ResultValue` case forces an
 * update here.
 *
 * Per [[karpathy-guidelines-mindset]] (smallest correct change +
 * Scala 2.13 idiom + match existing style): pure data → String
 * and Object dispatch. No Scala 3 `enum`. Java-friendly
 * `Option[ResultValue]` overload added for null safety.
 *
 * Per [[scala-error-handling-mindset]]: pure function; unknown
 * types are impossible (sealed ADT exhaustiveness). The legacy
 * `default: throw new IllegalArgumentException(...)` branch is
 * deleted by the compiler — no imperative fallback.
 *
 * Per [[scala-impact-analysis-mindset]]: 0 callers in our reactor
 * (the Java `QueryService.encodePortableCell` + `toJavaValue`
 * stay in `/tmp/semanticdf` for later migration PRs). Pure
 * function, dormant until PR-C3+ (`toQueryResultFromPortable`)
 * + PR-C5 (`runQueryViaEngineRegistry`) consume it.
 *
 * ==Behavior change vs Java (dormant)==
 *
 * The Java `encodePortableCell` + `toJavaValue` handled 7 cases
 * (NullV, BoolV, IntV, DoubleV, DecimalV, StringV, TimestampV)
 * and threw `IllegalArgumentException` on `DateV` (a pre-DateV
 * case object added later). The Scala version is exhaustive
 * over all 8 `ResultValue` cases — the compiler enforces this.
 * `DateV` returns ISO-8601 string (`LocalDate.toString`, e.g.
 * "2024-01-15"). 0 callers in our reactor today, so the change
 * is dormant until the engine-portable path migration (PR-C5+)
 * wires the consumer to match.
 */
package io.sm8.platform.query

import io.sm8.core.engine.ResultValue

/**
 * Cell-level encoder/decoder for the engine-portable cached-row
 * wire format.
 *
 * Used by the journal-write path (`encodeCell`) and the MCP wire
 * response path (`toJavaValue`). Both are pure functions of the
 * `ResultValue` sealed ADT.
 */
object PortableCellCodec {

  /**
   * Encode a `ResultValue` to its String form for journaling.
   *
   * Wire format:
   *   - `None` / `NullV` → `null`
   *   - `BoolV(b)` → `"true"` / `"false"`
   *   - `IntV(n)` → `"42"` (decimal Long)
   *   - `DoubleV(d)` → `"3.14"` (Java's `Double.toString`)
   *   - `DecimalV(bd)` → `BigDecimal.toString` (preserves precision/scale)
   *   - `StringV(s)` → raw `s` (no quotes)
   *   - `TimestampV(i)` → `Instant.toString` (ISO-8601, e.g. "2024-01-15T10:30:00Z")
   *   - `DateV(d)` → `LocalDate.toString` (ISO-8601, e.g. "2024-01-15")
   */
  def encodeCell(v: Option[ResultValue]): String = v match {
    case None                            => null
    case Some(ResultValue.NullV)         => null
    case Some(ResultValue.BoolV(b))      => String.valueOf(b)
    case Some(ResultValue.IntV(n))       => String.valueOf(n)
    case Some(ResultValue.DoubleV(d))    => String.valueOf(d)
    case Some(ResultValue.DecimalV(bd))  => bd.toString
    case Some(ResultValue.StringV(s))    => s
    case Some(ResultValue.TimestampV(i)) => i.toString
    case Some(ResultValue.DateV(d))      => d.toString
  }

  /** Java-friendly overload: accept `null` directly. */
  def encodeCell(v: ResultValue): String = encodeCell(Option(v))

  /**
   * Convert a `ResultValue` to a boxed Java `Object` for the MCP
   * wire response.
   *
   * Wire format:
   *   - `None` / `NullV` → `null`
   *   - `BoolV(b)` → boxed `Boolean`
   *   - `IntV(n)` → boxed `Long`
   *   - `DoubleV(d)` → boxed `Double`
   *   - `DecimalV(bd)` → `BigDecimal`
   *   - `StringV(s)` → `String`
   *   - `TimestampV(i)` → `String` (ISO-8601, via `Instant.toString`)
   *   - `DateV(d)` → `String` (ISO-8601, via `LocalDate.toString`)
   *
   * Timestamp/Date are wire-encoded as ISO-8601 Strings (matching
   * the legacy Java behavior at `QueryService.java:365`+
   * `QueryService.java:800`).
   */
  def toJavaValue(v: Option[ResultValue]): Object = v match {
    case None                            => null
    case Some(ResultValue.NullV)         => null
    case Some(ResultValue.BoolV(b))      => java.lang.Boolean.valueOf(b)
    case Some(ResultValue.IntV(n))       => java.lang.Long.valueOf(n)
    case Some(ResultValue.DoubleV(d))    => java.lang.Double.valueOf(d)
    case Some(ResultValue.DecimalV(bd))  => bd
    case Some(ResultValue.StringV(s))    => s
    case Some(ResultValue.TimestampV(i)) => i.toString
    case Some(ResultValue.DateV(d))      => d.toString
  }

  /** Java-friendly overload: accept `null` directly. */
  def toJavaValue(v: ResultValue): Object = toJavaValue(Option(v))

  /**
   * Inverse of [[encodeCell]]. Decodes a string-encoded cell back
   * to its typed Java Object.
   *
   * Throws [[IllegalArgumentException]] on unknown tags
   * (forward-compatibility break — the cache row will be rejected
   * if a new tag has been added since the row was written).
   *
   * ==T_DATE timezone handling==
   *
   * `Date.getTime()` must be JVM-timezone-independent on decode.
   * `Date.valueOf(LocalDate.parse(s))` would reconstruct a Date
   * whose `getTime()` is computed at JVM-default midnight — which
   * silently shifts across JVM restarts in different timezones. The
   * Java legacy code (and this Scala version) builds the Date from
   * an Instant anchored at UTC midnight of the date, so `getTime()`
   * returns the underlying millis (UTC midnight of the date) and
   * is JVM-timezone-independent.
   *
   * The Date is constructed via `java.sql.Date(long)` (not
   * `Date.from(Instant)`, which would return `java.util.Date` via
   * the parent-class static method — the parent class). The
   * `java.sql.Date(long)` constructor pins the runtime class to
   * `java.sql.Date`.
   *
   * ==T_TIMESTAMP timezone handling==
   *
   * `Timestamp.from(Instant)` gives a `java.sql.Timestamp` with the
   * same Instant regardless of JVM timezone — the underlying millis
   * are preserved.
   *
   * @param tag      one of the 9 `RestateCachedRow.T_*` constants
   * @param encoded  the string-encoded cell value
   * @return         the typed Java Object (or `null` for `T_NULL` / null
   *                 encoded)
   * @throws IllegalArgumentException if `tag` is not one of the 9 known tags
   */
  def decodeCell(tag: String, encoded: String): Object = {
    if (encoded == null || tag == RestateCachedRow.T_NULL) {
      null
    } else tag match {
      case RestateCachedRow.T_STRING    => encoded
      case RestateCachedRow.T_LONG      => java.lang.Long.valueOf(encoded)
      case RestateCachedRow.T_DOUBLE    => java.lang.Double.valueOf(encoded)
      case RestateCachedRow.T_DECIMAL   => new java.math.BigDecimal(encoded)
      case RestateCachedRow.T_BOOLEAN   => java.lang.Boolean.valueOf(encoded)
      case RestateCachedRow.T_TIMESTAMP =>
        java.sql.Timestamp.from(java.time.Instant.parse(encoded))
      case RestateCachedRow.T_DATE =>
        new java.sql.Date(
          java.time.LocalDate.parse(encoded)
            .atStartOfDay(java.time.ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli()
        )
      case RestateCachedRow.T_BINARY =>
        java.util.Base64.getDecoder.decode(encoded)
      case _ =>
        throw new IllegalArgumentException(
          "unknown RestateCachedRow type tag: " + tag
        )
    }
  }
}
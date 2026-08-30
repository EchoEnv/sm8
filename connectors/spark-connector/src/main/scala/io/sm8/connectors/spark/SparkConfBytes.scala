/*
 * SM8 Spark Connector — SparkConfBytes (PR-210).
 *
 * Parses the byte-string grammar Spark's own `SQLConf` accepts for
 * memory/byte-sized keys such as `spark.sql.autoBroadcastJoinThreshold`.
 *
 * ==Why a connector-local parser (not a Spark API call)==
 *
 * The authoritative parser Spark itself uses for
 * `spark.sql.autoBroadcastJoinThreshold` is
 * `org.apache.spark.internal.config.ConfigHelpers.byteFromString`
 * (wired via `ConfigBuilder.bytesConf(ByteUnit.BYTE)`; see
 * `SQLConf.AUTO_BROADCASTJOIN_THRESHOLD`). That helper is
 * `private[spark]`: it is NOT callable from downstream connectors.
 * The public `org.apache.spark.network.util.JavaUtils.byteStringAsBytes`
 * is NOT equivalent — its grammar `([0-9]+)([a-z]+)?` rejects the
 * leading `-`, so every negative sentinel (`-1`, `-2b`) throws
 * NumberFormatException, and the documented `-1` "disable
 * broadcasting" value becomes unparsable. `Utils.byteStringAsBytes`
 * (`org.apache.spark.util`) delegates to the same grammar and is
 * additionally `private[spark]`.
 *
 * `byteFromString` is a two-step rule:
 *
 *   1. Split an optional leading `-` sign off, remembering the sign.
 *   2. Parse the remainder with `JavaUtils.byteStringAs` — the
 *      case-insensitive suffix grammar
 *      `b|k|kb|m|mb|g|gb|t|tb|p|pb` (no suffix = plain bytes),
 *      as registered in `JavaUtils.byteSuffixes`. NOTE: the
 *      `kib`/`mib`/`gib`/`tib`/`pib` variants that Spark's error
 *      message names are NOT registered in the byteSuffixes map
 *      and therefore throw NumberFormatException — they are NOT
 *      accepted. Malformed input (bad suffix, fractional value,
 *      non-numeric text) throws NumberFormatException. Values
 *      whose magnitude overflows `Long.MAX_VALUE` after the
 *      suffix-scaled conversion throw IllegalArgumentException
 *      (from `ByteUnit.convertFrom`).
 *
 * Multiplying the magnitudes back by the sign reproduces Spark's
 * semantics exactly: `-1` → `-1`, `-2b` → `-2`, `-2m` → `-2097152`,
 * `1g` → `1073741824`, `512kb` → `524288`, `10MB` → `10485760`,
 * `abc` → NumberFormatException.
 *
 * ==Fallback contract (unchanged from PR-197/PR-209)==
 *
 * Callers keep their existing `NumberFormatException` catch arms:
 * malformed values (`abc`) still fall back to the seed default /
 * not-disabled, matching the documented PR-197 semantics. The
 * PR-210 change is only that previously-misparsed VALID values
 * (`1g`, `-2m`, `512kb`) now parse to their true byte counts.
 *
 * Per scala-impact-analysis: this object is pure (no SparkSession,
 * no conf reads, no I/O) and lives in the connector layer — the
 * only layer allowed to know Spark's config grammar (RULE#1: core
 * carries no engine-specific knowledge).
 *
 * Per scala-bug-hunting: the sign-split happens BEFORE suffix
 * parsing, so a value like `1g-` (suffix then sign) is rejected —
 * matching Spark, which also only honors a LEADING sign.
 */
package io.sm8.connectors.spark

import org.apache.spark.network.util.{ByteUnit, JavaUtils}

private[spark] object SparkConfBytes {

  /** Parse a Spark-style byte string to its exact byte count.
   *
   * Accepts the same grammar `SQLConf.bytesConf(ByteUnit.BYTE)`
   * accepts: optional leading `-`, then an optional binary suffix
   * (`b`/`k`/`kb`/`m`/`mb`/`g`/`gb`/`t`/`tb`/`p`/`pb`,
   * case-insensitive). No suffix means plain bytes. The
   * `kib`/`mib`/`gib`/`tib`/`pib` variants named in Spark's error
   * message are NOT accepted — they are NOT in
   * `JavaUtils.byteSuffixes`.
   *
   * @param raw the raw conf value (e.g. `"1g"`, `"-2m"`, `"512kb"`,
   *            `"104857600"`)
   * @return the exact byte count, negative when the input was
   *         negative
   * @throws NumberFormatException when the input does not match
   *         Spark's grammar (bad suffix, fractional value,
   *         non-numeric text, empty string — same fallback contract
   *         as PR-197)
   * @throws IllegalArgumentException when the parsed magnitude
   *         overflows `Long.MAX_VALUE` after the suffix-scaled
   *         conversion (callers widen their catch arms to include
   *         this type so seedBroadcastThreshold keeps the same
   *         fallback semantics for both exception shapes)
   */
  def parseBytes(raw: String): Long = {
    val (magnitude, sign) =
      if (raw.length > 0 && raw.charAt(0) == '-') (raw.substring(1), -1L)
      else (raw, 1L)
    sign * JavaUtils.byteStringAs(magnitude, ByteUnit.BYTE)
  }
}

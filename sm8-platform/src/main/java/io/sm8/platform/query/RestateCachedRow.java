package io.sm8.platform.query;

import java.io.Serializable;
import java.util.List;

/**
 * A purely-data record mirror of the library's
 * {@link io.sm8.core.cache.CachedResult} for use across a
 * Restate journal boundary.
 *
 * <p>Why this exists: the library's {@code CachedResult} carries
 * {@code Array[org.apache.spark.sql.Row>}, but the Restate SDK's
 * default Jackson serializer can WRITE {@code Row} values via
 * {@code GenericRowWithSchema} but cannot READ them back on journal
 * replay \u2014 {@code Row} is an abstract Spark class with no
 * default constructor. End-to-end tests showed the deserialization
 * throws {@code InvalidDefinitionException: Cannot construct
 * instance of org.apache.spark.sql.Row}.
 *
 * <p>This record uses a 9-tag vocabulary (T_LONG, T_STRING,
 * T_BIG_DECIMAL, etc.) written at journal-write time and decoded
 * back to the matching Java type at journal-read time. Earlier
 * designs used an untyped {@code List<Object[]>} payload where
 * Jackson round-tripped
 * {@code Long \u2192 Integer} (overflow), {@code BigDecimal \u2192 Double}
 * (precision loss), {@code Timestamp \u2192 epoch Long} (unit
 * confusion), and {@code byte[] \u2192 Base64 String} (silent type
 * change). The fix:
 * <ul>
 *   <li>{@code fieldTypes} is now a {@code List<String>} of type
 *       tags, one per column. Tags are stable strings
 *       (one of {@code "null"}, {@code "string"}, {@code "long"},
 *       {@code "double"}, {@code "decimal"}, {@code "boolean"},
 *       {@code "timestamp"}, {@code "binary"}).
 *   <li>{@code rows} is now {@code List<String[]>}; each cell is
 *       a string-encoded form of the original value, decoded back
 *       to the typed Object based on the corresponding
 *       {@code fieldTypes} entry on replay.
 * </ul>
 * The result: every Spark cell type round-trips with full type
 * fidelity. {@code Long(5_000_000_000L)} is no longer truncated to
 * an {@code Integer} (overflow); {@code BigDecimal("1234.567890")}
 * is no longer coerced to {@code Double(1234.56789)} (precision
 * loss); a {@code java.sql.Timestamp} carrying microseconds is no
 * longer flattened to a millisecond {@code Long}.
 *
 * <p>Defined as a Java {@code record} so it round-trips cleanly
 * through Jackson out of the box.
 */
public record RestateCachedRow(
    List<String> fieldNames,
    List<String> fieldTypes,
    List<String[]> rows) implements Serializable {

  /** Allowed cell-type tags. */
  public static final String T_NULL = "null";
  public static final String T_STRING = "string";
  public static final String T_LONG = "long";
  public static final String T_DOUBLE = "double";
  public static final String T_DECIMAL = "decimal";
  public static final String T_BOOLEAN = "boolean";
  public static final String T_TIMESTAMP = "timestamp";
  public static final String T_DATE = "date";
  public static final String T_BINARY = "binary";

  /**
   * Compact constructor \u2014 reject obviously-malformed input early.
   */
  public RestateCachedRow {
    if (fieldNames == null) {
      throw new IllegalArgumentException("fieldNames must be non-null");
    }
    if (fieldTypes == null) {
      throw new IllegalArgumentException("fieldTypes must be non-null");
    }
    if (rows == null) {
      throw new IllegalArgumentException("rows must be non-null");
    }
    if (fieldNames.size() != fieldTypes.size()) {
      throw new IllegalArgumentException(
          "fieldNames.size() (" + fieldNames.size() + ") != fieldTypes.size() ("
              + fieldTypes.size() + ")");
    }
    for (int i = 0; i < rows.size(); i++) {
      String[] row = rows.get(i);
      if (row != null && row.length != fieldNames.size()) {
        throw new IllegalArgumentException(
            "row " + i + " has " + row.length + " cells, expected "
                + fieldNames.size());
      }
    }
  }
}

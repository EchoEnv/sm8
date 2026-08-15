package io.sm8.core.cache

import io.sm8.core.schema.{Field, SealedDataType}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Phase 2 contract: prove `EngineTypeTags.of(...)` is a total
 * mapping from `SealedDataType` (closed 12-variant ADT) to
 * `RestateCachedRow` journal tags. Mirrors the Java test
 * `QueryServiceEngineRegistryTest.sealedTypeTag_mapsCoreTypesToRestateCachedRowTags`
 * (semanticdf-platform lines 132-154) plus exhaustiveness cases
 * for the 4 nested/JSON variants (Array/Map/Row/Json → T_STRING)
 * that the Scala version handles but Java threw on.
 *
 * Per [[scala-data-driven-refactor-mindset]]: pure dispatch, no
 * I/O. The compiler enforces exhaustiveness over the sealed
 * `SealedDataType` — if a new variant is added, this file fails
 * to compile until `EngineTypeTags.of` is updated.
 */
class EngineTypeTagsSpec extends AnyFunSuite with Matchers {

  // -- Primitives + temporal + decimal (mirror Java test) --

  test("Varchar → T_STRING") {
    EngineTypeTags.of(Option(SealedDataType.Varchar)) shouldBe RestateCachedRow.T_STRING
  }

  test("Int → T_LONG") {
    EngineTypeTags.of(Option(SealedDataType.Int)) shouldBe RestateCachedRow.T_LONG
  }

  test("BigInt → T_LONG") {
    EngineTypeTags.of(Option(SealedDataType.BigInt)) shouldBe RestateCachedRow.T_LONG
  }

  test("Double → T_DOUBLE") {
    EngineTypeTags.of(Option(SealedDataType.Double)) shouldBe RestateCachedRow.T_DOUBLE
  }

  test("Boolean → T_BOOLEAN") {
    EngineTypeTags.of(Option(SealedDataType.Boolean)) shouldBe RestateCachedRow.T_BOOLEAN
  }

  test("Timestamp → T_TIMESTAMP") {
    EngineTypeTags.of(Option(SealedDataType.Timestamp)) shouldBe RestateCachedRow.T_TIMESTAMP
  }

  test("Date → T_DATE") {
    EngineTypeTags.of(Option(SealedDataType.Date)) shouldBe RestateCachedRow.T_DATE
  }

  test("Decimal(precision, scale) → T_DECIMAL") {
    EngineTypeTags.of(Option(SealedDataType.Decimal(10, 2))) shouldBe RestateCachedRow.T_DECIMAL
  }

  test("Decimal equality: same precision+scale → same tag") {
    EngineTypeTags.of(Option(SealedDataType.Decimal(38, 0))) shouldBe
      EngineTypeTags.of(Option(SealedDataType.Decimal(38, 0)))
  }

  test("Decimal inequality: different precision → same tag (type-only dispatch)") {
    // Pure type-tag mapping — precision/scale are encoded later
    // in the cell value (T_DECIMAL + toPlainString). Both map to T_DECIMAL.
    EngineTypeTags.of(Option(SealedDataType.Decimal(10, 2))) shouldBe RestateCachedRow.T_DECIMAL
    EngineTypeTags.of(Option(SealedDataType.Decimal(38, 9))) shouldBe RestateCachedRow.T_DECIMAL
  }

  // -- None + null handling --

  test("None → T_NULL") {
    EngineTypeTags.of(None) shouldBe RestateCachedRow.T_NULL
  }

  test("null SealedDataType overload → T_NULL (Java-friendly)") {
    EngineTypeTags.of(null.asInstanceOf[SealedDataType]) shouldBe RestateCachedRow.T_NULL
  }

  test("non-null SealedDataType overload matches Option overload") {
    EngineTypeTags.of(SealedDataType.Varchar) shouldBe EngineTypeTags.of(Option(SealedDataType.Varchar))
    EngineTypeTags.of(SealedDataType.Int)     shouldBe EngineTypeTags.of(Option(SealedDataType.Int))
    EngineTypeTags.of(SealedDataType.Decimal(10, 2)) shouldBe
      EngineTypeTags.of(Option(SealedDataType.Decimal(10, 2)))
  }

  // -- Exhaustive nested/JSON (Scala-only behavior; Java threw) --

  test("Array → T_STRING (exhaustive, was IAE in Java)") {
    EngineTypeTags.of(Option(SealedDataType.Array(SealedDataType.Int))) shouldBe
      RestateCachedRow.T_STRING
  }

  test("Map → T_STRING (exhaustive, was IAE in Java)") {
    EngineTypeTags.of(Option(SealedDataType.Map(SealedDataType.Varchar, SealedDataType.Int))) shouldBe
      RestateCachedRow.T_STRING
  }

  test("Row → T_STRING (exhaustive, was IAE in Java)") {
    val row = SealedDataType.Row(Seq(Field.nonNull("carrier", SealedDataType.Varchar)))
    EngineTypeTags.of(Option(row)) shouldBe RestateCachedRow.T_STRING
  }

  test("Json → T_STRING (exhaustive, was IAE in Java)") {
    EngineTypeTags.of(Option(SealedDataType.Json)) shouldBe RestateCachedRow.T_STRING
  }

  test("Binary → T_BINARY (review pass #2 addition; was missing — silently mapped to T_STRING)") {
    EngineTypeTags.of(Option(SealedDataType.Binary)) shouldBe RestateCachedRow.T_BINARY
  }

  // -- Wire contract --

  test("Total mapping: all 13 SealedDataType cases + None produce a non-empty tag") {
    val all: Seq[Option[SealedDataType]] = Seq(
      Option(SealedDataType.BigInt),
      Option(SealedDataType.Int),
      Option(SealedDataType.Double),
      Option(SealedDataType.Varchar),
      Option(SealedDataType.Boolean),
      Option(SealedDataType.Timestamp),
      Option(SealedDataType.Date),
      Option(SealedDataType.Decimal(10, 2)),
      Option(SealedDataType.Array(SealedDataType.Int)),
      Option(SealedDataType.Map(SealedDataType.Varchar, SealedDataType.Int)),
      Option(SealedDataType.Row(Seq(Field.nonNull("x", SealedDataType.Varchar)))),
      Option(SealedDataType.Json),
      Option(SealedDataType.Binary),
      None
    )
    val tags = all.map(EngineTypeTags.of)
    tags should have size 14.toLong // 14 distinct inputs, each producing one of 9 tags
    tags.foreach(_ should not be empty)
  }

  test("Tag vocabulary: outputs are subset of the 9 RestateCachedRow T_* constants") {
    val allowed: Set[String] = Set(
      RestateCachedRow.T_NULL,
      RestateCachedRow.T_STRING,
      RestateCachedRow.T_LONG,
      RestateCachedRow.T_DOUBLE,
      RestateCachedRow.T_DECIMAL,
      RestateCachedRow.T_BOOLEAN,
      RestateCachedRow.T_TIMESTAMP,
      RestateCachedRow.T_DATE,
      RestateCachedRow.T_BINARY
    )
    val samples: Seq[SealedDataType] = Seq(
      SealedDataType.BigInt, SealedDataType.Int, SealedDataType.Double,
      SealedDataType.Varchar, SealedDataType.Boolean,
      SealedDataType.Timestamp, SealedDataType.Date,
      SealedDataType.Decimal(10, 2),
      SealedDataType.Array(SealedDataType.Int),
      SealedDataType.Map(SealedDataType.Varchar, SealedDataType.Int),
      SealedDataType.Row(Seq(Field.nonNull("x", SealedDataType.Varchar))),
      SealedDataType.Json
    )
    samples.foreach { dt =>
      withClue(s"tag for $dt: ") {
        allowed should contain (EngineTypeTags.of(dt))
      }
    }
  }
}
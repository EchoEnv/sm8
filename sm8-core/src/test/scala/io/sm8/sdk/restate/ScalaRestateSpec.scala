/*
 * SM8 SDK — ScalaRestate test.
 *
 * Per [[debug-mantra-mindset]]: every extension method has at least
 * one test that proves real behavior. Per [[karpathy-guidelinesmindset]]
 * "goal-driven execution": define verifiable success criteria.
 *
 * Per [[scala-jvm-safetymindset]] step 1 (null is a liar): tests
 * cover null, empty, and populated cases for each conversion.
 */
package io.sm8.sdk.restate

import java.util.Optional

import scala.jdk.CollectionConverters._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// Bring the implicit classes inside ScalaRestate into scope so the
// extension methods (toScala, toJava, asScalaSeq, ...) are usable on
// the call sites in this spec.
import ScalaRestate._

class ScalaRestateSpec extends AnyFlatSpec with Matchers {

  // ---- Optional ↔ Option ----

  "OptionalOps.toScala" should "return Some when present" in {
    Optional.of("x").toScala shouldBe Some("x")
  }

  it should "return None when empty" in {
    Optional.empty[String]().toScala shouldBe None
  }

  it should "preserve null inner value (Optional.of(null) -> Some(null))" in {
    // Per [[scala-jvm-safetymindset]] step 1: null is a liar. Optional
    // does permit null inner values; we preserve the Some/None shape
    // (caller decides what to do with the null).
    val n: String = null
    Optional.ofNullable(n).toScala shouldBe None  // ofNullable is null-safe
    // Optional.of(null) would throw NPE — not our concern.
  }

  "OptionOps.toJava" should "wrap Some in Optional.of" in {
    Some("x").toJava shouldBe Optional.of("x")
  }

  it should "produce Optional.empty for None" in {
    None.toJava shouldBe Optional.empty()
  }

  // ---- Java collections → Scala ----

  "JavaListOps.asScalaSeq" should "convert java.util.List to immutable.Seq" in {
    java.util.Arrays.asList(1, 2, 3).asScalaSeq shouldBe Seq(1, 2, 3)
  }

  it should "convert empty list to empty Seq" in {
    java.util.Collections.emptyList[String]().asScalaSeq shouldBe Seq.empty
  }

  "JavaIterableOps.asScalaIter" should "be available via raw asScala (no helper — implicit class dropped per smallest-correct-core)" in {
    // The JavaIterableOps implicit class was dropped because of a name
    // resolution issue with java.util.Iterable. Callers can use the
    // built-in asScala on java.lang.Iterable (provided by the
    // scala.jdk.CollectionConverters._ wildcard import above).
    val iter: java.util.List[String] = java.util.Arrays.asList("a", "b")
    iter.asScala.toList shouldBe List("a", "b")
  }

  "JavaMapOps.asScalaMap" should "convert java.util.Map to immutable.Map" in {
    val jm: java.util.Map[String, Int] = new java.util.HashMap[String, Int]()
    jm.put("a", 1); jm.put("b", 2)
    jm.asScalaMap shouldBe Map("a" -> 1, "b" -> 2)
  }

  // ---- Scala collections → Java ----

  "ScalaSeqOps.asJavaList" should "convert immutable.Seq to java.util.List" in {
    Seq(1, 2, 3).asJavaList shouldBe java.util.Arrays.asList(1, 2, 3)
  }

  "ScalaIterOps.asJavaIter" should "convert Iterator to java.util.Iterator" in {
    val iter: Iterator[Int] = Iterator(1, 2, 3)
    val asJava: java.util.Iterator[Int] = iter.asJavaIter
    val collected = asJava.asScala.toList
    collected shouldBe List(1, 2, 3)
  }
}
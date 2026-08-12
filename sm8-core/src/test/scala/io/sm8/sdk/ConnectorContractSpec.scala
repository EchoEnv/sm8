/*
 * SM8 Core — ConnectorContractSpec.
 *
 * Step-1 skeleton. Asserts that the Connector trait exists, has the
 * expected shape (name, connect, query, schema), and that a minimal
 * in-memory implementation compiles.
 *
 * Step 2 promotes this to the full conformance suite enforcing the
 * RFC §12 four assertions:
 *   1. connect() with valid config succeeds
 *   2. connect() with invalid config raises a clear error
 *   3. query() returns data matching schema()
 *   4. query() on a malformed request raises (no garbage data)
 *
 * Per karpathy-guidelines-mindset + scala-error-handling-mindset:
 * the full Step 2 suite will use Either[EngineError, *] for typed
 * failures, once EngineError moves in (Step 0).
 */
package io.sm8.sdk

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConnectorContractSpec extends AnyFlatSpec with Matchers {

  "Connector" should "expose name, connect, query, schema as its contract" in {
    val methods = classOf[Connector].getMethods.map(_.getName).toSet
    Seq("name", "connect", "query", "schema").foreach { m =>
      methods should contain(m)
    }
  }

  it should "be implementable by a minimal in-memory Connector" in {
    val inMem = new Connector {
      def name: String = "in-memory-test"
      def connect(config: ConnectorConfig): Unit = ()
      def query(request: SemanticQuery): ResultRows = ???
      def schema(): ConnectorSchema = ???
    }
    inMem.name shouldBe "in-memory-test"
    inMem shouldBe a [Connector]
  }

  "The Connector contract" should "freeze name as a String property" in {
    val inMem = new Connector {
      def name: String = "x"
      def connect(config: ConnectorConfig): Unit = ()
      def query(request: SemanticQuery): ResultRows = ???
      def schema(): ConnectorSchema = ???
    }
    inMem.name shouldBe a [String]
  }
}
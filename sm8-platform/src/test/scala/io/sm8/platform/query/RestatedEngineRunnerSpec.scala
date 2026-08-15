package io.sm8.platform.query

import io.sm8.platform.query.cache._
import java.util.function.Supplier

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for `RestatedEngineRunner` (PR-C5b-ext-γ).
 *
 * Per [[karpathy-guidelines-mindset]] "smallest correct test
 * footprint": this spec only covers the surface that exists
 * today (the supplier-direct `runJournaled` method). When the
 * follow-up PR adds the actual `@Service` handler wiring with
 * `HandlerContext.submitRun`, those tests will land alongside
 * `sdk-testing` + Testcontainers.
 *
 * Per [[scala-jvm-safety-mindset]] "null is a liar":
 * the `isInRestateHandlerThread` probe is encapsulated — tests
 * don't need to mock thread-locals or Restate internals.
 *
 * Per [[scala-error-handling-mindset]]: supplier exceptions
 * propagate; no swallow.
 */
class RestatedEngineRunnerSpec extends AnyFunSuite with Matchers {

  // -- runJournaled: supplier-direct path --

  test("runJournaled: invokes the supplier exactly once and returns its value") {
    // Per [[karpathy-guidelines-mindset]] "happy path first": the
    // simplest correct behavior the helper must guarantee.
    var callCount = 0
    val supplier = new Supplier[String] {
      override def get(): String = {
        callCount += 1
        "first call"
      }
    }
    val result = RestatedEngineRunner.runJournaled(
      "test.handler",
      classOf[String],
      supplier
    )
    result shouldBe "first call"
    callCount shouldBe 1
  }

  test("runJournaled: returns the supplier's value bit-for-bit (no wrapping)") {
    val sentinel: Array[Byte] = Array[Byte](1, 2, 3, 4, 5)
    val supplier = new Supplier[Array[Byte]] {
      override def get(): Array[Byte] = sentinel
    }
    val out = RestatedEngineRunner.runJournaled(
      "test.byte-handler",
      classOf[Array[Byte]],
      supplier
    )
    out should be theSameInstanceAs sentinel
  }

  test("runJournaled: supplier exceptions propagate (no swallow)") {
    // Per [[scala-error-handling-mindset]]: the helper must
    // rethrow supplier exceptions — wrapping them in
    // `Either` or `Try` would hide the original error and
    // complicate the engine-boundary contract.
    val cause = new RuntimeException("engine threw")
    val supplier = new Supplier[String] {
      override def get(): String = throw cause
    }
    val ex = intercept[RuntimeException] {
      RestatedEngineRunner.runJournaled("test.throw", classOf[String], supplier)
    }
    ex should be theSameInstanceAs cause
  }

  // -- Future handler-thread probe --

  test("isInRestateHandlerThread: returns false today (no Restate runtime wired)") {
    // Per [[scala-jvm-safety-mindset]]: thread-locals leak
    // across tests when not cleaned. We don't currently use
    // a thread-local, but we explicitly verify the public
    // observable: callers on the regular test thread (not a
    // Restate handler thread) must succeed. The probe is
    // private; we exercise it through `runJournaled`'s
    // happy path (which would throw `IllegalStateException`
    // if the probe ever started returning `true`).
    val supplier = new Supplier[String] {
      override def get(): String = "ok"
    }
    noException should be thrownBy RestatedEngineRunner.runJournaled(
      "test.probe",
      classOf[String],
      supplier
    )
  }
}

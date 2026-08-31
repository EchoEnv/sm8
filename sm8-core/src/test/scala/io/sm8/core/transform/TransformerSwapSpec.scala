/*
 * SM8 Core — TransformerSwapSpec.
 *
 * Step 5: direct unit tests for TransformerRegistry swap semantics
 * (Q3 = exactly one Transformer active at a time) and Pipeline
 * format-stage integration.
 *
 * Per [[debug-mantra-mindset]]: these tests assert real behavior —
 * not silent passes (the Step 3 audit caught one such test).
 *
 * Per [[scala-perf-testing-mindset]]: side-channel counters via
 * AtomicInteger (no `var`, thread-safe) to verify which Transformer
 * the Pipeline invoked.
 *
 * The pipeline-integration tests ride a `new Request {}` vehicle —
 * the Pipeline performs no engine dispatch in the in-tree fallback
 * (production requests flow through the `EngineProvider` family).
 * The format stage still invokes the active Transformer on the
 * Context regardless of request type.
 */
package io.sm8.core.transform

import java.util.concurrent.atomic.AtomicInteger

import io.sm8.core.EngineImpl
import io.sm8.sdk.{Context, Plugin, Request, Transformer}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransformerSwapSpec extends AnyFlatSpec with Matchers {

  // ---- Test fixtures ----

  /**
   * Counting Transformer — increments a counter every time it
   * runs. Test fixture only (per [[scala-jvm-safety-mindset]]: var
   * in core internals would be a violation; here it's a fixture
   * counter, single-threaded, scope-bounded).
   */
  private final class CountingTransformer(
      override val name: String,
      override val priority: Int,
      counter: AtomicInteger
  ) extends Transformer {
    override def transform(context: Context): Context = {
      counter.incrementAndGet()
      context
    }
  }

  // ---- Tests ----

  "TransformerRegistry" should "auto-activate the first registered Transformer" in {
    val engine = EngineImpl()
    engine.transformers.register(JsonTransformer())
    engine.transformers.active.map(_.name) shouldBe Some("json")
  }

  it should "swap the active Transformer via setActive(name)" in {
    val engine = EngineImpl()
    engine.transformers.register(JsonTransformer())
    val markdown = new CountingTransformer("markdown", priority = 100, counter = new AtomicInteger(0))
    engine.transformers.register(markdown)

    // First registered is auto-active.
    engine.transformers.active.map(_.name) shouldBe Some("json")

    // Swap to markdown.
    engine.transformers.setActive("markdown") shouldBe Some(markdown)
    engine.transformers.active.map(_.name) shouldBe Some("markdown")
  }

  it should "return None when setActive is called with an unknown name" in {
    val engine = EngineImpl()
    engine.transformers.register(JsonTransformer())
    engine.transformers.setActive("does-not-exist") shouldBe None
    // Active remains unchanged on failed swap.
    engine.transformers.active.map(_.name) shouldBe Some("json")
  }

  "Pipeline" should "invoke the active Transformer at the format stage" in {
    val jsonCounter    = new AtomicInteger(0)
    val markdownCounter = new AtomicInteger(0)

    val engine = EngineImpl()

    // Replace the built-in JsonTransformer with our counting ones
    // (still named "json" and "markdown" so swap works on names).
    val jsonTr = new CountingTransformer("json",    priority = 100, counter = jsonCounter)
    val mdTr   = new CountingTransformer("markdown", priority = 100, counter = markdownCounter)
    engine.transformers.register(jsonTr)
    engine.transformers.register(mdTr)

    // Run with the auto-active (first registered = "json").
    engine.run(new Request {})
    jsonCounter.get() shouldBe 1
    markdownCounter.get() shouldBe 0

    // Swap and run again.
    engine.transformers.setActive("markdown")
    engine.run(new Request {})
    markdownCounter.get() shouldBe 1
    jsonCounter.get() shouldBe 1  // unchanged from previous run
  }

  "Plugin" should "be able to register a Transformer via setup(engine)" in {
    // Verifies the Plugin → Transformer integration (a Plugin's setup
    // method is the standard way to register built-in transformers).
    val engine = EngineImpl()

    val jsonCounter = new AtomicInteger(0)
    val plugin = new Plugin {
      override def setup(engine: io.sm8.sdk.Engine): Unit = {
        engine.transformers.register(
          new CountingTransformer("counting-json", priority = 100, counter = jsonCounter)
        )
      }
    }
    engine.use(plugin)
    engine.transformers.active.map(_.name) shouldBe Some("counting-json")

    engine.run(new Request {})
    jsonCounter.get() shouldBe 1
  }
}
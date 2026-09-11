/*
 * SM8 Core — CronContractSpec.
 *
 * Tests the NextFireTimeCalculator CONTRACT via a reference
 * implementation. The production cron-utils impl lives in the
 * platform layer per RFC §3; the core tests pin the contract
 * semantics with a deterministic stub so they stay dependency-free.
 */
package io.sm8.core.schedule

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CronContractSpec extends AnyFlatSpec with Matchers {

  behavior of "NextFireTimeCalculator contract"

  private val stubCalc = new NextFireTimeCalculator {
    override def nextFire(expression: String, afterMillis: Long): Either[String, Long] =
      if (expression == "bad cron") Left(s"invalid cron expression 'bad cron'")
      else Right(afterMillis + 60_000L)
  }

  it should "return Right(nextMillis) for a valid expression" in {
    stubCalc.nextFire("0 0 * * *", 1000L) shouldBe Right(61000L)
  }

  it should "return Left(reason) for an unparseable expression" in {
    val r = stubCalc.nextFire("bad cron", 1000L)
    r.isLeft shouldBe true
    r.swap.toOption.get should include("bad cron")
  }

  it should "guarantee strict monotonicity: next > after" in {
    val r = stubCalc.nextFire("*/5 * * * *", 5000L)
    r.toOption.get should be > 5000L
  }

  behavior of "JobDescriptor and friends"

  it should "carry schedule + target as pure data" in {
    val d = JobDescriptor("j1", CronSchedule("0 0 * * *"),
      JobTarget("Svc", "collect", Some("key1"), Some("""{"a":1}""")))
    d.jobId shouldBe "j1"
    d.schedule.expression shouldBe "0 0 * * *"
    d.target.objectKey shouldBe Some("key1")
  }

  it should "be a value class over the expression string" in {
    CronSchedule("*/5 * * * *").expression shouldBe "*/5 * * * *"
  }
}

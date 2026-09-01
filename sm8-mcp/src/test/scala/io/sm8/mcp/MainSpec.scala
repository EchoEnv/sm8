/*
 * SM8 MCP — MainSpec (CLI parser).
 *
 * Typed parse tests for the `io.sm8.mcp.Main.parseArgs` function.
 * Mirrors the MainSpec pattern in sm8-server. No HTTP, no SDK
 * lifecycle — pure arg parsing.
 */
package io.sm8.mcp

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Duration

class MainSpec extends AnyFunSuite with Matchers {

  test("parseArgs: defaults are ingress-url=http://127.0.0.1:8080, timeout=30s") {
    Main.parseArgs(List.empty) match {
      case Right(cli) =>
        cli.ingressUrl shouldBe "http://127.0.0.1:8080"
        cli.requestTimeout shouldBe Duration.ofSeconds(30)
      case Left(err) => fail(s"unexpected: ${err.reason}")
    }
  }

  test("parseArgs: --ingress-url <u> parses") {
    Main.parseArgs(List("--ingress-url", "http://10.0.0.42:9090")) match {
      case Right(cli) => cli.ingressUrl shouldBe "http://10.0.0.42:9090"
      case Left(err) => fail(s"unexpected: ${err.reason}")
    }
  }

  test("parseArgs: --request-timeout <secs> parses to Duration") {
    Main.parseArgs(List("--request-timeout", "60")) match {
      case Right(cli) => cli.requestTimeout shouldBe Duration.ofSeconds(60)
      case Left(err) => fail(s"unexpected: ${err.reason}")
    }
  }

  test("parseArgs: --ingress-url + --request-timeout co-exist") {
    Main.parseArgs(List(
      "--ingress-url", "http://127.0.0.1:8081",
      "--request-timeout", "5"
    )) match {
      case Right(cli) =>
        cli.ingressUrl shouldBe "http://127.0.0.1:8081"
        cli.requestTimeout shouldBe Duration.ofSeconds(5)
      case Left(err) => fail(s"unexpected: ${err.reason}")
    }
  }

  test("parseArgs: --request-timeout non-integer is a typed error") {
    Main.parseArgs(List("--request-timeout", "abc")) shouldBe
      Left(Main.CliError.BadInt("--request-timeout", "abc"))
  }

  test("parseArgs: --request-timeout without value is a typed error") {
    Main.parseArgs(List("--request-timeout")) shouldBe
      Left(Main.CliError.MissingValue("--request-timeout"))
  }

  test("parseArgs: --ingress-url without value is a typed error") {
    Main.parseArgs(List("--ingress-url")) shouldBe
      Left(Main.CliError.MissingValue("--ingress-url"))
  }

  test("parseArgs: unknown flag is a typed error") {
    Main.parseArgs(List("--bogus")) shouldBe
      Left(Main.CliError.UnknownFlag("--bogus"))
  }
}
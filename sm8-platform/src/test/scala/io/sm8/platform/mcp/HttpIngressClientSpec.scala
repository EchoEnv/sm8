/*
 * SM8 Platform — HttpIngressClientSpec.
 *
 * Per C5-r2-arch-002: the C5-arch-L3 fix (baseUrl path-stripping)
 * was implemented without a regression test; a future refactor
 * could silently re-introduce the path-concatenation bug. This
 * spec pins the baseUrl contract:
 * - path components are stripped (http://host:8080/api -> http://host:8080)
 * - trailing slashes are handled (http://host:8080/ -> http://host:8080)
 * - host-only URLs keep no port (http://host -> http://host)
 * - userinfo is dropped (http://user:pass@host -> http://host)
 *
 * baseUrl is `private[mcp]` precisely so this test can assert it
 * without booting a Restate ingress.
 */
package io.sm8.platform.mcp

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Duration

class HttpIngressClientSpec extends AnyFunSuite with Matchers {

  private def client(url: String): HttpIngressClient.Impl =
    new HttpIngressClient.Impl(url, Duration.ofSeconds(1))

  test("baseUrl strips the path component (C5-arch-L3)") {
    client("http://host:8080/api").baseUrl shouldBe "http://host:8080"
  }

  test("baseUrl strips a multi-segment path") {
    client("http://host:8080/a/b/c").baseUrl shouldBe "http://host:8080"
  }

  test("baseUrl handles a trailing slash (no path at all)") {
    client("http://host:8080/").baseUrl shouldBe "http://host:8080"
  }

  test("baseUrl omits the port segment when the URL has no explicit port") {
    client("http://host").baseUrl shouldBe "http://host"
  }

  test("baseUrl drops userinfo (C5-r2-de-L3 documented behavior)") {
    client("http://user:pass@host:8080").baseUrl shouldBe "http://host:8080"
  }

  test("baseUrl keeps https scheme + explicit port, strips path") {
    client("https://ingress.example.com:9090/rest").baseUrl shouldBe "https://ingress.example.com:9090"
  }
}

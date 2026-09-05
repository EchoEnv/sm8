/*
 * SM8 Platform — ToolRegistrySpec (ADR-0021).
 *
 * Verifies the ToolRegistry helper: register, unregister, entries
 * iteration order, apply (build all registered tools for a given
 * client + mapper).
 *
 * Per ADR-0021 §Sm8ToolHandlers → ToolRegistry helper: the helper
 * extracts the construction pattern so adding tool #N is 1 line
 * of `ToolRegistry.register(...)` + a ~20-30 LOC build body, not a
 * 50-LOC boilerplate clone.
 */
package io.sm8.platform.mcp

import com.fasterxml.jackson.databind.ObjectMapper
import io.modelcontextprotocol.server.McpServerFeatures
import io.modelcontextprotocol.server.McpSyncServerExchange
import io.modelcontextprotocol.spec.McpSchema

import java.time.Duration

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ToolRegistrySpec extends AnyFunSuite with Matchers {

  /** A trivial stub `HttpIngressClient.Impl` that returns a known
    * canned response — the ToolRegistry test only cares that the
    * per-build closure is called once per registered tool. */
  private final class StubClient extends HttpIngressClient.Impl(
      ingressUrl = "http://stub",
      requestTimeout = Duration.ofSeconds(1)
  ) {
    /** Stub POST: returns the request path as the body.
      *
      * @param path the request path (echoed back in body)
      * @param body the request body (ignored by the stub)
      * @return     a 200 IngressResult whose body echoes the path
      */
    override def post(path: String, body: String): HttpIngressClient.IngressResult =
      HttpIngressClient.IngressResult(statusCode = 200, body = s"""{"path":"$path"}""")
  }

  /** Build a trivial `SyncToolSpecification` whose body is just a name echo. */
  private def echoTool(name: String): ToolRegistryEntry =
    ToolRegistryEntry(name, (client, _) => {
      val tool = McpSchema.Tool.builder()
        .name(name)
        .title(s"echo-$name")
        .description("")
        .inputSchema(McpSchema.JsonSchema.builder().`type`("object").build())
        .build()
      McpServerFeatures.SyncToolSpecification.builder()
        .tool(tool)
        .callHandler(
          new java.util.function.BiFunction[
            McpSyncServerExchange, McpSchema.CallToolRequest, McpSchema.CallToolResult
          ] {
            def apply(exch: McpSyncServerExchange, req: McpSchema.CallToolRequest): McpSchema.CallToolResult =
              McpSchema.CallToolResult.builder().addTextContent(s"echo:$name").build()
          }
        )
        .build()
    })

  /** Capture/restore the current registry state around each test
    * so tests don't leak into each other (the registry is a process-
    * scoped singleton). */
  private def withRegistry[A](body: => A): A = {
    val saved = ToolRegistry.entries.toList
    saved.foreach(e => ToolRegistry.unregister(e.name))
    try body
    finally saved.foreach(e => ToolRegistry.register(e))
  }

  test("ToolRegistry.register + entries: entries returns registered tools in insertion order") {
    withRegistry {
      ToolRegistry.register(echoTool("a"))
      ToolRegistry.register(echoTool("b"))
      ToolRegistry.register(echoTool("c"))
      ToolRegistry.entries.map(_.name) shouldBe Seq("a", "b", "c")
    }
  }

  test("ToolRegistry.register: overwrite on duplicate name") {
    withRegistry {
      ToolRegistry.register(echoTool("a"))
      ToolRegistry.register(echoTool("a"))
      ToolRegistry.entries.count(_.name == "a") shouldBe 1
    }
  }

  test("ToolRegistry.unregister: returns true on hit, false on miss") {
    withRegistry {
      ToolRegistry.unregister("missing") shouldBe false
      ToolRegistry.register(echoTool("hit"))
      ToolRegistry.unregister("hit") shouldBe true
      ToolRegistry.unregister("hit") shouldBe false
    }
  }

  test("ToolRegistry.apply: builds all registered tools for a given client + mapper") {
    withRegistry {
      ToolRegistry.register(echoTool("x"))
      ToolRegistry.register(echoTool("y"))
      val out = ToolRegistry(new StubClient, new ObjectMapper())
      out.size shouldBe 2
      out.map(_.tool().name()) shouldBe Seq("x", "y")
    }
  }

  test("ToolRegistry.apply: returns empty for empty registry") {
    withRegistry {
      // (the registry was just cleared in withRegistry; this asserts
      //  the empty-list behavior explicitly)
      ToolRegistry.entries shouldBe empty
      ToolRegistry(new StubClient, new ObjectMapper()) shouldBe empty
    }
  }
}

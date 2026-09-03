/*
 * SM8 Platform — TrackingInputStreamSpec.
 *
 * Per the stdio design verification criteria for the partial-frame
 * EOF handler introduced in PR-271 (C6 ticket #274): unit tests for
 * the `TrackingInputStream` wrapper around `System.in`. The wrapper
 * is package-private to `mcp` (same package as `McpStdioRoute`), so
 * the test class lives here too.
 *
 * 8 tests:
 * 1. EOF on newline -> partialFramePending = false (clean boundary)
 * 2. EOF mid-frame (last byte non-newline) -> partialFramePending = true
 * 3. EOF after `}` but before `\n` -> partialFramePending = true
 * 4. Empty stream (no bytes) -> partialFramePending = false
 * 5. Two complete frames, EOF on newline -> partialFramePending = false
 * 6. Two complete frames, EOF mid-frame -> partialFramePending = true
 * 7. Bulk read(byte[]) mid-frame -> partialFramePending = true
 * 8. Bulk read(byte[]) on newline-terminated frame -> partialFramePending = false
 *
 * The tests cover both `read()` overloads the SDK actually invokes
 * (single-byte `read()` and bulk `read(byte[], int, int)`) against
 * in-memory `ByteArrayInputStream`s.
 */
package io.sm8.platform.mcp

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TrackingInputStreamSpec extends AnyFunSuite with Matchers {

  private def newTracking(bytes: Array[Byte]): TrackingInputStream =
    new TrackingInputStream(new java.io.ByteArrayInputStream(bytes))

  test("EOF after a newline leaves partialFramePending = false (clean frame boundary)") {
    val t = newTracking("{\"jsonrpc\":\"2.0\"}\n".getBytes("UTF-8"))
    while (t.read() != -1) ()
    assert(t.partialFramePending == false)
  }

  test("EOF mid-frame (last byte is non-newline) leaves partialFramePending = true") {
    val t = newTracking("{\"jsonrpc\":\"2.0\",\"meth".getBytes("UTF-8"))
    while (t.read() != -1) ()
    assert(t.partialFramePending == true)
  }

  test("EOF after '}' but before newline leaves partialFramePending = true") {
    val t = newTracking("{\"jsonrpc\":\"2.0\"}".getBytes("UTF-8"))
    while (t.read() != -1) ()
    assert(t.partialFramePending == true)
  }

  test("Empty stream (no bytes read) leaves partialFramePending = false") {
    val t = newTracking(Array.empty[Byte])
    while (t.read() != -1) ()
    assert(t.partialFramePending == false)
  }

  test("Two complete frames then EOF on newline leaves partialFramePending = false") {
    val t = newTracking("{\"id\":1}\n{\"id\":2}\n".getBytes("UTF-8"))
    while (t.read() != -1) ()
    assert(t.partialFramePending == false)
  }

  test("Two complete frames then EOF mid-frame leaves partialFramePending = true") {
    val t = newTracking("{\"id\":1}\n{\"id\":2,\"meth".getBytes("UTF-8"))
    while (t.read() != -1) ()
    assert(t.partialFramePending == true)
  }

  test("Bulk read(byte[]) path behaves the same as single-byte read (mid-frame)") {
    val payload = "{\"jsonrpc\":\"2.0\",\"meth".getBytes("UTF-8")
    val t = newTracking(payload)
    val buf = new Array[Byte](16)
    var r = 0
    while ({ r = t.read(buf, 0, buf.length); r != -1 }) ()
    assert(r == -1)
    assert(t.partialFramePending == true)
  }

  test("Bulk read on a newline-terminated frame leaves partialFramePending = false") {
    val payload = "{\"id\":1}\n".getBytes("UTF-8")
    val t = newTracking(payload)
    val buf = new Array[Byte](16)
    var r = 0
    while ({ r = t.read(buf, 0, buf.length); r != -1 }) ()
    assert(t.partialFramePending == false)
  }
}

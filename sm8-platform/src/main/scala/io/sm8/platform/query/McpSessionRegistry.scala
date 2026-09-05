/*
 * SM8 MCP — McpSessionRegistry (ADR-0021 Phase 2b).
 *
 * Session state for the Streamable HTTP MCP transport: a
 * `ConcurrentHashMap` keyed by session-id, plus the `notifyClients`
 * channel (server-push notification fan-out to every live session).
 *
 * Extracted from `McpHttpRoute` per ADR-0021 — session state is a
 * separate concern from Vert.x lifecycle (`McpHttpServer`) and MCP
 * protocol handling (`McpMessageRouter`). This class owns no Vert.x
 * types and no JSON-RPC parsing; it is pure session bookkeeping.
 *
 * Thread-safety: `ConcurrentHashMap` — safe for concurrent put/get/
 * remove from Vert.x's worker threads. `notifyClients` iterates via
 * `parallelStream` (each `sendNotification` blocks on its own; the
 * fan-out is async via `Mono.fromRunnable`).
 */
package io.sm8.platform.query

import io.modelcontextprotocol.spec.McpStreamableServerSession

import java.util.concurrent.ConcurrentHashMap

import org.slf4j.LoggerFactory

import reactor.core.publisher.Mono

import scala.util.control.NonFatal

/** Session registry for the Streamable HTTP MCP transport.
  *
  * @constructor built once per `McpHttpRoute` instance; shared by the
  *              route's `McpMessageRouter` (which reads `get` / writes
  *              `put` / `remove`) and the route itself (which calls
  *              `notifyClients` / `closeGracefully`).
  */
final class McpSessionRegistry {

  private val Log = LoggerFactory.getLogger(getClass)

  /** Active sessions, keyed by session-id. Removed on DELETE or
    * when the listening stream closes. */
  private val sessions =
    new ConcurrentHashMap[String, McpStreamableServerSession]()

  /** Look up a live session by id.
    *
    * @param id the session-id from the `Mcp-Session-Id` header
    * @return   `Some(session)` if present; `None` otherwise
    */
  def get(id: String): Option[McpStreamableServerSession] =
    Option(sessions.get(id))

  /** Register a newly-initialized session.
    *
    * @param id      the session-id assigned by `factory.startSession`
    * @param session the live session returned by the SDK factory
    */
  def put(id: String, session: McpStreamableServerSession): Unit =
    sessions.put(id, session)

  /** Remove a session (on DELETE or on initialization failure).
    *
    * @param id the session-id to remove
    * @return   `Some(removedSession)` if present; `None` otherwise
    */
  def remove(id: String): Option[McpStreamableServerSession] =
    Option(sessions.remove(id))

  /** Server-push notification fan-out to every live session.
    *
    * Per the SDK `McpStreamableServerTransportProvider.notifyClients`
    * contract: the returned `Mono[Void]` completes when every
    * notification has been sent (or failed; per-session failures are
    * logged and skipped so one broken session doesn't prevent the
    * others from receiving the notification).
    *
    * @param method the JSON-RPC notification method name
    * @param params the notification params object
    * @return       `Mono[Void]` that completes after the fan-out
    */
  def notifyClients(method: String, params: Object): Mono[Void] =
    Mono.fromRunnable(new java.lang.Runnable {
      def run(): Unit = {
        sessions.values.parallelStream.forEach { session =>
          try session.sendNotification(method, params).block()
          catch { case NonFatal(e) =>
            Log.warn(s"notifyClients: sendNotification failed for session ${session.getId}", e)
          }
        }
      }
    })

  /** Close every live session (used on graceful shutdown).
    * Clears the map so a subsequent `get` returns `None`. */
  def clear(): Unit = sessions.clear()
}

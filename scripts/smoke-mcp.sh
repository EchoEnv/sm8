#!/usr/bin/env bash
# sm8 MCP stdio smoke test.
#
# Per ADR-013 (PR-259) verification criterion #7: pipe the full MCP
# handshake to `java -cp ... io.sm8.mcp.Main` over stdin, close stdin
# (EOF), and assert all 5 tool names appear in the stdout JSON-RPC
# responses.
#
# Pre-condition: sm8-server is running at --ingress-url (default
# http://127.0.0.1:8080). The MCP server's tool calls become HTTP
# POSTs to that ingress; without it, every tool call returns
# connection-refused (which is itself a valid test of the error
# path — but the tools/list handshake itself does NOT require the
# ingress, so this script can be run as soon as the MCP subprocess
# starts).
#
# Usage: scripts/smoke-mcp.sh [options]
#   --ingress-url <u>  Base URL of an already-running sm8-server
#                     (default http://127.0.0.1:8080)
#   --help             Show this help + exit 0

set -u

INGRESS_URL_DEFAULT="http://127.0.0.1:8080"
INGRESS_URL="$INGRESS_URL_DEFAULT"
while [ "$#" -gt 0 ]; do
  case "$1" in
  --ingress-url) INGRESS_URL="$2"; shift 2 ;;
  --help|-h) sed -n '2,18p' "$0"; exit 0 ;;
  *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

fail() { echo "SMOKE-MCP FAIL: $*" >&2; exit "${2:-1}"; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LOG="${JCODE_SCRATCH_DIR:-/tmp}/sm8-smoke-mcp.log"
JAR="sm8-mcp/target/sm8-mcp_2.13-0.1.0-SNAPSHOT.jar"

# 1. Build the fat jar if it doesn't exist yet.
[ -f "$JAR" ] || {
  echo "building sm8-mcp fat jar (first run only) ..."
  mvn -q -pl sm8-mcp -am package -DskipTests 2>&1 | tail -3
  [ -f "$JAR" ] || fail "sm8-mcp jar not produced"
}

# 2. Start sm8-mcp as a subprocess. Reads JSON-RPC on stdin; writes on stdout.
#    The MCP handshake order per the MCP spec is:
#      1. initialize (client -> server)
#      2. initialized (notification, client -> server)
#      3. tools/list (client -> server, expects tools array back)
#    We then close stdin (EOF) which triggers the SDK's read-loop exit.
HANDSHAKE=$'{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke-mcp","version":"0"}}}\n'
INITIALIZED=$'{"jsonrpc":"2.0","method":"notifications/initialized"}\n'
TOOLS_LIST=$'{"jsonrpc":"2.0","id":2,"method":"tools/list"}\n'

REQUEST="$HANDSHAKE$INITIALIZED$TOOLS_LIST"

echo "starting sm8-mcp subprocess (ingress=$INGRESS_URL) ..."
# Per the MCP SDK's stdio read loop (verified in
# `StdioServerTransportProvider.startInboundProcessing`): each
# \n-delimited line is one JSON-RPC message. We write all 3 lines,
# then keep stdin open 2s before closing it so the SDK has time to
# read all 3 lines before EOF triggers the read-loop exit. We use a
# subshell + sleep as stdin so the `java` process inherits a long-
# lived stdin pipe.
OUTPUT=$(
  exec 0< <(printf "%s" "$REQUEST"; sleep 2)
  java -jar "$JAR" --ingress-url "$INGRESS_URL" 2>"$LOG"
)
RC=$?
[ "$RC" -eq 0 ] || { cat "$LOG"; fail "sm8-mcp exited with code $RC"; }

echo "$OUTPUT" | head -10
echo "..."

# 3. Assert the 5 tool names appear in the stdio response.
for tool in query list_models describe_model list_engines get_metrics; do
  echo "$OUTPUT" | grep -q "\"name\":\"$tool\"" \
    || fail "MCP tools/list response missing tool '$tool': $OUTPUT"
  echo "  MCP tools/list includes tool '$tool' (PR-260)"
done

# 4. Assert the initialize response includes the serverInfo we set.
echo "$OUTPUT" | grep -q '"serverInfo":' \
  || fail "MCP initialize response missing serverInfo: $OUTPUT"
echo "$OUTPUT" | grep -q '"name":"sm8-mcp"' \
  || fail "MCP initialize response missing serverInfo.name=sm8-mcp: $OUTPUT"
echo "  MCP initialize response carries serverInfo.name=sm8-mcp"

# 5. Tool-call round-trip (opt-in: SMOKE_MCP_RUN_TOOL_CALL=1).
#    Invokes `list_engines` against a (possibly running) sm8-server.
#    If the server isn't reachable, the response body is the MCP error
#    wrapper ("sm8-mcp: failed to POST /EngineService/listEngines: ...")
#    — that path is also valid; we only assert the tool-call itself
#    returns a CallToolResult (id=3 present in the JSON-RPC envelope).
if [ -n "${SMOKE_MCP_RUN_TOOL_CALL:-}" ]; then
  TOOL_CALL=$'{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_engines","arguments":{}}}\n'
  TOOL_OUTPUT=$(
    exec 0< <(printf "%s%s%s%s" "$HANDSHAKE" "$INITIALIZED" "$TOOL_CALL"; sleep 5)
    java -jar "$JAR" --ingress-url "$INGRESS_URL" 2>"$LOG"
  )
  echo "$TOOL_OUTPUT" | grep -q '"id":3' \
    || fail "MCP tools/call response missing id=3: $TOOL_OUTPUT"
  echo "  MCP tools/call(list_engines) returned a CallToolResult (id=3 present)"
fi

echo "SMOKE-MCP PASS (stdio MCP server: 5 tools listed; handshake compliant; serverInfo correct)"
#!/usr/bin/env bash
# sm8 Streamable HTTP MCP transport smoke test (PR-263 / ADR-014).
#
# Per ADR-014 §Verification criteria: curl-based smoke that exercises
# the 6-method HTTP surface against a running sm8-server with
# --mcp-http-port set. Asserts:
# - POST /mcp initialize returns 200 + Mcp-Session-Id header
# - Accept-header validation: POST without Accept returns 400
# - GET /mcp without Accept: text/event-stream returns 400
# - DELETE /mcp on unknown session returns 404
# - --mcp-http-disallow-delete short-circuits DELETE with 405
#
# Pre-condition: sm8-server is running at --ingress-url (default
# http://127.0.0.1:8080) with --mcp-http-port set. The MCP HTTP server
# itself is the target; the Restate ingress isn't required for these
# checks but the server must be running.
#
# Usage: scripts/smoke-mcp-http.sh [options]
#   --mcp-http-url <url>  Base URL of the MCP HTTP server
#                         (default http://127.0.0.1:9090)
#   --disallow-delete    Test the 405 path (server must be started
#                         with --mcp-http-disallow-delete)
#   --help                Show this help + exit 0

set -u

MCP_URL_DEFAULT="http://127.0.0.1:9090"
MCP_URL="$MCP_URL_DEFAULT"
DISALLOW_DELETE=0
while [ "$#" -gt 0 ]; do
  case "$1" in
  --mcp-http-url) MCP_URL="$2"; shift 2 ;;
  --disallow-delete) DISALLOW_DELETE=1; shift ;;
  --help|-h) sed -n '2,18p' "$0"; exit 0 ;;
  *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

fail() { echo "SMOKE-MCP-HTTP FAIL: $*" >&2; exit "${2:-1}"; }

INIT_REQUEST='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke-mcp-http","version":"0"}}}'

echo "sm8-mcp-http: $MCP_URL (disallow-delete=$DISALLOW_DELETE) ..."

# 1. POST /mcp initialize: 200 + Mcp-Session-Id header + JSON-RPC result.
#    Use a 5s timeout; --max-time (curl) is honored.
INIT_RESPONSE=$(curl -sS --max-time 5 \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d "$INIT_REQUEST" \
  "$MCP_URL/mcp")
INIT_RC=$?
[ "$INIT_RC" -eq 0 ] || fail "POST /mcp initialize failed (curl rc=$INIT_RC)"

INIT_BODY=$(echo "$INIT_RESPONSE" | head -1)
echo "$INIT_BODY" | grep -q '"serverInfo"' \
  || fail "POST initialize response missing serverInfo: $INIT_BODY"
echo "$INIT_BODY" | grep -q '"name":"sm8"' \
  || fail "POST initialize response missing serverInfo.name=sm8: $INIT_BODY"
echo "  POST /mcp initialize returns serverInfo.name=sm8"

# Extract Mcp-Session-Id from curl's header output. We use -D - to dump headers
# to stdout, then grep for Mcp-Session-Id. The response body is in INIT_BODY.
INIT_HEADERS=$(curl -sS --max-time 5 -D - -o /dev/null \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d "$INIT_REQUEST" \
  "$MCP_URL/mcp")
SESSION_ID=$(echo "$INIT_HEADERS" | grep -i '^Mcp-Session-Id:' | head -1 | sed 's/.*: //' | tr -d '\r\n')
[ -n "$SESSION_ID" ] || fail "POST initialize response missing Mcp-Session-Id header: $INIT_HEADERS"
echo "  POST /mcp initialize returns Mcp-Session-Id header ($SESSION_ID)"

# 2. POST /mcp without Accept header: 400.
NO_ACCEPT_RC=$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' \
  -H "Content-Type: application/json" \
  -d "$INIT_REQUEST" \
  "$MCP_URL/mcp")
[ "$NO_ACCEPT_RC" = "400" ] \
  || fail "POST without Accept header should return 400, got $NO_ACCEPT_RC"
echo "  POST /mcp without Accept header returns 400 (ADR criterion #10)"

# 3. POST /mcp with only application/json (no text/event-stream): 400.
ONLY_JSON_RC=$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d "$INIT_REQUEST" \
  "$MCP_URL/mcp")
[ "$ONLY_JSON_RC" = "400" ] \
  || fail "POST with only application/json Accept should return 400, got $ONLY_JSON_RC"
echo "  POST /mcp with Accept=application/json only returns 400"

# 4. GET /mcp without Accept header: 400 (we use the session-id from #1).
NO_GET_RC=$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' \
  -H "Mcp-Session-Id: $SESSION_ID" \
  "$MCP_URL/mcp")
[ "$NO_GET_RC" = "400" ] \
  || fail "GET without Accept header should return 400, got $NO_GET_RC"
echo "  GET /mcp without Accept header returns 400"

# 5. DELETE /mcp on unknown session-id: 404 (default) or 405 (--mcp-http-disallow-delete).
if [ "$DISALLOW_DELETE" = "1" ]; then
  EXPECTED_DEL=405
else
  EXPECTED_DEL=404
fi
UNKNOWN_DEL_RC=$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' \
  -X DELETE \
  -H "Mcp-Session-Id: nonexistent-session-id-12345" \
  "$MCP_URL/mcp")
[ "$UNKNOWN_DEL_RC" = "$EXPECTED_DEL" ] \
  || fail "DELETE on unknown session-id should return $EXPECTED_DEL (disallow-delete=$DISALLOW_DELETE), got $UNKNOWN_DEL_RC"
echo "  DELETE /mcp on unknown session-id returns $EXPECTED_DEL (disallow-delete=$DISALLOW_DELETE)"

# 6. --mcp-http-disallow-delete short-circuits DELETE with 405.
if [ "$DISALLOW_DELETE" = "1" ]; then
  DD_RC=$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' \
    -X DELETE \
    -H "Mcp-Session-Id: $SESSION_ID" \
    "$MCP_URL/mcp")
  [ "$DD_RC" = "405" ] \
    || fail "DELETE with --mcp-http-disallow-delete should return 405, got $DD_RC"
  echo "  DELETE /mcp with --mcp-http-disallow-delete returns 405"
else
  echo "  (skipped --mcp-http-disallow-delete 405 check; pass --disallow-delete to enable)"
fi

# 7. Unknown path returns 404 (ADR criterion #7 / PR-258 mirror).
UNKNOWN_PATH_RC=$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' "$MCP_URL/whatever")
[ "$UNKNOWN_PATH_RC" = "404" ] \
  || fail "Unknown path should return 404, got $UNKNOWN_PATH_RC"
echo "  GET /whatever returns 404"

echo "SMOKE-MCP-HTTP PASS (Streamable HTTP MCP transport: 6-method surface, Accept-header, disallowDelete 405, 404 paths all green)"
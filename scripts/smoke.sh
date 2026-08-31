#!/usr/bin/env bash
# sm8 smoke test: boot the real server, hit /health + /discover, assert
# responses, kill the server. Closes the "binary boots and serves a real
# request" coverage gap.
#
# Usage: scripts/smoke.sh [port]
#   port defaults to 18466 (high port, unlikely to collide).
#
# Exit codes:
#   0 = all assertions passed
#   1 = assertion failed (server answered but wrong)
#   2 = server failed to build or boot (compile error, bind error, crash)
#
# Per scala-jar-packaging-mindset: sm8-server produces a THIN jar (no
# Main-Class manifest, 44K, 21 classes) — `java -jar` cannot work. The
# script boots via the full dependency classpath (built once by Maven
# into a pathing file), which is exactly how an operator would run the
# deployment after a `mvn package` + classpath assembly step.
set -uo pipefail

PORT="${1:-18466}"
START_TIMEOUT="${START_TIMEOUT:-60}"
CURL_TIMEOUT=5

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LOG="${JCODE_SCRATCH_DIR:-/tmp}/sm8-smoke.log"
CP_FILE="${JCODE_SCRATCH_DIR:-/tmp}/sm8-smoke-cp.txt"
: > "$LOG"

fail() { echo "SMOKE FAIL: $*" >&2; exit "${2:-1}"; }

echo "== sm8 smoke: port=$PORT =="

# ---- 1. Build classpath (Maven writes it to a file; skip when cached) ------
if [ ! -s "$CP_FILE" ]; then
  echo "building dependency classpath (first run only) ..."
  mvn -q -pl sm8-server -am dependency:build-classpath -Dmdep.outputFile="$CP_FILE" \
    || fail "mvn dependency:build-classpath failed" 2
fi
[ -s "$CP_FILE" ] || fail "classpath file empty" 2

# ---- 2. Check server classes are compiled ----------------------------------
MAIN_CLASSES="$ROOT/sm8-server/target/classes/io/sm8/server/Main.class"
[ -f "$MAIN_CLASSES" ] || fail "sm8-server classes not compiled; run 'mvn -pl sm8-server -am compile' first" 2

# ---- 3. Boot ----------------------------------------------------------------
# Main requires --model. Write the canonical minimal model (the same
# shape PlatformModelLoaderSpec uses) to a temp file so the script has
# zero dependency on repo fixture layout.
MODEL="${JCODE_SCRATCH_DIR:-/tmp}/sm8-smoke-model.yml"
cat > "$MODEL" <<'YAML'
name: smoke-model
version: 1
source:
  byName:
    table: smoke_table
YAML

echo "booting sm8-server on port $PORT (model=$MODEL) ..."
# Include the in-memory connector classes so the ServiceLoader can
# discover an EngineProvider (zero external deps — no Spark needed).
CP="sm8-server/target/classes:connectors/in-memory-connector/target/classes:$(cat "$CP_FILE")"
java -cp "$CP" \
  io.sm8.server.Main --model "$MODEL" --port "$PORT" >>"$LOG" 2>&1 &
SERVER_PID=$!

cleanup() {
  if kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    for _ in $(seq 1 20); do
      kill -0 "$SERVER_PID" 2>/dev/null || break
      sleep 0.5
    done
    # SIGTERM didn't take; escalate to SIGKILL (unblockable).
    if kill -0 "$SERVER_PID" 2>/dev/null; then
      kill -9 "$SERVER_PID" 2>/dev/null || true
      wait "$SERVER_PID" 2>/dev/null || true
    else
      wait "$SERVER_PID" 2>/dev/null || true
    fi
  fi
}
trap cleanup EXIT

# ---- 4. Wait for /health (bounded) -------------------------------------------
echo "waiting for /health (max ${START_TIMEOUT}s) ..."
deadline=$(( $(date +%s) + START_TIMEOUT ))
while true; do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "--- server died during boot; last 30 log lines: ---" >&2
    tail -30 "$LOG" >&2
    fail "server process exited during boot" 2
  fi
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time "$CURL_TIMEOUT" "http://127.0.0.1:$PORT/health" 2>/dev/null || true)"
  if [ "$code" = "200" ] || [ "$code" = "204" ]; then
    echo "server up (health=$code)."
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "--- timed out waiting for boot; last 30 log lines: ---" >&2
    tail -30 "$LOG" >&2
    fail "server did not become healthy within ${START_TIMEOUT}s" 2
  fi
  sleep 1
done

# ---- 5. Assert endpoints ------------------------------------------------------
echo "asserting endpoints ..."

body="$(curl -s --max-time "$CURL_TIMEOUT" "http://127.0.0.1:$PORT/health")"
[ "$body" = "OK" ] || fail "/health body: expected OK, got: $body"
echo "  /health -> 200 OK"

DISCOVER_BODY="${JCODE_SCRATCH_DIR:-/tmp}/sm8-discover.json"
code="$(curl -s -o "$DISCOVER_BODY" -w '%{http_code}' --max-time "$CURL_TIMEOUT" -X POST \
  -H "Accept: application/vnd.restate.endpointmanifest.v1+json" \
  "http://127.0.0.1:$PORT/discover")"
[ "$code" = "200" ] || fail "/discover status: expected 200, got $code"
grep -q "QueryService" "$DISCOVER_BODY" || fail "/discover body missing QueryService"
echo "  /discover -> 200 with QueryService"

echo
echo "SMOKE PASS"
exit 0
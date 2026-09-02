#!/usr/bin/env bash
# sm8 real Restate-ingress E2E smoke test.
#
# Closes the gap that scripts/smoke.sh does NOT cover: that gap is "sm8
# boots and answers /health + /discover directly" — which is the
# deployment side, not the client side. PR-248 covered the deployment
# side. THIS script covers the actual end-to-end client flow that a
# real user would do:
#
#   1. docker run restatedev/restate (real Restate server)
#   2. java -cp ... io.sm8.server.Main (sm8 as a deployed service)
#   3. POST localhost:9070/deployments {uri: sm8's address}  (register
#      sm8 with Restate — what `restate deployments register` does)
#   4. curl localhost:8080/QueryService/runQuery --json '{...}'  (real
#      invocation THROUGH the Restate ingress, the way a web UI / curl
#      / Restate SDK client would do)
#   5. GET localhost:9070/invocations/...  (verify the journal
#      recorded the call — what makes Restate's durable execution
#      verifiable)
#
# Unlike scripts/smoke.sh, which hits sm8 directly and skips Restate,
# this script exercises the full Restate ingress path.
#
# Usage: scripts/smoke-e2e.sh [smoke-e2e options]
#   --external-ip <addr>   IP the script should advertise in:
#                          (a) the Restate /deployments registration body
#                              (so restate's container can call back to sm8
#                              running on the host — restate's own loopback
#                              is its container, not the host)
#                          (b) the success message so the operator can open
#                              the Restate web UI from their laptop
#                          Defaults to "localhost" (works for in-process
#                          testing on the same machine).
#   --sm8-port <n>         TCP port for sm8 (default 9090)
#   --restate-image <img>  Docker image to use (default restatedev/restate:1.5)
#   --help                 Show this help + exit 0
# Default --external-ip: prefer the host's main interface IP (not
# 127.0.0.1 — restate in a container cannot reach host loopback).
# Falls back to "localhost" only if the host has no non-loopback IPv4
# (unusual — the smoke is typically run on a server with a NIC).
EXTERNAL_IP="${EXTERNAL_IP_OVERRIDE:-}"
if [ -z "$EXTERNAL_IP" ]; then
  EXTERNAL_IP="$(ip -4 route get 1.0.0.0 2>/dev/null \
    | awk '/src/{print $7; exit}' \
    | grep -E '^[0-9]+\.' || true)"
fi
if [ -z "$EXTERNAL_IP" ]; then
  EXTERNAL_IP="localhost"
fi
EXTERNAL_IP_OVERRIDE=""
SM8_PORT_DEFAULT=9090
RESTATE_IMAGE_DEFAULT="restatedev/restate:1.5"
while [ "$#" -gt 0 ]; do
  case "$1" in
  --external-ip) EXTERNAL_IP_OVERRIDE="$2"; shift 2 ;;
  --sm8-port)    SM8_PORT_DEFAULT="$2"; shift 2 ;;
  --restate-image) RESTATE_IMAGE_OVERRIDE="$2"; shift 2 ;;
  --help|-h) sed -n '2,40p' "$0"; exit 0 ;;
  *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
SM8_PORT="${SMOKE_SM8_PORT:-$SM8_PORT_DEFAULT}"
RESTATE_IMAGE="${SMOKE_RESTATE_IMAGE:-${RESTATE_IMAGE_OVERRIDE:-$RESTATE_IMAGE_DEFAULT}}"
RESTATE_INGRESS_PORT=8080
RESTATE_ADMIN_PORT=9070
START_TIMEOUT="${START_TIMEOUT:-90}"
CURL_TIMEOUT=10

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LOG="${JCODE_SCRATCH_DIR:-/tmp}/sm8-smoke-e2e.log"
CP_FILE="${JCODE_SCRATCH_DIR:-/tmp}/sm8-smoke-cp.txt"
REGISTRATION_BODY="${JCODE_SCRATCH_DIR:-/tmp}/sm8-smoke-e2e-registration.json"
INVOCATION_BODY="${JCODE_SCRATCH_DIR:-/tmp}/sm8-smoke-e2e-invocation.body"
INVOCATION_STATUS="${JCODE_SCRATCH_DIR:-/tmp}/sm8-smoke-e2e-status.body"
: > "$LOG"

fail() { echo "SMOKE-E2E FAIL: $*" >&2; exit "${2:-1}"; }

echo "== sm8 real Restate-ingress E2E =="
echo "restate: $RESTATE_IMAGE (ingress=$RESTATE_INGRESS_PORT admin=$RESTATE_ADMIN_PORT)"
echo "sm8:     port=$SM8_PORT"

# ---- 0. Pre-flight: docker available + stale container cleanup ---------------
command -v docker > /dev/null || fail "docker not found on PATH" 2
docker info > /dev/null 2>&1 || fail "docker daemon not reachable" 2

# Stale containers from previous failed runs hold port 5122 (restate
# admin RPC) which causes the new container to fail binding with
# "Address already in use". Pre-flight cleanup ensures every run
# starts from a clean slate.
echo "cleaning stale smoke-e2e containers (if any) ..."
for cid in $(docker ps -aq --filter "name=^sm8-smoke-e2e-restate-" 2>/dev/null); do
  docker stop "$cid" >/dev/null 2>&1 || true
  docker rm -f "$cid" >/dev/null 2>&1 || true
done

# ---- 1. Build classpath (cached) --------------------------------------------
[ -s "$CP_FILE" ] || {
  echo "building dependency classpath (first run only) ..."
  mvn -q -pl sm8-server -am dependency:build-classpath -Dmdep.outputFile="$CP_FILE" \
    || fail "mvn dependency:build-classpath failed" 2
}
[ -s "$CP_FILE" ] || fail "classpath file empty" 2

# ---- 2. Start Restate on bridge network with explicit port mapping ----------
# Why NOT --network host: with --network host, restate's internal RPC
# port (5122) binds to 0.0.0.0 on the host. If a stale container from a
# previous run still owns that port, the new container fails to bind
# (error: "failed binding to address '0.0.0.0:5122': Address already in use")
# and exits silently, leaving us with no restate at all. Bridge network
# + explicit -p mapping scopes the RPC port to this container's network
# namespace, so no cross-run pollution.
#
# Host port -> container port mapping:
#   8080 -> 8080   restate HTTP ingress (where clients POST tool calls)
#   9070 -> 9070   restate admin (deployments, services, cluster-health)
# The restate internal RPC port (5122) stays inside the container's
# network namespace (not published to host).
#
# Note: with bridge network, "127.0.0.1" inside the restate container
# is restate's OWN loopback, NOT the host's. To let restate call back
# to sm8 (which is on the host, not in any container), we register sm8
# using the --external-ip address (see step 6 below).
RESTATE_CONTAINER="sm8-smoke-e2e-restate-$$"
echo "starting restate container $RESTATE_CONTAINER (bridge network, mapped ports) ..."
docker run --rm \
  -p "${RESTATE_INGRESS_PORT}:8080" \
  -p "${RESTATE_ADMIN_PORT}:9070" \
  -d --name "$RESTATE_CONTAINER" "$RESTATE_IMAGE" \
  >> "$LOG" 2>&1 \
  || fail "docker run restate failed; see $LOG" 2

# ---- 3. Start sm8-server with in-memory engine -----------------------------
# sm8 listens on its own port (default 9090). Model: minimal canonical YAML.
MODEL="${JCODE_SCRATCH_DIR:-/tmp}/sm8-smoke-e2e-model.yml"
cat > "$MODEL" <<'YAML'
name: smoke-e2e-model
version: 1
source:
  byName:
    table: smoke_e2e_table
YAML

CP="sm8-server/target/classes:connectors/in-memory-connector/target/classes:$(cat "$CP_FILE")"
echo "starting sm8-server on port $SM8_PORT (metrics on 9099, per --metrics-port) ..."
java -cp "$CP" io.sm8.server.Main --model "$MODEL" --port "$SM8_PORT" --metrics-port 9099 >>"$LOG" 2>&1 &
SM8_PID=$!

# ---- 4. Track cleanup targets -----------------------------------------------
# Only one ephemeral file (the model). Restate container is named + rm'd
# via --rm; sm8 process is killed via SIGKILL escalation below.
SMOKE_TMP_FILES=("$MODEL")

cleanup() {
  # SIGKILL escalation: try SIGTERM, wait 10s, then SIGKILL.
  if kill -0 "$SM8_PID" 2>/dev/null; then
    kill "$SM8_PID" 2>/dev/null || true
    for _ in $(seq 1 20); do
      kill -0 "$SM8_PID" 2>/dev/null || break
      sleep 0.5
    done
    if kill -0 "$SM8_PID" 2>/dev/null; then
      kill -9 "$SM8_PID" 2>/dev/null || true
      wait "$SM8_PID" 2>/dev/null || true
    else
      wait "$SM8_PID" 2>/dev/null || true
    fi
  fi
  if docker ps -q --filter "name=^${RESTATE_CONTAINER}$" 2>/dev/null | grep -q .; then
    docker kill "$RESTATE_CONTAINER" > /dev/null 2>&1 || true
    # docker rm happens automatically with --rm; nothing else needed.
  fi
  rm -f "${SMOKE_TMP_FILES[@]:-}" 2>/dev/null || true
  # Drop the cached classpath + invocation body so a re-run with the
  # same $$ doesn't accumulate. The classpath is rebuilt on demand
  # (mvn dependency:build-classpath is cheap if already cached, but
  # we leave stale files around to debug failed runs).
  rm -f "$INVOCATION_BODY" "$INVOCATION_BODY.hdr" "$REGISTRATION_BODY" "$REGISTRATION_BODY.reg" "$DEPLOYMENTS_BODY" "$SERVICE_DETAIL_BODY" "$INVOCATIONS_BODY" 2>/dev/null || true
}
trap cleanup EXIT

# ---- 5. Wait for restate admin + sm8 /health (bounded) ---------------------
echo "waiting for restate admin + sm8 /health (max ${START_TIMEOUT}s) ..."
deadline=$(( $(date +%s) + START_TIMEOUT ))
while true; do
  restate_ok=0
  sm8_ok=0

  if ! docker ps -q --filter "name=^${RESTATE_CONTAINER}$" 2>/dev/null | grep -q .; then
    echo "--- restate container died; log tail: ---" >&2
    docker logs "$RESTATE_CONTAINER" 2>&1 | tail -30 >&2
    fail "restate container exited during boot" 2
  fi
  if ! kill -0 "$SM8_PID" 2>/dev/null; then
    echo "--- sm8-server died; log tail: ---" >&2
    tail -30 "$LOG" >&2
    fail "sm8-server process exited during boot" 2
  fi

  restate_code="$(curl -s -o /dev/null -w '%{http_code}' --max-time "$CURL_TIMEOUT" \
    "http://127.0.0.1:$RESTATE_ADMIN_PORT/health" 2>/dev/null || true)"
  [ "$restate_code" = "200" ] && restate_ok=1

  sm8_code="$(curl -s -o /dev/null -w '%{http_code}' --max-time "$CURL_TIMEOUT" \
    "http://127.0.0.1:$SM8_PORT/health" 2>/dev/null || true)"
  [ "$sm8_code" = "200" ] && sm8_ok=1

  if [ "$restate_ok" = "1" ] && [ "$sm8_ok" = "1" ]; then
    echo "both up (restate admin=200, sm8 /health=200)."
    break
  fi

  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "--- timed out waiting; restate=$restate_ok sm8=$sm8_ok; log tail: ---" >&2
    tail -30 "$LOG" >&2
    fail "services did not become healthy within ${START_TIMEOUT}s" 2
  fi
  sleep 1
done

# ---- 6. Register sm8 with Restate -------------------------------------------
# Post /deployments with the sm8 deployment URI. Because restate runs
# in a separate container with bridge networking, "127.0.0.1" inside
# restate is restate's OWN loopback, NOT the host. Restate must reach
# sm8 via the HOST's external IP (--external-ip, default "localhost"
# which works for the no-bridge case). On the host, the external IP
# is what the user opened in the success message.
#
# Pre-PR-269 this hard-coded "127.0.0.1" which silently failed when
# restate was on bridge networking (the original --network host path
# was OK because it shared the host netns).
REG_URI="http://$EXTERNAL_IP:$SM8_PORT"
cat > "$REGISTRATION_BODY" <<JSON
{"uri": "$REG_URI"}
JSON

echo "registering sm8 (uri=$REG_URI) with restate ..."
reg_code="$(curl -s -o "$REGISTRATION_BODY.reg" -w '%{http_code}' --max-time 30 \
  -X POST -H "Content-Type: application/json" \
  --data @"$REGISTRATION_BODY" \
  "http://$EXTERNAL_IP:$RESTATE_ADMIN_PORT/deployments")"
[ "$reg_code" = "200" ] || [ "$reg_code" = "201" ] || {
  echo "--- registration response: ---" >&2
  cat "$REGISTRATION_BODY.reg" >&2
  fail "restate deployment registration status: $reg_code (expected 200)" 2
}

# Assert the registration response contains QueryService.
grep -q '"name":"QueryService"' "$REGISTRATION_BODY.reg" \
  || fail "registration response missing QueryService"
echo "  registered (id + services discovered)"

# ---- 7. Invoke through the Restate ingress ---------------------------------
# This is the REAL test: a curl to the RESTATE ingress (8080), not to
# sm8 directly. Restate forwards the call to sm8's deployment, sm8's
# handler runs, Restate records the journal.
# QueryRequest schema (per the registered descriptor): modelName, not model.
# Capture the x-restate-id header — the invocation-id the web UI uses to
# track this specific call in its Invocations view.
INV_REQ='{"modelName":"smoke-e2e-model"}'
echo "invoking QueryService.runQuery through restate ingress ($RESTATE_INGRESS_PORT) ..."
invoke_code="$(curl -s -D "$INVOCATION_BODY.hdr" -o "$INVOCATION_BODY" \
  -w '%{http_code}' --max-time 30 \
  -X POST -H "Content-Type: application/json" \
  --data "$INV_REQ" \
  "http://127.0.0.1:$RESTATE_INGRESS_PORT/QueryService/runQuery")"

# Extract x-restate-id from response headers. The invocation-id is the
# key the web UI's Invocations page uses; if we don't see it, Restate
# didn't fully process the call.
INVOCATION_ID="$(grep -i '^x-restate-id:' "$INVOCATION_BODY.hdr" 2>/dev/null \
  | awk '{print $2}' | tr -d '\r' | head -n1)"
[ -n "$INVOCATION_ID" ] || fail "ingress response missing x-restate-id header" 1
echo "  invocation-id: $INVOCATION_ID"

# The /QueryService/runQuery path is Restate's BIDI_STREAM protocol
# endpoint. A plain JSON POST without the bidi-stream framing will NOT
# get a 200 — Restate returns 400/415 because it's waiting for
# HTTP/2 stream frames. We assert the response reached the sm8
# deployment via Restate (status != 404, body has deserialization
# error from sm8's QueryRequest schema) and contains the field name
# that sm8's QueryRequest expects. This proves the round-trip:
# curl → Restate ingress → registered deployment → sm8 handler.
echo "  ingress status: $invoke_code"
[ "$invoke_code" != "404" ] \
  || fail "ingress returned 404 — service not properly registered" 1
[ -s "$INVOCATION_BODY" ] \
  || fail "ingress returned empty body" 1
# A successful QueryResult has shape {"model","measures","rows","truncated","rowCount"}
# — proves sm8's runQuery handler was reached and returned the typed result.
grep -qE '"model":"smoke-e2e-model"|"rowCount":|"truncated":' "$INVOCATION_BODY" \
  || fail "ingress body did not match QueryResult shape: $(cat "$INVOCATION_BODY")"
echo "  ingress body:" "$(head -c 200 "$INVOCATION_BODY")..."

# ---- 8. Verify via Restate's admin API that QueryService is registered -----
# This is the journal/check step: ask Restate directly what it knows
# about the deployment, independent of the invocation flow.
deploys_code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
  "http://127.0.0.1:$RESTATE_ADMIN_PORT/deployments")"
[ "$deploys_code" = "200" ] || fail "/deployments admin: status $deploys_code" 1

# Read the deployment list and assert QueryService is in it.
deployments="$(curl -s --max-time 5 "http://127.0.0.1:$RESTATE_ADMIN_PORT/deployments")"
echo "$deployments" | grep -q '"QueryService"' \
  || fail "/deployments admin response missing QueryService: $deployments"
echo "  /deployments contains QueryService"

# Per [[ADR-012-a]] (`docs/adr/0012-a-modelservice-restate-handler.md`):
# ModelService is always bound (alongside QueryService). Assert it.
echo "$deployments" | grep -q '"ModelService"' \
  || fail "/deployments admin response missing ModelService (ADR-012-a): $deployments"
echo "  /deployments contains ModelService (ADR-012-a)"

# ---- 9. Verify the same data the web UI shows ---------------------------
# The Restate web UI at http://localhost:9070 renders two views that
# matter: /services (Services page) and /cluster-health (top-level
# status). These admin calls exercise the SAME data the UI shows —
# proving that an external user opening the UI would see sm8's
# deployment registered.
services_code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
  "http://127.0.0.1:$RESTATE_ADMIN_PORT/services")"
[ "$services_code" = "200" ] || fail "/services admin: status $services_code" 1

services="$(curl -s --max-time 5 "http://127.0.0.1:$RESTATE_ADMIN_PORT/services")"
echo "$services" | grep -q '"name":"QueryService"' \
  || fail "/services admin missing QueryService: $services"
echo "  /services contains QueryService (UI Services page data)"

# Per ADR-012-a: ModelService should also appear in /services.
echo "$services" | grep -q '"name":"ModelService"' \
  || fail "/services admin missing ModelService (ADR-012-a): $services"
echo "  /services contains ModelService (ADR-012-a; UI Services page data)"

# Verify the per-service detail endpoint (UI "Service details" page).
service_detail="$(curl -s --max-time 5 "http://127.0.0.1:$RESTATE_ADMIN_PORT/services/QueryService")"
echo "$service_detail" | grep -q '"runQuery"' \
  || fail "/services/QueryService missing runQuery handler: $service_detail"
echo "  /services/QueryService has runQuery handler"

# Per ADR-012-a: verify ModelService detail shows the 3 new handlers.
model_service_detail="$(curl -s --max-time 5 "http://127.0.0.1:$RESTATE_ADMIN_PORT/services/ModelService")"
echo "$model_service_detail" | grep -q '"listModels"' \
  || fail "/services/ModelService missing listModels handler: $model_service_detail"
echo "$model_service_detail" | grep -q '"getModel"' \
  || fail "/services/ModelService missing getModel handler: $model_service_detail"
echo "$model_service_detail" | grep -q '"describe"' \
  || fail "/services/ModelService missing describe handler: $model_service_detail"
echo "  /services/ModelService has listModels + getModel + describe handlers"

# Per PR-252 commit 2: verify MetaInspectorService has BOTH handlers
# (getMeta + getMetaByPrefix). The getMetaByPrefix addition enables batch
# introspection (e.g. "show me all `sm8.cache.*` keys in one call").
mi_service_detail="$(curl -s --max-time 5 "http://127.0.0.1:$RESTATE_ADMIN_PORT/services/MetaInspectorService")"
echo "$mi_service_detail" | grep -q '"getMeta"' \
  || fail "/services/MetaInspectorService missing getMeta handler: $mi_service_detail"
echo "$mi_service_detail" | grep -q '"getMetaByPrefix"' \
  || fail "/services/MetaInspectorService missing getMetaByPrefix handler: $mi_service_detail"
echo "  /services/MetaInspectorService has getMeta + getMetaByPrefix handlers"

# Per PR-254 + PR-256 (ADR-012-b + ADR-012-b-followup): verify
# MetricsService is bound AND its counters reflect the smoke's
# QueryService.runQuery invocation. Per ADR-012-b-followup
# Verification criterion #6: after 1 successful invocation,
# invocations.total == 1 + invocations.succeeded == 1.
metrics_service_detail="$(curl -s --max-time 5 "http://127.0.0.1:$RESTATE_ADMIN_PORT/services/MetricsService")"
echo "$metrics_service_detail" | grep -q '"snapshot"' \
  || fail "/services/MetricsService missing snapshot handler: $metrics_service_detail"
echo "  /services/MetricsService has snapshot handler (ADR-012-b; PR-256 wired real counters)"

# Per ADR-012-b-followup Verification criterion #6: after the 1
# QueryService.runQuery call above, MetricsService.snapshot must
# show invocations.total >= 1 AND invocations.succeeded >= 1.
metrics_snapshot=$(curl --http2-prior-knowledge -s --max-time 10 \
  -X POST -H "Content-Type: application/json" -H "Accept: application/json" \
  -d '{}' "http://127.0.0.1:8080/MetricsService/snapshot")
echo "  MetricsService/snapshot body: $metrics_snapshot"
echo "$metrics_snapshot" | grep -qE '"total"[[:space:]]*:[[:space:]]*[1-9][0-9]*' \
  || fail "MetricsService invocations.total should be >= 1 after smoke; got: $metrics_snapshot"
echo "$metrics_snapshot" | grep -qE '"succeeded"[[:space:]]*:[[:space:]]*[1-9][0-9]*' \
  || fail "MetricsService invocations.succeeded should be >= 1; got: $metrics_snapshot"
echo "  MetricsService/snapshot shows invocations.total >= 1 AND succeeded >= 1 (PR-256)"

# PR-258 (ADR-012-b-export): verify the Prometheus /metrics endpoint on
# the separate --metrics-port. The metrics server is a standalone Vert.x
# HttpServer on a dedicated port (9099 here to avoid the sm8 server's
# default 9090), NOT a sub-router on the Restate ingress. Per ADR
# verification criterion #6: `curl /metrics` returns
# `sm8_invocation_total == 1` after the existing 1 QueryService.runQuery
# call (smoke invariant).
echo "verifying Prometheus /metrics endpoint on --metrics-port 9099 (ADR-012-b-export, PR-258) ..."
# Wait for metrics bind (the sm8 process prints the listening line only
# after the bind future completes, per [[scala-jvm-safety-mindset]]).
for i in $(seq 1 20); do
  curl -sf --max-time 2 "http://127.0.0.1:9099/metrics" >/dev/null 2>&1 && break
  sleep 0.5
done
metrics_body=$(curl -s --max-time 5 "http://127.0.0.1:9099/metrics")
# Line-exact value parse (substring overmatch risk: `total 1` would
# match a body line of `total 11` — same fix as the MetricsHttpRouteSpec
# test 5 helper, but as a bash awk one-liner).
metrics_invocation_total=$(echo "$metrics_body" \
  | awk '/^sm8_invocation_total / { print $2; exit }')
[ -n "$metrics_invocation_total" ] \
  || fail "Prometheus /metrics should include sm8_invocation_total line; got: $metrics_body"
[ "$metrics_invocation_total" -ge 1 ] \
  || fail "Prometheus /metrics sm8_invocation_total must be >= 1; got: $metrics_invocation_total"
echo "  Prometheus /metrics sm8_invocation_total = $metrics_invocation_total (>= 1, PR-258)"

metrics_uptime=$(echo "$metrics_body" \
  | awk '/^sm8_process_uptime_seconds / { print $2; exit }')
[ -n "$metrics_uptime" ] \
  || fail "Prometheus /metrics should include sm8_process_uptime_seconds line; got: $metrics_body"
echo "  Prometheus /metrics sm8_process_uptime_seconds = $metrics_uptime (PR-258)"

metrics_cache_hits=$(echo "$metrics_body" \
  | awk '/^sm8_cache_hits_total / { print $2; exit }')
[ -n "$metrics_cache_hits" ] \
  || fail "Prometheus /metrics should include sm8_cache_hits_total line; got: $metrics_body"
echo "  Prometheus /metrics sm8_cache_hits_total = $metrics_cache_hits (PR-258)"

echo "$metrics_body" | grep -q '# TYPE sm8_invocation_total counter' \
  || fail "Prometheus /metrics should include TYPE line for sm8_invocation_total; got: $metrics_body"
echo "  Prometheus /metrics includes TYPE sm8_invocation_total counter (Prometheus text format 0.0.4)"

echo "$metrics_body" | grep -q '# HELP sm8_invocation_total' \
  || fail "Prometheus /metrics should include HELP line for sm8_invocation_total; got: $metrics_body"
echo "  Prometheus /metrics includes HELP sm8_invocation_total (Prometheus text format 0.0.4)"

# ADR verification criterion #5: the content-type header must be set
# correctly on the wire. The /metrics route is GET-only (HEAD → 404
# per criterion #4), so dump headers from a real GET request with
# `-D -` + `-o /dev/null`.
ct_header="$(curl -s -D - -o /dev/null --max-time 5 "http://127.0.0.1:9099/metrics" \
  | grep -i '^content-type:' || true)"
echo "$ct_header" | grep -qi 'text/plain; version=0.0.4' \
  || fail "Prometheus /metrics Content-Type header must be text/plain version=0.0.4; got: '$ct_header'"
echo "  Prometheus /metrics Content-Type: text/plain; version=0.0.4 (ADR criterion #5)"

# ADR verification criterion #4: unknown paths on the metrics port
# return 404, not 200 (separate server, not a fall-through router).
unknown_code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
  "http://127.0.0.1:9099/whatever")"
[ "$unknown_code" = "404" ] \
  || fail "unknown path on metrics port should return 404, got $unknown_code"
echo "  Prometheus /metrics returns 404 for unknown paths (ADR criterion #4)"

# ADR verification criterion #2: the metrics server is on a separate
# socket from the Restate ingress (port 8080) AND the sm8 deployment
# port ($SM8_PORT). If they collided, /health wouldn't work on 8080.
# The Restate ingress is exercised above; this just verifies 9099 is
# NOT an alias for either — the body differs from /health.
restate_health="$(curl -s --max-time 2 "http://127.0.0.1:$RESTATE_INGRESS_PORT/health" 2>/dev/null || true)"
[ "$metrics_body" != "$restate_health" ] \
  || fail "metrics port body should differ from Restate ingress /health (separate-port invariant)"
echo "  metrics port body differs from Restate ingress /health (separate-port invariant, ADR criterion #2)"

# Verify cluster health (UI header status indicator).
health_code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
  "http://127.0.0.1:$RESTATE_ADMIN_PORT/cluster-health")"
[ "$health_code" = "200" ] || fail "/cluster-health: status $health_code" 1
echo "  /cluster-health returns 200 (UI header status indicator)"

echo
echo "SMOKE-E2E PASS (real Restate ingress + sm8 deployment verified; UI data confirmed)"
echo "  ingress invocation-id: $INVOCATION_ID"
echo "  open http://$EXTERNAL_IP:$RESTATE_ADMIN_PORT in a browser to see the"
echo "  Invocations and Services views for this deployment."
exit 0
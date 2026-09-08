#!/usr/bin/env bash
# Rollup routing observation harness (design: docs/measurement/).
#
# Boots a local[1] Spark session via the observation harness class,
# materializes a declared rollup, drives a MIX of rollup-eligible and
# rollup-ineligible queries through the REAL SparkEngineProvider.query()
# path, and prints the counter snapshot in the `sm8 rollup-report`
# shape so the reading maps directly to the handbook's interpretation
# rubric (docs/measurement/rollup-routing-observation.md).
#
# Usage:
#   scripts/rollup-observe.sh          # runs the harness, prints the report
#   scripts/rollup-observe.sh --json   # reserved for future JSON output
#
# Exit codes:
#   0 = harness verdict PASS (routing counters match the query mix)
#   1 = harness verdict FAIL (routing counters diverge — inspect)
#   2 = build/compile failure
#
# Per scala-jar-packaging-mindset: spark-connector produces a THIN jar
# (no Main-Class manifest) — java -jar cannot work. The script boots
# via the full dependency classpath (built once by Maven into a
# pathing file), which is exactly how the other smoke scripts boot.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fail() { echo "ROLLOUP-OBSERVE FAIL: $*" >&2; exit "${2:-1}"; }

echo "== rollup observation harness =="

# ---- 1. Build classpath (Maven writes it to a file; skip when cached) ------
CP_FILE="${JCODE_SCRATCH_DIR:-/tmp}/sm8-observe-cp.txt"
if [ ! -s "$CP_FILE" ]; then
  echo "building dependency classpath (first run only, includes Spark) ..."
  mvn -q -pl connectors/spark-connector -am dependency:build-classpath -Dmdep.outputFile="$CP_FILE" \
    || fail "mvn dependency:build-classpath failed" 2
fi
[ -s "$CP_FILE" ] || fail "classpath file empty" 2

# ---- 2. Check classes are compiled -----------------------------------------
HARNESS_CLASS="$ROOT/connectors/spark-connector/target/classes/io/sm8/connectors/spark/RollupObservationHarness.class"
[ -f "$HARNESS_CLASS" ] || fail "harness class not compiled; run 'mvn -pl connectors/spark-connector -am compile' first" 2

# ---- 3. Run ------------------------------------------------------------------
# The harness owns the Spark session lifecycle (local[1], stop() in
# finally). No external services needed: the base table is an in-process
# temp view, the rollup is a materialized temp view via the production
# write path.
CP="connectors/spark-connector/target/classes:sm8-core/target/classes:$(cat "$CP_FILE")"
set +e
java -Xmx2g \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  -cp "$CP" \
  io.sm8.connectors.spark.RollupObservationHarness
HARNESS_EXIT=$?
set -e

echo
if [ "$HARNESS_EXIT" -eq 0 ]; then
  echo "== rollup observation: PASS (routing counters match the query mix) =="
else
  echo "== rollup observation: FAIL (see the harness output above) ==" >&2
fi
exit "$HARNESS_EXIT"

#!/usr/bin/env bash
# Gate B representative-workload stager.
#
# Synthesizes the rep_events base table into the Gate B bootstrap
# warehouse (embedded HadoopCatalog) so GateBTraceRunner live-model
# mode has a real workload to measure against. Sized for 7.5 GiB: 7
# days × 150k rows × 4 regions (~1.05M rows).
#
# Run once after fresh warehouse creation; safe to re-run (overwrite).
#
# Resource guard (per the user's standing rule): if free RAM < 25%
# available (~1.9 GiB on 7.5 GiB), abort to leave headroom.
set -euo pipefail

usage() {
  echo "usage: $0 [--warehouse DIR]" >&2
  echo "  default --warehouse: /var/lib/sm8/gate-b/warehouse" >&2
  exit 2
}

WAREHOUSE=/var/lib/sm8/gate-b/warehouse
while [ $# -gt 0 ]; do
  case "$1" in
    --warehouse) WAREHOUSE="$2"; shift 2;;
    *) usage;;
  esac
done

# Resource-sentry: bail before spinning Spark if headroom is too thin.
# Scala-jvm-safety mantra 3: long-lived state is a leak waiting on
# eviction — but a RUNAWAY Spark job that consumes what little RAM
# remains is the more immediate risk on this 7.5 GiB box.
if command -v free >/dev/null 2>&1; then
  AVAIL_KB=$(free | awk '/^Mem:/ {print $7}')
  AVAIL_MB=$((AVAIL_KB / 1024))
  if [ "$AVAIL_MB" -lt 1900 ]; then
    echo "GATEB-STAGE: refusing — only ${AVAIL_MB} MiB available (< 1900 MiB threshold)" >&2
    exit 3
  fi
fi

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO"

# Reuse the observe script's classpath-cached file.
CP_FILE="${JCODE_SCRATCH_DIR:-/tmp}/sm8-observe-cp.txt"
if [ ! -s "$CP_FILE" ]; then
  echo "GATEB-STAGE: building classpath..." >&2
  mvn -q -pl connectors/spark-connector -am dependency:build-classpath \
    -Dmdep.outputFile="$CP_FILE" || { echo "GATEB-STAGE: build-classpath failed" >&2; exit 1; }
fi

ADD_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

# Connect to test scope so the object class is visible (test-classes
# directory), with the connector test-jar on the classpath. The Iceberg
# runtime jar is appended EXPLICITLY: it is runtime-scoped in the pom
# and `dependency:build-classpath` may omit it from the test-scope
# classpath, but the HadoopCatalog session config requires the
# org.apache.iceberg.spark.SparkCatalog provider at launch.
CP="connectors/spark-connector/target/classes:connectors/spark-connector/target/test-classes:$(cat "$CP_FILE")"
ICEBERG_JAR="$(find "$HOME/.m2/repository/org/apache/iceberg" -name 'iceberg-spark-runtime-3.5_2.13-*.jar' 2>/dev/null | head -1)"
if [ -z "$ICEBERG_JAR" ]; then
  echo "GATEB-STAGE: iceberg runtime jar not found in ~/.m2 — run 'mvn -pl connectors/spark-connector -am dependency:resolve' first" >&2
  exit 1
fi
CP="$CP:$ICEBERG_JAR"

echo "GATEB-STAGE: synthesizing rep_events into $WAREHOUSE ..."
java $ADD_OPENS -Xmx1024m -cp "$CP" \
  io.sm8.connectors.spark.GateBStageWorkload "$WAREHOUSE"

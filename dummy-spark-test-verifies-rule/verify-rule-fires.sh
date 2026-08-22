#!/usr/bin/env bash
# Verify the parent's Zero-Spark enforcer rule fires when a non-spark-connector
# module declares a Spark dependency.
#
# This script is run from the repo root. The dummy module's <build> config
# is intentionally omitted so the parent's enforcer rule applies.
#
# Expected: BUILD FAILURE with the "Zero-Spark invariant" message.
#
# Per scala-debug-mantra-mindset: reproduce (declare Spark dep),
# trace (enforcer logs violation), falsify (build fails), verify.

set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "Running mvn -B -ntp -f dummy-spark-test-verifies-rule/pom.xml validate ..."
echo "(This MUST fail with 'Zero-Spark invariant' violation.)"
echo ""

set +e
mvn -B -ntp -f dummy-spark-test-verifies-rule/pom.xml validate 2>&1 | tail -20
EXIT=$?
set -e

echo ""
echo "Maven exit code: $EXIT"
if [ "$EXIT" -ne 0 ]; then
    echo "PASS: the enforcer rule fired as expected (Maven exit code $EXIT)"
else
    echo "FAIL: the enforcer rule did NOT fire — investigate immediately"
    exit 1
fi

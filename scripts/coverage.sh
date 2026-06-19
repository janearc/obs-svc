#!/usr/bin/env bash
# coverage.sh — compute obs-svc-agg unit-test coverage and enforce the floor.
#
# The gate measures ./pkg/... (the business logic: aggregation, the health state
# machine, the SR decode, the consumer fold/dispatch semantics, and the HTTP
# surface). It deliberately excludes two surfaces that cannot be unit-tested
# without a live cluster and carry no logic of their own:
#   - gen/go/...            generated protobuf bindings (no hand-written logic)
#   - cmd/obs-svc-agg       process wiring + signal/backoff loop (integration)
# The Kafka client lifecycle inside pkg/consumer.Run is exercised end-to-end by
# the running obs-svc-agg container against live topics, not in CI.
#
# Emits JSON on stdout (agent-first convention) and exits non-zero below floor.
set -euo pipefail

FLOOR="${OBS_COVERAGE_FLOOR:-87.0}"
PROFILE="${OBS_COVERAGE_PROFILE:-coverage.out}"

cd "$(dirname "$0")/.."

go test ./pkg/... -coverprofile="$PROFILE" >/dev/null

# total: ... 87.4% of statements -> 87.4
TOTAL="$(go tool cover -func="$PROFILE" | awk '/^total:/ {gsub(/%/,"",$NF); print $NF}')"

PASS="$(awk -v t="$TOTAL" -v f="$FLOOR" 'BEGIN { print (t+0 >= f+0) ? "true" : "false" }')"

printf '{"scope":"./pkg/...","coverage_pct":%s,"floor_pct":%s,"pass":%s}\n' \
  "$TOTAL" "$FLOOR" "$PASS"

if [ "$PASS" != "true" ]; then
  echo "coverage ${TOTAL}% is below the ${FLOOR}% floor" >&2
  exit 1
fi

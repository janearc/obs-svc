#!/usr/bin/env bash
# run-obs-apple.sh -- launch the obs-svc-apple floating widget against the live
# obs-svc-agg aggregator.
#
# obs-svc-agg is a ClusterIP Service on :8090 in the k3s `fleet` namespace (reached
# in-cluster via traefik; it publishes no host port). To reach it from this host we
# stand up a local `kubectl port-forward` to the Service, point the widget at it, and
# tear the forward down on exit. We forward the Service, never a pod -- pods carry hash
# suffixes and restart.
#
# overrides:
#   OBS_AGG_URL=http://host:port/state   skip cluster discovery, use this endpoint
#   OBS_AGG_PORT=<localport>             local port for the forward (default 18090;
#                                        :8090 is held by another process on this host)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"

if [ -n "${OBS_AGG_URL:-}" ]; then
  # explicit endpoint: skip discovery, trust the caller. no forward to manage, so exec.
  echo "run-obs-apple: aggregator -> ${OBS_AGG_URL} (OBS_AGG_URL override)"
  exec env OBS_AGG_URL="${OBS_AGG_URL}" cargo run --release -- "$@"
fi

ns="fleet"
svc="obs-svc-agg"
localport="${OBS_AGG_PORT:-18090}"
url="http://127.0.0.1:${localport}/state"

command -v kubectl >/dev/null 2>&1 || {
  echo "run-obs-apple: kubectl not found on PATH; cannot reach the cluster." >&2
  echo "  or set OBS_AGG_URL=http://host:port/state to bypass discovery." >&2
  exit 1
}

# fail loudly if the cluster/service is not reachable, rather than launching the widget
# into a false "down".
if ! kubectl get -n "$ns" "svc/$svc" >/dev/null 2>&1; then
  echo "run-obs-apple: cannot reach svc/${svc} in namespace '${ns}'." >&2
  echo "  check the cluster:   kubectl -n ${ns} get svc ${svc}" >&2
  echo "  or set OBS_AGG_URL=http://host:port/state to bypass discovery." >&2
  exit 1
fi

pflog="$(mktemp -t run-obs-apple.XXXXXX)"
kubectl port-forward -n "$ns" "svc/$svc" "${localport}:8090" >"$pflog" 2>&1 &
pf_pid=$!
cleanup() {
  kill "$pf_pid" 2>/dev/null || true
  rm -f "$pflog"
}
# keep the trap (do NOT exec below) so the forward is always torn down on exit.
trap cleanup EXIT INT TERM

# wait until the forward actually answers /state before launching (up to ~10s), and bail
# early if the forwarder died (e.g. the local port is already in use).
ready=""
for _ in $(seq 1 20); do
  if curl -fsS --max-time 1 "$url" >/dev/null 2>&1; then ready=1; break; fi
  kill -0 "$pf_pid" 2>/dev/null || break
  sleep 0.5
done
if [ -z "$ready" ]; then
  echo "run-obs-apple: port-forward to svc/${svc} never answered on ${url}." >&2
  echo "  forwarder output:" >&2
  sed 's/^/    /' "$pflog" >&2 || true
  echo "  is local port ${localport} free?  retry with OBS_AGG_PORT=<other> $0" >&2
  exit 1
fi

echo "run-obs-apple: aggregator -> ${url} (kubectl port-forward svc/${svc}, pid ${pf_pid})"
# foreground (not exec) so the EXIT trap fires and the port-forward is cleaned up.
env OBS_AGG_URL="${url}" cargo run --release -- "$@"

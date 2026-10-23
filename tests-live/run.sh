#!/usr/bin/env bash
# Live Murmur multi-process scenario checks: one murmur-server child per node.
# Port agents NEVER run this (single-package `cargo test --no-run` only); it is
# the CI/operator runner. Each scenario is a standalone package and runs alone.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
live="$root/tests-live"
scenario=${1:-all}
if [[ "${MURMUR_ENGINE_INTEGRATION:-}" != "1" ]]; then
  echo "Live scenarios are opt-in. Set MURMUR_ENGINE_INTEGRATION=1 to run the real-engine scenarios." >&2
  exit 1
fi

# One shared target dir: per-package target/ dirs duplicate the full
# dependency build (~1.6 GB x 70 scenarios exhausted a 219 GB disk).
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-"$live/target"}"

runtime_root=${MURMUR_LIVE_RUNTIME:-"$live/runtime"}
started_at=$SECONDS

echo "======================================================================"
echo "  MURMUR GO SERVER / RUST LIVE SCENARIO RUNNER"
echo "======================================================================"

# Reap server processes rooted at OUR runtime dir on the way out (Ctrl-C,
# inter-scenario kills, CI cancel). Scoped by the --config path so parallel
# runs with their own MURMUR_LIVE_RUNTIME roots are never touched. No-op on
# clean runs (in-test cleanup already reaped everything).
sweep_runtime_servers() {
  local pids
  pids=$(pgrep -f "murmur-server.*$runtime_root" 2>/dev/null || true)
  if [[ -z "$pids" ]]; then
    return 0
  fi
  # shellcheck disable=SC2086
  kill -TERM $pids 2>/dev/null || true
  sleep 2
  pids=$(pgrep -f "murmur-server.*$runtime_root" 2>/dev/null || true)
  if [[ -n "$pids" ]]; then
    # shellcheck disable=SC2086
    kill -KILL $pids 2>/dev/null || true
  fi
}
trap sweep_runtime_servers EXIT INT TERM

if [[ -z "${MURMUR_SERVER_BIN:-}" ]]; then
  echo "Building murmur-server binaries..."
  mkdir -p "$root/bin"
  (cd "$root" && go build -o bin/murmur-server ./cmd/murmur-server && go build -o bin/murmur-admin ./cmd/murmur-admin)
  export MURMUR_SERVER_BIN="$root/bin/murmur-server"
fi
if [[ ! -x "$MURMUR_SERVER_BIN" ]]; then
  echo "MURMUR_SERVER_BIN is not executable: $MURMUR_SERVER_BIN" >&2
  exit 1
fi

scenario_timeout_secs() {
  # Per-scenario caps for the `timeout(1)` wrapper (mirrors Go run.sh).
  case "$1" in
    endurance-chaos) echo "273600" ;;       # 76h
    soak-slo) echo "10800" ;;               # 3h
    long-running-five-node) echo "4500" ;;  # 75m
    files-soak|compression) echo "900" ;;   # 15m
    allow-nodes) echo "480" ;;              # 8m
    crash-recovery|chaos-load|snapshot-resync) echo "600" ;;  # 10m/6m
    abuse) echo "1800" ;;                   # 30m
    resource-exhaustion) echo "1500" ;;     # 25m
    *) echo "600" ;;                        # 10m
  esac
}

run_scenario() {
  local scn=$1
  local manifest="$live/$scn/Cargo.toml"
  if [[ ! -f "$manifest" ]]; then
    echo ""
    echo ">>> MISSING LIVE SCENARIO: $scn" >&2
    return 1
  fi
  echo ""
  echo ">>> RUNNING LIVE SCENARIO: $scn"
  if [[ "$scn" == "downgrade-guard" && -z "${MURMUR_FORMAT_FIXTURE_BIN:-}" ]]; then
    echo "Building the engine-owned future-format artifact injector..."
    export MURMUR_FORMAT_FIXTURE_BIN
    MURMUR_FORMAT_FIXTURE_BIN=$(cargo test --manifest-path "$root/../MURMUR/Cargo.toml" \
      --lib --features replication --no-run --message-format=json | \
      python3 -c 'import json,sys; artifacts=[json.loads(line) for line in sys.stdin if line.startswith("{")]; binaries=[a["executable"] for a in artifacts if a.get("reason")=="compiler-artifact" and a.get("profile",{}).get("test") and a.get("executable") and a.get("target",{}).get("name")=="sqlite_session"]; assert len(binaries)==1, binaries; print(binaries[0])')
  fi
  timeout -s TERM "$(scenario_timeout_secs "$scn")s" \
    cargo test --manifest-path "$manifest"
  echo ">>> SCENARIO COMPLETED: $scn"
}

ALL_SCENARIOS="three-node-sync partition chaos-load flap-partition three-way-heal tail-repair partial-mesh swim-discovery plumtree-live scale-mesh long-running-five-node txchunk-resume corrupt-snapshot large-payload crdt-contention typed-records typed-bridge bridge-two-streams merge-policies delete-pruning views schema-evolution migration-concurrency migration-crash clock-skew crash-recovery crash-loop rejoin-storm rolling-restart graceful-shutdown pause-resume churn-retirement snapshot-resync open-progress compression gc-balance log-boundedness maintenance-under-load overload-budgets loadshare write-priority soak-slo api-mtls tls-floor cert-lifecycle allow-nodes addrpolicy dbid-isolation version-skew unlock-abuse hostile-peer hostile-schema encryption plaintext-audit rekey file-permissions backup-restore backup-under-fire tampered-backup downgrade-guard files-bridge files-soak files-crash files-corrupt-source highlow origin-signatures abuse endurance-chaos resource-exhaustion dos-client diskfull-live impaired-network sqli-api search-mesh"

# Release-gate subset (mirrors the Go gate minus the deliberately-not-ported
# release-upgrade/spool/rime suites). Missing scenario manifests fail the run.
GATE_SCENARIOS="merge-policies origin-signatures open-progress api-mtls three-node-sync typed-records typed-bridge views rekey encryption backup-restore partition version-skew schema-evolution highlow files-bridge files-soak scale-mesh loadshare pause-resume partial-mesh rolling-restart snapshot-resync gc-balance crash-recovery migration-crash chaos-load graceful-shutdown tampered-backup files-crash file-permissions plaintext-audit unlock-abuse rejoin-storm migration-concurrency sqli-api crash-loop swim-discovery txchunk-resume hostile-peer hostile-schema tail-repair three-way-heal flap-partition delete-pruning corrupt-snapshot backup-under-fire bridge-two-streams tls-floor dbid-isolation cert-lifecycle files-corrupt-source maintenance-under-load search-mesh"

case "$scenario" in
  all)
    for scn in $ALL_SCENARIOS; do
      run_scenario "$scn"
    done
    ;;
  gate)
    for scn in $GATE_SCENARIOS; do
      run_scenario "$scn"
    done
    ;;
  soak)
    MURMUR_FILES_SOAK_DURATION_SECONDS=${MURMUR_FILES_SOAK_DURATION_SECONDS:-600} \
    MURMUR_FILES_SOAK_INTERVAL_SECONDS=${MURMUR_FILES_SOAK_INTERVAL_SECONDS:-60} \
      run_scenario "files-soak"
    MURMUR_SLO_DURATION_SECONDS=${MURMUR_SLO_DURATION_SECONDS:-7200} \
    MURMUR_SLO_SETTLE_SECONDS=${MURMUR_SLO_SETTLE_SECONDS:-300} \
      run_scenario "soak-slo"
    MURMUR_FIVE_NODE_DURATION_SECONDS=${MURMUR_FIVE_NODE_DURATION_SECONDS:-3600} \
    MURMUR_FIVE_NODE_SETTLE_SECONDS=${MURMUR_FIVE_NODE_SETTLE_SECONDS:-300} \
      run_scenario "long-running-five-node"
    ;;
  stress)
    stress_count=${MURMUR_STRESS_COUNT:-5}
    for scn in rolling-restart partial-mesh crash-recovery partition snapshot-resync; do
      echo ""
      echo ">>> STRESS $scn x$stress_count"
      for ((i = 1; i <= stress_count; i++)); do
        run_scenario "$scn"
      done
    done
    ;;
  *)
    if [[ " $ALL_SCENARIOS " == *" $scenario "* ]]; then
      run_scenario "$scenario"
    else
      echo "Unknown scenario: $scenario" >&2
      echo "Available scenarios: $ALL_SCENARIOS all gate soak stress" >&2
      exit 1
    fi
    ;;
esac

echo ""
echo "======================================================================"
echo "  ALL REQUESTED LIVE SCENARIOS PASSED (Elapsed: $((SECONDS - started_at))s)"
echo "======================================================================"

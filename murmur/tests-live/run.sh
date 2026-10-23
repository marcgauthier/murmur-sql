#!/usr/bin/env bash
# Live Murmur Go multi-Database scenario runner with isolated node directories.
# Each scenario is a standalone Go test package (tests-live/<scenario>/) and
# runs alone, sequentially. Shared scaffolding lives in tests-live/harness/.
set -euo pipefail

root=$(cd "$(dirname "$0")" && pwd)
scenario=${1:-all}

runtime_root=${MURMUR_LIVE_RUNTIME:-"$root/runtime"}
started_at=$SECONDS

echo "======================================================================"
echo "  MURMUR GO LIVE SCENARIO RUNNER"
echo "======================================================================"

# Reap fixture processes rooted at OUR runtime dir on the way out (Ctrl-C,
# inter-scenario kills, CI cancel). Scoped by the --dir path so parallel runs
# with their own MURMUR_LIVE_RUNTIME roots are never touched. No-op on clean
# runs (in-test cleanup already reaped everything).
sweep_runtime_fixtures() {
  local pids
  pids=$(pgrep -f "testnode.*$runtime_root" 2>/dev/null || true)
  if [[ -z "$pids" ]]; then
    return 0
  fi
  # shellcheck disable=SC2086
  kill -TERM $pids 2>/dev/null || true
  sleep 2
  pids=$(pgrep -f "testnode.*$runtime_root" 2>/dev/null || true)
  if [[ -n "$pids" ]]; then
    # shellcheck disable=SC2086
    kill -KILL $pids 2>/dev/null || true
  fi
}
trap sweep_runtime_fixtures EXIT INT TERM

FAILED_SCENARIOS=()
PASSED_COUNT=0

preflight() {
  # Fail fast on environment problems so they never masquerade as product
  # failures (a full disk once failed log-boundedness with ENOSPC).
  local missing=()
  local tool
  for tool in go timeout pgrep df awk; do
    command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
  done
  if ((${#missing[@]} > 0)); then
    echo "PREFLIGHT FAIL: missing required tools: ${missing[*]}" >&2
    exit 1
  fi
  local min_gb=${MURMUR_LIVE_MIN_DISK_GB:-5}
  local dir
  for dir in "${GOCACHE:-"$HOME/.cache/go-build"}" "$runtime_root" "${TMPDIR:-/tmp}"; do
    mkdir -p "$dir" 2>/dev/null || true
    local avail_kb
    avail_kb=$(df -Pk "$dir" 2>/dev/null | awk 'NR==2{print $4}')
    if [[ "$avail_kb" =~ ^[0-9]+$ ]] && ((avail_kb < min_gb * 1024 * 1024)); then
      echo "PREFLIGHT FAIL: only $((avail_kb / 1024 / 1024)) GiB free for $dir (need ${min_gb} GiB; override with MURMUR_LIVE_MIN_DISK_GB)" >&2
      exit 1
    fi
  done
  local stale
  stale=$(pgrep -f "testnode.*$runtime_root" 2>/dev/null | wc -l || true)
  if ((stale > 0)); then
    echo "PREFLIGHT: sweeping $stale stale fixture process(es) rooted at $runtime_root"
    sweep_runtime_fixtures
    stale=$(pgrep -f "testnode.*$runtime_root" 2>/dev/null | wc -l || true)
    if ((stale > 0)); then
      echo "PREFLIGHT FAIL: $stale fixture process(es) still alive under $runtime_root" >&2
      exit 1
    fi
  fi
  echo "PREFLIGHT OK: tools present, disk >= ${min_gb} GiB, no stale fixtures"
}

preflight

echo "Building testnode fixture binary..."
mkdir -p "$root/bin"
(cd "$root/../.." && go build -o "$root/bin/testnode" ./murmur/tests-live/testnode/)
export MURMUR_TESTNODE_BIN="$root/bin/testnode"

scenario_timeout_secs() {
  # Per-scenario caps for the `timeout(1)` wrapper.
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
  local dir="$root/$scn"
  if [[ ! -d "$dir" ]]; then
    echo ""
    echo ">>> SKIP LIVE SCENARIO (not ported yet): $scn"
    return 0
  fi
  echo ""
  echo ">>> RUNNING LIVE SCENARIO: $scn"
  echo "LIVE_START $scn"
  local start=$SECONDS
  local cap
  cap="$(scenario_timeout_secs "$scn")"
  if timeout -s TERM "${cap}s" \
    go test -count=1 -timeout "${cap}s" "$dir"; then
    PASSED_COUNT=$((PASSED_COUNT + 1))
    echo ">>> SCENARIO COMPLETED: $scn ($((SECONDS - start))s)"
    echo "LIVE_RESULT $scn PASS $((SECONDS - start))"
  else
    local code=$?
    echo ">>> SCENARIO FAILED: $scn (exit $code, $((SECONDS - start))s)"
    echo "LIVE_RESULT $scn FAIL $((SECONDS - start))"
    if [[ "${MURMUR_LIVE_CONTINUE_ON_FAILURE:-0}" == "1" ]]; then
      FAILED_SCENARIOS+=("$scn")
      return 0
    fi
    return $code
  fi
}

ALL_SCENARIOS="three-node-sync partition chaos-load flap-partition three-way-heal tail-repair partial-mesh swim-discovery plumtree-live scale-mesh long-running-five-node txchunk-resume corrupt-snapshot large-payload crdt-contention typed-records typed-bridge bridge-two-streams merge-policies delete-pruning views schema-evolution migration-concurrency migration-crash clock-skew crash-recovery crash-loop rejoin-storm rolling-restart graceful-shutdown pause-resume churn-retirement snapshot-resync open-progress compression gc-balance log-boundedness maintenance-under-load overload-budgets loadshare write-priority soak-slo api-mtls tls-floor cert-lifecycle allow-nodes addrpolicy dbid-isolation version-skew unlock-abuse hostile-peer hostile-schema encryption plaintext-audit rekey file-permissions backup-restore backup-under-fire tampered-backup downgrade-guard files-bridge files-soak files-crash files-corrupt-source files-burst files-rekey files-fanout files-failover files-dedup files-conflict files-source-restart files-rotate-load files-prune files-dbid files-resume files-boundary highlow origin-signatures abuse endurance-chaos resource-exhaustion dos-client diskfull-live impaired-network sqli-api search-chaos linearizability config-churn client-failover slow-peer"

# Release-gate subset. Missing dirs SKIP until ported.
GATE_SCENARIOS="merge-policies origin-signatures open-progress api-mtls three-node-sync typed-records typed-bridge views rekey encryption backup-restore partition version-skew schema-evolution highlow files-bridge files-soak scale-mesh loadshare pause-resume partial-mesh rolling-restart snapshot-resync gc-balance crash-recovery migration-crash chaos-load graceful-shutdown tampered-backup files-crash file-permissions plaintext-audit unlock-abuse rejoin-storm migration-concurrency sqli-api crash-loop swim-discovery txchunk-resume hostile-peer hostile-schema tail-repair three-way-heal flap-partition delete-pruning corrupt-snapshot backup-under-fire bridge-two-streams tls-floor dbid-isolation cert-lifecycle files-corrupt-source maintenance-under-load"

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
if ((${#FAILED_SCENARIOS[@]} > 0)); then
  echo "  LIVE SCENARIOS FAILED (${#FAILED_SCENARIOS[@]}, passed $PASSED_COUNT, elapsed $((SECONDS - started_at))s):"
  printf '    - %s\n' "${FAILED_SCENARIOS[@]}"
  echo "======================================================================"
  echo "LIVE_ALL_DONE pass=$PASSED_COUNT fail=${#FAILED_SCENARIOS[@]}"
  exit 1
fi
echo "  ALL REQUESTED LIVE SCENARIOS PASSED ($PASSED_COUNT, elapsed $((SECONDS - started_at))s)"
echo "======================================================================"
echo "LIVE_ALL_DONE pass=$PASSED_COUNT fail=0"

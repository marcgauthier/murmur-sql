# MURMUR Go live multi-process tests (`tests-live/`)

Go port of the Rust live harness in `/home/marc/MURMUR/tests-live/`
(package `murmur-live-harness`): the same in-process multi-`Database`
topology, cluster API, cert provisioning, open watchdog, forensics,
artifact retention, and fixture-subprocess protocol, against the Go
product (`murmur`, `replication`, `spool` packages in this module).

## 1. Topology decision (read first)

Scenarios are **in-process multi-`Database` integration tests**, not
daemon drivers:

- A "node" is an open `murmur.Database` on its own directory
  (`runtime/<scenario>/nodeN/`).
- Stop/restart/kill is `Close()` + `Open(same dir, same key)`; true
  async `SIGKILL`/`SIGSTOP`/`SIGTERM` legs additionally need per-node
  fixture **subprocesses** (see `testnode/` below) because signals
  cannot target one in-process node without freezing the test driver.
- Ports use SQL via `Database.Transaction` / `Database.Read`, the
  `Sql` policy ops (`CounterAdd`, `SetAdd`/`SetRemove`, `Min`, `Max`),
  and `replication.Handle` (`AddPeer`, `RemovePeer`, `RetirePeer`,
  `ForceSync`).

## 2. Exact layout

```text
tests-live/
  README.md                    # this file
  run.sh                       # sequential runner (CI/operator; port agents use go vet/go test only)
  harness/                     # package harness (this module; no new go.mod)
    harness.go                 # EnvSeconds, DeadlineScale, WaitUntil
    cluster.go                 # Cluster / Node open-close-mesh management
    certs.go                   # test CA + per-node certs (SAN URI replicateddb://node/<hex>)
    unlock.go                  # open-with-key + on-open-progress watchdog (stall/overall env)
    forensics.go               # DumpNodeForensics via Status()/metrics (never panics)
    artifacts.go               # runtime/ lifecycle, failures/<ts>-<scenario>/ retention, RESULT line
    fixture.go                 # fixture-subprocess spawn/signal/wait helpers
    ports.go                   # best-effort free-port helpers (fixture subprocesses only)
  testnode/                    # fixture binary for crash/signal scenarios (CLI + exit-code protocol, NOT a daemon)
    main.go                    # package main: open/wait/crash/version
  <scenario>/                  # one dir per scenario, e.g. three-node-sync/
    <scenario>_test.go         # TestXxx fns (env-gated ones Skip)
    README.md                  # port notes: source, env knobs, pass criteria, deviations
  runtime/                     # created at run time, gitignored
  failures/                    # created at run time, gitignored
  bin/                         # testnode build output (run.sh), gitignored
```

## 3. Harness public API

Package `github.com/marcgauthier/murmur-sql/murmur/tests-live/harness`.

```go
import "github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
```

- `NewCluster(ClusterOptions{ Name, NumNodes: 3, DatabaseID?,
  NodeIDs?, Key?, Replication?, Sqlite?, SpoolByNode?, ManualPeers,
  BootstrapSeeds?, AllowedPeers?, AllowedNetworks?, Files?, Bridge,
  NodeCertTTL?, ... })`:
  wipes + rebuilds `runtime/<name>/`, provisions a test CA + per-node
  certs (`tls/` per node), opens nodes under the unlock watchdog (seed
  indices first, others get seed `bootstrap` addrs), then full-meshes
  unless `ManualPeers`. A zero `Key` selects the fixed test key.
- `cluster.Node(i) -> *murmur.Database` (panics if stopped: a test bug),
  `NodeInfo(i) -> *Node` (label/dir/data_dir/node_id/repl_addr, also
  while stopped), `NodeID(i)`, `NodeOriginPubkey(i)`, `Len()`, `Name()`,
  `RuntimeDir()`, `DatabaseID()`.
- `AddPeer(from, target)`, `AddPeerAddr(from, netip.AddrPort)` (dial an
  explicit address, e.g. through a throttle proxy), `RemovePeer(from,
  target)`, `Mesh()` (redial every ordered pair of *running* nodes; use
  after heals and restarts), `StopNode(i)`, `StartNode(i)` (reopen same
  dir+key; rebinds a fresh ephemeral port, so re-mesh afterwards),
  `StartNodesParallel(idxs)` (barrier restart for storm legs),
  `SetBridgeConfig(i, cfg?)`, `SetSpoolOptions(i, opts?)`,
  `SetNodeKey(i, key)`, `SetFilesConfig(i, key, generation, previous?)`,
  `RotateNodeCert(i, ttl?)` (reopen stopped node
  `i` with a freshly issued leaf for the same node id; rewrites `tls/`;
  re-mesh afterwards), `RestartNodeWithIdentity(i, newID)` (clone legs),
  `CaDER()` + `AddTrustRoot(der)` (cross-trust legs: exchange CA ders,
  restart all nodes, re-mesh).
- `DumpForensics(reason)` (infallible; call inside the failure branch),
  `Finish(failed)` (closes nodes, retains-or-cleans runtime, prints
  `RESULT: PASS/FAIL`).
- `WaitUntil(timeout, poll, cond) -> bool` for convergence waits.
- `certs`: `GenerateCa`, `Issue(nodeID, ttl?)`, `CaDER`, `WriteTLSDir`
  for custom-cert legs.
- `OpenWithWatchdog(path, key, opts)` for opens outside `Cluster`
  (same stall/overall env knobs; a timed-out open stays detached: never
  reuse that dir).
- `artifacts`: `RuntimeDir`, `FailuresDir`, `PrepareRuntimeDir`,
  `UTCStamp`.
- `fixture`: `BinaryPath`, `Spawn`, `WaitTimeout`, `Terminate`,
  `StopProcess`, `ContinueProcess`, `Kill`, `Signal`,
  `CrashExitCode=73`, plus the `testnode` CLI (`open --dir --key-hex
  [--mode --insert --ready --hold-secs --stop-file --dbid-hex
  --node-hex]`, `wait --secs`, `crash [--code]`, `version`).
  `run.sh` builds it and exports `MURMUR_TESTNODE_BIN`
  (`MURMUR_BIN` also honored).
- `ports`: `FreeAddr`, `FreePort` (best-effort, fixture subprocesses
  only; QUIC nodes bind `:0` directly).

Env knobs (same names as Rust): `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS` (120),
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS` (1800).
`MURMUR_TEST_DEADLINE_SCALE` (float, default 1) multiplies every harness
`WaitUntil` deadline; loaded CI sets 3 or higher.
`MURMUR_LIVE_CONTINUE_ON_FAILURE=1` runs all requested scenarios despite
failures; `MURMUR_LIVE_MIN_DISK_GB` (default 5) sets the runner preflight
disk floor.

## 4. Per-scenario file template

`tests-live/<scenario>/<scenario>_test.go`:

```go
package scenario_test

import (
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

func TestScenario(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{Name: "<scenario>", NumNodes: 3})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := false
	defer func() {
		if failed {
			cluster.DumpForensics("<short-reason>")
		}
		cluster.Finish(failed)
	}()
	// ... port body: writes, AddPeer/RemovePeer, Stop/Start, waits with deadlines ...
	// on failure: failed = true; t.Fatalf(...)
}
```

Env-gated scenarios (`clock-skew`, `diskfull-live`, `impaired-network`)
start the test body with a capability probe and `t.Skip(...)` when absent.

`tests-live/<scenario>/README.md`: source dir, what it proves, env knobs,
pass criteria, and any deviation from the original.

## 5. Compile-check command (port agents)

Run exactly this from the module root:

```sh
GOCACHE=/tmp/gocache GOTOOLCHAIN=local go vet ./tests-live/harness/ ./tests-live/testnode/
```

per scenario:

```sh
GOCACHE=/tmp/gocache GOTOOLCHAIN=local go vet ./tests-live/<scenario>/
```

- Do NOT run `run.sh` (CI/operator runner); scenario authors verify with
  `go vet` plus a targeted `go test` run only when explicitly asked.
- Never touch `go.mod`/`go.sum`, product code (`murmur/`,
  `replication/`, `spool/`, `internal/`, `cmd/`), or any other
  scenario's directory.

## 6. Rules for port agents (binding)

1. Sequential single writer: one scenario in flight at a time per checkout.
2. Never touch product code or any other scenario's directory.
3. Never fork harness code into a scenario; shared needs go to
   `tests-live/harness/`, product gaps go in the report.
4. Keep the scenario's pass criteria; document every deviation in the
   scenario `README.md`.

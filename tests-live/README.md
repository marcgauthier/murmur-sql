# Murmur Go live multi-process tests (`tests-live/`)

74 scenario suites drive live `murmur-server` children and assert over SQL
plus `MURMUR ...` admin commands. Everything here is Go: the suites were
migrated from Rust (which itself was ported from an older archive) and the
Rust tree was removed afterwards. Live runs are opt-in:
`MURMUR_ENGINE_INTEGRATION=1` enables them; without it every suite
short-circuits to `t.Skip` so plain `go test ./...` stays fast.
See [verification status](../docs/TESTING.md).

## 1. Topology decision (read first)

A "node" is a `murmur-server` child process on its own directory
(`runtime/<scenario>/nodeN/` with `config.yaml`, `tls/`, `data/`,
`logs/`, `backups/`). The product is a **PostgreSQL-wire server**:
SQL plus `MURMUR ...` admin commands over one Postgres-compatible
port, with replication as a background QUIC mesh between servers.

Therefore the suites are **multi-process integration tests**:

- `run.sh` builds `bin/murmur-server` + `bin/murmur-admin` once (or
  reuses `MURMUR_SERVER_BIN`/`MURMUR_ADMIN_BIN`). The harness spawns
  one child per node, picks free SQL ports, provisions a test CA plus
  per-node certs (SAN URI `replicateddb://node/<hex>`), writes
  per-node `config.yaml` files, meshes replication via `bootstrap` +
  runtime `MURMUR PEER ADD`, and pins `database_id`/`node_id` plus
  the unlock key per node.
- Stop/restart/kill act on the child directly (`StopNode`,
  `StartNode`, `KillNode`); unix-only `SuspendNode`/`ResumeNode`
  (SIGSTOP/SIGCONT) cover pause legs on the classic harness.
- All 74 suites speak explicit SQL through the harness SQL client
  (stdlib pgwire, no external driver): application data, DDL,
  settings, diagnostics, backups, and files, including `MURMUR ...`
  commands. The attack fixture reuses the engine's replication
  protocol types to send hostile QUIC traffic; honest database
  operations use SQL.
- Scheduler, persistence, and replication-counter assertions read
  the engine snapshot through `MURMUR SHOW STATUS`;
  `MURMUR SHOW PEERS` verifies topology.

## 2. Exact layout

`tests-live/` is part of the root Go module: every scenario (and
each harness) is a standalone test package so it compiles and
skip-checks alone.

```text
tests-live/
  README.md                    # this file
  run.sh                       # sequential runner (all/gate/soak/stress/single)
  goharness/                   # classic harness package (process + wire client)
    goharness.go               # ClusterOptions/ReplicationOptions, WaitUntil, EnvSeconds, Hex, ...
    cluster.go                 # Cluster/Node multi-process management + NodeClient
    client.go                  # stdlib pgwire SQL client (Connect/Query/Execute)
    sql.go                     # Fixture/Rows/Status/Peers/Converge/PeerCounts/AssertRows
    certs.go                   # stdlib-x509 CA + per-node certs
    attacker.go                # hostile-QUIC fixture (corrupt-snapshot, origin-signatures, ...)
    unlock.go                  # ConnectWithWatchdog (stall/overall env watchdog)
    forensics.go               # DumpNodeForensics (never fails the test)
    artifacts.go               # runtime/ lifecycle, failures/<ts>-<scenario>/ retention
    ports.go                   # free-port picking (replication binds :0)
  gosqlharness/                # SQL-harness package (returns the client directly)
    sqlharness.go              # Cluster/ClusterOptions/Node over server children
    transfers.go               # Download/Upload/AssertNoPlaintext
  <scenario>/                  # one dir per suite, e.g. three-node-sync/
    doc.go                     # package doc (keeps test-only dirs buildable)
    <scenario_snake>_test.go   # TestXxx fns (skip unless MURMUR_ENGINE_INTEGRATION=1)
    README.md                  # what it proves, env knobs, pass criteria, deviations
  runtime/                     # created at run time, gitignored
  failures/                    # created at run time, gitignored
```

## 3. Harness API

Classic (`goharness`):

- `NewCluster(opts ClusterOptions) (*Cluster, error)` wipes +
  rebuilds `runtime/<name>/`, provisions CA + certs, writes
  `config.yaml` files (free SQL ports, `:0` replication binds,
  shared database id, distinct node ids, mutual trusted origins),
  starts nodes under the unlock watchdog (seeds first, others get
  seed `bootstrap` addrs), then full-meshes unless
  `ManualPeers`. `DefaultClusterOptions(name)` fills the defaults.
- `cluster.Node(i)` (panics if stopped: a test bug) with
  `Execute`/`Query`; `NodeInfo(i)` (label/dir/node id/repl + sql
  addrs, also while stopped); `NodeID`, `NodeOriginPubkey`,
  `NodeOriginSeed`, `NodePID`, `Len`, `Name`, `RuntimeDir`,
  `DatabaseID`, `Connect`.
- `AddPeer`/`AddPeerAddr`/`RemovePeer`/`RetirePeer`/`SyncAll`/
  `Mesh` (redial every ordered pair of *running* nodes; use after
  heals and restarts); `StopNode`/`KillNode`/`SuspendNode`/
  `ResumeNode`/`StartNode` (same dir+key, fresh ports; re-mesh
  afterwards); `StartNodesParallel`; `SetSpoolOptions`/
  `SetBridgeConfig`/`SetNodeKey` (stored; next start applies them);
  `RotateNodeCert`; `RestartNodeWithIdentity`; `CaPEM`/`CaDER`/
  `AddTrustRoot`/`IssueCert` for custom-cert legs;
  `EnsureMigrated`; `DumpForensics` (infallible; call inside the
  failure branch); `Finish(failed)` (kills + reaps children,
  retains-or-cleans runtime, prints the `RESULT` line).
- `WaitUntil`, `EnvSeconds`, `Hex`, `DecodeHex32`, `SQLLiteral`,
  `DBMessage`/`ErrorCode`; `ConnectWithWatchdog` for connections
  outside `Cluster`; `FreeAddr`/`FreePort`; `certs` CA helpers
  (`GenerateCa`, `Issue`, `WriteTLSDir`).

SQL (`gosqlharness`): `NewCluster(ClusterOptions)` with the same
lifecycle; `Node(i)` returns the `*goharness.Client` directly;
`SetConfig`/`SetPeers`/`SetKey`/`SetClientTLS` reconfigure nodes;
`StartNode`/`StopNode`/`PID`/`Config`/`NodeInfo`; `AssertRows`;
`Download`/`Upload` for backup legs; `AssertNoPlaintext`.

Env knobs (same names everywhere): `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS` (120),
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS` (1800), plus
`MURMUR_SERVER_BIN`/`MURMUR_ADMIN_BIN` (prebuilt servers).

## 4. Per-scenario file template

`tests-live/<scenario>/<scenario_snake>_test.go`:

```go
func TestSomething(t *testing.T) {
    if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
        t.Skip("live suite; set MURMUR_ENGINE_INTEGRATION=1")
    }
    cluster, err := goharness.NewCluster(goharness.ClusterOptions{
        Name:     "scenario",
        NumNodes: 3,
    })
    if err != nil {
        t.Fatalf("cluster setup: %v", err)
    }
    failed := true
    defer func() {
        if failed {
            cluster.DumpForensics("short-reason")
        }
        cluster.Finish(failed)
    }()
    // ... body: SQL, AddPeer/RemovePeer, stop/start, waits with deadlines ...
    failed = false
}
```

Return errors from helpers instead of failing mid-flight so every
exit path still reaches `DumpForensics`/`Finish` (which kill the
children). `run.sh` sweeps stragglers scoped to its runtime dir.

Env-gated scenarios (`clock-skew`, `diskfull-live`,
`impaired-network`) start the body with a capability probe and
`t.Skip` when absent.

`tests-live/<scenario>/README.md`: what it proves, env knobs, pass
criteria, and any deviation from the original suite.

Replicated tables need `MURMUR SET TABLE <name> REPLICATED true` on
EACH node (the flag is per-node config). Restarts preserve
`[replication.tables]` automatically; fresh starts require a
re-mesh afterwards.

## 5. Compile gate and runtime validation

Per-scenario compile gate (no cluster is started; the suite skips):

```sh
go test -count=1 ./tests-live/<scenario>/
```

- Package scoping is what makes the check cheap: one scenario
  package + its harness, nothing else under `tests-live/`.
- Never run `tests-live/run.sh` from an automated port/check agent;
  it spins real clusters.
- A green skip-gated `go test` is the ONLY gate for scenario work.

Runtime validation (CI/operators) is `run.sh`, which builds the
server binaries, exports `MURMUR_SERVER_BIN`, and runs scenarios
sequentially with per-scenario `timeout(1)` caps:

```sh
tests-live/run.sh <scenario>  # one scenario by directory name
tests-live/run.sh all         # ALL_SCENARIOS in order
tests-live/run.sh gate        # GATE_SCENARIOS release-gate subset
tests-live/run.sh soak        # files-soak + soak-slo + long-running-five-node
tests-live/run.sh stress      # rolling-restart, partial-mesh, crash-recovery,
                              # partition, snapshot-resync x MURMUR_STRESS_COUNT (default 5)
```

Harness-only checks (no scenario): `go build` + `go vet` on
`./tests-live/goharness/` and `./tests-live/gosqlharness/`.

## 6. Rules for scenario authors

1. Never run `run.sh` from automation; single-package skip-gated `go test` only.
2. Sequential single writer: one scenario in flight at a time per checkout.
3. Never touch product code or any other scenario's directory.
4. Never fix a missing product API by forking harness code into your scenario;
   shared needs go to the harness, product gaps go in your report.
5. Keep the suite's pass criteria; document every deviation in the
   scenario `README.md`.

## 7. Suite coverage

All suites drive `murmur-server` children over the PG wire (SQL +
`MURMUR PEER/SHOW/RETENTION/POLICY/FILE/BACKUP` commands) and Unix signals
where noted. Crash (`KillNode`), restart (`StopNode`/`StartNode`),
suspend (`SuspendNode`, unix), and parallel-restart
(`StartNodesParallel`) legs work directly on the children with no
fixture binary.

Covered: three-node-sync, partition, chaos-load,
flap-partition, three-way-heal, tail-repair, partial-mesh, swim-discovery,
plumtree-live (plumtree dissemination; mixed modes fail closed),
scale-mesh (fanout), long-running-five-node, txchunk-resume
(`max_transaction_bytes` + snapshot resume), corrupt-snapshot
(`trusted_snapshot_sources`), large-payload, crdt-contention, typed-records
(`MURMUR POLICY` + subscriptions), typed-bridge, bridge-two-streams,
merge-policies, delete-pruning, views, schema-evolution,
migration-concurrency, migration-crash, clock-skew (env-gated),
crash-recovery, crash-loop, rejoin-storm, rolling-restart, pause-resume
(`SuspendNode`), churn-retirement (`RetirePeer`), snapshot-resync,
open-progress (connect watchdog; no intra-open phases over the wire),
compression, gc-balance, log-boundedness, maintenance-under-load,
loadshare, write-priority, soak-slo, cert-lifecycle (rotation =
`RotateNodeCert`; expiry enforced by the QUIC verifiers), allow-nodes
(`allowed_peers`), addrpolicy (`allowed_networks`), dbid-isolation,
version-skew, hostile-peer, hostile-schema (signed manifests), encryption
(`WrongKey` on unlock), plaintext-audit, rekey (`MURMUR ROTATE`),
file-permissions (0600/0700 in data/backup), backup-restore,
backup-under-fire, tampered-backup, downgrade-guard, files-bridge,
files-soak, files-crash, files-corrupt-source, highlow (bridge
export/stage/process/release), origin-signatures (`trusted_origins`),
abuse, endurance-chaos, resource-exhaustion, diskfull-live (env-gated),
impaired-network (env-gated), graceful-shutdown (`StopNode` losslessness;
SIGTERM is the default stop path), unlock-abuse (`MURMUR UNLOCK` key
auth), tls-floor (PG-wire TLS versions), api-mtls (PG-wire mTLS;
production mode), dos-client (PG-wire + QUIC flood legs), search-mesh
(managed FTS/vector/JSON over the mesh; node 0 is schema master),
sqli-api (bound-parameter payloads; the PG wire rejects non-SQL input
at parse time), overload-budgets (value and concurrency/p99 legs
against wire/server budgets).

## 8. Validation notes

Compile every scenario independently (section 5). Runtime checks must
use `tests-live/run.sh <scenario>` (or `all`, `gate`, `soak`,
`stress`). Missing scenario packages are errors rather than skips.
The runner accepts `MURMUR_SERVER_BIN` and `MURMUR_ADMIN_BIN` for
prebuilt binaries. A compile sweep does not replace runtime fault
checks; shortened soak runs do not establish long-duration stability.

The downgrade guard's future-format corruption fixture is generated
by a separate engine-owned ignored unit-test executable.
`run.sh downgrade-guard` refuses to run unless
`MURMUR_FORMAT_FIXTURE_BIN` points at it. The live scenario itself
links no engine implementation and verifies refusal and recovery
over SQL. This requires the matching fixture test in the sibling
engine checkout.

// Live scenario: a migrated peer crashes at the schema-manifest store
// boundary, recovers, rejoins, and converges with both peers.
//
// Go port of tests/migration_crash.rs using tests-live/goharness.
// Identical behavior: two cases (`before-store` and `after-store`) seed
// 24 rows, isolate node 1, stop/restart it on either side of the
// synchronous migration store, rejoin, adopt the new schema on the
// peers, and converge on a migrated-field write. The Rust
// wait_until_async polls become goharness.WaitUntil.
package migrationcrash

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// Base table DDL (Go: typed records with a Name field).
const table = "live_typed_records"

const tableDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY, counter INTEGER NOT NULL)"

// Additive migration DDL (Go: `MigrateTypedRecords` registers Note).
const noteDDL = "ALTER TABLE live_typed_records ADD COLUMN note TEXT"

// Seeded rows (Go: `seedRows = 24`).
const seedRows = 24

// Seed convergence deadline (Go: 45 s).
const seedTimeout = 45 * time.Second

// Post-rejoin convergence deadline (Go: 90 s).
const rejoinTimeout = 90 * time.Second

// Peer-adoption deadline (Go: 45 s `waitEpoch`).
const epochTimeout = 45 * time.Second

// Final epoch+field convergence deadline (Go: 45 s).
const finalTimeout = 45 * time.Second

// Poll interval (Go: 250 ms).
const poll = 250 * time.Millisecond

// names returns the canonical ordered names on one node (Go:
// `Cluster.TypedNames`).
func names(cluster *goharness.Cluster, idx int) ([]string, error) {
	rows, err := cluster.Node(idx).Query("SELECT name FROM live_typed_records ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("node%d read names: %w", idx+1, err)
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.Get(0))
	}
	return out, nil
}

// insertRow inserts one record (Go: `Cluster.TypedInsertWithID`).
func insertRow(cluster *goharness.Cluster, idx int, name string) error {
	if _, err := cluster.Node(idx).Execute(
		"INSERT INTO live_typed_records(name, counter) VALUES($1, 0)", name); err != nil {
		return fmt.Errorf("node%d insert failed: %w", idx+1, err)
	}
	if _, err := cluster.Node(idx).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("node%d flush after insert failed: %w", idx+1, err)
	}
	return nil
}

// setNote writes the migrated Note field (Go: `Cluster.TypedSetNote`).
func setNote(cluster *goharness.Cluster, idx int, name, note string) error {
	changed, err := cluster.Node(idx).Execute(
		"UPDATE live_typed_records SET note = $1 WHERE name = $2", note, name)
	if err != nil {
		return fmt.Errorf("node%d set note failed: %w", idx+1, err)
	}
	if changed != 1 {
		return fmt.Errorf("node%d set note hit %d rows", idx+1, changed)
	}
	if _, err := cluster.Node(idx).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("node%d flush after set note failed: %w", idx+1, err)
	}
	return nil
}

// hasColumn reports column presence (Go: schema-manifest epoch
// assertions; established port pattern). Absent = epoch 1, present =
// epoch 2. `PRAGMA table_info` is on the server's side-effect-free
// inspection allowlist.
func hasColumn(cluster *goharness.Cluster, idx int, table, column string) bool {
	rows, err := cluster.Node(idx).Simple("PRAGMA table_info(" + table + ")")
	if err != nil {
		return false
	}
	for _, r := range rows {
		if len(r) > 1 && r[1] == column {
			return true
		}
	}
	return false
}

// epoch returns the schema epoch of one node (Go: `schema_epoch` in
// `/v1/status`).
func epoch(cluster *goharness.Cluster, idx int) uint64 {
	if hasColumn(cluster, idx, table, "note") {
		return 2
	}
	return 1
}

// assertEpoch requires an exact epoch (Go: `assertEpoch`).
func assertEpoch(cluster *goharness.Cluster, idx int, want uint64) error {
	if got := epoch(cluster, idx); got != want {
		return fmt.Errorf("node%d schema epoch = %d, want %d", idx+1, got, want)
	}
	return nil
}

// tryReadNote is a note read that surfaces SQL errors (Go: `TypedNote`).
// It returns nil for a missing row or a NULL note.
func tryReadNote(cluster *goharness.Cluster, idx int, name string) (*string, error) {
	rows, err := cluster.Node(idx).Query(
		"SELECT note FROM live_typed_records WHERE name = $1", name)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 || rows[0].IsNull(0) {
		return nil, nil
	}
	note := rows[0].Get(0)
	return &note, nil
}

// migrateUnlessAdopted applies the additive migration unless the column
// is already present (Go `MigrateTypedRecords` is an idempotent manifest
// bind/rebind; Rust DDL errors on a duplicate column, so adoption is
// checked first).
func migrateUnlessAdopted(cluster *goharness.Cluster, idx int) error {
	if hasColumn(cluster, idx, table, "note") {
		return nil
	}
	if _, err := cluster.Node(idx).Simple(noteDDL); err != nil {
		return fmt.Errorf("migrate node%d: %w", idx+1, err)
	}
	if _, err := cluster.Node(idx).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("flush migration on node%d: %w", idx+1, err)
	}
	return nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// waitConverged waits until every node holds exactly `want` (Go:
// `waitConverged`).
func waitConverged(cluster *goharness.Cluster, want []string, timeout time.Duration) error {
	converged := goharness.WaitUntil(timeout, poll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			got, err := names(cluster, i)
			if err != nil || !equalStrings(got, want) {
				return false
			}
		}
		return true
	})
	if !converged {
		return fmt.Errorf("nodes did not converge on %d managed record names within %v", len(want), timeout)
	}
	return nil
}

// waitEpoch waits until the listed nodes adopt `want` (Go: `waitEpoch`).
func waitEpoch(cluster *goharness.Cluster, nodes []int, want uint64, timeout time.Duration) error {
	converged := goharness.WaitUntil(timeout, poll, func() bool {
		for _, i := range nodes {
			if epoch(cluster, i) != want {
				return false
			}
		}
		return true
	})
	if !converged {
		return fmt.Errorf("nodes %v did not adopt schema epoch %d within %v", nodes, want, timeout)
	}
	return nil
}

// runSchemaCrashCase is one crash-boundary case (Go:
// `runSchemaCrashCase`). When `migrateBeforeRestart` is false the
// isolated node stops before the store (recovery reopens at epoch 1 and
// retries); when true it stores the migration first (recovery reopens at
// epoch 2).
func runSchemaCrashCase(t *testing.T, phase string, migrateBeforeRestart bool) {
	t.Helper()
	opts := goharness.DefaultClusterOptions("migration-crash-" + phase)
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		fmt.Fprintf(os.Stderr, "%s dir=%s sql=%s repl=%s\n",
			info.Label, info.Dir, info.SQLAddr, info.ReplAddr)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()
	if err := runBody(cluster, phase, migrateBeforeRestart); err != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", err)
		cluster.DumpForensics("migration-crash-" + phase + "-no-convergence")
		t.Fatalf("scenario failed: %v", err)
	}
	failed = false
}

func runBody(cluster *goharness.Cluster, phase string, migrateBeforeRestart bool) error {
	// Same base schema on every node (Go: typed fixture tables), flagged
	// replicated on each node (row convergence needs the per-node flag).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			return fmt.Errorf("migrate node%d: %w", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE " + table + " REPLICATED true"); err != nil {
			return fmt.Errorf("node%d SET TABLE REPLICATED: %w", i+1, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("node%d flush after SET TABLE: %w", i+1, err)
		}
	}

	// Seed on node 2, converge, all old.
	wantNames := make([]string, 0, seedRows)
	for i := 0; i < seedRows; i++ {
		name := fmt.Sprintf("seed-%02d", i)
		if err := insertRow(cluster, 1, name); err != nil {
			return err
		}
		wantNames = append(wantNames, name)
	}
	sort.Strings(wantNames)
	if err := waitConverged(cluster, wantNames, seedTimeout); err != nil {
		return err
	}
	for idx := 0; idx < cluster.Len(); idx++ {
		if err := assertEpoch(cluster, idx, 1); err != nil {
			return err
		}
	}

	// Keep the two old-schema peers connected to each other while
	// isolating node 1 so neither can observe its recovery early.
	for _, peer := range []int{1, 2} {
		if err := cluster.RemovePeer(0, peer); err != nil {
			return fmt.Errorf("isolate node1 from node%d: %w", peer+1, err)
		}
		if err := cluster.RemovePeer(peer, 0); err != nil {
			return fmt.Errorf("isolate node%d from node1: %w", peer+1, err)
		}
	}

	// Crash at the schema-manifest store boundary: stop/restart the child
	// on either side of the synchronous store (see README).
	if migrateBeforeRestart {
		if err := migrateUnlessAdopted(cluster, 0); err != nil {
			return err
		}
	}
	cluster.StopNode(0)
	if err := cluster.StartNode(0); err != nil {
		return fmt.Errorf("restart node1: %w", err)
	}
	restartEpoch := uint64(1)
	if migrateBeforeRestart {
		restartEpoch = 2
	}
	if err := assertEpoch(cluster, 0, restartEpoch); err != nil {
		return err
	}
	for _, idx := range []int{1, 2} {
		if err := assertEpoch(cluster, idx, 1); err != nil {
			return err
		}
	}
	fmt.Fprintf(os.Stderr, "node1 recovered at schema manifest %s boundary while both peers remained on epoch 1\n", phase)

	// Recovered rows readable before reconnect.
	got, err := names(cluster, 0)
	if err != nil {
		return err
	}
	if !equalStrings(got, wantNames) {
		return fmt.Errorf("recovered rows = %v, want %v", got, wantNames)
	}
	if restartEpoch == 1 {
		if err := migrateUnlessAdopted(cluster, 0); err != nil {
			return err
		}
	}
	if err := assertEpoch(cluster, 0, 2); err != nil {
		return err
	}
	if note, err := tryReadNote(cluster, 0, wantNames[0]); err != nil {
		return err
	} else if note != nil {
		return fmt.Errorf("new field after local recovery = %q; want empty (NULL) default", *note)
	}

	// Rejoin and converge (fresh ports after the restart need a re-mesh).
	for _, peer := range []int{1, 2} {
		if err := cluster.AddPeer(0, peer); err != nil {
			return fmt.Errorf("rejoin node1 to node%d: %w", peer+1, err)
		}
		if err := cluster.AddPeer(peer, 0); err != nil {
			return fmt.Errorf("rejoin node%d to node1: %w", peer+1, err)
		}
	}
	if err := cluster.SyncAll(); err != nil {
		return fmt.Errorf("sync after rejoin: %w", err)
	}
	if err := waitConverged(cluster, wantNames, rejoinTimeout); err != nil {
		return err
	}
	if err := waitEpoch(cluster, []int{1, 2}, 2, epochTimeout); err != nil {
		return err
	}
	for _, idx := range []int{1, 2} {
		if err := migrateUnlessAdopted(cluster, idx); err != nil {
			return err
		}
		if !hasColumn(cluster, idx, table, "note") {
			return fmt.Errorf("node%d did not rebind to the adopted v2 schema", idx+1)
		}
	}

	// A migrated-field write converges with both peers at epoch 2.
	if err := setNote(cluster, 0, wantNames[0], phase+"-recovered"); err != nil {
		return err
	}
	wantNote := phase + "-recovered"
	deadline := time.Now().Add(finalTimeout)
	converged := false
	var lastState string
	for {
		var states []string
		ready := true
		for idx := 0; idx < cluster.Len(); idx++ {
			ep := epoch(cluster, idx)
			note, noteErr := tryReadNote(cluster, idx, wantNames[0])
			noteDesc := "<error>"
			if noteErr == nil {
				if note == nil {
					noteDesc = "<nil>"
				} else {
					noteDesc = *note
				}
			}
			states = append(states, fmt.Sprintf("node%d epoch=%d note=%s", idx+1, ep, noteDesc))
			noteOK := noteErr == nil && note != nil && *note == wantNote
			if ep != 2 || !noteOK {
				ready = false
			}
		}
		if ready {
			converged = true
			lastState = fmt.Sprintf("%v", states)
			break
		}
		if !time.Now().Before(deadline) {
			lastState = fmt.Sprintf("%v", states)
			break
		}
		time.Sleep(poll)
	}
	if !converged {
		return fmt.Errorf("schema epoch and migrated field did not converge after %s crash: %s",
			phase, lastState)
	}
	fmt.Fprintf(os.Stderr, "%s schema-store crash recovered and converged with both peers\n", phase)
	return nil
}

func TestMigratedPeerCrashBeforeStoreReopensAndConverges(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	runSchemaCrashCase(t, "before-store", false)
}

func TestMigratedPeerCrashAfterStoreReopensAndConverges(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	runSchemaCrashCase(t, "after-store", true)
}

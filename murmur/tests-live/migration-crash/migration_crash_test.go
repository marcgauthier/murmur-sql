// Live scenario: an isolated node recovers from a crash at the
// schema-manifest store boundary, then rejoins.
//
// Go port of the Rust `migration-crash` scenario
// (/home/marc/MURMUR/tests-live/migration-crash/tests/migration_crash.rs).
// An isolated node recovers from a crash at the schema-manifest store
// boundary while its peers stay on the old epoch; afterwards it rejoins,
// the peers adopt the new schema, and a migrated-field write converges
// everywhere. Two cases: before-store (recovery reopens old and retries
// the migration) and after-store (recovery reopens already migrated).
// Plain SQL replaces the typed-records HTTP API, and in-process
// close/reopen replaces the mid-migration process exit.
package migrationcrash

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// table is the base table (Rust: typed records with a Name field).
const table = "live_typed_records"

// tableDDL is the base table DDL.
const tableDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY, counter INTEGER NOT NULL)"

// noteDDL is the additive migration DDL (Rust: MigrateTypedRecords
// registers Note).
const noteDDL = "ALTER TABLE live_typed_records ADD COLUMN note TEXT"

const (
	// seedRows is the seeded row count (Rust: seedRows = 24).
	seedRows = 24
	// seedTimeout is the seed convergence deadline (Rust: 45 s).
	seedTimeout = 45 * time.Second
	// rejoinTimeout is the post-rejoin convergence deadline (Rust:
	// 90 s).
	rejoinTimeout = 90 * time.Second
	// epochTimeout is the peer-adoption deadline (Rust: 45 s
	// waitEpoch).
	epochTimeout = 45 * time.Second
	// finalTimeout is the final epoch+field convergence deadline
	// (Rust: 45 s).
	finalTimeout = 45 * time.Second
	// poll is the poll interval (Rust: 250 ms).
	poll = 250 * time.Millisecond
)

// names is the canonical ordered names on one node (Rust:
// Cluster.TypedNames).
func names(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM live_typed_records ORDER BY name")
	})
	if err != nil {
		t.Fatalf("read names failed: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

// insertRow inserts one record (Rust: Cluster.TypedInsertWithID).
func insertRow(t *testing.T, db *murmur.Database, node int, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO live_typed_records(name, counter) VALUES(?, 0)", name)
		return nil, err
	}); err != nil {
		t.Fatalf("node%d insert failed: %v", node, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after insert failed: %v", node, err)
	}
}

// setNote writes the migrated Note field (Rust: Cluster.TypedSetNote).
func setNote(t *testing.T, db *murmur.Database, node int, name, note string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		changed, err := s.Execute("UPDATE live_typed_records SET note = ? WHERE name = ?", note, name)
		if err != nil {
			return nil, err
		}
		if changed != 1 {
			return nil, fmt.Errorf("node%d set note hit %d rows", node, changed)
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("node%d set note failed: %v", node, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after set note failed: %v", node, err)
	}
}

// hasColumn reports column presence (Rust: schema-manifest epoch
// assertions; established typed-records port pattern). Absent = epoch 1,
// present = epoch 2.
func hasColumn(t *testing.T, db *murmur.Database, tableName, column string) bool {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("PRAGMA table_info(" + tableName + ")")
	})
	if err != nil {
		return false
	}
	for _, row := range v.([][]sqlite.Value) {
		if len(row) > 1 && row[1].Str == column {
			return true
		}
	}
	return false
}

// epoch is the schema epoch of one node (Rust: schema_epoch in
// /v1/status).
func epoch(t *testing.T, cluster *harness.Cluster, idx int) uint64 {
	t.Helper()
	if hasColumn(t, cluster.Node(idx), table, "note") {
		return 2
	}
	return 1
}

// assertEpoch requires an exact epoch (Rust: assertEpoch).
func assertEpoch(t *testing.T, cluster *harness.Cluster, idx int, want uint64) {
	t.Helper()
	if got := epoch(t, cluster, idx); got != want {
		t.Fatalf("node%d schema epoch = %d, want %d", idx+1, got, want)
	}
}

// tryReadNote is a Note read that surfaces SQL errors (Rust: TypedNote).
// It returns (value, present, exposed): exposed is false when the column
// does not exist yet; present is false when the row is missing or NULL.
func tryReadNote(db *murmur.Database, name string) (string, bool, error) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT note FROM live_typed_records WHERE name = ?", name)
	})
	if err != nil {
		return "", false, err
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 || rows[0][0].Type == sqlite.Null {
		return "", false, nil
	}
	return rows[0][0].Str, true, nil
}

// migrateUnlessAdopted applies the additive migration unless the column is
// already present (Rust MigrateTypedRecords is an idempotent manifest
// bind/rebind; DDL errors on a duplicate column, so adoption is checked
// first).
func migrateUnlessAdopted(t *testing.T, cluster *harness.Cluster, idx int) {
	t.Helper()
	if hasColumn(t, cluster.Node(idx), table, "note") {
		return
	}
	if err := cluster.Node(idx).Migrate(murmur.NewMigration(noteDDL)); err != nil {
		t.Fatalf("migrate node%d: %v", idx+1, err)
	}
	if err := cluster.Node(idx).Flush(); err != nil {
		t.Fatalf("flush migration on node%d: %v", idx+1, err)
	}
}

// waitConverged waits until every node holds exactly want (Rust:
// waitConverged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want []string, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, poll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if !slices.Equal(names(t, cluster.Node(i)), want) {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("nodes did not converge on %d managed record names within %v", len(want), timeout)
	}
}

// waitEpoch waits until the listed nodes adopt want (Rust: waitEpoch).
func waitEpoch(t *testing.T, cluster *harness.Cluster, nodes []int, want uint64, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, poll, func() bool {
		for _, i := range nodes {
			if epoch(t, cluster, i) != want {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("nodes %v did not adopt schema epoch %d within %v", nodes, want, timeout)
	}
}

// runSchemaCrashCase is one crash-boundary case (Rust: runSchemaCrashCase).
// When migrateBeforeRestart is false the isolated node closes before the
// store (recovery reopens at epoch 1 and retries); when true it stores the
// migration first (recovery reopens at epoch 2).
func runSchemaCrashCase(t *testing.T, phase string, migrateBeforeRestart bool) {
	t.Helper()
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "migration-crash-" + phase,
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("migration-crash-" + phase + "-no-convergence")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}

	// Same base schema on every node (Rust: typed fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).Migrate(murmur.NewMigration(tableDDL)); err != nil {
			t.Fatalf("migrate records table: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush schema: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication(table, true); err != nil {
			t.Fatalf("opt-in %s: %v", table, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}

	// Seed on node 2, converge, all old.
	var wantNames []string
	for i := 0; i < seedRows; i++ {
		name := fmt.Sprintf("seed-%02d", i)
		insertRow(t, cluster.Node(1), 2, name)
		wantNames = append(wantNames, name)
	}
	slices.Sort(wantNames)
	waitConverged(t, cluster, wantNames, seedTimeout)
	for idx := 0; idx < cluster.Len(); idx++ {
		assertEpoch(t, cluster, idx, 1)
	}

	// Keep the two old-schema peers connected to each other while
	// isolating node 1 so neither can observe its recovery early.
	for _, peer := range []int{1, 2} {
		if err := cluster.RemovePeer(0, peer); err != nil {
			t.Fatalf("isolate node1 from node%d: %v", peer+1, err)
		}
		if err := cluster.RemovePeer(peer, 0); err != nil {
			t.Fatalf("isolate node%d from node1: %v", peer+1, err)
		}
	}

	// Crash at the schema-manifest store boundary: Migrate is a
	// synchronous call with no crash-hook boundary, so the two cases
	// close/reopen on either side of the synchronous store.
	if migrateBeforeRestart {
		migrateUnlessAdopted(t, cluster, 0)
	}
	cluster.StopNode(0)
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart node1: %v", err)
	}
	var restartEpoch uint64 = 1
	if migrateBeforeRestart {
		restartEpoch = 2
	}
	assertEpoch(t, cluster, 0, restartEpoch)
	for _, idx := range []int{1, 2} {
		assertEpoch(t, cluster, idx, 1)
	}
	t.Logf("node1 recovered at schema manifest %s boundary while both peers remained on epoch 1", phase)

	// Recovered rows readable before reconnect.
	if got := names(t, cluster.Node(0)); !slices.Equal(got, wantNames) {
		t.Fatalf("recovered rows = %v, want %v", got, wantNames)
	}
	if restartEpoch == 1 {
		migrateUnlessAdopted(t, cluster, 0)
	}
	assertEpoch(t, cluster, 0, 2)
	if got, present, err := tryReadNote(cluster.Node(0), wantNames[0]); err != nil || present {
		t.Fatalf("new field after local recovery = (%q,%v,%v); want empty (NULL) default", got, present, err)
	}

	// Rejoin and converge.
	for _, peer := range []int{1, 2} {
		if err := cluster.AddPeer(0, peer); err != nil {
			t.Fatalf("rejoin node1 to node%d: %v", peer+1, err)
		}
		if err := cluster.AddPeer(peer, 0); err != nil {
			t.Fatalf("rejoin node%d to node1: %v", peer+1, err)
		}
	}
	waitConverged(t, cluster, wantNames, rejoinTimeout)
	waitEpoch(t, cluster, []int{1, 2}, 2, epochTimeout)
	for _, idx := range []int{1, 2} {
		migrateUnlessAdopted(t, cluster, idx)
		if !hasColumn(t, cluster.Node(idx), table, "note") {
			t.Fatalf("node%d did not rebind to the adopted v2 schema", idx+1)
		}
	}

	// A migrated-field write converges with both peers at epoch 2.
	wantNote := phase + "-recovered"
	setNote(t, cluster.Node(0), 1, wantNames[0], wantNote)
	lastState := ""
	if ok := harness.WaitUntil(finalTimeout, poll, func() bool {
		var states []string
		ready := true
		for idx := 0; idx < cluster.Len(); idx++ {
			ep := epoch(t, cluster, idx)
			note, present, err := tryReadNote(cluster.Node(idx), wantNames[0])
			states = append(states, fmt.Sprintf("node%d epoch=%d note=(%q,%v,%v)", idx+1, ep, note, present, err))
			if ep != 2 || err != nil || !present || note != wantNote {
				ready = false
			}
		}
		lastState = fmt.Sprintf("%v", states)
		return ready
	}); !ok {
		t.Fatalf("schema epoch and migrated field did not converge after %s crash: %s", phase, lastState)
	}
	t.Logf("%s schema-store crash recovered and converged with both peers", phase)
	failed = false
}

func TestMigratedPeerCrashBeforeStoreReopensAndConverges(t *testing.T) {
	runSchemaCrashCase(t, "before-store", false)
}

func TestMigratedPeerCrashAfterStoreReopensAndConverges(t *testing.T) {
	runSchemaCrashCase(t, "after-store", true)
}

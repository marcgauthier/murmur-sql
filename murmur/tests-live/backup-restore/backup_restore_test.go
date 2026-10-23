// Live scenario: offline backup, total loss, fresh-identity restore, mesh
// rejoin.
//
// Go port of the Rust `backup-restore` scenario
// (/home/marc/MURMUR/tests-live/backup-restore/tests/backup_restore.rs):
//   - TestBackupRestoreRejoinMesh: three nodes converge on 20 rows, node3
//     stops, its data dir is backed up offline, wiped (total loss), and
//     restored under a fresh writer identity; node3 restarts on the
//     restored data with a reissued TLS certificate and fresh origin key,
//     the mesh reconverges with no lost rows, and the restarted node's
//     identity is the fresh one.
//   - TestRestoreRejectsStaleIdentity: rollback safety — opening restored
//     data under the old NodeID is rejected, while the fresh identity
//     opens cleanly.
//   - TestTypedBackupRestoreRoundTripLive: a typed row (plain field plus
//     PN_COUNTER / OR_SET / MAX / MIN policy ops) survives backup of the
//     encrypted store, fresh-identity restore, and reopen with identical
//     materialized values.
//
// Plain SQL plus policy ops replace the managed typed-records HTTP API;
// stop/start replaces StopNode/StartNode + unlock; the /metrics identity
// assertion becomes Database.NodeID.
package backuprestore

import (
	"crypto/rand"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// table is the typed table (Rust: live_typed_records managed typed
// table).
const table = "live_typed_records"

// namesDDL is the mesh-leg DDL: plain replicated rows (Rust:
// TypedInsert names).
const namesDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)"

// typedDDL is the typed-leg DDL: plain field plus counter/set/extrema
// policy columns (the typed-records table shape). Set projections start as
// x'00' (postcard empty vec), the convention for fresh set cells.
const typedDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, counter INTEGER NOT NULL, tags BLOB NOT NULL, peak INTEGER NOT NULL, floor REAL NOT NULL)"

const (
	// numRows is the seed row count (Rust: 20 rows).
	numRows = 20
	// convergeTimeout is the initial convergence deadline (Rust: 30 s).
	convergeTimeout = 30 * time.Second
	// reconvergeTimeout is the post-restart deadline (Rust: 90 s).
	reconvergeTimeout = 90 * time.Second
	// poll is the convergence poll interval (Rust: 200 ms).
	poll = 200 * time.Millisecond
)

// mergePolicies maps (table, column) -> merge policy for the typed leg
// (Rust: PN_COUNTER, OR_SET, numeric MAX/MIN field kinds).
func mergePolicies() map[replication.PolicyColumn]replication.Policy {
	return map[replication.PolicyColumn]replication.Policy{
		{Table: table, Column: "counter"}: replication.PolicyCounter,
		{Table: table, Column: "tags"}:    replication.PolicySet,
		{Table: table, Column: "peak"}:    replication.PolicyMax,
		{Table: table, Column: "floor"}:   replication.PolicyMin,
	}
}

// migrateAll migrates every node with ddl (Rust: shared Config.Tables
// definition).
func migrateAll(t *testing.T, cluster *harness.Cluster, ddl string) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, ddl); err != nil {
			t.Fatalf("node%d migrate %q failed: %v", i+1, ddl, err)
		}
	}
	// Opt in after every node migrated: opting in publishes the schema
	// immediately, and an early publication lets a peer adopt the table
	// before its own migrate runs ("table already exists").
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication(table, true); err != nil {
			t.Fatalf("node%d opt-in failed: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush after opt-in failed: %v", i+1, err)
		}
	}
}

// typedInsert is a typed insert on one node (Rust: Cluster.TypedInsert).
func typedInsert(t *testing.T, db *murmur.Database, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO live_typed_records(name) VALUES(?)", name)
		return nil, err
	}); err != nil {
		t.Fatalf("typed insert failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after typed insert: %v", err)
	}
}

// typedNames is the canonical ordered names on one node (Rust:
// Cluster.TypedNames).
func typedNames(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM live_typed_records ORDER BY name")
	})
	if err != nil {
		t.Fatalf("read typed names: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

// waitConverged waits until every running node holds the same want names
// (Rust: waitConverged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, poll, func() bool {
		var first []string
		for i := 0; i < cluster.Len(); i++ {
			names := typedNames(t, cluster.Node(i))
			if len(names) != want {
				return false
			}
			if first == nil {
				first = names
			} else if !slices.Equal(first, names) {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("nodes did not converge on %d rows within %v", want, timeout)
	}
}

// openOffline opens a stopped node's data dir without replication (Rust:
// openNodeDir offline library open for backup/restore legs).
func openOffline(t *testing.T, dataDir string, key [32]byte, databaseID, nodeID [16]byte) *murmur.Database {
	t.Helper()
	opts := murmur.DefaultOptions()
	opts.Mode = murmur.OpenExisting
	opts.DatabaseID = &databaseID
	opts.NodeID = &nodeID
	db, err := murmur.Open(dataDir, key[:], opts)
	if err != nil {
		t.Fatalf("offline open of node data dir: %v", err)
	}
	return db
}

// backupStoppedNode backs up a stopped node's data dir through an offline
// open (Rust: offline.Backup), returning the backup directory.
func backupStoppedNode(t *testing.T, cluster *harness.Cluster, key [32]byte, idx int, destName string) string {
	t.Helper()
	info := cluster.NodeInfo(idx)
	offline := openOffline(t, info.DataDir, key, cluster.DatabaseID(), info.NodeID)
	backupDir := cluster.RuntimeDir() + "/" + destName
	if err := offline.Backup(backupDir); err != nil {
		t.Fatalf("offline backup of stopped node: %v", err)
	}
	if err := offline.Close(); err != nil {
		t.Fatalf("close offline handle: %v", err)
	}
	return backupDir
}

// restoreFreshIdentity restores backupDir over a wiped data dir under a
// fresh writer identity (Rust: RemoveAll(data) + backup.Restore with
// RestoreClone).
func restoreFreshIdentity(t *testing.T, backupDir, targetDataDir string, key [32]byte, fresh [16]byte) {
	t.Helper()
	if err := os.RemoveAll(targetDataDir); err != nil {
		t.Fatalf("wipe data dir (total loss): %v", err)
	}
	if err := murmur.RestoreWithOptions(backupDir, targetDataDir, key[:], murmur.RestoreOptions{NewNodeID: &fresh}); err != nil {
		t.Fatalf("restore backup under fresh identity: %v", err)
	}
}

// freshNodeID mints a random node id distinct from old.
func freshNodeID(t *testing.T, old [16]byte) [16]byte {
	t.Helper()
	var fresh [16]byte
	if _, err := rand.Read(fresh[:]); err != nil {
		t.Fatalf("fresh node id entropy: %v", err)
	}
	if fresh == old {
		t.Fatalf("fresh identity must differ")
	}
	return fresh
}

func TestBackupRestoreRejoinMesh(t *testing.T) {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatalf("key entropy: %v", err)
	}
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "backup-restore",
		NumNodes: 3,
		Key:      key,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("backup-restore-rejoin")
		}
		cluster.Finish(failed)
	}()
	migrateAll(t, cluster, namesDDL)
	for i := 0; i < numRows; i++ {
		typedInsert(t, cluster.Node(0), fmt.Sprintf("backup-row-%02d", i))
	}
	waitConverged(t, cluster, numRows, convergeTimeout)

	// Offline backup of the stopped node through the library.
	oldID := cluster.NodeID(2)
	cluster.StopNode(2)
	backupDir := backupStoppedNode(t, cluster, key, 2, "node3-backup")

	// Total loss of node3's durable data, then fresh-identity restore
	// into the same location the node will reopen.
	fresh := freshNodeID(t, oldID)
	dataDir := cluster.NodeInfo(2).DataDir
	restoreFreshIdentity(t, backupDir, dataDir, key, fresh)

	// Restart on the restored data with a reissued TLS certificate and
	// fresh origin key; the mesh must reconverge with no lost rows.
	if err := cluster.RestartNodeWithIdentity(2, fresh); err != nil {
		t.Fatalf("restart node3 under fresh identity: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	waitConverged(t, cluster, numRows, reconvergeTimeout)
	if got := cluster.Node(2).NodeID(); got != fresh {
		t.Fatalf("node3 identity must be the fresh one")
	}
	if got := cluster.NodeID(2); got != fresh {
		t.Fatalf("harness node id must track")
	}

	// Post-rejoin write from a survivor proves the restored node is a
	// live replication peer again (its fresh origin key is unknown to
	// the survivors, so it must not originate yet — see
	// RestartNodeWithIdentity).
	typedInsert(t, cluster.Node(0), "backup-row-post-rejoin")
	waitConverged(t, cluster, numRows+1, reconvergeTimeout)
	t.Log("PASS: backup-restore rejoin reconverged with no lost rows")
	failed = false
}

func TestRestoreRejectsStaleIdentity(t *testing.T) {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatalf("key entropy: %v", err)
	}
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "backup-restore-id",
		NumNodes: 1,
		Key:      key,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("backup-restore-stale-id")
		}
		cluster.Finish(failed)
	}()
	migrateAll(t, cluster, namesDDL)
	typedInsert(t, cluster.Node(0), "backup-stale-identity")
	oldID := cluster.NodeID(0)
	cluster.StopNode(0)
	backupDir := backupStoppedNode(t, cluster, key, 0, "node1-backup")

	staging := cluster.RuntimeDir() + "/staging"
	fresh := freshNodeID(t, oldID)
	if err := murmur.RestoreWithOptions(backupDir, staging, key[:], murmur.RestoreOptions{NewNodeID: &fresh}); err != nil {
		t.Fatalf("restore backup under fresh identity: %v", err)
	}

	// Opening under the OLD identity must be rejected (rollback safety).
	databaseID := cluster.DatabaseID()
	badOpts := murmur.DefaultOptions()
	badOpts.Mode = murmur.OpenExisting
	badOpts.DatabaseID = &databaseID
	badOpts.NodeID = &oldID
	if stale, err := murmur.Open(staging, key[:], badOpts); err != nil {
		t.Logf("stale identity rejected: %v", err)
	} else {
		_ = stale.Close()
		t.Fatalf("open under stale identity succeeded, want rejection")
	}

	// Opening under the fresh identity succeeds with the row intact.
	goodOpts := murmur.DefaultOptions()
	goodOpts.Mode = murmur.OpenExisting
	goodOpts.DatabaseID = &databaseID
	goodOpts.NodeID = &fresh
	restored, err := murmur.Open(staging, key[:], goodOpts)
	if err != nil {
		t.Fatalf("open under fresh identity: %v", err)
	}
	if got := restored.NodeID(); got != fresh {
		_ = restored.Close()
		t.Fatalf("restored node id mismatch")
	}
	if got, want := typedNames(t, restored), []string{"backup-stale-identity"}; !slices.Equal(got, want) {
		_ = restored.Close()
		t.Fatalf("restored names = %v, want %v", got, want)
	}
	if err := restored.Close(); err != nil {
		t.Fatalf("close restored handle: %v", err)
	}
	t.Log("PASS: stale identity rejected, fresh identity opens")
	failed = false
}

// typedRowInsert is the typed-leg insert (Rust: Cluster.TypedInsert on the
// typed record table).
func typedRowInsert(t *testing.T, db *murmur.Database, name string) {
	t.Helper()
	// Extrema baselines must sit outside the op range: max raises 0 -> 7
	// and min lowers 9.5 -> 1.5. A 0.0 floor baseline would be a real
	// minimum (min over {0.0, 1.5, 4.5} is 0.0), masking the min ops.
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute(
			"INSERT INTO live_typed_records(name, title, counter, tags, peak, floor) VALUES(?, '', 0, x'00', 0, 9.5)",
			name,
		)
		return nil, err
	}); err != nil {
		t.Fatalf("typed row insert failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after typed row insert: %v", err)
	}
}

// counterAdd is a PN_COUNTER op (Rust: counter field write on the typed
// record).
func counterAdd(t *testing.T, db *murmur.Database, name string, delta int64) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		return nil, s.CounterAdd(table, []murmur.Value{murmur.TextValue(name)}, "counter", delta)
	}); err != nil {
		t.Fatalf("counter add failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after counter add: %v", err)
	}
}

// setAdd is an OR_SET add (Rust: tags field write on the typed record).
func setAdd(t *testing.T, db *murmur.Database, name, value string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		return nil, s.SetAdd(table, []murmur.Value{murmur.TextValue(name)}, "tags", murmur.TextValue(value))
	}); err != nil {
		t.Fatalf("set add failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after set add: %v", err)
	}
}

// extremaUpdate is a numeric MIN/MAX op (Rust: peak/floor field writes on
// the typed record).
func extremaUpdate(t *testing.T, db *murmur.Database, name string, peak int64, floor float64) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		if err := s.Max(table, []murmur.Value{murmur.TextValue(name)}, "peak", murmur.IntValue(peak)); err != nil {
			return nil, err
		}
		return nil, s.Min(table, []murmur.Value{murmur.TextValue(name)}, "floor", murmur.RealValue(floor))
	}); err != nil {
		t.Fatalf("extrema update failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after extrema update: %v", err)
	}
}

// typedProjection is the full typed projection (Rust: RIME materializer
// read-back of the record).
type typedProjection struct {
	title   string
	counter int64
	set     []string
	peak    int64
	floor   float64
}

func readTypedProjection(t *testing.T, db *murmur.Database, name string) typedProjection {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT title, counter, tags, peak, floor FROM live_typed_records WHERE name = ?", name)
	})
	if err != nil {
		t.Fatalf("read typed projection: %v", err)
	}
	row := v.([][]sqlite.Value)[0]
	values, err := murmur.DecodeValues(row[2].Blob)
	if err != nil {
		t.Fatalf("decode set projection: %v", err)
	}
	var set []string
	for _, value := range values {
		if value.Tag == murmur.ValueText {
			set = append(set, value.S)
		}
	}
	slices.Sort(set)
	return typedProjection{title: row[0].Str, counter: row[1].Int, set: set, peak: row[3].Int, floor: row[4].Float}
}

func TestTypedBackupRestoreRoundTripLive(t *testing.T) {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatalf("key entropy: %v", err)
	}
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:        "typed-backup-restore",
		NumNodes:    1,
		Key:         key,
		Replication: &harness.ReplicationOptions{MergePolicies: mergePolicies()},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("typed-backup-restore")
		}
		cluster.Finish(failed)
	}()
	migrateAll(t, cluster, typedDDL)
	typedRowInsert(t, cluster.Node(0), "encrypted-live-backup")
	counterAdd(t, cluster.Node(0), "encrypted-live-backup", 20)
	counterAdd(t, cluster.Node(0), "encrypted-live-backup", 22)
	setAdd(t, cluster.Node(0), "encrypted-live-backup", "z")
	setAdd(t, cluster.Node(0), "encrypted-live-backup", "aa")
	extremaUpdate(t, cluster.Node(0), "encrypted-live-backup", 7, 1.5)
	extremaUpdate(t, cluster.Node(0), "encrypted-live-backup", 3, 4.5)
	want := readTypedProjection(t, cluster.Node(0), "encrypted-live-backup")
	if want.counter != 42 {
		t.Fatalf("counter must accumulate before backup, got %d", want.counter)
	}
	if !slices.Equal(want.set, []string{"aa", "z"}) {
		t.Fatalf("set before backup = %v", want.set)
	}
	if want.peak != 7 || want.floor != 1.5 {
		t.Fatalf("extrema before backup = (%d,%v)", want.peak, want.floor)
	}

	cluster.StopNode(0)
	backupDir := backupStoppedNode(t, cluster, key, 0, "typed-backup")

	var fresh [16]byte
	if _, err := rand.Read(fresh[:]); err != nil {
		t.Fatalf("fresh node id entropy: %v", err)
	}
	target := cluster.RuntimeDir() + "/restored"
	if err := murmur.RestoreWithOptions(backupDir, target, key[:], murmur.RestoreOptions{NewNodeID: &fresh}); err != nil {
		t.Fatalf("restore typed backup under fresh identity: %v", err)
	}

	databaseID := cluster.DatabaseID()
	opts := murmur.DefaultOptions()
	opts.Mode = murmur.OpenExisting
	opts.DatabaseID = &databaseID
	opts.NodeID = &fresh
	clone, err := murmur.Open(target, key[:], opts)
	if err != nil {
		t.Fatalf("open restored typed backup under fresh identity: %v", err)
	}
	got := readTypedProjection(t, clone, "encrypted-live-backup")
	if got.title != want.title || got.counter != want.counter || !slices.Equal(got.set, want.set) ||
		got.peak != want.peak || got.floor != want.floor {
		_ = clone.Close()
		t.Fatalf("restored typed record must match original: got %+v want %+v", got, want)
	}
	if err := clone.Close(); err != nil {
		t.Fatalf("close restored handle: %v", err)
	}
	t.Log("PASS: typed backup-restore round trip preserved the record")
	failed = false
}

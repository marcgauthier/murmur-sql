// Live scenario: typed records replicate, migrate, and survive restarts.
//
// Go port of the Rust `typed-records` scenario
// (/home/marc/MURMUR/tests-live/typed-records/tests/typed_records.rs).
// Two nodes share one typed table; inserts, PN_COUNTER / OR_SET / MIN /
// MAX policy ops, explicit transactions, typed subscriptions, operator GC,
// schema migration, offline CRDT convergence, snapshot catch-up,
// schema-branch merge, typed files, and node-local rows are exercised
// across seven tests (one cluster each, distinct runtime/ names so parallel
// execution stays isolated).
//
// Plain SQL plus policy ops replace the managed typed HTTP API of the
// original Go scenario (the product has no HTTP server).
package typedrecords

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// table is the typed table (Rust: live_typed_records with
// counter/set/extrema fields).
const table = "live_typed_records"

// baseDDL is the base DDL. counter/tags/peak/floor are policy columns (see
// mergePolicies); title is a plain LWW field for the rename leg. Set
// projections start as x'00' (postcard empty vec), the convention for fresh
// set cells.
const baseDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, counter INTEGER NOT NULL, tags BLOB NOT NULL, peak INTEGER NOT NULL, floor REAL NOT NULL)"

// privateDDL is the node-local table (Rust: TableScopeNodeLocal;
// replication disabled).
const privateDDL = "CREATE TABLE typed_local_private(name TEXT NOT NULL PRIMARY KEY, v INTEGER NOT NULL)"

const (
	// wait is the convergence deadline (Rust: 30 s waits).
	wait = 30 * time.Second
	// poll is the convergence poll interval (Rust: 25-100 ms polls).
	poll = 50 * time.Millisecond
)

// mergePolicies maps (table, column) -> merge policy for the typed table
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
func migrateAll(t *testing.T, cluster *harness.Cluster, ddl, tableName string) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, ddl); err != nil {
			t.Fatalf("node%d migrate %q failed: %v", i+1, ddl, err)
		}
	}
	// Opt in after every node migrated: opting in publishes the schema
	// immediately, and an early publication lets a peer adopt the table
	// before its own migrate runs ("table already exists"). The private
	// table is explicitly disabled again by its scenario right after this.
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication(tableName, true); err != nil {
			t.Fatalf("node%d opt-in %s failed: %v", i+1, tableName, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush after opt-in failed: %v", i+1, err)
		}
	}
}

// insertRow is a typed insert on one node (Rust: Cluster.TypedInsert).
func insertRow(t *testing.T, db *murmur.Database, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute(
			"INSERT INTO live_typed_records(name, title, counter, tags, peak, floor) VALUES(?, '', 0, x'00', 0, 0.0)",
			name,
		)
		return nil, err
	}); err != nil {
		t.Fatalf("typed insert failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after typed insert: %v", err)
	}
}

// typedCount is the row count by name (Rust: Cluster.TypedCount).
func typedCount(t *testing.T, db *murmur.Database, name string) int64 {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT count(*) FROM live_typed_records WHERE name = ?", name)
	})
	if err != nil {
		t.Fatalf("read typed count: %v", err)
	}
	return v.([][]sqlite.Value)[0][0].Int
}

// counterValue is the PN_COUNTER projection (Rust: TypedCounterValue).
func counterValue(t *testing.T, db *murmur.Database, name string) int64 {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT counter FROM live_typed_records WHERE name = ?", name)
	})
	if err != nil {
		t.Fatalf("read counter: %v", err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 {
		return -1 << 62
	}
	return rows[0][0].Int
}

// setValues is the OR_SET projection, sorted (Rust: TypedSetValues).
func setValues(t *testing.T, db *murmur.Database, name string) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT tags FROM live_typed_records WHERE name = ?", name)
	})
	if err != nil {
		t.Fatalf("read set: %v", err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 {
		return []string{"<read-error>"}
	}
	values, err := murmur.DecodeValues(rows[0][0].Blob)
	if err != nil {
		return []string{"<read-error>"}
	}
	var out []string
	for _, value := range values {
		if value.Tag == murmur.ValueText {
			out = append(out, value.S)
		}
	}
	slices.Sort(out)
	return out
}

// extrema is the numeric extrema projection (Rust: TypedExtremaValues).
func extrema(t *testing.T, db *murmur.Database, name string) (int64, float64) {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT peak, floor FROM live_typed_records WHERE name = ?", name)
	})
	if err != nil {
		t.Fatalf("read extrema: %v", err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 {
		return -1 << 62, 0
	}
	return rows[0][0].Int, rows[0][1].Float
}

// counterAdd is a PN_COUNTER op (Rust: TypedCounterAdd).
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

// setAdd is an OR_SET add (Rust: TypedSetAdd).
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

// setRemove is an OR_SET remove (Rust: TypedSetRemove).
func setRemove(t *testing.T, db *murmur.Database, name, value string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		return nil, s.SetRemove(table, []murmur.Value{murmur.TextValue(name)}, "tags", murmur.TextValue(value))
	}); err != nil {
		t.Fatalf("set remove failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after set remove: %v", err)
	}
}

// extremaUpdate is a numeric MIN/MAX op (Rust: TypedExtremaUpdate).
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

// updateNote is a nullable note column write (Rust: TypedSetNote).
func updateNote(t *testing.T, db *murmur.Database, name, note string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("UPDATE live_typed_records SET note = ? WHERE name = ?", note, name)
		return nil, err
	}); err != nil {
		t.Fatalf("note update failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after note update: %v", err)
	}
}

// updateRegion is a nullable region column write (Rust: TypedSetRegion).
func updateRegion(t *testing.T, db *murmur.Database, name, region string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("UPDATE live_typed_records SET region = ? WHERE name = ?", region, name)
		return nil, err
	}); err != nil {
		t.Fatalf("region update failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after region update: %v", err)
	}
}

// updateTitle is a known-field update from the older peer (Rust:
// TypedRename, applied to the LWW title field so the primary-key row
// identity is preserved).
func updateTitle(t *testing.T, db *murmur.Database, name, title string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("UPDATE live_typed_records SET title = ? WHERE name = ?", title, name)
		return nil, err
	}); err != nil {
		t.Fatalf("title update failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after title update: %v", err)
	}
}

// titleCount is the row count by title.
func titleCount(t *testing.T, db *murmur.Database, title string) int64 {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT count(*) FROM live_typed_records WHERE title = ?", title)
	})
	if err != nil {
		t.Fatalf("read title count: %v", err)
	}
	return v.([][]sqlite.Value)[0][0].Int
}

// noteValue is the note field read (Rust: TypedNote).
func noteValue(t *testing.T, db *murmur.Database, name string) string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT note FROM live_typed_records WHERE name = ?", name)
	})
	if err != nil {
		t.Fatalf("read note: %v", err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 || rows[0][0].Type == sqlite.Null {
		return "<missing>"
	}
	return rows[0][0].Str
}

// branchValues is the merged branch fields read (Rust: TypedBranchValues).
func branchValues(t *testing.T, db *murmur.Database, name string) (string, string) {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT note, region FROM live_typed_records WHERE name = ?", name)
	})
	if err != nil {
		t.Fatalf("read branch values: %v", err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 {
		return "<missing>", "<missing>"
	}
	note, region := "<missing>", "<missing>"
	if rows[0][0].Type != sqlite.Null {
		note = rows[0][0].Str
	}
	if rows[0][1].Type != sqlite.Null {
		region = rows[0][1].Str
	}
	return note, region
}

// hasColumn reports column presence (Rust: schema-manifest epoch
// assertions).
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

// schemaAdoptions is the schema-adoption metric (Rust: epoch advance on the
// adopting peer).
func schemaAdoptions(cluster *harness.Cluster, idx int) uint64 {
	h := cluster.Node(idx).Replication()
	if h == nil {
		return 0
	}
	return h.Status().Metrics.SchemaAdoptions
}

// logCounters are the log-path activity counters (received, applied):
// snapshot application bypasses both, so a flat pair across a resync proves
// the snapshot path (Rust: metrics endpoint).
func logCounters(cluster *harness.Cluster, idx int) (uint64, uint64) {
	m := cluster.Node(idx).Replication().Status().Metrics
	return m.ReceivedTransactions, m.AppliedTransactions
}

// snapshotRowCount is the snapshot-leg row count (Rust: reconstructed RIME
// record count).
func snapshotRowCount(t *testing.T, db *murmur.Database) int64 {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT count(*) FROM live_typed_records WHERE name LIKE 'snapshot-row-%'")
	})
	if err != nil {
		t.Fatalf("read snapshot row count: %v", err)
	}
	return v.([][]sqlite.Value)[0][0].Int
}

// privateInsert is a node-local write (Rust: TypedLocalInsert).
func privateInsert(t *testing.T, db *murmur.Database, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO typed_local_private(name, v) VALUES(?, 1)", name)
		return nil, err
	}); err != nil {
		t.Fatalf("node-local insert failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after node-local insert: %v", err)
	}
}

// privateCount is the node-local row count (Rust: TypedLocalCount).
func privateCount(t *testing.T, db *murmur.Database, name string) int64 {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT count(*) FROM typed_local_private WHERE name = ?", name)
	})
	if err != nil {
		t.Fatalf("read node-local count: %v", err)
	}
	return v.([][]sqlite.Value)[0][0].Int
}

func waitCount(t *testing.T, cluster *harness.Cluster, node int, name string, want int64, what string) {
	t.Helper()
	ok := harness.WaitUntil(wait, poll, func() bool {
		return typedCount(t, cluster.Node(node), name) == want
	})
	if !ok {
		t.Fatalf("node%d %s count for %q = %d, want %d", node+1, what, name, typedCount(t, cluster.Node(node), name), want)
	}
}

func waitCounter(t *testing.T, cluster *harness.Cluster, node int, name string, want int64) {
	t.Helper()
	ok := harness.WaitUntil(wait, poll, func() bool {
		return counterValue(t, cluster.Node(node), name) == want
	})
	if !ok {
		t.Fatalf("node%d counter for %q = %d, want %d", node+1, name, counterValue(t, cluster.Node(node), name), want)
	}
}

func waitSet(t *testing.T, cluster *harness.Cluster, node int, name string, want []string) {
	t.Helper()
	ok := harness.WaitUntil(wait, poll, func() bool {
		return slices.Equal(setValues(t, cluster.Node(node), name), want)
	})
	if !ok {
		t.Fatalf("node%d set for %q = %v, want %v", node+1, name, setValues(t, cluster.Node(node), name), want)
	}
}

func waitExtrema(t *testing.T, cluster *harness.Cluster, node int, name string, peak int64, floor float64) {
	t.Helper()
	ok := harness.WaitUntil(wait, poll, func() bool {
		gotPeak, gotFloor := extrema(t, cluster.Node(node), name)
		return gotPeak == peak && gotFloor == floor
	})
	if !ok {
		gotPeak, gotFloor := extrema(t, cluster.Node(node), name)
		t.Fatalf("node%d extrema for %q = (%d,%v), want (%d,%v)", node+1, name, gotPeak, gotFloor, peak, floor)
	}
}

func waitTitleCount(t *testing.T, cluster *harness.Cluster, node int, title string, want int64) {
	t.Helper()
	ok := harness.WaitUntil(wait, poll, func() bool {
		return titleCount(t, cluster.Node(node), title) == want
	})
	if !ok {
		t.Fatalf("node%d title count for %q = %d, want %d", node+1, title, titleCount(t, cluster.Node(node), title), want)
	}
}

func waitNote(t *testing.T, cluster *harness.Cluster, node int, name, want string) {
	t.Helper()
	ok := harness.WaitUntil(wait, poll, func() bool {
		return noteValue(t, cluster.Node(node), name) == want
	})
	if !ok {
		t.Fatalf("node%d note for %q = %q, want %q", node+1, name, noteValue(t, cluster.Node(node), name), want)
	}
}

func waitBranchValues(t *testing.T, cluster *harness.Cluster, node int, name, wantNote, wantRegion string) {
	t.Helper()
	ok := harness.WaitUntil(wait, poll, func() bool {
		gotNote, gotRegion := branchValues(t, cluster.Node(node), name)
		return gotNote == wantNote && gotRegion == wantRegion
	})
	if !ok {
		gotNote, gotRegion := branchValues(t, cluster.Node(node), name)
		t.Fatalf("node%d merged branch values for %q = (%q,%q), want (%q,%q)",
			node+1, name, gotNote, gotRegion, wantNote, wantRegion)
	}
}

// assertNoFilesystemEscape fails when marker appears in any filesystem path
// under root (Rust: filepath.WalkDir escape scan after the
// traversal-shaped round-trip).
func assertNoFilesystemEscape(t *testing.T, root, marker string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.Contains(d.Name(), marker) {
			t.Fatalf("typed traversal-shaped name escaped into filesystem path: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan %s: %v", root, err)
	}
}

// typedCluster is the shared cluster setup: two nodes, typed merge
// policies, manual peers so the local migrate wins over schema
// auto-adoption (identical DDL converges either way, but peering first
// would race adoption against migrate).
func typedCluster(t *testing.T, name string) *harness.Cluster {
	t.Helper()
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:        name,
		NumNodes:    2,
		ManualPeers: true,
		Replication: &harness.ReplicationOptions{MergePolicies: mergePolicies()},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	return cluster
}

// waitSubscriptionRow waits until the peer subscription observes the
// replicated row (Rust: watch.Next). A quiet 5 s window reports Busy; that
// only retries until the outer deadline (a single window must not fail the
// wait).
func waitSubscriptionRow(t *testing.T, sub *murmur.Subscription[string]) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		if !time.Now().Before(deadline) {
			t.Fatalf("peer subscription never observed the replicated row")
		}
		ev, err := sub.RecvTimeout(5 * time.Second)
		if err != nil {
			if errors.Is(err, murmur.ErrBusy) {
				continue
			}
			t.Fatalf("subscription update: %v", err)
		}
		switch ev.Kind {
		case murmur.SubUpdate:
			if len(ev.Rows) == 1 {
				return
			}
		case murmur.SubReset:
			t.Fatalf("subscription reset while waiting for the row: %s", ev.Reason)
		case murmur.SubInitial:
		}
	}
}

func TestTypedRecordReplicatesAndRebuildsAcrossRestart(t *testing.T) {
	cluster := typedCluster(t, "typed-records")
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("typed-record-no-convergence")
		}
		cluster.Finish(failed)
	}()
	migrateAll(t, cluster, baseDDL, table)
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("initial mesh: %v", err)
	}

	// The schema manifest carries the typed table (Rust: /v1/schema).
	schema, err := cluster.Node(0).Schema()
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	found := false
	for _, tbl := range schema.Tables {
		if tbl.Name == table {
			found = true
			break
		}
	}
	if !found {
		var names []string
		for _, tbl := range schema.Tables {
			names = append(names, tbl.Name)
		}
		t.Fatalf("schema manifest lacks the typed table: %v", names)
	}

	// Typed subscription on the peer (Rust: StartTypedWatch).
	sub, err := murmur.SubscribeQuery(cluster.Node(1),
		func(s *murmur.Sql) ([]string, error) {
			rows, err := s.Query("SELECT name FROM live_typed_records WHERE name = 'native-rime-write'")
			if err != nil {
				return nil, err
			}
			out := make([]string, 0, len(rows))
			for _, row := range rows {
				out = append(out, row[0].Str)
			}
			return out, nil
		},
		func(name *string) []byte { return []byte(*name) },
		murmur.SubscriptionOptions{BufferSize: 32, Tables: []string{table}},
	)
	if err != nil {
		t.Fatalf("typed subscription: %v", err)
	}
	defer sub.Close()
	ev, err := sub.RecvTimeout(30 * time.Second)
	if err != nil {
		t.Fatalf("initial subscription event: %v", err)
	}
	if ev.Kind != murmur.SubInitial {
		t.Fatalf("expected initial subscription event, got kind %d", ev.Kind)
	}
	if len(ev.Rows) != 0 {
		t.Fatalf("initial count = %d, want 0", len(ev.Rows))
	}

	insertRow(t, cluster.Node(0), "native-rime-write")

	// Operator-triggered GC (Rust: TriggerGC through the mTLS admin API).
	collected, err := cluster.Node(0).CollectRetention()
	if err != nil {
		t.Fatalf("operator-triggered GC: %v", err)
	}
	fmt.Fprintf(os.Stderr, "operator GC collected %d transaction records\n", collected)

	// The peer subscription observes the remote insert.
	waitSubscriptionRow(t, sub)
	waitCount(t, cluster, 1, "native-rime-write", 1, "replicated")

	// An explicit transaction commits while an explicitly rolled-back row
	// stays absent on both nodes (Rust: TypedExplicitTransaction).
	insertRow(t, cluster.Node(0), "explicit-lifecycle")
	if _, err := cluster.Node(0).Transaction(func(s *murmur.Sql) (any, error) {
		if _, err := s.Execute("INSERT INTO live_typed_records(name, title, counter, tags, peak, floor) VALUES('explicit-lifecycle-rolled-back', '', 0, x'00', 0, 0.0)"); err != nil {
			return nil, err
		}
		return nil, errors.New("rollback probe")
	}); err == nil {
		t.Fatalf("rolled-back transaction unexpectedly committed")
	}
	waitCount(t, cluster, 1, "explicit-lifecycle", 1, "explicit-tx")
	if got := typedCount(t, cluster.Node(0), "explicit-lifecycle-rolled-back"); got != 0 {
		t.Fatalf("rolled-back row visible on its writer")
	}
	if got := typedCount(t, cluster.Node(1), "explicit-lifecycle-rolled-back"); got != 0 {
		t.Fatalf("rolled-back row replicated to the peer")
	}

	// PN_COUNTER converges (Rust: TypedCounterAdd +20).
	counterAdd(t, cluster.Node(0), "native-rime-write", 20)
	waitCounter(t, cluster, 1, "native-rime-write", 20)

	// OR_SET add/add/remove converges to {"aa"}.
	setAdd(t, cluster.Node(0), "native-rime-write", "z")
	setAdd(t, cluster.Node(0), "native-rime-write", "aa")
	setRemove(t, cluster.Node(0), "native-rime-write", "z")
	waitSet(t, cluster, 1, "native-rime-write", []string{"aa"})

	// Numeric MIN/MAX converge (Rust: TypedExtremaUpdate 18/-3).
	extremaUpdate(t, cluster.Node(0), "native-rime-write", 18, -3.0)
	waitExtrema(t, cluster, 1, "native-rime-write", 18, -3.0)

	// The restarted peer rebuilds the record, counter, set, and extrema
	// from durable state (Rust: StopNode/StartNode + waits).
	cluster.StopNode(1)
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart peer: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	waitCount(t, cluster, 1, "native-rime-write", 1, "post-restart")
	waitCounter(t, cluster, 1, "native-rime-write", 20)
	waitSet(t, cluster, 1, "native-rime-write", []string{"aa"})
	waitExtrema(t, cluster, 1, "native-rime-write", 18, -3.0)
	failed = false
}

func TestTypedSchemaMigrationAdoptedByOlderPeerAndRetainsNewFields(t *testing.T) {
	cluster := typedCluster(t, "typed-schema-evolution")
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("typed-migration-no-adoption")
		}
		cluster.Finish(failed)
	}()
	migrateAll(t, cluster, baseDDL, table)
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("initial mesh: %v", err)
	}
	insertRow(t, cluster.Node(0), "schema-evolution-row")
	waitCount(t, cluster, 1, "schema-evolution-row", 1, "seed")

	// Runtime additive migration on the newer node (Rust:
	// MigrateTypedRecords adds the Note field).
	if err := cluster.Node(0).Migrate(murmur.NewMigration("ALTER TABLE live_typed_records ADD COLUMN note TEXT")); err != nil {
		t.Fatalf("additive typed migration: %v", err)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush migration: %v", err)
	}

	// The older peer adopts the manifest (Rust: epoch 2 on both nodes).
	if ok := harness.WaitUntil(wait, poll, func() bool {
		return hasColumn(t, cluster.Node(1), table, "note")
	}); !ok {
		t.Fatalf("older peer never adopted the additive typed column")
	}
	// The adoption counter lags the DDL: the column lands at DDL commit
	// while the counter increments after a later spool commit recording
	// the acceptance, so poll it (a bare assert flakes).
	if ok := harness.WaitUntil(wait, poll, func() bool {
		return schemaAdoptions(cluster, 1) >= 1
	}); !ok {
		t.Fatalf("older peer adopted the column without recording a schema adoption")
	}

	// The new field is written from the newer node while a known field is
	// updated from the older peer (Rust: TypedSetNote + TypedRename).
	updateNote(t, cluster.Node(0), "schema-evolution-row", "new-field-survives")
	updateTitle(t, cluster.Node(1), "schema-evolution-row", "schema-evolution-renamed")
	waitTitleCount(t, cluster, 0, "schema-evolution-renamed", 1)
	waitTitleCount(t, cluster, 1, "schema-evolution-renamed", 1)
	waitNote(t, cluster, 0, "schema-evolution-row", "new-field-survives")
	waitNote(t, cluster, 1, "schema-evolution-row", "new-field-survives")

	// The new field survives the older peer's restart (Rust:
	// StopNode/StartNode + count/epoch/note waits).
	cluster.StopNode(1)
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart older peer: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	waitTitleCount(t, cluster, 1, "schema-evolution-renamed", 1)
	if !hasColumn(t, cluster.Node(1), table, "note") {
		t.Fatalf("restarted peer lost the adopted typed column")
	}
	waitNote(t, cluster, 1, "schema-evolution-row", "new-field-survives")
	failed = false
}

func TestTypedCRDTWritesConvergeAfterOfflinePartition(t *testing.T) {
	cluster := typedCluster(t, "typed-offline-crdt")
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("typed-offline-no-convergence")
		}
		cluster.Finish(failed)
	}()
	migrateAll(t, cluster, baseDDL, table)

	// Both nodes independently write one shared record while partitioned
	// (Rust: TypedInsertWithID with one row id on both nodes).
	insertRow(t, cluster.Node(0), "offline-shared-row")
	insertRow(t, cluster.Node(1), "offline-shared-row")
	counterAdd(t, cluster.Node(0), "offline-shared-row", 7)
	counterAdd(t, cluster.Node(1), "offline-shared-row", 11)
	setAdd(t, cluster.Node(0), "offline-shared-row", "from-node-0")
	setAdd(t, cluster.Node(1), "offline-shared-row", "from-node-1")

	if err := cluster.Mesh(); err != nil {
		t.Fatalf("connect partitions: %v", err)
	}
	for node := 0; node < cluster.Len(); node++ {
		waitCount(t, cluster, node, "offline-shared-row", 1, "offline row")
		waitCounter(t, cluster, node, "offline-shared-row", 18)
		waitSet(t, cluster, node, "offline-shared-row", []string{"from-node-0", "from-node-1"})
	}
	failed = false
}

func TestTypedEmptyPeerCatchesUpThroughSnapshot(t *testing.T) {
	var node1, node2 [16]byte
	for i := range node1 {
		node1[i] = 1
		node2[i] = 2
	}
	retention := time.Millisecond
	minimumRetained := uint64(1)
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "typed-snapshot-catchup",
		NumNodes: 2,
		NodeIDs:  [][16]byte{node1, node2},
		Replication: &harness.ReplicationOptions{
			Retention:       &retention,
			MinimumRetained: &minimumRetained,
		},
		ManualPeers: true,
		// The empty receiver trusts the source as its snapshot donor
		// (Rust fixture policy trusts all nodes; the harness trusts
		// nobody).
		TrustedSnapshotSources: map[int]map[[16]byte]bool{1: {node1: true}},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("typed-snapshot-no-catchup")
		}
		cluster.Finish(failed)
	}()
	migrateAll(t, cluster, baseDDL, table)
	for i := 0; i < 12; i++ {
		insertRow(t, cluster.Node(0), fmt.Sprintf("snapshot-row-%02d", i))
	}
	// Let the configured log retention elapse, then collect old batches
	// before the empty peer is admitted, so the receiver must use a
	// snapshot rather than ordinary log catch-up (Rust: sleep +
	// TriggerGC).
	time.Sleep(1100 * time.Millisecond)
	pruned, err := cluster.Node(0).CollectRetention()
	if err != nil {
		t.Fatalf("pre-admission source GC: %v", err)
	}
	fmt.Fprintf(os.Stderr, "pre-admission GC pruned %d transaction records\n", pruned)
	beforeReceived, beforeApplied := logCounters(cluster, 1)
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("admit empty peer: %v", err)
	}
	if ok := harness.WaitUntil(wait, poll, func() bool {
		return snapshotRowCount(t, cluster.Node(1)) == 12
	}); !ok {
		t.Fatalf("empty peer reconstructed %d of 12 typed rows", snapshotRowCount(t, cluster.Node(1)))
	}
	// Flat log counters prove snapshot transfer rather than log catch-up
	// (Rust: snapshots-received metric >= 1; the established
	// flat-counters proof stands in).
	afterReceived, afterApplied := logCounters(cluster, 1)
	if beforeReceived != afterReceived || beforeApplied != afterApplied {
		t.Fatalf("receiver advanced via log catch-up (%d,%d) -> (%d,%d), want flat counters proving snapshot transfer",
			beforeReceived, beforeApplied, afterReceived, afterApplied)
	}
	if cluster.Node(1).Status().Committed == 0 {
		t.Fatalf("receiver commit cursor never advanced past the snapshot")
	}
	failed = false
}

func TestTypedConcurrentSchemaBranchesMergeAcrossPartition(t *testing.T) {
	cluster := typedCluster(t, "typed-schema-branches")
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("typed-branches-no-merge")
		}
		cluster.Finish(failed)
	}()
	migrateAll(t, cluster, baseDDL, table)
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("initial mesh: %v", err)
	}
	insertRow(t, cluster.Node(0), "branched-record")
	waitCount(t, cluster, 1, "branched-record", 1, "seed")

	if err := cluster.RemovePeer(0, 1); err != nil {
		t.Fatalf("partition node 0: %v", err)
	}
	if err := cluster.RemovePeer(1, 0); err != nil {
		t.Fatalf("partition node 1: %v", err)
	}
	// Each node adds a different compatible field while partitioned
	// (Rust: MigrateTypedRecords Note branch vs Region branch).
	if err := cluster.Node(0).Migrate(murmur.NewMigration("ALTER TABLE live_typed_records ADD COLUMN note TEXT")); err != nil {
		t.Fatalf("Note schema branch: %v", err)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush Note branch: %v", err)
	}
	if err := cluster.Node(1).Migrate(murmur.NewMigration("ALTER TABLE live_typed_records ADD COLUMN region TEXT")); err != nil {
		t.Fatalf("Region schema branch: %v", err)
	}
	if err := cluster.Node(1).Flush(); err != nil {
		t.Fatalf("flush Region branch: %v", err)
	}
	// Independent branches before reconnect (Rust: /v1/schema manifests).
	if !hasColumn(t, cluster.Node(0), table, "note") || hasColumn(t, cluster.Node(0), table, "region") {
		t.Fatalf("node 1 did not hold its independent Note branch")
	}
	if !hasColumn(t, cluster.Node(1), table, "region") || hasColumn(t, cluster.Node(1), table, "note") {
		t.Fatalf("node 2 did not hold its independent Region branch")
	}
	updateNote(t, cluster.Node(0), "branched-record", "note-from-node-0")
	updateRegion(t, cluster.Node(1), "branched-record", "region-from-node-1")

	if err := cluster.Mesh(); err != nil {
		t.Fatalf("rejoin: %v", err)
	}
	// Both nodes publish the merged schema (Rust: epoch 3 on both nodes;
	// the legacy mode mutually adopts additive manifests).
	for node := 0; node < cluster.Len(); node++ {
		if ok := harness.WaitUntil(wait, poll, func() bool {
			return hasColumn(t, cluster.Node(node), table, "note") && hasColumn(t, cluster.Node(node), table, "region")
		}); !ok {
			t.Fatalf("node%d never converged on the merged typed schema", node+1)
		}
	}
	// Both field values survive on the union record type (Rust: V4 bind +
	// branch-value waits; the union columns themselves are the assertion
	// surface).
	for node := 0; node < cluster.Len(); node++ {
		waitBranchValues(t, cluster, node, "branched-record", "note-from-node-0", "region-from-node-1")
	}
	failed = false
}

func TestTypedFilesReplicateAndFetchWithoutSQL(t *testing.T) {
	var fileKey [32]byte
	for i := range fileKey {
		fileKey[i] = 0x44
	}
	fetchInterval := 100 * time.Millisecond
	maxFileBytes := uint64(1 << 20)
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "typed-files",
		NumNodes: 2,
		Files: &harness.FilesOptions{
			Key:           fileKey,
			FetchInterval: &fetchInterval,
			MaxFileBytes:  &maxFileBytes,
		},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("typed-files-no-fetch")
		}
		cluster.Finish(failed)
	}()

	name := "typed/report.bin"
	want := []byte("file metadata uses Spool while bytes are fetched from the peer")
	if _, err := cluster.Node(0).UploadFile(name, bytes.NewReader(want)); err != nil {
		t.Fatalf("typed node upload: %v", err)
	}
	// Spool metadata replicates first (Rust: WaitFileAvailable
	// meta=false).
	if ok := harness.WaitUntil(wait, poll, func() bool {
		meta, err := cluster.Node(1).FileMetadata(name)
		return err == nil && meta != nil
	}); !ok {
		t.Fatalf("replicated typed file metadata never arrived")
	}
	// The content-addressed object is fetched from the peer (Rust:
	// explicit FetchFile; the product fetches automatically on
	// fetch_interval).
	if ok := harness.WaitUntil(wait, poll, func() bool {
		available, err := cluster.Node(1).FileAvailable(name)
		return err == nil && available
	}); !ok {
		t.Fatalf("typed file object never became available")
	}
	var got bytes.Buffer
	if _, err := cluster.Node(1).GetFile(name, &got); err != nil {
		t.Fatalf("peer download: %v", err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("typed peer download bytes differ")
	}

	// A typed-node delete tombstone replicates (Rust: WaitFileDeleted).
	if err := cluster.Node(0).DeleteFile(name); err != nil {
		t.Fatalf("typed file delete: %v", err)
	}
	if ok := harness.WaitUntil(wait, poll, func() bool {
		meta, err := cluster.Node(1).FileMetadata(name)
		return err == nil && meta == nil
	}); !ok {
		t.Fatalf("replicated typed file tombstone never arrived")
	}

	// Traversal probes are rejected or treated as opaque object keys; none
	// escape into the filesystem (Rust: download/status/search probes).
	for _, traversal := range []string{
		"../../../../etc/passwd",
		"..\\..\\windows\\win.ini",
		"%2e%2e%2fetc%2fpasswd",
		"/etc/passwd",
		"typed/report.bin/../../etc/passwd",
		"..",
	} {
		var probe bytes.Buffer
		if _, err := cluster.Node(0).GetFile(traversal, &probe); err == nil && probe.Len() != 0 {
			t.Fatalf("traversal download %q returned %d bytes", traversal, probe.Len())
		}
		meta, err := cluster.Node(0).FileMetadata(traversal)
		if err != nil {
			t.Fatalf("traversal metadata probe: %v", err)
		}
		if meta != nil {
			t.Fatalf("traversal status %q reports an object: %+v", traversal, meta)
		}
		hits, err := cluster.Node(0).SearchFiles(traversal)
		if err != nil {
			t.Fatalf("traversal search probe: %v", err)
		}
		for _, hit := range hits {
			if hit.Name == traversal {
				t.Fatalf("traversal search %q reports an object", traversal)
			}
		}
		_ = cluster.Node(0).DeleteFile(traversal)
	}

	// A traversal-shaped name round-trips as an opaque key, then deletes.
	marker := "typed-file-traversal-marker"
	traversalName := "../../" + marker
	traversalBytes := []byte("typed file names stay opaque object keys")
	if _, err := cluster.Node(0).UploadFile(traversalName, bytes.NewReader(traversalBytes)); err != nil {
		t.Fatalf("upload traversal-shaped typed file name: %v", err)
	}
	var roundtrip bytes.Buffer
	if _, err := cluster.Node(0).GetFile(traversalName, &roundtrip); err != nil {
		t.Fatalf("traversal-shaped round-trip: %v", err)
	}
	if !bytes.Equal(roundtrip.Bytes(), traversalBytes) {
		t.Fatalf("traversal-shaped typed file round-trip differs")
	}
	if err := cluster.Node(0).DeleteFile(traversalName); err != nil {
		t.Fatalf("delete traversal-shaped typed file name: %v", err)
	}
	assertNoFilesystemEscape(t, cluster.RuntimeDir(), marker)
	failed = false
}

func TestTypedNodeLocalRowsPersistOnlyOnTheirNode(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:        "typed-node-local",
		NumNodes:    2,
		ManualPeers: true,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("typed-node-local-leak")
		}
		cluster.Finish(failed)
	}()
	migrateAll(t, cluster, baseDDL, table)
	migrateAll(t, cluster, privateDDL, "typed_local_private")
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("typed_local_private", false); err != nil {
			t.Fatalf("node%d disable private replication: %v", i+1, err)
		}
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("initial mesh: %v", err)
	}

	// A node-local write plus a replicated barrier row (Rust:
	// TypedLocalInsert + TypedInsert).
	privateInsert(t, cluster.Node(0), "private-node-zero")
	insertRow(t, cluster.Node(0), "replication-barrier")
	waitCount(t, cluster, 1, "replication-barrier", 1, "barrier")
	if got := privateCount(t, cluster.Node(0), "private-node-zero"); got != 1 {
		t.Fatalf("local node's private row missing")
	}
	if got := privateCount(t, cluster.Node(1), "private-node-zero"); got != 0 {
		t.Fatalf("peer received the node-local row")
	}
	failed = false
}

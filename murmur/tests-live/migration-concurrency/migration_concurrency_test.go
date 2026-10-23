// Live scenario: the same additive migration fires simultaneously on two
// meshed nodes while all keep writing.
//
// Go port of the Rust `migration-concurrency` scenario
// (/home/marc/MURMUR/tests-live/migration-concurrency/tests/migration_concurrency.rs).
// The SAME additive Note migration fires simultaneously on two of three
// meshed nodes while all keep writing. Both migrating nodes must land
// well-defined (old or new, never mixed), mixed-version replication must
// flow in both directions during the overlap, and after the straggler
// migrates the whole mesh converges fully migrated with identical counts,
// digests, and materialized new-column reads on every node. Plain SQL
// replaces the typed-records HTTP API.
package migrationconcurrency

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// table is the base table (Rust: typed records with a Name field).
const table = "live_typed_records"

// tableDDL is the base table DDL.
const tableDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY, counter INTEGER NOT NULL)"

// noteDDL is the additive migration DDL (Rust: MigrateTypedRecords
// registers Note).
const noteDDL = "ALTER TABLE live_typed_records ADD COLUMN note TEXT"

// seedEnv is the seed-row knob (Rust:
// MURMUR_MIGRATION_CONCURRENCY_SEED, default 60).
const seedEnv = "MURMUR_MIGRATION_CONCURRENCY_SEED"
const defaultSeed = 60

const (
	// writeTick is the background-writer cadence (Rust: 25 ms ticker).
	writeTick = 25 * time.Millisecond
	// seedTimeout is the seed convergence deadline (Rust: 60 s).
	seedTimeout = 60 * time.Second
	// healTimeout is the post-heal convergence deadline (Rust: 90 s).
	healTimeout = 90 * time.Second
	// poll is the convergence poll interval (Rust: 200 ms).
	poll = 200 * time.Millisecond
)

// seedRows reads the seed-row knob (Rust: SEED_ENV, default 60, positive
// only).
func seedRows() int {
	if v, err := strconv.Atoi(os.Getenv(seedEnv)); err == nil && v > 0 {
		return v
	}
	return defaultSeed
}

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

// tryInsert is a best-effort insert; true when acknowledged (Rust: writers
// keep only err == nil names in acked).
func tryInsert(db *murmur.Database, name string) bool {
	_, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO live_typed_records(name, counter) VALUES(?, 0)", name)
		return nil, err
	})
	return err == nil
}

// insertRow inserts one record, failing the test on error (seeds and
// post-race writes).
func insertRow(t *testing.T, db *murmur.Database, node int, name string) {
	t.Helper()
	if !tryInsert(db, name) {
		t.Fatalf("node%d insert of %q failed", node, name)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after insert failed: %v", node, err)
	}
}

// setNote writes the added Note field (Rust: Cluster.TypedSetNote).
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
		t.Fatalf("node%d schema_epoch = %d, want %d", idx+1, got, want)
	}
}

// tryReadNote is a raw Note read that surfaces SQL errors (Rust: TypedNote
// returning nil vs non-nil error distinguishes exposed from hidden Note).
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

// classifyNode requires a well-defined schema state: epoch exactly 1 or 2,
// the Note column exposed iff new, and logs the (still converging) row
// count (Rust: classifyNode).
func classifyNode(t *testing.T, cluster *harness.Cluster, idx int) {
	t.Helper()
	ep := epoch(t, cluster, idx)
	if ep != 1 && ep != 2 {
		t.Fatalf("node%d epoch = %d, want exactly 1 or 2 (mixed state)", idx+1, ep)
	}
	_, _, err := tryReadNote(cluster.Node(idx), "seed-0")
	if ep == 1 {
		if err == nil {
			t.Fatalf("node%d epoch 1 yet exposes Note (mixed state)", idx+1)
		}
	} else {
		if err != nil {
			t.Fatalf("node%d epoch 2 yet hides Note (mixed state): %v", idx+1, err)
		}
	}
	rows := names(t, cluster.Node(idx))
	t.Logf("node%d classified: epoch=%d rows=%d (converging)", idx+1, ep, len(rows))
}

// waitNamesConverged polls id/name convergence (Rust: waitNamesConverged;
// tolerates schema skew on the new column during the mixed-version
// window).
func waitNamesConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, poll, func() bool {
		var first []string
		for i := 0; i < cluster.Len(); i++ {
			got := names(t, cluster.Node(i))
			if len(got) != want {
				return false
			}
			if first == nil {
				first = got
			} else if !slices.Equal(first, got) {
				return false
			}
		}
		return true
	})
	if !ok {
		union := make(map[string]bool)
		sets := make([]map[string]bool, 0, cluster.Len())
		for i := 0; i < cluster.Len(); i++ {
			got := names(t, cluster.Node(i))
			t.Logf("node%d at timeout: names=%d", i+1, len(got))
			set := make(map[string]bool, len(got))
			for _, n := range got {
				union[n] = true
				set[n] = true
			}
			sets = append(sets, set)
		}
		for i, set := range sets {
			var missing []string
			for n := range union {
				if !set[n] {
					missing = append(missing, n)
				}
			}
			if len(missing) != 0 {
				t.Logf("node%d at timeout: missing %d names: %v", i+1, len(missing), missing)
			}
		}
		t.Fatalf("nodes did not converge on %d names within %v", want, timeout)
	}
}

// waitFullConverged polls exact counts plus ordered-digest equality (Rust:
// waitFullConverged).
func waitFullConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, poll, func() bool {
		var first []string
		for i := 0; i < cluster.Len(); i++ {
			got := names(t, cluster.Node(i))
			if len(got) != want {
				return false
			}
			if first == nil {
				first = got
			} else if !slices.Equal(first, got) {
				return false
			}
		}
		return true
	})
	if !ok {
		for i := 0; i < cluster.Len(); i++ {
			got := names(t, cluster.Node(i))
			t.Logf("node%d at timeout: count=%d digest=%v", i+1, len(got), got)
		}
		t.Fatalf("nodes did not fully converge on %d rows within %v", want, timeout)
	}
}

// assertNote requires a materialized Note value (Rust: assertNote).
func assertNote(t *testing.T, cluster *harness.Cluster, idx int, name, want string) {
	t.Helper()
	ok := harness.WaitUntil(healTimeout, poll, func() bool {
		got, present, err := tryReadNote(cluster.Node(idx), name)
		return err == nil && present && got == want
	})
	// A single-shot read races the Note UPDATEs: names converge (and
	// waitFullConverged passes) while the per-origin UPDATEs are still
	// in flight, so the value must be awaited, not sampled once.
	if !ok {
		got, present, err := tryReadNote(cluster.Node(idx), name)
		t.Fatalf("node%d Note(%q) = (%q,%v,%v), want %q", idx+1, name, got, present, err, want)
	}
}

// doMigrateUnlessAdopted applies the additive migration unless the column
// is already present (Rust MigrateTypedRecords is an idempotent manifest
// bind; DDL errors on a duplicate column, so adoption is checked first).
// It returns an error instead of failing so the simultaneous trigger can
// run it off the test goroutine.
func doMigrateUnlessAdopted(cluster *harness.Cluster, idx int) (bool, error) {
	v, err := cluster.Node(idx).Read(func(s *murmur.Sql) (any, error) {
		return s.Query("PRAGMA table_info(" + table + ")")
	})
	if err == nil {
		for _, row := range v.([][]sqlite.Value) {
			if len(row) > 1 && row[1].Str == "note" {
				return false, nil
			}
		}
	}
	if err := cluster.Node(idx).Migrate(murmur.NewMigration(noteDDL)); err != nil {
		return false, fmt.Errorf("migrate node%d: %w", idx+1, err)
	}
	if err := cluster.Node(idx).Flush(); err != nil {
		return false, fmt.Errorf("flush migration on node%d: %w", idx+1, err)
	}
	return true, nil
}

// migrateUnlessAdopted is the test-goroutine wrapper.
func migrateUnlessAdopted(t *testing.T, cluster *harness.Cluster, idx int) bool {
	t.Helper()
	migrated, err := doMigrateUnlessAdopted(cluster, idx)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return migrated
}

func TestSimultaneousMigrationConverges(t *testing.T) {
	seed := seedRows()

	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "migration-concurrency",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("migration-concurrency-no-convergence")
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

	// Honest baseline: all old, seed converged everywhere.
	for i := 0; i < seed; i++ {
		insertRow(t, cluster.Node(0), 1, fmt.Sprintf("seed-%d", i))
	}
	waitNamesConverged(t, cluster, seed, seedTimeout)
	for i := 0; i < 3; i++ {
		assertEpoch(t, cluster, i, 1)
	}

	// Pin node 1 on the old schema with peer isolation: adoption is fast
	// once a revision exists, so without isolation there would be no
	// deterministic mixed-version window after the race.
	for _, pair := range [][2]int{{0, 1}, {1, 0}, {0, 2}, {2, 0}} {
		if err := cluster.RemovePeer(pair[0], pair[1]); err != nil {
			t.Fatalf("isolate node1: %v", err)
		}
	}

	// All nodes keep writing across the migration window; every acked
	// name must converge afterwards.
	var stop atomic.Bool
	var wg sync.WaitGroup
	// A failed assertion below must still release the writers so the
	// WaitGroup join cannot deadlock (t.Fatalf runs deferred calls).
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()
	var ackMu sync.Mutex
	acked := make(map[string]bool)
	var seqs [3]atomic.Int64
	for n := 0; n < 3; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			db := cluster.Node(n)
			for !stop.Load() {
				time.Sleep(writeTick)
				if stop.Load() {
					break
				}
				seq := seqs[n].Add(1)
				name := fmt.Sprintf("flow-n%d-%08d", n+1, seq)
				if tryInsert(db, name) {
					ackMu.Lock()
					acked[name] = true
					ackMu.Unlock()
				}
			}
		}(n)
	}
	time.Sleep(time.Second) // writers flowing before the race

	// Simultaneous trigger: the same migration starts on nodes 2 and 3
	// at the same instant (start barrier), node 1 stays old.
	start := make(chan struct{})
	results := make(chan error, 2)
	var racers sync.WaitGroup
	for _, idx := range []int{1, 2} {
		racers.Add(1)
		go func(idx int) {
			defer racers.Done()
			<-start
			_, err := doMigrateUnlessAdopted(cluster, idx)
			results <- err
		}(idx)
	}
	close(start)
	t.Log("simultaneous migrate fired on nodes 2 and 3")
	racers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("migrate result: %v", err)
		}
	}
	t.Log("migrate result: nodes 2 and 3 done")
	stop.Store(true)
	wg.Wait()

	ackMu.Lock()
	overlap := len(acked)
	ackMu.Unlock()
	total := seed + overlap
	t.Logf("migration window closed with %d overlap writes", overlap)
	if overlap == 0 {
		t.Fatalf("no writes acknowledged during the migration window; overlap proved nothing")
	}

	// Both migrating nodes land well-defined; node 1 is still old.
	for _, idx := range []int{1, 2} {
		classifyNode(t, cluster, idx)
	}
	assertEpoch(t, cluster, 0, 1)
	newCount := 0
	for _, idx := range []int{1, 2} {
		if epoch(t, cluster, idx) == 2 {
			newCount++
		}
	}
	if newCount == 0 {
		t.Fatalf("simultaneous migration landed nowhere: both nodes still old")
	}
	t.Logf("simultaneous trigger: %d/2 nodes new; sequentially migrating laggards", newCount)
	for _, idx := range []int{1, 2} {
		if epoch(t, cluster, idx) != 2 {
			migrateUnlessAdopted(t, cluster, idx)
		}
	}

	// Heal with versions asserted different at the instant: node 1 is
	// deterministically old, the racers are new. Overlap writes from both
	// sides then converge by name across the boundary.
	assertEpoch(t, cluster, 0, 1)
	for _, pair := range [][2]int{{0, 1}, {1, 0}, {0, 2}, {2, 0}} {
		if err := cluster.AddPeer(pair[0], pair[1]); err != nil {
			t.Fatalf("heal node1: %v", err)
		}
	}
	waitNamesConverged(t, cluster, total, healTimeout)
	t.Logf("overlap writes converged in both directions: %d names", total)

	// Explicit exchange across the (former) boundary: new-column writes
	// from a new node plus an old-schema insert from node 1.
	assertEpoch(t, cluster, 1, 2)
	for i := 0; i < 5; i++ {
		setNote(t, cluster.Node(1), 2, fmt.Sprintf("seed-%d", i), fmt.Sprintf("%d", 100+i))
	}
	insertRow(t, cluster.Node(0), 1, "post-race")
	waitNamesConverged(t, cluster, total+1, healTimeout)

	// Migrate the third (unless it already auto-adopted), then full
	// convergence: exact counts, ordered digests, new schema everywhere.
	for i := 0; i < cluster.Len(); i++ {
		migrateUnlessAdopted(t, cluster, i)
	}
	waitFullConverged(t, cluster, total+1, healTimeout)
	for i := 0; i < 3; i++ {
		assertEpoch(t, cluster, i, 2)
	}
	// Materialized-query check: the new column reads identically
	// everywhere.
	for i := 0; i < 3; i++ {
		assertNote(t, cluster, i, "seed-0", "100")
		assertNote(t, cluster, i, "seed-4", "104")
	}
	t.Log("migration concurrency proven: simultaneous trigger, defined states, full convergence")
	failed = false
}

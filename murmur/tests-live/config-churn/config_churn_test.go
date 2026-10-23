// Live scenario: table replication flags gate sending, survive churn,
// and persist across restarts.
//
// Table flags are node-local send gates (the sign path drops records of
// locally-disabled tables; schema adoption force-enables tables learned
// from peers), so this suite asserts local behavior, not cross-node flag
// convergence:
//
//   - Gating leg: with node 0's flag off, rows committed on node 0 must
//     not reach its peers while control rows between the peers still
//     converge (proving the mesh is alive and the absence is the gate).
//     Re-enabling must resume propagation.
//   - Churn leg: concurrent flag flips on all nodes under rolling
//     restarts must never error, wedge the storage worker, or lose the
//     locally last-written value; deterministic final values must
//     survive a restart each.
//   - Ownership leg: the config-owned marker must persist across a full
//     cluster restart with flags intact.
package configchurn

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

const (
	// recordsDDL is the gated table.
	recordsDDL = "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
	// flipsPerNode is the churn-leg flips per node.
	flipsPerNode = 50
	// gatedRows is the disabled-era row count.
	gatedRows = 10
	// controlRows is the control row count proving mesh liveness.
	controlRows = 5
	// openRows is the post-re-enable row count.
	openRows = 10
	// convergeTimeout bounds every convergence wait.
	convergeTimeout = 90 * time.Second
	// poll is the convergence poll interval.
	poll = 200 * time.Millisecond
)

// insert writes one row in its own transaction.
func insert(t *testing.T, db *murmur.Database, id int64, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
		return nil, err
	}); err != nil {
		t.Fatalf("insert %s: %v", name, err)
	}
}

// names returns the ordered names on one node.
func names(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM records ORDER BY name")
	})
	if err != nil {
		t.Fatalf("read names: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

// hasPrefix reports whether any name carries the prefix.
func hasPrefix(t *testing.T, db *murmur.Database, prefix string) bool {
	t.Helper()
	for _, n := range names(t, db) {
		if len(n) >= len(prefix) && n[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

// waitNames waits until one node holds exactly the want names.
func waitNames(t *testing.T, db *murmur.Database, want []string, what string) {
	t.Helper()
	if ok := harness.WaitUntil(convergeTimeout, poll, func() bool {
		return slices.Equal(names(t, db), want)
	}); !ok {
		t.Fatalf("%s: names = %q, want %q", what, names(t, db), want)
	}
}

// flag returns the local replication flag for the records table.
func flag(t *testing.T, db *murmur.Database) bool {
	t.Helper()
	enabled, err := db.ReplicationEnabled("records")
	if err != nil {
		t.Fatalf("replication flag: %v", err)
	}
	return enabled
}

// setFlag flips the local flag and flushes.
func setFlag(t *testing.T, db *murmur.Database, enabled bool) {
	t.Helper()
	if err := db.SetReplication("records", enabled); err != nil {
		t.Fatalf("set flag %v: %v", enabled, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush flag %v: %v", enabled, err)
	}
}

func TestTableConfigGatingChurnAndDurability(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "config-churn",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("config-churn-no-convergence")
		}
		cluster.Finish(failed)
	}()

	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, recordsDDL); err != nil {
			t.Fatalf("migrate records table: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		setFlag(t, cluster.Node(i), true)
	}

	// Leg 1: gating. Node 0's flag goes off; its rows must not propagate
	// while peer-to-peer control rows still converge.
	setFlag(t, cluster.Node(0), false)
	if flag(t, cluster.Node(0)) {
		t.Fatalf("node0 flag reads enabled after disabling")
	}
	for n := 0; n < gatedRows; n++ {
		insert(t, cluster.Node(0), int64(n), "gated-"+string(rune('a'+n)))
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush gated rows: %v", err)
	}
	var control []string
	for n := 0; n < controlRows; n++ {
		name := "control-" + string(rune('a'+n))
		control = append(control, name)
		insert(t, cluster.Node(1), int64(100+n), name)
	}
	if err := cluster.Node(1).Flush(); err != nil {
		t.Fatalf("flush control rows: %v", err)
	}
	// The mesh is alive once node 2 holds every control row; only then is
	// the absence of gated rows meaningful.
	waitNames(t, cluster.Node(2), control, "control convergence")
	for _, i := range []int{1, 2} {
		if hasPrefix(t, cluster.Node(i), "gated-") {
			t.Fatalf("node%d received rows through a closed send gate", i)
		}
	}
	t.Logf("closed gate held %d rows off both peers", gatedRows)

	// Re-enable: fresh rows must propagate everywhere again. (Whether the
	// disabled-era backlog also flushes is product-defined; the suite
	// pins only that the open gate propagates.)
	setFlag(t, cluster.Node(0), true)
	var open []string
	for n := 0; n < openRows; n++ {
		name := "open-" + string(rune('a'+n))
		open = append(open, name)
		insert(t, cluster.Node(0), int64(200+n), name)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush open rows: %v", err)
	}
	if ok := harness.WaitUntil(convergeTimeout, poll, func() bool {
		for _, i := range []int{1, 2} {
			got := names(t, cluster.Node(i))
			for _, want := range open {
				if !slices.Contains(got, want) {
					return false
				}
			}
		}
		return true
	}); !ok {
		t.Fatalf("post-re-enable rows did not reach both peers")
	}

	// Leg 2: churn under a rolling restart. Flippers run in phases so no
	// goroutine ever touches a stopped node (Cluster.Node panics on a
	// stopped node); while node 0 is down the survivors keep flipping to
	// prove churn elsewhere is unaffected by the outage.
	flip := func(nodes []int, count int) {
		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, nodeIdx := range nodes {
			wg.Add(1)
			go func(nodeIdx int) {
				defer wg.Done()
				db := cluster.Node(nodeIdx)
				<-start
				for n := 0; n < count; n++ {
					if err := db.SetReplication("records", n%2 == 0); err != nil {
						t.Errorf("node%d flip %d: %v", nodeIdx, n, err)
						return
					}
					if err := db.Flush(); err != nil {
						t.Errorf("node%d flush flip %d: %v", nodeIdx, n, err)
						return
					}
				}
			}(nodeIdx)
		}
		close(start)
		wg.Wait()
	}
	flip([]int{0, 1, 2}, flipsPerNode/2)
	cluster.StopNode(0)
	flip([]int{1, 2}, flipsPerNode/2)
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart node0: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh: %v", err)
	}
	flip([]int{0, 1, 2}, flipsPerNode/2)

	// Deterministic final values, then one restart each to prove
	// durability of exactly what was last written locally.
	want := []bool{true, false, true}
	for i := 0; i < cluster.Len(); i++ {
		setFlag(t, cluster.Node(i), want[i])
	}
	for i := 0; i < cluster.Len(); i++ {
		cluster.StopNode(i)
		if err := cluster.StartNode(i); err != nil {
			t.Fatalf("restart node%d: %v", i, err)
		}
		if got := flag(t, cluster.Node(i)); got != want[i] {
			t.Fatalf("node%d flag = %v after restart, want %v", i, got, want[i])
		}
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restarts: %v", err)
	}

	// Leg 3: the config-owned marker persists across a full restart with
	// flags intact, and replication still converges afterwards (the
	// worker was not wedged by the churn).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).MarkTableConfigOwned(); err != nil {
			t.Fatalf("node%d mark owned: %v", i, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		cluster.StopNode(i)
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.StartNode(i); err != nil {
			t.Fatalf("restart node%d: %v", i, err)
		}
		owned, err := cluster.Node(i).TableConfigOwned()
		if err != nil {
			t.Fatalf("node%d owned marker: %v", i, err)
		}
		if !owned {
			t.Fatalf("node%d lost its config-owned marker across restart", i)
		}
		if got := flag(t, cluster.Node(i)); got != want[i] {
			t.Fatalf("node%d flag = %v after owned restart, want %v", i, got, want[i])
		}
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("final re-mesh: %v", err)
	}
	// Node 1's gate is closed, so the final proof writes from node 0 and
	// reads back on node 2.
	insert(t, cluster.Node(0), 9999, "final-proof")
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush final proof: %v", err)
	}
	if ok := harness.WaitUntil(convergeTimeout, poll, func() bool {
		return slices.Contains(names(t, cluster.Node(2)), "final-proof")
	}); !ok {
		t.Fatalf("replication did not converge after config churn")
	}
	t.Logf("table flags gated, churned, and persisted across restarts")
	failed = false
}

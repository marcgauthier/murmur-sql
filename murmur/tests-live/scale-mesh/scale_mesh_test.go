// Live scenario: ten nodes mesh with the default fanout while two origins
// write 400 rows; every node must converge with the fanout bound intact.
//
// Go port of the Rust `scale-mesh` scenario
// (/home/marc/MURMUR/tests-live/scale-mesh/tests/scale_mesh.rs). Ten nodes
// mesh with the default fanout of 4 while two origins write 400 rows;
// every node must converge to identical row sets, and a 2 s sampler
// asserts no node ever selects more than 4 replication targets for the
// whole run. Plain SQL replaces the typed-contention HTTP API.
package scalemesh

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

const (
	// numNodes is the node count (Rust: nodes = 10).
	numNodes = 10
	// firstOriginRows is the rows written by the first origin on node 1
	// (Rust: 0..300 on node 0).
	firstOriginRows = 300
	// totalRows is the total rows incl. the second origin (Rust:
	// 300..400 on node 8).
	totalRows = 400
	// secondOrigin is the zero-based index of the second origin (Rust:
	// node index 7).
	secondOrigin = 7
	// fanout is the replication fanout bound (Rust: default fanout 4).
	fanout = 4
	// sampleInterval is the fanout sampler interval (Rust: 2 s).
	sampleInterval = 2 * time.Second
	// convergenceTimeout is the convergence deadline (Rust: 300 s for
	// ten nodes on a saturated box).
	convergenceTimeout = 300 * time.Second
	// pollInterval is the convergence poll interval (Rust: 200 ms).
	pollInterval = 200 * time.Millisecond
	// breachCap is the breach channel capacity (Rust: 64).
	breachCap = 64
)

// selectedPeers is the selected (push-mode) replication targets on one
// node.
//
// Rust reads selected_peers from /v1/status; the equivalent is the count
// of connected peers flagged push_mode in the replication status. The
// engine caps push peers at config.fanout on insertion and swaps them
// one-for-one under a single lock during rotation, so the count must never
// exceed the fanout. It returns ok=false when the node has no replication
// handle.
func selectedPeers(db *murmur.Database) (got int, ok bool) {
	h := db.Replication()
	if h == nil {
		return 0, false
	}
	n := 0
	for _, d := range h.Status().PeersDiagnostics {
		if d.Connected && d.PushMode {
			n++
		}
	}
	return n, true
}

// sampleFanoutBound takes one fanout-bound sample over every node (Rust:
// sampleFanoutBound).
//
// It never fails the test: sampler failures are reported as breach
// strings, and a full channel drops (Rust: non-blocking send with
// default).
func sampleFanoutBound(dbs []*murmur.Database, fanout int, breaches chan<- string) {
	for idx, db := range dbs {
		got, ok := selectedPeers(db)
		if !ok {
			select {
			case breaches <- fmt.Sprintf("node%d replication handle missing", idx+1):
			default:
			}
			continue
		}
		if got > fanout {
			select {
			case breaches <- fmt.Sprintf("node%d selected_peers=%d exceeds fanout %d", idx+1, got, fanout):
			default:
			}
		}
	}
}

// rowCount is the row count on one node, or ok=false when the read fails
// (Rust: len(rows)).
func rowCount(db *murmur.Database) (n int, ok bool) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT COUNT(*) FROM records")
	})
	if err != nil {
		return 0, false
	}
	return int(v.([][]sqlite.Value)[0][0].Int), true
}

// canonicalRows is the PK-ordered id:name lines on one node (Rust:
// meshDigest input).
func canonicalRows(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name FROM records ORDER BY id")
	})
	if err != nil {
		t.Fatalf("read canonical rows: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str+":"+row[1].Str)
	}
	return out
}

// insertRow is one insert on the origin node (Rust: TypedContentionInsert,
// one call/row).
func insertRow(t *testing.T, db *murmur.Database, i int) {
	t.Helper()
	id := fmt.Sprintf("%032x", i)
	name := fmt.Sprintf("w-%d", i)
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
		return nil, err
	}); err != nil {
		t.Fatalf("write %d failed: %v", i, err)
	}
}

func TestScaleMesh(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "scale-mesh",
		NumNodes: numNodes,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("scale-mesh-convergence-or-fanout")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("Node %s running in directory %s (repl=%s)", info.Label, info.Dir, info.ReplAddr)
	}

	// Same schema on every node (Rust: TypedRecords fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).Migrate(murmur.NewMigration("CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)")); err != nil {
			t.Fatalf("migrate records table: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush schema: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("records", true); err != nil {
			t.Fatalf("opt-in records: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}

	// Bound sampling runs for the whole test. The sampler reports
	// breaches over a channel; only the test goroutine may fail.
	samplerDBs := make([]*murmur.Database, 0, cluster.Len())
	for i := 0; i < cluster.Len(); i++ {
		samplerDBs = append(samplerDBs, cluster.Node(i))
	}
	var stop atomic.Bool
	breaches := make(chan string, breachCap)
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		for !stop.Load() {
			time.Sleep(sampleInterval)
			if stop.Load() {
				break
			}
			sampleFanoutBound(samplerDBs, fanout, breaches)
		}
	}()

	// Two origins: 300 rows on node 1, 100 on node 8. The second origin
	// proves multi-hop forwarding, not just hub fan-out.
	for i := 0; i < firstOriginRows; i++ {
		insertRow(t, cluster.Node(0), i)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush first origin: %v", err)
	}
	for i := firstOriginRows; i < totalRows; i++ {
		insertRow(t, cluster.Node(secondOrigin), i)
	}
	if err := cluster.Node(secondOrigin).Flush(); err != nil {
		t.Fatalf("flush second origin: %v", err)
	}

	// Every node must hold all 400 rows (Rust: waitAllCounts, 300 s).
	if ok := harness.WaitUntil(convergenceTimeout, pollInterval, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			n, ok := rowCount(cluster.Node(i))
			if !ok || n != totalRows {
				return false
			}
		}
		return true
	}); !ok {
		t.Fatalf("10-node counts did not reach %d within %v", totalRows, convergenceTimeout)
	}

	// Identical canonical row sets on every node (Rust: meshDigest
	// equality; byte-identical vectors imply identical digests).
	want := canonicalRows(t, cluster.Node(0))
	if len(want) != totalRows {
		t.Fatalf("node1 canonical row count = %d, want %d", len(want), totalRows)
	}
	for idx := 1; idx < cluster.Len(); idx++ {
		if got := canonicalRows(t, cluster.Node(idx)); !slices.Equal(got, want) {
			t.Fatalf("node%d rows differ after 10-node convergence", idx+1)
		}
	}
	t.Logf("Ten nodes converged on %d rows with fanout %d", totalRows, fanout)

	// Final sample, then stop the sampler and fail on any breach.
	sampleFanoutBound(samplerDBs, fanout, breaches)
	stop.Store(true)
	samplerWG.Wait()
	close(breaches)
	var found []string
	for b := range breaches {
		found = append(found, b)
	}
	if len(found) != 0 {
		t.Fatalf("fanout breaches: %v", found)
	}
	failed = false
}

// Live scenario: three nodes each commit 30 uniquely-named rows
// concurrently; every node must converge on all 90 rows in canonical
// ordered sequence.
//
// Go port of the Rust `three-node-sync` scenario
// (/home/marc/MURMUR/tests-live/three-node-sync/tests/three_node_sync.rs).
// Plain SQL replaces the Go typed-records HTTP API of the original.
package threenodesync

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

const (
	// rowsPerNode is the rows each node inserts (Rust: ROWS_PER_NODE = 30).
	rowsPerNode = 30
	// convergenceTimeout is the convergence deadline (Rust: 25 s).
	convergenceTimeout = 25 * time.Second
	// pollInterval is the convergence poll interval (Rust: 100 ms).
	pollInterval = 100 * time.Millisecond
)

// names is the canonical ordered names on one node (Rust: names).
func names(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM records ORDER BY name")
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

func TestThreeNodeConcurrentWritesConverge(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "three-node-sync",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("no-convergence")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("Node %s running in directory %s (repl=%s)", info.Label, info.Dir, info.ReplAddr)
	}

	// Same schema on every node (Rust: TypedRecords fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).Migrate(murmur.NewMigration(
			"CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")); err != nil {
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

	// Concurrent per-node writes (Rust: one worker per node behind a
	// start barrier, one insert call per row). Row ids are node-disjoint
	// by construction.
	start := make(chan struct{})
	var wg sync.WaitGroup
	for nodeIdx := 0; nodeIdx < cluster.Len(); nodeIdx++ {
		wg.Add(1)
		go func(nodeIdx int) {
			defer wg.Done()
			db := cluster.Node(nodeIdx)
			<-start
			for row := 0; row < rowsPerNode; row++ {
				name := fmt.Sprintf("node-%d-row-%02d", nodeIdx, row)
				id := int64(nodeIdx*1000 + row)
				if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
					_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
					return nil, err
				}); err != nil {
					t.Errorf("node %d insert failed: %v", nodeIdx, err)
					return
				}
			}
			if err := db.Flush(); err != nil {
				t.Errorf("node %d flush after inserts: %v", nodeIdx, err)
			}
		}(nodeIdx)
	}
	close(start)
	wg.Wait()

	// Canonical ordered expectation (Rust: node-major, zero-padded).
	var want []string
	for node := 0; node < cluster.Len(); node++ {
		for row := 0; row < rowsPerNode; row++ {
			want = append(want, fmt.Sprintf("node-%d-row-%02d", node, row))
		}
	}

	converged := harness.WaitUntil(convergenceTimeout, pollInterval, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if !slices.Equal(names(t, cluster.Node(i)), want) {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("three nodes did not converge within deadline (expected %d records)", len(want))
	}
	t.Logf("Three nodes converged on %d records", len(want))
	failed = false
}

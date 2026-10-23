// Live scenario: Plumtree dissemination mesh convergence and mixed-mode refusal.
//
// Go port of the Rust `plumtree-live` scenario
// (/home/marc/MURMUR/tests-live/plumtree-live/tests/plumtree_live.rs), itself
// a port of the original Go `plumtree-live` scenario
// (TestPlumtreeMeshConverges, TestMixedModeRefusesGossipPeer).
// TestPlumtreeMeshConverges requires end-to-end convergence of a three-node
// all-Plumtree mesh; TestMixedModeRefusesGossipPeer requires a gossip-mode
// node meshed with Plumtree peers to stay isolated in both directions while
// handshake refusals are recorded and the Plumtree pair converges.
//
// Plain SQL replaces the managed typed-records HTTP API.
package plumtreelive

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/replication"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// rows is the row count inserted by the origin node (Rust: 10 per test).
const rows = 10

const (
	// convergeTimeout is the all-Plumtree convergence deadline (Rust: 60 s).
	convergeTimeout = 60 * time.Second
	// mixedTimeout is the mixed-mode pair-convergence deadline (Rust: 30 s).
	mixedTimeout = 30 * time.Second
	// pollInterval is the convergence poll interval (Rust: 200 ms).
	pollInterval = 200 * time.Millisecond
	// leakWindow is the leak window after mixed-mode convergence (Rust: 5 s).
	leakWindow = 5 * time.Second
)

// ddl is the shared records table (Rust: TypedRecords fixture tables).
const ddl = "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"

// queryNames reads the canonical ordered names on one node (Rust:
// Cluster.TypedNames). Errors are returned so convergence polls can treat
// them as not-yet-converged.
func queryNames(db *murmur.Database) ([]string, error) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM records ORDER BY name")
	})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out, nil
}

// mustNames reads the canonical ordered names on one node, failing the test
// on error.
func mustNames(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	names, err := queryNames(db)
	if err != nil {
		t.Fatalf("read names: %v", err)
	}
	return names
}

// migrateAll creates the same records table on every node and opts it into
// replication (Rust: migrate_all).
func migrateAll(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, ddl); err != nil {
			t.Fatalf("node%d migrate records table: %v", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("records", true); err != nil {
			t.Fatalf("node%d opt-in records: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush opt-in: %v", i+1, err)
		}
	}
}

// insertNames inserts count prefixed rows on one node with a single flush
// (Rust: insert_names).
func insertNames(t *testing.T, cluster *harness.Cluster, idx int, prefix string, count int) {
	t.Helper()
	for row := 0; row < count; row++ {
		name := prefix + "-" + strconv.Itoa(row)
		id := int64(row)
		if _, err := cluster.Node(idx).Transaction(func(s *murmur.Sql) (any, error) {
			_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
			return nil, err
		}); err != nil {
			t.Fatalf("node%d insert failed: %v", idx, err)
		}
	}
	if err := cluster.Node(idx).Flush(); err != nil {
		t.Fatalf("node%d flush after inserts: %v", idx, err)
	}
}

// lastErr renders the replication last_error (empty when none).
func lastErr(cluster *harness.Cluster, idx int) string {
	h := cluster.Node(idx).Replication()
	if h == nil {
		return "<no replication handle>"
	}
	if e := h.Status().LastError; e != nil {
		return *e
	}
	return ""
}

// TestPlumtreeMeshConverges requires three Plumtree nodes to converge on 10
// rows from node 0 (Rust: plumtree_mesh_converges).
func TestPlumtreeMeshConverges(t *testing.T) {
	plumtree := replication.DisseminationPlumtree
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:        "plumtree-live",
		NumNodes:    3,
		Replication: &harness.ReplicationOptions{Dissemination: &plumtree},
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

	migrateAll(t, cluster)
	insertNames(t, cluster, 0, "plum", rows)

	var want []string
	for row := 0; row < rows; row++ {
		want = append(want, "plum-"+strconv.Itoa(row))
	}
	converged := harness.WaitUntil(convergeTimeout, pollInterval, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			got, err := queryNames(cluster.Node(i))
			if err != nil || !equalStrings(got, want) {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("three Plumtree nodes did not converge on %d rows within %v", rows, convergeTimeout)
	}
	t.Logf("three Plumtree nodes converged on %d rows", rows)
	failed = false
}

// TestMixedModeRefusesGossipPeer meshes two Plumtree nodes with one
// gossip-mode node: the pair converges while the gossip node stays isolated
// in both directions with handshake refusals recorded (Rust:
// mixed_mode_refuses_gossip_peer).
func TestMixedModeRefusesGossipPeer(t *testing.T) {
	plumtree := replication.DisseminationPlumtree
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:        "plumtree-mixed",
		NumNodes:    3,
		Replication: &harness.ReplicationOptions{Dissemination: &plumtree},
		DisseminationByNode: map[int]replication.Dissemination{
			2: replication.DisseminationGossip,
		},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("mode-isolation")
		}
		cluster.Finish(failed)
	}()

	migrateAll(t, cluster)
	insertNames(t, cluster, 0, "mixed-plum", rows)
	insertNames(t, cluster, 2, "gossip-only", 1)

	// The Plumtree pair converges; the gossip node stays isolated.
	pairConverged := harness.WaitUntil(mixedTimeout, pollInterval, func() bool {
		n0, err0 := queryNames(cluster.Node(0))
		n1, err1 := queryNames(cluster.Node(1))
		return err0 == nil && err1 == nil && len(n0) == rows && len(n1) == rows
	})
	if !pairConverged {
		t.Fatalf("Plumtree pair did not converge on %d rows within %v", rows, mixedTimeout)
	}
	time.Sleep(leakWindow) // let any leak arrive
	for i, want := range []int{rows, rows, 1} {
		got := mustNames(t, cluster.Node(i))
		if len(got) != want {
			t.Fatalf("node%d count = %d, want %d (mode isolation broken?)", i, len(got), want)
		}
	}

	// Refusal evidence (Rust: last_error contains "incompatible"): the
	// engine fails closed with "incompatible hello/welcome" on both dial
	// and accept sides.
	refusals := 0
	for i := 0; i < cluster.Len(); i++ {
		e := lastErr(cluster, i)
		t.Logf("node%d: peers=%d last_error=%q", i,
			cluster.Node(i).Replication().Status().Peers, e)
		if strings.Contains(e, "incompatible") {
			refusals++
		}
	}
	if refusals < 1 {
		t.Fatalf("no handshake incompatibility refusals recorded (negotiation silent?)")
	}
	t.Logf("mode refusals recorded on %d node(s)", refusals)

	// Session-level isolation: the gossip node never holds a session,
	// while the converged Plumtree pair holds exactly each other.
	if peers := cluster.Node(2).Replication().Status().Peers; peers != 0 {
		t.Fatalf("gossip node holds %d sessions, want 0 (mode isolation broken?)", peers)
	}
	for i := 0; i < 2; i++ {
		if peers := cluster.Node(i).Replication().Status().Peers; peers != 1 {
			t.Fatalf("plumtree node%d holds %d sessions, want 1 (peer of the gossip node?)", i, peers)
		}
	}
	failed = false
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

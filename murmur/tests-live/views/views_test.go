// Live scenario: a managed view tracks replicated rows and rebuilds across
// a restart.
//
// Go port of the Rust `views` scenario
// (/home/marc/MURMUR/tests-live/views/tests/views.rs). Three nodes each
// insert one contact; two contacts are enabled via a counter add and must
// appear in the managed view on every node while the third stays out. Node
// 3 then restarts and its rebuilt view must contain the same two rows.
// Plain SQL (a SELECT over the replicated table) replaces the Go
// typed-RIME managed query; application VIEWs are rejected by the product.
package views

import (
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// tableDDL is the base table DDL (Rust: typed contacts records; score
// plays the role of the enabling PN counter).
const tableDDL = "CREATE TABLE contacts(name TEXT NOT NULL PRIMARY KEY, score INTEGER NOT NULL)"

// viewQuery is the managed-view query (Rust: TypedEnabledNames over
// managed typed queries). The product rejects application VIEWs, so the
// same SELECT runs directly against the replicated base table (identical
// rows, no DDL object).
const viewQuery = "SELECT name FROM contacts WHERE score > 0 ORDER BY name"

const (
	// viewTimeout is the view wait deadline (Rust: 20 s waitForViewRows).
	viewTimeout = 20 * time.Second
	// pollInterval is the view poll interval (Rust: 25 ms).
	pollInterval = 25 * time.Millisecond
)

// enabledNames is the canonical ordered enabled names on one node (Rust:
// TypedEnabledNames).
func enabledNames(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query(viewQuery)
	})
	if err != nil {
		t.Fatalf("read view failed: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

// insertContact inserts one contact, disabled (Rust: Cluster.TypedInsert).
func insertContact(t *testing.T, db *murmur.Database, node int, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO contacts(name, score) VALUES(?, 0)", name)
		return nil, err
	}); err != nil {
		t.Fatalf("node%d insert failed: %v", node, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after insert failed: %v", node, err)
	}
}

// enableContact enables one contact via a counter add (Rust:
// Cluster.TypedCounterAdd).
func enableContact(t *testing.T, db *murmur.Database, node int, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		changed, err := s.Execute("UPDATE contacts SET score = score + 1 WHERE name = ?", name)
		if err != nil {
			return nil, err
		}
		if changed != 1 {
			t.Fatalf("node%d enable hit %d rows", node, changed)
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("node%d enable failed: %v", node, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after enable failed: %v", node, err)
	}
}

// waitForViewRows waits until the managed view on each listed node holds
// exactly want (Rust: waitForViewRows).
func waitForViewRows(t *testing.T, cluster *harness.Cluster, nodes []int, want []string) {
	t.Helper()
	matched := harness.WaitUntil(viewTimeout, pollInterval, func() bool {
		for _, i := range nodes {
			got := enabledNames(t, cluster.Node(i))
			if len(got) != len(want) {
				return false
			}
			for k := range want {
				if got[k] != want[k] {
					return false
				}
			}
		}
		return true
	})
	if !matched {
		t.Fatalf("timed out waiting for typed view to contain %v on nodes %v", want, nodes)
	}
}

func TestTypedViewTracksReplicatedRowsAndRebuilds(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "views",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("views-no-convergence")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}

	// Same schema on every node (Rust: typed fixture tables + view).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			t.Fatalf("migrate contacts table: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("contacts", true); err != nil {
			t.Fatalf("opt-in contacts: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}

	// One insert per node; only ann and bob are enabled (Rust: hidden
	// gets no counter add and must stay out of the view).
	for i, name := range []string{"ann", "bob", "hidden"} {
		insertContact(t, cluster.Node(i), i+1, name)
		if name != "hidden" {
			enableContact(t, cluster.Node(i), i+1, name)
		}
	}
	waitForViewRows(t, cluster, []int{0, 1, 2}, []string{"ann", "bob"})

	// Restart the third node: startup rebuild must restore both the
	// replicated rows and the local view (fresh ephemeral port, so
	// re-mesh afterwards).
	cluster.StopNode(2)
	if err := cluster.StartNode(2); err != nil {
		t.Fatalf("restart node3: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	waitForViewRows(t, cluster, []int{2}, []string{"ann", "bob"})
	failed = false
}

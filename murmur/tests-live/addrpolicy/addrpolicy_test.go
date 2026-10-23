// Live scenario: address admission is enforced on the replication mesh.
//
// Go port of the Rust `addrpolicy` scenario
// (/home/marc/MURMUR/tests-live/addrpolicy/tests/addrpolicy.rs).
// Node1 admits loopback peers (127.0.0.0/8); node2's TEST-NET policy
// (198.51.100.0/24) excludes every actual loopback socket address; node3
// is unrestricted. The admitted pair must replicate while the denied node
// keeps its write local. Plain SQL replaces the typed-records HTTP API,
// and QUIC session state replaces connected_peers.
package addrpolicy

import (
	"net/netip"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

const (
	// peerTimeout is the peer-topology deadline (Rust: 20 s).
	peerTimeout = 20 * time.Second
	// countTimeout is the per-node row-count deadline (Rust: 20 s).
	countTimeout = 20 * time.Second
	// poll is the convergence poll interval (Rust: 50 ms).
	poll = 50 * time.Millisecond
	// denySettle is the post-convergence settle window: several
	// dial/accept retries must not move the denied node's state (Rust:
	// 750 ms sleep).
	denySettle = 750 * time.Millisecond
)

// peerCount is the live session count (Rust: connectedPeers).
func peerCount(t *testing.T, cluster *harness.Cluster, node int) int {
	t.Helper()
	h := cluster.Node(node).Replication()
	if h == nil {
		t.Fatalf("node%d replication disabled", node+1)
	}
	return h.Status().Peers
}

// waitForPeers waits until every node's live session count matches want
// (Rust: waitForPeers, 50 ms poll).
func waitForPeers(t *testing.T, cluster *harness.Cluster, want [3]int) {
	t.Helper()
	ok := harness.WaitUntil(peerTimeout, poll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if peerCount(t, cluster, i) != want[i] {
				return false
			}
		}
		return true
	})
	if !ok {
		actual := make([]int, 0, cluster.Len())
		for i := 0; i < cluster.Len(); i++ {
			actual = append(actual, peerCount(t, cluster, i))
		}
		t.Fatalf("peer topology %v != want %v within %v", actual, want, peerTimeout)
	}
}

// insert writes one replicated row (Rust: TypedInsert + flush).
func insert(t *testing.T, db *murmur.Database, id int64, name string) {
	t.Helper()
	_, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
		return nil, err
	})
	if err != nil {
		t.Fatalf("insert record row: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}
}

// names is the canonical ordered names on one node (Rust: TypedNames).
func names(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM records ORDER BY name")
	})
	if err != nil {
		t.Fatalf("read record names: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

// waitForCount waits until node holds want rows (Rust: waitForCount,
// 50 ms poll).
func waitForCount(t *testing.T, cluster *harness.Cluster, node, want int) {
	t.Helper()
	ok := harness.WaitUntil(countTimeout, poll, func() bool {
		return len(names(t, cluster.Node(node))) == want
	})
	if !ok {
		got := names(t, cluster.Node(node))
		t.Fatalf("%s record names=%v, want %d", cluster.NodeInfo(node).Label, got, want)
	}
}

func TestAddressPolicyAcrossProcesses(t *testing.T) {
	// Node1 admits loopback peers; node2's TEST-NET policy excludes every
	// actual loopback socket address. Node3 is unrestricted and provides
	// the allowed path with node1.
	loopback := netip.MustParsePrefix("127.0.0.0/8")
	testnet := netip.MustParsePrefix("198.51.100.0/24")
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "addrpolicy",
		NumNodes: 3,
		AllowedNetworks: map[int][]netip.Prefix{
			0: {loopback},
			1: {testnet},
		},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("address-policy-or-convergence")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("Node %s running in directory %s (repl=%s)", info.Label, info.Dir, info.ReplAddr)
	}

	// Same schema on every node (Rust: TypedRecords fixture tables).
	// Row ids are node-disjoint by construction; names keep the Go
	// TypedInsert values.
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
			t.Fatalf("migrate records table: %v", err)
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

	// Let QUIC dial retries settle, then confirm the allowed pair
	// formed a session while the denied node has no admitted path to
	// either peer (Rust: waitForPeers [1, 0, 1]).
	waitForPeers(t, cluster, [3]int{1, 0, 1})

	insert(t, cluster.Node(0), (1<<32)|1, "allowed-loopback")
	insert(t, cluster.Node(1), (2<<32)|1, "denied-testnet")
	insert(t, cluster.Node(2), (3<<32)|1, "unrestricted-loopback")

	waitForCount(t, cluster, 0, 2)
	waitForCount(t, cluster, 2, 2)
	time.Sleep(denySettle) // several dial/accept retries
	denied := names(t, cluster.Node(1))
	if len(denied) != 1 || denied[0] != "denied-testnet" {
		t.Fatalf("denied node state changed across network: got %v, want only its local row", denied)
	}
	admitted := names(t, cluster.Node(0))
	if len(admitted) != 2 {
		t.Fatalf("admitted node did not receive both its local and node3-origin row: names=%v", admitted)
	}

	for i := 0; i < cluster.Len(); i++ {
		if cluster.NodeInfo(i).Dir == "" {
			t.Fatalf("node%d has no isolated directory", i+1)
		}
	}
	failed = false
}

// Live scenario: SWIM seed-only discovery forms a full mesh, and a killed
// seed rediscovers through one live member.
//
// Go port of the Rust `swim-discovery` scenario
// (/home/marc/MURMUR/tests-live/swim-discovery/tests/swim_discovery.rs).
// Two legs:
//   - seed-only discovery forms a full mesh: four nodes start with a single
//     SWIM seed address and no static peers; a full mesh must form purely via
//     discovery (membership view plus a live session to every peer), and
//     writes from every node must converge with identical contents.
//   - seed kill and rediscovery: after baseline convergence the seed is
//     stopped, survivors must observe the death and keep converging writes
//     while it is down, then the restarted seed rejoins through one live
//     member and the full mesh reconverges with identical contents.
//
// Plain SQL replaces the typed-records HTTP API.
package swimdiscovery

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

const (
	// topoPoll is the membership/session poll interval (Rust: 250 ms).
	topoPoll = 250 * time.Millisecond
	// convergePoll is the convergence poll interval (Rust: 200 ms).
	convergePoll = 200 * time.Millisecond
	// convergeTimeout is the convergence deadline (Rust: 150 s loaded-box
	// rationale).
	convergeTimeout = 150 * time.Second
)

// discoveryTimeout is the seed-discovery bound (Rust:
// MURMUR_SWIM_DISCOVERY_SECONDS, default 60).
func discoveryTimeout() time.Duration {
	return harness.EnvSeconds("MURMUR_SWIM_DISCOVERY_SECONDS", 60)
}

// migrateRecords installs the same schema on every node (Rust: TypedRecords
// fixture tables).
func migrateRecords(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i,
			"CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
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
}

// insert writes one row (Rust: TypedInsert).
func insert(t *testing.T, cluster *harness.Cluster, node int, id int64, name string) {
	t.Helper()
	if _, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
		return nil, err
	}); err != nil {
		t.Fatalf("node %d insert '%s' failed: %v", node+1, name, err)
	}
	if err := cluster.Node(node).Flush(); err != nil {
		t.Fatalf("node %d flush after insert: %v", node+1, err)
	}
}

// names is the canonical ordered names on one node (Rust:
// Cluster.TypedNames).
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

// waitConvergedOn waits until every listed node holds exactly want (Rust:
// waitConvergedOn).
func waitConvergedOn(t *testing.T, cluster *harness.Cluster, idxs []int, want []string, timeout time.Duration, what string) {
	t.Helper()
	converged := harness.WaitUntil(timeout, convergePoll, func() bool {
		for _, i := range idxs {
			if !slices.Equal(names(t, cluster.Node(i)), want) {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("nodes %v did not converge on %d records with equal contents within %v (%s)",
			idxs, len(want), timeout, what)
	}
}

// memberCount is the discovered-member count, excluding self (Rust:
// status().members; spedsql_membership_count minus one).
func memberCount(cluster *harness.Cluster, node int) int {
	return cluster.Node(node).Replication().Status().Members
}

// peerCount is the live-session count (Rust: status().peers;
// spedsql_peer_count).
func peerCount(cluster *harness.Cluster, node int) int {
	return cluster.Node(node).Replication().Status().Peers
}

// peerConnected is the direct-session probe (Rust:
// spedsql_peer_connected{peer=...} == 1).
func peerConnected(cluster *harness.Cluster, node int, peerID [16]byte) bool {
	for _, d := range cluster.Node(node).Replication().Status().PeersDiagnostics {
		if d.NodeID == peerID && d.Connected {
			return true
		}
	}
	return false
}

// waitMembership waits until every node sees want discovered members
// (Rust: waitMembership).
func waitMembership(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration, what string) {
	t.Helper()
	ok := harness.WaitUntil(timeout, topoPoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if memberCount(cluster, i) != want {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("nodes did not reach membership %d (excl. self) within %v (%s)", want+1, timeout, what)
	}
}

// sessionMeshComplete reports whether every directed pair holds a live
// session (Rust: sessionMeshComplete).
func sessionMeshComplete(cluster *harness.Cluster) bool {
	for i := 0; i < cluster.Len(); i++ {
		for j := 0; j < cluster.Len(); j++ {
			if i != j && !peerConnected(cluster, i, cluster.NodeID(j)) {
				return false
			}
		}
	}
	return true
}

// waitFullSessionMesh waits for a live session on every directed pair
// (Rust: waitFullSessionMesh).
func waitFullSessionMesh(t *testing.T, cluster *harness.Cluster, timeout time.Duration, what string) {
	t.Helper()
	ok := harness.WaitUntil(timeout, topoPoll, func() bool {
		return sessionMeshComplete(cluster)
	})
	if !ok {
		t.Fatalf("full session mesh did not form within %v (%s)", timeout, what)
	}
}

// waitDeathObserved waits until every survivor's live-session count drops
// below the full mesh, proving the seed's death was observed before the
// restart makes rediscovery meaningful (Rust: waitDeathObserved).
func waitDeathObserved(t *testing.T, cluster *harness.Cluster, survivors []int, timeout time.Duration) {
	t.Helper()
	full := cluster.Len() - 1
	ok := harness.WaitUntil(timeout, topoPoll, func() bool {
		for _, i := range survivors {
			if peerCount(cluster, i) >= full {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("survivors %v did not observe seed death within %v", survivors, timeout)
	}
}

func TestSeedOnlyDiscoveryFormsFullMesh(t *testing.T) {
	// Single seed (index 0); no static peers anywhere. The harness opens
	// the seed first and gives every other node only the seed's address as
	// its bootstrap; ManualPeers means zero explicit AddPeer calls, so the
	// mesh under test is discovered, not configured (Rust: ManualPeers +
	// BootstrapSeeds []int{0} plus assertNoStaticPeers).
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:           "swim-discovery",
		NumNodes:       4,
		ManualPeers:    true,
		BootstrapSeeds: []int{0},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("discovery-no-mesh")
		}
		cluster.Finish(failed)
	}()

	deadline := discoveryTimeout()
	migrateRecords(t, cluster)

	// Full mesh purely via SWIM: every node sees all 4 members ...
	waitMembership(t, cluster, 3, deadline, "initial discovery")
	// ... and holds a live replication session to every other node.
	waitFullSessionMesh(t, cluster, deadline, "initial discovery")

	// Writes from every node converge with identical contents.
	const rowsPerNode = int64(10)
	for i := 0; i < cluster.Len(); i++ {
		for r := int64(0); r < rowsPerNode; r++ {
			insert(t, cluster, i, int64(i)*rowsPerNode+r, fmt.Sprintf("n%d-r%d", i, r))
		}
	}
	var want []string
	for i := 0; i < cluster.Len(); i++ {
		for r := int64(0); r < rowsPerNode; r++ {
			want = append(want, fmt.Sprintf("n%d-r%d", i, r))
		}
	}
	slices.Sort(want)
	waitConvergedOn(t, cluster, []int{0, 1, 2, 3}, want, convergeTimeout, "seed-only mesh")
	failed = false
}

func TestSeedKillAndRediscovery(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:           "swim-rediscovery",
		NumNodes:       4,
		ManualPeers:    true,
		BootstrapSeeds: []int{0},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("rediscovery-no-reconverge")
		}
		cluster.Finish(failed)
	}()

	deadline := discoveryTimeout()
	migrateRecords(t, cluster)
	waitMembership(t, cluster, 3, deadline, "pre-kill discovery")
	waitFullSessionMesh(t, cluster, deadline, "pre-kill discovery")

	// Baseline writes converge before the kill (honest-path control).
	for i := int64(0); i < 8; i++ {
		insert(t, cluster, 2, i, fmt.Sprintf("base-%d", i))
	}
	var want []string
	for i := int64(0); i < 8; i++ {
		want = append(want, fmt.Sprintf("base-%d", i))
	}
	slices.Sort(want)
	waitConvergedOn(t, cluster, []int{0, 1, 2, 3}, want, convergeTimeout, "baseline")

	cluster.StopNode(0)
	waitDeathObserved(t, cluster, []int{1, 2, 3}, deadline)

	// Survivors keep writing while the seed is down.
	for i := int64(0); i < 12; i++ {
		insert(t, cluster, 1+int(i%3), 100+i, fmt.Sprintf("down-%d", i))
	}
	for i := int64(0); i < 12; i++ {
		want = append(want, fmt.Sprintf("down-%d", i))
	}
	slices.Sort(want)
	waitConvergedOn(t, cluster, []int{1, 2, 3}, want, convergeTimeout, "seed down")

	// The restarted seed started life with an empty bootstrap list, so
	// give it one live member to rejoin through (Rust: patchSeedBootstrap
	// to node 2's address, then StartNode): any live member rejoins the
	// whole cluster via gossip, and discovery rebuilds the rest of the
	// mesh from there.
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart seed: %v", err)
	}
	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatalf("seed rejoins via node 2: %v", err)
	}

	waitMembership(t, cluster, 3, deadline, "rediscovery")
	waitFullSessionMesh(t, cluster, deadline, "rediscovery")
	waitConvergedOn(t, cluster, []int{0, 1, 2, 3}, want, convergeTimeout, "reconverge")
	failed = false
}

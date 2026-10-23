// Live scenario: certificate validity alone does not authorize a peer.
//
// Go port of the Rust `allow-nodes` scenario
// (/home/marc/MURMUR/tests-live/allow-nodes/tests/allow_nodes.rs:
// TestAllowedNodeIDsAcrossProcesses). Node2's identity allow-list accepts
// only node1; node1 and node3 accept every identity. All three nodes write
// concurrently for MURMUR_ALLOW_NODES_WRITE_SECONDS (default 180 s, Go
// parity) while the unauthorized node3-to-node2 session must stay absent;
// afterwards every node must hold all rows with identical ordered digests
// (node3's rows reach node2 through node1).
//
// Plain SQL replaces the typed-records HTTP API, and QUIC session state
// replaces connected_peers.
package allownodes

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// Fixed distinct node ids so the allow-list overlay can be built before
// the cluster opens.
var nodeIDs = [][16]byte{
	bytes16(0xA1),
	bytes16(0xB2),
	bytes16(0xC3),
}

func bytes16(b byte) [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = b
	}
	return id
}

// restricted is the node index whose allow-list accepts only node 0 (Rust:
// AllowedPeersByNode {1: {node1}}).
const restricted = 1

const (
	// writeInterval is the per-writer insert cadence (Rust: 100 ms ticker).
	writeInterval = 100 * time.Millisecond
	// initialPeersTimeout is the initial peer-topology deadline (Rust: 20 s).
	initialPeersTimeout = 20 * time.Second
	// convergeTimeout is the convergence deadline after the write window
	// (Rust: 45 s).
	convergeTimeout = 45 * time.Second
	// poll is the convergence poll interval (Rust: 100 ms).
	poll = 100 * time.Millisecond
	// finalPeersTimeout is the final topology re-check bound (Rust: 2 s).
	finalPeersTimeout = 2 * time.Second
)

// peerCount is the live session count (Rust: connectedPeers).
func peerCount(cluster *harness.Cluster, node int) int {
	h := cluster.Node(node).Replication()
	if h == nil {
		return 0
	}
	return h.Status().Peers
}

// peerConnected is the direct-session probe (Rust: debug/peers Connected).
// Peers whose handshakes fail the allow-list never enter the session table,
// so absence also reads as not connected.
func peerConnected(cluster *harness.Cluster, node int, peerID [16]byte) bool {
	h := cluster.Node(node).Replication()
	if h == nil {
		return false
	}
	for _, d := range h.Status().PeersDiagnostics {
		if d.NodeID == peerID && d.Connected {
			return true
		}
	}
	return false
}

// waitForPeers waits until every node's live session count matches want
// (Rust: waitForPeers, 50 ms poll).
func waitForPeers(t *testing.T, cluster *harness.Cluster, want [3]int, timeout time.Duration, what string) {
	t.Helper()
	ok := harness.WaitUntil(timeout, 50*time.Millisecond, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if peerCount(cluster, i) != want[i] {
				return false
			}
		}
		return true
	})
	if !ok {
		actual := make([]int, 0, cluster.Len())
		for i := 0; i < cluster.Len(); i++ {
			actual = append(actual, peerCount(cluster, i))
		}
		t.Fatalf("peer topology %v != want %v within %v (%s)", actual, want, timeout, what)
	}
}

// names returns the canonical ordered names on one node (Rust: TypedNames).
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

func TestAllowedNodeIDsAcrossProcesses(t *testing.T) {
	writeWindow := harness.EnvSeconds("MURMUR_ALLOW_NODES_WRITE_SECONDS", 180)
	// Node2 accepts only node1. Node1 and node3 have no identity filter
	// (an empty allow-list accepts every authenticated identity).
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "allow-nodes",
		NumNodes: 3,
		NodeIDs:  nodeIDs,
		AllowedPeers: map[int]map[[16]byte]bool{
			restricted: {nodeIDs[0]: true},
		},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("allowlist-or-convergence")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("Node %s running in directory %s (repl=%s)", info.Label, info.Dir, info.ReplAddr)
	}
	// Same schema on every node (Rust: TypedRecords fixture tables).
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

	// Node1 sees both peers; node2 sees only node1; node3 sees only
	// node1 (its dials to node2 fail the allow-list handshake).
	waitForPeers(t, cluster, [3]int{2, 1, 1}, initialPeersTimeout, "initial mesh")

	// Concurrent per-node writers (Rust: one worker per node, one insert
	// per tick). Row ids are node-disjoint by construction; names keep
	// the `node{n}-write-{seq}` shape.
	var stop atomic.Bool
	var written [3]atomic.Int64
	var mu sync.Mutex
	var firstErr *string
	var wg sync.WaitGroup
	for node := 0; node < cluster.Len(); node++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db := cluster.Node(node)
			var seq int64
			for {
				time.Sleep(writeInterval)
				if stop.Load() {
					return
				}
				seq++
				name := fmt.Sprintf("node%d-write-%d", node+1, seq)
				id := (int64(node+1) << 32) | seq
				_, txErr := db.Transaction(func(s *murmur.Sql) (any, error) {
					_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
					return nil, err
				})
				var opErr error
				if txErr != nil {
					opErr = txErr
				} else {
					opErr = db.Flush()
				}
				if opErr != nil {
					mu.Lock()
					if firstErr == nil {
						msg := fmt.Sprintf("node%d write %d: %v", node+1, seq, opErr)
						firstErr = &msg
					}
					mu.Unlock()
					return
				}
				written[node].Add(1)
			}
		}()
	}
	time.Sleep(writeWindow)
	stop.Store(true)
	wg.Wait()
	mu.Lock()
	errCopy := firstErr
	mu.Unlock()
	if errCopy != nil {
		t.Fatalf("%s", *errCopy)
	}
	counts := [3]int64{written[0].Load(), written[1].Load(), written[2].Load()}
	want := int(counts[0] + counts[1] + counts[2])
	if want < 3 {
		t.Fatalf("too few writes completed during %v: %d", writeWindow, want)
	}
	t.Logf("wrote node1=%d node2=%d node3=%d rows; node2 allow-list accepts only node1",
		counts[0], counts[1], counts[2])

	// Every node must hold all rows with an identical ordered digest
	// (Rust: NUL-joined names, 45 s deadline, 100 ms poll).
	digestOf := func(t *testing.T, db *murmur.Database) string {
		t.Helper()
		got := names(t, db)
		if len(got) != want {
			return ""
		}
		joined := ""
		for i, n := range got {
			if i > 0 {
				joined += "\x00"
			}
			joined += n
		}
		return joined
	}
	converged := harness.WaitUntil(convergeTimeout, poll, func() bool {
		var digest string
		for i := 0; i < cluster.Len(); i++ {
			joined := digestOf(t, cluster.Node(i))
			if joined == "" {
				return false
			}
			if digest == "" {
				digest = joined
			} else if joined != digest {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("nodes did not converge on %d rows within %v", want, convergeTimeout)
	}
	var digest string
	for i := 0; i < cluster.Len(); i++ {
		got := names(t, cluster.Node(i))
		if len(got) != want {
			t.Fatalf("node%d records=%d, want all %d application writes", i+1, len(got), want)
		}
		joined := digestOf(t, cluster.Node(i))
		if digest == "" {
			digest = joined
		} else if joined != digest {
			t.Fatalf("node%d digest mismatch", i+1)
		}
	}

	// The restricted node must keep just node1 as a direct peer: node3's
	// rows arrive through node1, but no unauthorized direct session may
	// exist in either direction.
	idRestricted := cluster.NodeID(restricted)
	idThird := cluster.NodeID(2)
	if peerConnected(cluster, restricted, idThird) {
		t.Fatal("unauthorized node3-to-node2 session present on node2")
	}
	if peerConnected(cluster, 2, idRestricted) {
		t.Fatal("unauthorized node2-to-node3 session present on node3")
	}
	waitForPeers(t, cluster, [3]int{2, 1, 1}, finalPeersTimeout, "final topology")
	failed = false
}

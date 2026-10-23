// Live scenario: nodes from two databases never peer, even with shared
// TLS trust.
//
// Go port of the Rust `dbid-isolation` scenario
// (/home/marc/MURMUR/tests-live/dbid-isolation/tests/dbid_isolation.rs).
// Two independent 2-node clusters with distinct DBIDs and byte-identical
// schema share CA trust (so TLS cannot be the refusal cause), then are
// cross-peered in both directions: every cross edge must stay
// disconnected for the MURMUR_DBID_ISO_WINDOW_S window (minimum 10 s),
// markers must never cross, identity refusals must be observed on all
// nodes, and both clusters must stay internally healthy. Plain SQL
// replaces the typed-records HTTP API, QUIC session state replaces
// connected_peers, and the accept-side last_error ("incompatible hello")
// replaces the identity-refusal counters.
package dbidisolation

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// Distinct database ids (Rust mints fixed values and asserts they
// differ; fixed values keep the same property deterministically).
var (
	dbidA = [16]byte{0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5}
	dbidB = [16]byte{0xB6, 0xB6, 0xB6, 0xB6, 0xB6, 0xB6, 0xB6, 0xB6, 0xB6, 0xB6, 0xB6, 0xB6, 0xB6, 0xB6, 0xB6, 0xB6}
)

// Distinct node ids across both clusters so no cross-cluster identity can
// alias.
var (
	nodeIDsA = [][16]byte{
		{0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1},
		{0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2},
	}
	nodeIDsB = [][16]byte{
		{0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1},
		{0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2},
	}
)

const (
	// markerA and markerB are the per-cluster markers.
	markerA = "cluster-a-marker"
	markerB = "cluster-b-marker"
	// schemaSQL is byte-identical on all four nodes, so schema
	// agreement cannot be the refusal cause.
	schemaSQL = "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
	// poll is the isolation-window poll interval (Rust: 500 ms).
	poll = 500 * time.Millisecond
	// convergeTimeout is the internal-convergence deadline (Rust: 60 s).
	convergeTimeout = 60 * time.Second
	// peerTimeout is the internal-peer deadline (Rust: 30 s).
	peerTimeout = 30 * time.Second
	// refusalTimeout is the identity-refusal grace wait after the
	// window (Rust: 30 s waitMetricAdvanced).
	refusalTimeout = 30 * time.Second
)

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

func insertMarker(t *testing.T, db *murmur.Database, id int64, name string) {
	t.Helper()
	_, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
		return nil, err
	})
	if err != nil {
		t.Fatalf("insert marker: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush marker: %v", err)
	}
}

// peerCount is the live session count (Rust: connectedPeers).
func peerCount(t *testing.T, cluster *harness.Cluster, node int) int {
	t.Helper()
	h := cluster.Node(node).Replication()
	if h == nil {
		t.Fatalf("node%d replication disabled", node+1)
	}
	return h.Status().Peers
}

// peerConnected is the direct-session probe (Rust: debug/peers
// Connected). Peers whose handshakes fail the DBID check never enter
// the session table, so absence also reads as not connected.
func peerConnected(t *testing.T, cluster *harness.Cluster, node int, peerID [16]byte) bool {
	t.Helper()
	h := cluster.Node(node).Replication()
	if h == nil {
		t.Fatalf("node%d replication disabled", node+1)
	}
	for _, d := range h.Status().PeersDiagnostics {
		if d.NodeID == peerID && d.Connected {
			return true
		}
	}
	return false
}

// recorder is a background last_error recorder. last_error is a single
// last-write-wins slot fed by both accept-side refusals
// (Auth("incompatible hello")) and dial-side failures, and the 5 s
// seed-redial tick recurs both for as long as a cross edge is
// configured. A 500 ms poll systematically misses the accept side on
// nodes whose redial tick phase lands their dial errors last, so every
// latch transition is recorded on a 10 ms poll instead.
type recorder struct {
	mu     sync.Mutex
	log    []recEntry
	stop   atomic.Bool
	joined chan struct{}
}

type recEntry struct {
	at  time.Time
	err string
}

func startRecorder(t *testing.T, cluster *harness.Cluster, idx int) *recorder {
	t.Helper()
	h := cluster.Node(idx).Replication()
	if h == nil {
		t.Fatalf("node%d replication disabled", idx+1)
	}
	r := &recorder{joined: make(chan struct{})}
	go func() {
		defer close(r.joined)
		for !r.stop.Load() {
			// Log every sample, not just transitions: consecutive
			// legs can latch the identical string, and a
			// transitions-only log cannot prove the repeat
			// happened after `since`.
			cur := ""
			if e := h.Status().LastError; e != nil {
				cur = *e
			}
			r.mu.Lock()
			r.log = append(r.log, recEntry{at: time.Now(), err: cur})
			r.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}()
	return r
}

func (r *recorder) sawSince(since time.Time, needle string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.log {
		if !e.at.Before(since) && strings.Contains(e.err, needle) {
			return true
		}
	}
	return false
}

func (r *recorder) stopRecorder() {
	r.stop.Store(true)
	<-r.joined
}

func waitConverged(t *testing.T, cluster *harness.Cluster, want, what string) {
	t.Helper()
	ok := harness.WaitUntil(convergeTimeout, 100*time.Millisecond, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			got := names(t, cluster.Node(i))
			if len(got) != 1 || got[0] != want {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("%s: nodes did not converge on [%s] within %v", what, want, convergeTimeout)
	}
}

func waitPeerConnected(t *testing.T, cluster *harness.Cluster, node int, peerID [16]byte, what string) {
	t.Helper()
	ok := harness.WaitUntil(peerTimeout, 200*time.Millisecond, func() bool {
		return peerConnected(t, cluster, node, peerID)
	})
	if !ok {
		t.Fatalf("%s: peer %s never connected within %v", what, replication.Hex(peerID[:]), peerTimeout)
	}
}

func crossAddPeer(t *testing.T, from *harness.Cluster, fromIdx int, to *harness.Cluster, toIdx int) {
	t.Helper()
	addr := to.NodeInfo(toIdx).ReplAddr
	h := from.Node(fromIdx).Replication()
	if h == nil {
		t.Fatalf("cross add_peer: replication disabled")
	}
	if err := h.AddPeer(addr); err != nil {
		t.Fatalf("cross add_peer: %v", err)
	}
}

func crossRemovePeer(t *testing.T, from *harness.Cluster, fromIdx int, to *harness.Cluster, toIdx int) {
	t.Helper()
	id := to.NodeID(toIdx)
	h := from.Node(fromIdx).Replication()
	if h == nil {
		t.Fatalf("cross remove_peer: replication disabled")
	}
	if err := h.RemovePeer(id); err != nil {
		t.Fatalf("cross remove_peer: %v", err)
	}
}

// assertNoCrossConnection fails fast if any cross-cluster session
// exists: every cross entry must be absent or Connected=false, and no
// node may report more live sessions than its one internal peer (Rust:
// assertNoCrossConnection).
func assertNoCrossConnection(t *testing.T, a, b *harness.Cluster) {
	t.Helper()
	aIDs := [2][16]byte{a.NodeID(0), a.NodeID(1)}
	bIDs := [2][16]byte{b.NodeID(0), b.NodeID(1)}
	for i := 0; i < a.Len(); i++ {
		for _, id := range bIDs {
			if peerConnected(t, a, i, id) {
				t.Fatalf("CROSS CONNECTED: A node%d reports cross peer %s Connected=true", i+1, replication.Hex(id[:]))
			}
		}
		if got := peerCount(t, a, i); got > 1 {
			t.Fatalf("A node%d connected_peers=%d, want <=1 (internal only)", i+1, got)
		}
	}
	for i := 0; i < b.Len(); i++ {
		for _, id := range aIDs {
			if peerConnected(t, b, i, id) {
				t.Fatalf("CROSS CONNECTED: B node%d reports cross peer %s Connected=true", i+1, replication.Hex(id[:]))
			}
		}
		if got := peerCount(t, b, i); got > 1 {
			t.Fatalf("B node%d connected_peers=%d, want <=1 (internal only)", i+1, got)
		}
	}
}

func assertMarkersIsolated(t *testing.T, a, b *harness.Cluster) {
	t.Helper()
	for i := 0; i < a.Len(); i++ {
		got := names(t, a.Node(i))
		if len(got) != 1 || got[0] != markerA {
			t.Fatalf("A node%d holds foreign or missing rows: %v", i+1, got)
		}
	}
	for i := 0; i < b.Len(); i++ {
		got := names(t, b.Node(i))
		if len(got) != 1 || got[0] != markerB {
			t.Fatalf("B node%d holds foreign or missing rows: %v", i+1, got)
		}
	}
}

func TestCrossDatabasePeersNeverConnect(t *testing.T) {
	window := harness.EnvSeconds("MURMUR_DBID_ISO_WINDOW_S", 10)
	if window < 10*time.Second {
		t.Fatalf("isolation window %v below the 10s minimum", window)
	}

	clusterA, err := harness.NewCluster(harness.ClusterOptions{
		Name:       "dbid-iso-a",
		NumNodes:   2,
		DatabaseID: &dbidA,
		NodeIDs:    nodeIDsA,
	})
	if err != nil {
		t.Fatalf("cluster A setup: %v", err)
	}
	clusterB, err := harness.NewCluster(harness.ClusterOptions{
		Name:       "dbid-iso-b",
		NumNodes:   2,
		DatabaseID: &dbidB,
		NodeIDs:    nodeIDsB,
	})
	if err != nil {
		t.Fatalf("cluster B setup: %v", err)
	}
	if clusterA.DatabaseID() == clusterB.DatabaseID() {
		t.Fatalf("clusters share a DBID; isolation test is vacuous")
	}
	failed := true
	defer func() {
		if failed {
			clusterA.DumpForensics("dbid-isolation-a")
			clusterB.DumpForensics("dbid-isolation-b")
		}
		clusterA.Finish(failed)
		clusterB.Finish(failed)
	}()

	// Share CA trust so cross dials pass TLS and reach the DBID check
	// (Rust crossTrustCAs: append PEM + restart all nodes).
	caA := clusterA.CaDER()
	caB := clusterB.CaDER()
	clusterA.AddTrustRoot(caB)
	clusterB.AddTrustRoot(caA)
	for i := 0; i < clusterA.Len(); i++ {
		clusterA.StopNode(i)
	}
	for i := 0; i < clusterB.Len(); i++ {
		clusterB.StopNode(i)
	}
	for i := 0; i < clusterA.Len(); i++ {
		if err := clusterA.StartNode(i); err != nil {
			t.Fatalf("restart A node: %v", err)
		}
	}
	for i := 0; i < clusterB.Len(); i++ {
		if err := clusterB.StartNode(i); err != nil {
			t.Fatalf("restart B node: %v", err)
		}
	}
	if err := clusterA.Mesh(); err != nil {
		t.Fatalf("re-mesh A: %v", err)
	}
	if err := clusterB.Mesh(); err != nil {
		t.Fatalf("re-mesh B: %v", err)
	}

	for i := 0; i < clusterA.Len(); i++ {
		if err := clusterA.EnsureMigrated(i, schemaSQL); err != nil {
			t.Fatalf("migrate A node: %v", err)
		}
	}
	for i := 0; i < clusterA.Len(); i++ {
		if err := clusterA.Node(i).SetReplication("records", true); err != nil {
			t.Fatalf("opt-in records: %v", err)
		}
		if err := clusterA.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}
	for i := 0; i < clusterB.Len(); i++ {
		if err := clusterB.EnsureMigrated(i, schemaSQL); err != nil {
			t.Fatalf("migrate B node: %v", err)
		}
	}
	for i := 0; i < clusterB.Len(); i++ {
		if err := clusterB.Node(i).SetReplication("records", true); err != nil {
			t.Fatalf("opt-in records: %v", err)
		}
		if err := clusterB.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}

	// Positive controls: each cluster converges internally.
	insertMarker(t, clusterA.Node(0), 1, markerA)
	insertMarker(t, clusterB.Node(0), 2, markerB)
	waitConverged(t, clusterA, markerA, "cluster A")
	waitConverged(t, clusterB, markerB, "cluster B")
	waitPeerConnected(t, clusterA, 0, clusterA.NodeID(1), "cluster A internal")
	waitPeerConnected(t, clusterB, 0, clusterB.NodeID(1), "cluster B internal")

	// Refusal recorders on every node: started before the first cross
	// dial so the initial burst is captured.
	recA := []*recorder{startRecorder(t, clusterA, 0), startRecorder(t, clusterA, 1)}
	recB := []*recorder{startRecorder(t, clusterB, 0), startRecorder(t, clusterB, 1)}
	dialStart := time.Now()

	// Cross-AddPeer in both directions (full bipartite).
	for i := 0; i < clusterA.Len(); i++ {
		for j := 0; j < clusterB.Len(); j++ {
			crossAddPeer(t, clusterA, i, clusterB, j)
			crossAddPeer(t, clusterB, j, clusterA, i)
		}
	}

	// Isolation window: cross peers never connect, markers never cross.
	windowEnd := time.Now().Add(window)
	for time.Now().Before(windowEnd) {
		assertNoCrossConnection(t, clusterA, clusterB)
		assertMarkersIsolated(t, clusterA, clusterB)
		time.Sleep(poll)
	}
	assertNoCrossConnection(t, clusterA, clusterB)
	assertMarkersIsolated(t, clusterA, clusterB)

	// The refusal cause must be identity (DBID) on every node. The
	// seed-redial tick recurs cross dials on its own; this grace wait
	// mirrors the 30 s waitMetricAdvanced.
	refused := func(recs []*recorder) []bool {
		out := make([]bool, 0, len(recs))
		for _, r := range recs {
			out = append(out, r.sawSince(dialStart, "incompatible"))
		}
		return out
	}
	ok := harness.WaitUntil(refusalTimeout, poll, func() bool {
		for _, r := range refused(recA) {
			if !r {
				return false
			}
		}
		for _, r := range refused(recB) {
			if !r {
				return false
			}
		}
		return true
	})
	seenA, seenB := refused(recA), refused(recB)
	for _, r := range recA {
		r.stopRecorder()
	}
	for _, r := range recB {
		r.stopRecorder()
	}
	if !ok {
		t.Fatalf("identity refusals did not advance on every node within %v (A=%v B=%v)", refusalTimeout, seenA, seenB)
	}
	// Cleanup: remove every cross edge, then re-verify internal health.
	for i := 0; i < clusterA.Len(); i++ {
		for j := 0; j < clusterB.Len(); j++ {
			crossRemovePeer(t, clusterA, i, clusterB, j)
			crossRemovePeer(t, clusterB, j, clusterA, i)
		}
	}
	waitConverged(t, clusterA, markerA, "cluster A after cleanup")
	waitConverged(t, clusterB, markerB, "cluster B after cleanup")
	assertNoCrossConnection(t, clusterA, clusterB)
	failed = false
}

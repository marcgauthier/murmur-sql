// Live scenario: an expired node certificate is rejected on fresh
// handshakes, and rotation heals the mesh.
//
// Go port of the Rust `cert-lifecycle` scenario
// (/home/marc/MURMUR/tests-live/cert-lifecycle/tests/cert_lifecycle.rs). A
// node's certificate is swapped for a short-expiry cert minted for the same
// NodeID; the mesh must stay connected pre-expiry. After NotAfter passes,
// fresh handshakes must reject the node (no session, peers 1->0 on the
// survivor, markers never cross during the isolation window), with the
// served cert parsed at assert time to prove it is genuinely expired.
// Rotating to a fresh cert must fully reconverge the mesh. Plain SQL
// replaces the typed-records HTTP API, and QUIC session state replaces
// debug/peers / connected_peers.
package certlifecycle

import (
	"crypto/x509"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

const (
	// pollFast is the convergence poll interval (Rust: 100ms).
	pollFast = 100 * time.Millisecond
	// poll is the session poll interval (Rust: 200ms).
	poll = 200 * time.Millisecond
	// convergeTimeout is the convergence deadline (Rust: 60s).
	convergeTimeout = 60 * time.Second
	// peerTimeout is the session-establishment deadline (Rust: 30s
	// pre-expiry).
	peerTimeout = 30 * time.Second
	// rejectTimeout is the rejection deadline (Rust: 45s).
	rejectTimeout = 45 * time.Second
	// skew is the wall-clock skew budget for the minted-vs-served NotAfter
	// check (Rust: 120s).
	skew = 120 * time.Second
)

// ddl is the plain-SQL stand-in for the typed-records fixture table.
const ddl = "CREATE TABLE records(name TEXT NOT NULL PRIMARY KEY)"

// sorted renders the sorted expectation list (Rust: sorted).
func sorted(names ...string) []string {
	out := append([]string(nil), names...)
	slices.Sort(out)
	return out
}

// queryNames reads all row names on one node, sorted (Rust: names).
func queryNames(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM records")
	})
	if err != nil {
		t.Fatalf("read record names: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	slices.Sort(out)
	return out
}

// insertOne inserts one row (Rust: insert).
func insertOne(t *testing.T, cluster *harness.Cluster, node int, name string) {
	t.Helper()
	if _, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(name) VALUES(?)", name)
		return nil, err
	}); err != nil {
		t.Fatalf("node%d insert failed: %v", node, err)
	}
	if err := cluster.Node(node).Flush(); err != nil {
		t.Fatalf("node%d flush after insert: %v", node, err)
	}
}

// waitConverged waits until every node holds exactly want (Rust:
// wait_converged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want []string, timeout time.Duration, what string) {
	t.Helper()
	converged := harness.WaitUntil(timeout, pollFast, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			v, err := cluster.Node(i).Read(func(s *murmur.Sql) (any, error) {
				return s.Query("SELECT name FROM records")
			})
			if err != nil {
				return false
			}
			var got []string
			for _, row := range v.([][]sqlite.Value) {
				got = append(got, row[0].Str)
			}
			slices.Sort(got)
			if !slices.Equal(got, want) {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("nodes did not converge on %q within %v (%s)", want, timeout, what)
	}
}

// peerCount reads the live session count (Rust: peer_count).
func peerCount(t *testing.T, cluster *harness.Cluster, node int) int {
	t.Helper()
	h := cluster.Node(node).Replication()
	if h == nil {
		t.Fatalf("node%d has no replication", node+1)
	}
	return h.Status().Peers
}

// peerConnected reports whether a live session to id exists (Rust:
// peer_connected). Peers whose handshakes fail never enter the session
// table, so absence also reads as not connected.
func peerConnected(cluster *harness.Cluster, node int, id [16]byte) bool {
	h := cluster.Node(node).Replication()
	if h == nil {
		return false
	}
	for _, d := range h.Status().PeersDiagnostics {
		if d.NodeID == id && d.Connected {
			return true
		}
	}
	return false
}

func waitPeerConnected(t *testing.T, cluster *harness.Cluster, node int, id [16]byte, want bool, timeout time.Duration, what string) {
	t.Helper()
	seen := harness.WaitUntil(timeout, poll, func() bool {
		return peerConnected(cluster, node, id) == want
	})
	if !seen {
		t.Fatalf("peer %x Connected != %v on node%d within %v (%s)", id, want, node+1, timeout, what)
	}
}

func waitPeerCount(t *testing.T, cluster *harness.Cluster, node, want int, timeout time.Duration, what string) {
	t.Helper()
	seen := harness.WaitUntil(timeout, poll, func() bool {
		h := cluster.Node(node).Replication()
		return h != nil && h.Status().Peers == want
	})
	if !seen {
		t.Fatalf("peers(node%d) != %d within %v (%s)", node+1, want, timeout, what)
	}
}

// servedNotAfter reads NotAfter of the leaf certificate node idx currently
// serves (Rust: served_not_after). The harness rewrites tls/ from the same
// material passed to open, so the file NotAfter always equals the served
// NotAfter.
func servedNotAfter(t *testing.T, cluster *harness.Cluster, node int) time.Time {
	t.Helper()
	der, err := os.ReadFile(filepath.Join(cluster.NodeInfo(node).Dir, "tls", "node.der"))
	if err != nil {
		t.Fatalf("read served leaf: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse served leaf DER: %v", err)
	}
	if cert.NotAfter.IsZero() || cert.NotAfter.Unix() <= 0 {
		t.Fatal("served cert NotAfter predates the epoch")
	}
	return cert.NotAfter
}

// TestCertExpiryRejectsAndRotationHeals expires a node's certificate,
// requires fresh handshakes to reject it, then heals via rotation (Rust:
// cert_expiry_rejects_and_rotation_heals).
func TestCertExpiryRejectsAndRotationHeals(t *testing.T) {
	ttl := harness.EnvSeconds("MURMUR_CERT_LIFECYCLE_TTL_S", 25)
	window := harness.EnvSeconds("MURMUR_CERT_LIFECYCLE_WINDOW_S", 10)
	if ttl < 15*time.Second {
		t.Fatalf("short-cert TTL %v too small to converge pre-expiry; raise MURMUR_CERT_LIFECYCLE_TTL_S", ttl)
	}
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "cert-lifecycle",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("cert-lifecycle")
		}
		cluster.Finish(failed)
	}()

	const a = 0
	const b = 1
	idA := cluster.NodeID(a)
	idB := cluster.NodeID(b)

	// Same schema on every node.
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

	// Phase 0: positive control, healthy mesh converges.
	insertOne(t, cluster, a, "marker-init")
	waitConverged(t, cluster, sorted("marker-init"), convergeTimeout, "phase0 positive control")
	waitPeerConnected(t, cluster, a, idB, true, peerTimeout, "phase0 session")
	if n := peerCount(t, cluster, a); n != 1 {
		t.Fatalf("pre-expiry peers(A) = %d, want 1", n)
	}

	// Phase 1: stop B, rotate to a short-expiry cert for the same NodeID.
	cluster.StopNode(b)
	if err := cluster.RotateNodeCert(b, &ttl); err != nil {
		t.Fatalf("rotate B to short-expiry cert: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after rotation: %v", err)
	}
	t.Logf("B rotated to a short-expiry cert (TTL %v) for the same NodeID", ttl)

	// Prove B actually serves the short cert (not a stale one): the served
	// leaf's remaining lifetime must fit the minted TTL.
	now := time.Now()
	notAfter := servedNotAfter(t, cluster, b)
	remaining := notAfter.Sub(now)
	if remaining <= 0 {
		t.Fatal("short cert already expired before the pre-expiry leg")
	}
	if remaining > ttl+skew {
		t.Fatalf("served cert expires in %v, beyond minted TTL %v (+%v skew): stale long-lived cert still served?", remaining, ttl, skew)
	}
	t.Logf("short cert expires at %v (in %v)", notAfter, remaining)
	preDeadline := remaining - 5*time.Second
	if preDeadline <= 0 {
		t.Fatalf("no pre-expiry budget left (NotAfter %v)", notAfter)
	}

	// Assert connected pre-expiry: session plus a converged marker.
	waitPeerConnected(t, cluster, a, idB, true, preDeadline, "phase1 pre-expiry session")
	insertOne(t, cluster, a, "marker-pre-expiry")
	waitConverged(t, cluster, sorted("marker-init", "marker-pre-expiry"), preDeadline, "phase1 pre-expiry marker")

	// Phase 2: wait out the TTL, then force fresh handshakes by restarting
	// B. B opens fine with an expired cert (open checks the SAN identity,
	// not validity); its handshakes must fail.
	t.Logf("waiting for cert expiry at %v", notAfter)
	postExpiry := notAfter.Add(2 * time.Second)
	for time.Now().Before(postExpiry) {
		time.Sleep(250 * time.Millisecond)
	}
	cluster.StopNode(b)
	if err := cluster.StartNode(b); err != nil {
		t.Fatalf("restart B with expired cert: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}

	// B is rejected: survivor sees no session and peers drops 1->0.
	waitPeerConnected(t, cluster, a, idB, false, rejectTimeout, "phase2 rejection")
	waitPeerCount(t, cluster, a, 0, rejectTimeout, "phase2 peers drop")
	// B's own view agrees.
	waitPeerConnected(t, cluster, b, idA, false, rejectTimeout, "phase2 B-side view")

	// Non-vacuity: at assert time, the cert B serves is expired.
	servedAfter := servedNotAfter(t, cluster, b)
	if !servedAfter.Before(time.Now()) {
		t.Fatalf("served cert NotAfter = %v, want it in the past", servedAfter)
	}
	t.Logf("rejection confirmed while serving expired cert (NotAfter %v)", servedAfter)

	// A-side marker written post-expiry must never reach B during the
	// isolation window, while A keeps serving it. The dial loop keeps
	// redialing (explicit mesh plus the 5s bootstrap retick), so a steady
	// zero proves rejection rather than idleness.
	insertOne(t, cluster, a, "marker-post-expiry")
	windowEnd := time.Now().Add(window)
	for {
		for _, n := range queryNames(t, cluster.Node(b)) {
			if n == "marker-post-expiry" {
				t.Fatal("post-expiry marker reached expired node B")
			}
		}
		if n := peerCount(t, cluster, a); n != 0 {
			t.Fatalf("peers(A) during isolation window = %d, want 0", n)
		}
		if !time.Now().Before(windowEnd) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if n := len(queryNames(t, cluster.Node(a))); n != 3 {
		t.Fatalf("A markers after isolation window = %d, want 3", n)
	}

	// Phase 3: rotate to a fresh cert and assert full reconvergence.
	cluster.StopNode(b)
	fresh := 24 * time.Hour
	if err := cluster.RotateNodeCert(b, &fresh); err != nil {
		t.Fatalf("rotate B to fresh cert: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after rotation: %v", err)
	}
	waitPeerConnected(t, cluster, a, idB, true, convergeTimeout, "phase3 session")
	waitConverged(t, cluster, sorted("marker-init", "marker-pre-expiry", "marker-post-expiry"), convergeTimeout, "phase3 heal")
	// Bidirectional proof: a B-side write converges too.
	insertOne(t, cluster, b, "marker-rotated")
	healed := sorted("marker-init", "marker-pre-expiry", "marker-post-expiry", "marker-rotated")
	waitConverged(t, cluster, healed, convergeTimeout, "phase3 bidirectional")
	if !slices.Equal(queryNames(t, cluster.Node(a)), queryNames(t, cluster.Node(b))) {
		t.Fatal("post-rotation names differ")
	}
	t.Logf("rotation healed mesh; names=%q", queryNames(t, cluster.Node(a)))

	failed = false
}

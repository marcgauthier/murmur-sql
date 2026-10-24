// Live scenario: a node's certificate is swapped for a short-expiry
// cert minted for the same NodeID; the mesh must stay connected
// pre-expiry, reject the node after NotAfter passes, and fully
// reconverge on rotation to a fresh cert.
//
// Go port of tests/cert_lifecycle.rs using tests-live/goharness.
// Identical behavior: healthy-mesh positive control, crash plus
// rotation to a short cert with stale-cert guards, pre-expiry session
// and marker convergence, post-expiry rejection (no session,
// connected_peers 1->0, isolation window), served-leaf non-vacuity
// checks, then fresh-cert rotation with bidirectional reconvergence.
package certlifecycle

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// Roles.
const (
	a = 0
	b = 1
)

const (
	// pollFast is the convergence poll interval.
	pollFast = 100 * time.Millisecond
	// poll is the session poll interval.
	poll = 200 * time.Millisecond
	// windowPoll is the isolation-window poll.
	windowPoll = 500 * time.Millisecond
	// convergeTimeout is the convergence deadline.
	convergeTimeout = 60 * time.Second
	// peerTimeout is the session-establishment deadline.
	peerTimeout = 30 * time.Second
	// rejectTimeout is the rejection deadline.
	rejectTimeout = 45 * time.Second
	// preExpiryMargin is the pre-expiry safety margin before NotAfter.
	preExpiryMargin = 5 * time.Second
	// postExpirySettle is the post-expiry settle past NotAfter before
	// forcing fresh handshakes.
	postExpirySettle = 2 * time.Second
)

// names returns the sorted names on one node.
func names(cluster *goharness.Cluster, idx int) ([]string, error) {
	rows, err := cluster.Node(idx).Query("SELECT name FROM records ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("node%d SELECT names: %w", idx+1, err)
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		name, err := row.String(0)
		if err != nil {
			return nil, fmt.Errorf("node%d decode name: %w", idx+1, err)
		}
		out = append(out, name)
	}
	return out, nil
}

// insert inserts one marker row.
func insert(cluster *goharness.Cluster, node int, id int64, name string) error {
	if _, err := cluster.Node(node).Execute(
		"INSERT INTO records(id, name) VALUES($1, $2)", id, name); err != nil {
		return fmt.Errorf("node%d insert %q: %w", node+1, name, err)
	}
	if _, err := cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("node%d flush after %q: %w", node+1, name, err)
	}
	return nil
}

// waitConverged waits until both nodes hold exactly want rows with equal
// ordered names.
func waitConverged(cluster *goharness.Cluster, want []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		na, err := names(cluster, a)
		if err != nil {
			return err
		}
		nb, err := names(cluster, b)
		if err != nil {
			return err
		}
		if equalStrings(na, want) && equalStrings(nb, want) {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("nodes did not converge on %v within %v", want, timeout)
		}
		time.Sleep(pollFast)
	}
}

// peerConnected reports whether from currently holds an open session to
// target (MURMUR SHOW PEERS).
func peerConnected(cluster *goharness.Cluster, from, target int) (bool, error) {
	targetID := cluster.NodeID(target)
	want := goharness.Hex(targetID[:])
	rows, err := cluster.Node(from).Simple("MURMUR SHOW PEERS")
	if err != nil {
		return false, fmt.Errorf("node%d SHOW PEERS: %w", from+1, err)
	}
	for _, row := range rows {
		if len(row) > 0 && strings.EqualFold(row[0], want) && len(row) > 2 && row[2] == "true" {
			return true, nil
		}
	}
	return false, nil
}

// connectedPeers returns the connected sessions on one node (the peers
// column of MURMUR SHOW REPLICATION).
func connectedPeers(cluster *goharness.Cluster, idx int) (int64, error) {
	rows, err := cluster.Node(idx).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		return 0, fmt.Errorf("node%d SHOW REPLICATION: %w", idx+1, err)
	}
	peers := ""
	if len(rows) > 0 && len(rows[0]) > 2 {
		peers = rows[0][2]
	}
	n, err := strconv.ParseInt(peers, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("node%d unparsable peers %q", idx+1, peers)
	}
	return n, nil
}

func waitPeerConnected(cluster *goharness.Cluster, from, target int, want bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		got, err := peerConnected(cluster, from, target)
		if err != nil {
			return err
		}
		if got == want {
			return nil
		}
		if !time.Now().Before(deadline) {
			targetID := cluster.NodeID(target)
			return fmt.Errorf("peer %s Connected != %v on node%d within %v",
				goharness.Hex(targetID[:]), want, from+1, timeout)
		}
		time.Sleep(poll)
	}
}

func waitConnectedPeers(cluster *goharness.Cluster, idx int, want int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		got, err := connectedPeers(cluster, idx)
		if err != nil {
			return err
		}
		if got == want {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("connected_peers != %d on node%d within %v", want, idx+1, timeout)
		}
		time.Sleep(poll)
	}
}

// servedLeaf returns the leaf certificate bytes node B currently serves.
// The harness rewrites tls/node.pem from the same material the restarted
// server loads, so file bytes always equal the served leaf; expiry itself
// is proven against the minted TTL clock (see the scenario README.md).
func servedLeaf(cluster *goharness.Cluster) ([]byte, error) {
	path := filepath.Join(cluster.NodeInfo(b).Dir, "tls", "node.pem")
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read served leaf %s: %w", path, err)
	}
	return body, nil
}

func servedLeafPath(cluster *goharness.Cluster) string {
	return filepath.Join(cluster.NodeInfo(b).Dir, "tls", "node.pem")
}

func runBody(cluster *goharness.Cluster, ttl, window time.Duration) error {
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i,
			"CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
			return fmt.Errorf("migrate node%d: %w", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			return fmt.Errorf("node%d SET TABLE REPLICATED: %w", i+1, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("node%d flush after SET TABLE: %w", i+1, err)
		}
	}

	// Phase 0: positive control, healthy mesh converges.
	if err := insert(cluster, a, 1, "marker-init"); err != nil {
		return err
	}
	if err := waitConverged(cluster, []string{"marker-init"}, convergeTimeout); err != nil {
		return err
	}
	if err := waitPeerConnected(cluster, a, b, true, peerTimeout); err != nil {
		return err
	}
	if n, err := connectedPeers(cluster, a); err != nil {
		return err
	} else if n != 1 {
		return fmt.Errorf("pre-expiry connected_peers(A) = %d, want 1", n)
	}

	// Phase 1: crash B (no graceful QUIC close: the survivor keeps a
	// zombie session), rotate to a short-expiry cert for the same NodeID.
	cluster.KillNode(b)
	leafBefore, err := servedLeaf(cluster)
	if err != nil {
		return err
	}
	rotateStart := time.Now()
	if err := cluster.RotateNodeCert(b, &ttl); err != nil {
		return fmt.Errorf("rotate node B to short cert: %w", err)
	}
	mintedAt := time.Now()
	if err := cluster.Mesh(); err != nil {
		return fmt.Errorf("re-mesh after rotation: %w", err)
	}
	// Stale-cert guard: the served leaf must be fresh material (rewritten
	// at rotation time), not the pre-rotation cert.
	leafShort, err := servedLeaf(cluster)
	if err != nil {
		return err
	}
	if bytes.Equal(leafShort, leafBefore) {
		return fmt.Errorf("served leaf unchanged by rotation (stale cert?)")
	}
	info, err := os.Stat(servedLeafPath(cluster))
	if err != nil {
		return fmt.Errorf("served leaf mtime: %w", err)
	}
	if time.Since(info.ModTime()) > time.Since(rotateStart)+5*time.Second {
		return fmt.Errorf("served leaf mtime predates the rotation (stale cert?)")
	}
	notAfter := mintedAt.Add(ttl)
	fmt.Fprintf(os.Stderr, "short cert expires in %v\n", ttl)

	// Assert connected pre-expiry: session plus a converged marker, all
	// within NotAfter minus the safety margin.
	preBudget := time.Until(notAfter) - preExpiryMargin
	if preBudget <= 0 {
		return fmt.Errorf("no pre-expiry budget left")
	}
	budget := min(preBudget, peerTimeout)
	if err := waitPeerConnected(cluster, a, b, true, budget); err != nil {
		return err
	}
	if err := insert(cluster, a, 2, "marker-pre-expiry"); err != nil {
		return err
	}
	preLeft := time.Until(notAfter) - preExpiryMargin
	if preLeft < 0 {
		preLeft = 0
	}
	if err := waitConverged(cluster,
		[]string{"marker-init", "marker-pre-expiry"}, preLeft); err != nil {
		return err
	}

	// Phase 2: wait out the TTL, then force fresh handshakes by
	// restarting B on the same expired leaf. An established session is not
	// re-validated mid-connection, so the restart forces both sides to
	// re-handshake. B's server starts fine with an expired cert (open
	// checks the SAN identity, not validity); its handshakes must fail.
	fmt.Fprintln(os.Stderr, "waiting for cert expiry")
	expiryDeadline := notAfter.Add(postExpirySettle)
	for time.Now().Before(expiryDeadline) {
		time.Sleep(250 * time.Millisecond)
	}
	cluster.StopNode(b)
	if err := cluster.StartNode(b); err != nil {
		return fmt.Errorf("restart node B on expired cert: %w", err)
	}
	if err := cluster.Mesh(); err != nil {
		return fmt.Errorf("re-mesh after expiry restart: %w", err)
	}

	// B is rejected: survivor sees no session and connected_peers 1->0.
	if err := waitPeerConnected(cluster, a, b, false, rejectTimeout); err != nil {
		return err
	}
	if err := waitConnectedPeers(cluster, a, 0, rejectTimeout); err != nil {
		return err
	}
	// B's own view agrees (its PG port is unaffected by the QUIC cert).
	if err := waitPeerConnected(cluster, b, a, false, rejectTimeout); err != nil {
		return err
	}

	// Non-vacuity: at assert time, the cert B serves is expired: the
	// served material is byte-identical to the short leaf minted in
	// phase 1, and its TTL has fully elapsed.
	current, err := servedLeaf(cluster)
	if err != nil {
		return err
	}
	if !bytes.Equal(current, leafShort) {
		return fmt.Errorf("served leaf changed across the expiry restart (wrong cert under test?)")
	}
	if time.Now().Before(notAfter) {
		return fmt.Errorf("asserting rejection before the served cert expired")
	}
	fmt.Fprintln(os.Stderr, "rejection confirmed while serving the expired short cert")

	// An A-side marker written post-expiry must never reach B during the
	// isolation window, while A keeps serving it.
	if err := insert(cluster, a, 3, "marker-post-expiry"); err != nil {
		return err
	}
	windowEnd := time.Now().Add(window)
	for {
		nb, err := names(cluster, b)
		if err != nil {
			return err
		}
		for _, n := range nb {
			if n == "marker-post-expiry" {
				return fmt.Errorf("post-expiry marker reached expired node B")
			}
		}
		if n, err := connectedPeers(cluster, a); err != nil {
			return err
		} else if n != 0 {
			return fmt.Errorf("connected_peers(A) = %d during isolation window, want 0", n)
		}
		if !time.Now().Before(windowEnd) {
			break
		}
		time.Sleep(windowPoll)
	}
	nb, err := names(cluster, b)
	if err != nil {
		return err
	}
	for _, n := range nb {
		if n == "marker-post-expiry" {
			return fmt.Errorf("post-expiry marker reached expired node B")
		}
	}
	na, err := names(cluster, a)
	if err != nil {
		return err
	}
	if len(na) != 3 {
		return fmt.Errorf("A markers = %d, want 3", len(na))
	}

	// Phase 3: rotate to a fresh cert and assert full reconvergence.
	cluster.StopNode(b)
	freshTTL := 24 * time.Hour
	if err := cluster.RotateNodeCert(b, &freshTTL); err != nil {
		return fmt.Errorf("rotate node B to fresh cert: %w", err)
	}
	if err := cluster.Mesh(); err != nil {
		return fmt.Errorf("re-mesh after fresh rotation: %w", err)
	}
	if err := waitPeerConnected(cluster, a, b, true, convergeTimeout); err != nil {
		return err
	}
	if err := waitConverged(cluster, []string{
		"marker-init",
		"marker-post-expiry",
		"marker-pre-expiry",
	}, convergeTimeout); err != nil {
		return err
	}
	// Bidirectional proof: a B-side write converges too.
	if err := insert(cluster, b, 4, "marker-rotated"); err != nil {
		return err
	}
	wantAll := []string{
		"marker-init",
		"marker-post-expiry",
		"marker-pre-expiry",
		"marker-rotated",
	}
	if err := waitConverged(cluster, wantAll, convergeTimeout); err != nil {
		return err
	}
	aNames, err := names(cluster, a)
	if err != nil {
		return err
	}
	bNames, err := names(cluster, b)
	if err != nil {
		return err
	}
	if !equalStrings(aNames, bNames) {
		return fmt.Errorf("post-rotation names differ: A=%v B=%v", aNames, bNames)
	}
	fmt.Fprintf(os.Stderr, "rotation healed mesh; names=%v\n", aNames)
	return nil
}

func TestCertExpiryRejectsAndRotationHeals(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	ttl := goharness.EnvSeconds("MURMUR_CERT_LIFECYCLE_TTL_S", 25)
	window := goharness.EnvSeconds("MURMUR_CERT_LIFECYCLE_WINDOW_S", 10)
	if ttl < 15*time.Second {
		t.Fatalf("short-cert TTL %v too small to converge pre-expiry; raise MURMUR_CERT_LIFECYCLE_TTL_S", ttl)
	}

	// Pinned adversarial id order (survivor A < restarted B): the
	// survivor's pre-restart session is then in its tie-break-preferred
	// direction, so a re-mesh that keeps zombies over fresh handshakes
	// stalls every rotation leg deterministically instead of flaking
	// 50/50 on random ids.
	opts := goharness.DefaultClusterOptions("cert-lifecycle")
	opts.NumNodes = 2
	opts.NodeIDs = [][16]byte{repeatByte(0x01), repeatByte(0x02)}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	if cluster.Len() != 2 {
		t.Fatalf("this scenario is written for 2 nodes, got %d", cluster.Len())
	}
	resultErr := runBody(cluster, ttl, window)
	failed := resultErr != nil
	if resultErr != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", resultErr)
	}
	if failed {
		cluster.DumpForensics("cert-lifecycle-rejection")
	}
	cluster.Finish(failed)
	if failed {
		t.Fatalf("scenario failed: %v; artifacts retained under tests-live/failures/", resultErr)
	}
}

func repeatByte(b byte) [16]byte {
	var out [16]byte
	for i := range out {
		out[i] = b
	}
	return out
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

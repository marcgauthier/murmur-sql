// Live scenario: node3 is excluded on the survivors, the exclusion
// survives a survivor restart, re-adding heals replication, and
// retirement releases retention.
//
// Go port of tests/churn_retirement.rs using tests-live/goharness.
// Identical behavior: three meshed servers converge on 5 rows, node3 is
// excluded on both survivors (sessions drop, its writes stop arriving),
// the exclusion survives a survivor restart with no rediscovery
// re-mesh, re-adding heals to 6 rows, and retiring node3 on both
// survivors marks its retention record retired and shrinks the gating
// set. Plain SQL replaces the Go typed-records HTTP API (see the
// scenario README.md).
package churnretirement

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// excluded is the excluded node.
const excluded = 2

const (
	// waitTimeout is the convergence/session deadline.
	waitTimeout = 60 * time.Second
	// poll is the poll interval.
	poll = 200 * time.Millisecond
	// isolationHold is the isolation hold after the excluded node's write.
	isolationHold = 5 * time.Second
	// remeshWatch is the post-restart rediscovery watch.
	remeshWatch = 5 * time.Second
)

// names returns the sorted names on one node.
func names(cluster *goharness.Cluster, idx int) ([]string, error) {
	rows, err := cluster.Node(idx).Query("SELECT name FROM records")
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
	sort.Strings(out)
	return out, nil
}

// insert inserts one row plus a durability barrier.
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

// waitConverged waits until every node holds exactly want rows with equal
// contents.
func waitConverged(cluster *goharness.Cluster, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var first []string
		seen := false
		ok := true
		for i := 0; i < cluster.Len(); i++ {
			nodeNames, err := names(cluster, i)
			if err != nil {
				return err
			}
			if len(nodeNames) != want {
				ok = false
				break
			}
			if !seen {
				first, seen = nodeNames, true
			} else if !equalStrings(first, nodeNames) {
				ok = false
				break
			}
		}
		if ok {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("nodes did not converge on %d rows within %v", want, timeout)
		}
		time.Sleep(poll)
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

// connectedCount returns the open sessions on one node (only rows with
// connected=true count).
func connectedCount(cluster *goharness.Cluster, idx int) (int, error) {
	rows, err := cluster.Node(idx).Simple("MURMUR SHOW PEERS")
	if err != nil {
		return 0, fmt.Errorf("node%d SHOW PEERS: %w", idx+1, err)
	}
	count := 0
	for _, row := range rows {
		if len(row) > 2 && row[2] == "true" {
			count++
		}
	}
	return count, nil
}

// gatingMembers returns the distinct non-retired peers pinning retention
// on one node (MURMUR SHOW RETENTION).
func gatingMembers(cluster *goharness.Cluster, idx int) (map[string]bool, error) {
	rows, err := cluster.Node(idx).Simple("MURMUR SHOW RETENTION")
	if err != nil {
		return nil, fmt.Errorf("node%d SHOW RETENTION: %w", idx+1, err)
	}
	out := map[string]bool{}
	for _, row := range rows {
		if len(row) > 2 && row[2] == "true" {
			continue
		}
		if len(row) > 0 {
			out[row[0]] = true
		}
	}
	return out, nil
}

// peerRetired reports whether target's retention record on from is marked
// retired.
func peerRetired(cluster *goharness.Cluster, from, target int) (bool, error) {
	targetID := cluster.NodeID(target)
	want := goharness.Hex(targetID[:])
	rows, err := cluster.Node(from).Simple("MURMUR SHOW RETENTION")
	if err != nil {
		return false, fmt.Errorf("node%d SHOW RETENTION: %w", from+1, err)
	}
	for _, row := range rows {
		if len(row) > 0 && strings.EqualFold(row[0], want) && len(row) > 2 && row[2] == "true" {
			return true, nil
		}
	}
	return false, nil
}

// waitSessionsDrop waits until sessions to node3 drop on both survivors.
func waitSessionsDrop(cluster *goharness.Cluster, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		c0, err := peerConnected(cluster, 0, excluded)
		if err != nil {
			return err
		}
		c1, err := peerConnected(cluster, 1, excluded)
		if err != nil {
			return err
		}
		if !c0 && !c1 {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("sessions to node3 did not drop within %v", timeout)
		}
		time.Sleep(poll)
	}
}

func runBody(cluster *goharness.Cluster) error {
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

	for i := int64(0); i < 5; i++ {
		if err := insert(cluster, 0, i+1, fmt.Sprintf("a-%d", i)); err != nil {
			return err
		}
	}
	if err := waitConverged(cluster, 5, waitTimeout); err != nil {
		return err
	}
	gatingBefore, err := gatingMembers(cluster, 0)
	if err != nil {
		return err
	}

	// Exclude node3 on both survivors.
	if err := cluster.RemovePeer(0, excluded); err != nil {
		return fmt.Errorf("remove peer 0->2: %w", err)
	}
	if err := cluster.RemovePeer(1, excluded); err != nil {
		return fmt.Errorf("remove peer 1->2: %w", err)
	}
	// Sessions to node3 drop on both survivors.
	if err := waitSessionsDrop(cluster, waitTimeout); err != nil {
		return err
	}
	for _, i := range []int{0, 1} {
		count, err := connectedCount(cluster, i)
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("node%d peers = %d, want 1 after exclusion", i+1, count)
		}
	}
	// Node3's writes no longer arrive on survivors.
	if err := insert(cluster, excluded, 100, "isolated"); err != nil {
		return err
	}
	time.Sleep(isolationHold)
	for _, i := range []int{0, 1} {
		survivor, err := names(cluster, i)
		if err != nil {
			return err
		}
		if len(survivor) != 5 {
			return fmt.Errorf("node%d names after isolation = %v; want five survivors", i+1, survivor)
		}
	}

	// Exclusion survives a survivor restart (no rediscovery re-mesh): the
	// restarted node re-meshes only its fellow survivor (fresh ephemeral
	// port), never the excluded node.
	cluster.StopNode(0)
	if err := cluster.StartNode(0); err != nil {
		return fmt.Errorf("restart node1: %w", err)
	}
	if err := cluster.AddPeer(0, 1); err != nil {
		return fmt.Errorf("re-mesh 0->1: %w", err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		return fmt.Errorf("re-mesh 1->0: %w", err)
	}
	time.Sleep(remeshWatch)
	if remeshed, err := peerConnected(cluster, 0, excluded); err != nil {
		return err
	} else if remeshed {
		return fmt.Errorf("node1 re-meshed excluded node3 after restart")
	}
	if count, err := connectedCount(cluster, 0); err != nil {
		return err
	} else if count != 1 {
		return fmt.Errorf("node1 peers after restart = %d, want 1 (exclusion lost?)", count)
	}

	// Re-adding heals replication (removal was not destructive).
	for _, pair := range [][2]int{{0, 2}, {2, 0}, {1, 2}, {2, 1}} {
		if err := cluster.AddPeer(pair[0], pair[1]); err != nil {
			return fmt.Errorf("re-add peer %d->%d: %w", pair[0], pair[1], err)
		}
	}
	if err := waitConverged(cluster, 6, waitTimeout); err != nil {
		return err
	}

	// The retired peer releases its GC retention pin. This leg runs after
	// the heal (not immediately after exclusion like Go): Rust splits Go's
	// single RemovePeer into reversible exclusion and permanent
	// retirement, and retiring before the re-add would make the heal leg
	// impossible. Every assertion is preserved; only the order changes.
	if err := cluster.RetirePeer(0, excluded); err != nil {
		return fmt.Errorf("retire peer 0->2: %w", err)
	}
	if err := cluster.RetirePeer(1, excluded); err != nil {
		return fmt.Errorf("retire peer 1->2: %w", err)
	}
	for _, i := range []int{0, 1} {
		retired, err := peerRetired(cluster, i, excluded)
		if err != nil {
			return err
		}
		if !retired {
			return fmt.Errorf("node%d shows no retired retention record for node3", i+1)
		}
		gating, err := gatingMembers(cluster, i)
		if err != nil {
			return err
		}
		if len(gating) >= len(gatingBefore) {
			return fmt.Errorf("node%d gating members = %d, want below baseline %d",
				i+1, len(gating), len(gatingBefore))
		}
	}
	return nil
}

func TestExcludePersistsAndReleasesRetention(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	// Pinned adversarial id order (restarted node1 sorts last): both
	// survivors' pre-restart sessions to it are then in their
	// tie-break-preferred direction, so the restart/re-mesh legs exercise
	// zombie replacement deterministically instead of flaking on random
	// ids.
	opts := goharness.DefaultClusterOptions("churn-retirement")
	opts.NodeIDs = [][16]byte{repeatByte(0x03), repeatByte(0x01), repeatByte(0x02)}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	if cluster.Len() != 3 {
		t.Fatalf("this scenario is written for 3 nodes, got %d", cluster.Len())
	}
	resultErr := runBody(cluster)
	failed := resultErr != nil
	if resultErr != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", resultErr)
	}
	if failed {
		cluster.DumpForensics("churn-retirement-no-convergence")
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

// Live scenario: three meshed servers share one logical row: concurrent
// edits to separate cells must all survive, and same-cell contention must
// resolve to one application-visible winner on every process.
//
// Go port of tests/crdt_contention.rs using tests-live/goharness.
// Identical behavior: the Rust tokio::join! over the concurrent updates
// becomes a sync.WaitGroup over goroutines.
package crdtcontention

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// Contended row id.
const rowID = "03141592-6000-4000-8000-000000000000"

// Same-cell updates per node.
const updatesPerNode = 40

// State/poll interval.
const poll = 50 * time.Millisecond

// Mesh deadline.
const peerTimeout = 20 * time.Second

// Baseline/disjoint-state deadlines.
const stateTimeout = 20 * time.Second

// Same-cell convergence deadline.
const convergeTimeout = 30 * time.Second

// row is one logical row.
type row struct {
	name  string
	phone string
	score int64
}

// readRow reads the contended row on one node.
func readRow(cluster *goharness.Cluster, idx int) (row, error) {
	var zero row
	rows, err := cluster.Node(idx).Query("SELECT name, phone, score FROM contention WHERE id = $1", rowID)
	if err != nil {
		return zero, fmt.Errorf("node%d read contention row: %w", idx+1, err)
	}
	if len(rows) == 0 {
		return zero, fmt.Errorf("node%d contention row missing", idx+1)
	}
	name, err := rows[0].String(0)
	if err != nil {
		return zero, fmt.Errorf("node%d decode name: %w", idx+1, err)
	}
	phone, err := rows[0].String(1)
	if err != nil {
		return zero, fmt.Errorf("node%d decode phone: %w", idx+1, err)
	}
	score, err := rows[0].Int64(2)
	if err != nil {
		return zero, fmt.Errorf("node%d decode score: %w", idx+1, err)
	}
	return row{name: name, phone: phone, score: score}, nil
}

// insertRow inserts the baseline row.
func insertRow(cluster *goharness.Cluster, node int, name, phone string, score int64) error {
	if _, err := cluster.Node(node).Execute(
		"INSERT INTO contention(id, name, phone, score) VALUES($1, $2, $3, $4)",
		rowID, name, phone, score); err != nil {
		return fmt.Errorf("node%d baseline insert: %w", node+1, err)
	}
	if _, err := cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("node%d flush after baseline: %w", node+1, err)
	}
	return nil
}

// updateText updates one text cell of the contended row.
func updateText(cluster *goharness.Cluster, node int, field, value string) error {
	if field != "name" && field != "phone" {
		panic(fmt.Sprintf("unexpected text field %s", field))
	}
	sql := fmt.Sprintf("UPDATE contention SET %s = $1 WHERE id = $2", field)
	if _, err := cluster.Node(node).Execute(sql, value, rowID); err != nil {
		return fmt.Errorf("node%d update %s=%q: %w", node+1, field, value, err)
	}
	if _, err := cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("node%d flush after %s update: %w", node+1, field, err)
	}
	return nil
}

// updateScore updates the score cell of the contended row.
func updateScore(cluster *goharness.Cluster, node int, value int64) error {
	if _, err := cluster.Node(node).Execute("UPDATE contention SET score = $1 WHERE id = $2", value, rowID); err != nil {
		return fmt.Errorf("node%d update score=%d: %w", node+1, value, err)
	}
	if _, err := cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("node%d flush after score update: %w", node+1, err)
	}
	return nil
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

// waitForPeers waits until every node reports want sessions.
func waitForPeers(cluster *goharness.Cluster, want int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ok := true
		for i := 0; i < cluster.Len(); i++ {
			n, err := connectedPeers(cluster, i)
			if err != nil {
				return err
			}
			if n != want {
				ok = false
				break
			}
		}
		if ok {
			return nil
		}
		if !time.Now().Before(deadline) {
			for i := 0; i < cluster.Len(); i++ {
				n, err := connectedPeers(cluster, i)
				if err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "node%d connected peers=%d, want %d\n", i+1, n, want)
			}
			return fmt.Errorf("peers did not reach %d on every node within %s", want, timeout)
		}
		time.Sleep(poll)
	}
}

// waitForState waits until every process shows the exact expected row.
func waitForState(cluster *goharness.Cluster, wantName, wantPhone string, wantScore int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	last := make([]string, cluster.Len())
	for {
		ok := true
		for i := 0; i < cluster.Len(); i++ {
			r, err := readRow(cluster, i)
			if err != nil {
				last[i] = fmt.Sprintf("err=%v", err)
				ok = false
				break
			}
			last[i] = fmt.Sprintf("row=%+v", r)
			if r.name != wantName || r.phone != wantPhone || r.score != wantScore {
				ok = false
				break
			}
		}
		if ok {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("timed out waiting for %q/%q/%d on every process; last=%q",
				wantName, wantPhone, wantScore, last)
		}
		time.Sleep(poll)
	}
}

// waitForCellConvergence waits until the name cell agrees everywhere
// while phone/score stay put; it returns the winning name.
func waitForCellConvergence(cluster *goharness.Cluster, wantPhone string, wantScore int64, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	last := make([]string, cluster.Len())
	for {
		winner := ""
		converged := true
		for i := 0; i < cluster.Len(); i++ {
			r, err := readRow(cluster, i)
			if err != nil {
				last[i] = fmt.Sprintf("err=%v", err)
				converged = false
				continue
			}
			last[i] = fmt.Sprintf("name=%q phone=%q score=%d", r.name, r.phone, r.score)
			if r.phone != wantPhone || r.score != wantScore {
				converged = false
			}
			if i == 0 {
				winner = r.name
			} else if r.name != winner {
				converged = false
			}
		}
		if converged {
			return winner, nil
		}
		if !time.Now().Before(deadline) {
			return "", fmt.Errorf("same-cell contention did not converge across %d processes; last=%q",
				cluster.Len(), last)
		}
		time.Sleep(poll)
	}
}

func runBody(cluster *goharness.Cluster) error {
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		pid, ok := cluster.NodePID(i)
		fmt.Fprintf(os.Stderr, "%s pid=%v dir=%s repl=%s\n",
			info.Label, pidOrNone(pid, ok), info.Dir, info.ReplAddr)
	}
	// Same schema on every node, then replicate it: cell convergence
	// needs the per-table flag on each node.
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i,
			"CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)"); err != nil {
			return fmt.Errorf("migrate node%d: %w", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE contention REPLICATED true"); err != nil {
			return fmt.Errorf("node%d SET TABLE REPLICATED: %w", i+1, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("node%d flush after SET TABLE: %w", i+1, err)
		}
	}
	if err := waitForPeers(cluster, 2, peerTimeout); err != nil {
		return err
	}

	if err := insertRow(cluster, 0, "baseline", "baseline", 0); err != nil {
		return err
	}
	if err := waitForState(cluster, "baseline", "baseline", 0, stateTimeout); err != nil {
		return err
	}

	// Concurrent edits to separate cells must all survive regardless of
	// arrival order at other processes.
	var wg sync.WaitGroup
	errs := make([]error, 3)
	wg.Add(3)
	go func() {
		defer wg.Done()
		errs[0] = updateText(cluster, 0, "name", "node1-name")
	}()
	go func() {
		defer wg.Done()
		errs[1] = updateText(cluster, 1, "phone", "node2-phone")
	}()
	go func() {
		defer wg.Done()
		errs[2] = updateScore(cluster, 2, 42)
	}()
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	if err := waitForState(cluster, "node1-name", "node2-phone", 42, stateTimeout); err != nil {
		return err
	}

	// All three processes now contend on the same name cell. The winning
	// value is intentionally not preselected; the LWW version must resolve
	// to the same application-visible result on every process.
	contend := func(node int) error {
		for seq := 0; seq < updatesPerNode; seq++ {
			value := fmt.Sprintf("node%d-cell-%03d", node+1, seq)
			if err := updateText(cluster, node, "name", value); err != nil {
				return err
			}
		}
		return nil
	}
	wg.Add(3)
	for node := 0; node < 3; node++ {
		go func(node int) {
			defer wg.Done()
			errs[node] = contend(node)
		}(node)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	winner, err := waitForCellConvergence(cluster, "node2-phone", 42, convergeTimeout)
	if err != nil {
		return err
	}
	if winner == "" {
		return fmt.Errorf("same-cell contention resolved to an empty name")
	}
	if !strings.HasPrefix(winner, "node1-cell-") &&
		!strings.HasPrefix(winner, "node2-cell-") &&
		!strings.HasPrefix(winner, "node3-cell-") {
		return fmt.Errorf("same-cell winner %q did not come from the concurrent write set", winner)
	}
	first, err := readRow(cluster, 0)
	if err != nil {
		return err
	}
	for i := 1; i < cluster.Len(); i++ {
		other, err := readRow(cluster, i)
		if err != nil {
			return err
		}
		if other != first {
			return fmt.Errorf("logical row differs: node1=%+v node%d=%+v", first, i+1, other)
		}
	}
	fmt.Fprintf(os.Stderr, "three-process cell conflict converged to %q after %d updates per node\n",
		winner, updatesPerNode)
	return nil
}

func pidOrNone(pid int, ok bool) any {
	if !ok {
		return "none"
	}
	return pid
}

func TestMultiProcessDisjointAndSharedCellContention(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	opts := goharness.DefaultClusterOptions("crdt-contention")
	opts.NumNodes = 3
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	if cluster.Len() != 3 {
		t.Fatal("this scenario is written for 3 nodes")
	}
	runErr := runBody(cluster)
	failed := runErr != nil
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", runErr)
	}
	if failed {
		cluster.DumpForensics("crdt-contention-no-convergence")
	}
	cluster.Finish(failed)
	if failed {
		t.Fatal("scenario failed; artifacts retained under tests-live/failures/")
	}
}

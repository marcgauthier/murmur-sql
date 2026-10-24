// Live scenario: background retention must bound log growth while real
// SQL clients keep writing.
//
// Go port of tests/log_boundedness.rs using tests-live/goharness.
// Identical behavior: two nodes with membership off and 1 s retention
// replicate one control row over a single intentional session, then both
// nodes take sustained alternating writes for MURMUR_LOG_BOUNDEDNESS_SECONDS
// (default 75 s); collection must run, retained growth and disk growth
// stay bounded, the replication error latch never moves, and both nodes
// converge on every acknowledged row.
package logboundedness

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// dirBytes sums the file sizes under root, recursing into subdirectories
// (metadata follows symlinks, matching the Rust walk).
func dirBytes(t *testing.T, root string) uint64 {
	t.Helper()
	var total uint64
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read dir %s: %v", root, err)
	}
	for _, e := range entries {
		p := filepath.Join(root, e.Name())
		info, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		if info.IsDir() {
			total += dirBytes(t, p)
			continue
		}
		total += uint64(info.Size())
	}
	return total
}

// satSub saturates at zero (Rust: u64::saturating_sub).
func satSub(a, b uint64) uint64 {
	if a < b {
		return 0
	}
	return a - b
}

type sample struct {
	committed uint64
	collected uint64
	disk      uint64
	err       string
}

func takeSample(t *testing.T, cluster *goharness.Cluster, node int) sample {
	t.Helper()
	status, err := cluster.Node(node).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		t.Fatalf("node %d SHOW REPLICATION: %v", node, err)
	}
	// Append-only extension requested from the server owner: the first seven
	// fields stay compatible with existing SQL clients and snapshot tests.
	if len(status[0]) < 8 {
		t.Fatal("SHOW REPLICATION must expose committed at appended column 8 for exact log-bound testing")
	}
	retained, err := cluster.Node(node).Simple("MURMUR SHOW RETENTION")
	if err != nil {
		t.Fatalf("node %d SHOW RETENTION: %v", node, err)
	}
	var collected uint64
	if len(retained) > 0 {
		collected, err = strconv.ParseUint(retained[0][5], 10, 64)
		if err != nil {
			t.Fatalf("node %d parse collected: %v", node, err)
		}
	}
	committed, err := strconv.ParseUint(status[0][7], 10, 64)
	if err != nil {
		t.Fatalf("node %d parse committed: %v", node, err)
	}
	return sample{
		committed: committed,
		collected: collected,
		disk:      dirBytes(t, cluster.NodeInfo(node).DataDir),
		err:       status[0][6],
	}
}

func TestRetainedSQLLogsAndDiskGrowthStayBoundedUnderSustainedWrites(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	membership := false
	retention := time.Second
	interval := time.Second
	var minimum uint64 = 10
	opts := goharness.DefaultClusterOptions("log-boundedness")
	opts.NumNodes = 2
	opts.ManualPeers = true
	opts.Replication = &goharness.ReplicationOptions{
		Membership:        &membership,
		Retention:         &retention,
		RetentionInterval: &interval,
		MinimumRetained:   &minimum,
	}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	for i := 0; i < 2; i++ {
		if _, err := cluster.Node(i).Simple("CREATE TABLE IF NOT EXISTS contention(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,phone TEXT NOT NULL,score INTEGER NOT NULL)"); err != nil {
			t.Fatalf("node %d create contention: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE contention REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate contention: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush schema: %v", i, err)
		}
	}
	// Establish one intentional session only after both servers are unlocked.
	// This excludes startup bootstrap retries and duplicate-dial closes from
	// the retention measurement, while keeping the no-new-errors assertion.
	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatalf("peer 0 -> 1: %v", err)
	}
	control := [][]string{{
		fmt.Sprintf("%032x", 0),
		fmt.Sprintf("payload-0-%s", strings.Repeat("x", 200)),
		"",
		"0",
	}}
	if _, err := cluster.Node(0).Execute(
		"INSERT INTO contention(id,name,phone,score) VALUES($1,$2,'',0)",
		control[0][0], control[0][1]); err != nil {
		t.Fatalf("insert control row: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush control row: %v", err)
	}
	if err := goharness.AssertRows(cluster, []int{0, 1},
		"SELECT id,name,phone,score FROM contention ORDER BY id", control); err != nil {
		t.Fatalf("converge control row: %v", err)
	}
	duration := 75 * time.Second
	if raw := os.Getenv("MURMUR_LOG_BOUNDEDNESS_SECONDS"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			duration = time.Duration(n) * time.Second
		}
	}
	start := []sample{takeSample(t, cluster, 0), takeSample(t, cluster, 1)}
	var mid []sample
	expected := append([][]string(nil), control...)
	end := time.Now().Add(duration)
	for time.Now().Before(end) {
		seq := len(expected)
		id := fmt.Sprintf("%032x", seq)
		body := fmt.Sprintf("payload-%d-%s", seq, strings.Repeat("x", 200))
		if _, err := cluster.Node(seq%2).Execute(
			"INSERT INTO contention(id,name,phone,score) VALUES($1,$2,'',0)", id, body); err != nil {
			t.Fatalf("insert row %d: %v", seq, err)
		}
		if _, err := cluster.Node(seq % 2).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("flush row %d: %v", seq, err)
		}
		expected = append(expected, []string{id, body, "", "0"})
		if mid == nil && time.Until(end) < duration/2 {
			mid = []sample{takeSample(t, cluster, 0), takeSample(t, cluster, 1)}
		}
	}
	if len(expected) < 100 {
		t.Fatalf("load too small to judge boundedness: %d acknowledged rows", len(expected))
	}
	finish := []sample{takeSample(t, cluster, 0), takeSample(t, cluster, 1)}
	if mid == nil {
		t.Fatal("load never reached midpoint")
	}
	for i := 0; i < 2; i++ {
		produced := satSub(finish[i].committed, start[i].committed)
		collected := satSub(finish[i].collected, start[i].collected)
		if collected == 0 {
			t.Fatalf("background retention did not run on node %d", i)
		}
		// Preserve the legacy growth diagnostic, but do not treat this as
		// an exact count: engine batches and collected logical transactions
		// differ when incoming work is batched. Collection and disk bounds
		// below are checked independently.
		retained := satSub(produced, collected)
		if retained > 512+produced/4 {
			t.Fatalf("node %d retained growth=%d, produced=%d, collected=%d",
				i, retained, produced, collected)
		}
		if finish[i].err != start[i].err {
			t.Fatalf("replication recorded an error under load: %q -> %q",
				start[i].err, finish[i].err)
		}
		growth := satSub(finish[i].disk, start[i].disk)
		if growth > uint64(len(expected))*8192 {
			t.Fatalf("node %d: disk growth %d exceeds 8KiB/row", i, growth)
		}
		first := satSub(mid[i].disk, start[i].disk)
		second := satSub(finish[i].disk, mid[i].disk)
		if second > 2*first+1024*1024 {
			t.Fatalf("node %d: disk growth accelerated: first=%d, second=%d", i, first, second)
		}
		fmt.Fprintf(os.Stderr, "node %d: produced=%d, collected=%d, retained_growth=%d, disk_growth=%d\n",
			i, produced, collected, retained, growth)
	}
	if err := goharness.AssertRows(cluster, []int{0, 1},
		"SELECT id,name,phone,score FROM contention ORDER BY id", expected); err != nil {
		t.Fatalf("converge all rows: %v", err)
	}
	failed = false
}

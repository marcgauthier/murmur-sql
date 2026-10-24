// Live scenario: real SQL writers cross healthy, partition, outage and
// whole-mesh crash phases.
//
// Go port of tests/abuse.rs using tests-live/gosqlharness. Identical
// behavior: N nodes (default 10) with static fanout-2 peering and full
// snapshot-donor trust take a baseline row, min(N,8) concurrent writers
// insert paced rows while the mesh crosses healthy, partition,
// partial-outage and whole-mesh-restart quarters, and every node must
// converge exactly on the acknowledged set. The Rust JoinSet of writer
// tasks becomes a sync.WaitGroup over goroutines, the slot RwLocks
// become sync.RWMutex slots, and the toml_edit closure becomes a
// SetConfig section write.
package abuse

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

// slot is one node's writer connection. A writer holds the read lock
// through SQL+FLUSH; stop takes the write lock to drain acknowledged
// writes before killing that server only.
type slot struct {
	mu     sync.RWMutex
	client *goharness.Client
}

func number(name string, def uint64) uint64 {
	if s, err := strconv.ParseUint(os.Getenv(name), 10, 64); err == nil && s > 0 {
		return s
	}
	return def
}

func mesh(t *testing.T, cluster *gosqlharness.Cluster) {
	t.Helper()
	for from := 0; from < cluster.Len(); from++ {
		for to := 0; to < cluster.Len(); to++ {
			if from != to {
				if _, err := cluster.Node(from).Simple("MURMUR PEER ADD '" +
					cluster.NodeInfo(to).ReplAddr.String() + "'"); err != nil {
					t.Fatalf("mesh %d->%d: %v", from, to, err)
				}
			}
		}
	}
}

func stopNodes(t *testing.T, cluster *gosqlharness.Cluster, slots []*slot, nodes []int) {
	t.Helper()
	for _, i := range nodes {
		// A writer holds this read lock through SQL+FLUSH. Taking the
		// write lock drains acknowledged writes before killing that
		// server only.
		slots[i].mu.Lock()
		func() {
			defer slots[i].mu.Unlock()
			slots[i].client = nil
			if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
				t.Fatalf("node %d pre-stop flush: %v", i, err)
			}
			if err := cluster.StopNode(i); err != nil {
				t.Fatalf("stop node %d: %v", i, err)
			}
		}()
	}
}

func restartNodes(t *testing.T, cluster *gosqlharness.Cluster, slots []*slot, nodes []int) {
	t.Helper()
	for _, i := range nodes {
		if err := cluster.StartNode(i); err != nil {
			t.Fatalf("restart node %d: %v", i, err)
		}
		conn, err := cluster.Connect(i)
		if err != nil {
			t.Fatalf("reconnect node %d: %v", i, err)
		}
		slots[i].mu.Lock()
		slots[i].client = conn
		slots[i].mu.Unlock()
	}
	mesh(t, cluster)
}

func phase(name string, duration, status time.Duration) {
	fmt.Fprintf(os.Stderr, "abuse phase=%s duration=%v\n", name, duration)
	end := time.Now().Add(duration)
	for time.Now().Before(end) {
		left := time.Until(end)
		wait := status
		if wait > left {
			wait = left
		}
		time.Sleep(wait)
		fmt.Fprintf(os.Stderr, "abuse phase=%s remaining=%v\n", name, time.Until(end))
	}
}

func TestAbuseMeshPreservesAcknowledgedWrites(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	nodes := int(number("MURMUR_ABUSE_NODES", 10))
	nodes = min(max(nodes, 3), 256)
	duration := time.Duration(number("MURMUR_ABUSE_DURATION_SECONDS", 600)) * time.Second
	quarter := duration / 4
	pace := time.Duration(number("MURMUR_ABUSE_WRITE_INTERVAL_MS", 100)) * time.Millisecond
	status := time.Duration(number("MURMUR_ABUSE_STATUS_SECONDS", 15)) * time.Second
	settle := time.Duration(number("MURMUR_ABUSE_SETTLE_SECONDS", 900)) * time.Second
	seed := int(number("MURMUR_ABUSE_SEED", 1))

	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:            "abuse",
		NumNodes:        nodes,
		ManualPeers:     true,
		ReplicationYAML: "membership: false\nfanout: 2",
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	all := make([]int, nodes)
	for i := range all {
		all[i] = i
	}
	for i := 0; i < nodes; i++ {
		if _, err := cluster.Node(i).Simple("CREATE TABLE contention(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,phone TEXT NOT NULL,score INTEGER NOT NULL)"); err != nil {
			t.Fatalf("node %d create contention: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE contention REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate contention: %v", i, err)
		}
		var donors []string
		for j := 0; j < nodes; j++ {
			if j != i {
				donors = append(donors, cluster.NodeInfo(j).NodeID)
			}
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush schema: %v", i, err)
		}
		if err := cluster.StopNode(i); err != nil {
			t.Fatalf("stop node %d: %v", i, err)
		}
		if err := cluster.SetConfig(i, "replication", map[string]any{
			"trusted_snapshot_sources": donors,
		}); err != nil {
			t.Fatalf("node %d trust donors: %v", i, err)
		}
		if err := cluster.StartNode(i); err != nil {
			t.Fatalf("restart node %d: %v", i, err)
		}
	}
	mesh(t, cluster)
	if _, err := cluster.Node(0).Simple("INSERT INTO contention(id,name,phone,score) VALUES('baseline','baseline','',0)"); err != nil {
		t.Fatalf("insert baseline: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush baseline: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, all,
		"SELECT id,name,phone,score FROM contention ORDER BY id",
		[][]string{{"baseline", "baseline", "", "0"}}); err != nil {
		t.Fatalf("converge baseline: %v", err)
	}

	slots := make([]*slot, nodes)
	for i := 0; i < nodes; i++ {
		conn, err := cluster.Connect(i)
		if err != nil {
			t.Fatalf("connect node %d: %v", i, err)
		}
		slots[i] = &slot{client: conn}
	}
	var stopWriters atomic.Bool
	var ackedMu sync.Mutex
	acked := map[string]string{"baseline": "baseline"}

	workers := min(nodes, 8)
	var wg sync.WaitGroup
	writerErrs := make([]error, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			seq := 0
			for !stopWriters.Load() {
				target := (seed + worker + seq) % len(slots)
				seq++
				s := slots[target]
				s.mu.RLock()
				client := s.client
				if client != nil {
					id := fmt.Sprintf("w%02d-%010d", worker, seq)
					name := fmt.Sprintf("writer-%d", worker)
					_, err := client.Execute(
						"INSERT INTO contention(id,name,phone,score) VALUES($1,$2,'',0)", id, name)
					if err == nil {
						_, err = client.Simple("MURMUR FLUSH")
					}
					if err != nil {
						writerErrs[worker] = err
						s.mu.RUnlock()
						return
					}
					ackedMu.Lock()
					acked[id] = name
					ackedMu.Unlock()
				}
				s.mu.RUnlock()
				time.Sleep(pace)
			}
		}(worker)
	}

	phase("healthy", quarter, status)
	half := nodes / 2
	for a := 0; a < half; a++ {
		for b := half; b < nodes; b++ {
			for _, pair := range [][2]int{{a, b}, {b, a}} {
				if _, err := cluster.Node(pair[0]).Simple("MURMUR PEER REMOVE '" +
					cluster.NodeInfo(pair[1]).NodeID + "'"); err != nil {
					t.Fatalf("partition %d->%d: %v", pair[0], pair[1], err)
				}
			}
		}
	}
	phase("partition", quarter, status)
	left, err := cluster.Node(0).Simple("SELECT id FROM contention ORDER BY id")
	if err != nil {
		t.Fatalf("read left side: %v", err)
	}
	right, err := cluster.Node(nodes - 1).Simple("SELECT id FROM contention ORDER BY id")
	if err != nil {
		t.Fatalf("read right side: %v", err)
	}
	if equalRows(left, right) {
		t.Fatal("partition failed to isolate acknowledged writes")
	}
	mesh(t, cluster)
	outage := make([]int, 0)
	for i := nodes - (nodes+3)/4; i < nodes; i++ {
		outage = append(outage, i)
	}
	stopNodes(t, cluster, slots, outage)
	ackedMu.Lock()
	before := len(acked)
	ackedMu.Unlock()
	phase("partial-outage", quarter, status)
	ackedMu.Lock()
	after := len(acked)
	ackedMu.Unlock()
	if after <= before {
		t.Fatal("survivors made no progress during outage")
	}
	restartNodes(t, cluster, slots, outage)
	stopNodes(t, cluster, slots, all)
	restartNodes(t, cluster, slots, all)
	phase("whole-mesh-restart", quarter, status)
	stopWriters.Store(true)
	wg.Wait()
	for _, err := range writerErrs {
		if err != nil {
			t.Fatalf("SQL writer failed: %v", err)
		}
	}
	ackedMu.Lock()
	ids := make([]string, 0, len(acked))
	for id := range acked {
		ids = append(ids, id)
	}
	ackedMu.Unlock()
	sort.Strings(ids)
	var expected [][]string
	for _, id := range ids {
		ackedMu.Lock()
		name := acked[id]
		ackedMu.Unlock()
		expected = append(expected, []string{id, name, "", "0"})
	}
	if len(expected) <= 1 {
		t.Fatal("no writes acknowledged during abuse")
	}
	deadline := time.Now().Add(settle)
	for {
		equal := true
		for i := 0; i < nodes; i++ {
			rows, err := cluster.Node(i).Simple("SELECT id,name,phone,score FROM contention ORDER BY id")
			if err != nil {
				t.Fatalf("node %d read contention: %v", i, err)
			}
			equal = equal && equalRows(rows, expected)
		}
		if equal {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("mesh failed exact convergence on %d acknowledged rows", len(expected))
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Fprintf(os.Stderr, "abuse verified %d acknowledged rows on %d SQL servers\n", len(expected), nodes)
	failed = false
}

func equalRows(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

// Live scenario: stale nodes rejoin via snapshot after log GC.
//
// Go port of the Rust `snapshot-resync` scenario
// (/home/marc/MURMUR/tests-live/snapshot-resync/tests/snapshot_resync.rs).
// Three nodes mesh with aggressive log retention; a node stops while the
// survivors write past retention and GC expiry, then restarts with the same
// durable directory. Its needed log ranges are unrecoverable, so it must
// rejoin by merging a snapshot without discarding its acknowledged pre-stop
// state. A second test restarts two stale nodes back-to-back so their
// snapshot requests overlap on one survivor, and a multi-source case gives
// the stale node three snapshot sources concurrently. Plain SQL replaces
// the typed-records HTTP API, close/reopen replaces process stop/start, and
// the snapshot path is proven by flat log-path counters.
package snapshotresync

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/replication"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// retention is the log retention (Rust: MinLogRetentionMs = 1000).
var retention = time.Second

// maximumOffline is the offline-peer memory (Rust:
// MaxOfflineLogRetentionMs = 20000).
var maximumOffline = 20 * time.Second

// minimumRetained is the newest transactions per origin the survivors must
// keep (Rust: MinRetainedBatches = 10).
var minimumRetained = uint64(10)

// minCollected is the minimum pruned transactions per survivor before the
// rejoin: 60 survivor writes minus the 10 retained, with margin. Proves the
// stale node's needed ranges are genuinely unrecoverable from logs, so the
// rejoin must use the snapshot path.
const minCollected = uint64(40)

const (
	// baselineTimeout is the baseline convergence deadline (Rust: 60 s).
	baselineTimeout = 60 * time.Second
	// rowTimeout is the survivor row-count deadline (Rust: 30 s).
	rowTimeout = 30 * time.Second
	// convergePoll is the convergence poll interval (Rust: 200 ms).
	convergePoll = 200 * time.Millisecond
	// collectTimeout is the GC collection-loop deadline past the expiry wait.
	collectTimeout = 60 * time.Second
	// resyncTimeout is the single-stale rejoin convergence deadline
	// (Rust: 90 s).
	resyncTimeout = 90 * time.Second
	// resyncTimeoutLong is the dual/multi-source rejoin convergence
	// deadline (Rust: 120 s).
	resyncTimeoutLong = 120 * time.Second
)

// names returns the canonical ordered names on one node (Rust:
// TypedContentionRows).
func names(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM records ORDER BY name")
	})
	if err != nil {
		t.Fatalf("read names: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

// digest is the canonical digest of a name set (Rust: sorted rows hashed;
// newline-joined names are equally strict here since every row differs by
// name).
func digest(names []string) string {
	return strings.Join(names, "\n")
}

// rowCount returns the row count on one node (cheap pre-check before full
// name reads).
func rowCount(t *testing.T, db *murmur.Database) int {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT COUNT(*) FROM records")
	})
	if err != nil {
		t.Fatalf("row count: %v", err)
	}
	return int(v.([][]sqlite.Value)[0][0].Int)
}

// insert writes one single-row transaction (Rust: TypedContentionInsert).
func insert(t *testing.T, cluster *harness.Cluster, node int, id int64, name string) {
	t.Helper()
	if _, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
		return nil, err
	}); err != nil {
		t.Fatalf("node%d insert failed: %v", node, err)
	}
}

// insertMany writes one multi-row transaction of count rows (Rust:
// TypedContentionInsertMany; the multi-source leg needs snapshot volume,
// not round trips).
func insertMany(t *testing.T, cluster *harness.Cluster, node int, startID, count int64, prefix string) {
	t.Helper()
	if _, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		for n := startID; n < startID+count; n++ {
			if _, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", n, fmt.Sprintf("%s-%d", prefix, n)); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("node%d batch insert failed: %v", node, err)
	}
}

// waitRowCount waits until one node holds exactly want rows (Rust:
// waitRowCount).
func waitRowCount(t *testing.T, cluster *harness.Cluster, idx, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, convergePoll, func() bool {
		return rowCount(t, cluster.Node(idx)) == want
	})
	if !ok {
		t.Fatalf("node%d record count = %d, want %d within %v", idx, rowCount(t, cluster.Node(idx)), want, timeout)
	}
}

// waitConverged waits until every node holds exactly want rows with equal
// digests (Rust: waitConverged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, convergePoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if rowCount(t, cluster.Node(i)) != want {
				return false
			}
		}
		first := digest(names(t, cluster.Node(0)))
		for i := 1; i < cluster.Len(); i++ {
			if digest(names(t, cluster.Node(i))) != first {
				return false
			}
		}
		return true
	})
	if !ok {
		for i := 0; i < cluster.Len(); i++ {
			nodeNames := names(t, cluster.Node(i))
			t.Logf("node%d at timeout: count=%d digest=%s", i, len(nodeNames), digest(nodeNames))
		}
		t.Fatalf("nodes did not converge on %d rows with equal digests within %v", want, timeout)
	}
}

// logCounters returns the log-path activity counters for one node:
// (received_transactions, applied_transactions). Snapshot application
// bypasses both counters, so a flat pair across the resync proves the rows
// arrived via snapshot.
func logCounters(cluster *harness.Cluster, idx int) (uint64, uint64) {
	metrics := cluster.Node(idx).Replication().Status().Metrics
	return metrics.ReceivedTransactions, metrics.AppliedTransactions
}

// assertSnapshotPath asserts the resync used the snapshot path. The resync's
// log growth must stay within the retained tail and far below the gap. The
// retained tail (minimum_retained per origin) legitimately arrives via logs,
// so a flat counter pair would be over-strict; the bulk can only have
// arrived via snapshot, the only other row path.
func assertSnapshotPath(t *testing.T, cluster *harness.Cluster, idx int, beforeReceived uint64, gap int) {
	t.Helper()
	received, _ := logCounters(cluster, idx)
	growth := received - beforeReceived
	if growth > 2*minimumRetained {
		t.Fatalf("node%d resync pulled %d log transactions, above the retained tail (2 x %d)", idx+1, growth, minimumRetained)
	}
	if growth >= uint64(gap) {
		t.Fatalf("node%d resync arrived over the log path (log growth %d, gap %d), not a snapshot (log catch-up would hide a snapshot-path regression)", idx+1, growth, gap)
	}
}

// collectedTotal returns the total transactions ever pruned on one node.
func collectedTotal(t *testing.T, cluster *harness.Cluster, idx int) uint64 {
	t.Helper()
	status, err := cluster.Node(idx).RetentionStatus()
	if err != nil {
		t.Fatalf("node%d retention status: %v", idx, err)
	}
	return status.CollectedTransactions
}

// staleRows counts acknowledged pre-stop rows surviving on one node (Rust:
// countPrefix(rows, "stale-")).
func staleRows(t *testing.T, cluster *harness.Cluster, idx int) int {
	t.Helper()
	n := 0
	for _, name := range names(t, cluster.Node(idx)) {
		if strings.HasPrefix(name, "stale-") {
			n++
		}
	}
	return n
}

// migrateAll installs the same schema on every node (Rust: TypedRecords
// fixture tables).
func migrateAll(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
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
}

func bytes16(b byte) [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = b
	}
	return id
}

// snapshotCluster builds the standard snapshot-resync cluster: aggressive
// retention plus all-node snapshot trust (the engine only requests
// snapshots from explicitly trusted sources).
func snapshotCluster(t *testing.T, name string, nodeIDs [][16]byte) *harness.Cluster {
	t.Helper()
	all := make(map[[16]byte]bool)
	for _, id := range nodeIDs {
		all[id] = true
	}
	trusted := make(map[int]map[[16]byte]bool)
	for i := range nodeIDs {
		perNode := make(map[[16]byte]bool, len(all))
		for id := range all {
			perNode[id] = true
		}
		trusted[i] = perNode
	}
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     name,
		NumNodes: len(nodeIDs),
		NodeIDs:  nodeIDs,
		Replication: &harness.ReplicationOptions{
			Retention:       &retention,
			MinimumRetained: &minimumRetained,
			MaximumOffline:  &maximumOffline,
		},
		TrustedSnapshotSources: trusted,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	return cluster
}

// ensureSnapshotPath drives member-deadline expiry plus explicit collection
// passes: every survivor must prune past retention and stop pinning every
// victim, proving the victims' needed ranges are genuinely unrecoverable
// from logs.
func ensureSnapshotPath(t *testing.T, cluster *harness.Cluster, survivors, victims []int) {
	t.Helper()
	t.Log("waiting for member-deadline expiry and collection")
	time.Sleep(harness.EnvSeconds("MURMUR_SNAPSHOT_RESYNC_GC_WAIT_SECONDS", 30))
	ok := harness.WaitUntil(collectTimeout, 2*time.Second, func() bool {
		for _, i := range survivors {
			if collectedTotal(t, cluster, i) < minCollected {
				for _, j := range survivors {
					if _, err := cluster.Node(j).CollectRetention(); err != nil {
						t.Logf("node%d collect retention: %v", j, err)
					}
				}
				return false
			}
		}
		return true
	})
	totals := make([]uint64, 0, len(survivors))
	for _, i := range survivors {
		totals = append(totals, collectedTotal(t, cluster, i))
	}
	if !ok {
		t.Fatalf("survivors collected only %v transactions, want >= %d each (snapshot path not forced)", totals, minCollected)
	}
	t.Logf("survivors pruned %v transactions past retention", totals)
	// Prove the deadline leg explicitly: every victim is now older than
	// every survivor's offline memory.
	for _, i := range survivors {
		status, err := cluster.Node(i).RetentionStatus()
		if err != nil {
			t.Fatalf("node%d retention status: %v", i, err)
		}
		for _, victim := range victims {
			victimID := cluster.NodeID(victim)
			var peer *replication.PeerRetention
			for k := range status.Peers {
				if status.Peers[k].Node == victimID {
					peer = &status.Peers[k]
					break
				}
			}
			if peer == nil {
				t.Fatalf("node%d has no retention record for node%d", i, victim+1)
			}
			age := replication.NowMillis() - peer.LastSeenMillis
			if age <= uint64(maximumOffline.Milliseconds()) {
				t.Fatalf("node%d still pins node%d (last seen %d ms ago)", i+1, victim+1, age)
			}
		}
	}
}

func TestStaleNodeRejoinsViaSnapshot(t *testing.T) {
	cluster := snapshotCluster(t, "snapshot-resync", [][16]byte{bytes16(0x10), bytes16(0x20), bytes16(0x30)})
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("snapshot-resync-no-convergence")
		}
		cluster.Finish(failed)
	}()
	// Phase 1: node3 writes acknowledged rows, then all three converge.
	migrateAll(t, cluster)
	for i := int64(0); i < 5; i++ {
		insert(t, cluster, 2, i+1, fmt.Sprintf("stale-%d", i))
	}
	if err := cluster.Node(2).Flush(); err != nil {
		t.Fatalf("flush baseline: %v", err)
	}
	waitConverged(t, cluster, 5, baselineTimeout)

	// Phase 2: node3 stops; survivors write far past retention.
	// Flush every write: the product mints one replication log entry per
	// flush batch, not per SQL transaction, so a single trailing flush
	// would leave fewer entries than minimum_retained and nothing would
	// ever become eligible for pruning.
	cluster.StopNode(2)
	for i := int64(0); i < 60; i++ {
		insert(t, cluster, 0, 100+i, fmt.Sprintf("fresh-%d", i))
		if err := cluster.Node(0).Flush(); err != nil {
			t.Fatalf("flush survivor write: %v", err)
		}
	}
	waitRowCount(t, cluster, 0, 65, rowTimeout)
	waitRowCount(t, cluster, 1, 65, rowTimeout)
	ensureSnapshotPath(t, cluster, []int{0, 1}, []int{2})

	// Phase 3: node3 restarts with the same durable directory and rejoins.
	if err := cluster.StartNode(2); err != nil {
		t.Fatalf("stale node restart: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	logBefore, _ := logCounters(cluster, 2)
	waitConverged(t, cluster, 65, resyncTimeout)

	// The rejoin must have used the snapshot path, not log catch-up.
	assertSnapshotPath(t, cluster, 2, logBefore, 65-5)

	// Node3's acknowledged pre-stop state survives the snapshot merge.
	if got := staleRows(t, cluster, 2); got != 5 {
		t.Fatalf("node3 stale rows after resync = %d, want 5", got)
	}
	failed = false
}

func TestDualStaleNodesRejoinViaSnapshot(t *testing.T) {
	cluster := snapshotCluster(t, "snapshot-resync-dual", [][16]byte{bytes16(0x11), bytes16(0x21), bytes16(0x31)})
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("snapshot-resync-dual-no-convergence")
		}
		cluster.Finish(failed)
	}()
	migrateAll(t, cluster)
	for i := int64(0); i < 5; i++ {
		insert(t, cluster, 2, i+1, fmt.Sprintf("stale-%d", i))
	}
	if err := cluster.Node(2).Flush(); err != nil {
		t.Fatalf("flush baseline: %v", err)
	}
	waitConverged(t, cluster, 5, baselineTimeout)

	// Two nodes stop; the survivor writes far past retention (flushed per
	// write: one replication log entry is minted per flush batch).
	cluster.StopNode(1)
	cluster.StopNode(2)
	for i := int64(0); i < 60; i++ {
		insert(t, cluster, 0, 100+i, fmt.Sprintf("fresh-%d", i))
		if err := cluster.Node(0).Flush(); err != nil {
			t.Fatalf("flush survivor write: %v", err)
		}
	}
	waitRowCount(t, cluster, 0, 65, rowTimeout)
	ensureSnapshotPath(t, cluster, []int{0}, []int{1, 2})

	// Restart both stale nodes back-to-back so their snapshot requests
	// overlap on the survivor.
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("stale node2 restart: %v", err)
	}
	if err := cluster.StartNode(2); err != nil {
		t.Fatalf("stale node3 restart: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	logBefore := make([]uint64, 0, 2)
	for _, i := range []int{1, 2} {
		received, _ := logCounters(cluster, i)
		logBefore = append(logBefore, received)
	}
	waitConverged(t, cluster, 65, resyncTimeoutLong)

	for k, idx := range []int{1, 2} {
		assertSnapshotPath(t, cluster, idx, logBefore[k], 65-5)
		if got := staleRows(t, cluster, idx); got != 5 {
			t.Fatalf("node%d stale rows after resync = %d, want 5", idx+1, got)
		}
		// The resync diagnostics surface end to end: each rejoined node
		// reports live replication sessions.
		if peers := cluster.Node(idx).Replication().Status().Peers; peers < 1 {
			t.Fatalf("node%d reports no connected peers after resync", idx+1)
		}
	}
	failed = false
}

func TestStaleNodeRejoinsViaSingleSnapshotSource(t *testing.T) {
	const victim = 3
	cluster := snapshotCluster(t, "snapshot-multisource", [][16]byte{bytes16(0x12), bytes16(0x22), bytes16(0x32), bytes16(0x42)})
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("snapshot-multisource-no-convergence")
		}
		cluster.Finish(failed)
	}()
	migrateAll(t, cluster)
	for i := int64(0); i < 5; i++ {
		insert(t, cluster, victim, i+1, fmt.Sprintf("stale-%d", i))
	}
	if err := cluster.Node(victim).Flush(); err != nil {
		t.Fatalf("flush baseline: %v", err)
	}
	waitConverged(t, cluster, 5, baselineTimeout)

	// Victim stops; survivors write thousands of rows so the resync
	// snapshot spans many chunks (a 1-2 chunk snapshot rarely interleaves
	// badly enough to expose source contention).
	cluster.StopNode(victim)
	// Batched multi-row transactions: the test needs snapshot volume (many
	// chunks), not round trips. Flush per batch: one replication log entry
	// is minted per flush batch, and retention needs dozens of entries to
	// prune past (a single trailing flush would leave fewer entries than
	// minimum_retained).
	const freshRows = int64(6000)
	const batch = int64(100)
	start := time.Now()
	for base := int64(0); base < freshRows; base += batch {
		insertMany(t, cluster, 0, 100+base, batch, "fresh")
		if err := cluster.Node(0).Flush(); err != nil {
			t.Fatalf("flush survivor batch: %v", err)
		}
	}
	t.Logf("wrote %d survivor rows in %v", freshRows, time.Since(start))
	want := 5 + int(freshRows)
	for i := 0; i < 3; i++ {
		waitRowCount(t, cluster, i, want, rowTimeout)
	}
	ensureSnapshotPath(t, cluster, []int{0, 1, 2}, []int{victim})

	if err := cluster.StartNode(victim); err != nil {
		t.Fatalf("victim restart: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	logBefore, _ := logCounters(cluster, victim)
	waitConverged(t, cluster, want, resyncTimeoutLong)

	// The rejoin used the snapshot path (not log catch-up) under real
	// multi-source contention: three survivors serve concurrently while
	// the transfer completes to convergence.
	assertSnapshotPath(t, cluster, victim, logBefore, want-5)
	if got := staleRows(t, cluster, victim); got != 5 {
		t.Fatalf("victim stale rows after resync = %d, want 5", got)
	}
	failed = false
}

// Live scenario: a stale node rejoins while its only available snapshot
// source is hostile, rejects the corrupt chunk with nothing partially
// published, then converges via the honest snapshot path.
//
// Go port of tests/corrupt_snapshot.rs using tests-live/goharness.
// Identical behavior: the attacker induces a snapshot pull with an
// unsolicited history-unavailable error, then serves a structurally valid
// manifest plus one intact and one bit-flipped chunk. The receiver must
// reject the corrupt chunk (row count, digest, and commit cursor
// untouched), discard the staged transfer, then converge via the honest
// snapshot path with bit-identical digests. The raw-QUIC attacker is the
// shared harness fixture; this scenario reaches product types only
// through it and the murmur/replication package.
package corruptsnapshot

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// Role index: node3 goes stale, then rejoins.
const stale = 2

// Attacker identity: fixed test id, asserted distinct from the node ids.
var attackerID = [16]byte{0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7}

// Attacker origin signing key (the victim trusts its public half,
// mirroring the Rust test authorizing the hostile source).
var attackerSeed = [32]byte{
	0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E,
	0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E,
}

// Forged snapshot payload: two 64 KiB wire chunks.
const forgedBytes = 70_000

// Forged manifest lifetime (attacker-side): short so the victim's expiry
// reclamation clears the staged corrupt transfer before the honest leg.
const forgedLifetime = 20 * time.Second

// Reclaim margin past the forged expiry (the engine's maintenance tick
// runs about every second).
const reclaimMargin = 10 * time.Second

// Log retention.
const retention = 1 * time.Second

// Offline-peer memory.
const maximumOffline = 20 * time.Second

// Newest transactions per origin the survivors must keep.
const minimumRetained = uint64(10)

// Minimum pruned transactions per survivor before the attack: 60 survivor
// writes minus the 10 retained, with margin. Proves the stale node's
// needed ranges are genuinely unrecoverable from logs, so the rejoin must
// use the snapshot path.
const minCollected = uint64(40)

// Baseline convergence deadline.
const baselineTimeout = 60 * time.Second

// Survivor row-count deadline.
const rowTimeout = 30 * time.Second

// Convergence poll interval.
const convergePoll = 200 * time.Millisecond

// GC collection-loop deadline past the expiry wait.
const collectTimeout = 60 * time.Second

// Snapshot-pull inducement deadline.
const requestTimeout = 30 * time.Second

// Forged-chunk request deadline.
const needTimeout = 15 * time.Second

// Corrupt-chunk rejection deadline.
const failTimeout = 30 * time.Second

// Post-attack drain: no SnapshotApplied may arrive.
const drainTimeout = 3 * time.Second

// Post-attack settle before asserting nothing was published.
const settle = 2 * time.Second

// Honest-resync convergence deadline.
const honestTimeout = 120 * time.Second

// names returns the canonical ordered names on one node.
func names(cluster *goharness.Cluster, idx int) ([]string, error) {
	rows, err := cluster.Node(idx).Query("SELECT name FROM records ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("read names: %w", err)
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		s, err := r.String(0)
		if err != nil {
			return nil, fmt.Errorf("read names: %w", err)
		}
		out = append(out, s)
	}
	return out, nil
}

// digest returns the canonical digest of a name set (sorted names joined
// with newlines).
func digest(nameList []string) string {
	out := ""
	for i, n := range nameList {
		if i != 0 {
			out += "\n"
		}
		out += n
	}
	return out
}

// rowCount returns the row count on one node (cheap pre-check before full
// name reads).
func rowCount(cluster *goharness.Cluster, idx int) (int, error) {
	rows, err := cluster.Node(idx).Query("SELECT COUNT(*) FROM records")
	if err != nil {
		return 0, fmt.Errorf("row count: %w", err)
	}
	n, err := rows[0].Int64(0)
	if err != nil {
		return 0, fmt.Errorf("row count: %w", err)
	}
	return int(n), nil
}

// insert writes one single-row transaction.
func insert(cluster *goharness.Cluster, node int, id int64, name string) error {
	if _, err := cluster.Node(node).Execute("INSERT INTO records(id, name) VALUES($1, $2)", id, name); err != nil {
		return fmt.Errorf("node %d insert failed: %w", node, err)
	}
	return nil
}

// waitRowCount waits until one node holds exactly want rows.
func waitRowCount(cluster *goharness.Cluster, idx int, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		n, err := rowCount(cluster, idx)
		if err != nil {
			return err
		}
		if n == want {
			return nil
		}
		if !time.Now().Before(deadline) {
			n, err := rowCount(cluster, idx)
			if err != nil {
				return err
			}
			return fmt.Errorf("node %d record count = %d, want %d within %s", idx, n, want, timeout)
		}
		time.Sleep(convergePoll)
	}
}

// waitConverged waits until every node holds exactly want rows with equal
// digests.
func waitConverged(cluster *goharness.Cluster, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		counts := make([]int, 0, cluster.Len())
		for i := 0; i < cluster.Len(); i++ {
			n, err := rowCount(cluster, i)
			if err != nil {
				return err
			}
			counts = append(counts, n)
		}
		all := true
		for _, n := range counts {
			if n != want {
				all = false
			}
		}
		if all {
			first := ""
			equal := true
			for idx := 0; idx < cluster.Len(); idx++ {
				nodeNames, err := names(cluster, idx)
				if err != nil {
					return err
				}
				d := digest(nodeNames)
				if idx == 0 {
					first = d
				} else if d != first {
					equal = false
					break
				}
			}
			if equal {
				return nil
			}
		}
		if !time.Now().Before(deadline) {
			for i := 0; i < cluster.Len(); i++ {
				nodeNames, err := names(cluster, i)
				if err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "node %d at timeout: count=%d digest=%s\n",
					i, len(nodeNames), digest(nodeNames))
			}
			return fmt.Errorf("nodes did not converge on %d rows with equal digests within %s", want, timeout)
		}
		time.Sleep(convergePoll)
	}
}

// replicationRow returns the first MURMUR SHOW REPLICATION row on one node.
func replicationRow(cluster *goharness.Cluster, idx int) ([]string, error) {
	rows, err := cluster.Node(idx).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		return nil, fmt.Errorf("SHOW REPLICATION: %w", err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("SHOW REPLICATION: no rows")
	}
	return rows[0], nil
}

// connectedPeers returns the connected replication sessions on one node.
func connectedPeers(cluster *goharness.Cluster, idx int) (int, error) {
	row, err := replicationRow(cluster, idx)
	if err != nil {
		return 0, err
	}
	if len(row) > 2 {
		if n, err := strconv.Atoi(row[2]); err == nil {
			return n, nil
		}
	}
	return 0, nil
}

// lastError returns the latest replication error on one node (the
// corrupt-chunk rejection lands here).
func lastError(cluster *goharness.Cluster, idx int) (string, bool, error) {
	row, err := replicationRow(cluster, idx)
	if err != nil {
		return "", false, err
	}
	if len(row) > 6 && row[6] != "" {
		return row[6], true, nil
	}
	return "", false, nil
}

// committed returns the local commit cursor on one node.
func committed(cluster *goharness.Cluster, idx int) (uint64, error) {
	row, err := replicationRow(cluster, idx)
	if err != nil {
		return 0, err
	}
	if len(row) > 7 {
		if n, err := strconv.ParseUint(row[7], 10, 64); err == nil {
			return n, nil
		}
	}
	return 0, nil
}

// logCounters returns the log-path activity counters for one node:
// (staged_transactions, applied_transactions). Snapshot application
// bypasses both counters, so a flat pair across the honest resync proves
// the rows arrived via snapshot.
func logCounters(cluster *goharness.Cluster, idx int) (uint64, uint64, error) {
	row, err := replicationRow(cluster, idx)
	if err != nil {
		return 0, 0, err
	}
	var staged, applied uint64
	if len(row) > 4 {
		staged, _ = strconv.ParseUint(row[4], 10, 64)
	}
	if len(row) > 5 {
		applied, _ = strconv.ParseUint(row[5], 10, 64)
	}
	return staged, applied, nil
}

// collectHistory runs one bounded retention pass on one node; it returns
// transactions pruned.
func collectHistory(cluster *goharness.Cluster, idx int) (uint64, error) {
	rows, err := cluster.Node(idx).Simple("MURMUR RETENTION COLLECT")
	if err != nil {
		return 0, fmt.Errorf("node %d collect retention: %w", idx, err)
	}
	if len(rows) > 0 && len(rows[0]) > 0 {
		if n, err := strconv.ParseUint(rows[0][0], 10, 64); err == nil {
			return n, nil
		}
	}
	return 0, nil
}

// collectedTotal returns total transactions ever pruned on one node.
func collectedTotal(cluster *goharness.Cluster, idx int) (uint64, error) {
	rows, err := cluster.Node(idx).Simple("MURMUR SHOW RETENTION")
	if err != nil {
		return 0, fmt.Errorf("SHOW RETENTION: %w", err)
	}
	if len(rows) > 0 && len(rows[0]) > 5 {
		if n, err := strconv.ParseUint(rows[0][5], 10, 64); err == nil {
			return n, nil
		}
	}
	return 0, nil
}

// lastSeenMillis reads last seen millis for the peer with node_id
// victimHex from node idx.
func lastSeenMillis(cluster *goharness.Cluster, idx int, victimHex string) (uint64, bool, error) {
	rows, err := cluster.Node(idx).Simple("MURMUR SHOW RETENTION")
	if err != nil {
		return 0, false, fmt.Errorf("SHOW RETENTION: %w", err)
	}
	for _, r := range rows {
		if len(r) > 0 && r[0] == victimHex {
			if len(r) > 1 {
				if n, err := strconv.ParseUint(r[1], 10, 64); err == nil {
					return n, true, nil
				}
			}
			return 0, false, nil
		}
	}
	return 0, false, nil
}

func runBody(cluster *goharness.Cluster) error {
	// Phase 0: create replicated table on all nodes.
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple("CREATE TABLE records(id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
			return fmt.Errorf("migrate records table node %d: %w", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			return fmt.Errorf("set replicated node %d: %w", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("flush schema node %d: %w", i, err)
		}
	}

	// Phase 1: build the honest mesh explicitly and converge a baseline.
	for _, pair := range [][2]int{{0, 1}, {1, 0}, {0, 2}, {2, 0}, {1, 2}, {2, 1}} {
		if err := cluster.AddPeer(pair[0], pair[1]); err != nil {
			return fmt.Errorf("AddPeer(%d,%d): %w", pair[0], pair[1], err)
		}
	}
	for i := 0; i < 5; i++ {
		if err := insert(cluster, stale, int64(i+1), fmt.Sprintf("stale-%d", i)); err != nil {
			return err
		}
	}
	if _, err := cluster.Node(stale).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("flush baseline: %w", err)
	}
	if err := waitConverged(cluster, 5, baselineTimeout); err != nil {
		return err
	}
	staleNames, err := names(cluster, stale)
	if err != nil {
		return err
	}
	preStopDigest := digest(staleNames)

	// Phase 2: isolate node3, stop it, and let the survivors write past
	// retention so the rejoin genuinely needs the snapshot path.
	for _, pair := range [][2]int{{2, 0}, {2, 1}, {0, 2}, {1, 2}} {
		if err := cluster.RemovePeer(pair[0], pair[1]); err != nil {
			return fmt.Errorf("RemovePeer(%d,%d): %w", pair[0], pair[1], err)
		}
	}
	cluster.StopNode(stale)

	// Flush every write: the product mints one replication log entry per
	// flush batch, not per SQL transaction, so a single trailing flush
	// would leave fewer entries than minimum_retained and nothing would
	// ever become eligible for pruning.
	for i := 0; i < 60; i++ {
		if err := insert(cluster, 0, int64(100+i), fmt.Sprintf("fresh-%d", i)); err != nil {
			return err
		}
		if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("flush survivor write: %w", err)
		}
	}
	if err := waitRowCount(cluster, 0, 65, rowTimeout); err != nil {
		return err
	}
	if err := waitRowCount(cluster, 1, 65, rowTimeout); err != nil {
		return err
	}

	// Member-deadline expiry, then explicit collection passes: the stale
	// node's retention pin must lapse before its needed ranges become
	// genuinely unrecoverable from logs.
	fmt.Fprintln(os.Stderr, "waiting for member-deadline expiry and collection")
	time.Sleep(goharness.EnvSeconds("MURMUR_CORRUPT_SNAPSHOT_GC_WAIT_SECONDS", 30))

	collectDeadline := time.Now().Add(collectTimeout)
	for {
		t0, err := collectedTotal(cluster, 0)
		if err != nil {
			return err
		}
		t1, err := collectedTotal(cluster, 1)
		if err != nil {
			return err
		}
		if t0 >= minCollected && t1 >= minCollected {
			fmt.Fprintf(os.Stderr, "survivors pruned [%d %d] transactions past retention\n", t0, t1)
			break
		}
		if !time.Now().Before(collectDeadline) {
			return fmt.Errorf("survivors collected only [%d %d] transactions, want >= %d each (snapshot path not forced)",
				t0, t1, minCollected)
		}
		if _, err := collectHistory(cluster, 0); err != nil {
			return err
		}
		if _, err := collectHistory(cluster, 1); err != nil {
			return err
		}
		time.Sleep(2 * time.Second)
	}

	// Prove the deadline leg explicitly: the victim is now older than
	// every survivor's offline memory.
	victimNodeID := cluster.NodeID(stale)
	victimIDHex := goharness.Hex(victimNodeID[:])
	for _, i := range []int{0, 1} {
		lastSeen, ok, err := lastSeenMillis(cluster, i, victimIDHex)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("node %d has no retention record for victim %s", i, victimIDHex)
		}
		age := replication.NowMillis() - lastSeen
		if lastSeen > replication.NowMillis() {
			age = 0
		}
		if age <= uint64(maximumOffline.Milliseconds()) {
			return fmt.Errorf("node %d still pins the victim (last seen %d ms ago, offline max %s)",
				i, age, maximumOffline)
		}
	}

	// Phase 3: node3 restarts peerless; the attacker's inbound session is
	// its only source. The attacker induces a snapshot pull, then serves
	// bit-flipped chunks.
	if err := cluster.StartNode(stale); err != nil {
		return fmt.Errorf("stale node restart: %w", err)
	}
	if _, err := cluster.Node(stale).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
		return fmt.Errorf("re-set replicated on stale node: %w", err)
	}
	if _, err := cluster.Node(stale).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("flush schema on stale node: %w", err)
	}
	victimAddr := cluster.NodeInfo(stale).ReplAddr
	victimID := cluster.NodeID(stale)
	dbid := cluster.DatabaseID()
	genBeforeAttack, err := committed(cluster, stale)
	if err != nil {
		return err
	}
	stagedBefore, appliedBefore, err := logCounters(cluster, stale)
	if err != nil {
		return err
	}

	// Run the attacker dialogue against the victim (shared harness
	// fixture; product wire implementation, not a fork).
	attacker, err := goharness.NewAttacker(cluster, attackerID, attackerSeed)
	if err != nil {
		return fmt.Errorf("attacker identity: %w", err)
	}
	attacker.Config().SnapshotTransferLifetime = forgedLifetime
	evil, err := attacker.Dial(victimAddr, dbid, victimID)
	if err != nil {
		return fmt.Errorf("attacker dial: %w", err)
	}

	// Prove the "only available source" premise: exactly one peer.
	pollDeadline := time.Now().Add(10 * time.Second)
	for {
		n, err := connectedPeers(cluster, stale)
		if err != nil {
			return err
		}
		if n == 1 {
			break
		}
		if !time.Now().Before(pollDeadline) {
			n, err := connectedPeers(cluster, stale)
			if err != nil {
				return err
			}
			return fmt.Errorf("stale node has %d connected peers during the attack, want 1", n)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Induce the pull: an unsolicited history-unavailable error makes
	// the node request a snapshot from this peer over the live session.
	if err := evil.Send(&replication.Message{Kind: replication.MsgError, Code: 3, ErrMessage: "ranges unavailable"}); err != nil {
		return fmt.Errorf("attacker induce pull: %w", err)
	}
	reqs := evil.CollectUntil(requestTimeout, func(m *replication.Message) bool {
		return m.Kind == replication.MsgSnapshotRequest
	})
	found := false
	for i := range reqs {
		if reqs[i].Kind == replication.MsgSnapshotRequest {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("stale node never requested a snapshot from the attacker")
	}

	forged, err := goharness.ForgeCorruptSnapshot(attacker.Config(), attackerID, dbid, victimID, evil.WelcomeSchema(), forgedBytes)
	if err != nil {
		return fmt.Errorf("forge corrupt snapshot: %w", err)
	}
	if err := evil.Send(&replication.Message{Kind: replication.MsgSnapshotManifest, Manifest: forged.Manifest}); err != nil {
		return fmt.Errorf("attacker serve manifest: %w", err)
	}

	// The manifest passed structural checks: the node asks for its chunks.
	needs := evil.CollectUntil(needTimeout, func(m *replication.Message) bool {
		return m.Kind == replication.MsgSnapshotNeed
	})
	asked := false
	for i := range needs {
		if needs[i].Kind == replication.MsgSnapshotNeed && needs[i].Transfer == forged.Transfer {
			asked = true
		}
	}
	if !asked {
		return fmt.Errorf("victim never asked for the forged chunks (manifest not accepted)")
	}

	if err := evil.Send(&replication.Message{Kind: replication.MsgSnapshotChunk, Transfer: forged.Transfer, Index: 0, Data: forged.Chunk0}); err != nil {
		return fmt.Errorf("attacker serve chunk0: %w", err)
	}
	if err := evil.Send(&replication.Message{Kind: replication.MsgSnapshotChunk, Transfer: forged.Transfer, Index: 1, Data: forged.Chunk1Bad}); err != nil {
		return fmt.Errorf("attacker serve chunk1: %w", err)
	}

	// The corrupt transfer must never complete: drain while asserting
	// no SnapshotApplied arrives.
	tail := evil.CollectUntil(drainTimeout, func(_ *replication.Message) bool { return false })
	for i := range tail {
		if tail[i].Kind == replication.MsgSnapshotApplied {
			return fmt.Errorf("victim applied the corrupt snapshot")
		}
	}
	evil.Close()

	// The receiver must detect the digest mismatch and fail the chunk...
	failDeadline := time.Now().Add(failTimeout)
	for {
		msg, ok, err := lastError(cluster, stale)
		if err != nil {
			return err
		}
		if ok && strings.Contains(msg, "snapshot chunk") {
			fmt.Fprintf(os.Stderr, "victim rejected the corrupt chunk: %s\n", msg)
			break
		}
		if !time.Now().Before(failDeadline) {
			return fmt.Errorf("victim never recorded the corrupt-chunk rejection")
		}
		time.Sleep(convergePoll)
	}

	// ...while no snapshot completed and nothing was partially published.
	time.Sleep(settle)
	n, err := rowCount(cluster, stale)
	if err != nil {
		return err
	}
	if n != 5 {
		n, err := rowCount(cluster, stale)
		if err != nil {
			return err
		}
		return fmt.Errorf("stale node row count moved on corrupt transfer: %d != 5", n)
	}
	staleNames, err = names(cluster, stale)
	if err != nil {
		return err
	}
	if digest(staleNames) != preStopDigest {
		return fmt.Errorf("stale node digest changed on corrupt transfer (partial publication)")
	}
	gen, err := committed(cluster, stale)
	if err != nil {
		return err
	}
	if gen != genBeforeAttack {
		return fmt.Errorf("commit cursor moved on corrupt transfer (partial publication)")
	}
	staged, applied, err := logCounters(cluster, stale)
	if err != nil {
		return err
	}
	if staged != stagedBefore || applied != appliedBefore {
		return fmt.Errorf("replication counters moved on corrupt transfer (partial publication)")
	}

	// The staged corrupt transfer is discarded on expiry (the only
	// staged-transfer discard path); the honest leg below proves it.
	time.Sleep(forgedLifetime + reclaimMargin)

	// Phase 4: honest peers return; the node retries from them and converges.
	for _, pair := range [][2]int{{2, 0}, {2, 1}, {0, 2}, {1, 2}} {
		if err := cluster.AddPeer(pair[0], pair[1]); err != nil {
			return fmt.Errorf("honest AddPeer(%d,%d): %w", pair[0], pair[1], err)
		}
	}
	_ = cluster.SyncAll()
	preStaged, preApplied, err := logCounters(cluster, stale)
	if err != nil {
		return err
	}
	if err := waitConverged(cluster, 65, honestTimeout); err != nil {
		return err
	}

	// The rejoin completed via an honest snapshot, not the corrupt one
	// and not the logs: 60 rows arrived while the log-path counters
	// stayed flat (snapshot application bypasses both).
	staged, applied, err = logCounters(cluster, stale)
	if err != nil {
		return err
	}
	if staged != preStaged || applied != preApplied {
		return fmt.Errorf("honest resync arrived over the log path, not a snapshot")
	}
	gen, err = committed(cluster, stale)
	if err != nil {
		return err
	}
	if gen < genBeforeAttack {
		return fmt.Errorf("commit cursor regressed (non-monotonic publication)")
	}
	staleNames, err = names(cluster, stale)
	if err != nil {
		return err
	}
	staleRows := 0
	for _, name := range staleNames {
		if len(name) >= 6 && name[:6] == "stale-" {
			staleRows++
		}
	}
	if staleRows != 5 {
		return fmt.Errorf("stale rows after honest resync = %d, want 5", staleRows)
	}
	return nil
}

func durationPtr(d time.Duration) *time.Duration { return &d }

func uint64Ptr(v uint64) *uint64 { return &v }

func TestStaleNodeDiscardsCorruptSnapshotThenConverges(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	// Fixed node ids (asserted distinct from the attacker id) so the
	// snapshot-trust overlays can be built before the cluster opens.
	nodeIDs := [][16]byte{
		{0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10},
		{0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20},
		{0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30},
	}
	for _, id := range nodeIDs {
		if id == attackerID {
			t.Fatal("attacker id collides with a node id")
		}
	}

	// Every node trusts every node as a snapshot donor; the victim
	// additionally authorizes the hostile source.
	allNodes := map[[16]byte]bool{}
	for _, id := range nodeIDs {
		allNodes[id] = true
	}
	trustedSnapshotSources := map[int]map[[16]byte]bool{}
	for i := 0; i < len(nodeIDs); i++ {
		cp := map[[16]byte]bool{}
		for id := range allNodes {
			cp[id] = true
		}
		trustedSnapshotSources[i] = cp
	}
	victimSources := map[[16]byte]bool{}
	for id := range allNodes {
		victimSources[id] = true
	}
	victimSources[attackerID] = true
	trustedSnapshotSources[stale] = victimSources
	extraTrustedOrigins := map[int]map[[16]byte][32]byte{
		stale: {attackerID: replication.PublicKey(&attackerSeed)},
	}

	opts := goharness.DefaultClusterOptions("corrupt-snapshot")
	opts.NumNodes = 3
	opts.NodeIDs = nodeIDs
	// Manual peers so the stale node restarts truly peerless: the
	// attacker becomes its only snapshot source by construction.
	opts.ManualPeers = true
	opts.Replication = &goharness.ReplicationOptions{
		Retention:       durationPtr(retention),
		MinimumRetained: uint64Ptr(minimumRetained),
		MaximumOffline:  durationPtr(maximumOffline),
	}
	opts.TrustedSnapshotSources = trustedSnapshotSources
	opts.ExtraTrustedOrigins = extraTrustedOrigins
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}

	runErr := runBody(cluster)
	failed := runErr != nil
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", runErr)
	}
	if failed {
		cluster.DumpForensics("corrupt-snapshot-no-convergence")
	}
	cluster.Finish(failed)
	if failed {
		t.Fatal("scenario failed; artifacts retained under tests-live/failures/")
	}
}

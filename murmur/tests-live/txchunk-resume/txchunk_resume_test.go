// Live scenario: a chunked transaction resumes after the receiver
// restarts mid-transfer.
//
// Go port of the Rust `txchunk-resume` scenario
// (/home/marc/MURMUR/tests-live/txchunk-resume/tests/txchunk_resume.rs). A
// single transaction larger than the replication frame travels as ~64 KiB
// transaction chunks; the receiving node is stopped mid-transfer and
// restarted, and the retry must complete with all-or-nothing visibility,
// the applied watermark must never advance before completion, and all
// digests must converge without any snapshot transfer. Plain SQL replaces
// the typed-records HTTP API, close/reopen replaces SIGKILL, and
// push-start replaces the inbound-byte kill trigger (see the scenario
// README.md for the deviations).
package txchunkresume

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// Role indices (Rust: sender/victim/witness).
const (
	sender  = 0
	victim  = 1
	witness = 2
)

const (
	// positiveTimeout is the positive-control convergence deadline (Rust:
	// 120 s).
	positiveTimeout = 120 * time.Second
	// haveTimeout is the baseline ack-settle deadline (Rust: 45 s;
	// waits out slow post-control ack paths plus anti-entropy
	// redundancy).
	haveTimeout = 45 * time.Second
	// havePoll is the ack-settle poll interval.
	havePoll = 200 * time.Millisecond
	// haveStableWindow is the window the ack value must hold to count as
	// settled (Rust-only; five seconds outlasts post-control ack
	// backlog; agreement with the witness carries the real signal).
	haveStableWindow = 5 * time.Second
	// splitPeersTimeout is the partition deadline on the victim (Rust:
	// 15 s).
	splitPeersTimeout = 15 * time.Second
	// survivorTimeout is the survivor trial-visibility deadline (Rust:
	// 60 s).
	survivorTimeout = 60 * time.Second
	// prefixPoll is the prefix-count poll interval (Rust: 100 ms).
	prefixPoll = 100 * time.Millisecond
	// killTimeout is the mid-transfer stop deadline (Rust: 15 s
	// byte-watch).
	killTimeout = 15 * time.Second
	// killPoll is the mid-transfer stop poll interval (Rust: 5 ms).
	killPoll = 5 * time.Millisecond
	// healPeersTimeout is the post-restart mesh deadline (Rust: 30 s,
	// matching the established heal leg).
	healPeersTimeout = 30 * time.Second
	// retryTimeout is the retry-completion deadline (Rust: 120 s).
	retryTimeout = 120 * time.Second
	// retryPoll is the retry-completion poll interval (Rust: 25 ms).
	retryPoll = 25 * time.Millisecond
	// publicationBound is the watermark-advance publication bound (Rust:
	// 10 s).
	publicationBound = 10 * time.Second
	// attemptConvergeTimeout is the reconvergence deadline after a
	// too-slow/too-late attempt (Rust: 60 s).
	attemptConvergeTimeout = 60 * time.Second
	// finalTimeout is the final convergence deadline (Rust: 60 s).
	finalTimeout = 60 * time.Second
	// convergePoll is the convergence poll interval (Rust: 200 ms).
	convergePoll = 200 * time.Millisecond
	// topoPoll is the topology poll interval (Rust: 100 ms).
	topoPoll = 100 * time.Millisecond
)

// bigRow is one replicated row: the typed contention row (ID, Name, Phone).
type bigRow struct {
	id    string
	name  string
	phone string
}

// rows returns all rows on one node (Rust: Cluster.TypedContentionRows).
func rows(t *testing.T, db *murmur.Database) []bigRow {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name, phone FROM records")
	})
	if err != nil {
		t.Fatalf("read rows: %v", err)
	}
	var out []bigRow
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, bigRow{id: row[0].Str, name: row[1].Str, phone: row[2].Str})
	}
	return out
}

// digest is the canonical digest of a row set (Rust: ordered
// TypedContentionDigest).
func digest(rs []bigRow) string {
	slices.SortFunc(rs, func(a, b bigRow) int {
		if a.id != b.id {
			return strings.Compare(a.id, b.id)
		}
		if a.name != b.name {
			return strings.Compare(a.name, b.name)
		}
		return strings.Compare(a.phone, b.phone)
	})
	return fmt.Sprintf("%q", rs)
}

// rowCount is the row count on one node (cheap pre-check before full
// digest reads).
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

// prefixCount counts rows on one node whose name starts with prefix (Rust:
// TypedContentionPrefixCount).
func prefixCount(t *testing.T, cluster *harness.Cluster, idx int, prefix string) int {
	t.Helper()
	v, err := cluster.Node(idx).Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT COUNT(*) FROM records WHERE substr(name, 1, ?) = ?",
			int64(len(prefix)), prefix)
	})
	if err != nil {
		t.Fatalf("node %d prefix count: %v", idx, err)
	}
	return int(v.([][]sqlite.Value)[0][0].Int)
}

// waitPrefixCount waits until one node holds exactly want prefixed rows
// (Rust: waitPrefixCount).
func waitPrefixCount(t *testing.T, cluster *harness.Cluster, idx int, prefix string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if prefixCount(t, cluster, idx, prefix) == want {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("node %d %q rows = %d, want %d within %v",
				idx, prefix, prefixCount(t, cluster, idx, prefix), want, timeout)
		}
		time.Sleep(prefixPoll)
	}
}

// waitConverged waits until every node holds exactly want rows with equal
// digests (Rust: waitConverged). Counts are checked first so the
// multi-megabyte digest reads only run once the counts already match (same
// pass criteria, cheaper polling).
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		allCounts := true
		for i := 0; i < cluster.Len(); i++ {
			if rowCount(t, cluster.Node(i)) != want {
				allCounts = false
				break
			}
		}
		if allCounts {
			first := digest(rows(t, cluster.Node(0)))
			equal := true
			for i := 1; i < cluster.Len(); i++ {
				if digest(rows(t, cluster.Node(i))) != first {
					equal = false
					break
				}
			}
			if equal {
				return
			}
		}
		if !time.Now().Before(deadline) {
			for i := 0; i < cluster.Len(); i++ {
				nodeRows := rows(t, cluster.Node(i))
				t.Logf("node %d at timeout: count=%d digest=%s", i, len(nodeRows), digest(nodeRows))
			}
			t.Fatalf("nodes did not converge on %d rows with equal digests within %v (%s)",
				want, timeout, what)
		}
		time.Sleep(convergePoll)
	}
}

// waitConnectedPeers waits until one node reports exactly want replication
// peers (Rust: waitConnectedPeers over connected_peers).
func waitConnectedPeers(t *testing.T, cluster *harness.Cluster, idx, want int, timeout time.Duration) {
	t.Helper()
	peers := func() int {
		return cluster.Node(idx).Replication().Status().Peers
	}
	deadline := time.Now().Add(timeout)
	for {
		if peers() == want {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("node %d connected peers = %d, want %d within %v", idx, peers(), want, timeout)
		}
		time.Sleep(topoPoll)
	}
}

// peerHave is the sender's record of a peer's applied watermark for one
// origin (Rust: peerHave over Have), or 0 when no record exists yet.
func peerHave(t *testing.T, cluster *harness.Cluster, snd int, peer, origin [16]byte) uint64 {
	t.Helper()
	status, err := cluster.Node(snd).RetentionStatus()
	if err != nil {
		t.Fatalf("retention status: %v", err)
	}
	for _, p := range status.Peers {
		if p.Node == peer {
			return p.Acknowledged[origin]
		}
	}
	return 0
}

// waitHaveSettled is the quiet-baseline ack for one origin: the mesh is
// converged at every call site, so the current ack settles at the sender's
// head within milliseconds; require it to hold still across
// haveStableWindow before treating it as the absolute baseline.
//
// Stability alone is not enough: a dropped or backlogged ack leaves the
// watermark stable *below* the true value, and the post-restart re-ack then
// misattributes as the trial's advance, measuring transfer time instead of
// publication lag. The mesh is converged at every call site, so both the
// victim and the witness have acked everything: wait for the two
// independent ack paths to agree exactly (plus stability) instead of
// trusting one path's stability. Ack redundancy arrives via 10s
// anti-entropy progress pages, so the timeout spans several cycles.
func waitHaveSettled(t *testing.T, cluster *harness.Cluster, snd int, peer, origin [16]byte) uint64 {
	t.Helper()
	witnessID := cluster.NodeID(witness)
	deadline := time.Now().Add(haveTimeout)
	current := peerHave(t, cluster, snd, peer, origin)
	stableSince := time.Now()
	for {
		wit := peerHave(t, cluster, snd, witnessID, origin)
		if time.Since(stableSince) >= haveStableWindow && current == wit {
			return current
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("sender view of peer watermark for origin did not settle within %v (victim %d vs witness %d)",
				haveTimeout, current, wit)
		}
		time.Sleep(havePoll)
		next := peerHave(t, cluster, snd, peer, origin)
		if next != current {
			current = next
			stableSince = time.Now()
		}
	}
}

// pushed counts transactions pushed by one node (transfer-start signal;
// Rust watches the victim's inbound frame bytes, which have no
// equivalent).
// logCounters are the log-path activity counters for one node:
// (received_transactions, applied_transactions). Snapshot application
// bypasses both counters, so post-restart advancement proves the trial
// arrived and applied as a log transaction, not as a snapshot.
func logCounters(cluster *harness.Cluster, idx int) (uint64, uint64) {
	m := cluster.Node(idx).Replication().Status().Metrics
	return m.ReceivedTransactions, m.AppliedTransactions
}

// committed is the local commit cursor (Rust: nodeStatus LocalSeq advance
// check).
func committed(cluster *harness.Cluster, idx int) uint64 {
	return cluster.Node(idx).Status().Committed
}

// commitBigTx commits one multi-megabyte transaction: nrows inserts with
// large phone values (Rust: buildBigRows + TypedContentionInsertMany).
// Large phone values carry the payload; name carries the prefix. It fails
// loudly unless the payload exceeds the 8 MiB replication frame (the engine
// chunks transactions at 64 KiB, so this trips chunking with wide margin).
func commitBigTx(t *testing.T, cluster *harness.Cluster, node int, prefix string, idBase int64, nrows, valueBytes int) {
	t.Helper()
	values := make([]bigRow, 0, nrows)
	payloadBytes := 0
	for i := 0; i < nrows; i++ {
		row := make([]byte, valueBytes)
		copy(row, "payload-")
		for j := 8; j < len(row); j++ {
			row[j] = byte('a' + (idBase+int64(i)+int64(j))%26)
		}
		phone := string(row)
		id := fmt.Sprintf("%032x", idBase+int64(i))
		name := fmt.Sprintf("%s%06d", prefix, i)
		payloadBytes += len(id) + len(name) + len(phone) + 8
		values = append(values, bigRow{id: id, name: name, phone: phone})
	}
	t.Logf("big transaction %.1f MiB (%d rows x %d B values)",
		float64(payloadBytes)/float64(1<<20), nrows, valueBytes)
	if payloadBytes <= 8<<20 {
		t.Fatalf("big transaction is %d bytes, want > 8 MiB so it must travel chunked", payloadBytes)
	}
	if _, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		for _, v := range values {
			if _, err := s.Execute("INSERT INTO records(id, name, phone) VALUES(?, ?, ?)",
				v.id, v.name, v.phone); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("node %d big-tx commit (%s): %v", node, prefix, err)
	}
	if err := cluster.Node(node).Flush(); err != nil {
		t.Fatalf("flush after big-tx: %v", err)
	}
}

// partition isolates victim from both peers in both directions (Rust:
// partition).
func partition(t *testing.T, cluster *harness.Cluster, v int) {
	t.Helper()
	for peer := 0; peer < cluster.Len(); peer++ {
		if peer == v {
			continue
		}
		if err := cluster.RemovePeer(v, peer); err != nil {
			t.Fatalf("remove peer %d->%d: %v", v, peer, err)
		}
		if err := cluster.RemovePeer(peer, v); err != nil {
			t.Fatalf("remove peer %d->%d: %v", peer, v, err)
		}
	}
}

// heal rejoins victim with both peers in both directions (Rust: heal).
func heal(t *testing.T, cluster *harness.Cluster, v int) {
	t.Helper()
	for peer := 0; peer < cluster.Len(); peer++ {
		if peer == v {
			continue
		}
		if err := cluster.AddPeer(v, peer); err != nil {
			t.Fatalf("add peer %d->%d: %v", v, peer, err)
		}
		if err := cluster.AddPeer(peer, v); err != nil {
			t.Fatalf("add peer %d->%d: %v", peer, v, err)
		}
	}
}

// stopOncePushFlows stops the victim once the trial transfer to it is
// provably underway but incomplete: a push from a trial holder started
// after the heal while the victim still shows none of the trial rows. It
// returns true when the victim converged first (the stop would prove
// nothing; the caller retries with fresh rows), mirroring the
// inbound-byte trigger, which keys on inbound frame bytes instead of
// push-start.
// served counts both delivery mechanisms to the victim: sender push
// plus need-driven (anti-entropy pull) responses.
func served(cluster *harness.Cluster, idx int) uint64 {
	m := cluster.Node(idx).Replication().Status().Metrics
	return m.PushedTransactions + m.NeedServed
}

func stopOncePushFlows(t *testing.T, cluster *harness.Cluster, prefix string, nrows int, servedBefore uint64) bool {
	t.Helper()
	// Check the cheap served counters first: a COUNT(*) over the
	// victim's writer every iteration costs milliseconds against a
	// transfer that completes in tens of milliseconds, so the stop
	// would systematically land after convergence. The row check runs
	// only once delivery started, to tell a converged victim (retry
	// with fresh rows) from a mid-transfer one.
	deadline := time.Now().Add(killTimeout)
	for {
		if !time.Now().Before(deadline) {
			n := prefixCount(t, cluster, victim, prefix)
			t.Fatalf("no delivery to the victim observed within %v (trial rows visible: %d/%d)",
				killTimeout, n, nrows)
		}
		servedNow := served(cluster, sender) + served(cluster, witness)
		if servedNow > servedBefore {
			if prefixCount(t, cluster, victim, prefix) == nrows {
				return true
			}
			t.Logf("stopping victim mid-transfer after %d trial deliveries started", servedNow-servedBefore)
			cluster.StopNode(victim)
			return false
		}
		time.Sleep(killPoll)
	}
}

// restartNode restarts the victim in place and rejoins the mesh (Rust:
// restartNode).
func restartNode(t *testing.T, cluster *harness.Cluster, idx int) {
	t.Helper()
	if err := cluster.StartNode(idx); err != nil {
		t.Fatalf("node%d restart failed: %v", idx, err)
	}
	// The reopened node rebinds a fresh ephemeral port: re-mesh so every
	// ordered pair dials the current addresses.
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	for i := 0; i < cluster.Len(); i++ {
		waitConnectedPeers(t, cluster, i, cluster.Len()-1, healPeersTimeout)
	}
}

// watchRetry polls the retry to completion and enforces the resume contract
// on every sample: the sender always shows the full trial (its commit was
// durable) and the victim shows none or all of it (never a partial
// transaction). The victim's applied watermark for the trial origin (as
// acked to the sender) advances on durable apply; the first
// advanced-watermark sample must precede full visibility by at most the
// publication bound. It returns the victim's post-restart (received,
// applied) counters (Rust: watchRetry).
func watchRetry(t *testing.T, cluster *harness.Cluster, prefix string, nrows int, havePre uint64) (uint64, uint64) {
	t.Helper()
	victimID := cluster.NodeID(victim)
	senderID := cluster.NodeID(sender)
	deadline := time.Now().Add(retryTimeout)
	var firstAdvanced, firstFull time.Time
	for {
		if !time.Now().Before(deadline) {
			t.Fatalf("retry did not complete within %v (victim rows: %d/%d, watermark: %d, want >= %d)",
				retryTimeout, prefixCount(t, cluster, victim, prefix), nrows,
				peerHave(t, cluster, sender, victimID, senderID), havePre+1)
		}
		now := time.Now()
		have := peerHave(t, cluster, sender, victimID, senderID)
		visVictim := prefixCount(t, cluster, victim, prefix)
		visSender := prefixCount(t, cluster, sender, prefix)
		received, applied := logCounters(cluster, victim)
		if visSender != nrows {
			t.Fatalf("sender shows %d/%d trial rows after committing them", visSender, nrows)
		}
		if visVictim != 0 && visVictim != nrows {
			t.Fatalf("victim shows %d/%d trial rows: partial transaction visible", visVictim, nrows)
		}
		if have > havePre && firstAdvanced.IsZero() {
			firstAdvanced = now
		}
		if visVictim == nrows && firstFull.IsZero() {
			firstFull = now
		}
		if visVictim == nrows && have >= havePre+1 {
			if firstAdvanced.IsZero() || firstFull.IsZero() {
				t.Fatalf("converged without observing watermark advance (advanced=%v full=%v)",
					firstAdvanced, firstFull)
			}
			lag := firstFull.Sub(firstAdvanced)
			if lag < 0 {
				lag = 0
			}
			t.Logf("watermark advanced %v before full trial visibility", lag)
			if lag > publicationBound {
				t.Fatalf("watermark advanced %v before completion, want <= %v (early advance)",
					lag, publicationBound)
			}
			return received, applied
		}
		time.Sleep(retryPoll)
	}
}

// envUsize is a positive-integer env knob with fallback (Rust: envInt).
func envUsize(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

func TestChunkedTransactionResumesAfterReceiverRestart(t *testing.T) {
	nrows := envUsize("MURMUR_TXCHUNK_ROWS", 1200)
	// ~42 MiB per trial: the kill must land mid-transfer, and a 10 MiB
	// localhost transfer completes before even a fast stop takes
	// effect. Larger trials widen the transfer window past kill
	// latency with margin.
	valueBytes := envUsize("MURMUR_TXCHUNK_VALUE_BYTES", 36000)
	maxAttempts := envUsize("MURMUR_TXCHUNK_ATTEMPTS", 3)
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "txchunk-resume",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("txchunk-resume-no-convergence")
		}
		cluster.Finish(failed)
	}()

	victimID := cluster.NodeID(victim)
	senderID := cluster.NodeID(sender)

	// Same schema on every node (Rust: TypedRecords fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i,
			"CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL)"); err != nil {
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

	// Positive control (honest behavior works): a multi-chunk
	// transaction replicates to every node with no stopping.
	commitBigTx(t, cluster, sender, "base-", 0, nrows, valueBytes)
	waitConverged(t, cluster, nrows, positiveTimeout, "positive control")
	t.Logf("positive control: multi-chunk transaction replicated to all nodes")

	// Resume trials. Each attempt commits a fresh big transaction
	// while the victim is partitioned, heals, stops the victim once
	// the trial transfer to it starts flowing, restarts it, and
	// watches the retry.
	expectedRows := nrows
	resumed := false
	for attempt := 1; attempt <= maxAttempts && !resumed; attempt++ {
		prefix := fmt.Sprintf("a%d-", attempt)
		t.Logf("resume attempt %d/%d", attempt, maxAttempts)

		// Quiet baseline BEFORE partitioning: the victim must have
		// acked everything the sender has before the trial starts.
		// The mesh is converged at every loop top (positive control
		// or the previous attempt's reconvergence wait).
		seqPre := committed(cluster, sender)
		havePre := waitHaveSettled(t, cluster, sender, victimID, senderID)

		partition(t, cluster, victim)
		waitConnectedPeers(t, cluster, victim, 0, splitPeersTimeout)
		// Log-path baseline on the victim (Rust: snapshot counters that
		// have no equivalent).
		recvPreKill, appliedPreKill := logCounters(cluster, victim)
		t.Logf("victim pre-trial received_transactions=%d applied_transactions=%d",
			recvPreKill, appliedPreKill)

		commitBigTx(t, cluster, sender, prefix, int64(attempt)*1000000, nrows, valueBytes)
		seqTx := committed(cluster, sender)
		if seqTx <= seqPre {
			t.Fatalf("attempt %d: sender commit cursor did not advance (%d -> %d)",
				attempt, seqPre, seqTx)
		}
		expectedRows += nrows

		// Survivors converge on the trial rows; the victim must miss them.
		waitPrefixCount(t, cluster, sender, prefix, nrows, survivorTimeout)
		waitPrefixCount(t, cluster, witness, prefix, nrows, survivorTimeout)
		if got := prefixCount(t, cluster, victim, prefix); got != 0 {
			t.Fatalf("attempt %d: victim sees %d trial rows while partitioned, want 0",
				attempt, got)
		}

		// Heal, then stop once the trial transfer to the victim is
		// flowing. Survivor convergence above proves every pre-heal
		// delivery already landed, so any push or need-serve
		// advancement after this sample is victim-bound trial flow.
		// Both mechanisms must be watched: post-heal delivery races
		// between sender push and victim anti-entropy pull, and
		// watching push alone times out whenever the pull wins
		// despite full convergence.
		servedBefore := served(cluster, sender) + served(cluster, witness)
		heal(t, cluster, victim)
		if stopOncePushFlows(t, cluster, prefix, nrows, servedBefore) {
			t.Logf("attempt %d: victim converged before the stop landed; retrying with fresh rows", attempt)
			waitConverged(t, cluster, expectedRows, attemptConvergeTimeout, "too-slow reconverge")
			continue
		}
		restartNode(t, cluster, victim)

		receivedPost, appliedPost := watchRetry(t, cluster, prefix, nrows, havePre)
		t.Logf("attempt %d: victim staged %d and applied %d transaction(s) after restart",
			attempt, receivedPost, appliedPost)
		if receivedPost < 1 {
			// The victim staged nothing after restarting, so it must
			// have staged the trial before the stop: the trial proves
			// nothing about resume; retry fresh.
			t.Logf("attempt %d: no post-restart receipt; stop landed too late, retrying", attempt)
			waitConverged(t, cluster, expectedRows, attemptConvergeTimeout, "too-late reconverge")
			continue
		}
		resumed = true

		// The retry must have used the log/chunk path, not snapshots:
		// snapshot application bypasses both counters, so post-restart
		// receipt AND apply prove log-path resume. The engine only
		// requests snapshots from explicitly trusted sources, and
		// harness clusters never configure any, so no snapshot could
		// have been taken anywhere.
		if appliedPost < 1 {
			t.Fatalf("victim applied %d transaction(s) after restart, want >= 1 "+
				"(resume must use chunks/log, not snapshot)", appliedPost)
		}
		metrics := cluster.Node(victim).Replication().Status().Metrics
		t.Logf("attempt %d: resume used log path (chunk_need_sent=%d chunk_need_served=%d)",
			attempt, metrics.ChunkNeedSent, metrics.ChunkNeedServed)
	}
	if !resumed {
		t.Fatalf("no valid resume trial in %d attempts (stop never landed before convergence)", maxAttempts)
	}

	// Final: exact row counts and identical ordered digests everywhere.
	waitConverged(t, cluster, expectedRows, finalTimeout, "final")
	failed = false
}

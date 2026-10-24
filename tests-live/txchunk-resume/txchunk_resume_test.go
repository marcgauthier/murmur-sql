// Live scenario: a chunked transaction resumes after a receiver restart
// with all-or-nothing visibility.
//
// Go port of tests/txchunk_resume.rs using tests-live/goharness.
// Identical behavior: three real murmur-server children mesh; a single
// transaction larger than the replication frame travels as ~64 KiB
// transaction chunks; the receiving node is stopped mid-transfer and
// restarted, and the retry must complete with all-or-nothing visibility,
// the applied watermark must never advance before completion, and all
// digests must converge without any snapshot transfer. A staged-gauge /
// head-start trigger replaces the inbound-byte kill trigger.
package txchunkresume

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

const (
	// sender, victim, witness are the role indices.
	sender  = 0
	victim  = 1
	witness = 2
)

const (
	// positiveTimeout is the positive-control convergence deadline
	// (Rust: 120 s).
	positiveTimeout = 120 * time.Second
	// haveTimeout is the baseline ack-settle deadline (Rust: 15 s).
	haveTimeout = 15 * time.Second
	// havePoll is the ack-settle poll interval (Rust: 200 ms).
	havePoll = 200 * time.Millisecond
	// haveStableWindow is how long the ack value must hold to count as
	// settled (Rust: 2 s).
	haveStableWindow = 2 * time.Second
	// splitPeersTimeout is the partition deadline on the victim
	// (Rust: 15 s).
	splitPeersTimeout = 15 * time.Second
	// survivorTimeout is the survivor trial-visibility deadline
	// (Rust: 60 s).
	survivorTimeout = 60 * time.Second
	// prefixPoll is the prefix-count poll interval (Rust: 100 ms).
	prefixPoll = 100 * time.Millisecond
	// killTimeout is the mid-transfer stop deadline (Rust: 15 s).
	killTimeout = 15 * time.Second
	// killPoll is the mid-transfer stop poll interval (Rust: 5 ms).
	killPoll = 5 * time.Millisecond
	// healPeersTimeout is the post-restart mesh deadline (Rust: 30 s).
	healPeersTimeout = 30 * time.Second
	// retryTimeout is the retry-completion deadline (Rust: 120 s).
	retryTimeout = 120 * time.Second
	// retryPoll is the retry-completion poll interval (Rust: 25 ms).
	retryPoll = 25 * time.Millisecond
	// publicationBound is the watermark-advance publication bound
	// (Rust: 10 s).
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
	// headStart is the post-heal delay before the stop lands when the
	// staged gauge never fires (Rust: 1500 ms).
	headStart = 1500 * time.Millisecond
)

const (
	// replPeers, replStaged, replApplied, replCommitted are MURMUR SHOW
	// REPLICATION column indices (no header over Simple).
	replPeers     = 2
	replStaged    = 4
	replApplied   = 5
	replCommitted = 7
)

const (
	// retNode, retOrigin, retAcked are MURMUR SHOW RETENTION column
	// indices (no header over Simple).
	retNode   = 0
	retOrigin = 3
	retAcked  = 4
)

// bigRow is one replicated row (Rust: BigRow).
type bigRow struct {
	id    string
	name  string
	phone string
}

// rows reads all rows on one node (Rust: rows).
func rows(cluster *goharness.Cluster, idx int) ([]bigRow, error) {
	dbRows, err := cluster.Node(idx).Query("SELECT id, name, phone FROM records")
	if err != nil {
		return nil, fmt.Errorf("node%d SELECT rows: %w", idx+1, err)
	}
	out := make([]bigRow, 0, len(dbRows))
	for _, row := range dbRows {
		id, err := row.String(0)
		if err != nil {
			return nil, fmt.Errorf("node%d decode id: %w", idx+1, err)
		}
		name, err := row.String(1)
		if err != nil {
			return nil, fmt.Errorf("node%d decode name: %w", idx+1, err)
		}
		phone, err := row.String(2)
		if err != nil {
			return nil, fmt.Errorf("node%d decode phone: %w", idx+1, err)
		}
		out = append(out, bigRow{id: id, name: name, phone: phone})
	}
	return out, nil
}

// digest renders the canonical digest of a row set (Rust: digest).
func digest(rows []bigRow) string {
	sort.Slice(rows, func(a, b int) bool {
		if rows[a].id != rows[b].id {
			return rows[a].id < rows[b].id
		}
		if rows[a].name != rows[b].name {
			return rows[a].name < rows[b].name
		}
		return rows[a].phone < rows[b].phone
	})
	return fmt.Sprintf("%#v", rows)
}

// rowCount counts rows on one node (cheap pre-check before full digest
// reads).
func rowCount(cluster *goharness.Cluster, idx int) (int, error) {
	dbRows, err := cluster.Node(idx).Query("SELECT COUNT(*) FROM records")
	if err != nil {
		return 0, fmt.Errorf("node%d SELECT count: %w", idx+1, err)
	}
	if len(dbRows) == 0 {
		return 0, fmt.Errorf("node%d count returned no rows", idx+1)
	}
	n, err := dbRows[0].Int64(0)
	if err != nil {
		return 0, fmt.Errorf("node%d decode count: %w", idx+1, err)
	}
	return int(n), nil
}

// prefixCount counts rows on one node whose name starts with prefix
// (Rust: prefix_count).
func prefixCount(cluster *goharness.Cluster, idx int, prefix string) (int, error) {
	// The length is inlined as a literal (injection-safe: int): the
	// server reports expression-position parameters as type Unknown and
	// the driver refuses to serialize non-string types for them, so
	// only string params may sit in positions the server cannot infer
	// from a column.
	dbRows, err := cluster.Node(idx).Query(
		fmt.Sprintf("SELECT COUNT(*) FROM records WHERE substr(name, 1, %d) = $1", len(prefix)),
		prefix)
	if err != nil {
		return 0, fmt.Errorf("node%d prefix count: %w", idx+1, err)
	}
	if len(dbRows) == 0 {
		return 0, fmt.Errorf("node%d prefix count returned no rows", idx+1)
	}
	n, err := dbRows[0].Int64(0)
	if err != nil {
		return 0, fmt.Errorf("node%d decode prefix count: %w", idx+1, err)
	}
	return int(n), nil
}

// waitPrefixCount waits until one node holds exactly want prefixed rows
// (Rust: wait_prefix_count).
func waitPrefixCount(cluster *goharness.Cluster, idx int, prefix string, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		n, err := prefixCount(cluster, idx, prefix)
		if err != nil {
			return err
		}
		if n == want {
			return nil
		}
		if !time.Now().Before(deadline) {
			n, err := prefixCount(cluster, idx, prefix)
			if err != nil {
				return err
			}
			return fmt.Errorf("node%d %q rows = %d, want %d within %v",
				idx+1, prefix, n, want, timeout)
		}
		time.Sleep(prefixPoll)
	}
}

// waitConverged waits until every node holds exactly want rows with
// equal digests (Rust: wait_converged). Counts are checked first so the
// multi-megabyte digest reads only run once the counts already match.
func waitConverged(cluster *goharness.Cluster, want int, timeout time.Duration, what string) error {
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
		allCounts := true
		for _, n := range counts {
			allCounts = allCounts && n == want
		}
		if allCounts {
			first := ""
			equal := true
			for i := 0; i < cluster.Len(); i++ {
				d, err := rows(cluster, i)
				if err != nil {
					return err
				}
				g := digest(d)
				if i == 0 {
					first = g
				} else if g != first {
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
				nodeRows, err := rows(cluster, i)
				if err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "node%d at timeout: count=%d digest=%s\n",
					i+1, len(nodeRows), digest(nodeRows))
			}
			return fmt.Errorf(
				"nodes did not converge on %d rows with equal digests within %v (%s)",
				want, timeout, what)
		}
		time.Sleep(convergePoll)
	}
}

// showReplication reads one MURMUR SHOW REPLICATION row (single-row
// output).
func showReplication(cluster *goharness.Cluster, idx int) ([]string, error) {
	got, err := cluster.Node(idx).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		return nil, fmt.Errorf("node%d SHOW REPLICATION: %w", idx+1, err)
	}
	if len(got) == 0 {
		return nil, fmt.Errorf("node%d SHOW REPLICATION returned no rows", idx+1)
	}
	return got[0], nil
}

func showCol(row []string, idx, col int, what string) (uint64, error) {
	if col >= len(row) {
		return 0, fmt.Errorf("node%d SHOW REPLICATION has no %s column", idx+1, what)
	}
	n, err := strconv.ParseUint(row[col], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("node%d parse %s: %w", idx+1, what, err)
	}
	return n, nil
}

// waitConnectedPeers waits until one node reports exactly want
// replication peers (Rust: wait_connected_peers).
func waitConnectedPeers(cluster *goharness.Cluster, idx, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		row, err := showReplication(cluster, idx)
		if err != nil {
			return err
		}
		peers, err := showCol(row, idx, replPeers, "peers")
		if err != nil {
			return err
		}
		if int(peers) == want {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("node%d connected peers = %d, want %d within %v",
				idx+1, peers, want, timeout)
		}
		time.Sleep(topoPoll)
	}
}

// peerHave reads the sender's record of a peer's applied watermark for
// one origin, or 0 when no record exists yet (Rust: peer_have).
func peerHave(cluster *goharness.Cluster, sender int, peer, origin [16]byte) (uint64, error) {
	peerHex := goharness.Hex(peer[:])
	originHex := goharness.Hex(origin[:])
	got, err := cluster.Node(sender).Simple("MURMUR SHOW RETENTION")
	if err != nil {
		return 0, fmt.Errorf("node%d SHOW RETENTION: %w", sender+1, err)
	}
	for _, row := range got {
		nodeMatch := retNode < len(row) && row[retNode] == peerHex
		originMatch := retOrigin < len(row) && row[retOrigin] == originHex
		if nodeMatch && originMatch {
			if retAcked >= len(row) {
				return 0, fmt.Errorf("node%d SHOW RETENTION has no acknowledged column", sender+1)
			}
			ack := row[retAcked]
			if ack == "" {
				return 0, nil
			}
			n, err := strconv.ParseUint(ack, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("node%d parse acknowledged: %w", sender+1, err)
			}
			return n, nil
		}
	}
	return 0, nil
}

// waitHaveSettled waits for the quiet-baseline ack for one origin: the
// mesh is converged at every call site, so the current ack settles at
// the sender's head within milliseconds; require it to hold still
// across haveStableWindow before treating it as the absolute baseline
// (Rust: wait_have_settled).
func waitHaveSettled(cluster *goharness.Cluster, sender int, peer, origin [16]byte) (uint64, error) {
	deadline := time.Now().Add(haveTimeout)
	current, err := peerHave(cluster, sender, peer, origin)
	if err != nil {
		return 0, err
	}
	stableSince := time.Now()
	for {
		if time.Since(stableSince) >= haveStableWindow {
			return current, nil
		}
		if !time.Now().Before(deadline) {
			return 0, fmt.Errorf(
				"sender view of peer watermark for origin did not settle within %v", haveTimeout)
		}
		time.Sleep(havePoll)
		next, err := peerHave(cluster, sender, peer, origin)
		if err != nil {
			return 0, err
		}
		if next != current {
			current = next
			stableSince = time.Now()
		}
	}
}

// staged reads the currently staged (received, not yet applied)
// transactions on one node: a gauge, used as the transfer-start signal.
func staged(cluster *goharness.Cluster, idx int) (uint64, error) {
	row, err := showReplication(cluster, idx)
	if err != nil {
		return 0, err
	}
	return showCol(row, idx, replStaged, "staged_transactions")
}

// applied reads the cumulative log-applied transactions on one node.
// Snapshot application bypasses this counter, so post-restart
// advancement proves the trial arrived and applied as a log
// transaction, not as a snapshot.
func applied(cluster *goharness.Cluster, idx int) (uint64, error) {
	row, err := showReplication(cluster, idx)
	if err != nil {
		return 0, err
	}
	return showCol(row, idx, replApplied, "applied_transactions")
}

// committed reads the local commit cursor (Rust: committed).
func committed(cluster *goharness.Cluster, idx int) (uint64, error) {
	row, err := showReplication(cluster, idx)
	if err != nil {
		return 0, err
	}
	return showCol(row, idx, replCommitted, "committed")
}

// commitBigTx commits one multi-megabyte transaction: n inserts with
// large phone values. Fails loudly unless the payload exceeds the 8 MiB
// replication frame (Rust: commit_big_tx).
func commitBigTx(cluster *goharness.Cluster, node int, prefix string, idBase int64, n, valueBytes int) error {
	values := make([]bigRow, 0, n)
	payloadBytes := 0
	for i := 0; i < n; i++ {
		buf := make([]byte, valueBytes)
		copy(buf, "payload-")
		for j := 8; j < len(buf); j++ {
			buf[j] = 'a' + byte((idBase+int64(i)+int64(j))%26)
		}
		id := fmt.Sprintf("%032x", idBase+int64(i))
		name := fmt.Sprintf("%s%06d", prefix, i)
		payloadBytes += len(id) + len(name) + len(buf) + 8
		values = append(values, bigRow{id: id, name: name, phone: string(buf)})
	}
	fmt.Fprintf(os.Stderr, "big transaction %.1f MiB (%d rows x %d B values)\n",
		float64(payloadBytes)/float64(1<<20), n, valueBytes)
	if payloadBytes <= 8<<20 {
		return fmt.Errorf("big transaction is %d bytes, want > 8 MiB so it must travel chunked",
			payloadBytes)
	}
	// One transaction over the shared connection: the driver is
	// sequential, so nothing else queries mid-transaction.
	client := cluster.Node(node).Inner()
	if err := client.BatchExecute("BEGIN"); err != nil {
		return fmt.Errorf("node%d big-tx BEGIN (%s): %w", node+1, prefix, err)
	}
	for _, v := range values {
		if _, err := client.Execute(
			"INSERT INTO records(id, name, phone) VALUES($1, $2, $3)", v.id, v.name, v.phone); err != nil {
			return fmt.Errorf("node%d big-tx insert (%s): %w", node+1, prefix, err)
		}
	}
	if err := client.BatchExecute("COMMIT"); err != nil {
		return fmt.Errorf("node%d big-tx COMMIT (%s): %w", node+1, prefix, err)
	}
	if _, err := cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("node%d flush after big-tx (%s): %w", node+1, prefix, err)
	}
	return nil
}

// partition isolates victim from both peers in both directions (Rust:
// partition).
func partition(cluster *goharness.Cluster, victim int) error {
	for peer := 0; peer < cluster.Len(); peer++ {
		if peer == victim {
			continue
		}
		if err := cluster.RemovePeer(victim, peer); err != nil {
			return fmt.Errorf("remove peer %d->%d: %w", victim, peer, err)
		}
		if err := cluster.RemovePeer(peer, victim); err != nil {
			return fmt.Errorf("remove peer %d->%d: %w", peer, victim, err)
		}
	}
	return nil
}

// heal rejoins victim with both peers in both directions (Rust: heal).
func heal(cluster *goharness.Cluster, victim int) error {
	for peer := 0; peer < cluster.Len(); peer++ {
		if peer == victim {
			continue
		}
		if err := cluster.AddPeer(victim, peer); err != nil {
			return fmt.Errorf("add peer %d->%d: %w", victim, peer, err)
		}
		if err := cluster.AddPeer(peer, victim); err != nil {
			return fmt.Errorf("add peer %d->%d: %w", peer, victim, err)
		}
	}
	return nil
}

// stopOncePushFlows stops the victim once the trial transfer to it is
// provably underway but incomplete. It returns true when the victim
// converged first (the stop would prove nothing; the caller retries
// with fresh rows). (Rust: stop_once_push_flows.)
func stopOncePushFlows(cluster *goharness.Cluster, prefix string, n int) (bool, error) {
	start := time.Now()
	deadline := start.Add(killTimeout)
	for {
		if !time.Now().Before(deadline) {
			vis, err := prefixCount(cluster, victim, prefix)
			if err != nil {
				return false, err
			}
			return false, fmt.Errorf(
				"no push to the victim observed within %v (trial rows visible: %d/%d)",
				killTimeout, vis, n)
		}
		vis, err := prefixCount(cluster, victim, prefix)
		if err != nil {
			return false, err
		}
		if vis == n {
			return true, nil
		}
		st, err := staged(cluster, victim)
		if err != nil {
			return false, err
		}
		if st > 0 {
			fmt.Fprintf(os.Stderr, "stopping victim mid-transfer: trial staged but not yet visible\n")
			cluster.StopNode(victim)
			return false, nil
		}
		if time.Since(start) >= headStart {
			fmt.Fprintf(os.Stderr,
				"stopping victim %v after the heal: multi-megabyte trial is mid-transfer\n", headStart)
			cluster.StopNode(victim)
			return false, nil
		}
		time.Sleep(killPoll)
	}
}

// restartNode restarts the victim in place and rejoins the mesh (Rust:
// restart_node).
func restartNode(cluster *goharness.Cluster, idx int) error {
	if err := cluster.StartNode(idx); err != nil {
		return fmt.Errorf("node%d restart failed: %w", idx, err)
	}
	// The restarted node rebinds fresh ports: re-mesh so every ordered
	// pair dials the current addresses.
	if err := cluster.Mesh(); err != nil {
		return fmt.Errorf("re-mesh after restart: %w", err)
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := waitConnectedPeers(cluster, i, cluster.Len()-1, healPeersTimeout); err != nil {
			return err
		}
	}
	return nil
}

// watchRetry polls the retry to completion and enforces the resume
// contract on every sample, returning the victim's post-restart applied
// counter (Rust: watch_retry). Counters reset on restart, so any
// post-restart value is retry activity.
func watchRetry(cluster *goharness.Cluster, prefix string, n int, havePre uint64) (uint64, error) {
	victimID := cluster.NodeID(victim)
	senderID := cluster.NodeID(sender)
	deadline := time.Now().Add(retryTimeout)
	var firstAdvanced, firstFull *time.Time
	for {
		if !time.Now().Before(deadline) {
			vis, err := prefixCount(cluster, victim, prefix)
			if err != nil {
				return 0, err
			}
			have, err := peerHave(cluster, sender, victimID, senderID)
			if err != nil {
				return 0, err
			}
			return 0, fmt.Errorf(
				"retry did not complete within %v (victim rows: %d/%d, watermark: %d, want >= %d)",
				retryTimeout, vis, n, have, havePre+1)
		}
		now := time.Now()
		have, err := peerHave(cluster, sender, victimID, senderID)
		if err != nil {
			return 0, err
		}
		visVictim, err := prefixCount(cluster, victim, prefix)
		if err != nil {
			return 0, err
		}
		visSender, err := prefixCount(cluster, sender, prefix)
		if err != nil {
			return 0, err
		}
		appliedNow, err := applied(cluster, victim)
		if err != nil {
			return 0, err
		}
		if visSender != n {
			return 0, fmt.Errorf("sender shows %d/%d trial rows after committing them", visSender, n)
		}
		if visVictim != 0 && visVictim != n {
			return 0, fmt.Errorf("victim shows %d/%d trial rows: partial transaction visible",
				visVictim, n)
		}
		if have > havePre && firstAdvanced == nil {
			firstAdvanced = &now
		}
		if visVictim == n && firstFull == nil {
			firstFull = &now
		}
		if visVictim == n && have >= havePre+1 {
			if firstAdvanced == nil || firstFull == nil {
				return 0, fmt.Errorf(
					"converged without observing watermark advance (advanced=%v full=%v)",
					firstAdvanced != nil, firstFull != nil)
			}
			lag := firstFull.Sub(*firstAdvanced)
			if lag < 0 {
				lag = 0
			}
			fmt.Fprintf(os.Stderr, "watermark advanced %v before full trial visibility\n", lag)
			if lag > publicationBound {
				return 0, fmt.Errorf(
					"watermark advanced %v before completion, want <= %v (early advance)",
					lag, publicationBound)
			}
			return appliedNow, nil
		}
		time.Sleep(retryPoll)
	}
}

// envUsize reads a positive-integer env knob with fallback (Rust:
// env_usize).
func envUsize(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

func runBody(cluster *goharness.Cluster, rowsPerTx, valueBytes, maxAttempts int) error {
	victimID := cluster.NodeID(victim)
	senderID := cluster.NodeID(sender)

	// Same schema on every node, then replicate it: row convergence
	// needs the per-table flag on each node.
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i,
			"CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL)"); err != nil {
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

	// Positive control (honest behavior works): a multi-chunk
	// transaction replicates to every node with no stopping.
	if err := commitBigTx(cluster, sender, "base-", 0, rowsPerTx, valueBytes); err != nil {
		return err
	}
	if err := waitConverged(cluster, rowsPerTx, positiveTimeout, "positive control"); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "positive control: multi-chunk transaction replicated to all nodes\n")

	// Resume trials. Each attempt commits a fresh big transaction while
	// the victim is partitioned, heals, stops the victim once the trial
	// transfer to it starts flowing, restarts it, and watches the retry.
	expectedRows := rowsPerTx
	resumed := false
	for attempt := 1; attempt <= maxAttempts && !resumed; attempt++ {
		prefix := fmt.Sprintf("a%d-", attempt)
		fmt.Fprintf(os.Stderr, "resume attempt %d/%d\n", attempt, maxAttempts)

		// Quiet baseline BEFORE partitioning: the victim must have
		// acked everything the sender has before the trial starts.
		// The mesh is converged at every loop top (positive control
		// or the previous attempt's reconvergence wait).
		seqPre, err := committed(cluster, sender)
		if err != nil {
			return err
		}
		havePre, err := waitHaveSettled(cluster, sender, victimID, senderID)
		if err != nil {
			return err
		}

		if err := partition(cluster, victim); err != nil {
			return err
		}
		if err := waitConnectedPeers(cluster, victim, 0, splitPeersTimeout); err != nil {
			return err
		}

		if err := commitBigTx(cluster, sender, prefix, int64(attempt)*1_000_000,
			rowsPerTx, valueBytes); err != nil {
			return err
		}
		seqTx, err := committed(cluster, sender)
		if err != nil {
			return err
		}
		if seqTx <= seqPre {
			return fmt.Errorf("attempt %d: sender commit cursor did not advance (%d -> %d)",
				attempt, seqPre, seqTx)
		}
		expectedRows += rowsPerTx

		// Survivors converge on the trial rows; the victim must miss them.
		if err := waitPrefixCount(cluster, sender, prefix, rowsPerTx, survivorTimeout); err != nil {
			return err
		}
		if err := waitPrefixCount(cluster, witness, prefix, rowsPerTx, survivorTimeout); err != nil {
			return err
		}
		leaked, err := prefixCount(cluster, victim, prefix)
		if err != nil {
			return err
		}
		if leaked != 0 {
			return fmt.Errorf("attempt %d: victim sees %d trial rows while partitioned, want 0",
				attempt, leaked)
		}

		// Heal, then stop once the trial transfer to the victim is
		// flowing.
		if err := heal(cluster, victim); err != nil {
			return err
		}
		convergedFirst, err := stopOncePushFlows(cluster, prefix, rowsPerTx)
		if err != nil {
			return err
		}
		if convergedFirst {
			fmt.Fprintf(os.Stderr,
				"attempt %d: victim converged before the stop landed; retrying with fresh rows\n", attempt)
			if err := waitConverged(cluster, expectedRows, attemptConvergeTimeout,
				"too-slow reconverge"); err != nil {
				return err
			}
			continue
		}
		if err := restartNode(cluster, victim); err != nil {
			return err
		}

		appliedPost, err := watchRetry(cluster, prefix, rowsPerTx, havePre)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "attempt %d: victim applied %d transaction(s) after restart\n",
			attempt, appliedPost)
		if appliedPost < 1 {
			// The victim applied nothing after restarting, so it
			// must have applied the trial before the stop: the
			// trial proves nothing about resume; retry fresh.
			fmt.Fprintf(os.Stderr,
				"attempt %d: no post-restart apply; stop landed too late, retrying\n", attempt)
			if err := waitConverged(cluster, expectedRows, attemptConvergeTimeout,
				"too-late reconverge"); err != nil {
				return err
			}
			continue
		}
		resumed = true

		// The retry must have used the log/chunk path, not snapshots:
		// snapshot application bypasses the applied counter, so
		// post-restart apply proves log-path resume. The engine only
		// requests snapshots from explicitly trusted sources, and this
		// cluster never configures any, so no snapshot could have been
		// taken anywhere.
	}
	if !resumed {
		return fmt.Errorf("no valid resume trial in %d attempts (stop never landed before convergence)",
			maxAttempts)
	}

	// Final: exact row counts and identical ordered digests everywhere.
	return waitConverged(cluster, expectedRows, finalTimeout, "final")
}

func TestChunkedTransactionResumesAfterReceiverRestart(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	n := envUsize("MURMUR_TXCHUNK_ROWS", 1200)
	valueBytes := envUsize("MURMUR_TXCHUNK_VALUE_BYTES", 9000)
	maxAttempts := envUsize("MURMUR_TXCHUNK_ATTEMPTS", 3)
	// Single-unit shipping: the background capture tick (1 s) would
	// stream the unflushed trial as incremental captures and the default
	// 4 MiB batch unit would sign/apply a 10 MiB session as three units,
	// so the trial would converge incrementally and the all-or-nothing
	// resume contract could never hold. Stretching the tick past the
	// trial and raising the batch unit past the trial size restores
	// one-transaction shape: each trial ships on its flush as one ~10
	// MiB signed transaction (still chunked over the wire past the 1
	// MiB engine threshold) and applies atomically.
	batchBytes := 16 << 20
	captureInterval := 3600 * time.Second
	opts := goharness.DefaultClusterOptions("txchunk-resume")
	opts.NumNodes = 3
	opts.Replication = &goharness.ReplicationOptions{BatchBytes: &batchBytes}
	opts.Sqlite = &goharness.SqliteOptions{CaptureInterval: &captureInterval}
	cluster, err := goharness.NewCluster(opts)
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
	if err := runBody(cluster, n, valueBytes, maxAttempts); err != nil {
		t.Logf("scenario failed: %v", err)
		t.Fatal("scenario failed; artifacts retained under tests-live/failures/")
	}
	failed = false
}

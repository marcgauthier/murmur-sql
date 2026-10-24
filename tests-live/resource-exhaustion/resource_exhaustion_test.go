// Live scenario: fail closed under resource pressure, stay exactly
// converged after.
//
// Go port of tests/resource_exhaustion.rs using tests-live/goharness
// (3 of 5 legs). Identical behavior: stalled compaction stays correct
// (victim piles sealed spool segments while the control does not, rows
// converge exactly, debt drains after re-enable with a stable digest),
// a slow peer converges without wedging through userspace UDP throttle
// proxies in both directions and reheals to direct peering, and huge
// transactions at default budgets (8 MiB byte-exact, oversize rejected
// naming the budget with no partial row, 20 x 512 KiB converge
// exactly). The near-OOM and fd-exhaustion legs are NOT portable; see
// the scenario README.md. The Rust proxy threads become goroutines and
// the token bucket maps one-to-one.
package resourceexhaustion

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// Contention table DDL.
const tableDDL = "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)"

// Count poll interval (Go: 500 ms waitCounts).
const countPoll = 500 * time.Millisecond

// Digest poll interval (Go: 1 s waitDigests).
const digestPoll = time.Second

// Point-read poll interval (Go: 200 ms readValue).
const readPoll = 200 * time.Millisecond

// xorShift is a deterministic xorshift64* (the legs use seeded random
// incompressible values; this is its dependency-free equivalent).
type xorShift uint64

func (x *xorShift) next() uint64 {
	v := uint64(*x)
	v ^= v >> 12
	v ^= v << 25
	v ^= v >> 27
	*x = xorShift(v)
	return v * 0x2545f4914f6cdd1d
}

// alnumStrings returns n distinct incompressible alphanumeric strings
// of size bytes (repeats would compress away and the pressure would
// never bind).
func (x *xorShift) alnumStrings(n, size int) []string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		var sb strings.Builder
		sb.Grow(size)
		for j := 0; j < size; j++ {
			sb.WriteByte(alphabet[x.next()%uint64(len(alphabet))])
		}
		out = append(out, sb.String())
	}
	return out
}

// migrateAll lays the same schema on every node, flagged replicated on
// each node (row convergence needs the per-node flag).
func migrateAll(cluster *goharness.Cluster) error {
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
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
	return nil
}

// rowPair is one ordered contention row.
type rowPair struct {
	id   string
	name string
}

// tryRows returns the ordered contention rows on one node, or false
// when unreadable (wait loops retry on false, distinguishing
// unreadable nodes from true content divergence).
func tryRows(cluster *goharness.Cluster, idx int) ([]rowPair, bool) {
	dbRows, err := cluster.Node(idx).Query("SELECT id, name FROM contention ORDER BY id")
	if err != nil {
		return nil, false
	}
	out := make([]rowPair, 0, len(dbRows))
	for _, row := range dbRows {
		id, err := row.String(0)
		if err != nil {
			return nil, false
		}
		name, err := row.String(1)
		if err != nil {
			return nil, false
		}
		out = append(out, rowPair{id: id, name: name})
	}
	return out, true
}

// rows returns the ordered contention rows on one node, erroring when
// unreadable.
func rows(cluster *goharness.Cluster, idx int) ([]rowPair, error) {
	rs, ok := tryRows(cluster, idx)
	if !ok {
		return nil, fmt.Errorf("node%d read contention rows failed", idx+1)
	}
	return rs, nil
}

// rowCount returns the row count on one node.
func rowCount(cluster *goharness.Cluster, idx int) (int, error) {
	rs, err := rows(cluster, idx)
	if err != nil {
		return 0, err
	}
	return len(rs), nil
}

// insertRow inserts one row, erroring on failure.
func insertRow(cluster *goharness.Cluster, idx int, id, name string) error {
	if _, err := cluster.Node(idx).Execute(
		"INSERT INTO contention(id, name) VALUES($1, $2)", id, name); err != nil {
		return fmt.Errorf("node%d insert failed: %w", idx+1, err)
	}
	if _, err := cluster.Node(idx).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("node%d flush after insert failed: %w", idx+1, err)
	}
	return nil
}

// insertRowRetry inserts one row, retrying replayable 40001 pressure
// (post-rejection probe only; all other writers stay single-shot).
func insertRowRetry(cluster *goharness.Cluster, idx int, id, name string) error {
	if _, err := cluster.Node(idx).ExecuteRetry(
		"INSERT INTO contention(id, name) VALUES($1, $2)",
		[]any{id, name}, 30*time.Second); err != nil {
		return fmt.Errorf("node%d insert failed: %w", idx+1, err)
	}
	if _, err := cluster.Node(idx).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("node%d flush after insert failed: %w", idx+1, err)
	}
	return nil
}

// readName point-reads one row by id: "" + false when missing.
func readName(cluster *goharness.Cluster, idx int, id string) (string, bool) {
	dbRows, err := cluster.Node(idx).Query("SELECT name FROM contention WHERE id = $1", id)
	if err != nil || len(dbRows) == 0 {
		return "", false
	}
	v, err := dbRows[0].String(0)
	if err != nil {
		return "", false
	}
	return v, true
}

// waitCounts polls until every listed node reports want rows.
func waitCounts(cluster *goharness.Cluster, nodes []int, want int, timeout time.Duration) error {
	converged := goharness.WaitUntil(timeout, countPoll, func() bool {
		for _, i := range nodes {
			rs, ok := tryRows(cluster, i)
			if !ok || len(rs) != want {
				return false
			}
		}
		return true
	})
	if !converged {
		return fmt.Errorf("row count did not reach %d on nodes %v within %v", want, nodes, timeout)
	}
	return nil
}

func pairsEqual(a, b []rowPair) bool {
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

// waitDigests polls until every listed node reports the same ordered
// digest; it returns a summary for logging (ordered-vec equality
// replaces the SHA-256 digest with identical strength).
func waitDigests(cluster *goharness.Cluster, nodes []int, timeout time.Duration) (string, error) {
	var lastErr string
	hasErr := false
	last := map[int]string{}
	deadline := time.Now().Add(timeout)
	converged := false
	for {
		var first []rowPair
		hasFirst := false
		ok := true
		for _, i := range nodes {
			got, readable := tryRows(cluster, i)
			if !readable {
				lastErr = fmt.Sprintf("node %d", i+1)
				hasErr = true
				ok = false
				break
			}
			last[i] = fmt.Sprintf("%d rows", len(got))
			if !hasFirst {
				first, hasFirst = got, true
			} else if !pairsEqual(first, got) {
				hasErr = false
				ok = false
				break
			}
		}
		if ok {
			converged = true
			break
		}
		if !time.Now().Before(deadline) {
			break
		}
		time.Sleep(digestPoll)
	}
	if !converged {
		if hasErr {
			return "", fmt.Errorf("digests unreadable on nodes %v within %v: last error %s",
				nodes, timeout, lastErr)
		}
		return "", fmt.Errorf("digest divergence on nodes %v within %v: %v", nodes, timeout, last)
	}
	n := 0
	if rs, ok := tryRows(cluster, nodes[0]); ok {
		n = len(rs)
	}
	return fmt.Sprintf("%d rows identical on nodes %v", n, nodes), nil
}

// readValue polls until id is readable on idx and returns its value.
func readValue(cluster *goharness.Cluster, idx int, id string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		if v, ok := readName(cluster, idx, id); ok {
			return v, nil
		}
		if !time.Now().Before(deadline) {
			return "", fmt.Errorf("value %s never readable on node %d", id, idx+1)
		}
		time.Sleep(readPoll)
	}
}

// countSegments counts encrypted spool segment files under dir
// (recursive), an observable proxy for compaction debt.
func countSegments(dir string) int {
	n := 0
	var walk func(string)
	walk = func(d string) {
		entries, err := os.ReadDir(d)
		if err != nil {
			return
		}
		for _, e := range entries {
			path := d + string(os.PathSeparator) + e.Name()
			if e.IsDir() {
				walk(path)
			} else if strings.HasSuffix(e.Name(), ".spool") {
				n++
			}
		}
	}
	walk(dir)
	return n
}

// updateValue updates one row's value with no durability barrier; the
// churn loop flushes every churnFlushEvery updates instead of per-write.
func updateValue(cluster *goharness.Cluster, idx int, id, name string) error {
	changed, err := cluster.Node(idx).Execute(
		"UPDATE contention SET name = $1 WHERE id = $2", name, id)
	if err != nil {
		return fmt.Errorf("node%d update failed: %w", idx+1, err)
	}
	if changed != 1 {
		return fmt.Errorf("node%d update hit %d rows", idx+1, changed)
	}
	return nil
}

// Churn-loop durability batching (see runCompact).
const churnFlushEvery = 20

// Max batches the victim may run ahead of the control's applied
// counter (see runCompact).
const churnAheadLag = uint64(5)

// Per-batch catch-up wait while pacing (see runCompact).
const churnCatchupTimeout = 120 * time.Second

// Catch-up poll interval.
const churnCatchupPoll = 200 * time.Millisecond

// applied returns the cumulative log-applied transactions on one node.
func applied(cluster *goharness.Cluster, idx int) (uint64, error) {
	rows, err := cluster.Node(idx).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		return 0, fmt.Errorf("node%d SHOW REPLICATION: %w", idx+1, err)
	}
	if len(rows) == 0 {
		return 0, fmt.Errorf("node%d SHOW REPLICATION returned no rows", idx+1)
	}
	if len(rows[0]) <= 5 {
		return 0, fmt.Errorf("node%d SHOW REPLICATION has no applied column", idx+1)
	}
	n, err := strconv.ParseUint(rows[0][5], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("node%d parse applied: %w", idx+1, err)
	}
	return n, nil
}

func runCompact(cluster *goharness.Cluster, updates int) error {
	if err := migrateAll(cluster); err != nil {
		return err
	}

	// Fixed 1-based key set: every update supersedes the previous
	// version, so sealed segments go fully dead and are collectible.
	const keys = 200
	// 64 KiB values: ~141 MiB logical churn lands ~420 MiB of spool
	// with write overhead, sealing ~6 victim segments while the 256
	// MiB control still holds ~2.
	const valBytes = 64 << 10
	var rng xorShift = 1
	for i := 0; i < keys; i++ {
		id := fmt.Sprintf("%032x", i+1)
		val := rng.alnumStrings(1, valBytes)[0]
		if _, err := cluster.Node(1).Execute(
			"INSERT INTO contention(id, name) VALUES($1, $2)", id, val); err != nil {
			return fmt.Errorf("node2 insert failed: %w", err)
		}
		// Same batching as the churn loop (one barrier per 20 writes).
		if (i+1)%churnFlushEvery == 0 || i+1 == keys {
			if _, err := cluster.Node(1).Simple("MURMUR FLUSH"); err != nil {
				return fmt.Errorf("node2 flush after insert batch failed: %w", err)
			}
		}
	}
	if err := waitCounts(cluster, []int{0, 1}, keys, 60*time.Second); err != nil {
		return err
	}

	// Churn through the victim so its sealed-segment debt is its own
	// writes (plus what replicates back); probe responsiveness along
	// the way. Each batch paces to the control's applied counter with
	// a bounded lag so the debug-build control never faces an
	// unbounded backlog (background retention is stalled for this leg,
	// so the backlog is pruned only by the explicit drain collects).
	applied0, err := applied(cluster, 0)
	if err != nil {
		return err
	}
	var batches uint64
	for i := 0; i < updates; i++ {
		id := fmt.Sprintf("%032x", i%keys+1)
		val := rng.alnumStrings(1, valBytes)[0]
		if err := updateValue(cluster, 1, id, val); err != nil {
			return err
		}
		// One durability barrier per 20 updates instead of per-write
		// capture: the same bytes and supersede pattern (each ~1.3
		// MiB capture stays far under the 64 MiB budget), but 2000
		// per-update barriers would cost ~700 s alone against the
		// 600 s run cap. Unflushed updates are still locally
		// visible, so the responsiveness probes below are
		// unaffected.
		if (i+1)%churnFlushEvery == 0 || i+1 == updates {
			if _, err := cluster.Node(1).Simple("MURMUR FLUSH"); err != nil {
				return fmt.Errorf("node2 flush after churn batch failed: %w", err)
			}
			batches++
			// A flush ships its pending session as >= 1 capture,
			// so the control's counter advances >= 1 per batch
			// (background ticks may split a batch into more;
			// `>=` tolerates that).
			var behind uint64
			if batches > churnAheadLag {
				behind = batches - churnAheadLag
			}
			want := applied0 + behind
			deadline := time.Now().Add(churnCatchupTimeout)
			for {
				got, err := applied(cluster, 0)
				if err != nil {
					return err
				}
				if got >= want {
					break
				}
				if !time.Now().Before(deadline) {
					return fmt.Errorf("control did not apply batch %d within %v (want applied >= %d)",
						batches, churnCatchupTimeout, want)
				}
				time.Sleep(churnCatchupPoll)
			}
		}
		if (i+1)%500 == 0 {
			n, err := rowCount(cluster, 1)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "churn %d/%d updates; victim segments=%d control segments=%d rows=%d\n",
				i+1, updates,
				countSegments(cluster.NodeInfo(1).Dir),
				countSegments(cluster.NodeInfo(0).Dir), n)
		}
	}

	// Stall proof: the victim must pile up observably more sealed
	// segments than the control.
	stalled := goharness.WaitUntil(120*time.Second, 2*time.Second, func() bool {
		v := countSegments(cluster.NodeInfo(1).Dir)
		c := countSegments(cluster.NodeInfo(0).Dir)
		return v >= 3 && v > c
	})
	v := countSegments(cluster.NodeInfo(1).Dir)
	c := countSegments(cluster.NodeInfo(0).Dir)
	if !stalled {
		return fmt.Errorf("no compaction debt: victim segments=%d control segments=%d", v, c)
	}
	fmt.Fprintf(os.Stderr, "stall proven: victim segments=%d control segments=%d\n", v, c)

	// Correct under stall: exact convergence with the control node.
	if err := waitCounts(cluster, []int{0, 1}, keys, 180*time.Second); err != nil {
		return err
	}
	d, err := waitDigests(cluster, []int{0, 1}, 180*time.Second)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "converged under stall: %s\n", d)

	// Re-enable reclamation (drop the storage override) and restart
	// on the same data dir, then collect retention until fully-dead
	// segments drain. The pre-restart count is the baseline: sampling
	// after the restart would race the drain itself.
	before := countSegments(cluster.NodeInfo(1).Dir)
	cluster.StopNode(1)
	if err := cluster.SetSpoolOptions(1, nil); err != nil {
		return fmt.Errorf("drop victim spool override: %w", err)
	}
	if err := cluster.StartNode(1); err != nil {
		return fmt.Errorf("restart victim: %w", err)
	}
	if err := cluster.Mesh(); err != nil {
		return fmt.Errorf("re-mesh after restart: %w", err)
	}
	deadline := time.Now().Add(240 * time.Second)
	// Retention collection drives the internal reclaim that deletes
	// fully-dead files. Poll the count every 2 s but COLLECT at most
	// every 10 s: a collection scans hundreds of MiB in debug builds,
	// so a coarse sleep would sit through the drop it just caused.
	var lastCollect time.Time
	hasCollect := false
	drained := false
	for {
		if countSegments(cluster.NodeInfo(1).Dir) < before {
			drained = true
			break
		}
		if !time.Now().Before(deadline) {
			break
		}
		if !hasCollect || time.Since(lastCollect) >= 10*time.Second {
			_, _ = cluster.Node(1).Simple("MURMUR RETENTION COLLECT")
			lastCollect = time.Now()
			hasCollect = true
		}
		time.Sleep(2 * time.Second)
	}
	after := countSegments(cluster.NodeInfo(1).Dir)
	if !drained {
		return fmt.Errorf("debt did not drain after re-enable: segments stuck at %d (was %d)",
			after, before)
	}
	fmt.Fprintf(os.Stderr, "debt draining: victim segments %d -> %d\n", before, after)

	if err := waitCounts(cluster, []int{0, 1}, keys, 180*time.Second); err != nil {
		return err
	}
	d2, err := waitDigests(cluster, []int{0, 1}, 180*time.Second)
	if err != nil {
		return err
	}
	if d2 != d {
		return fmt.Errorf("digest moved across compaction drain: %s -> %s", d, d2)
	}
	fmt.Fprintf(os.Stderr, "PASS: stall survived, debt drained, digest stable %s\n", d2)
	return nil
}

// envPositive reads a positive-integer env knob with fallback.
func envPositive(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

func TestStalledCompactionStaysCorrect(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	// 1000 updates: the update count is a means to pile debt, not a
	// pass criterion. ~77 MiB of churn lands ~250 MiB of spool,
	// sealing ~4 victim segments while the control holds ~1; 2000
	// would cost past the 600 s run cap at debug-build flush
	// throughput with no additional proof.
	updates := envPositive("MURMUR_RX_COMPACT_ROWS", 1000)

	retention := time.Second
	minRetained := uint64(10)
	maxOffline := 20 * time.Second
	// Stall background reclamation for the whole run: without a stall,
	// working reclamation deletes fully-dead files as fast as churn
	// seals them, so no debt ever piles and the stall proof below is
	// vacuous. All retention checkpoints, prunes, and physical
	// reclaims are collect-gated, so an hourly cadence piles debt
	// deterministically on both nodes. The drain loop drives pruning
	// and reclamation explicitly with MURMUR RETENTION COLLECT, which
	// ignores this cadence.
	retentionInterval := 3600 * time.Second
	segmentBytes := 66 << 20
	opts := goharness.DefaultClusterOptions("resource-compact")
	opts.NumNodes = 2
	opts.Replication = &goharness.ReplicationOptions{
		Retention:         &retention,
		MinimumRetained:   &minRetained,
		MaximumOffline:    &maxOffline,
		RetentionInterval: &retentionInterval,
	}
	// 66 MiB victim segments pile ~4 sealed files over the ~77 MiB
	// churn while the 256 MiB control holds ~1 (spool overhead lands
	// ~3x logical bytes, so 66 MiB still separates cleanly; see
	// README).
	opts.SpoolByNode = map[int]goharness.SpoolOptions{
		1: {SegmentBytes: &segmentBytes},
	}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	if cluster.Len() != 2 {
		t.Fatalf("cluster has %d nodes, this scenario is written for 2", cluster.Len())
	}
	runLeg(t, cluster, "resource-compact-no-convergence", func() error {
		return runCompact(cluster, updates)
	})
}

// runLeg runs one leg body, dumping forensics and failing on error.
func runLeg(t *testing.T, cluster *goharness.Cluster, forensics string, body func() error) {
	t.Helper()
	result := body()
	failed := result != nil
	if result != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", result)
	}
	if failed {
		cluster.DumpForensics(forensics)
	}
	cluster.Finish(failed)
	if failed {
		t.Fatal("scenario failed; artifacts retained under tests-live/failures/")
	}
}

// bucket is a token-bucket pacer: sustained rate bytes per second with
// a burst allowance. The burst absorbs QUIC's windowed bursts without
// loss; only sustained overload is paced.
type bucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newBucket(rate, burst int) *bucket {
	return &bucket{rate: float64(rate), burst: float64(burst), tokens: float64(burst), last: time.Now()}
}

// pace blocks until n bytes fit the bucket.
func (b *bucket) pace(n int) {
	for {
		var sleep time.Duration
		needed := false
		func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			now := time.Now()
			b.tokens += now.Sub(b.last).Seconds() * b.rate
			if b.tokens > b.burst {
				b.tokens = b.burst
			}
			b.last = now
			if b.tokens >= float64(n) {
				b.tokens -= float64(n)
			} else {
				deficit := float64(n) - b.tokens
				b.tokens = 0
				sleep = time.Duration(deficit / b.rate * float64(time.Second))
				needed = true
			}
		}()
		if !needed {
			return
		}
		time.Sleep(sleep)
	}
}

type queuedDatagram struct {
	dst  *net.UDPConn
	peer *net.UDPAddr
	data []byte
}

// proxyStats are forwarding counters so a stall can be attributed to
// the proxy (flows frozen) or the endpoints (bytes flow, rows stall).
type proxyStats struct {
	fwdPackets uint64
	fwdBytes   uint64
	fwdDrops   uint64
	bwdPackets uint64
	bwdBytes   uint64
	bwdDrops   uint64
}

// Queue depth per lane (burst absorption comes from the token bucket,
// not the queue).
const proxyQueueDepth = 32

// Token-bucket burst allowance per direction (256 KiB).
const proxyBurstBytes = 256 << 10

// Small-packet priority cutoff (QUIC ACKs/keepalives take the priority
// lane so bulk data cannot starve the ACK clock).
const proxyPriorityCutoff = 300

// throttleProxy is a transparent UDP forwarder with a token-bucket
// rate limit in BOTH directions: it emulates a slow link the way
// tc-tbf does, including drop-tail on queue overflow. QUIC identifies
// connections by connection ID, so NAT-style forwarding (one
// server-side socket per client address) is transparent to both ends.
//
// Two properties are load-bearing for QUIC stability: 1. Read loops
// never block on pacing (reads enqueue; sender goroutines pace the
// drain). 2. Queues are bounded with drop-tail (drops are the
// backpressure signal congestion control is designed for).
type throttleProxy struct {
	listener  *net.UDPConn
	target    *net.UDPAddr
	mu        sync.Mutex
	servers   map[string]*net.UDPConn
	closed    atomic.Bool
	done      chan struct{}
	closeOnce sync.Once
	fwdHi     chan queuedDatagram
	fwdLo     chan queuedDatagram
	bwdHi     chan queuedDatagram
	bwdLo     chan queuedDatagram
	fwdBucket *bucket
	bwdBucket *bucket
	statsMu   sync.Mutex
	stats     proxyStats
}

func startThrottleProxy(target netip.AddrPort, rate int) *throttleProxy {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		panic(fmt.Sprintf("proxy listen: %v", err))
	}
	// NOTE: the kernel socket read buffer stays at its default (any
	// kernel drops under burst read as ordinary loss, which the 90 s
	// wedge trip still guards).
	p := &throttleProxy{
		listener:  listener,
		target:    net.UDPAddrFromAddrPort(target),
		servers:   map[string]*net.UDPConn{},
		done:      make(chan struct{}),
		fwdHi:     make(chan queuedDatagram, proxyQueueDepth),
		fwdLo:     make(chan queuedDatagram, proxyQueueDepth),
		bwdHi:     make(chan queuedDatagram, proxyQueueDepth),
		bwdLo:     make(chan queuedDatagram, proxyQueueDepth),
		fwdBucket: newBucket(rate, proxyBurstBytes),
		bwdBucket: newBucket(rate, proxyBurstBytes),
	}
	// Serve/sender/backward loops all observe done/closed with <= 100
	// ms timeouts, so every goroutine exits promptly after Close; none
	// need joining.
	go p.serve()
	go p.sender(p.fwdHi, p.fwdLo, true)
	go p.sender(p.bwdHi, p.bwdLo, false)
	return p
}

// addr returns the proxy listen address (what slow peers dial through).
func (p *throttleProxy) addr() netip.AddrPort {
	return p.listener.LocalAddr().(*net.UDPAddr).AddrPort()
}

func (p *throttleProxy) getStats() proxyStats {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	return p.stats
}

func (p *throttleProxy) close() {
	if p.closed.Swap(true) {
		return
	}
	p.closeOnce.Do(func() { close(p.done) })
}

// enqueue queues a datagram with drop-tail on overflow; small packets
// take the priority lane (priority still consumes wire tokens — it is
// ordering, not free bandwidth).
func (p *throttleProxy) enqueue(d queuedDatagram, fwd bool) {
	var q chan queuedDatagram
	if fwd {
		if len(d.data) <= proxyPriorityCutoff {
			q = p.fwdHi
		} else {
			q = p.fwdLo
		}
	} else if len(d.data) <= proxyPriorityCutoff {
		q = p.bwdHi
	} else {
		q = p.bwdLo
	}
	select {
	case q <- d:
	default:
		p.statsMu.Lock()
		if fwd {
			p.stats.fwdDrops++
		} else {
			p.stats.bwdDrops++
		}
		p.statsMu.Unlock()
	}
}

func (p *throttleProxy) serve() {
	buf := make([]byte, 65535)
	for {
		if p.closed.Load() {
			return
		}
		_ = p.listener.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, client, err := p.listener.ReadFromUDP(buf)
		if err != nil {
			continue // timeout (or closed): re-check flag
		}
		datagram := append([]byte(nil), buf[:n]...)
		p.mu.Lock()
		if p.closed.Load() {
			p.mu.Unlock()
			return
		}
		server, ok := p.servers[client.String()]
		if !ok {
			sock, err := net.DialUDP("udp", nil, p.target)
			if err != nil {
				fmt.Fprintf(os.Stderr, "proxy: dial target: %v\n", err)
				p.mu.Unlock()
				continue
			}
			server = sock
			p.servers[client.String()] = sock
			go p.backward(sock, client)
		}
		p.mu.Unlock()
		p.enqueue(queuedDatagram{dst: server, data: datagram}, true)
	}
}

// backward relays target->client datagrams for one server-side socket
// into the paced backward queue (never times out logically — an idle
// relay survives so a quiet connection's return path works).
func (p *throttleProxy) backward(server *net.UDPConn, client *net.UDPAddr) {
	buf := make([]byte, 65535)
	for {
		if p.closed.Load() {
			return
		}
		_ = server.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, err := server.Read(buf)
		if err != nil {
			continue // timeout: re-check flag
		}
		p.enqueue(queuedDatagram{peer: client, data: append([]byte(nil), buf[:n]...)}, false)
	}
}

// sender drains one direction's lanes through its token bucket,
// serving the priority lane first.
func (p *throttleProxy) sender(hi, lo chan queuedDatagram, fwd bool) {
	bucket := p.fwdBucket
	if !fwd {
		bucket = p.bwdBucket
	}
	for {
		var d queuedDatagram
		select {
		case d = <-hi:
		default:
			select {
			case d = <-hi:
			case d = <-lo:
			case <-p.done:
				return
			}
		}
		bucket.pace(len(d.data))
		var err error
		if d.peer != nil {
			_, err = p.listener.WriteToUDP(d.data, d.peer)
		} else if d.dst != nil {
			_, err = d.dst.Write(d.data)
		}
		if err != nil {
			return
		}
		p.statsMu.Lock()
		if fwd {
			p.stats.fwdPackets++
			p.stats.fwdBytes += uint64(len(d.data))
		} else {
			p.stats.bwdPackets++
			p.stats.bwdBytes += uint64(len(d.data))
		}
		p.statsMu.Unlock()
	}
}

func runSlow(cluster *goharness.Cluster, rate, rowsN, valBytes int, bound time.Duration) error {
	if err := migrateAll(cluster); err != nil {
		return err
	}

	// Both directions cross a throttle: replication data follows the
	// connection the receiver initiated, so throttling only one way
	// would leave the data path unthrottled.
	real0 := cluster.NodeInfo(0).ReplAddr
	real1 := cluster.NodeInfo(1).ReplAddr
	toSlow := startThrottleProxy(real1, rate)
	toFast := startThrottleProxy(real0, rate)
	fmt.Fprintf(os.Stderr, "throttle: node0 -> %s -> node1, node1 -> %s -> node0 at %d B/s\n",
		toSlow.addr(), toFast.addr(), rate)
	if err := cluster.AddPeerAddr(0, toSlow.addr()); err != nil {
		return fmt.Errorf("add peer 0->1 via proxy: %w", err)
	}
	if err := cluster.AddPeerAddr(1, toFast.addr()); err != nil {
		return fmt.Errorf("add peer 1->0 via proxy: %w", err)
	}

	// Baseline through the throttle proves the proxied path works
	// before the timed leg starts.
	if err := insertRow(cluster, 0, fmt.Sprintf("%032x", 1), "baseline"); err != nil {
		return err
	}
	if err := waitCounts(cluster, []int{1}, 1, 120*time.Second); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "baseline crossed the throttle")

	// Churn on the fast node; the slow node must make steady progress
	// (no wedge) until fully converged within the bound. Values are
	// unique incompressible bytes per row: repeats would compress away
	// and the throttle would never bind.
	var rng xorShift = 0x5eed
	vals := rng.alnumStrings(rowsN, valBytes)
	for i, val := range vals {
		if err := insertRow(cluster, 0, fmt.Sprintf("%032x", i+2), val); err != nil {
			return err
		}
	}
	total := rowsN + 1
	convergeStart := time.Now()
	deadline := time.Now().Add(bound)
	last := 0
	lastIncrease := time.Now()
	stalled := false
	for {
		n, err := rowCount(cluster, 1)
		if err != nil {
			return err
		}
		if n > last {
			last = n
			lastIncrease = time.Now()
		}
		if n == total {
			break
		}
		if time.Since(lastIncrease) > 90*time.Second {
			stalled = true
			break
		}
		if time.Now().After(deadline) {
			break
		}
		s1 := toSlow.getStats()
		s2 := toFast.getStats()
		fmt.Fprintf(os.Stderr, "slow node %d/%d rows; toSlow fwd %dB(%dd) bwd %dB(%dd) | toFast fwd %dB(%dd) bwd %dB(%dd)\n",
			n, total,
			s1.fwdBytes, s1.fwdDrops, s1.bwdBytes, s1.bwdDrops,
			s2.fwdBytes, s2.fwdDrops, s2.bwdBytes, s2.bwdDrops)
		time.Sleep(5 * time.Second)
	}
	if stalled {
		return fmt.Errorf("WEDGE: slow node stuck at %d/%d rows for 90s", last, total)
	}
	if last != total {
		return fmt.Errorf("slow node reached %d/%d rows within %v", last, total, bound)
	}
	took := time.Since(convergeStart)
	// Byte-level proof the data crossed the throttles: ~1 MB of
	// incompressible values must have traversed the slow paths (data
	// may flow through either proxy in either direction). This is the
	// non-vacuous guard: a fast pass with few throttled bytes means
	// traffic bypassed the proxies and the test is broken.
	s1 := toSlow.getStats()
	s2 := toFast.getStats()
	throttled := s1.fwdBytes + s1.bwdBytes + s2.fwdBytes + s2.bwdBytes
	if throttled < uint64(rowsN*valBytes/2) {
		return fmt.Errorf("only %d throttled bytes for %d x %dB rows: data bypassed the proxies?",
			throttled, rowsN, valBytes)
	}
	d, err := waitDigests(cluster, []int{0, 1}, 60*time.Second)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "converged through %d B/s throttles in %v: %d rows, %d throttled bytes (%d drops), %s\n",
		rate, took, total, throttled, s1.fwdDrops+s1.bwdDrops+s2.fwdDrops+s2.bwdDrops, d)

	// Heal: remove the throttles, re-peer direct, and prove fast
	// convergence is restored.
	toSlow.close()
	toFast.close()
	if err := cluster.AddPeer(0, 1); err != nil {
		return fmt.Errorf("re-peer 0->1 direct: %w", err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		return fmt.Errorf("re-peer 1->0 direct: %w", err)
	}
	if err := cluster.SyncAll(); err != nil {
		return fmt.Errorf("sync after heal: %w", err)
	}
	for i := 0; i < 10; i++ {
		if err := insertRow(cluster, 0, fmt.Sprintf("%032x", total+i+1), "healed"); err != nil {
			return err
		}
	}
	if err := waitCounts(cluster, []int{0, 1}, total+10, 60*time.Second); err != nil {
		return err
	}
	d2, err := waitDigests(cluster, []int{0, 1}, 60*time.Second)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "PASS: healed to direct peering, %d rows, %s\n", total+10, d2)
	return nil
}

func TestSlowPeerConvergesWithoutWedge(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	rate := envPositive("MURMUR_RX_SLOW_RATE", 262144)
	rowsN := envPositive("MURMUR_RX_SLOW_ROWS", 600)
	valBytes := envPositive("MURMUR_RX_SLOW_VALBYTES", 1024)
	bound := time.Duration(envPositive("MURMUR_RX_SLOW_BOUND_SECONDS", 300)) * time.Second

	opts := goharness.DefaultClusterOptions("resource-slow")
	opts.NumNodes = 2
	opts.ManualPeers = true
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	if cluster.Len() != 2 {
		t.Fatalf("cluster has %d nodes, this scenario is written for 2", cluster.Len())
	}
	runLeg(t, cluster, "resource-slow-no-convergence", func() error {
		return runSlow(cluster, rate, rowsN, valBytes, bound)
	})
}

func runHuge(cluster *goharness.Cluster) error {
	if err := migrateAll(cluster); err != nil {
		return err
	}

	// (a) 8 MiB value: accepted, byte-exact on both nodes.
	huge := strings.Repeat("H", 8<<20)
	hugeID := fmt.Sprintf("%032x", 1)
	if err := insertRow(cluster, 0, hugeID, huge); err != nil {
		return err
	}
	for _, i := range []int{0, 1} {
		got, err := readValue(cluster, i, hugeID, 120*time.Second)
		if err != nil {
			return err
		}
		if len(got) != len(huge) || got != huge {
			return fmt.Errorf("node%d value mismatch: len=%d", i+1, len(got))
		}
	}
	fmt.Fprintln(os.Stderr, "8 MiB value byte-exact on both nodes")

	// (b) Oversize statement: clean rejection naming the budget, no
	// partial row, writer healthy. One statement carries a 64 MiB + 1
	// B value past the default 64 MiB transaction budget — the same
	// fail-closed proof at default budgets. A multi-row BEGIN/COMMIT
	// cannot express this: wire transaction words are accepted
	// no-ops, so each statement would autocommit and only trip
	// retryable 40001 pressure mid-stream.
	const txBudget = 64 << 20
	overID := fmt.Sprintf("%032x", 1000)
	overVal := strings.Repeat("O", txBudget+1)
	// Display is a bare `db error`; the budget text lives in the
	// source chain (`54000: ... budget exceeded`).
	if _, err := cluster.Node(0).Execute(
		"INSERT INTO contention(id, name) VALUES($1, $2)", overID, overVal); err == nil {
		return fmt.Errorf("oversize statement accepted, want budget rejection")
	} else {
		msg := goharness.DBMessage(err)
		if !strings.Contains(msg, "budget exceeded") {
			return fmt.Errorf("rejection error %q does not name the budget", msg)
		}
	}
	for _, i := range []int{0, 1} {
		n, err := rowCount(cluster, i)
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("node%d has %d rows after rejection, want 1 (partial apply?)", i+1, n)
		}
	}
	// The rejection leaves momentary queue pressure (40001,
	// documented back-off-and-retry), so the probe retries it.
	if err := insertRowRetry(cluster, 0, fmt.Sprintf("%032x", 3), "probe"); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "oversize statement rejected as budget exceeded, no partial row, writer healthy")

	// (c) Sustained large-value load: 20 x 512 KiB converge exactly.
	for i := 0; i < 20; i++ {
		v := strings.Repeat(strconv.Itoa(i%10), 512<<10)
		node := i % 2
		if err := insertRow(cluster, node, fmt.Sprintf("%032x", 100+i), v); err != nil {
			return err
		}
	}
	if err := waitCounts(cluster, []int{0, 1}, 22, 180*time.Second); err != nil {
		return err
	}
	d, err := waitDigests(cluster, []int{0, 1}, 180*time.Second)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "PASS: 22 rows (8MiB + probe + 20x512KiB) converged, %s\n", d)
	return nil
}

func TestHugeTransactionsAtDefaultBudgets(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	// Production default budgets (64 MiB transaction_bytes, no
	// per-value limit, so the rejection leg trips the transaction
	// budget instead — see above).
	opts := goharness.DefaultClusterOptions("resource-huge")
	opts.NumNodes = 2
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	if cluster.Len() != 2 {
		t.Fatalf("cluster has %d nodes, this scenario is written for 2", cluster.Len())
	}
	runLeg(t, cluster, "resource-huge-no-convergence", func() error {
		return runHuge(cluster)
	})
}

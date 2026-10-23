// Live scenario: fail closed under pressure, stay exactly converged after.
//
// Go port of the Rust `resource-exhaustion` scenario
// (/home/marc/MURMUR/tests-live/resource-exhaustion/tests/resource_exhaustion.rs,
// 3 of 5 legs). Ported legs: stalled compaction stays correct, slow peer
// converges without wedging (userspace UDP throttle proxies), and huge
// transactions at default budgets. The near-OOM and fd-exhaustion legs are
// NOT portable in-process (per-process GOMEMLIMIT/cgroup/prlimit mechanisms
// plus a victims-only squeeze and, for fd, held TLS connections to an HTTPS
// API that does not exist); see the scenario README.md.
//
// Every leg demands the same contract: fail closed under pressure, stay
// exactly converged after.
package resourceexhaustion

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

const (
	// tableDDL is the contention table (Rust: shared fixture).
	tableDDL = "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)"
	// countPoll is the count poll interval (Rust: 500ms).
	countPoll = 500 * time.Millisecond
	// digestPoll is the digest poll interval (Rust: 1s).
	digestPoll = time.Second
	// readPoll is the point-read poll interval (Rust: 200ms).
	readPoll = 200 * time.Millisecond
)

// envInt reads a positive-integer env knob with fallback (Rust: inline
// parsing).
func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

// xorShift is a deterministic xorshift64* (Rust: XorShift; the original
// legs use seeded math/rand for incompressible values).
type xorShift uint64

func (x *xorShift) next() uint64 {
	v := uint64(*x)
	v ^= v >> 12
	v ^= v << 25
	v ^= v >> 27
	*x = xorShift(v)
	return v * 0x2545f4914f6cdd1d
}

// alnumStrings renders n distinct incompressible alphanumeric strings of
// size bytes (Rust: alnum_strings; repeats would compress away and the
// pressure would never bind).
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

// migrateAll creates the same schema on every node (Rust: migrate_all).
func migrateAll(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).Migrate(murmur.NewMigration(tableDDL)); err != nil {
			t.Fatalf("node%d migrate contention table: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush schema: %v", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("contention", true); err != nil {
			t.Fatalf("node%d opt-in contention: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush opt-in: %v", i+1, err)
		}
	}
}

// contentionRow is one ordered contention row.
type contentionRow struct {
	id   string
	name string
}

// tryRows reads the ordered contention rows on one node, reporting false
// when unreadable (Rust: try_rows). Wait loops retry on false,
// distinguishing unreadable nodes from true content divergence.
func tryRows(db *murmur.Database) ([]contentionRow, bool) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name FROM contention ORDER BY id")
	})
	if err != nil {
		return nil, false
	}
	var out []contentionRow
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, contentionRow{id: row[0].Str, name: row[1].Str})
	}
	return out, true
}

func equalContentionRows(a, b []contentionRow) bool {
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

// mustRows reads the ordered contention rows on one node, failing when
// unreadable (Rust: rows).
func mustRows(t *testing.T, db *murmur.Database) []contentionRow {
	t.Helper()
	rows, ok := tryRows(db)
	if !ok {
		t.Fatal("read contention rows failed")
	}
	return rows
}

// rowCount counts rows on one node (Rust: row_count).
func rowCount(t *testing.T, db *murmur.Database) int {
	t.Helper()
	return len(mustRows(t, db))
}

// insertRow inserts one row, failing on error.
func insertRow(t *testing.T, db *murmur.Database, node int, id, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO contention(id, name) VALUES(?, ?)", id, name)
		return nil, err
	}); err != nil {
		t.Fatalf("node%d insert failed: %v", node, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after insert failed: %v", node, err)
	}
}

// readName point-reads one row by id: ("", false) when missing (Rust:
// read_name).
func readName(db *murmur.Database, id string) (string, bool) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM contention WHERE id = ?", id)
	})
	if err != nil {
		return "", false
	}
	found := v.([][]sqlite.Value)
	if len(found) == 0 {
		return "", false
	}
	return found[0][0].Str, true
}

// waitCounts polls until every listed node reports want rows (Rust:
// wait_counts).
func waitCounts(t *testing.T, cluster *harness.Cluster, nodes []int, want int, timeout time.Duration) {
	t.Helper()
	converged := harness.WaitUntil(timeout, countPoll, func() bool {
		for _, i := range nodes {
			rows, ok := tryRows(cluster.Node(i))
			if !ok || len(rows) != want {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("row count did not reach %d on nodes %v within %v", want, nodes, timeout)
	}
}

// waitDigests polls until every listed node reports the same ordered
// digest; returns a summary for logging (Rust: wait_digests;
// ordered-vec equality replaces the SHA-256 digest with identical
// strength, and in-process reads have no client timeout, so huge tables
// need no segmenting).
func waitDigests(t *testing.T, cluster *harness.Cluster, nodes []int, timeout time.Duration) string {
	t.Helper()
	var lastErr *string
	last := map[int]string{}
	converged := harness.WaitUntil(timeout, digestPoll, func() bool {
		var first []contentionRow
		for k, i := range nodes {
			got, ok := tryRows(cluster.Node(i))
			if !ok {
				s := fmt.Sprintf("node %d", i+1)
				lastErr = &s
				return false
			}
			last[i] = fmt.Sprintf("%d rows", len(got))
			if k == 0 {
				first = got
			} else if !equalContentionRows(first, got) {
				lastErr = nil
				return false
			}
		}
		return true
	})
	if !converged {
		if lastErr != nil {
			t.Fatalf("digests unreadable on nodes %v within %v: last error %s", nodes, timeout, *lastErr)
		}
		t.Fatalf("digest divergence on nodes %v within %v: %v", nodes, timeout, last)
	}
	n := 0
	if rows, ok := tryRows(cluster.Node(nodes[0])); ok {
		n = len(rows)
	}
	return fmt.Sprintf("%d rows identical on nodes %v", n, nodes)
}

// readValue polls until id is readable on idx and returns its value (Rust:
// read_value).
func readValue(t *testing.T, cluster *harness.Cluster, idx int, id string, timeout time.Duration) string {
	t.Helper()
	got := ""
	found := harness.WaitUntil(timeout, readPoll, func() bool {
		var ok bool
		got, ok = readName(cluster.Node(idx), id)
		return ok
	})
	if !found {
		t.Fatalf("value %s never readable on node %d", id, idx+1)
	}
	return got
}

// countSegments counts encrypted spool segment files under dir
// (recursive), an observable proxy for compaction debt (Rust:
// count_segments).
func countSegments(dir string) int {
	n := 0
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(d.Name(), ".spool") {
			n++
		}
		return nil
	})
	return n
}

// updateName updates one row's value (Rust: update_name).
func updateName(t *testing.T, db *murmur.Database, node int, id, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		n, err := s.Execute("UPDATE contention SET name = ? WHERE id = ?", name, id)
		if err != nil {
			return nil, err
		}
		if n != 1 {
			return nil, fmt.Errorf("node%d update hit %d rows", node, n)
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("node%d update failed: %v", node, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after update failed: %v", node, err)
	}
}

// repeatID renders a 16-byte node id filled with b.
func repeatID(b byte) [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = b
	}
	return id
}

// TestStalledCompactionStaysCorrect piles sealed-segment debt on a
// small-segment victim under update churn, requires exact convergence
// under the stall, then proves drain hygiene and digest stability (Rust:
// stalled_compaction_stays_correct).
func TestStalledCompactionStaysCorrect(t *testing.T) {
	updates := envInt("MURMUR_RX_COMPACT_ROWS", 2000)

	// Deterministic ids so snapshot trust can be wired up front. The
	// control must be able to rescue past queue-drop gaps via snapshot:
	// with 64KiB values the 4MiB peer queue drops under backlog, 1s
	// retention prunes the dropped ranges, and without a trusted snapshot
	// source the gap wedges permanently. minimum_retained 100 keeps
	// micro-gaps servable from the log; the segment-debt premise (65MiB
	// victim segments) is unaffected.
	nodeIDs := [][16]byte{repeatID(101), repeatID(102)}
	all := map[[16]byte]bool{nodeIDs[0]: true, nodeIDs[1]: true}
	retention := time.Second
	minimumRetained := uint64(100)
	maximumOffline := 20 * time.Second
	victimSegment := 65 << 20
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "resource-compact",
		NumNodes: 2,
		NodeIDs:  nodeIDs,
		Replication: &harness.ReplicationOptions{
			Retention:       &retention,
			MinimumRetained: &minimumRetained,
			MaximumOffline:  &maximumOffline,
		},
		TrustedSnapshotSources: map[int]map[[16]byte]bool{0: all, 1: all},
		// Small victim segments pile several sealed files over the ~128
		// MiB value churn (plus replication log overhead) while the 256
		// MiB control holds one. The product requires segments > 64 MiB,
		// so 65 MiB is the smallest compliant victim.
		SpoolByNode: map[int]harness.SpoolOptions{
			1: {SegmentBytes: &victimSegment},
		},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("resource-compact-no-convergence")
		}
		cluster.Finish(failed)
	}()

	migrateAll(t, cluster)

	// Fixed 1-based key set: every update supersedes the previous version,
	// so sealed segments go fully dead and are collectible.
	const keys = 200
	rng := xorShift(1)
	for i := 0; i < keys; i++ {
		id := fmt.Sprintf("%032x", i+1)
		val := rng.alnumStrings(1, 64<<10)[0]
		insertRow(t, cluster.Node(1), 2, id, val)
	}
	waitCounts(t, cluster, []int{0, 1}, keys, 60*time.Second)

	// Churn through the victim so its sealed-segment debt is its own
	// writes (plus what replicates back); probe responsiveness along the
	// way.
	for i := 0; i < updates; i++ {
		id := fmt.Sprintf("%032x", i%keys+1)
		val := rng.alnumStrings(1, 64<<10)[0]
		updateName(t, cluster.Node(1), 2, id, val)
		if (i+1)%500 == 0 {
			n := rowCount(t, cluster.Node(1))
			t.Logf("churn %d/%d updates; victim segments=%d control segments=%d rows=%d",
				i+1, updates,
				countSegments(cluster.NodeInfo(1).Dir),
				countSegments(cluster.NodeInfo(0).Dir), n)
		}
	}

	// Stall proof: the victim must pile up observably more sealed segments
	// than the control (rotation still runs; nothing is collected away).
	stalled := harness.WaitUntil(120*time.Second, 2*time.Second, func() bool {
		v := countSegments(cluster.NodeInfo(1).Dir)
		c := countSegments(cluster.NodeInfo(0).Dir)
		return v >= 3 && v > c
	})
	v, c := countSegments(cluster.NodeInfo(1).Dir), countSegments(cluster.NodeInfo(0).Dir)
	if !stalled {
		t.Fatalf("no compaction debt: victim segments=%d control segments=%d", v, c)
	}
	t.Logf("stall proven: victim segments=%d control segments=%d", v, c)

	// Correct under stall: exact convergence with the control node.
	waitCounts(t, cluster, []int{0, 1}, keys, 300*time.Second)
	d := waitDigests(t, cluster, []int{0, 1}, 300*time.Second)
	t.Logf("converged under stall: %s", d)

	// Re-enable reclamation (drop the storage override) and restart on the
	// same data dir, then prove steady-state hygiene under pumped
	// collection. The engine reclaims autonomously (30s loop plus every
	// collection), so fully-dead files already drained during the stall
	// and only the live tail remains -- there is no count drop left to
	// observe. What the drain proves instead: debt does not grow (no
	// leak), reclaim keeps running (liveness), and the digest stays
	// bit-stable below (GC never eats live data).
	before := countSegments(cluster.NodeInfo(1).Dir)
	// SetSpoolOptions requires the node stopped (takes effect on the next
	// start): stop first, then drop the override.
	cluster.StopNode(1)
	if err := cluster.SetSpoolOptions(1, nil); err != nil {
		t.Fatalf("drop victim spool override: %v", err)
	}
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart victim: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	// Spool metrics are process-local: sample after the restart.
	// NOTE (Go product gap): Database.SpoolMetrics does not map the
	// Reclaims pass counter (spoolMetricsFrom drops it), so the Rust
	// "reclaim counter advances" liveness assert is unobservable here.
	// Liveness is proven through its Go-observable effects instead: a
	// Reclaim pass either rewrites a dead-heavy sealed file
	// (Compactions++, mapped) or unlinks a fully-dead one (Segments--,
	// mapped). The victim's oldest sealed files hold only superseded
	// churn plus 1s-retention-pruned log records, so at least one of the
	// two must fire while passes run (rate-limited to 1/60s, so the 120s
	// drain admits several).
	compactionsBefore := cluster.Node(1).SpoolMetrics().Compactions
	segmentsBefore := cluster.Node(1).SpoolMetrics().Segments
	soakUntil := time.Now().Add(120 * time.Second)
	for time.Now().Before(soakUntil) {
		// Retention collection drives the internal reclaim; errors fail
		// loudly instead of being swallowed per poll.
		if _, err := cluster.Node(1).CollectRetention(); err != nil {
			t.Fatalf("drain-phase collection: %v", err)
		}
		time.Sleep(10 * time.Second)
	}
	after := countSegments(cluster.NodeInfo(1).Dir)
	compactionsAfter := cluster.Node(1).SpoolMetrics().Compactions
	segmentsAfter := cluster.Node(1).SpoolMetrics().Segments
	if after > before {
		t.Fatalf("segment leak during drain: %d -> %d segments", before, after)
	}
	if compactionsAfter <= compactionsBefore && segmentsAfter >= segmentsBefore {
		t.Fatalf("reclaim never ran during drain (compactions stuck at %d, segments %d -> %d)",
			compactionsBefore, segmentsBefore, segmentsAfter)
	}
	t.Logf("drain steady: victim segments %d -> %d, compactions %d -> %d, spool segments %d -> %d",
		before, after, compactionsBefore, compactionsAfter, segmentsBefore, segmentsAfter)

	waitCounts(t, cluster, []int{0, 1}, keys, 300*time.Second)
	d2 := waitDigests(t, cluster, []int{0, 1}, 300*time.Second)
	if d2 != d {
		t.Fatalf("digest moved across compaction drain: %s -> %s", d, d2)
	}
	t.Logf("PASS: stall survived, debt drained, digest stable %s", d2)

	failed = false
}

// bucket is a token-bucket pacer: sustained rate bytes per second with a
// burst allowance. The burst absorbs QUIC's windowed bursts without loss;
// only sustained overload is paced (Rust: Bucket).
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

// pace blocks until n bytes fit the bucket (Rust: pace).
func (b *bucket) pace(n int) {
	for {
		var sleep time.Duration
		ready := false
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
				ready = true
			} else {
				deficit := float64(n) - b.tokens
				b.tokens = 0
				sleep = time.Duration(deficit / b.rate * float64(time.Second))
			}
		}()
		if ready {
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

// proxyStats holds forwarding counters so a stall can be attributed to the
// proxy (flows frozen) or the endpoints (bytes flow, rows stall) (Rust:
// ProxyStats).
type proxyStats struct {
	fwdPackets uint64
	fwdBytes   uint64
	fwdDrops   uint64
	bwdPackets uint64
	bwdBytes   uint64
	bwdDrops   uint64
}

const (
	// proxyQueueDepth is the queue depth per lane (Rust: 32; burst
	// absorption comes from the token bucket, not the queue).
	proxyQueueDepth = 32
	// proxyBurstBytes is the token-bucket burst allowance per direction
	// (Rust: 256 KiB).
	proxyBurstBytes = 256 << 10
	// proxyPriorityCutoff is the small-packet priority cutoff (Rust: QUIC
	// ACKs/keepalives take the priority lane so bulk data cannot starve
	// the ACK clock).
	proxyPriorityCutoff = 300
)

// throttleProxy is a transparent UDP forwarder with a token-bucket rate
// limit in BOTH directions: it emulates a slow link the way tc-tbf does,
// including drop-tail on queue overflow (Rust: ThrottleProxy). QUIC
// identifies connections by connection ID, so NAT-style forwarding (one
// server-side socket per client address) is transparent to both ends.
//
// Two properties are load-bearing for QUIC stability (same as Rust):
// 1. Read loops never block on pacing (reads enqueue; sender goroutines
// pace the drain). 2. Queues are bounded with drop-tail (drops are the
// backpressure signal congestion control is designed for).
type throttleProxy struct {
	listener *net.UDPConn
	target   *net.UDPAddr

	mu      sync.Mutex
	servers map[string]*net.UDPConn

	done     chan struct{}
	doneOnce sync.Once

	fwdHi chan queuedDatagram
	fwdLo chan queuedDatagram
	bwdHi chan queuedDatagram
	bwdLo chan queuedDatagram

	fwdBucket *bucket
	bwdBucket *bucket

	statsMu sync.Mutex
	stats   proxyStats
}

func startThrottleProxy(t *testing.T, target netip.AddrPort, rate int) *throttleProxy {
	t.Helper()
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	targetAddr, err := net.ResolveUDPAddr("udp", target.String())
	if err != nil {
		_ = listener.Close()
		t.Fatalf("proxy target: %v", err)
	}
	p := &throttleProxy{
		listener:  listener,
		target:    targetAddr,
		servers:   map[string]*net.UDPConn{},
		done:      make(chan struct{}),
		fwdHi:     make(chan queuedDatagram, proxyQueueDepth),
		fwdLo:     make(chan queuedDatagram, proxyQueueDepth),
		bwdHi:     make(chan queuedDatagram, proxyQueueDepth),
		bwdLo:     make(chan queuedDatagram, proxyQueueDepth),
		fwdBucket: newBucket(rate, proxyBurstBytes),
		bwdBucket: newBucket(rate, proxyBurstBytes),
	}
	go p.serve()
	go p.sender(p.fwdHi, p.fwdLo, p.fwdBucket, true)
	go p.sender(p.bwdHi, p.bwdLo, p.bwdBucket, false)
	return p
}

// addr returns the proxy listen address (what slow peers dial through).
func (p *throttleProxy) addr() netip.AddrPort {
	udpAddr := p.listener.LocalAddr().(*net.UDPAddr)
	return netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), uint16(udpAddr.Port))
}

func (p *throttleProxy) snapshot() proxyStats {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	return p.stats
}

func (p *throttleProxy) closed() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *throttleProxy) close() {
	p.doneOnce.Do(func() { close(p.done) })
	_ = p.listener.Close()
	p.mu.Lock()
	for _, s := range p.servers {
		_ = s.Close()
	}
	p.mu.Unlock()
}

// enqueue queues a datagram with drop-tail on overflow; small packets take
// the priority lane (Rust: enqueue; priority still consumes wire tokens —
// it is ordering, not free bandwidth).
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
	for !p.closed() {
		_ = p.listener.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, client, err := p.listener.ReadFromUDP(buf)
		if err != nil {
			continue // timeout (or closed): re-check flag
		}
		datagram := append([]byte(nil), buf[:n]...)
		server := func() *net.UDPConn {
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.closed() {
				return nil
			}
			if s, ok := p.servers[client.String()]; ok {
				return s
			}
			sock, err := net.DialUDP("udp", nil, p.target)
			if err != nil {
				fmt.Printf("proxy: dial target: %v\n", err)
				return nil
			}
			p.servers[client.String()] = sock
			clientCopy := *client
			go p.backward(sock, &clientCopy)
			return sock
		}()
		if server == nil {
			continue
		}
		p.enqueue(queuedDatagram{dst: server, data: datagram}, true)
	}
}

// backward relays target->client datagrams for one server-side socket into
// the paced backward queue (Rust: backward; never times out logically —
// an idle relay survives so a quiet connection's return path works).
func (p *throttleProxy) backward(server *net.UDPConn, client *net.UDPAddr) {
	buf := make([]byte, 65535)
	for !p.closed() {
		_ = server.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, err := server.Read(buf)
		if err != nil {
			continue // timeout: re-check flag
		}
		p.enqueue(queuedDatagram{peer: client, data: append([]byte(nil), buf[:n]...)}, false)
	}
}

// sender drains one direction's lanes through its token bucket, serving the
// priority lane first (Rust: sender).
func (p *throttleProxy) sender(hi, lo <-chan queuedDatagram, b *bucket, fwd bool) {
	for {
		var d queuedDatagram
		select {
		case q := <-hi:
			d = q
		default:
			select {
			case q := <-hi:
				d = q
			case q := <-lo:
				d = q
			case <-p.done:
				return
			}
		}
		b.pace(len(d.data))
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

// TestSlowPeerConvergesWithoutWedge replicates across userspace UDP
// throttle proxies in both directions: the slow node must make steady
// progress (90s without progress = wedge = FAIL) until converged within
// the bound, with byte-count proof the data crossed the proxies; direct
// re-peer heals (Rust: slow_peer_converges_without_wedge).
func TestSlowPeerConvergesWithoutWedge(t *testing.T) {
	rate := envInt("MURMUR_RX_SLOW_RATE", 262144)
	rows := envInt("MURMUR_RX_SLOW_ROWS", 600)
	valBytes := envInt("MURMUR_RX_SLOW_VALBYTES", 1024)
	bound := time.Duration(envInt("MURMUR_RX_SLOW_BOUND_SECONDS", 300)) * time.Second

	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:        "resource-slow",
		NumNodes:    2,
		ManualPeers: true,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("resource-slow-no-convergence")
		}
		cluster.Finish(failed)
	}()

	migrateAll(t, cluster)

	// Both directions cross a throttle: replication data follows the
	// connection the receiver initiated, so throttling only one way would
	// leave the data path unthrottled.
	real0 := cluster.NodeInfo(0).ReplAddr
	real1 := cluster.NodeInfo(1).ReplAddr
	toSlow := startThrottleProxy(t, real1, rate)
	toFast := startThrottleProxy(t, real0, rate)
	t.Cleanup(toSlow.close)
	t.Cleanup(toFast.close)
	t.Logf("throttle: node0 -> %s -> node1, node1 -> %s -> node0 at %d B/s", toSlow.addr(), toFast.addr(), rate)
	if err := cluster.AddPeerAddr(0, toSlow.addr()); err != nil {
		t.Fatalf("add peer 0->1 via proxy: %v", err)
	}
	if err := cluster.AddPeerAddr(1, toFast.addr()); err != nil {
		t.Fatalf("add peer 1->0 via proxy: %v", err)
	}

	// Baseline through the throttle proves the proxied path works before
	// the timed leg starts.
	insertRow(t, cluster.Node(0), 1, fmt.Sprintf("%032x", 1), "baseline")
	waitCounts(t, cluster, []int{1}, 1, 120*time.Second)
	t.Log("baseline crossed the throttle")

	// Churn on the fast node; the slow node must make steady progress (no
	// wedge) until fully converged within the bound. Values are unique
	// incompressible bytes per row: repeats would compress away and the
	// throttle would never bind.
	rng := xorShift(0x5eed)
	vals := rng.alnumStrings(rows, valBytes)
	for i, val := range vals {
		insertRow(t, cluster.Node(0), 1, fmt.Sprintf("%032x", i+2), val)
	}
	total := rows + 1
	convergeStart := time.Now()
	deadline := time.Now().Add(bound)
	last := 0
	lastIncrease := time.Now()
	stalled := false
	for {
		n := len(mustRows(t, cluster.Node(1)))
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
		s1 := toSlow.snapshot()
		s2 := toFast.snapshot()
		t.Logf("slow node %d/%d rows; toSlow fwd %dB(%dd) bwd %dB(%dd) | toFast fwd %dB(%dd) bwd %dB(%dd)",
			n, total,
			s1.fwdBytes, s1.fwdDrops, s1.bwdBytes, s1.bwdDrops,
			s2.fwdBytes, s2.fwdDrops, s2.bwdBytes, s2.bwdDrops)
		time.Sleep(5 * time.Second)
	}
	if stalled {
		t.Fatalf("WEDGE: slow node stuck at %d/%d rows for 90s", last, total)
	}
	if last != total {
		t.Fatalf("slow node reached %d/%d rows within %v", last, total, bound)
	}
	took := time.Since(convergeStart)
	// Byte-level proof the data crossed the throttles: ~1 MB of
	// incompressible values must have traversed the slow paths (data may
	// flow through either proxy in either direction). This is the
	// non-vacuous guard: a fast pass with few throttled bytes means
	// traffic bypassed the proxies and the test is broken.
	s1 := toSlow.snapshot()
	s2 := toFast.snapshot()
	throttled := s1.fwdBytes + s1.bwdBytes + s2.fwdBytes + s2.bwdBytes
	if throttled < uint64(rows*valBytes/2) {
		t.Fatalf("only %d throttled bytes for %d x %dB rows: data bypassed the proxies?", throttled, rows, valBytes)
	}
	d := waitDigests(t, cluster, []int{0, 1}, 60*time.Second)
	t.Logf("converged through %d B/s throttles in %v: %d rows, %d throttled bytes (%d drops), %s",
		rate, took, total, throttled, s1.fwdDrops+s1.bwdDrops+s2.fwdDrops+s2.bwdDrops, d)

	// Heal: remove the throttles, re-peer direct, and prove fast
	// convergence is restored.
	toSlow.close()
	toFast.close()
	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatalf("re-peer 0->1 direct: %v", err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		t.Fatalf("re-peer 1->0 direct: %v", err)
	}
	for i := 0; i < 10; i++ {
		insertRow(t, cluster.Node(0), 1, fmt.Sprintf("%032x", total+i+1), "healed")
	}
	waitCounts(t, cluster, []int{0, 1}, total+10, 60*time.Second)
	d2 := waitDigests(t, cluster, []int{0, 1}, 60*time.Second)
	t.Logf("PASS: healed to direct peering, %d rows, %s", total+10, d2)

	failed = false
}

// TestHugeTransactionsAtDefaultBudgets replicates an 8 MiB value
// byte-exact, rejects an oversize transaction naming the budget with no
// partial row and a healthy writer, then converges 20 x 512 KiB exactly
// (Rust: huge_transactions_at_default_budgets).
func TestHugeTransactionsAtDefaultBudgets(t *testing.T) {
	// Production default budgets (64 MiB transaction_bytes, no per-value
	// limit, so the rejection leg trips the transaction budget instead —
	// see below).
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "resource-huge",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("resource-huge-no-convergence")
		}
		cluster.Finish(failed)
	}()

	migrateAll(t, cluster)

	// (a) 8 MiB value: accepted, byte-exact on both nodes.
	huge := strings.Repeat("H", 8<<20)
	hugeID := fmt.Sprintf("%032x", 1)
	insertRow(t, cluster.Node(0), 1, hugeID, huge)
	for _, i := range []int{0, 1} {
		got := readValue(t, cluster, i, hugeID, 120*time.Second)
		if len(got) != len(huge) || got != huge {
			t.Fatalf("node%d value mismatch: len=%d", i+1, len(got))
		}
	}
	t.Log("8 MiB value byte-exact on both nodes")

	// (b) Oversize transaction: clean rejection naming the budget, no
	// partial row, writer healthy. (No per-value limit exists, so one
	// transaction carries 9 x 8 MiB rows past the default 64 MiB
	// transaction budget instead — same fail-closed proof at default
	// budgets.)
	over := make([][2]string, 0, 9)
	for r := 0; r < 9; r++ {
		over = append(over, [2]string{fmt.Sprintf("%032x", 1000+r), strings.Repeat("O", 8<<20)})
	}
	_, err = cluster.Node(0).Transaction(func(s *murmur.Sql) (any, error) {
		for _, pair := range over {
			if _, err := s.Execute("INSERT INTO contention(id, name) VALUES(?, ?)", pair[0], pair[1]); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	if err == nil {
		t.Fatal("oversize transaction accepted, want budget rejection")
	}
	var mErr *murmur.Error
	if !errors.As(err, &mErr) || mErr.Kind != murmur.KindLimit {
		t.Fatalf("rejection error %v is not the budget limit", err)
	}
	if !strings.Contains(err.Error(), "budget exceeded") {
		t.Fatalf("rejection error %v does not name the budget", err)
	}
	for _, i := range []int{0, 1} {
		if n := rowCount(t, cluster.Node(i)); n != 1 {
			t.Fatalf("node%d has %d rows after rejection, want 1 (partial apply?)", i+1, n)
		}
	}
	insertRow(t, cluster.Node(0), 1, fmt.Sprintf("%032x", 3), "probe")
	t.Log("oversize transaction rejected as budget exceeded, no partial row, writer healthy")

	// (c) Sustained large-value load: 20 x 512 KiB converge exactly.
	for i := 0; i < 20; i++ {
		v := strings.Repeat(fmt.Sprintf("%d", i%10), 512<<10)
		node := i % 2
		insertRow(t, cluster.Node(node), node+1, fmt.Sprintf("%032x", 100+i), v)
	}
	waitCounts(t, cluster, []int{0, 1}, 22, 180*time.Second)
	d := waitDigests(t, cluster, []int{0, 1}, 180*time.Second)
	t.Logf("PASS: 22 rows (8MiB + probe + 20x512KiB) converged, %s", d)

	failed = false
}

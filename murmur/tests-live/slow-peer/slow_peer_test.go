// Live scenario: a throttled joiner pulls a snapshot through a slow link
// while the donor stays bounded.
//
// Two nodes mesh directly for a baseline, then the joiner stops while the
// donor writes past aggressive retention and GC expiry (the snapshot-resync
// forcing pattern). After the restart both sides dial through their own UDP
// throttle proxy (token-bucket rate limit, order-preserving): QUIC sees
// only latency and low throughput, so the multi-megabyte snapshot trickles
// instead of bursting. The donor must not buffer unboundedly for the slow
// peer (spool peaks stay under cap), the relay counters must prove traffic
// actually flowed through the proxies (non-vacuous throttle), and the
// joiner must still converge on every row via the snapshot path (flat log
// counters).
package slowpeer

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

const (
	// recordsDDL is the bulk table (payloads size the snapshot).
	recordsDDL = "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL, payload TEXT NOT NULL)"
	// rowsEnv scales the snapshot volume (row count).
	rowsEnv = "MURMUR_SLOW_PEER_ROWS"
	// defaultRows is the default bulk row count (~1.2 MB of payload).
	defaultRows = 1200
	// payloadBytes is the per-row payload size.
	payloadBytes = 1024
	// batchRows is the rows per bulk transaction.
	batchRows = 50
	// kbpsEnv scales the throttle rate (KiB/s per direction).
	kbpsEnv = "MURMUR_SLOW_PEER_KBPS"
	// defaultKBPS is the default throttle rate (~75 s per MB).
	defaultKBPS = 16
	// peakEnv caps donor spool pending bytes during the transfer.
	peakEnv = "MURMUR_SLOW_PEER_MAX_PENDING_MB"
	// defaultPeakMB is the default donor pending-bytes cap.
	defaultPeakMB = 64
	// baselineRows is the pre-stop converged row count.
	baselineRows = 10
	// minCollected is the minimum pruned transactions before the rejoin
	// (bulk batches minus the retained tail, with margin).
	minCollected = uint64(10)
	// resyncEnv scales the throttled convergence wait (seconds).
	resyncEnv = "MURMUR_SLOW_PEER_RESYNC_SECONDS"
	// defaultResyncSecs is the default convergence wait.
	defaultResyncSecs = 420
	// collectTimeout bounds the GC collection loop.
	collectTimeout = 60 * time.Second
	// poll is the convergence poll interval.
	poll = 200 * time.Millisecond
)

var (
	retention       = time.Second
	maximumOffline  = 20 * time.Second
	minimumRetained = uint64(10)
)

// envInt reads a positive-int knob with a default.
func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

// xorShift is a deterministic xorshift64* for incompressible payloads.
type xorShift uint64

func (x *xorShift) next() uint64 {
	v := uint64(*x)
	v ^= v >> 12
	v ^= v << 25
	v ^= v >> 27
	*x = xorShift(v)
	return v * 0x2545f4914f6cdd1d
}

// payload renders n deterministic pseudo-random bytes as text.
func payload(seed uint64, n int) string {
	var rng xorShift = xorShift(seed)
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = byte('a' + rng.next()%26)
	}
	return string(buf)
}

// insertMany writes one multi-row transaction.
func insertMany(t *testing.T, db *murmur.Database, startID int64, count int, prefix string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		for n := int64(0); n < int64(count); n++ {
			id := startID + n
			if _, err := s.Execute("INSERT INTO records(id, name, payload) VALUES(?, ?, ?)",
				id, fmt.Sprintf("%s-%d", prefix, id), payload(uint64(id+1), payloadBytes)); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("batch insert failed: %v", err)
	}
}

// rowCount returns the row count on one node.
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

// waitRowCount waits until one node holds exactly want rows.
func waitRowCount(t *testing.T, db *murmur.Database, want int, timeout time.Duration, what string) {
	t.Helper()
	if ok := harness.WaitUntil(timeout, poll, func() bool {
		return rowCount(t, db) == want
	}); !ok {
		t.Fatalf("%s: count = %d, want %d", what, rowCount(t, db), want)
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

// logCounters returns the log-path counters (received, applied) for the
// snapshot-path proof.
func logCounters(cluster *harness.Cluster, idx int) (uint64, uint64) {
	metrics := cluster.Node(idx).Replication().Status().Metrics
	return metrics.ReceivedTransactions, metrics.AppliedTransactions
}

// throttle is a UDP relay between one node's dial socket (learned from
// its first packet) and the other node's listen socket (configured).
// Exactly one side dials through the proxy: two dialers cannot handshake
// with each other, so relaying dial-to-dial stalls forever on Initial
// retransmits. One goroutine serves every packet with a token-bucket rate
// limit, so per-direction order is preserved; QUIC only observes latency
// and low throughput. Packets from any other source (direct discovery
// dials) are dropped so they cannot bypass the throttle.
type throttle struct {
	conn    *net.UDPConn
	listen  netip.AddrPort // acceptor's listen address
	rate    float64        // bytes per second
	burst   float64
	delay   time.Duration
	stopped atomic.Bool
	relayed atomic.Uint64
	packets atomic.Uint64
	dropped atomic.Uint64
	done    chan struct{}
}

func newThrottle(t *testing.T, kbps int, listen netip.AddrPort) *throttle {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("throttle listen: %v", err)
	}
	th := &throttle{
		conn:   conn,
		listen: listen,
		rate:   float64(kbps) * 1024,
		burst:  64 * 1024,
		delay:  10 * time.Millisecond,
		done:   make(chan struct{}),
	}
	go th.run()
	return th
}

func (th *throttle) addr() netip.AddrPort {
	port := th.conn.LocalAddr().(*net.UDPAddr).Port
	return netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), uint16(port))
}

func (th *throttle) run() {
	defer close(th.done)
	tokens := th.burst
	last := time.Now()
	buf := make([]byte, 65535)
	listenUDP := net.UDPAddrFromAddrPort(th.listen)
	var dial *net.UDPAddr // learned dial socket, once
	for !th.stopped.Load() {
		th.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, src, err := th.conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}
		var dst *net.UDPAddr
		switch src.AddrPort() {
		case listenUDP.AddrPort():
			if dial == nil {
				continue
			}
			dst = dial
		default:
			if dial == nil {
				dial = src
			} else if src.AddrPort() != dial.AddrPort() {
				th.dropped.Add(1)
				continue
			}
			dst = listenUDP
		}
		// Token bucket + fixed latency, then relay.
		now := time.Now()
		tokens += now.Sub(last).Seconds() * th.rate
		last = now
		if tokens > th.burst {
			tokens = th.burst
		}
		if deficit := float64(n) - tokens; deficit > 0 {
			time.Sleep(time.Duration(deficit / th.rate * float64(time.Second)))
			last = time.Now()
			tokens = 0
		} else {
			tokens -= float64(n)
		}
		time.Sleep(th.delay)
		_, _ = th.conn.WriteToUDP(buf[:n], dst)
		th.relayed.Add(uint64(n))
		th.packets.Add(1)
	}
}

func (th *throttle) stop() {
	th.stopped.Store(true)
	th.conn.Close()
	<-th.done
}

// fixedID returns a deterministic node id filler byte.
func fixedID(b byte) [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = b
	}
	return id
}

func TestSlowPeerSnapshotStaysBounded(t *testing.T) {
	rows := envInt(rowsEnv, defaultRows)
	kbps := envInt(kbpsEnv, defaultKBPS)
	peakCap := uint64(envInt(peakEnv, defaultPeakMB)) * 1024 * 1024
	resyncTimeout := time.Duration(envInt(resyncEnv, defaultResyncSecs)) * time.Second
	total := baselineRows + rows

	nodeIDs := [][16]byte{fixedID(0x51), fixedID(0x71)}
	trusted := map[int]map[[16]byte]bool{}
	for i := range nodeIDs {
		perNode := map[[16]byte]bool{}
		for _, id := range nodeIDs {
			perNode[id] = true
		}
		trusted[i] = perNode
	}
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "slow-peer",
		NumNodes: 2,
		NodeIDs:  nodeIDs,
		// Membership stays on: retention collection needs peer liveness
		// records, which static topology does not track. Discovery cannot
		// bypass the proxy below: the proxy sessions establish in
		// milliseconds while discovery needs gossip rounds to learn the
		// joiner's fresh port, so any later direct dial loses duplicate
		// arbitration; the relay counters verify no bypass happened.
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
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("slow-peer-no-convergence")
		}
		cluster.Finish(failed)
	}()
	// Both directions re-dial through the throttle proxy after the
	// restart; the proxy sessions win duplicate arbitration over any
	// later direct discovery dial (see above).

	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, recordsDDL); err != nil {
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

	// Baseline over the direct mesh; then the joiner stops and the donor
	// writes the bulk past retention.
	insertMany(t, cluster.Node(0), 0, baselineRows, "base")
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush baseline: %v", err)
	}
	waitRowCount(t, cluster.Node(1), baselineRows, 60*time.Second, "baseline")
	cluster.StopNode(1)
	// NOTE: node0 deliberately keeps node1 in its peer set (no RemovePeer
	// here). The pre-stop direct session is dead (node1's old port is
	// gone) and cannot carry traffic, but the set entry is what lets node0
	// accept node1's post-restart dial: with node1 removed, node0 refuses
	// the inbound handshake and the proxy session never establishes.
	// Flush every batch: the product mints one replication log entry per
	// flush batch, not per SQL transaction, so a single trailing flush
	// would leave fewer entries than minimum_retained and nothing would
	// ever become eligible for pruning.
	for start := int64(baselineRows); start < int64(total); start += batchRows {
		n := batchRows
		if start+int64(n) > int64(total) {
			n = int(total) - int(start)
		}
		insertMany(t, cluster.Node(0), start, n, "bulk")
		if err := cluster.Node(0).Flush(); err != nil {
			t.Fatalf("flush bulk batch: %v", err)
		}
	}

	// Force the snapshot path: expiry wait plus collection passes until
	// the donor prunes past the retained tail.
	time.Sleep(harness.EnvSeconds("MURMUR_SLOW_PEER_GC_WAIT_SECONDS", 30))
	if ok := harness.WaitUntil(collectTimeout, 2*time.Second, func() bool {
		if collectedTotal(t, cluster, 0) < minCollected {
			if _, err := cluster.Node(0).CollectRetention(); err != nil {
				t.Logf("collect retention: %v", err)
			}
			return false
		}
		return true
	}); !ok {
		t.Fatalf("donor collected only %d transactions, want >= %d (snapshot path not forced)",
			collectedTotal(t, cluster, 0), minCollected)
	}

	// The joiner restarts then both sides dial through their own
	// throttle proxy (one relay per direction; a single relay cannot
	// carry two dialers). Duplicate arbitration keeps exactly one of the
	// two sessions on both sides, and both relays throttle, so whichever
	// wins is slow; direct discovery dials lose the same arbitration
	// because the proxy sessions establish first.
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart joiner: %v", err)
	}
	if err := cluster.RemovePeer(1, 0); err != nil {
		t.Fatalf("drop joiner direct leg: %v", err)
	}
	toDonor := newThrottle(t, kbps, cluster.NodeInfo(0).ReplAddr)
	defer toDonor.stop()
	toJoiner := newThrottle(t, kbps, cluster.NodeInfo(1).ReplAddr)
	defer toJoiner.stop()
	relayed := func() uint64 { return toDonor.relayed.Load() + toJoiner.relayed.Load() }
	beforeReceived, _ := logCounters(cluster, 1)
	if err := cluster.AddPeerAddr(1, toDonor.addr()); err != nil {
		t.Fatalf("joiner dial via proxy: %v", err)
	}
	if err := cluster.AddPeerAddr(0, toJoiner.addr()); err != nil {
		t.Fatalf("donor dial via proxy: %v", err)
	}
	// Sessions must establish through a proxy (fail fast here, not at
	// the resync timeout, when the relay is broken).
	for _, i := range []int{0, 1} {
		if ok := harness.WaitUntil(60*time.Second, poll, func() bool {
			return cluster.Node(i).Replication().Status().Peers >= 1
		}); !ok {
			t.Fatalf("node%d never peered via proxy (relayed=%d bytes)", i, relayed())
		}
	}
	proxies := map[string]bool{toDonor.addr().String(): true, toJoiner.addr().String(): true}
	for i := 0; i < cluster.Len(); i++ {
		for _, p := range cluster.Node(i).Replication().Status().PeersDiagnostics {
			if p.Address == nil || !proxies[p.Address.String()] {
				t.Fatalf("node%d session bypasses the proxies: %v", i, p.Address)
			}
		}
	}
	t.Logf("proxy sessions established (relayed=%d bytes)", relayed())

	// During the trickle, sample the donor's spool: pending bytes must
	// stay under cap (no unbounded slow-peer buffering).
	var peakPending, peakDisk uint64
	var sampling atomic.Bool
	sampling.Store(true)
	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		for sampling.Load() {
			m := cluster.Node(0).SpoolMetrics()
			if m.PendingBytes > peakPending {
				peakPending = m.PendingBytes
			}
			if m.DiskBytes > peakDisk {
				peakDisk = m.DiskBytes
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()
	if ok := harness.WaitUntil(resyncTimeout, 5*time.Second, func() bool {
		return rowCount(t, cluster.Node(1)) == total
	}); !ok {
		for i := 0; i < cluster.Len(); i++ {
			st := cluster.Node(i).Replication().Status()
			lastErr := "<nil>"
			if st.LastError != nil {
				lastErr = *st.LastError
			}
			t.Logf("node%d rows=%d peers=%d members=%d lastError=%s metrics=%+v",
				i, rowCount(t, cluster.Node(i)), st.Peers, st.Members, lastErr, st.Metrics)
			for _, p := range st.PeersDiagnostics {
				addr := "<nil>"
				if p.Address != nil {
					addr = p.Address.String()
				}
				t.Logf("node%d peer=%x addr=%s connected=%v schemaAgreed=%v push=%v",
					i, p.NodeID, addr, p.Connected, p.SchemaAgreed, p.PushMode)
			}
		}
		t.Fatalf("throttled resync: count = %d, want %d (relayed=%d bytes)",
			rowCount(t, cluster.Node(1)), total, relayed())
	}
	sampling.Store(false)
	<-sampleDone
	t.Logf("donor peaks during %d KiB/s trickle: pending=%d disk=%d (cap %d)",
		kbps, peakPending, peakDisk, peakCap)
	if peakPending > peakCap {
		t.Fatalf("donor pending bytes peaked at %d, above the %d cap", peakPending, peakCap)
	}
	// Bulk-exclusivity tripwire: the ~1.2 MB snapshot plus the retained
	// log tail must all have crossed the proxies. A direct bypass would
	// leave the relays far below this floor.
	if relayed() < 512*1024 {
		t.Fatalf("proxies relayed only %d bytes: traffic bypassed the throttle", relayed())
	}
	t.Logf("proxies relayed %d bytes total (toDonor=%d toJoiner=%d, dropped %d)",
		relayed(), toDonor.relayed.Load(), toJoiner.relayed.Load(),
		toDonor.dropped.Load()+toJoiner.dropped.Load())

	// Snapshot-path proof: log growth must stay within the retained tail,
	// far below the gap (the bulk arrived via snapshot, the only other
	// row path).
	received, _ := logCounters(cluster, 1)
	if growth := received - beforeReceived; growth > 2*minimumRetained {
		t.Fatalf("joiner pulled %d log transactions, above the retained tail (2 x %d)", growth, minimumRetained)
	} else if growth >= uint64(rows) {
		t.Fatalf("joiner arrived over the log path (growth %d, gap %d), not a snapshot", growth, rows)
	}

	// Digest equality: same ordered names on both nodes.
	digest := func(db *murmur.Database) string {
		v, err := db.Read(func(s *murmur.Sql) (any, error) {
			return s.Query("SELECT name FROM records ORDER BY name")
		})
		if err != nil {
			t.Fatalf("read names: %v", err)
		}
		out := ""
		for _, row := range v.([][]sqlite.Value) {
			out += row[0].Str + "\n"
		}
		return out
	}
	if a, b := digest(cluster.Node(0)), digest(cluster.Node(1)); a != b {
		t.Fatalf("row digests differ after throttled resync")
	}
	t.Logf("slow peer converged on %d rows via snapshot", total)
	failed = false
}

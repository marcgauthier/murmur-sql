// Live scenario: the node keeps serving its legit peer under a QUIC flood.
//
// Go port of the Rust `dos-client` scenario
// (/home/marc/MURMUR/tests-live/dos-client/tests/dos_client.rs:
// TestDoSClientFlood, QUIC surface only). While an attacker sprays
// garbage UDP and holds valid-credential QUIC half-opens (stream
// opened, Hello never sent) against the victim's replication port, the
// node must keep serving its legit peer (writes converge both ways
// within latency bounds) and attacker state must stay bounded (garbage
// creates no sessions, half-opens never attach as replication peers,
// peer counts recover after teardown). All HTTPS flood vectors are
// absent: there is no HTTPS API server.
package dosclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/replication"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// tableDDL is the contention table DDL (Rust: typed contention rows).
const tableDDL = "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)"

// attackEnv is the attack-duration knob (Rust: same name, default 60).
const attackEnv = "MURMUR_DOS_CLIENT_ATTACK_SECONDS"

// quicEnv is the QUIC half-open knob (Rust: same name, default 24).
const quicEnv = "MURMUR_DOS_CLIENT_QUIC_HALFOPEN"

const (
	// defaultAttackSecs is the default attack duration in seconds.
	defaultAttackSecs = uint64(60)
	// defaultQuicHalf is the default held half-open count.
	defaultQuicHalf = 24
	// garbageThreads is the garbage sprayer count (Rust: 2).
	garbageThreads = 2
	// garbageTick is the spray cadence (Rust: 10 ms).
	garbageTick = 10 * time.Millisecond
	// controlTimeout is the baseline wait (Rust: 30 s converge).
	controlTimeout = 30 * time.Second
	// settleTimeout is the session-settle wait (Rust: 15 s).
	settleTimeout = 15 * time.Second
	// pressureTimeout is the flood-pressure wait (Rust: 30 s).
	pressureTimeout = 30 * time.Second
	// victimWriteBound is the victim-write latency bound during the
	// flood (Rust: < 10 s).
	victimWriteBound = 10 * time.Second
	// recoverTimeout is the post-attack peer recovery wait (Rust: 30 s).
	recoverTimeout = 30 * time.Second
	// postTimeout is the post-attack convergence wait (Rust: 15 s).
	postTimeout = 15 * time.Second
	// poll is the convergence poll interval (Rust: 200 ms).
	poll = 200 * time.Millisecond
)

// attackDuration reads the attack-duration knob (Rust: positive only,
// default 60 s).
func attackDuration() time.Duration {
	if v, err := strconv.ParseUint(os.Getenv(attackEnv), 10, 64); err == nil && v > 0 {
		return time.Duration(v) * time.Second
	}
	return time.Duration(defaultAttackSecs) * time.Second
}

// quicHalf reads the half-open knob (Rust: default 24).
func quicHalf() int {
	if v, err := strconv.Atoi(os.Getenv(quicEnv)); err == nil {
		return v
	}
	return defaultQuicHalf
}

// xorShift is a deterministic xorshift64* (Rust: same).
type xorShift uint64

func (x *xorShift) next() uint64 {
	v := uint64(*x)
	v ^= v >> 12
	v ^= v << 25
	v ^= v >> 27
	*x = xorShift(v)
	return v * 0x2545f4914f6cdd1d
}

func (x *xorShift) fill(buf []byte) {
	for len(buf) > 0 {
		v := x.next()
		for i := 0; i < 8 && len(buf) > 0; i++ {
			buf[0] = byte(v >> (8 * i))
			buf = buf[1:]
		}
	}
}

// contentionRow is one ordered contention row.
type contentionRow struct {
	id   string
	name string
}

// rows returns the ordered contention rows on one node (Rust:
// TypedContentionRows).
func rows(t *testing.T, db *murmur.Database) []contentionRow {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name FROM contention ORDER BY id")
	})
	if err != nil {
		t.Fatalf("read contention rows failed: %v", err)
	}
	var out []contentionRow
	for _, r := range v.([][]sqlite.Value) {
		out = append(out, contentionRow{id: r[0].Str, name: r[1].Str})
	}
	return out
}

// insertRow inserts one row (Rust: TypedContentionInsert).
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

// peerCount returns the live session count (Rust: connected_peers; the
// sessions-analog: garbage and half-opens must never move it).
func peerCount(cluster *harness.Cluster, node int) int {
	h := cluster.Node(node).Replication()
	if h == nil {
		return 0
	}
	return h.Status().Peers
}

func convergedOn(cluster *harness.Cluster, t *testing.T, want int) bool {
	t.Helper()
	var first []contentionRow
	for i := 0; i < cluster.Len(); i++ {
		got := rows(t, cluster.Node(i))
		if len(got) != want {
			return false
		}
		if first == nil {
			first = got
		} else if !equalRows(first, got) {
			return false
		}
	}
	return true
}

func equalRows(a, b []contentionRow) bool {
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

// waitConverged polls exact counts plus ordered-digest equality.
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	if ok := harness.WaitUntil(timeout, poll, func() bool {
		return convergedOn(cluster, t, want)
	}); !ok {
		t.Fatalf("nodes did not converge on %d rows with equal digests within %v", want, timeout)
	}
}

// waitConvergedDeadline polls exact counts plus ordered-digest equality
// until a deadline.
func waitConvergedDeadline(t *testing.T, cluster *harness.Cluster, want int, deadline time.Time, what string) {
	t.Helper()
	rest := time.Until(deadline)
	if rest < 0 {
		rest = 0
	}
	if ok := harness.WaitUntil(rest, poll, func() bool {
		return convergedOn(cluster, t, want)
	}); !ok {
		t.Fatalf("%s: nodes did not converge on %d rows with equal digests by the deadline", what, want)
	}
}

// waitPeersStable waits until one node's peer count holds steady for 5 s
// (Rust: waitSessionsStable on the sessions counter).
func waitPeersStable(cluster *harness.Cluster, t *testing.T, node int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := peerCount(cluster, node)
	stableSince := time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(time.Second)
		cur := peerCount(cluster, node)
		if cur != last {
			last = cur
			stableSince = time.Now()
			continue
		}
		if time.Since(stableSince) >= 5*time.Second {
			return
		}
	}
}

// waitConnectedPeers waits until one node reports want peers.
func waitConnectedPeers(t *testing.T, cluster *harness.Cluster, node, want int, timeout time.Duration) {
	t.Helper()
	if ok := harness.WaitUntil(timeout, 500*time.Millisecond, func() bool {
		return peerCount(cluster, node) == want
	}); !ok {
		t.Fatalf("node%d peers != %d within %v", node+1, want, timeout)
	}
}

// attackerTLS builds the stolen-credential QUIC client config:
// the victim's own node certificate from its tls/ dir, TLS 1.3 + the
// replication ALPN, matching the product server. Any CA-valid server
// identity is accepted (the server name is ignored exactly like the
// product does).
func attackerTLS(t *testing.T, tlsDir string) *tls.Config {
	t.Helper()
	caDER, err := os.ReadFile(filepath.Join(tlsDir, "ca.der"))
	if err != nil {
		t.Fatalf("read ca.der: %v", err)
	}
	nodeDER, err := os.ReadFile(filepath.Join(tlsDir, "node.der"))
	if err != nil {
		t.Fatalf("read node.der: %v", err)
	}
	keyDER, err := os.ReadFile(filepath.Join(tlsDir, "node.key"))
	if err != nil {
		t.Fatalf("read node.key: %v", err)
	}
	key, err := x509.ParsePKCS8PrivateKey(keyDER)
	if err != nil {
		t.Fatalf("parse attacker key: %v", err)
	}
	leaf, err := x509.ParseCertificate(nodeDER)
	if err != nil {
		t.Fatalf("parse attacker leaf: %v", err)
	}
	roots := x509.NewCertPool()
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA root: %v", err)
	}
	roots.AddCert(ca)
	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{nodeDER},
			PrivateKey:  key,
			Leaf:        leaf,
		}},
		ServerName:         "murmur.invalid",
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{string(replication.ALPN)},
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("missing peer certificate")
			}
			peer, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return err
			}
			intermediates := x509.NewCertPool()
			for _, raw := range rawCerts[1:] {
				if cert, err := x509.ParseCertificate(raw); err == nil {
					intermediates.AddCert(cert)
				}
			}
			if _, err := peer.Verify(x509.VerifyOptions{
				Roots:         roots,
				Intermediates: intermediates,
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			}); err != nil {
				return err
			}
			_, err = replication.NodeID(rawCerts[0])
			return err
		},
	}
}

// quicHalfOpenFlood runs the QUIC half-open flood: valid-cert dials that
// open a stream and never send Hello, held open; a top-up loop replaces
// connections the idle timeout reaps. Runs until stop is set.
func quicHalfOpenFlood(tlsConf *tls.Config, repl netip.AddrPort, want int, stop *atomic.Bool, held, attempted *atomic.Int64) {
	quicConf := &quic.Config{
		MaxIdleTimeout:        30 * time.Second,
		KeepAlivePeriod:       10 * time.Second,
		MaxIncomingStreams:    40,
		MaxIncomingUniStreams: -1,
		EnableDatagrams:       true,
	}
	var conns []*quic.Conn
	alive := func(c *quic.Conn) bool {
		select {
		case <-c.Context().Done():
			return false
		default:
			return true
		}
	}
	for {
		kept := conns[:0]
		for _, c := range conns {
			if alive(c) {
				kept = append(kept, c)
			}
		}
		conns = kept
		held.Store(int64(len(conns)))
		for len(conns) < want {
			attempted.Add(1)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			conn, err := quic.DialAddr(ctx, repl.String(), tlsConf, quicConf)
			cancel()
			if err != nil {
				break
			}
			openCtx, openCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err = conn.OpenStreamSync(openCtx)
			openCancel()
			if err != nil {
				_ = conn.CloseWithError(0, "open failed")
				break
			}
			held.Add(1)
			conns = append(conns, conn)
		}
		if stop.Load() {
			break
		}
		time.Sleep(5 * time.Second)
	}
	for _, c := range conns {
		_ = c.CloseWithError(0, "done")
	}
}

// garbageSpray sprays garbage UDP at the replication port. Runs until
// stop is set.
func garbageSpray(repl netip.AddrPort, seed uint64, stop *atomic.Bool) {
	conn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(repl.Port())})
	if err != nil {
		return
	}
	defer conn.Close()
	var rng xorShift = xorShift(seed)
	buf := make([]byte, 1200)
	for !stop.Load() {
		time.Sleep(garbageTick)
		if stop.Load() {
			break
		}
		rng.fill(buf)
		n := 100 + int(rng.next()%1100)
		_, _ = conn.Write(buf[:n])
	}
}

func TestDoSClientFlood(t *testing.T) {
	attackFor := attackDuration()
	nQuicHalf := quicHalf()

	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "dos-client",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("dos-client-no-convergence")
		}
		cluster.Finish(failed)
	}()

	// Same schema on every node.
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			t.Fatalf("migrate contention table: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("contention", true); err != nil {
			t.Fatalf("opt-in contention: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}
	const victim, peer = 0, 1
	repl := cluster.NodeInfo(victim).ReplAddr
	tlsDir := filepath.Join(cluster.NodeInfo(victim).Dir, "tls")

	// Positive control (pre-attack): both directions converge.
	insertRow(t, cluster.Node(peer), peer+1, fmt.Sprintf("%032x", 1), "honest-1")
	waitConverged(t, cluster, 1, controlTimeout)
	insertRow(t, cluster.Node(victim), victim+1, fmt.Sprintf("%032x", 2), "honest-2")
	waitConverged(t, cluster, 2, controlTimeout)

	// Let the mesh settle so peer counts are stable before baselining.
	waitPeersStable(cluster, t, victim, settleTimeout)
	basePeers := peerCount(cluster, victim)
	t.Logf("baseline: peers=%d", basePeers)

	// Phase 1: garbage UDP is rejected cheaply (no sessions, no harm).
	func() {
		var stop atomic.Bool
		var wg sync.WaitGroup
		for g := 0; g < garbageThreads; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				garbageSpray(repl, 0x9a47+uint64(g), &stop)
			}()
		}
		for i := 0; i < 5; i++ {
			insertRow(t, cluster.Node(peer), peer+1, fmt.Sprintf("%032x", 100+i), "garbage-hb")
			time.Sleep(2 * time.Second)
		}
		stop.Store(true)
		wg.Wait()
	}()
	waitConverged(t, cluster, 7, controlTimeout)
	if got := peerCount(cluster, victim); got != basePeers {
		t.Fatalf("garbage UDP moved peers %d -> %d (junk created server state)", basePeers, got)
	}
	t.Logf("garbage phase: peers stable at %d, heartbeats converged", basePeers)

	// Phase 2 (Rust) is HTTPS-only (unauthenticated-exec 401, TLS 1.0
	// refusal) and has no target: there is no HTTPS API server. QUIC
	// pins TLS 1.3 structurally.

	// Phase 3: sustained QUIC flood (garbage spray + half-opens).
	func() {
		var stop atomic.Bool
		var held, attempted atomic.Int64
		var wg sync.WaitGroup
		for g := 0; g < garbageThreads; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				garbageSpray(repl, 0x51ed+uint64(g), &stop)
			}()
		}
		tlsConf := attackerTLS(t, tlsDir)
		wg.Add(1)
		go func() {
			defer wg.Done()
			quicHalfOpenFlood(tlsConf, repl, nQuicHalf, &stop, &held, &attempted)
		}()
		defer func() {
			stop.Store(true)
			wg.Wait()
		}()
		t.Logf("flood running for %v (quic-half-open=%d + garbage)", attackFor, nQuicHalf)
		// Confirm the flood is actually holding victim state before
		// asserting availability under it (non-vacuous: the victim
		// must feel pressure).
		pressured := harness.WaitUntil(pressureTimeout, 500*time.Millisecond, func() bool {
			h, a := held.Load(), attempted.Load()
			return a > 0 && h*2 >= a
		})
		h, a := held.Load(), attempted.Load()
		if !pressured {
			t.Fatalf("flood too weak to prove anything: held %d/%d attacker conns", h, a)
		}
		t.Logf("victim under pressure: attacker held %d/%d QUIC half-opens", h, a)

		deadline := time.Now().Add(attackFor)
		// Legit write directly on the flooded victim succeeds fast.
		start := time.Now()
		insertRow(t, cluster.Node(victim), victim+1, fmt.Sprintf("%032x", 200), "during-flood-victim")
		if dt := time.Since(start); dt > victimWriteBound {
			t.Fatalf("write on flooded victim took %v, want < %v", dt, victimWriteBound)
		}
		// Attacker half-opens must not have attached as sessions.
		if sessions := peerCount(cluster, victim); sessions > basePeers+2 {
			t.Fatalf("peers grew %d -> %d during flood (attacker state attaching?)", basePeers, sessions)
		}
		// The legit peer's write converges on the victim within the
		// latency bound.
		insertRow(t, cluster.Node(peer), peer+1, fmt.Sprintf("%032x", 201), "during-flood-peer")
		waitConvergedDeadline(t, cluster, 9, deadline, "peer write during flood")
		t.Logf("mid-flood convergence ok with %v of attack left", time.Until(deadline))
		// Hold the flood for the remainder so budgets face the full duration.
		if rest := time.Until(deadline); rest > 0 {
			time.Sleep(rest)
		}
	}()

	// Phase 4: teardown recovery is bounded and complete.
	waitConnectedPeers(t, cluster, victim, 1, recoverTimeout)
	if sessions := peerCount(cluster, victim); sessions > basePeers+2 {
		t.Fatalf("post-attack peers %d exceeds baseline %d + 2 (leaked attacker state?)", sessions, basePeers)
	}
	t.Logf("peers recovered to %d (baseline %d)", peerCount(cluster, victim), basePeers)

	// Positive control (post-attack): fast convergence, equal digests.
	insertRow(t, cluster.Node(peer), peer+1, fmt.Sprintf("%032x", 202), "post-attack")
	start := time.Now()
	waitConverged(t, cluster, 10, postTimeout)
	t.Logf("post-attack write converged in %v", time.Since(start))

	failed = false
}

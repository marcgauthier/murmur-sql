// Live test: honest SQL clients remain usable during PostgreSQL and
// QUIC connection pressure.
//
// Go port of tests/dos_client.rs using tests-live/gosqlharness. Identical
// behavior: an attacker holds authenticated QUIC half-open connections
// (TLS client cert from the node dir, never a Hello) plus pre-startup TCP
// and partial-SSLRequest SQL half-opens while spraying UDP garbage; honest
// durable writes on both nodes must keep landing within 10 s, half-opens
// must never become replication peers, and after the attack the mesh
// converges exactly and the peer count returns to baseline. The Rust
// quinn/ring endpoint becomes quic-go with a stdlib TLS client config.
package dosclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

// seconds is a positive-integer seconds env knob with fallback.
func seconds(name string, fallback uint64) time.Duration {
	if v, err := strconv.ParseUint(os.Getenv(name), 10, 64); err == nil && v > 0 {
		return time.Duration(v) * time.Second
	}
	return time.Duration(fallback) * time.Second
}

// quicAttacker dials authenticated QUIC connections using the node TLS
// material from dir (never completing the replication Hello).
type quicAttacker struct {
	tlsConf  *tls.Config
	quicConf *quic.Config
}

// newQuicAttacker builds the attacker config from the node dir (ca.pem,
// cert.pem, key.pem), verifying the server chain and presenting the node
// leaf as the client certificate over ALPN murmur-sqlite/1.
func newQuicAttacker(t *testing.T, dir string) *quicAttacker {
	t.Helper()
	ca, err := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if err != nil {
		t.Fatalf("read ca.pem: %v", err)
	}
	leaf, err := os.ReadFile(filepath.Join(dir, "cert.pem"))
	if err != nil {
		t.Fatalf("read cert.pem: %v", err)
	}
	private, err := os.ReadFile(filepath.Join(dir, "key.pem"))
	if err != nil {
		t.Fatalf("read key.pem: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("parse ca.pem")
	}
	cert, err := tls.X509KeyPair(leaf, private)
	if err != nil {
		t.Fatalf("parse node keypair: %v", err)
	}
	return &quicAttacker{
		tlsConf: &tls.Config{
			RootCAs:      roots,
			Certificates: []tls.Certificate{cert},
			ServerName:   "localhost",
			MinVersion:   tls.VersionTLS13,
			NextProtos:   []string{string(replication.ALPN)},
		},
		quicConf: &quic.Config{},
	}
}

// halfOpen dials one QUIC connection and opens one bidirectional stream,
// holding both open without ever sending a Hello. It reports false when
// either step times out (the caller retries during top-up).
func (a *quicAttacker) halfOpen(addr string) (*quic.Conn, *quic.Stream, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	conn, err := quic.DialAddr(ctx, addr, a.tlsConf, a.quicConf)
	cancel()
	if err != nil {
		return nil, nil, false
	}
	openCtx, openCancel := context.WithTimeout(context.Background(), 2*time.Second)
	stream, err := conn.OpenStreamSync(openCtx)
	openCancel()
	if err != nil {
		_ = conn.CloseWithError(0, "done")
		return nil, nil, false
	}
	return conn, stream, true
}

// peerCount returns the connected-peer count on one node (SHOW PEERS rows
// with connected "true").
func peerCount(t *testing.T, cluster *gosqlharness.Cluster, node int) int {
	t.Helper()
	rows, err := cluster.Node(node).Simple("MURMUR SHOW PEERS")
	if err != nil {
		t.Fatalf("SHOW PEERS: %v", err)
	}
	n := 0
	for _, r := range rows {
		if len(r) > 2 && r[2] == "true" {
			n++
		}
	}
	return n
}

// aliveHalfOpens counts half-opens whose connection has no close reason.
func aliveHalfOpens(halfOpens []halfOpen) int {
	n := 0
	for _, h := range halfOpens {
		if h.conn.Context().Err() == nil {
			n++
		}
	}
	return n
}

type halfOpen struct {
	conn   *quic.Conn
	stream *quic.Stream
}

// durableWriteTimeout bounds one honest durable write (INSERT + FLUSH).
const durableWriteTimeout = 10 * time.Second

func TestSQLAndQUICHalfOpenPressureDoesNotStarveHonestClients(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "dos-client",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	for i := 0; i < 2; i++ {
		if _, err := cluster.Node(i).Simple("CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY,body TEXT NOT NULL)"); err != nil {
			t.Fatalf("create records: %v", err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			t.Fatalf("replicate records: %v", err)
		}
	}
	if _, err := cluster.Node(0).Simple("INSERT INTO records(id,body) VALUES(0,'baseline')"); err != nil {
		t.Fatalf("insert baseline: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush baseline: %v", err)
	}
	expected := [][]string{{"baseline"}}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1}, "SELECT body FROM records ORDER BY id", expected); err != nil {
		t.Fatalf("converge baseline: %v", err)
	}
	base := peerCount(t, cluster, 0)
	if base != 1 {
		t.Fatalf("peer count = %d, want 1", base)
	}
	count := 24
	if v, err := strconv.Atoi(os.Getenv("MURMUR_DOS_CLIENT_QUIC_HALFOPEN")); err == nil && v > 0 {
		count = v
	}
	attacker := newQuicAttacker(t, cluster.NodeInfo(0).Dir)
	replAddr := cluster.NodeInfo(0).ReplAddr.String()
	var halfOpens []halfOpen
	for i := 0; i < count; i++ {
		if conn, stream, ok := attacker.halfOpen(replAddr); ok {
			halfOpens = append(halfOpens, halfOpen{conn: conn, stream: stream})
		}
	}
	if len(halfOpens) < (count+1)/2 {
		t.Fatalf("attack failed to hold enough authenticated QUIC half-opens: %d of %d",
			len(halfOpens), count)
	}
	var sqlHalf []net.Conn
	sqlAddr := cluster.NodeInfo(0).SQLAddr.String()
	for i := 0; i < count; i++ {
		// Hold one pre-startup connection and one partial PostgreSQL SSLRequest.
		pre, err := net.Dial("tcp", sqlAddr)
		if err != nil {
			t.Fatalf("dial SQL half-open: %v", err)
		}
		sqlHalf = append(sqlHalf, pre)
		partial, err := net.Dial("tcp", sqlAddr)
		if err != nil {
			t.Fatalf("dial SQL partial: %v", err)
		}
		if _, err := partial.Write([]byte{0, 0, 0}); err != nil {
			t.Fatalf("write SQL partial: %v", err)
		}
		sqlHalf = append(sqlHalf, partial)
	}
	udpLocal, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("resolve UDP local: %v", err)
	}
	udp, err := net.DialUDP("udp", udpLocal, net.UDPAddrFromAddrPort(cluster.NodeInfo(0).ReplAddr))
	if err != nil {
		t.Fatalf("dial UDP: %v", err)
	}
	defer udp.Close()
	garbage := make([]byte, 1200)
	for i := range garbage {
		garbage[i] = 0xa5
	}
	deadline := time.Now().Add(seconds("MURMUR_DOS_CLIENT_ATTACK_SECONDS", 60))
	id := int64(1)
	for time.Now().Before(deadline) {
		// The server expires missing-Hello connections after five seconds.
		// Replace expired connections so long runs keep applying pressure.
		alive := halfOpens[:0]
		for _, h := range halfOpens {
			if h.conn.Context().Err() == nil {
				alive = append(alive, h)
			}
		}
		halfOpens = alive
		for len(halfOpens) < count {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			conn, err := quic.DialAddr(ctx, replAddr, attacker.tlsConf, attacker.quicConf)
			cancel()
			if err != nil {
				t.Fatalf("half-open top-up handshake failed: %v", err)
			}
			openCtx, openCancel := context.WithTimeout(context.Background(), 2*time.Second)
			stream, err := conn.OpenStreamSync(openCtx)
			openCancel()
			if err != nil {
				_ = conn.CloseWithError(0, "done")
				t.Fatal("half-open stream timed out")
			}
			halfOpens = append(halfOpens, halfOpen{conn: conn, stream: stream})
		}
		for i := 0; i < 10; i++ {
			if _, err := udp.Write(garbage); err != nil {
				t.Fatalf("send UDP garbage: %v", err)
			}
		}
		for node := 0; node < 2; node++ {
			body := fmt.Sprintf("heartbeat-%04d-node-%d", id, node)
			start := time.Now()
			type writeResult struct {
				err error
			}
			done := make(chan writeResult, 1)
			go func() {
				_, err := cluster.Node(node).Execute("INSERT INTO records(id,body) VALUES($1,$2)", id, body)
				if err != nil {
					done <- writeResult{err: err}
					return
				}
				_, err = cluster.Node(node).Simple("MURMUR FLUSH")
				done <- writeResult{err: err}
			}()
			select {
			case res := <-done:
				if res.err != nil {
					t.Fatalf("honest durable SQL write failed: %v", res.err)
				}
			case <-time.After(durableWriteTimeout):
				t.Fatal("honest durable SQL write starved")
			}
			if elapsed := time.Since(start); elapsed >= durableWriteTimeout {
				t.Fatalf("honest durable SQL write took %s", elapsed)
			}
			expected = append(expected, []string{body})
			id++
		}
		if n := peerCount(t, cluster, 0); n > base+2 {
			t.Fatalf("half-opens became replication peers (peers=%d, base=%d)", n, base)
		}
		if alive := aliveHalfOpens(halfOpens); alive < (count+1)/2 {
			t.Fatalf("half-open attack pressure vanished early (%d of %d)", alive, count)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(expected) < 3 {
		t.Fatal("no honest write made under pressure")
	}
	for _, leg := range []struct {
		node    int
		pattern string
	}{{0, "%node-1"}, {1, "%node-0"}} {
		rows, err := cluster.Node(leg.node).Query("SELECT count(*) FROM records WHERE body LIKE $1", leg.pattern)
		if err != nil {
			t.Fatalf("remote apply count: %v", err)
		}
		n, err := rows[0].Int64(0)
		if err != nil {
			t.Fatalf("decode remote apply count: %v", err)
		}
		if n <= 0 {
			t.Fatal("remote apply starved for the entire attack window")
		}
	}
	for _, conn := range sqlHalf {
		_ = conn.Close()
	}
	for _, h := range halfOpens {
		_ = h.conn.CloseWithError(0, "done")
	}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1}, "SELECT body FROM records ORDER BY id", expected); err != nil {
		t.Fatalf("converge after attack: %v", err)
	}
	recovery := time.Now().Add(30 * time.Second)
	for peerCount(t, cluster, 0) != base {
		if !time.Now().Before(recovery) {
			t.Fatal("attacker left replication sessions attached")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := cluster.Node(0).Execute("INSERT INTO records(id,body) VALUES($1,'post-attack')", id); err != nil {
		t.Fatalf("insert post-attack: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush post-attack: %v", err)
	}
	expected = append(expected, []string{"post-attack"})
	if err := gosqlharness.AssertRows(cluster, []int{0, 1}, "SELECT body FROM records ORDER BY id", expected); err != nil {
		t.Fatalf("converge post-attack: %v", err)
	}
	failed = false
}

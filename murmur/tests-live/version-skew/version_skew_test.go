// Live scenario: unknown protocol version refused.
//
// Go port of the Rust `version-skew` scenario
// (/home/marc/MURMUR/tests-live/version-skew/tests/version_skew.rs),
// itself a port of the original Go `version-skew` scenario
// (TestUnknownProtocolVersionRefused). A node advertising handshake
// protocol version 99 never connects in either direction, exchanges no
// data either way, but stays alive and keeps serving local
// reads/writes, while the compatible pair converges normally
// throughout.
//
// The product has no protocol-version testing knob (the version gate
// is the hardcoded `version == Version` check in `engine.go` plus the
// frame-header check in `protocol.go`), so the skewed node is split in
// two: a raw QUIC fixture speaks version 99 in both directions, and
// the unmeshed third cluster node covers the isolated-but-alive leg.
// Plain SQL replaces the typed-records HTTP API.
package versionskew

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/replication"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
	"github.com/quic-go/quic-go"
)

// skewedID is the skewed fixture identity (asserted distinct from the
// node ids).
var skewedID = [16]byte{
	0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99,
	0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99,
}

// skewedVersion is the advertised handshake version of the skewed peer.
const skewedVersion uint16 = 99

// helloKind is the wire discriminant of a Hello frame.
const helloKind uint16 = 1

const (
	// baselineRows are written on node0 (Rust: 10).
	baselineRows = 10
	// localRows are written on the isolated node3 (Rust: 3).
	localRows = 3
	// pairTimeout bounds compatible-pair convergence (Rust: 30s).
	pairTimeout = 30 * time.Second
	// window is the skew observation window (Rust: 10s).
	window = 10 * time.Second
	// poll is the window poll interval (Rust: 500ms).
	poll = 500 * time.Millisecond
	// outboundWindow bounds the outbound leg (Rust: 10s).
	outboundWindow = 10 * time.Second
	// acceptSlice is one fixture accept slice inside the outbound window.
	acceptSlice = 500 * time.Millisecond
	// settle is the post-local-write settle (Rust: 5s).
	settle = 5 * time.Second
	// dialTimeout bounds fixture dial/stream/setup (Rust: 10s).
	dialTimeout = 10 * time.Second
	// welcomeTimeout bounds the wait for a Welcome (Rust: 5s).
	welcomeTimeout = 5 * time.Second
)

// queryNames reads canonical ordered names on one node.
func queryNames(db *murmur.Database) ([]string, error) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM records ORDER BY name")
	})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out, nil
}

// rowCount counts rows on one node.
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

// insert writes one single-row transaction.
func insert(t *testing.T, cluster *harness.Cluster, node int, id int64, name string) {
	t.Helper()
	_, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
		return nil, err
	})
	if err != nil {
		t.Fatalf("node %d insert failed: %v", node, err)
	}
}

// waitPairCount waits until nodes 0 and 1 hold `want` rows each.
func waitPairCount(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		v0, err0 := cluster.Node(0).Read(func(s *murmur.Sql) (any, error) {
			return s.Query("SELECT COUNT(*) FROM records")
		})
		v1, err1 := cluster.Node(1).Read(func(s *murmur.Sql) (any, error) {
			return s.Query("SELECT COUNT(*) FROM records")
		})
		if err0 == nil && err1 == nil {
			n0 := int(v0.([][]sqlite.Value)[0][0].Int)
			n1 := int(v1.([][]sqlite.Value)[0][0].Int)
			if n0 == want && n1 == want {
				return
			}
			if !time.Now().Before(deadline) {
				t.Fatalf("compatible pair counts=%d,%d, want %d within %v", n0, n1, want, timeout)
			}
		} else if !time.Now().Before(deadline) {
			t.Fatalf("compatible pair unreadable, want %d within %v", want, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// connectedPeers reads connected replication sessions on one node.
func connectedPeers(t *testing.T, cluster *harness.Cluster, idx int) int {
	t.Helper()
	h := cluster.Node(idx).Replication()
	if h == nil {
		t.Fatalf("node%d has no replication handle", idx+1)
	}
	return h.Status().Peers
}

// lastError reads the latest replication error on one node.
func lastError(t *testing.T, cluster *harness.Cluster, idx int) string {
	t.Helper()
	h := cluster.Node(idx).Replication()
	if h == nil {
		t.Fatalf("node%d has no replication handle", idx+1)
	}
	if s := h.Status().LastError; s != nil {
		return *s
	}
	return ""
}

// parsePrivateKey parses a PKCS8, EC, or PKCS1 DER private key
// (mirrors the engine endpoint setup).
func parsePrivateKey(t *testing.T, der []byte) any {
	t.Helper()
	if key, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		return key
	}
	if key, err := x509.ParseECPrivateKey(der); err == nil {
		return key
	}
	key, err := x509.ParsePKCS1PrivateKey(der)
	if err != nil {
		t.Fatalf("invalid private key: %v", err)
	}
	return key
}

// skewedPeer is a raw QUIC fixture speaking the replication handshake
// with a skewed protocol version. It shares the cluster CA trust, so
// TLS can never be the refusal cause.
type skewedPeer struct {
	listener *quic.Listener
	dialTLS  *tls.Config
	quicConf *quic.Config
	db       [16]byte
}

// newSkewedPeer builds the fixture endpoint from cluster-issued
// credentials, mirroring the engine mTLS setup.
func newSkewedPeer(t *testing.T, creds replication.Credentials, db [16]byte) *skewedPeer {
	t.Helper()
	roots := x509.NewCertPool()
	for _, der := range creds.Roots {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatalf("invalid trust root: %v", err)
		}
		roots.AddCert(cert)
	}
	leaf, err := x509.ParseCertificate(creds.Chain[0])
	if err != nil {
		t.Fatalf("invalid leaf certificate: %v", err)
	}
	cert := tls.Certificate{
		Certificate: creds.Chain,
		PrivateKey:  parsePrivateKey(t, creds.PrivateKey),
		Leaf:        leaf,
	}
	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    roots,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{string(replication.ALPN)},
	}
	clientTLS := &tls.Config{
		Certificates:       []tls.Certificate{cert},
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
				if c, err := x509.ParseCertificate(raw); err == nil {
					intermediates.AddCert(c)
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
	quicConf := &quic.Config{
		MaxIdleTimeout:        30 * time.Second,
		KeepAlivePeriod:       10 * time.Second,
		MaxIncomingStreams:    40,
		MaxIncomingUniStreams: -1,
		EnableDatagrams:       true,
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, quicConf)
	if err != nil {
		t.Fatalf("fixture listen: %v", err)
	}
	return &skewedPeer{listener: listener, dialTLS: clientTLS, quicConf: quicConf, db: db}
}

// localAddr returns the fixture bind address.
func (p *skewedPeer) localAddr(t *testing.T) netip.AddrPort {
	t.Helper()
	addr, err := netip.ParseAddrPort(p.listener.Addr().String())
	if err != nil {
		t.Fatalf("fixture bind addr: %v", err)
	}
	return addr
}

// hello builds a fully compatible Hello except for the version field,
// so the version is the only reachable refusal cause.
func (p *skewedPeer) hello(version uint16) *replication.Message {
	return &replication.Message{
		Kind:           replication.MsgHello,
		Capabilities:   replication.Capabilities,
		Required:       replication.Required,
		Version:        version,
		Database:       p.db,
		Node:           skewedID,
		Mode:           replication.DisseminationGossip,
		MaxTransaction: 64 << 20,
	}
}

// writeVersioned frames one message with an explicit frame-header
// version. The product writer always stamps `Version`, so a faithful
// skewed peer needs its own framer; Hellos stay far under the
// 1024-byte compression threshold, so flags are always 0.
func writeVersioned(t *testing.T, stream *quic.Stream, msg *replication.Message, frameVersion uint16) {
	t.Helper()
	data := replication.EncodeMessage(msg)
	if len(data) > 1024 {
		t.Fatalf("fixture hello grew past the uncompressed framing limit")
	}
	var h [12]byte
	copy(h[:2], "RD")
	binary.BigEndian.PutUint16(h[2:4], frameVersion)
	binary.BigEndian.PutUint16(h[4:6], helloKind)
	binary.BigEndian.PutUint16(h[6:8], 0)
	binary.BigEndian.PutUint32(h[8:12], uint32(len(data)))
	_ = stream.SetWriteDeadline(time.Now().Add(dialTimeout))
	if _, err := stream.Write(h[:]); err != nil {
		t.Fatalf("fixture write frame header: %v", err)
	}
	if _, err := stream.Write(data); err != nil {
		t.Fatalf("fixture write hello: %v", err)
	}
}

// readWithTimeout reads one framed message with a deadline.
func readWithTimeout(stream *quic.Stream, timeout time.Duration) (replication.Message, error) {
	_ = stream.SetReadDeadline(time.Now().Add(timeout))
	return replication.ReadMessage(stream)
}

// dial opens one handshake stream to addr.
func (p *skewedPeer) dial(t *testing.T, addr netip.AddrPort, what string) (*quic.Conn, *quic.Stream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	conn, err := quic.DialAddr(ctx, addr.String(), p.dialTLS, p.quicConf)
	if err != nil {
		t.Fatalf("%s: fixture connect: %v", what, err)
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		_ = conn.CloseWithError(0, "fixture open failed")
		t.Fatalf("%s: fixture open stream: %v", what, err)
	}
	return conn, stream
}

// assertWelcomed is the positive control: the same fixture with the
// product version MUST be welcomed. Proves TLS trust, identity,
// address, and the hand-rolled framer are all correct, so later
// refusals are caused by the version alone.
func (p *skewedPeer) assertWelcomed(t *testing.T, addr netip.AddrPort) {
	t.Helper()
	conn, stream := p.dial(t, addr, "control")
	defer func() { _ = conn.CloseWithError(0, "control done") }()
	writeVersioned(t, stream, p.hello(replication.Version), replication.Version)
	msg, err := readWithTimeout(stream, welcomeTimeout)
	if err != nil || msg.Kind != replication.MsgWelcome {
		t.Fatalf("control dial with product version not welcomed: %v %+v", err, msg)
	}
	// Drive until the close frame flushes and the engine tears the
	// session down.
	time.Sleep(500 * time.Millisecond)
}

// assertRefused runs one refused handshake: TLS, stream open, and the
// Hello send must all succeed (else the fixture is broken and the
// probe is vacuous); then NO Welcome may arrive -- the stream must die
// or stay silent.
func (p *skewedPeer) assertRefused(t *testing.T, addr netip.AddrPort, frameVersion, helloVersion uint16) {
	t.Helper()
	conn, stream := p.dial(t, addr, "skew probe")
	defer func() { _ = conn.CloseWithError(0, "skew probe done") }()
	writeVersioned(t, stream, p.hello(helloVersion), frameVersion)
	msg, err := readWithTimeout(stream, welcomeTimeout)
	if err == nil {
		if msg.Kind == replication.MsgWelcome {
			t.Fatalf("node at %s sent Welcome to a version-%d hello (frame v%d): fail-closed violated",
				addr, helloVersion, frameVersion)
		}
		t.Fatalf("node at %s answered a version-%d hello with %+v, want refusal",
			addr, helloVersion, msg)
	}
	// Refused: stream died or stayed silent.
}

// acceptWithoutWelcome is the outbound-leg accept side: behave like a
// version-99 node that cannot parse our frames -- read one Hello, then
// close without a Welcome. Returns nil on a quiet slice.
func (p *skewedPeer) acceptWithoutWelcome(t *testing.T, slice time.Duration) *replication.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), slice)
	defer cancel()
	conn, err := p.listener.Accept(ctx)
	if err != nil {
		return nil
	}
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		_ = conn.CloseWithError(0, "skewed: cannot parse")
		return nil
	}
	_ = stream.SetReadDeadline(time.Now().Add(slice))
	msg, err := replication.ReadMessage(stream)
	_ = conn.CloseWithError(0, "skewed: cannot parse")
	if err != nil {
		return nil
	}
	return &msg
}

// TestUnknownProtocolVersionRefused proves version-99 peers are refused
// in both directions while the pair converges and the isolated node
// stays alive (Rust: unknown_protocol_version_refused).
func TestUnknownProtocolVersionRefused(t *testing.T) {
	// Fixed node ids (asserted distinct from the fixture id) so no
	// handshake identity can alias.
	nodeIDs := [][16]byte{
		{0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10},
		{0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20},
		{0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30},
	}
	for _, id := range nodeIDs {
		if id == skewedID {
			t.Fatalf("skewed fixture id collides with a node id")
		}
	}
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "version-skew",
		NumNodes: 3,
		NodeIDs:  nodeIDs,
		// Manual peers: only the compatible pair (0, 1) is meshed,
		// so node3 stays peerless -- alive and writable, but
		// exchanging no data in either direction.
		ManualPeers: true,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("version-skew")
		}
		cluster.Finish(failed)
	}()

	// Same schema on every node (Rust: TypedRecords fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i,
			"CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
			t.Fatalf("node%d migrate records table: %v", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("records", true); err != nil {
			t.Fatalf("node%d opt-in records: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush opt-in: %v", i+1, err)
		}
	}
	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatalf("AddPeer(0,1): %v", err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		t.Fatalf("AddPeer(1,0): %v", err)
	}

	// Skewed-version fixture sharing the cluster CA trust.
	issued, err := cluster.IssueCert(skewedID)
	if err != nil {
		t.Fatalf("issue fixture cert: %v", err)
	}
	dbid := cluster.DatabaseID()
	skewed := newSkewedPeer(t, issued.Credentials, dbid)
	defer func() { _ = skewed.listener.Close() }()

	// Positive control first: a product-version hello from this exact
	// fixture must be welcomed (TLS/identity/framer all proven good).
	skewed.assertWelcomed(t, cluster.NodeInfo(0).ReplAddr)
	t.Logf("control: product-version hello welcomed (fixture identity OK)")
	if !harness.WaitUntil(10*time.Second, 100*time.Millisecond, func() bool {
		return connectedPeers(t, cluster, 0) == 1
	}) {
		t.Fatalf("control session did not tear down before the baseline")
	}

	// Baseline: 10 writes on node0; the compatible pair converges.
	for i := 0; i < baselineRows; i++ {
		insert(t, cluster, 0, int64(i+1), fmt.Sprintf("w-%d", i))
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush baseline: %v", err)
	}
	waitPairCount(t, cluster, baselineRows, pairTimeout)
	if got := rowCount(t, cluster.Node(2)); got != 0 {
		t.Fatalf("isolated node3 holds baseline rows (must stay empty)")
	}

	// Skew window: repeated version-99 dials against both honest nodes
	// must all be refused while peers stay 1/1/0 and node3 stays empty.
	// Rounds alternate the two version gates: the frame-header check in
	// `protocol.go` and the Hello-payload check in `engine.go`.
	var refusals uint32
	var sawFrameGate [2]bool
	var sawHelloGate [2]bool
	deadline := time.Now().Add(window)
	var round uint32
	for time.Now().Before(deadline) {
		if got := connectedPeers(t, cluster, 0); got != 1 {
			t.Fatalf("node1 connected_peers = %d, want 1 (node2 only)", got)
		}
		if got := connectedPeers(t, cluster, 1); got != 1 {
			t.Fatalf("node2 connected_peers = %d, want 1 (node1 only)", got)
		}
		if got := connectedPeers(t, cluster, 2); got != 0 {
			t.Fatalf("isolated node3 connected_peers = %d, want 0", got)
		}
		names, err := queryNames(cluster.Node(2))
		if err != nil {
			t.Fatalf("read node3 names: %v", err)
		}
		if len(names) != 0 {
			t.Fatalf("isolated node3 received inbound data (must stay empty)")
		}
		frameV, helloV := skewedVersion, skewedVersion
		if round%2 != 0 {
			frameV = replication.Version
		}
		for k, idx := range []int{0, 1} {
			addr := cluster.NodeInfo(idx).ReplAddr
			skewed.assertRefused(t, addr, frameV, helloV)
			refusals++
			// Latch the refusal cause: `last_error` is
			// overwritten by every later error, so sample it
			// every round.
			e := lastError(t, cluster, idx)
			if strings.Contains(e, "invalid frame header") {
				sawFrameGate[k] = true
			}
			if strings.Contains(e, "incompatible hello") {
				sawHelloGate[k] = true
			}
		}
		round++
		time.Sleep(poll)
	}
	if refusals < 4 {
		t.Fatalf("only %d skew probes ran (both gates against both nodes required)", refusals)
	}
	for k, idx := range []int{0, 1} {
		if !sawFrameGate[k] {
			t.Fatalf("node%d never latched the frame-header version refusal", idx+1)
		}
		if !sawHelloGate[k] {
			t.Fatalf("node%d never latched the hello-payload version refusal", idx+1)
		}
	}
	t.Logf("skew window: %d version-99 dials refused, peers stayed 1/1/0", refusals)

	// Outbound leg: node0 dials the skewed address; the skewed side
	// reads the Hello and closes without a Welcome, so no session may
	// ever form (either honest node may place the dial if the address
	// spreads via gossip; both must stay at one session).
	skewedAddr := skewed.localAddr(t)
	if err := cluster.AddPeerAddr(0, skewedAddr); err != nil {
		t.Fatalf("add skewed addr: %v", err)
	}
	honest := [][16]byte{cluster.NodeID(0), cluster.NodeID(1)}
	var attempts uint32
	deadline = time.Now().Add(outboundWindow)
	for time.Now().Before(deadline) {
		if hello := skewed.acceptWithoutWelcome(t, acceptSlice); hello != nil {
			if hello.Kind != replication.MsgHello {
				t.Fatalf("fixture got %+v, want Hello", hello)
			}
			if hello.Database != dbid {
				t.Fatalf("dial has wrong DBID")
			}
			known := false
			for _, id := range honest {
				if hello.Node == id {
					known = true
				}
			}
			if !known {
				t.Fatalf("dial is not from an honest node")
			}
			if hello.Version != replication.Version {
				t.Fatalf("dial has wrong version")
			}
			attempts++
		}
		if got := connectedPeers(t, cluster, 0); got != 1 {
			t.Fatalf("node1 formed a session toward the skewed addr")
		}
		if got := connectedPeers(t, cluster, 1); got != 1 {
			t.Fatalf("node2 formed a session toward the skewed addr")
		}
	}
	if attempts < 1 {
		t.Fatalf("no honest dial reached the skewed addr (outbound leg vacuous)")
	}
	t.Logf("outbound leg: %d honest dials answered with skewed silence", attempts)

	// The isolated node stays alive and keeps serving local
	// reads/writes; its data just never leaves the node.
	for i := 0; i < localRows; i++ {
		insert(t, cluster, 2, int64(101+i), fmt.Sprintf("skewed-local-%d", i))
	}
	if err := cluster.Node(2).Flush(); err != nil {
		t.Fatalf("skewed-side local write failed (must stay writable): %v", err)
	}
	time.Sleep(settle)
	names0, err := queryNames(cluster.Node(0))
	if err != nil {
		t.Fatalf("read node1 names: %v", err)
	}
	if len(names0) != baselineRows {
		t.Fatalf("node1 holds skewed-side rows (must stay at baseline)")
	}
	names2, err := queryNames(cluster.Node(2))
	if err != nil {
		t.Fatalf("read node3 names: %v", err)
	}
	if len(names2) != localRows {
		t.Fatalf("isolated node3 lost its local rows")
	}
	failed = false
}

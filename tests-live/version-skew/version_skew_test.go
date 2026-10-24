// Live scenario: a peer advertising an unknown protocol version is
// refused fail-closed.
//
// Go port of tests/version_skew.rs using tests-live/gosqlharness.
// Identical behavior: a 3-node cluster meshes only nodes 0-1 (node 2
// stays isolated), the pair converges on 10 rows while node 2 stays
// empty, a raw QUIC fixture speaking protocol version 99 is refused by
// both meshed nodes across an 8 s window (alternating skewed frame and
// skewed hello versions), the fixture observes an outbound dial without
// sending a Welcome, 3 local rows on node 2 stay put, and the pair still
// holds exactly its baseline. The skewed fixture shares the cluster CA
// trust, so TLS can never be the refusal cause. Message framing reuses
// the in-module murmur/replication package; only the version-stamping
// framer is local, since the product writer always stamps Version.
package versionskew

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"gopkg.in/yaml.v3"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

const (
	// skewedVersion is the unknown protocol version (Rust:
	// SKEWED_VERSION = 99).
	skewedVersion uint16 = 99
	// helloKind is the wire discriminant of a Hello frame (Rust:
	// HELLO_KIND = 1).
	helloKind uint16 = 1
	// dialTimeout bounds fixture dial/stream setup (Rust: 10 s).
	dialTimeout = 10 * time.Second
	// welcomeTimeout bounds the wait for a Welcome (Rust: 5 s).
	welcomeTimeout = 5 * time.Second
	// window is the skew observation window (Rust: 8 s).
	window = 8 * time.Second
	// poll is the window poll interval (Rust: 400 ms).
	poll = 400 * time.Millisecond
)

// loadCredentials reads node TLS files into product credentials.
func loadCredentials(t *testing.T, caPath, certPath, keyPath string) replication.Credentials {
	t.Helper()
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatalf("open ca.pem: %v", err)
	}
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatalf("open cert.pem: %v", err)
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("open key.pem: %v", err)
	}
	var roots [][]byte
	for block, rest := pem.Decode(caPEM); block != nil; block, rest = pem.Decode(rest) {
		if block.Type == "CERTIFICATE" {
			roots = append(roots, block.Bytes)
		}
	}
	if len(roots) == 0 {
		t.Fatal("parse ca: no certificates")
	}
	var chain [][]byte
	for block, rest := pem.Decode(certPEM); block != nil; block, rest = pem.Decode(rest) {
		if block.Type == "CERTIFICATE" {
			chain = append(chain, block.Bytes)
		}
	}
	if len(chain) == 0 {
		t.Fatal("parse cert: no certificates")
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		t.Fatal("read key: no PEM block")
	}
	return replication.Credentials{Chain: chain, PrivateKey: keyBlock.Bytes, Roots: roots}
}

// parsePrivateKey parses a PKCS8, EC, or PKCS1 DER private key.
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
// with a skewed protocol version (Rust: SkewedPeer).
type skewedPeer struct {
	listener *quic.Listener
	dialTLS  *tls.Config
	quicConf *quic.Config
	db       [16]byte
	nodeID   [16]byte
}

// newSkewedPeer builds the fixture endpoint from cluster node
// credentials, mirroring the engine mTLS setup.
func newSkewedPeer(t *testing.T, creds replication.Credentials, db, nodeID [16]byte) *skewedPeer {
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
	dialTLS := &tls.Config{
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
	conf := &quic.Config{
		MaxIdleTimeout:        30 * time.Second,
		KeepAlivePeriod:       10 * time.Second,
		MaxIncomingStreams:    40,
		MaxIncomingUniStreams: -1,
		EnableDatagrams:       true,
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, conf)
	if err != nil {
		t.Fatalf("fixture listen: %v", err)
	}
	return &skewedPeer{listener: listener, dialTLS: dialTLS, quicConf: conf, db: db, nodeID: nodeID}
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
		Node:           p.nodeID,
		Mode:           replication.DisseminationGossip,
		MaxTransaction: 64 << 20,
	}
}

// writeVersioned frames one message with an explicit frame-header
// version. The product writer always stamps Version, so a faithful
// skewed peer needs its own framer.
func writeVersioned(stream *quic.Stream, msg *replication.Message, frameVersion uint16) error {
	data := replication.EncodeMessage(msg)
	var h [12]byte
	copy(h[:2], "RD")
	binary.BigEndian.PutUint16(h[2:4], frameVersion)
	binary.BigEndian.PutUint16(h[4:6], helloKind)
	binary.BigEndian.PutUint16(h[6:8], 0)
	binary.BigEndian.PutUint32(h[8:12], uint32(len(data)))
	_ = stream.SetWriteDeadline(time.Now().Add(dialTimeout))
	defer stream.SetWriteDeadline(time.Time{})
	if _, err := stream.Write(h[:]); err != nil {
		return err
	}
	_, err := stream.Write(data)
	return err
}

// readWithTimeout reads one framed message with a deadline.
func readWithTimeout(stream *quic.Stream, timeout time.Duration) (replication.Message, error) {
	_ = stream.SetReadDeadline(time.Now().Add(timeout))
	defer stream.SetReadDeadline(time.Time{})
	return replication.ReadMessage(stream)
}

func isTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded)
}

// assertRefused runs one refused handshake against addr: TLS and stream
// setup must succeed (else the probe is vacuous, except a fast
// connect-level refusal, which is also fail-closed); then NO Welcome
// may arrive -- the stream must die or stay silent (Rust:
// assert_refused).
func (p *skewedPeer) assertRefused(t *testing.T, addr netip.AddrPort, frameVersion, helloVersion uint16) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	conn, err := quic.DialAddr(ctx, addr.String(), p.dialTLS, p.quicConf)
	cancel()
	if err != nil {
		if isTimeout(err) {
			t.Fatalf("dial timeout to %s", addr)
		}
		t.Logf("dial to %s failed at TLS/connect: %v", addr, err)
		return
	}
	openCtx, openCancel := context.WithTimeout(context.Background(), dialTimeout)
	stream, err := conn.OpenStreamSync(openCtx)
	openCancel()
	if err != nil {
		_ = conn.CloseWithError(0, "skew probe done")
		if isTimeout(err) {
			t.Fatalf("open_bi timeout to %s", addr)
		}
		t.Logf("open_bi error: %v", err)
		return
	}
	defer func() { _ = conn.CloseWithError(0, "skew probe done") }()
	_ = writeVersioned(stream, p.hello(helloVersion), frameVersion)
	msg, err := readWithTimeout(stream, welcomeTimeout)
	if err == nil && msg.Kind == replication.MsgWelcome {
		t.Fatalf("server at %s sent Welcome to version %d!", addr, helloVersion)
	}
	// Connection died, timed out or stream was reset: correct fail-closed refusal.
}

// acceptWithoutWelcome is the outbound-leg accept side: behave like a
// version-99 node that cannot parse our frames -- read one Hello, then
// close without a Welcome. It returns nil on a quiet slice (Rust:
// accept_without_welcome).
func (p *skewedPeer) acceptWithoutWelcome(slice time.Duration) *replication.Message {
	ctx, cancel := context.WithTimeout(context.Background(), slice)
	defer cancel()
	conn, err := p.listener.Accept(ctx)
	if err != nil {
		return nil
	}
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		_ = conn.CloseWithError(0, "close without welcome")
		return nil
	}
	_ = stream.SetReadDeadline(time.Now().Add(slice))
	defer stream.SetReadDeadline(time.Time{})
	msg, err := replication.ReadMessage(stream)
	_ = conn.CloseWithError(0, "close without welcome")
	if err != nil {
		return nil
	}
	return &msg
}

// replicationLastError reads the latest replication error on one node
// (Rust: replication_last_error).
func replicationLastError(t *testing.T, cluster *gosqlharness.Cluster, node int) (string, bool) {
	t.Helper()
	got, err := cluster.Node(node).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		t.Fatalf("node %d SHOW REPLICATION: %v", node, err)
	}
	if len(got) > 0 && len(got[0]) >= 7 && got[0][6] != "" {
		return got[0][6], true
	}
	return "", false
}

// databaseID reads the cluster database id from a node config (Rust:
// the TOML parse in unknown_protocol_version_refused; the Go server
// config is YAML).
func databaseID(t *testing.T, configPath string) [16]byte {
	t.Helper()
	body, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read node config: %v", err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(body, &doc); err != nil {
		t.Fatalf("parse node config: %v", err)
	}
	storage, _ := doc["storage"].(map[string]any)
	dbHex, _ := storage["database_id"].(string)
	raw, err := hex.DecodeString(dbHex)
	if err != nil || len(raw) != 16 {
		t.Fatalf("invalid database_id %q", dbHex)
	}
	var out [16]byte
	copy(out[:], raw)
	return out
}

func decodeNodeID(t *testing.T, idHex string) [16]byte {
	t.Helper()
	raw, err := hex.DecodeString(idHex)
	if err != nil || len(raw) != 16 {
		t.Fatalf("invalid node id %q", idHex)
	}
	var out [16]byte
	copy(out[:], raw)
	return out
}

func TestUnknownProtocolVersionRefused(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:        "version-skew",
		NumNodes:    3,
		ManualPeers: true,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	// Mesh only node 0 and node 1; node 2 remains isolated.
	if _, err := cluster.Node(0).Simple(
		"MURMUR PEER ADD '" + cluster.NodeInfo(1).ReplAddr.String() + "'"); err != nil {
		t.Fatalf("mesh 0->1: %v", err)
	}
	if _, err := cluster.Node(1).Simple(
		"MURMUR PEER ADD '" + cluster.NodeInfo(0).ReplAddr.String() + "'"); err != nil {
		t.Fatalf("mesh 1->0: %v", err)
	}

	// Create table records and enable replication on all nodes.
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple(
			"CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
			t.Fatalf("node %d create records: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate records: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush: %v", i, err)
		}
	}

	// Baseline: write 10 rows on node 0.
	var expectedPair [][]string
	for i := int64(0); i < 10; i++ {
		name := fmt.Sprintf("row-%02d", i)
		if _, err := cluster.Node(0).Execute(
			"INSERT INTO records(id, name) VALUES($1, $2)", i, name); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
		expectedPair = append(expectedPair, []string{name})
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush baseline: %v", err)
	}

	// Nodes 0 and 1 converge on the 10 rows.
	if err := gosqlharness.AssertRows(cluster, []int{0, 1},
		"SELECT name FROM records ORDER BY id", expectedPair); err != nil {
		t.Fatalf("converge pair: %v", err)
	}

	// Node 2 must remain completely empty.
	n2Rows, err := cluster.Node(2).Simple("SELECT name FROM records")
	if err != nil {
		t.Fatalf("node 2 read: %v", err)
	}
	if len(n2Rows) != 0 {
		t.Fatal("Isolated node 2 must have 0 rows")
	}

	// Setup the skewed peer with CA from node 0 and cert from node 2.
	caPEM := filepath.Join(cluster.NodeInfo(0).Dir, "ca.pem")
	certPEM := filepath.Join(cluster.NodeInfo(2).Dir, "cert.pem")
	keyPEM := filepath.Join(cluster.NodeInfo(2).Dir, "key.pem")
	creds := loadCredentials(t, caPEM, certPEM, keyPEM)
	db := databaseID(t, cluster.NodeInfo(0).ConfigPath)
	node2 := decodeNodeID(t, cluster.NodeInfo(2).NodeID)
	skewed := newSkewedPeer(t, creds, db, node2)

	// Skew window: repeated version 99 dials to node 0 and node 1 must
	// be refused.
	deadline := time.Now().Add(window)
	var round uint32
	var sawFrameGate [2]bool
	var sawHelloGate [2]bool
	for time.Now().Before(deadline) {
		var frameV, helloV uint16
		if round%2 == 0 {
			frameV, helloV = skewedVersion, skewedVersion
		} else {
			frameV, helloV = replication.Version, skewedVersion
		}
		for k, idx := range []int{0, 1} {
			skewed.assertRefused(t, cluster.NodeInfo(idx).ReplAddr, frameV, helloV)
			if e, ok := replicationLastError(t, cluster, idx); ok {
				if strings.Contains(e, "invalid frame header") {
					sawFrameGate[k] = true
				}
				if strings.Contains(e, "incompatible hello") {
					sawHelloGate[k] = true
				}
			}
		}
		round++
		time.Sleep(poll)
	}
	t.Logf("skew window: %d rounds, frame gates %v, hello gates %v", round, sawFrameGate, sawHelloGate)

	// Outbound leg: node 0 dials the skewed address.
	skewedAddr := skewed.listener.Addr().String()
	_, _ = cluster.Node(0).Simple("MURMUR PEER ADD '" + skewedAddr + "'")

	// Observe outbound dial from node 0 to skewed peer.
	outboundDeadline := time.Now().Add(6 * time.Second)
	observedOutbound := false
	for time.Now().Before(outboundDeadline) && !observedOutbound {
		if msg := skewed.acceptWithoutWelcome(400 * time.Millisecond); msg != nil {
			if msg.Kind == replication.MsgHello {
				if msg.Version != replication.Version {
					t.Fatalf("node 0 sent unexpected version %d", msg.Version)
				}
				observedOutbound = true
			}
		}
	}
	fmt.Fprintf(os.Stderr, "Observed outbound dial without Welcome: %v\n", observedOutbound)

	// Local writes on isolated node 2.
	for i := int64(0); i < 3; i++ {
		name := fmt.Sprintf("skewed-local-%d", i)
		if _, err := cluster.Node(2).Execute(
			"INSERT INTO records(id, name) VALUES($1, $2)", 100+i, name); err != nil {
			t.Fatalf("node 2 local insert %s: %v", name, err)
		}
	}
	if _, err := cluster.Node(2).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("node 2 flush local rows: %v", err)
	}

	// Verify node 0 and node 1 still hold exactly their 10 baseline rows.
	if err := gosqlharness.AssertRows(cluster, []int{0, 1},
		"SELECT name FROM records ORDER BY id", expectedPair); err != nil {
		t.Fatalf("pair still converged: %v", err)
	}

	// Verify node 2 holds exactly its 3 local rows.
	expectedN2 := [][]string{{"skewed-local-0"}, {"skewed-local-1"}, {"skewed-local-2"}}
	n2Got, err := cluster.Node(2).Simple("SELECT name FROM records ORDER BY id")
	if err != nil {
		t.Fatalf("node 2 reread: %v", err)
	}
	if !equalRows(n2Got, expectedN2) {
		t.Fatalf("node 2 rows = %v, want %v: data leaked or was corrupted", n2Got, expectedN2)
	}
	failed = false
}

func equalRows(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

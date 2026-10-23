// Live scenario: the QUIC replication endpoint pins the TLS floor at 1.3.
//
// Go port of the Rust `tls-floor` scenario
// (/home/marc/MURMUR/tests-live/tls-floor/tests/tls_floor.rs). The product
// has no HTTPS API, so the floor is proven on the QUIC replication
// endpoint: a TLS-1.2-only handshake must fail, TLS 1.3 succeeds (with the
// negotiated version asserted), and a 2-node mesh converges over it.
package tlsfloor

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// tableDDL is the marker table (Rust: typed records marker).
const tableDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)"

const (
	// meshTimeout is the mesh convergence deadline (Rust: 60 s).
	meshTimeout = 60 * time.Second
	// dialTimeout is the per-dial timeout (Rust: 10 s client timeout).
	dialTimeout = 10 * time.Second
	// poll is the convergence poll interval (Rust: 100 ms).
	poll = 100 * time.Millisecond
)

// probeID is the fixed external identity for the TLS probe clients.
var probeID = [16]byte{
	0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0,
	0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0, 0xF0,
}

// probeTLSConfig builds an attacker-side TLS client config presenting a
// cluster-issued certificate with exactly the given TLS versions enabled
// (Rust: MinVersion/MaxVersion pinning so the ClientHello genuinely emits
// the old version). Chain verification mirrors the product client: roots
// pinned, server name ignored, leaf SAN node id parsed.
func probeTLSConfig(t *testing.T, creds replication.Credentials, minVersion, maxVersion uint16) *tls.Config {
	t.Helper()
	key, err := x509.ParsePKCS8PrivateKey(creds.PrivateKey)
	if err != nil {
		t.Fatalf("parse probe key: %v", err)
	}
	leaf, err := x509.ParseCertificate(creds.Chain[0])
	if err != nil {
		t.Fatalf("parse probe leaf: %v", err)
	}
	roots := x509.NewCertPool()
	for _, der := range creds.Roots {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatalf("parse trust root: %v", err)
		}
		roots.AddCert(cert)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: creds.Chain,
			PrivateKey:  key,
			Leaf:        leaf,
		}},
		ServerName:         "murmur.invalid",
		MinVersion:         minVersion,
		MaxVersion:         maxVersion,
		NextProtos:         []string{string(replication.ALPN)},
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("missing peer certificate")
			}
			leaf, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return err
			}
			intermediates := x509.NewCertPool()
			for _, raw := range rawCerts[1:] {
				if cert, err := x509.ParseCertificate(raw); err == nil {
					intermediates.AddCert(cert)
				}
			}
			if _, err := leaf.Verify(x509.VerifyOptions{
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

func quicConf() *quic.Config {
	return &quic.Config{
		MaxIdleTimeout:        30 * time.Second,
		KeepAlivePeriod:       10 * time.Second,
		MaxIncomingStreams:    40,
		MaxIncomingUniStreams: -1,
		EnableDatagrams:       true,
	}
}

// names returns the canonical ordered names on one node (Rust: TypedNames).
func names(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM live_typed_records ORDER BY name")
	})
	if err != nil {
		t.Fatalf("read names failed: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

// peerCount is the live session count (failed handshakes must never attach
// as sessions).
func peerCount(cluster *harness.Cluster, node int) int {
	h := cluster.Node(node).Replication()
	if h == nil {
		return 0
	}
	return h.Status().Peers
}

func TestQUICFloorRejectsBelowTLS13(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "tls-floor",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("tls-floor-no-convergence")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}
	// Same schema on every node (Rust: typed fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			t.Fatalf("migrate records table: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("live_typed_records", true); err != nil {
			t.Fatalf("opt-in live_typed_records: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}
	repl := cluster.NodeInfo(0).ReplAddr

	// Mesh positive control: a marker converges over QUIC (TLS 1.3),
	// proving the floor does not break replication.
	if _, err := cluster.Node(0).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO live_typed_records(name) VALUES(?)", "tls-version-floor-marker")
		return nil, err
	}); err != nil {
		t.Fatalf("marker insert: %v", err)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush marker: %v", err)
	}
	converged := harness.WaitUntil(meshTimeout, poll, func() bool {
		var first []string
		for i := 0; i < cluster.Len(); i++ {
			got := names(t, cluster.Node(i))
			if len(got) != 1 {
				return false
			}
			if first == nil {
				first = got
			} else if len(first) != len(got) || first[0] != got[0] {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("marker did not converge within %v", meshTimeout)
	}

	issued, err := cluster.IssueCert(probeID)
	if err != nil {
		t.Fatalf("issue probe cert: %v", err)
	}
	dial := func(t *testing.T, addr netip.AddrPort, conf *tls.Config, timeout time.Duration) (*quic.Conn, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return quic.DialAddr(ctx, addr.String(), conf, quicConf())
	}

	// Floor positive first: the identical client shape at TLS 1.3
	// completes a handshake and opens a stream. This leg runs before the
	// legacy probe so its proof is banked even if the toolchain reacts
	// badly to a sub-1.3 config below.
	modern := probeTLSConfig(t, issued.Credentials, tls.VersionTLS13, tls.VersionTLS13)
	conn, err := dial(t, repl, modern, dialTimeout)
	if err != nil {
		t.Fatalf("TLS 1.3 dial failed: %v", err)
	}
	state := conn.ConnectionState().TLS
	if !state.HandshakeComplete || state.Version != tls.VersionTLS13 {
		_ = conn.CloseWithError(0, "probe done")
		t.Fatalf("TLS 1.3 dial negotiated version %x (complete=%v), want TLS 1.3", state.Version, state.HandshakeComplete)
	}
	openCtx, openCancel := context.WithTimeout(context.Background(), 5*time.Second)
	stream, err := conn.OpenStreamSync(openCtx)
	openCancel()
	if err != nil {
		_ = conn.CloseWithError(0, "probe done")
		t.Fatalf("TLS 1.3 stream open failed: %v", err)
	}
	_ = stream.Close()
	_ = conn.CloseWithError(0, "probe done")
	// Drive until the close frame flushes so the no-phantom-session check
	// below sees the teardown.
	time.Sleep(500 * time.Millisecond)
	t.Log("TLS 1.3 client completed handshake + stream with negotiated TLS 1.3")

	// Floor negative: a TLS-1.2-only client must fail the handshake against
	// the 1.3-only server. Dialability itself was proven by the positive
	// leg above, so any failure here is version-caused.
	legacy := probeTLSConfig(t, issued.Credentials, tls.VersionTLS12, tls.VersionTLS12)
	legacyConn, err := dial(t, repl, legacy, dialTimeout)
	if err == nil {
		_ = legacyConn.CloseWithError(0, "floor violated")
		t.Fatal("TLS 1.2 dial unexpectedly succeeded: floor violated")
	}
	t.Logf("TLS 1.2 dial refused (%v); floor holds", err)

	// The rejected handshake attached no session.
	if ok := harness.WaitUntil(5*time.Second, 50*time.Millisecond, func() bool {
		return peerCount(cluster, 0) == 1
	}); !ok {
		t.Fatalf("rejected handshake moved peers (mesh peer + phantom?): peers=%d", peerCount(cluster, 0))
	}
	t.Log("PASS: floor holds at TLS 1.3, mesh peer intact, no phantom session")
	failed = false
}

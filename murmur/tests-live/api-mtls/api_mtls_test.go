// Live scenario: the product's TLS server requires mutual authentication.
//
// Go port of the Rust `api-mtls` scenario
// (/home/marc/MURMUR/tests-live/api-mtls/tests/api_mtls.rs). The product
// has no HTTPS API server, so this port proves the same TLS policy on the
// product's only TLS server — the QUIC replication endpoint, which
// verifies client certificates against the cluster CA at handshake:
// trusted client certs complete handshakes and open streams, missing or
// foreign-CA certs are rejected at handshake, plaintext is refused, and
// rejected handshakes attach no sessions.
package apimtls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/marcgauthier/murmur-sql-go/murmur/replication"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// dialTimeout is the per-dial timeout (Rust: 10 s).
const dialTimeout = 10 * time.Second

// rogueID is the fixed external identity for the foreign-CA probe.
var rogueID = [16]byte{
	0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0A,
	0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0A,
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

// attackerTLSConfig builds an attacker-side TLS client config: the CA
// roots used to verify the server plus an optional client chain/key.
// Server verification mirrors the product client: roots pinned, server
// name ignored, leaf SAN node id parsed.
func attackerTLSConfig(t *testing.T, rootsDER [][]byte, chain [][]byte, keyDER []byte) *tls.Config {
	t.Helper()
	roots := x509.NewCertPool()
	for _, der := range rootsDER {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatalf("parse trust root: %v", err)
		}
		roots.AddCert(cert)
	}
	conf := &tls.Config{
		ServerName:         "murmur.invalid",
		MinVersion:         tls.VersionTLS13,
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
	if chain != nil && keyDER != nil {
		key, err := x509.ParsePKCS8PrivateKey(keyDER)
		if err != nil {
			t.Fatalf("parse client key: %v", err)
		}
		leaf, err := x509.ParseCertificate(chain[0])
		if err != nil {
			t.Fatalf("parse client leaf: %v", err)
		}
		conf.Certificates = []tls.Certificate{{
			Certificate: chain,
			PrivateKey:  key,
			Leaf:        leaf,
		}}
	}
	return conf
}

// tryHandshake attempts one QUIC handshake; true when it completes and
// stays open. TLS 1.3 completes client-side when the server Finished
// arrives, before the server validates our certificate flight, so a
// rejection always lands as an async close: staying open means the
// handshake genuinely succeeded.
func tryHandshake(addr netip.AddrPort, conf *tls.Config) bool {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	conn, err := quic.DialAddr(ctx, addr.String(), conf, quicConf())
	cancel()
	if err != nil {
		return false
	}
	defer func() { _ = conn.CloseWithError(0, "probe done") }()
	select {
	case <-conn.Context().Done():
		return false
	case <-time.After(5 * time.Second):
		return true
	}
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

func TestTLSEndpointRequiresMutualAuth(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "api-mtls",
		NumNodes: 1,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("api-mtls-no-convergence")
		}
		cluster.Finish(failed)
	}()
	info := cluster.NodeInfo(0)
	t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)

	repl := cluster.NodeInfo(0).ReplAddr
	// Trusted client material, issued from the cluster CA for an external
	// identity (same trust as a node leaf).
	nodeID := cluster.NodeID(0)
	issued, err := cluster.IssueCert(nodeID)
	if err != nil {
		t.Fatalf("issue trusted probe cert: %v", err)
	}
	roots := issued.Credentials.Roots

	// Trusted client certificate: the handshake completes and a stream
	// opens.
	trusted := attackerTLSConfig(t, roots, issued.Credentials.Chain, issued.Credentials.PrivateKey)
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	conn, err := quic.DialAddr(ctx, repl.String(), trusted, quicConf())
	cancel()
	if err != nil {
		t.Fatalf("trusted handshake failed: %v", err)
	}
	openCtx, openCancel := context.WithTimeout(context.Background(), 5*time.Second)
	stream, err := conn.OpenStreamSync(openCtx)
	openCancel()
	if err != nil {
		_ = conn.CloseWithError(0, "probe done")
		t.Fatalf("trusted stream open failed: %v", err)
	}
	_ = stream.Close()
	_ = conn.CloseWithError(0, "probe done")
	time.Sleep(500 * time.Millisecond)
	t.Log("trusted client certificate: handshake + stream ok")

	// No client certificate: the server must close the connection. QUIC
	// has no HTTP layer for a 401, so the rejection is always observed
	// as an async close — and there is no /healthz-style exemption to
	// probe: every connection to this endpoint is authenticated.
	noCert := attackerTLSConfig(t, roots, nil, nil)
	if tryHandshake(repl, noCert) {
		t.Fatal("handshake without a client certificate succeeded, want rejection")
	}
	t.Log("no client certificate: handshake rejected")

	// Foreign-CA client certificate: the handshake must fail.
	rogueCA, err := harness.GenerateCa()
	if err != nil {
		t.Fatalf("rogue CA: %v", err)
	}
	rogue, err := rogueCA.Issue(rogueID, nil)
	if err != nil {
		t.Fatalf("rogue node cert: %v", err)
	}
	foreign := attackerTLSConfig(t, roots, rogue.Credentials.Chain, rogue.Credentials.PrivateKey)
	if tryHandshake(repl, foreign) {
		t.Fatal("handshake with a foreign-CA certificate succeeded, want rejection")
	}
	t.Log("foreign-CA client certificate: handshake rejected")

	// Failed handshakes attached no sessions.
	if n := peerCount(cluster, 0); n != 0 {
		t.Fatalf("rejected handshakes attached %d sessions", n)
	}

	// Plaintext: nothing listens on TCP at the replication port, and UDP
	// garbage creates no session.
	if tcp, err := net.DialTimeout("tcp", repl.String(), 5*time.Second); err == nil {
		_ = tcp.Close()
		t.Fatal("plaintext TCP unexpectedly reached the TLS endpoint")
	} else {
		t.Log("plaintext TCP refused")
	}
	udp, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(repl.Port())})
	if err != nil {
		t.Fatalf("garbage socket: %v", err)
	}
	garbage := make([]byte, 300)
	for i := range garbage {
		garbage[i] = 0x55
	}
	for i := 0; i < 10; i++ {
		_, _ = udp.Write(garbage)
		time.Sleep(50 * time.Millisecond)
	}
	_ = udp.Close()
	time.Sleep(time.Second)
	if n := peerCount(cluster, 0); n != 0 {
		t.Fatalf("garbage UDP attached %d sessions", n)
	}
	t.Log("PASS: mutual TLS enforced, plaintext refused, no phantom sessions")
	failed = false
}

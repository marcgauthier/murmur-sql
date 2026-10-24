// Live scenario: the QUIC replication endpoint enforces a TLS 1.3 floor.
//
// Go port of tests/tls_floor.rs using tests-live/gosqlharness.
// Identical behavior: a 2-node cluster converges a marker over QUIC,
// a TLS-1.2-only QUIC client is refused, a TLS 1.3 client completes a
// handshake and opens a stream, and the mesh still converges a
// post-probe marker. The Rust negative leg (a rustls QUIC client
// constructor that rejects TLS 1.2) has no quic-go equivalent, so the
// floor is proven the Go way: a MaxVersion-TLS1.2 dial must fail while
// the identical TLS 1.3 dial succeeds. Chain verification mirrors the
// product client: roots pinned, server name ignored, leaf SAN node id
// parsed.
package tlsfloor

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

// loadCredentials reads node TLS files into product credentials (Rust:
// the PEM reads in build_client_config).
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

// probeTLSConfig builds a probe-side TLS client config with exactly the
// given TLS versions enabled (Rust: build_client_config with a version
// pin).
func probeTLSConfig(t *testing.T, creds replication.Credentials, minVersion, maxVersion uint16) *tls.Config {
	t.Helper()
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
			PrivateKey:  parsePrivateKey(t, creds.PrivateKey),
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

func quicConf() *quic.Config {
	return &quic.Config{
		MaxIdleTimeout:        30 * time.Second,
		KeepAlivePeriod:       10 * time.Second,
		MaxIncomingStreams:    40,
		MaxIncomingUniStreams: -1,
		EnableDatagrams:       true,
	}
}

// dial opens a QUIC connection to addr with the given TLS config.
func dial(addr netip.AddrPort, conf *tls.Config, timeout time.Duration) (*quic.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return quic.DialAddr(ctx, addr.String(), conf, quicConf())
}

func TestQUICFloorRejectsBelowTLS13(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "tls-floor",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple(
			"CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
			t.Fatalf("node %d create records: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate records: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush: %v", i, err)
		}
	}

	// Mesh positive control: a marker converges over QUIC (TLS 1.3).
	if _, err := cluster.Node(0).Execute(
		"INSERT INTO records(id, name) VALUES($1, $2)", int64(1), "tls-version-floor-marker"); err != nil {
		t.Fatalf("marker insert: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush marker: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1},
		"SELECT name FROM records ORDER BY id",
		[][]string{{"tls-version-floor-marker"}}); err != nil {
		t.Fatalf("converge marker: %v", err)
	}

	repl := cluster.NodeInfo(0).ReplAddr
	dir := cluster.NodeInfo(0).Dir
	creds := loadCredentials(t,
		filepath.Join(dir, "ca.pem"), filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"))

	// Floor negative: TLS 1.2 is strictly rejected for QUIC (RFC 9001
	// requires TLS 1.3 exclusively). Dialability itself was proven by
	// the mesh above, so any failure here is version-caused.
	legacy := probeTLSConfig(t, creds, tls.VersionTLS12, tls.VersionTLS12)
	legacyConn, err := dial(repl, legacy, 10*time.Second)
	if err == nil {
		_ = legacyConn.CloseWithError(0, "floor violated")
		t.Fatal("TLS 1.2 dial unexpectedly succeeded: floor violated")
	}
	t.Logf("TLS 1.2 dial refused (%v); floor holds", err)

	// Floor positive: TLS 1.3 handshake completes and opens a stream.
	modern := probeTLSConfig(t, creds, tls.VersionTLS13, tls.VersionTLS13)
	conn, err := dial(repl, modern, 10*time.Second)
	if err != nil {
		t.Fatalf("TLS 1.3 dial failed: %v", err)
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

	// Verify mesh replication remains healthy post-probe.
	if _, err := cluster.Node(1).Execute(
		"INSERT INTO records(id, name) VALUES($1, $2)", int64(2), "post-probe-marker"); err != nil {
		t.Fatalf("post-probe insert: %v", err)
	}
	if _, err := cluster.Node(1).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush post-probe: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1},
		"SELECT name FROM records ORDER BY id",
		[][]string{{"tls-version-floor-marker"}, {"post-probe-marker"}}); err != nil {
		t.Fatalf("converge post-probe: %v", err)
	}
	failed = false
}

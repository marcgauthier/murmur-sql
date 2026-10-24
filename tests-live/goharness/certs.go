// Test CA + per-node QUIC/TLS credential provisioning.
//
// One self-signed CA per cluster, one leaf per node with SAN URI
// replicateddb://node/<hex> (the identity the replication transport
// parses) and server+client EKU. Material is written as PEM under each
// node's tls/ dir for the server's replication.*_file settings and kept
// for forensics parity.
package goharness

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// Ca is a cluster test certificate authority.
type Ca struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	der  []byte
}

// Issued is the issued leaf material for one node (PEM for the server's
// *_file settings, plus DER for in-process QUIC endpoints: the attacker
// fixture builds product Credentials from the DER without PEM re-parsing).
type Issued struct {
	CertPEM string
	KeyPEM  string
	CertDER []byte
	KeyDER  []byte
}

// TlsPaths holds the PEM files under a node's tls/ dir.
type TlsPaths struct {
	Ca    string
	Chain string
	Key   string
}

// GenerateCa creates a fresh self-signed test CA.
func GenerateCa() (*Ca, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "murmur-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(30, 0, 0),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &Ca{cert: cert, key: key, der: der}, nil
}

// CaPEM returns the PEM of the cluster CA certificate.
func (c *Ca) CaPEM() string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.der}))
}

// CaDER returns the DER bytes of the cluster CA certificate.
func (c *Ca) CaDER() []byte {
	out := make([]byte, len(c.der))
	copy(out, c.der)
	return out
}

// Issue issues a leaf for nodeID. A non-nil ttl overrides the
// (effectively unbounded) default validity; used by cert-lifecycle for
// expiry legs.
func (c *Ca) Issue(nodeID [16]byte, ttl *time.Duration) (*Issued, error) {
	return c.issue(nodeID, ttl, nil, nil)
}

// IssueWithHosts issues a leaf for nodeID carrying extra DNS/IP SANs. The
// SQL harness uses this for its combined replication/SQL listener
// certificates (localhost + 127.0.0.1 alongside the node URI).
func (c *Ca) IssueWithHosts(nodeID [16]byte, dns []string, ips []net.IP) (*Issued, error) {
	return c.issue(nodeID, nil, dns, ips)
}

func (c *Ca) issue(nodeID [16]byte, ttl *time.Duration, dns []string, ips []net.IP) (*Issued, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	notBefore := time.Now().Add(-time.Hour)
	notAfter := time.Now().AddDate(30, 0, 0)
	if ttl != nil {
		notBefore = time.Now()
		notAfter = notBefore.Add(*ttl)
	}
	serial := make([]byte, 8)
	if _, err := rand.Read(serial); err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: new(big.Int).SetBytes(serial),
		Subject:      pkix.Name{CommonName: "murmur-test-node"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     dns,
		IPAddresses:  ips,
		URIs:         []*url.URL{{Scheme: "replicateddb", Host: "node", Path: "/" + Hex(nodeID[:])}},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, c.cert, key.Public(), c.key)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	return &Issued{
		CertPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		KeyPEM:  string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})),
		CertDER: der,
		KeyDER:  keyDER,
	}, nil
}

// WriteTLSDir writes tls/ca.pem, tls/node.pem, tls/node-key.pem under
// nodeDir.
func (c *Ca) WriteTLSDir(issued *Issued, nodeDir string) (*TlsPaths, error) {
	tlsDir := filepath.Join(nodeDir, "tls")
	if err := os.MkdirAll(tlsDir, 0o755); err != nil {
		return nil, err
	}
	paths := &TlsPaths{
		Ca:    filepath.Join(tlsDir, "ca.pem"),
		Chain: filepath.Join(tlsDir, "node.pem"),
		Key:   filepath.Join(tlsDir, "node-key.pem"),
	}
	if err := os.WriteFile(paths.Ca, []byte(c.CaPEM()), 0o644); err != nil {
		return nil, err
	}
	if err := os.WriteFile(paths.Chain, []byte(issued.CertPEM), 0o644); err != nil {
		return nil, err
	}
	if err := os.WriteFile(paths.Key, []byte(issued.KeyPEM), 0o600); err != nil {
		return nil, err
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(paths.Key, 0o600); err != nil {
			return nil, fmt.Errorf("chmod node key: %w", err)
		}
	}
	return paths, nil
}

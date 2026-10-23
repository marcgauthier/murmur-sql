// Test CA + per-node QUIC/TLS credential provisioning.
//
// Mirrors the compatConfigs pattern in murmur/replication_compat_test.go:
// one self-signed CA per cluster, one leaf per node with SAN URI
// replicateddb://node/<hex> (the identity replication.NodeID parses) and
// server+client EKU. tls/ files are kept per node for forensics parity.
package harness

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/replication"
)

// Ca is a cluster test certificate authority.
type Ca struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	der  []byte
}

// Issued is the issued leaf material for one node.
type Issued struct {
	Credentials replication.Credentials
	CertDER     []byte
	KeyDER      []byte
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
		NotAfter:              time.Now().Add(24 * time.Hour),
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

// CaDER returns the DER bytes of the cluster CA certificate.
func (c *Ca) CaDER() []byte {
	out := make([]byte, len(c.der))
	copy(out, c.der)
	return out
}

// Issue issues a leaf for nodeID. A non-nil ttl overrides the default
// validity; used by cert-lifecycle for expiry legs.
func (c *Ca) Issue(nodeID [16]byte, ttl *time.Duration) (*Issued, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	notBefore := time.Now().Add(-time.Hour)
	notAfter := time.Now().Add(24 * time.Hour)
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
		URIs:         []*url.URL{{Scheme: "replicateddb", Host: "node", Path: "/" + replication.Hex(nodeID[:])}},
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
		Credentials: replication.Credentials{
			Chain:      [][]byte{der},
			PrivateKey: keyDER,
			Roots:      [][]byte{c.CaDER()},
		},
		CertDER: der,
		KeyDER:  keyDER,
	}, nil
}

// WriteTLSDir writes tls/ca.der, tls/node.der, tls/node.key under nodeDir.
func (c *Ca) WriteTLSDir(issued *Issued, nodeDir string) error {
	tls := filepath.Join(nodeDir, "tls")
	if err := os.MkdirAll(tls, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(tls, "ca.der"), c.CaDER(), 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(tls, "node.der"), issued.CertDER, 0o600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(tls, "node.key"), issued.KeyDER, 0o600)
}

// Live scenario: production PostgreSQL-wire endpoints require a trusted
// client certificate.
//
// Go port of tests/api_mtls.rs using tests-live/gosqlharness plus the
// goharness wire client. Identical behavior: a single node takes a row,
// restarts with production TLS + mTLS required, serves the valid client,
// rejects plaintext / missing-certificate / untrusted-certificate clients,
// still enforces password and unlock-key auth for valid-certificate
// clients, accepts a new row, and keeps both rows across a restart.
// The Rust edit_config closure becomes two SetConfig section writes and
// the rcgen rogue certificate becomes a stdlib x509 self-signed one.
package apimtls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

func TestSQLMTLS(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "api-mtls",
		NumNodes: 1,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	if _, err := cluster.Node(0).Simple(
		"CREATE TABLE records(id INTEGER PRIMARY KEY,body TEXT); INSERT INTO records VALUES(1,'before-tls')"); err != nil {
		t.Fatalf("seed records: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("stop node: %v", err)
	}
	dir := cluster.NodeInfo(0).Dir
	cert := filepath.Join(dir, "cert.pem")
	key := filepath.Join(dir, "key.pem")
	ca := filepath.Join(dir, "ca.pem")
	if err := cluster.SetConfig(0, "server", map[string]any{"production": true}); err != nil {
		t.Fatalf("enable production: %v", err)
	}
	if err := cluster.SetConfig(0, "server.tls", map[string]any{
		"enabled":   true,
		"cert_file": cert,
		"key_file":  key,
		"ca_file":   ca,
	}); err != nil {
		t.Fatalf("enable server TLS: %v", err)
	}
	cluster.SetClientTLS(0, goharness.TLSConfig{
		Mode:     goharness.TLSVerifyFull,
		CaFile:   ca,
		CertFile: cert,
		KeyFile:  key,
	})
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart with TLS: %v", err)
	}
	valid := cluster.Config(0)
	assertRows(t, "valid client read", cluster.Node(0), "SELECT body FROM records",
		[][]string{{"before-tls"}})

	plaintext := valid
	plaintext.TLS = goharness.TLSConfig{}
	if conn, err := goharness.Connect(plaintext); err == nil {
		_ = conn.Close()
		t.Fatal("production accepted plaintext SQL")
	}

	missing := valid
	missing.TLS.CertFile = ""
	missing.TLS.KeyFile = ""
	if conn, err := goharness.Connect(missing); err == nil {
		_ = conn.Close()
		t.Fatal("production accepted no client certificate")
	}

	rogueCert, rogueKey := writeRogueCert(t, dir)
	untrusted := valid
	untrusted.TLS.CertFile = rogueCert
	untrusted.TLS.KeyFile = rogueKey
	if conn, err := goharness.Connect(untrusted); err == nil {
		_ = conn.Close()
		t.Fatal("untrusted client certificate accepted")
	}

	badPassword := valid
	badPassword.Password = "wrong-mtls-password"
	conn, err := goharness.Connect(badPassword)
	if err == nil {
		_ = conn.Close()
		t.Fatal("certificate bypassed password auth")
	}
	if code := goharness.ErrorCode(err); code != "28P01" {
		t.Fatalf("bad password code = %q, want 28P01 (%v)", code, err)
	}

	badKey := valid
	badKey.Key = "wrong-mtls-unlock-key"
	conn, err = goharness.Connect(badKey)
	if err == nil {
		_ = conn.Close()
		t.Fatal("certificate bypassed unlock")
	}
	if code := goharness.ErrorCode(err); code != "28000" {
		t.Fatalf("bad key code = %q, want 28000 (%v)", code, err)
	}

	fresh, err := goharness.Connect(valid)
	if err != nil {
		t.Fatalf("fresh valid connect: %v", err)
	}
	if _, err := fresh.Simple("INSERT INTO records VALUES(2,'after-rejections')"); err != nil {
		_ = fresh.Close()
		t.Fatalf("insert after rejections: %v", err)
	}
	if _, err := fresh.Simple("MURMUR FLUSH"); err != nil {
		_ = fresh.Close()
		t.Fatalf("flush after rejections: %v", err)
	}
	_ = fresh.Close()

	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("stop node: %v", err)
	}
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart node: %v", err)
	}
	assertRows(t, "post-restart read", cluster.Node(0),
		"SELECT body FROM records ORDER BY id",
		[][]string{{"before-tls"}, {"after-rejections"}})
	failed = false
}

func assertRows(t *testing.T, what string, client *goharness.Client, query string, expected [][]string) {
	t.Helper()
	rows, err := client.Simple(query)
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if len(rows) != len(expected) {
		t.Fatalf("%s: got %v, want %v", what, rows, expected)
	}
	for i := range rows {
		if len(rows[i]) != len(expected[i]) {
			t.Fatalf("%s: got %v, want %v", what, rows, expected)
		}
		for j := range rows[i] {
			if rows[i][j] != expected[i][j] {
				t.Fatalf("%s: got %v, want %v", what, rows, expected)
			}
		}
	}
}

// writeRogueCert writes a self-signed client certificate the cluster CA did
// not issue (Rust: rcgen self-signed ClientAuth cert) and returns the PEM
// paths.
func writeRogueCert(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate rogue key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "rogue-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("self-sign rogue cert: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal rogue key: %v", err)
	}
	certPath = filepath.Join(dir, "untrusted-cert.pem")
	keyPath = filepath.Join(dir, "untrusted-key.pem")
	if err := os.WriteFile(certPath,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatalf("write rogue cert: %v", err)
	}
	if err := os.WriteFile(keyPath,
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write rogue key: %v", err)
	}
	return certPath, keyPath
}

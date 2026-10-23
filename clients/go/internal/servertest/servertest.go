// Package servertest builds murmur-server once per process and spawns
// scratch instances for integration tests and examples.
package servertest

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// TB is the testing surface servertest needs. TB satisfies it;
// examples use a trivial main-package implementation.
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
	Cleanup(func())
}

// RepoRoot locates the workspace root (the directory holding go.mod).
func RepoRoot(t TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("cannot locate servertest package")
	}
	root := filepath.Join(filepath.Dir(file), "..", "..", "..", "..")
	root, err := filepath.Abs(root)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repository root not found at %s", root)
	}
	return root
}

var buildOnce sync.Once

// Build compiles murmur-server and murmur-admin once per test process, unless
// MURMUR_SERVER_BIN points at a prebuilt server binary.
func Build(t TB) (serverBin, adminBin string) {
	t.Helper()
	if prebuilt := os.Getenv("MURMUR_SERVER_BIN"); prebuilt != "" {
		admin := "murmur-admin"
		if runtime.GOOS == "windows" {
			admin += ".exe"
		}
		return prebuilt, filepath.Join(filepath.Dir(prebuilt), admin)
	}
	buildOnce.Do(func() {
		root := RepoRoot(t)
		for _, name := range []string{"murmur-server", "murmur-admin"} {
			suffix := ""
			if runtime.GOOS == "windows" {
				suffix = ".exe"
			}
			target := filepath.Join(root, "bin", name+suffix)
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				t.Fatalf("%v", err)
			}
			cmd := exec.Command("go", "build", "-o", target, "./cmd/"+name)
			cmd.Dir = root
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("go build %s failed: %v\n%s", name, err, out)
			}
		}

	})
	suffix := ""
	if runtime.GOOS == "windows" {
		suffix = ".exe"
	}
	target := filepath.Join(RepoRoot(t), "bin")
	return filepath.Join(target, "murmur-server"+suffix), filepath.Join(target, "murmur-admin"+suffix)
}

var hashOnce sync.Once
var cachedHash string

// PasswordHash returns an Argon2 hash for password, generated once through
// murmur-admin hash-password to cover the CLI-to-config chain.
func PasswordHash(t TB, password string) string {
	t.Helper()
	hashOnce.Do(func() {
		_, adminBin := Build(t)
		out, err := exec.Command(adminBin, "hash-password", "--password", password).CombinedOutput()
		if err != nil {
			t.Fatalf("murmur-admin hash-password failed: %v\n%s", err, out)
		}
		cachedHash = strings.TrimSpace(string(out))
		if !strings.HasPrefix(cachedHash, "$argon2id$") {
			t.Fatalf("unexpected hash shape: %q", cachedHash)
		}
	})
	return cachedHash
}

// FreePort returns an unused loopback TCP port.
func FreePort(t TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// Options configures one scratch server.
type Options struct {
	NodeID string
	// DatabaseID pins [storage].database_id; mesh members must share it.
	DatabaseID string
	Port       int
	Production bool
	// TLSFiles, when set, enables [server.tls] with these PEM paths.
	TLSFiles *TLSFiles
	// ExtraYAML is appended to the generated config.
	ExtraYAML string
	// ExtraTOML alias for backwards compatibility.
	ExtraTOML string
}

// TLSFiles points at PEM files for [server.tls].
type TLSFiles struct {
	Cert, Key, CA string
}

// Server is a running scratch murmur-server.
type Server struct {
	Dir        string
	ConfigPath string
	Port       int
	cmd        *exec.Cmd
}

// Start writes a scratch config (user "admin") and spawns murmur-server. The
// server is killed during test cleanup; call Stop for an early shutdown
// (e.g. restart tests).
func Start(t TB, dir string, opts Options) *Server {

	t.Helper()
	serverBin, _ := Build(t)
	if opts.Port == 0 {
		opts.Port = FreePort(t)
	}
	configPath := filepath.Join(dir, "config.yaml")
	var tlsSection string
	if opts.TLSFiles != nil {
		caLine := ""
		if opts.TLSFiles.CA != "" {
			caLine = fmt.Sprintf("    ca_file: %q\n", opts.TLSFiles.CA)
		}
		tlsSection = fmt.Sprintf("  tls:\n    enabled: true\n    cert_file: %q\n    key_file: %q\n%s",
			opts.TLSFiles.Cert, opts.TLSFiles.Key, caLine)
	}
	production := "false"
	if opts.Production {
		production = "true"
	}
	if existing, err := os.ReadFile(configPath); err == nil && len(existing) > 0 {
		lines := strings.Split(string(existing), "\n")
		for i, line := range lines {
			if strings.HasPrefix(strings.TrimSpace(line), "bind:") || strings.HasPrefix(strings.TrimSpace(line), "bind :") {
				lines[i] = fmt.Sprintf("  bind: \"127.0.0.1:%d\"", opts.Port)
				break
			}
		}
		if err := os.WriteFile(configPath, []byte(strings.Join(lines, "\n")), 0o644); err != nil {
			t.Fatalf("%v", err)
		}
	} else {
		extra := opts.ExtraYAML
		if extra == "" {
			extra = opts.ExtraTOML
		}
		config := fmt.Sprintf("server:\n  bind: \"127.0.0.1:%d\"\n  production: %s\n%s\n"+
			"storage:\n  data_dir: %q\n%s%s\n"+
			"auth:\n  users:\n    - name: \"admin\"\n      password_hash: %q\n\n"+
			"logging:\n  dir: %q\n\n"+
			"backups:\n  dir: %q\n\n%s",
			opts.Port, production, tlsSection,
			filepath.Join(dir, "data"), nodeIDLine(opts.NodeID), databaseIDLine(opts.DatabaseID), PasswordHash(t, "test-password"),
			filepath.Join(dir, "logs"), filepath.Join(dir, "backups"), extra)
		if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
			t.Fatalf("%v", err)
		}
	}
	cmd := exec.Command(serverBin, "--config", configPath)
	cmd.Stdout = nil
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("%v", err)
	}
	server := &Server{Dir: dir, ConfigPath: configPath, Port: opts.Port, cmd: cmd}
	deadline := time.Now().Add(20 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", opts.Port), 200*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			t.Fatalf("server exited early: %s", stderr.String())
		}
		if time.Now().After(deadline) {
			cmd.Process.Kill()
			t.Fatalf("server on port %d did not start: %s", opts.Port, stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Cleanup(server.Stop)
	return server
}

// Stop kills the server process.
func (s *Server) Stop() {
	if s.cmd == nil || s.cmd.Process == nil {
		return
	}
	s.cmd.Process.Kill()
	s.cmd.Wait()
	s.cmd = nil
}

// PKI is a test certificate hierarchy: CA plus server and client leaves.
type PKI struct {
	CA         []byte
	ServerCert []byte
	ServerKey  []byte
	ClientCert []byte
	ClientKey  []byte
}

// IssuePKI mints a CA, a server certificate for 127.0.0.1, and a client
// certificate, all ECDSA P-256 with 24h validity.
func IssuePKI(t TB) *PKI {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("%v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "murmur test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("%v", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("%v", err)
	}
	leaf := func(serial int64, usage x509.ExtKeyUsage, ips ...net.IP) (certPEM, keyPEM []byte) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("%v", err)
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: "murmur test leaf"},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{usage},
			IPAddresses:  ips,
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatalf("%v", err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatalf("%v", err)
		}
		certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
		return certPEM, keyPEM
	}
	srvCert, srvKey := leaf(2, x509.ExtKeyUsageServerAuth, net.ParseIP("127.0.0.1"))
	cliCert, cliKey := leaf(3, x509.ExtKeyUsageClientAuth)
	return &PKI{
		CA:         pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		ServerCert: srvCert,
		ServerKey:  srvKey,
		ClientCert: cliCert,
		ClientKey:  cliKey,
	}
}

// WriteServerFiles stores the CA and server certificate; returns TLSFiles for Options.
func (p *PKI) WriteServerFiles(t TB, dir string) *TLSFiles {
	t.Helper()
	paths := map[string][]byte{"ca.crt": p.CA, "server.crt": p.ServerCert, "server.key": p.ServerKey}
	out := map[string]string{}
	for name, data := range paths {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatalf("%v", err)
		}
		out[name] = path
	}
	return &TLSFiles{Cert: out["server.crt"], Key: out["server.key"], CA: out["ca.crt"]}
}

// WriteClientFiles stores the client certificate and key.
func (p *PKI) WriteClientFiles(t TB, dir string) (certFile, keyFile string) {
	t.Helper()
	certFile = filepath.Join(dir, "client.crt")
	keyFile = filepath.Join(dir, "client.key")
	if err := os.WriteFile(certFile, p.ClientCert, 0o644); err != nil {
		t.Fatalf("%v", err)
	}
	if err := os.WriteFile(keyFile, p.ClientKey, 0o600); err != nil {
		t.Fatalf("%v", err)
	}
	return certFile, keyFile
}

func nodeIDLine(node string) string {
	if node == "" {
		return ""
	}
	return fmt.Sprintf("  node_id: %q\n", node)
}

func databaseIDLine(db string) string {
	if db == "" {
		return ""
	}
	return fmt.Sprintf("  database_id: %q\n", db)
}

// nodeCredential is one leaf certificate plus its private key (PEM).
type nodeCredential struct {
	certPEM, keyPEM []byte
}

// issueNodeCredentials mints a CA plus one leaf per node id, each carrying a
// SAN URI replicateddb://node/<hex> with server+client EKU. Scratch keys only.
func issueNodeCredentials(t TB, commonName string, nodes []string) (caPEM []byte, creds []nodeCredential) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("%v", err)
	}
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(10), Subject: pkix.Name{CommonName: commonName + " CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("%v", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("%v", err)
	}
	for i, node := range nodes {
		leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("%v", err)
		}
		uri, _ := url.Parse("replicateddb://node/" + node)
		leaf := &x509.Certificate{SerialNumber: big.NewInt(11 + int64(i)), Subject: pkix.Name{CommonName: commonName + " node"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour), BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, URIs: []*url.URL{uri}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, caKey)
		if err != nil {
			t.Fatalf("%v", err)
		}
		private, err := x509.MarshalPKCS8PrivateKey(leafKey)
		if err != nil {
			t.Fatalf("%v", err)
		}
		creds = append(creds, nodeCredential{
			certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
			keyPEM:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}),
		})
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), creds
}

// FileOptions provisions a standalone encrypted file store with replication
// credentials but no peers. Tests and examples use scratch keys only.
func FileOptions(t TB, dir string) Options {
	t.Helper()
	node := strings.Repeat("09", 16)
	caPEM, creds := issueNodeCredentials(t, "files", []string{node})
	for name, data := range map[string][]byte{"file-ca.pem": caPEM, "file-node.pem": creds[0].certPEM, "file-key.pem": creds[0].keyPEM} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatalf("%v", err)
		}
	}
	return Options{
		NodeID: node,
		ExtraYAML: fmt.Sprintf("replication:\n  enabled: true\n  bind: \"127.0.0.1:0\"\n  cert_chain_file: %q\n  private_key_file: %q\n  ca_file: %q\n  signing_key_hex: %q\n  trusted_origins:\n    %q: %q\n  files:\n    directory: %q\n    key_hex: %q\n",
			filepath.Join(dir, "file-node.pem"), filepath.Join(dir, "file-key.pem"), filepath.Join(dir, "file-ca.pem"),
			strings.Repeat("03", 32), node, fmt.Sprintf("%x", ed25519.NewKeyFromSeed([]byte(strings.Repeat("\x03", 32))).Public()),
			filepath.Join(dir, "objects"), strings.Repeat("07", 32)),
	}
}

func hexID(t TB, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("%v", err)
	}
	return hex.EncodeToString(b)
}

// MeshOptions provisions len(dirs) replication nodes sharing one database id
// and CA, with mutual trusted_origins. It returns one Options per dir, in
// order. replicationExtra is appended inside each node's replication
// section (for example merge_policies). Nodes start unmeshed; dial them
// together with PeerAdd.
func MeshOptions(t TB, dirs []string, replicationExtra string) []Options {
	t.Helper()
	if len(dirs) == 0 {
		t.Fatalf("MeshOptions needs at least one directory")
	}
	dbid := hexID(t, 16)
	nodes := make([]string, len(dirs))
	for i := range nodes {
		for {
			id := hexID(t, 16)
			dup := false
			for _, n := range nodes[:i] {
				if n == id {
					dup = true
				}
			}
			if !dup {
				nodes[i] = id
				break
			}
		}
	}
	seeds := make([]string, len(dirs))
	pubs := make([]string, len(dirs))
	for i := range dirs {
		seed := make([]byte, 32)
		if _, err := rand.Read(seed); err != nil {
			t.Fatalf("%v", err)
		}
		seeds[i] = hex.EncodeToString(seed)
		pubs[i] = fmt.Sprintf("%x", ed25519.NewKeyFromSeed(seed).Public())
	}
	caPEM, creds := issueNodeCredentials(t, "mesh", nodes)
	var origins []string
	for i := range nodes {
		origins = append(origins, fmt.Sprintf("    %q: %q", nodes[i], pubs[i]))
	}
	opts := make([]Options, len(dirs))
	for i, dir := range dirs {
		for name, data := range map[string][]byte{"repl-ca.pem": caPEM, "repl-node.pem": creds[i].certPEM, "repl-key.pem": creds[i].keyPEM} {
			if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
				t.Fatalf("%v", err)
			}
		}
		extra := ""
		if replicationExtra != "" {
			extra = "\n" + strings.TrimRight(replicationExtra, "\n") + "\n"
		}
		opts[i] = Options{
			NodeID:     nodes[i],
			DatabaseID: dbid,
			ExtraYAML: fmt.Sprintf("replication:\n  enabled: true\n  bind: \"127.0.0.1:0\"\n  cert_chain_file: %q\n  private_key_file: %q\n  ca_file: %q\n  signing_key_hex: %q\n  trusted_origins:\n%s%s\n",
				filepath.Join(dir, "repl-node.pem"), filepath.Join(dir, "repl-key.pem"), filepath.Join(dir, "repl-ca.pem"),
				seeds[i], strings.Join(origins, "\n"), extra),
		}
	}
	return opts
}

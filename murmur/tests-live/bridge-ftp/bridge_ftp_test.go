// Live scenario: sealed bridge bundles cross an FTP dropzone.
//
// A two-node Low mesh and a two-node High mesh (separate DBIDs, CAs,
// storage keys, and file object keys) move files across two FTP legs:
// Low upload, Low mesh convergence, signed export of sealed bundles,
// PublishFTP to a real FTP server speaking the full protocol (login,
// TYPE I, CWD/MKD, PASV data connections, STOR), retrieval into the
// High staging area, High import with re-encryption under the High
// object key, and High mesh convergence. File one crosses over plain
// FTP published concurrently by both Low exporters; file two crosses
// over explicit FTPS with a pinned private CA; a wrong-password leg
// proves authentication failures surface as errors and store nothing;
// an HTTP leg proves PublishHTTP carries a sealed bundle byte-identical
// and that re-importing it is a harmless no-op.
package bridgeftp

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// lowName is the Low (exporter) cluster name; owns
// runtime/bridge-ftp-low/, which also holds the Low staging dir so
// artifacts are retained/cleaned with it.
const lowName = "bridge-ftp-low"

// highName is the High (importer) cluster name; owns
// runtime/bridge-ftp-high/ plus the High staging dir.
const highName = "bridge-ftp-high"

// stream is the bridge stream.
const stream = "field-files"

// fileOne is the plain-FTP file.
const fileOne = "shared/field-map.bin"

// fileOneBytes is the plain-FTP upload size (384 KiB, as in
// files-bridge).
const fileOneBytes = 384 << 10

// fileTwo is the FTPS file.
const fileTwo = "shared/field-notes.bin"

// fileTwoBytes is the FTPS upload size (48 KiB: fewer chunks, the TLS
// leg is the point).
const fileTwoBytes = 48 << 10

const (
	// wait is the convergence deadline.
	wait = 30 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
)

func bytes16(b byte) [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = b
	}
	return id
}

func bytes32(b byte) [32]byte {
	var id [32]byte
	for i := range id {
		id[i] = b
	}
	return id
}

// Independent bridge domains (each harness cluster already provisions
// its own CA).
var (
	lowDBID       = bytes16(0xF0)
	highDBID      = bytes16(0xF1)
	lowNode0      = bytes16(0xF2)
	lowNode1      = bytes16(0xF3)
	highNode0     = bytes16(0xF4)
	highNode1     = bytes16(0xF5)
	lowKey        = bytes32(0xF6)
	highKey       = bytes32(0xF7)
	lowObjectKey  = bytes32(0x11)
	highObjectKey = bytes32(0x22)
	// lowSigner is the Low bundle-signer seed (ed25519; the High
	// importer pins its public key).
	lowSigner = bytes32(0x21)
	// highSecret is the High receiver secret (X25519; seals the file
	// chunks for High).
	highSecret = bytes32(0x31)
)

// payload renders the deterministic payload: data[i] = (i*37 +
// i/251) % 253, seeded per file so the two files differ.
func payload(size int, seed int) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = byte((i*37 + i/251 + seed) % 253)
	}
	return out
}

func nodeBridge(t *testing.T, cluster *harness.Cluster, idx int) *replication.Bridge {
	t.Helper()
	h := cluster.Node(idx).Replication()
	if h == nil {
		t.Fatalf("node%d replication handle missing", idx+1)
	}
	b := h.Bridge()
	if b == nil {
		t.Fatalf("node%d bridge missing", idx+1)
	}
	return b
}

// pump is the export+import acceleration pump. Counts are never
// asserted: the replication engine also drives capture/process in the
// background, so progress is proven by file state instead of
// published/imported counters.
func pump(t *testing.T, low, high *harness.Cluster) {
	t.Helper()
	if _, err := nodeBridge(t, low, 0).Capture(); err != nil {
		t.Fatalf("low capture: %v", err)
	}
	if _, err := nodeBridge(t, high, 0).Process(); err != nil {
		t.Fatalf("high process: %v", err)
	}
}

// assertNodeBytes downloads name from cluster node idx and proves full
// integrity: exact bytes plus the uploader's digest carried in the
// metadata.
func assertNodeBytes(t *testing.T, cluster *harness.Cluster, idx int, name string, want []byte, wantDigest [32]byte) {
	t.Helper()
	var got bytes.Buffer
	meta, err := cluster.Node(idx).GetFile(name, &got)
	if err != nil {
		t.Fatalf("node%d download %s failed: %v", idx+1, name, err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("node%d %s bytes differ", idx+1, name)
	}
	if meta.Digest != wantDigest {
		t.Fatalf("node%d %s digest differs", idx+1, name)
	}
}

// waitMetadata waits until cluster node idx carries nondeleted metadata
// for name with the expected digest.
func waitMetadata(t *testing.T, cluster *harness.Cluster, idx int, name string, wantDigest [32]byte, what string) {
	t.Helper()
	arrived := harness.WaitUntil(wait, poll, func() bool {
		meta, err := cluster.Node(idx).FileMetadata(name)
		return err == nil && meta != nil && meta.Digest == wantDigest
	})
	if !arrived {
		t.Fatalf("%s: metadata never arrived", what)
	}
}

// waitAvailable waits until cluster node idx holds the object locally.
func waitAvailable(t *testing.T, cluster *harness.Cluster, idx int, name string, what string) {
	t.Helper()
	arrived := harness.WaitUntil(wait, poll, func() bool {
		available, err := cluster.Node(idx).FileAvailable(name)
		return err == nil && available
	})
	if !arrived {
		t.Fatalf("%s: object never became available", what)
	}
}

func contains(haystack, needle []byte) bool {
	return len(needle) == 0 || bytes.Index(haystack, needle) >= 0
}

// ftpServer is a real (not scripted) minimal FTP server: concurrent
// control connections, fixed credentials, directory creation, PASV
// data connections, STOR and RETR, and explicit FTPS when configured
// with a certificate. Stored files live in memory keyed by dir/name.
type ftpServer struct {
	listener net.Listener
	user     string
	pass     string
	cert     *tls.Certificate

	mu          sync.Mutex
	dirs        map[string]bool
	files       map[string][]byte
	connections int
	stors       int
	retrs       int
	tlsControls int
	tlsDatas    int
	authFails   int
}

func startFTPServer(t *testing.T, user, pass string, cert *tls.Certificate) *ftpServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &ftpServer{
		listener: listener, user: user, pass: pass, cert: cert,
		dirs: map[string]bool{"": true}, files: map[string][]byte{},
	}
	go func() {
		for {
			sock, err := listener.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.connections++
			s.mu.Unlock()
			go s.serve(sock)
		}
	}()
	t.Cleanup(func() { listener.Close() })
	return s
}

func (s *ftpServer) addr() string { return s.listener.Addr().String() }

// snapshot returns a copy of the stored files.
func (s *ftpServer) snapshot() map[string][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]byte, len(s.files))
	for k, v := range s.files {
		out[k] = append([]byte(nil), v...)
	}
	return out
}

type ftpStats struct {
	connections int
	stors       int
	retrs       int
	tlsControls int
	tlsDatas    int
	authFails   int
	files       int
}

func (s *ftpServer) stats() ftpStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ftpStats{
		connections: s.connections, stors: s.stors, retrs: s.retrs,
		tlsControls: s.tlsControls, tlsDatas: s.tlsDatas,
		authFails: s.authFails, files: len(s.files),
	}
}

func (s *ftpServer) serve(sock net.Conn) {
	defer sock.Close()
	sock.SetDeadline(time.Now().Add(60 * time.Second))
	var conn net.Conn = sock
	writeLine := func(format string, args ...any) {
		fmt.Fprintf(conn, format+"\r\n", args...)
	}
	readLine := func() (string, bool) {
		var line []byte
		var b [1]byte
		for {
			if _, err := conn.Read(b[:]); err != nil {
				return "", false
			}
			if b[0] == '\n' {
				break
			}
			if len(line) > 4096 {
				return "", false
			}
			line = append(line, b[0])
		}
		return strings.TrimRight(string(line), "\r"), true
	}
	writeLine("220 murmur-ftp ready")
	var user string
	var authed bool
	var dir string
	var protP bool
	// pendingData holds the accepted (and TLS-upgraded, when PROT P)
	// data connection. It is accepted eagerly in the PASV handler
	// because the product client dials and handshakes immediately
	// after 227, before sending STOR/RETR.
	var pendingData net.Conn
	closePending := func() {
		if pendingData != nil {
			pendingData.Close()
			pendingData = nil
		}
	}
	defer closePending()
	needAuth := func() bool {
		if !authed {
			writeLine("530 login first")
			return false
		}
		return true
	}
	for {
		line, ok := readLine()
		if !ok {
			return
		}
		verb, arg, _ := strings.Cut(line, " ")
		switch strings.ToUpper(verb) {
		case "AUTH":
			if !strings.EqualFold(arg, "TLS") || s.cert == nil {
				writeLine("502 unsupported")
				continue
			}
			writeLine("234 upgrading")
			tlsConn := tls.Server(sock, &tls.Config{Certificates: []tls.Certificate{*s.cert}})
			if err := tlsConn.Handshake(); err != nil {
				return
			}
			conn = tlsConn
			sock.SetDeadline(time.Time{})
			tlsConn.SetDeadline(time.Now().Add(60 * time.Second))
			s.mu.Lock()
			s.tlsControls++
			s.mu.Unlock()
			// AUTH resets authentication state (RFC 2228).
			user, authed = "", false
		case "PBSZ":
			// Allowed before login (RFC 2228): the product client
			// sends PBSZ/PROT right after AUTH TLS.
			writeLine("200 ok")
		case "PROT":
			if strings.EqualFold(arg, "P") {
				protP = true
				writeLine("200 ok")
			} else {
				writeLine("502 unsupported level")
			}
		case "USER":
			user = arg
			if arg == s.user {
				writeLine("331 need password")
			} else {
				// Do not reveal unknown users; fail at PASS.
				writeLine("331 need password")
			}
		case "PASS":
			if user == s.user && arg == s.pass {
				authed = true
				writeLine("230 logged in")
			} else {
				s.mu.Lock()
				s.authFails++
				s.mu.Unlock()
				writeLine("530 login incorrect")
			}
		case "TYPE":
			if !needAuth() {
				continue
			}
			if arg == "I" {
				writeLine("200 ok")
			} else {
				writeLine("502 unsupported type")
			}
		case "CWD":
			if !needAuth() {
				continue
			}
			s.mu.Lock()
			_, exists := s.dirs[arg]
			s.mu.Unlock()
			if exists {
				dir = arg
				writeLine("250 ok")
			} else {
				writeLine("550 no such directory")
			}
		case "MKD":
			if !needAuth() {
				continue
			}
			s.mu.Lock()
			s.dirs[arg] = true
			s.mu.Unlock()
			dir = arg
			writeLine("257 created")
		case "PASV":
			if !needAuth() {
				continue
			}
			closePending()
			pl, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				writeLine("425 cannot open passive connection")
				continue
			}
			port := pl.Addr().(*net.TCPAddr).Port
			writeLine("227 Entering Passive Mode (127,0,0,1,%d,%d)", port>>8, port&0xff)
			// Accept eagerly: the product client dials (and, under
			// PROT P, handshakes TLS) immediately after 227, before
			// sending STOR/RETR.
			pl.(*net.TCPListener).SetDeadline(time.Now().Add(30 * time.Second))
			dataSock, err := pl.Accept()
			pl.Close()
			if err != nil {
				writeLine("425 data accept failed")
				continue
			}
			pendingData = dataSock
			if protP {
				if s.cert == nil {
					closePending()
					writeLine("425 no TLS context")
					continue
				}
				tlsData := tls.Server(dataSock, &tls.Config{Certificates: []tls.Certificate{*s.cert}})
				tlsData.SetDeadline(time.Now().Add(30 * time.Second))
				if err := tlsData.Handshake(); err != nil {
					closePending()
					writeLine("425 data TLS failed")
					continue
				}
				pendingData = tlsData
				s.mu.Lock()
				s.tlsDatas++
				s.mu.Unlock()
			}
		case "STOR":
			if !needAuth() {
				continue
			}
			if pendingData == nil {
				writeLine("425 use PASV first")
				continue
			}
			if arg == "" || strings.Contains(arg, "/") {
				writeLine("553 bad name")
				continue
			}
			dataConn := pendingData
			pendingData = nil
			writeLine("150 opening")
			var buf bytes.Buffer
			dataConn.SetDeadline(time.Now().Add(60 * time.Second))
			_, err := buf.ReadFrom(dataConn)
			dataConn.Close()
			if err != nil {
				writeLine("426 transfer failed")
				continue
			}
			key := arg
			if dir != "" {
				key = dir + "/" + arg
			}
			s.mu.Lock()
			s.files[key] = buf.Bytes()
			s.stors++
			s.mu.Unlock()
			writeLine("226 done")
		case "RETR":
			if !needAuth() {
				continue
			}
			if pendingData == nil {
				writeLine("425 use PASV first")
				continue
			}
			key := arg
			if dir != "" {
				key = dir + "/" + arg
			}
			s.mu.Lock()
			data, exists := s.files[key]
			s.mu.Unlock()
			if !exists {
				writeLine("550 no such file")
				continue
			}
			dataSock := pendingData
			pendingData = nil
			writeLine("150 opening")
			dataSock.SetDeadline(time.Now().Add(60 * time.Second))
			_, err := dataSock.Write(data)
			dataSock.Close()
			if err != nil {
				writeLine("426 transfer failed")
				continue
			}
			s.mu.Lock()
			s.retrs++
			s.mu.Unlock()
			writeLine("226 done")
		case "QUIT":
			writeLine("221 bye")
			return
		default:
			writeLine("502 unsupported")
		}
	}
}

// ftpCA mints a private CA plus a server certificate valid for
// 127.0.0.1 (IP SAN) and localhost (DNS SAN). It returns the server
// certificate and the CA roots PEM the FTPS client pins.
func ftpCA(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "murmur-ftp-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caKey.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	srvTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "murmur-ftp-test-server"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTemplate, ca, srvKey.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	srvKeyDER, err := x509.MarshalPKCS8PrivateKey(srvKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: srvKeyDER}),
	)
	if err != nil {
		t.Fatal(err)
	}
	rootsPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	return cert, rootsPEM
}

// ftpRetrieve downloads dir/name from a plain FTP server. The product
// has no FTP fetch path (PublishFTP is upload-only), so retrieval is
// test scaffolding; the bytes it carries are asserted end to end.
func ftpRetrieve(t *testing.T, addr, user, pass, dir, name string) []byte {
	t.Helper()
	sock, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		t.Fatalf("ftp dial: %v", err)
	}
	defer sock.Close()
	sock.SetDeadline(time.Now().Add(30 * time.Second))
	readReply := func() (int, string) {
		var lines []string
		var code int
		for {
			var line []byte
			var b [1]byte
			for {
				if _, err := sock.Read(b[:]); err != nil {
					t.Fatalf("ftp read: %v", err)
				}
				if b[0] == '\n' {
					break
				}
				line = append(line, b[0])
			}
			s := strings.TrimRight(string(line), "\r")
			if len(s) < 4 {
				t.Fatalf("ftp bad reply %q", s)
			}
			v, err := strconv.Atoi(s[:3])
			if err != nil {
				t.Fatalf("ftp bad reply %q", s)
			}
			if code == 0 {
				code = v
			}
			lines = append(lines, s)
			if s[3] != '-' {
				return code, strings.Join(lines, "\n")
			}
		}
	}
	send := func(want int, format string, args ...any) string {
		fmt.Fprintf(sock, format+"\r\n", args...)
		code, text := readReply()
		if code != want {
			t.Fatalf("ftp %s: got %d, want %d (%s)", fmt.Sprintf(format, args...), code, want, text)
		}
		return text
	}
	if code, _ := readReply(); code != 220 {
		t.Fatalf("ftp greeting: got %d, want 220", code)
	}
	if code, _ := func() (int, string) {
		fmt.Fprintf(sock, "USER %s\r\n", user)
		return readReply()
	}(); code == 230 {
		_ = code
	} else if code == 331 {
		send(230, "PASS %s", pass)
	} else {
		t.Fatalf("ftp USER: got %d", code)
	}
	send(200, "TYPE I")
	send(250, "CWD %s", dir)
	msg := send(227, "PASV")
	open := strings.Index(msg, "(")
	closeIdx := strings.Index(msg, ")")
	if open < 0 || closeIdx < 0 {
		t.Fatalf("ftp bad PASV %q", msg)
	}
	var nums []int
	for _, part := range strings.Split(msg[open+1:closeIdx], ",") {
		v, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			t.Fatalf("ftp bad PASV %q", msg)
		}
		nums = append(nums, v)
	}
	if len(nums) != 6 {
		t.Fatalf("ftp bad PASV %q", msg)
	}
	dataSock, err := net.DialTimeout("tcp",
		fmt.Sprintf("%d.%d.%d.%d:%d", nums[0], nums[1], nums[2], nums[3], nums[4]<<8|nums[5]),
		10*time.Second)
	if err != nil {
		t.Fatalf("ftp data dial: %v", err)
	}
	fmt.Fprintf(sock, "RETR %s\r\n", name)
	code, _ := readReply()
	if code != 150 && code != 125 {
		dataSock.Close()
		t.Fatalf("ftp RETR: got %d", code)
	}
	dataSock.SetDeadline(time.Now().Add(30 * time.Second))
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(dataSock); err != nil {
		dataSock.Close()
		t.Fatalf("ftp data read: %v", err)
	}
	dataSock.Close()
	code, _ = readReply()
	if code != 226 && code != 250 {
		t.Fatalf("ftp RETR done: got %d", code)
	}
	fmt.Fprintf(sock, "QUIT\r\n")
	return buf.Bytes()
}

// listBundles returns the sorted *.bundle file paths under dir.
func listBundles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read staging dir: %v", err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".bundle") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(out)
	return out
}

// publishAll uploads every path to dir on the FTP server at addr,
// concurrently from both Low exporters (identical bytes, so overwrites
// are idempotent). tlsOpts selects the ftps:// leg; nil selects ftp://.
func publishAll(t *testing.T, low *harness.Cluster, paths []string, addr, dir string, tlsOpts *replication.FtpOptions) {
	t.Helper()
	scheme := "ftp"
	if tlsOpts != nil {
		scheme = "ftps"
	}
	// Resolve the bridges up front: nodeBridge may Fatalf, which is
	// only legal on the test goroutine.
	bridges := []*replication.Bridge{nodeBridge(t, low, 0), nodeBridge(t, low, 1)}
	var wg sync.WaitGroup
	errs := make(chan error, 2*len(paths))
	for _, b := range bridges {
		for _, path := range paths {
			wg.Add(1)
			go func(b *replication.Bridge, path string) {
				defer wg.Done()
				url := scheme + "://uploader:s3cret@" + addr + "/" + dir + "/"
				if err := b.PublishFTP(path, url, tlsOpts); err != nil {
					errs <- err
				}
			}(b, path)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("publish: %v", err)
	}
}

// fetchInto retrieves every named bundle from dir on the FTP server
// into staging, and returns the retrieved bytes keyed by file name.
func fetchInto(t *testing.T, srv *ftpServer, names []string, dir, staging string) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte, len(names))
	for _, name := range names {
		data := ftpRetrieve(t, srv.addr(), "uploader", "s3cret", dir, name)
		if err := os.WriteFile(filepath.Join(staging, name), data, 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		out[name] = data
	}
	return out
}

func TestFileBridgeOverFTP(t *testing.T) {
	lowStaging := filepath.Join(harness.RuntimeDir(lowName), "staging")
	highStaging := filepath.Join(harness.RuntimeDir(highName), "staging")
	if err := os.MkdirAll(lowStaging, 0o700); err != nil {
		t.Fatalf("mkdir low staging: %v", err)
	}
	if err := os.MkdirAll(highStaging, 0o700); err != nil {
		t.Fatalf("mkdir high staging: %v", err)
	}
	fetchInterval := 100 * time.Millisecond
	maxBytes := uint64(4 << 20)
	newExporter := func() *replication.BridgeConfig {
		return replication.NewBridgeExporter(lowDBID, lowStaging, stream, lowSigner,
			replication.RecipientPublic(&highSecret))
	}
	low, err := harness.NewCluster(harness.ClusterOptions{
		Name:       lowName,
		NumNodes:   2,
		DatabaseID: &lowDBID,
		NodeIDs:    [][16]byte{lowNode0, lowNode1},
		Key:        lowKey,
		Files: &harness.FilesOptions{
			Key:           lowObjectKey,
			FetchInterval: &fetchInterval,
			MaxFileBytes:  &maxBytes,
		},
		Bridge: harness.BridgeOptions{
			// Both Low nodes export so both can publish.
			Nodes: map[int]*replication.BridgeConfig{0: newExporter(), 1: newExporter()},
		},
	})
	if err != nil {
		t.Fatalf("low cluster setup: %v", err)
	}
	// The High importer pins the Low bundle signer and trusts both Low
	// origin keys so imported file transactions verify; the seeds stay
	// inside the harness.
	lowOrigin0 := low.NodeOriginPubkey(0)
	lowOrigin1 := low.NodeOriginPubkey(1)
	high, err := harness.NewCluster(harness.ClusterOptions{
		Name:       highName,
		NumNodes:   2,
		DatabaseID: &highDBID,
		NodeIDs:    [][16]byte{highNode0, highNode1},
		Key:        highKey,
		Files: &harness.FilesOptions{
			Key:           highObjectKey,
			FetchInterval: &fetchInterval,
			MaxFileBytes:  &maxBytes,
		},
		Bridge: harness.BridgeOptions{
			Nodes: map[int]*replication.BridgeConfig{
				0: replication.NewBridgeReceiver(highDBID, highStaging, stream, highSecret,
					map[[16]byte]bool{lowDBID: true},
					map[[16]byte][32]byte{lowNode0: replication.PublicKey(&lowSigner)},
					map[[16]byte][32]byte{lowNode0: lowOrigin0, lowNode1: lowOrigin1},
				),
			},
		},
	})
	if err != nil {
		t.Fatalf("high cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			low.DumpForensics("bridge-ftp-low")
			high.DumpForensics("bridge-ftp-high")
		}
		low.Finish(failed)
		high.Finish(failed)
	}()

	// One server handles the plain leg; a second, TLS-capable server
	// handles the FTPS leg. Both start empty.
	plain := startFTPServer(t, "uploader", "s3cret", nil)
	cert, rootsPEM := ftpCA(t)
	secure := startFTPServer(t, "uploader", "s3cret", &cert)

	// Leg 1 (plain FTP): upload, Low mesh, export, concurrent publish.
	dataOne := payload(fileOneBytes, 0)
	upOne, err := low.Node(0).UploadFile(fileOne, bytes.NewReader(dataOne))
	if err != nil {
		t.Fatalf("low upload one: %v", err)
	}
	waitMetadata(t, low, 1, fileOne, upOne.Digest, "low mesh metadata one")
	waitAvailable(t, low, 1, fileOne, "low peer fetch one")
	assertNodeBytes(t, low, 1, fileOne, dataOne, upOne.Digest)
	if err := nodeBridge(t, low, 0).ExportFile(fileOne); err != nil {
		t.Fatalf("bridge export one: %v", err)
	}
	if _, err := nodeBridge(t, low, 0).Capture(); err != nil {
		t.Fatalf("low capture after export one: %v", err)
	}
	if _, err := nodeBridge(t, low, 1).Capture(); err != nil {
		t.Fatalf("low-1 capture after export one: %v", err)
	}
	stagedOne := listBundles(t, lowStaging)
	if len(stagedOne) < 2 {
		t.Fatalf("staged %d bundles, want file chunks plus metadata", len(stagedOne))
	}
	publishAll(t, low, stagedOne, plain.addr(), "dropzone", nil)
	// The server holds exactly the published set, byte-identical, with
	// no document plaintext.
	stored := plain.snapshot()
	if len(stored) != len(stagedOne) {
		t.Fatalf("server holds %d files, want %d", len(stored), len(stagedOne))
	}
	var namesOne []string
	for _, path := range stagedOne {
		name := filepath.Base(path)
		namesOne = append(namesOne, name)
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read staged %s: %v", path, err)
		}
		got, ok := stored["dropzone/"+name]
		if !ok {
			t.Fatalf("server missing %s", name)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("server bytes differ for %s", name)
		}
	}
	windowsOne := [][]byte{dataOne[0:1024], dataOne[100000:101024], dataOne[300000:301024]}
	for key, raw := range stored {
		for i, w := range windowsOne {
			if contains(raw, w) {
				t.Fatalf("server %s contains plaintext window %d", key, i)
			}
		}
	}
	if stats := plain.stats(); stats.stors < 2*len(stagedOne) {
		t.Fatalf("server recorded %d STORs, want at least %d (two publishers)", stats.stors, 2*len(stagedOne))
	}

	// Retrieve into the High staging area and import: metadata applies,
	// chunks install, and the object re-encrypts under the High key.
	for name, data := range fetchInto(t, plain, namesOne, "dropzone", highStaging) {
		want, err := os.ReadFile(filepath.Join(lowStaging, name))
		if err != nil {
			t.Fatalf("read staged %s: %v", name, err)
		}
		if !bytes.Equal(data, want) {
			t.Fatalf("retrieved %s differs", name)
		}
	}
	imported := harness.WaitUntil(wait, poll, func() bool {
		pump(t, low, high)
		available, err := high.Node(0).FileAvailable(fileOne)
		return err == nil && available
	})
	if !imported {
		t.Fatal("high-0 import one never completed")
	}
	assertNodeBytes(t, high, 0, fileOne, dataOne, upOne.Digest)
	assertReencrypted(t, low, high)
	waitMetadata(t, high, 1, fileOne, upOne.Digest, "high mesh metadata one")
	waitAvailable(t, high, 1, fileOne, "high-1 peer fetch one")
	assertNodeBytes(t, high, 1, fileOne, dataOne, upOne.Digest)

	// Leg 2 (FTPS): a second file crosses the TLS leg with a pinned CA.
	// The server must observe TLS on control and data connections.
	dataTwo := payload(fileTwoBytes, 1)
	upTwo, err := low.Node(0).UploadFile(fileTwo, bytes.NewReader(dataTwo))
	if err != nil {
		t.Fatalf("low upload two: %v", err)
	}
	waitMetadata(t, low, 1, fileTwo, upTwo.Digest, "low mesh metadata two")
	waitAvailable(t, low, 1, fileTwo, "low peer fetch two")
	if err := nodeBridge(t, low, 0).ExportFile(fileTwo); err != nil {
		t.Fatalf("bridge export two: %v", err)
	}
	if _, err := nodeBridge(t, low, 0).Capture(); err != nil {
		t.Fatalf("low capture after export two: %v", err)
	}
	before := map[string]bool{}
	for _, path := range stagedOne {
		before[filepath.Base(path)] = true
	}
	var stagedTwo []string
	for _, path := range listBundles(t, lowStaging) {
		if !before[filepath.Base(path)] {
			stagedTwo = append(stagedTwo, path)
		}
	}
	if len(stagedTwo) == 0 {
		t.Fatal("no new bundles for file two")
	}
	ftpsOpts := &replication.FtpOptions{RootsPEM: rootsPEM, Timeout: 30 * time.Second}
	publishAll(t, low, stagedTwo, secure.addr(), "secure-zone", ftpsOpts)
	stats := secure.stats()
	if stats.tlsControls == 0 || stats.tlsDatas == 0 {
		t.Fatalf("ftps leg never used TLS: %+v", stats)
	}
	for _, path := range stagedTwo {
		name := filepath.Base(path)
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read staged %s: %v", path, err)
		}
		got := ftpRetrieve(t, secure.addr(), "uploader", "s3cret", "secure-zone", name)
		if !bytes.Equal(got, want) {
			t.Fatalf("ftps server bytes differ for %s", name)
		}
		if err := os.WriteFile(filepath.Join(highStaging, name), got, 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	importedTwo := harness.WaitUntil(wait, poll, func() bool {
		pump(t, low, high)
		available, err := high.Node(0).FileAvailable(fileTwo)
		return err == nil && available
	})
	if !importedTwo {
		t.Fatal("high-0 import two never completed")
	}
	assertNodeBytes(t, high, 0, fileTwo, dataTwo, upTwo.Digest)
	waitMetadata(t, high, 1, fileTwo, upTwo.Digest, "high mesh metadata two")
	waitAvailable(t, high, 1, fileTwo, "high-1 peer fetch two")
	assertNodeBytes(t, high, 1, fileTwo, dataTwo, upTwo.Digest)

	// Search across the replicated metadata finds both files.
	found, err := high.Node(1).SearchFiles("shared/")
	if err != nil {
		t.Fatalf("high search: %v", err)
	}
	if len(found) != 2 {
		t.Fatalf("search found %d files, want two", len(found))
	}
	byDigest := map[[32]byte]bool{}
	for _, f := range found {
		byDigest[f.Digest] = true
	}
	if !byDigest[upOne.Digest] || !byDigest[upTwo.Digest] {
		t.Fatal("search digests mismatch")
	}

	// Leg 3 (auth failure): a wrong password surfaces an error and
	// stores nothing.
	filesBefore := plain.stats().files
	badURL := "ftp://uploader:wrong@" + plain.addr() + "/dropzone/"
	if err := nodeBridge(t, low, 0).PublishFTP(stagedOne[0], badURL, nil); err == nil {
		t.Fatal("wrong-password publish succeeded")
	}
	if stats := plain.stats(); stats.files != filesBefore || stats.authFails == 0 {
		t.Fatalf("auth failure stored data or went unrecorded: %+v", stats)
	}

	// Leg 4 (HTTP): one sealed bundle crosses over HTTP PUT. Replaying
	// the PUT bytes through the High importer is a no-op: both files
	// still download byte-identical afterwards (double-import
	// idempotency).
	var putPath string
	var putBytes []byte
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var buf bytes.Buffer
		buf.ReadFrom(r.Body)
		putPath, putBytes = r.URL.Path, buf.Bytes()
	}))
	defer httpServer.Close()
	httpName := filepath.Base(stagedOne[0])
	if err := nodeBridge(t, low, 0).PublishHTTP(stagedOne[0], httpServer.URL+"/drop/"+httpName); err != nil {
		t.Fatalf("http publish: %v", err)
	}
	if putPath != "/drop/"+httpName {
		t.Fatalf("http PUT path %q, want %q", putPath, "/drop/"+httpName)
	}
	wantHTTP, err := os.ReadFile(stagedOne[0])
	if err != nil {
		t.Fatalf("read staged %s: %v", httpName, err)
	}
	if !bytes.Equal(putBytes, wantHTTP) {
		t.Fatal("http PUT bytes differ")
	}
	if err := nodeBridge(t, low, 0).PublishHTTP(stagedOne[0], "ftp://x"); err == nil {
		t.Fatal("http publish accepted an ftp URL")
	}
	if err := os.WriteFile(filepath.Join(highStaging, httpName), putBytes, 0o600); err != nil {
		t.Fatalf("write %s: %v", httpName, err)
	}
	// The duplicate must process (or already be processed) without
	// corrupting either file; progress is proven by file state.
	pump(t, low, high)
	assertNodeBytes(t, high, 0, fileOne, dataOne, upOne.Digest)
	assertNodeBytes(t, high, 1, fileOne, dataOne, upOne.Digest)
	assertNodeBytes(t, high, 0, fileTwo, dataTwo, upTwo.Digest)
	assertNodeBytes(t, high, 1, fileTwo, dataTwo, upTwo.Digest)
	failed = false
}

// assertReencrypted verifies High at-rest bytes differ from Low at-rest
// bytes for the same verified content (separate domain object keys).
func assertReencrypted(t *testing.T, low, high *harness.Cluster) {
	t.Helper()
	lowObj := firstObjectFile(t, low.NodeInfo(0).Dir)
	highObj := firstObjectFile(t, high.NodeInfo(0).Dir)
	lowRaw, err := os.ReadFile(lowObj)
	if err != nil {
		t.Fatalf("read %s: %v", lowObj, err)
	}
	highRaw, err := os.ReadFile(highObj)
	if err != nil {
		t.Fatalf("read %s: %v", highObj, err)
	}
	if bytes.Equal(lowRaw, highRaw) {
		t.Fatal("high object bytes identical to low object bytes; expected re-encryption")
	}
}

// firstObjectFile is the first *.spfo object file under a node dir.
func firstObjectFile(t *testing.T, root string) string {
	t.Helper()
	var found string
	stack := []string{root}
	for len(stack) > 0 {
		dir := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
		for _, e := range entries {
			path := filepath.Join(dir, e.Name())
			if e.IsDir() {
				stack = append(stack, path)
			} else if found == "" && strings.HasSuffix(e.Name(), ".spfo") {
				found = path
			}
		}
	}
	if found == "" {
		t.Fatalf("no object file under %s", root)
	}
	return found
}

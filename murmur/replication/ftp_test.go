package replication

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFTPRoots(t *testing.T) {
	pemBytes := testFTPCert(t)
	if _, err := ftpTLSConfig(pemBytes, "localhost"); err != nil {
		t.Fatal(err)
	}
	doubled := append(append([]byte(" \r\n"), pemBytes...), pemBytes...)
	doubled = append(doubled, '\t')
	if _, err := ftpTLSConfig(doubled, "localhost"); err != nil {
		t.Fatal(err)
	}
	bad := []string{
		"",
		"junk" + string(pemBytes),
		string(pemBytes) + "junk",
		strings.Replace(string(pemBytes), "CERTIFICATE", "PRIVATE KEY", -1),
		strings.Replace(string(pemBytes), "-----END CERTIFICATE-----", "", 1),
		"-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----",
	}
	for _, s := range bad {
		if _, err := ftpTLSConfig([]byte(s), "localhost"); err == nil {
			t.Fatalf("accepted %q", s[:20])
		}
	}
}

func testFTPCert(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		DNSNames: []string{"localhost"}, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func testFTPCA(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1),
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		BasicConstraintsValid: true, IsCA: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		t.Fatal(err)
	}
	return cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

type ftpCapture struct {
	user, pass, dir, file string
	bytes                 []byte
	cwdCount              int
	tlsControl, tlsData   bool
}

type ftpScript struct {
	tls         *tls.Certificate
	rejectPass  bool
	failFirstCW bool
}

func spawnFTPServer(t *testing.T, script ftpScript) (string, *ftpCapture, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cap := &ftpCapture{}
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		defer close(done)
		sock, err := listener.Accept()
		if err != nil {
			return
		}
		defer sock.Close()
		sock.SetDeadline(time.Now().Add(10 * time.Second))
		var conn net.Conn = sock
		writeLine := func(s string) {
			fmt.Fprintf(conn, "%s\r\n", s)
		}
		readLine := func() string {
			line, err := bufio.NewReader(conn).ReadString('\n')
			if err != nil {
				panic(err)
			}
			return strings.TrimRight(line, "\r\n")
		}
		// Multiline banner exercises the client's reply reader.
		writeLine("220-fake ftp")
		writeLine("220 ready")
		if script.tls != nil {
			if got := readLine(); got != "AUTH TLS" {
				panic("want AUTH TLS, got " + got)
			}
			writeLine("234 go ahead")
			tlsConn := tls.Server(sock, &tls.Config{Certificates: []tls.Certificate{*script.tls}})
			if err := tlsConn.Handshake(); err != nil {
				return // client aborted (negative test)
			}
			conn = tlsConn
			mu.Lock()
			cap.tlsControl = true
			mu.Unlock()
		}
		if script.tls != nil {
			if got := readLine(); got != "PBSZ 0" {
				panic("want PBSZ 0, got " + got)
			}
			writeLine("200 pbsz")
			if got := readLine(); got != "PROT P" {
				panic("want PROT P, got " + got)
			}
			writeLine("200 prot")
		}
		line := readLine()
		if !strings.HasPrefix(line, "USER ") {
			panic("want USER, got " + line)
		}
		mu.Lock()
		cap.user = strings.TrimPrefix(line, "USER ")
		mu.Unlock()
		writeLine("331 password please")
		line = readLine()
		if !strings.HasPrefix(line, "PASS ") {
			panic("want PASS, got " + line)
		}
		mu.Lock()
		cap.pass = strings.TrimPrefix(line, "PASS ")
		mu.Unlock()
		if script.rejectPass {
			writeLine("530 bad password")
			return
		}
		writeLine("230 logged in")
		if got := readLine(); got != "TYPE I" {
			panic("want TYPE I, got " + got)
		}
		writeLine("200 type set")
		firstCWD := true
		for {
			line = readLine()
			if dir, ok := strings.CutPrefix(line, "CWD "); ok {
				mu.Lock()
				cap.cwdCount++
				cap.dir = dir
				mu.Unlock()
				if script.failFirstCW && firstCWD {
					firstCWD = false
					writeLine("550 no such dir")
					continue
				}
				writeLine("250 cwd ok")
			} else if strings.HasPrefix(line, "MKD ") {
				writeLine("257 mkd ok")
			} else if line == "PASV" {
				break
			} else {
				panic("unexpected command " + line)
			}
		}
		dataListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(err)
		}
		dataPort := dataListener.Addr().(*net.TCPAddr).Port
		writeLine(fmt.Sprintf("227 Entering Passive Mode (127,0,0,1,%d,%d).", dataPort>>8, dataPort&0xff))
		dataSock, err := dataListener.Accept()
		if err != nil {
			panic(err)
		}
		var data net.Conn = dataSock
		if script.tls != nil {
			tlsData := tls.Server(dataSock, &tls.Config{Certificates: []tls.Certificate{*script.tls}})
			if err := tlsData.Handshake(); err != nil {
				panic(err)
			}
			data = tlsData
			mu.Lock()
			cap.tlsData = true
			mu.Unlock()
		}
		line = readLine()
		name, ok := strings.CutPrefix(line, "STOR ")
		if !ok {
			panic("want STOR, got " + line)
		}
		mu.Lock()
		cap.file = name
		mu.Unlock()
		writeLine("150 opening data")
		var payload bytes.Buffer
		buf := make([]byte, 32<<10)
		for {
			n, err := data.Read(buf)
			payload.Write(buf[:n])
			if err != nil {
				break
			}
		}
		mu.Lock()
		cap.bytes = payload.Bytes()
		mu.Unlock()
		data.Close()
		writeLine("226 transfer complete")
		if got := readLine(); got != "QUIT" {
			panic("want QUIT, got " + got)
		}
		writeLine("221 bye")
	}()
	return listener.Addr().String(), cap, func() {
		<-done
		listener.Close()
	}
}

func testExporter(t *testing.T, dir string) *Bridge {
	t.Helper()
	cfg := NewBridgeExporter(idOf(1), dir, "s", fill32(30), RecipientPublicForBridge(fill32(40)))
	b, err := OpenBridge(*cfg, newTestBackend())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestFTPPlainRoundTrip(t *testing.T) {
	dir := t.TempDir()
	bridge := testExporter(t, dir)
	payload := bytesRepeat(0xAB, 300_000)
	file := filepath.Join(dir, "node-00000000000000000001.bundle")
	if err := os.WriteFile(file, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	addr, cap, wait := spawnFTPServer(t, ftpScript{failFirstCW: true})
	defer wait()
	if err := bridge.PublishFTP(file, "ftp://uploader:s3cret@"+addr+"/dropzone/", nil); err != nil {
		t.Fatal(err)
	}
	if cap.user != "uploader" || cap.pass != "s3cret" || cap.dir != "dropzone" ||
		cap.file != "node-00000000000000000001.bundle" || !bytes.Equal(cap.bytes, payload) ||
		cap.cwdCount != 2 || cap.tlsControl || cap.tlsData {
		t.Fatalf("capture = %+v", cap)
	}
}

func TestFTPSRoundTrip(t *testing.T) {
	dir := t.TempDir()
	bridge := testExporter(t, dir)
	cert, pemBytes := testFTPCA(t)
	payload := []byte("sealed bundle bytes")
	file := filepath.Join(dir, "node-99.bundle")
	if err := os.WriteFile(file, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	addr, cap, wait := spawnFTPServer(t, ftpScript{tls: &cert})
	defer wait()
	opts := &FtpOptions{RootsPEM: pemBytes, Timeout: 10 * time.Second}
	if err := bridge.PublishFTP(file, "ftps://"+addr+"/secure-zone/remote-name.bundle", opts); err != nil {
		t.Fatal(err)
	}
	if cap.user != "anonymous" || cap.pass != "anonymous@" || cap.dir != "secure-zone" ||
		cap.file != "remote-name.bundle" || !bytes.Equal(cap.bytes, payload) ||
		!cap.tlsControl || !cap.tlsData {
		t.Fatalf("capture = %+v", cap)
	}
}

func TestFTPSWrongCA(t *testing.T) {
	dir := t.TempDir()
	bridge := testExporter(t, dir)
	cert, _ := testFTPCA(t)
	_, otherPEM := testFTPCA(t)
	file := filepath.Join(dir, "node-1.bundle")
	if err := os.WriteFile(file, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	addr, _, wait := spawnFTPServer(t, ftpScript{tls: &cert})
	defer wait()
	opts := &FtpOptions{RootsPEM: otherPEM, Timeout: 10 * time.Second}
	err := bridge.PublishFTP(file, "ftps://"+addr, opts)
	var rerr *Error
	if !errors.As(err, &rerr) || rerr.Kind != KindUnavailable {
		t.Fatalf("err = %v", err)
	}
}

func TestFTPBadPassword(t *testing.T) {
	dir := t.TempDir()
	bridge := testExporter(t, dir)
	file := filepath.Join(dir, "node-1.bundle")
	if err := os.WriteFile(file, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	addr, _, wait := spawnFTPServer(t, ftpScript{rejectPass: true})
	defer wait()
	err := bridge.PublishFTP(file, "ftp://u:wrong@"+addr, nil)
	var rerr *Error
	if !errors.As(err, &rerr) || rerr.Kind != KindAuth {
		t.Fatalf("err = %v", err)
	}
}

func TestFTPValidation(t *testing.T) {
	dir := t.TempDir()
	bridge := testExporter(t, dir)
	file := filepath.Join(dir, "node-1.bundle")
	if err := os.WriteFile(file, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	tls := &FtpOptions{RootsPEM: []byte("bogus"), Timeout: 2 * time.Second}
	for _, url := range []string{
		"http://127.0.0.1/bundle",
		"ftp://",
		"ftp:///only-path",
		"ftp://:21/x",
		"ftp://127.0.0.1:bad/x",
		"ftp://127.0.0.1/../escape",
		"ftp://127.0.0.1/a/./b",
		"ftp://@127.0.0.1/x",
		"ftp://127.0.0.1/bad name!",
	} {
		err := bridge.PublishFTP(file, url, nil)
		var rerr *Error
		if !errors.As(err, &rerr) || rerr.Kind != KindInvalid {
			t.Fatalf("%s: err = %v", url, err)
		}
	}
	err := bridge.PublishFTP(file, "ftps://127.0.0.1/x", nil)
	var rerr *Error
	if !errors.As(err, &rerr) || rerr.Kind != KindInvalid {
		t.Fatalf("ftps-without-tls err = %v", err)
	}
	err = bridge.PublishFTP(file, "ftp://127.0.0.1/x", tls)
	if !errors.As(err, &rerr) || rerr.Kind != KindInvalid {
		t.Fatalf("ftp-with-tls err = %v", err)
	}
	// Receiver role cannot publish.
	rxCfg := NewBridgeReceiver(idOf(1), dir, "s", fill32(40), map[NodeId]bool{idOf(2): true}, nil, nil)
	rx, err := OpenBridge(*rxCfg, newTestBackend())
	if err != nil {
		t.Fatal(err)
	}
	if err := rx.PublishFTP(file, "ftp://127.0.0.1/x", nil); err == nil {
		t.Fatal("receiver published")
	}
	// Empty and oversized payloads.
	empty := filepath.Join(dir, "empty.bundle")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := bridge.PublishFTP(empty, "ftp://127.0.0.1/x", nil); err == nil {
		t.Fatal("empty published")
	}
	limited := *NewBridgeExporter(idOf(1), filepath.Join(dir, "limited"), "s", fill32(30), RecipientPublicForBridge(fill32(40)))
	limited.MaxBundleBytes = 2
	small, err := OpenBridge(limited, newTestBackend())
	if err != nil {
		t.Fatal(err)
	}
	err = small.PublishFTP(file, "ftp://127.0.0.1/x", nil)
	if !errors.As(err, &rerr) || rerr.Kind != KindOverloaded {
		t.Fatalf("oversize err = %v", err)
	}
	// Unreachable host is a transport failure.
	if err := bridge.PublishFTP(file, "ftp://127.0.0.1:1/x", nil); err == nil {
		t.Fatal("unreachable accepted")
	} else if !errors.As(err, &rerr) || rerr.Kind != KindUnavailable {
		t.Fatalf("unreachable err = %v", err)
	}
}

func TestFTPServerName(t *testing.T) {
	for _, host := range []string{"localhost", "a-b.example.com", "x1.y2"} {
		name, err := ftpServerName(host)
		if err != nil || name != host {
			t.Fatalf("%s = %q, %v", host, name, err)
		}
	}
	if name, err := ftpServerName("127.0.0.1"); err != nil || name != "127.0.0.1" {
		t.Fatalf("ip = %q, %v", name, err)
	}
	invalid := []string{
		"",
		"bad_host",
		"-leading.example.com",
		"trailing-.example.com",
		"a..b.example.com",
		".leading.example.com",
		"example.com.",
		strings.Repeat("a", 64) + ".example.com",
		strings.Repeat("a", 250) + ".com",
		"white space.com",
		"bad!char.com",
	}
	for _, host := range invalid {
		if _, err := ftpServerName(host); err == nil {
			t.Fatalf("%q accepted", host)
		}
	}
	// Wired into uploads: an invalid FTPS host is rejected before any
	// connection attempt (no roots needed to trip it).
	err := FtpUpload("ftps://bad_host!/x", "d", []byte("data"), &FtpOptions{})
	var rerr *Error
	if !errors.As(err, &rerr) || rerr.Kind != KindInvalid {
		t.Fatalf("upload err = %v", err)
	}
}

package server

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/auth"
	"github.com/marcgauthier/murmur-sql/config"
	"github.com/marcgauthier/murmur-sql/engine"
)

// fixtureEngine is a protocol fixture, not a database implementation. Each SQL
// string has a fixed response; it is never compiled into either production CLI.
type fixtureEngine struct {
	mu             sync.Mutex
	key            [32]byte
	passphrase     string
	opened, closed bool
	writes         int
	disconnected   []uint64
	commands       []engine.Command
	sink           func(map[string]bool) error
	lastSQL        string
	lastParams     []engine.Value
}

func (f *fixtureEngine) Open(ctx context.Context, r engine.OpenRequest) (engine.Database, engine.OpenOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opened = true
	f.key = r.Key
	f.passphrase = r.Passphrase
	f.sink = r.TableFlagsSink
	return f, engine.OpenedNew, nil
}
func (f *fixtureEngine) Admin(context.Context, engine.AdminRequest) (any, error) {
	return nil, engine.ErrUnavailable
}
func (f *fixtureEngine) Unlock(ctx context.Context, k [32]byte, passphrase string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if k != f.key || passphrase != f.passphrase {
		return sqlError("28000", "incorrect key")
	}
	return nil
}
func (f *fixtureEngine) Execute(ctx context.Context, sql string, args []engine.Value) (engine.Result, error) {
	f.mu.Lock()
	f.lastSQL = sql
	f.lastParams = args
	f.mu.Unlock()
	switch sql {
	case "SELECT ?1, ?2, ?3":
		return engine.Result{Columns: cols("number:i text data:b"), Rows: [][]engine.Value{{args[0], args[1], args[2]}}, Tag: "SELECT 1"}, nil
	case "SELECT rows":
		return engine.Result{Columns: cols("number:i"), Rows: [][]engine.Value{{int64(1)}, {int64(2)}, {int64(3)}}, Tag: "SELECT 3"}, nil
	case "SELECT 42":
		return engine.Result{Columns: cols("answer:i"), Rows: [][]engine.Value{{int64(42)}}, Tag: "SELECT 1"}, nil
	case "INSERT fixture":
		f.mu.Lock()
		f.writes++
		f.mu.Unlock()
		return engine.Result{Tag: "INSERT 0 1"}, nil
	case "SELECT wait":
		<-ctx.Done()
		return engine.Result{}, ctx.Err()
	}
	return engine.Result{}, sqlError("42P01", "unknown fixture table")
}
func (f *fixtureEngine) Describe(ctx context.Context, sql string, hints []uint32) (engine.Description, error) {
	switch sql {
	case "SELECT ?1, ?2, ?3":
		return engine.Description{Parameters: []uint32{engine.Int8, engine.Text, engine.Bytea}, Columns: cols("number:i text data:b")}, nil
	case "SELECT rows":
		return engine.Description{Columns: cols("number:i")}, nil
	case "SELECT 42":
		return engine.Description{Columns: cols("answer:i")}, nil
	case "INSERT fixture", "CREATE TABLE fixture(id INTEGER PRIMARY KEY)":
		return engine.Description{}, nil
	}
	return engine.Description{}, sqlError("42P01", "unknown fixture table")
}
func (f *fixtureEngine) Migrate(ctx context.Context, sql string) (engine.Result, error) {
	return engine.Result{Tag: "CREATE TABLE"}, nil
}
func (f *fixtureEngine) Command(ctx context.Context, id uint64, c engine.Command) (engine.Result, error) {
	f.mu.Lock()
	f.commands = append(f.commands, c)
	f.mu.Unlock()
	switch c.Operation {
	case "SHOW STATUS":
		return engine.Result{Columns: cols("status"), Rows: [][]engine.Value{{`{"fixture":true}`}}}, nil
	case "SET TABLE":
		if e := f.sink(map[string]bool{"stable-id": c.Arguments[1].(bool)}); e != nil {
			return engine.Result{}, e
		}
		return engine.Result{Tag: "SET"}, nil
	case "BACKUP DIRECTORY":
		dir := c.Arguments[0].(string)
		if e := os.MkdirAll(dir, 0700); e != nil {
			return engine.Result{}, e
		}
		return engine.Result{}, os.WriteFile(filepath.Join(dir, "manifest"), []byte("fixture-checkpoint"), 0600)
	case "RESTORE DIRECTORY":
		return engine.Result{Tag: "RESTORE"}, nil
	case "FLUSH":
		return engine.Result{Tag: "FLUSH"}, nil
	case "ROTATE KEY":
		match := func(v engine.Value) bool {
			switch t := v.(type) {
			case []byte:
				return len(t) == 32 && string(t) == string(f.key[:])
			case string:
				return t != "" && t == f.passphrase
			}
			return false
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if !match(c.Arguments[0]) {
			return engine.Result{}, sqlError("28000", "incorrect key")
		}
		switch t := c.Arguments[1].(type) {
		case []byte:
			copy(f.key[:], t)
			f.passphrase = ""
		case string:
			f.passphrase = t
			f.key = [32]byte{}
		default:
			return engine.Result{}, sqlError("22023", "key must be bytes or text")
		}
		return engine.Result{Tag: "ROTATE"}, nil
	}
	return engine.Result{}, engine.ErrUnavailable
}
func (f *fixtureEngine) Disconnect(id uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.disconnected = append(f.disconnected, id)
}
func (f *fixtureEngine) Close(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}
func startFixture(t *testing.T, c config.Config, f engine.Factory) (*Server, string, context.CancelFunc) {
	t.Helper()
	c.Server.Bind = "127.0.0.1:0"
	c.Auth.Users = []config.UserConfig{{Name: "admin", PasswordHash: auth.DummyHash}}
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if e := os.WriteFile(path, []byte("# fixture config\nreplication:\n  tables: {}\n"), 0600); e != nil {
		t.Fatal(e)
	}
	c.Backups.Dir = filepath.Join(dir, "backups")
	s, e := New(path, c, f, WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	if e != nil {
		t.Fatal(e)
	}
	l, e := net.Listen("tcp", c.Server.Bind)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, l) }()
	t.Cleanup(func() {
		cancel()
		select {
		case e := <-done:
			if e != nil {
				t.Error(e)
			}
		case <-time.After(5 * time.Second):
			t.Error("server did not shut down")
		}
	})
	return s, l.Addr().String(), cancel
}

type wireClient struct {
	conn        net.Conn
	r           *bufio.Reader
	pid, secret uint32
}
type message struct {
	kind byte
	body []byte
}

func connectWire(t *testing.T, addr, user, password string, tlsConfig *tls.Config) (*wireClient, error) {
	t.Helper()
	c, e := net.DialTimeout("tcp", addr, 2*time.Second)
	if e != nil {
		return nil, e
	}
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if tlsConfig != nil {
		if _, e = c.Write(put32(put32(nil, 8), 80877103)); e != nil {
			return nil, e
		}
		var response [1]byte
		if _, e = io.ReadFull(c, response[:]); e != nil {
			return nil, e
		}
		if response[0] != 'S' {
			return nil, fmt.Errorf("TLS refused")
		}
		tc := tls.Client(c, tlsConfig)
		if e = tc.Handshake(); e != nil {
			c.Close()
			return nil, e
		}
		c = tc
	}
	w := &wireClient{conn: c, r: bufio.NewReader(c)}
	startup := put32(nil, 196608)
	startup = cstr(cstr(startup, "user"), user)
	startup = cstr(cstr(startup, "database"), "murmur")
	startup = append(startup, 0)
	if _, e = c.Write(append(put32(nil, uint32(len(startup)+4)), startup...)); e != nil {
		return nil, e
	}
	m, e := w.read()
	if e != nil {
		c.Close()
		return nil, e
	}
	if m.kind == 'E' {
		c.Close()
		return nil, errors.New(errorCode(m.body))
	}
	if m.kind != 'R' {
		c.Close()
		return nil, fmt.Errorf("expected auth request")
	}
	w.send('p', cstr(nil, password))
	for {
		m, e = w.read()
		if e != nil {
			c.Close()
			return nil, e
		}
		if m.kind == 'E' {
			c.Close()
			return nil, errors.New(errorCode(m.body))
		}
		if m.kind == 'K' {
			w.pid = binary.BigEndian.Uint32(m.body)
			w.secret = binary.BigEndian.Uint32(m.body[4:])
		}
		if m.kind == 'Z' {
			break
		}
	}
	t.Cleanup(func() { w.conn.Close() })
	return w, nil
}
func (w *wireClient) send(kind byte, b []byte) error {
	raw := append([]byte{kind}, put32(nil, uint32(len(b)+4))...)
	raw = append(raw, b...)
	_, e := w.conn.Write(raw)
	return e
}
func (w *wireClient) read() (message, error) {
	k, e := w.r.ReadByte()
	if e != nil {
		return message{}, e
	}
	b, e := readLength(w.r)
	return message{k, b}, e
}
func (w *wireClient) untilReady(t *testing.T) []message {
	t.Helper()
	var out []message
	for {
		m, e := w.read()
		if e != nil {
			t.Fatal(e)
		}
		out = append(out, m)
		if m.kind == 'Z' {
			return out
		}
	}
}
func (w *wireClient) query(t *testing.T, q string) []message {
	t.Helper()
	if e := w.send('Q', cstr(nil, q)); e != nil {
		t.Fatal(e)
	}
	return w.untilReady(t)
}
func errorCode(b []byte) string {
	for len(b) > 1 {
		key := b[0]
		b = b[1:]
		i := strings.IndexByte(string(b), 0)
		if i < 0 {
			return "invalid"
		}
		value := string(b[:i])
		b = b[i+1:]
		if key == 'C' {
			return value
		}
	}
	return ""
}
func findCode(ms []message) string {
	for _, m := range ms {
		if m.kind == 'E' {
			return errorCode(m.body)
		}
	}
	return ""
}
func kinds(ms []message) string {
	var b strings.Builder
	for _, m := range ms {
		b.WriteByte(m.kind)
	}
	return b.String()
}
func mustConnect(t *testing.T, addr string) *wireClient {
	t.Helper()
	w, e := connectWire(t, addr, "admin", "murmur-dummy-unusable-login-value", nil)
	if e != nil {
		t.Fatal(e)
	}
	return w
}
func unlockFixture(t *testing.T, w *wireClient) {
	t.Helper()
	if ms := w.query(t, "MURMUR UNLOCK KEY 'fixture-secret'"); findCode(ms) != "" {
		t.Fatal(ms)
	}
}
func TestUnavailableProductionFactory(t *testing.T) {
	_, addr, _ := startFixture(t, config.DefaultConfig(), nil)
	w := mustConnect(t, addr)
	if code := findCode(w.query(t, "SELECT 42")); code != "28000" {
		t.Fatal(code)
	}
	if code := findCode(w.query(t, "MURMUR UNLOCK KEY 'secret'")); code != "0A000" {
		t.Fatal(code)
	}
	if code := findCode(w.query(t, "SELECT 42")); code != "28000" {
		t.Fatal("failed unlock granted access", code)
	}
}
func TestAuthenticationAndSessionIsolation(t *testing.T) {
	f := &fixtureEngine{}
	_, addr, _ := startFixture(t, config.DefaultConfig(), f)
	for _, u := range []string{"admin", "unknown"} {
		if _, e := connectWire(t, addr, u, "wrong", nil); e == nil || e.Error() != "28P01" {
			t.Fatal(u, e)
		}
	}
	a, b := mustConnect(t, addr), mustConnect(t, addr)
	unlockFixture(t, a)
	if code := findCode(b.query(t, "SELECT 42")); code != "28000" {
		t.Fatal(code)
	}
	if code := findCode(b.query(t, "MURMUR UNLOCK KEY 'wrong'")); code != "28000" {
		t.Fatal(code)
	}
	unlockFixture(t, b)
	if got := kinds(b.query(t, "SELECT 42; SELECT version(); MURMUR SHOW STATUS")); got != "TDCTDCTDCZ" {
		t.Fatal(got)
	}
	if code := findCode(b.query(t, "SELECT absent; INSERT fixture")); code != "42P01" {
		t.Fatal(code)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writes != 0 {
		t.Fatal("executed statement after error")
	}
}
func bindPacket(portal, stmt string, formats []uint16, values [][]byte, resultFormats []uint16) []byte {
	b := cstr(cstr(nil, portal), stmt)
	b = put16(b, uint16(len(formats)))
	for _, f := range formats {
		b = put16(b, f)
	}
	b = put16(b, uint16(len(values)))
	for _, v := range values {
		if v == nil {
			b = put32(b, ^uint32(0))
		} else {
			b = put32(b, uint32(len(v)))
			b = append(b, v...)
		}
	}
	b = put16(b, uint16(len(resultFormats)))
	for _, f := range resultFormats {
		b = put16(b, f)
	}
	return b
}
func TestExtendedBinaryParametersAndPortals(t *testing.T) {
	f := &fixtureEngine{}
	_, addr, _ := startFixture(t, config.DefaultConfig(), f)
	w := mustConnect(t, addr)
	unlockFixture(t, w)
	sql := "SELECT $1, $2, $3"
	w.send('P', put16(cstr(cstr(nil, "echo"), sql), 0))
	w.send('B', bindPacket("p", "echo", []uint16{1, 0, 1}, [][]byte{binary.BigEndian.AppendUint64(nil, 42), []byte("x'; DROP TABLE users;--"), {0, 1, 255}}, []uint16{1}))
	w.send('D', cstr([]byte{'P'}, "p"))
	w.send('E', put32(cstr(nil, "p"), 0))
	w.send('S', nil)
	ms := w.untilReady(t)
	if got := kinds(ms); got != "12TDCZ" {
		t.Fatalf("%s %s", got, findCode(ms))
	}
	for _, m := range ms {
		if m.kind == 'D' {
			p := packet{b: m.body}
			if p.u16() != 3 {
				t.Fatal("row width")
			}
			if p.u32() != 8 || binary.BigEndian.Uint64(p.read(8)) != 42 {
				t.Fatal("integer encoding")
			}
			n := p.u32()
			if string(p.read(int(n))) != "x'; DROP TABLE users;--" {
				t.Fatal("text binding")
			}
			n = p.u32()
			if string(p.read(int(n))) != string([]byte{0, 1, 255}) {
				t.Fatal("bytea binding")
			}
		}
	}
	f.mu.Lock()
	if f.lastSQL != "SELECT ?1, ?2, ?3" || f.lastParams[1] != "x'; DROP TABLE users;--" {
		t.Fatal("parameter interpolation")
	}
	f.mu.Unlock()
	w.send('P', put16(cstr(cstr(nil, "rows"), "SELECT rows"), 0))
	w.send('B', bindPacket("rows", "rows", nil, nil, nil))
	w.send('E', put32(cstr(nil, "rows"), 2))
	w.send('S', nil)
	if got := kinds(w.untilReady(t)); got != "12DDsZ" {
		t.Fatal(got)
	}
	w.send('E', put32(cstr(nil, "rows"), 2))
	w.send('S', nil)
	if got := kinds(w.untilReady(t)); got != "DCZ" {
		t.Fatal(got)
	}
	// An extended-protocol error discards messages until Sync.
	w.send('E', put32(cstr(nil, "missing"), 0))
	w.send('Q', cstr(nil, "INSERT fixture"))
	w.send('S', nil)
	ms = w.untilReady(t)
	if findCode(ms) != "34000" || kinds(ms) != "EZ" {
		t.Fatal(kinds(ms), findCode(ms))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writes != 0 {
		t.Fatal("executed while awaiting Sync")
	}
}
func TestCancelAndShutdown(t *testing.T) {
	f := &fixtureEngine{}
	_, addr, cancel := startFixture(t, config.DefaultConfig(), f)
	w := mustConnect(t, addr)
	unlockFixture(t, w)
	w.send('Q', cstr(nil, "SELECT wait"))
	time.Sleep(20 * time.Millisecond)
	c, e := net.Dial("tcp", addr)
	if e != nil {
		t.Fatal(e)
	}
	raw := put32(put32(put32(put32(nil, 16), 80877102), w.pid), w.secret)
	c.Write(raw)
	c.Close()
	ms := w.untilReady(t)
	if code := findCode(ms); code != "57014" {
		t.Fatal(code)
	}
	w.send('Q', cstr(nil, "SELECT wait"))
	cancel()
}
func TestConfigCallback(t *testing.T) {
	f := &fixtureEngine{}
	s, addr, _ := startFixture(t, config.DefaultConfig(), f)
	w := mustConnect(t, addr)
	unlockFixture(t, w)
	if code := findCode(w.query(t, "MURMUR SET TABLE widgets REPLICATED true")); code != "" {
		t.Fatal(code)
	}
	c, e := config.Load(s.path)
	if e != nil || !c.Replication.Tables["stable-id"] || !s.Config().Replication.Tables["stable-id"] {
		t.Fatal(c, e)
	}
}
func certificates(t *testing.T) (config.TlsConfig, *tls.Config) {
	t.Helper()
	dir := t.TempDir()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, e := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caPath := filepath.Join(dir, "ca.pem")
	os.WriteFile(caPath, caPEM, 0600)
	issue := func(serial int64, client bool) ([]byte, []byte) {
		k, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		cert := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
		if client {
			cert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		}
		der, e := x509.CreateCertificate(rand.Reader, cert, ca, &k.PublicKey, key)
		if e != nil {
			t.Fatal(e)
		}
		kb, e := x509.MarshalPKCS8PrivateKey(k)
		if e != nil {
			t.Fatal(e)
		}
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb})
	}
	certPEM, keyPEM := issue(2, false)
	certPath, keyPath := filepath.Join(dir, "server.pem"), filepath.Join(dir, "server.key")
	os.WriteFile(certPath, certPEM, 0600)
	os.WriteFile(keyPath, keyPEM, 0600)
	clientPEM, clientKey := issue(3, true)
	pair, e := tls.X509KeyPair(clientPEM, clientKey)
	if e != nil {
		t.Fatal(e)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	return config.TlsConfig{Enabled: true, CertFile: &certPath, KeyFile: &keyPath, CaFile: &caPath}, &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: "127.0.0.1", Certificates: []tls.Certificate{pair}}
}
func TestProductionMTLS(t *testing.T) {
	tlsCfg, clientCfg := certificates(t)
	cfg := config.DefaultConfig()
	cfg.Server.Production = true
	cfg.Server.TLS = tlsCfg
	_, addr, _ := startFixture(t, cfg, nil)
	if _, e := connectWire(t, addr, "admin", "wrong", nil); e == nil || e.Error() != "28000" {
		t.Fatal("plaintext accepted", e)
	}
	without := clientCfg.Clone()
	without.Certificates = nil
	if _, e := connectWire(t, addr, "admin", "murmur-dummy-unusable-login-value", without); e == nil {
		t.Fatal("client without certificate accepted")
	}
	w, e := connectWire(t, addr, "admin", "murmur-dummy-unusable-login-value", clientCfg)
	if e != nil {
		t.Fatal(e)
	}
	if code := findCode(w.query(t, "MURMUR UNLOCK KEY 'secret'")); code != "0A000" {
		t.Fatal(code)
	}
}

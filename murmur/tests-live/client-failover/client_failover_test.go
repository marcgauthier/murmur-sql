// Live scenario: SQL clients fail over across server outages with no
// acknowledged write lost.
//
// Two in-process murmur servers (real server.Server + engine/native over
// separate data dirs, QUIC-replicated) sit behind one TCP proxy each. A
// minimal PostgreSQL-wire client (startup, cleartext auth, simple query)
// drives the workload:
//
//   - Baseline: rows written through proxy 0 converge to server 1.
//   - Flap leg: proxy 0 goes down (server 0 itself is untouched); the
//     client reconnects through proxy 1, keeps writing, and every id
//     acknowledged before or during the outage must appear on server 1.
//   - Restart leg: server 0 shuts down fully (engine closed) and a fresh
//     Server reopens the same data dir; after the mesh reheals the client
//     fails back and the full id set converges on both servers.
//
// In-flight statements that error during an outage are abandoned with
// their id range (they may or may not have committed); only acknowledged
// ids are asserted. Client-library-specific retry behavior stays with the
// client suites; this suite pins the server side: availability across
// frontends plus replication durability of acked writes.
package clientfailover

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/auth"
	"github.com/marcgauthier/murmur-sql/config"
	"github.com/marcgauthier/murmur-sql/engine/native"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
	"github.com/marcgauthier/murmur-sql/server"
)

const (
	// tableDDL is the failover table on both servers.
	tableDDL = "CREATE TABLE failover(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
	// user and password are the scratch admin credential.
	user     = "admin"
	password = "test-password"
	// unlockKey is the scratch data key (non-empty passphrase).
	unlockKey = "failover-test-key"
	// rowsPerPhase is the acknowledged rows per workload phase.
	rowsPerPhase = 20
	// opTimeout bounds one client operation.
	opTimeout = 30 * time.Second
	// convergeTimeout bounds every convergence wait.
	convergeTimeout = 120 * time.Second
	// poll is the convergence poll interval.
	poll = 200 * time.Millisecond
)

// proxy is a TCP relay with a mutable target and a kill switch. down
// closes the listener and every active relay (refused dials + reset
// streams); up re-listens on the same address.
type proxy struct {
	mu     sync.Mutex
	addr   string
	target atomic.Value // string
	ln     net.Listener
	conns  map[net.Conn]struct{}
	closed bool
}

func newProxy(t *testing.T, target string) *proxy {
	t.Helper()
	p := &proxy{conns: map[net.Conn]struct{}{}}
	p.target.Store(target)
	p.up(t)
	return p
}

func (p *proxy) up(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = false
	bind := "127.0.0.1:0"
	if p.addr != "" {
		bind = p.addr
	}
	ln, err := net.Listen("tcp", bind)
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p.ln = ln
	p.addr = ln.Addr().String()
	go p.accept(ln)
}

func (p *proxy) accept(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		target, _ := p.target.Load().(string)
		up, err := net.DialTimeout("tcp", target, 5*time.Second)
		if err != nil {
			c.Close()
			continue
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			c.Close()
			up.Close()
			continue
		}
		p.conns[c] = struct{}{}
		p.conns[up] = struct{}{}
		p.mu.Unlock()
		go func() {
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); io.Copy(up, c) }()
			go func() { defer wg.Done(); io.Copy(c, up) }()
			wg.Wait()
			c.Close()
			up.Close()
			p.mu.Lock()
			delete(p.conns, c)
			delete(p.conns, up)
			p.mu.Unlock()
		}()
	}
}

// down breaks every relayed stream and refuses new dials.
func (p *proxy) down() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	if p.ln != nil {
		p.ln.Close()
	}
	for c := range p.conns {
		c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
}

func (p *proxy) setTarget(target string) { p.target.Store(target) }

func (p *proxy) close() { p.down() }

// wireErr is a server error with its SQLSTATE code.
type wireErr struct {
	code    string
	message string
}

func (e *wireErr) Error() string { return e.code + " " + e.message }

// wireConn is a minimal PostgreSQL-wire client: startup, cleartext
// password, simple query, terminate.
type wireConn struct {
	conn net.Conn
	r    *bufio.Reader
}

func put32(n uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], n)
	return b[:]
}

// dialWire connects and runs the startup handshake through auth.
func dialWire(addr string) (*wireConn, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	w := &wireConn{conn: conn, r: bufio.NewReader(conn)}
	startup := put32(196608)
	startup = append(startup, []byte("user")...)
	startup = append(startup, 0)
	startup = append(startup, []byte(user)...)
	startup = append(startup, 0, 0)
	packet := append(put32(uint32(len(startup)+4)), startup...)
	conn.SetDeadline(time.Now().Add(opTimeout))
	if _, err := conn.Write(packet); err != nil {
		conn.Close()
		return nil, err
	}
	for {
		kind, body, err := w.readMessage()
		if err != nil {
			conn.Close()
			return nil, err
		}
		switch kind {
		case 'R':
			if len(body) < 4 {
				conn.Close()
				return nil, fmt.Errorf("short auth response")
			}
			switch binary.BigEndian.Uint32(body) {
			case 0:
				// AuthOk: parameter statuses and backend key follow.
			case 3:
				pw := append([]byte(password), 0)
				msg := append([]byte{'p'}, append(put32(uint32(len(pw)+4)), pw...)...)
				if _, err := conn.Write(msg); err != nil {
					conn.Close()
					return nil, err
				}
			default:
				conn.Close()
				return nil, fmt.Errorf("unsupported auth kind %d", binary.BigEndian.Uint32(body))
			}
		case 'E':
			conn.Close()
			return nil, parseError(body)
		case 'Z':
			conn.SetDeadline(time.Time{})
			return w, nil
		default:
			// S (parameter status) and K (backend key): skip.
		}
	}
}

func (w *wireConn) readMessage() (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(w.r, header); err != nil {
		return 0, nil, err
	}
	n := int(binary.BigEndian.Uint32(header[1:])) - 4
	if n < 0 {
		return 0, nil, fmt.Errorf("negative message length")
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(w.r, body); err != nil {
		return 0, nil, err
	}
	return header[0], body, nil
}

// parseError extracts the SQLSTATE code and message from an ErrorResponse.
func parseError(body []byte) *wireErr {
	out := &wireErr{code: "?????"}
	for len(body) > 1 {
		field := body[0]
		body = body[1:]
		end := 0
		for end < len(body) && body[end] != 0 {
			end++
		}
		value := string(body[:end])
		if end < len(body) {
			end++
		}
		body = body[end:]
		switch field {
		case 'C':
			out.code = value
		case 'M':
			out.message = value
		}
	}
	return out
}

// run sends a simple-query message and collects data rows until Ready.
func (w *wireConn) run(sql string) ([][]string, error) {
	w.conn.SetDeadline(time.Now().Add(opTimeout))
	defer w.conn.SetDeadline(time.Time{})
	body := append([]byte(sql), 0)
	if _, err := w.conn.Write(append([]byte{'Q'}, append(put32(uint32(len(body)+4)), body...)...)); err != nil {
		return nil, err
	}
	var rows [][]string
	var cmdErr *wireErr
	for {
		kind, msg, err := w.readMessage()
		if err != nil {
			return nil, err
		}
		switch kind {
		case 'D':
			if len(msg) < 2 {
				return nil, fmt.Errorf("short data row")
			}
			n := int(binary.BigEndian.Uint16(msg))
			msg = msg[2:]
			row := make([]string, 0, n)
			for i := 0; i < n; i++ {
				if len(msg) < 4 {
					return nil, fmt.Errorf("short data field")
				}
				ln := int(int32(binary.BigEndian.Uint32(msg)))
				msg = msg[4:]
				if ln < 0 {
					row = append(row, "\x00NULL")
					continue
				}
				if len(msg) < ln {
					return nil, fmt.Errorf("short data value")
				}
				row = append(row, string(msg[:ln]))
				msg = msg[ln:]
			}
			rows = append(rows, row)
		case 'E':
			// Record the error but keep draining until Ready: the
			// trailing Z belongs to this command, and leaving it
			// unread would make the next command look successful.
			if cmdErr == nil {
				cmdErr = parseError(msg)
			}
		case 'Z':
			if cmdErr != nil {
				return nil, cmdErr
			}
			return rows, nil
		default:
			// T (description), C (tag), S, N: skip.
		}
	}
}

// exec runs a statement that must not fail.
func (w *wireConn) exec(sql string) error {
	_, err := w.run(sql)
	return err
}

func (w *wireConn) close() {
	w.conn.Write(append([]byte{'X'}, put32(4)...))
	w.conn.Close()
}

// runningServer is one live in-process server.
type runningServer struct {
	srv    *server.Server
	addr   string
	cancel context.CancelFunc
	done   chan error
}

// pemBlock PEM-encodes one DER block.
func pemBlock(t *testing.T, typ string, der []byte) []byte {
	t.Helper()
	out := pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
	if out == nil {
		t.Fatalf("pem encode %s", typ)
	}
	return out
}

// pairCreds is the shared PKI + identity material for both servers.
type pairCreds struct {
	dbHex string
	nodes []string
	seeds []string
	trust map[string]string
}

func hexID(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("id entropy: %v", err)
	}
	return hex.EncodeToString(b)
}

// makePairCreds provisions one CA, two node leaves, and mutual origin
// trust, writing each node's PEM files into its dir.
func makePairCreds(t *testing.T, dirs []string) *pairCreds {
	t.Helper()
	ca, err := harness.GenerateCa()
	if err != nil {
		t.Fatalf("ca: %v", err)
	}
	c := &pairCreds{dbHex: hexID(t, 16), trust: map[string]string{}}
	for range dirs {
		for {
			id := hexID(t, 16)
			dup := false
			for _, n := range c.nodes {
				if n == id {
					dup = true
				}
			}
			if !dup {
				c.nodes = append(c.nodes, id)
				break
			}
		}
		seed := make([]byte, 32)
		if _, err := rand.Read(seed); err != nil {
			t.Fatalf("seed entropy: %v", err)
		}
		c.seeds = append(c.seeds, hex.EncodeToString(seed))
	}
	for i := range dirs {
		seed, _ := hex.DecodeString(c.seeds[i])
		pub := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
		c.trust[c.nodes[i]] = hex.EncodeToString(pub)
	}
	caPEM := pemBlock(t, "CERTIFICATE", ca.CaDER())
	for i, dir := range dirs {
		nodeRaw, _ := hex.DecodeString(c.nodes[i])
		var nodeArr [16]byte
		copy(nodeArr[:], nodeRaw)
		issued, err := ca.Issue(nodeArr, nil)
		if err != nil {
			t.Fatalf("issue node%d: %v", i, err)
		}
		files := map[string][]byte{
			"repl-ca.pem":   caPEM,
			"repl-node.pem": pemBlock(t, "CERTIFICATE", issued.CertDER),
			"repl-key.pem":  pemBlock(t, "PRIVATE KEY", issued.KeyDER),
		}
		for name, data := range files {
			if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
				t.Fatalf("write %s: %v", name, err)
			}
		}
	}
	return c
}

// serveOne starts (or restarts) one server on an ephemeral port.
func serveOne(t *testing.T, dir string, creds *pairCreds, idx int, passHash string) *runningServer {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Server.Bind = "127.0.0.1:0"
	cfg.Storage.DataDir = filepath.Join(dir, "data")
	cfg.Storage.DatabaseID = &creds.dbHex
	nodeID := creds.nodes[idx]
	cfg.Storage.NodeID = &nodeID
	cfg.Auth.Users = []config.UserConfig{{Name: user, PasswordHash: passHash}}
	cfg.Logging.Dir = filepath.Join(dir, "logs")
	cfg.Backups.Dir = filepath.Join(dir, "backups")
	cfg.Replication.Enabled = true
	cfg.Replication.Bind = "127.0.0.1:0"
	chain := filepath.Join(dir, "repl-node.pem")
	key := filepath.Join(dir, "repl-key.pem")
	ca := filepath.Join(dir, "repl-ca.pem")
	cfg.Replication.CertChainFile = &chain
	cfg.Replication.PrivateKeyFile = &key
	cfg.Replication.CaFile = &ca
	seed := creds.seeds[idx]
	cfg.Replication.SigningKeyHex = &seed
	cfg.Replication.TrustedOrigins = map[string]string{}
	for k, v := range creds.trust {
		cfg.Replication.TrustedOrigins[k] = v
	}
	configPath := filepath.Join(dir, "config.yaml")
	if _, err := os.Stat(configPath); err != nil {
		if err := os.WriteFile(configPath, []byte("# failover fixture config\nreplication:\n  tables: {}\n"), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
	} else {
		// Restart: reload the persisted table flags the way production
		// startup does (SET TABLE saved them into this file). Without
		// this the restarted engine would silently drop the opt-in.
		raw, err := os.ReadFile(configPath)
		if err != nil {
			t.Fatalf("read config: %v", err)
		}
		loaded, err := config.Parse(raw)
		if err != nil {
			t.Fatalf("parse config: %v", err)
		}
		if loaded.Replication.Tables != nil {
			cfg.Replication.Tables = loaded.Replication.Tables
		}
		t.Logf("restart merges persisted table flags: %v", cfg.Replication.Tables)
	}
	srv, err := server.New(configPath, cfg, native.Factory{},
		server.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	if err != nil {
		t.Fatalf("server new: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()
	return &runningServer{srv: srv, addr: ln.Addr().String(), cancel: cancel, done: done}
}

// stop shuts one server down fully (engine closed) and waits for Serve.
func (r *runningServer) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil {
			t.Logf("serve exited: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("server did not shut down")
	}
}

// connect dials through a proxy and unlocks the session.
func connect(t *testing.T, addr string) *wireConn {
	t.Helper()
	w, err := dialWire(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	if err := w.exec("MURMUR UNLOCK KEY '" + unlockKey + "'"); err != nil {
		w.close()
		t.Fatalf("unlock: %v", err)
	}
	return w
}

// queryIDs returns the ordered failover ids visible on one connection.
func queryIDs(t *testing.T, w *wireConn) []int {
	t.Helper()
	rows, err := w.run("SELECT id FROM failover ORDER BY id")
	if err != nil {
		t.Fatalf("select ids: %v", err)
	}
	var out []int
	for _, r := range rows {
		id, err := strconv.Atoi(r[0])
		if err != nil {
			t.Fatalf("id %q: %v", r[0], err)
		}
		out = append(out, id)
	}
	return out
}

// waitIDs waits until one connection sees exactly the want ids.
func waitIDs(t *testing.T, w *wireConn, want []int, what string) {
	t.Helper()
	if ok := harness.WaitUntil(convergeTimeout, poll, func() bool {
		rows, err := w.run("SELECT id FROM failover ORDER BY id")
		if err != nil {
			return false
		}
		var got []int
		for _, r := range rows {
			id, err := strconv.Atoi(r[0])
			if err != nil {
				return false
			}
			got = append(got, id)
		}
		return slices.Equal(got, want)
	}); !ok {
		t.Fatalf("%s: ids did not converge on %v", what, want)
	}
}

// waitPeersConnected waits until conn reports a live peer session to
// the expected node id (SHOW PEERS columns: node_id address connected
// ...).
func waitPeersConnected(t *testing.T, w *wireConn, nodeHex, what string) {
	t.Helper()
	if ok := harness.WaitUntil(convergeTimeout, poll, func() bool {
		rows, err := w.run("MURMUR SHOW PEERS")
		if err != nil || len(rows) == 0 {
			return false
		}
		for _, r := range rows {
			if len(r) >= 3 && r[0] == nodeHex && r[2] == "true" {
				return true
			}
		}
		return false
	}); !ok {
		t.Fatalf("%s: peer %s never connected", what, nodeHex)
	}
}

// replAddr returns the replication listen address of one server.
func replAddr(t *testing.T, w *wireConn) string {
	t.Helper()
	rows, err := w.run("MURMUR SHOW REPLICATION")
	if err != nil || len(rows) != 1 || len(rows[0]) < 2 {
		t.Fatalf("show replication: %+v %v", rows, err)
	}
	if rows[0][0] != "true" {
		t.Fatalf("replication not running: %+v", rows[0])
	}
	return rows[0][1]
}

func TestClientFailoverAcrossOutages(t *testing.T) {
	runtimeDir, err := harness.PrepareRuntimeDir("client-failover")
	if err != nil {
		t.Fatalf("runtime dir: %v", err)
	}
	failed := true
	defer func() { harness.Finish("client-failover", runtimeDir, failed) }()

	dirs := []string{filepath.Join(runtimeDir, "n0"), filepath.Join(runtimeDir, "n1")}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}
	passHash, err := auth.HashPassword([]byte(password))
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	creds := makePairCreds(t, dirs)
	s0 := serveOne(t, dirs[0], creds, 0, passHash)
	s1 := serveOne(t, dirs[1], creds, 1, passHash)
	defer s0.cancel()
	defer s1.cancel()
	p0 := newProxy(t, s0.addr)
	defer p0.close()
	p1 := newProxy(t, s1.addr)
	defer p1.close()

	c0 := connect(t, p0.addr)
	defer c0.close()
	c1 := connect(t, p1.addr)
	defer c1.close()

	for _, w := range []*wireConn{c0, c1} {
		if err := w.exec(tableDDL); err != nil {
			t.Fatalf("create table: %v", err)
		}
		if err := w.exec("MURMUR SET TABLE 'failover' REPLICATED true"); err != nil {
			t.Fatalf("opt-in: %v", err)
		}
	}
	a0, a1 := replAddr(t, c0), replAddr(t, c1)
	if err := c0.exec("MURMUR PEER ADD '" + a1 + "'"); err != nil {
		t.Fatalf("peer add 0->1: %v", err)
	}
	if err := c1.exec("MURMUR PEER ADD '" + a0 + "'"); err != nil {
		t.Fatalf("peer add 1->0: %v", err)
	}
	waitPeersConnected(t, c0, creds.nodes[1], "mesh 0->1")
	waitPeersConnected(t, c1, creds.nodes[0], "mesh 1->0")

	var acked []int
	phase := func(w *wireConn, base int) {
		for n := 0; n < rowsPerPhase; n++ {
			id := base + n
			if err := w.exec("INSERT INTO failover(id, name) VALUES(" + strconv.Itoa(id) + ", 'row-" + strconv.Itoa(id) + "')"); err != nil {
				t.Fatalf("insert %d: %v", id, err)
			}
			acked = append(acked, id)
		}
	}

	// Baseline through proxy 0; server 1 must converge on every acked id.
	phase(c0, 1)
	waitIDs(t, c1, acked, "baseline")

	// Flap leg: proxy 0 dies while server 0 is untouched. Fresh dials
	// must refuse (non-vacuous outage); the client reconnects through
	// proxy 1 and every acked id must already be (or become) visible.
	p0.down()
	if c, err := net.DialTimeout("tcp", p0.addr, 2*time.Second); err == nil {
		c.Close()
		t.Fatalf("proxy 0 still accepts dials after down()")
	}
	c1b := connect(t, p1.addr)
	defer c1b.close()
	waitIDs(t, c1b, acked, "failover backlog")
	phase(c1b, 101)
	waitIDs(t, c1b, acked, "post-flap local")
	p0.up(t)

	// Restart leg: server 0 shuts down fully and a fresh Server reopens
	// the same data dir; the client fails back and the full set converges
	// on both servers.
	s0.stop(t)
	c0.close()
	s0 = serveOne(t, dirs[0], creds, 0, passHash)
	defer s0.cancel()
	p0.setTarget(s0.addr)
	c0b := connect(t, p0.addr)
	defer c0b.close()
	// Peers are runtime state: the restarted engine redials from scratch
	// (same reason live suites re-mesh after StartNode). Both repl addrs
	// are re-read because the restart rebound to fresh ports.
	a0b, a1b := replAddr(t, c0b), replAddr(t, c1b)
	if err := c0b.exec("MURMUR PEER ADD '" + a1b + "'"); err != nil {
		t.Fatalf("re-peer add 0->1: %v", err)
	}
	if err := c1b.exec("MURMUR PEER ADD '" + a0b + "'"); err != nil {
		t.Fatalf("re-peer add 1->0: %v", err)
	}
	waitPeersConnected(t, c0b, creds.nodes[1], "re-mesh 0->1")
	waitPeersConnected(t, c1b, creds.nodes[0], "re-mesh 1->0")
	phase(c0b, 201)
	waitIDs(t, c0b, acked, "failback local")
	waitIDs(t, c1b, acked, "final convergence")
	t.Logf("failover complete: %d acked ids on both servers", len(acked))
	failed = false
}

// Live scenario: every MURMUR operator command works end to end over the
// PostgreSQL wire against real servers.
//
// Two in-process murmur servers (real server.Server + engine/native over
// separate data dirs, QUIC-replicated) are driven by a minimal
// PostgreSQL-wire client. The suite walks the whole operator surface in
// dependency order:
//
//   - Lock gate: pre-UNLOCK statements fail with 28000; unknown commands
//     fail with a syntax error.
//   - SHOW STATUS / TABLES REPLICATION / COLUMNS FTS|VECTOR / METRICS
//     (+SQLITE/+SPOOL) / REPLICATION / PEERS / RETENTION return sane rows.
//   - SET TABLE ... REPLICATED toggles the flag and replicated writes
//     converge to the peer; SET COLUMN toggles FTS and VECTOR (with
//     DIMENSIONS/DISTANCE validation) and SHOW COLUMNS reflects them.
//   - FLUSH, REFRESH INDEXES, PEER SYNC, RETENTION COLLECT succeed;
//     malformed PEER ADD/REMOVE/RETIRE fail with 22023.
//   - POLICY COUNTER_ADD/MIN/MAX/SET_ADD/SET_REMOVE apply through the
//     configured merge policies and read back over SQL.
//   - BACKUP SAVE/LIST/DOWNLOAD round-trip; RESTORE FROM reverts a
//     sentinel row written after the backup.
//   - Unconfigured surfaces fail cleanly: BRIDGE RELEASE and FILE LIST
//     report 55000.
//   - ROTATE KEY validates its arguments, rekeys the store, and after a
//     full server restart only the new key unlocks the data dir.
//
// Unit tests pin the parser and executor shapes; this suite pins the
// wired behavior operators actually depend on.
package murmuops

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/auth"
	"github.com/marcgauthier/murmur-sql/config"
	"github.com/marcgauthier/murmur-sql/engine/native"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
	"github.com/marcgauthier/murmur-sql/server"
)

const (
	// opsDDL is the replicated scratch table on both servers. note is
	// the FTS target; emb (BLOB) is the vector target.
	opsDDL = "CREATE TABLE ops(id INTEGER PRIMARY KEY, note TEXT NOT NULL, emb BLOB)"
	// countersDDL carries one column per merge policy. Policy keys arrive
	// over the wire as TEXT, so the key column is TEXT; set cells must be
	// BLOB or NULL.
	countersDDL = "CREATE TABLE counters(k TEXT NOT NULL PRIMARY KEY, n INTEGER NOT NULL, lo INTEGER NOT NULL, hi INTEGER NOT NULL, tags BLOB)"
	// user and password are the scratch admin credential.
	user     = "admin"
	password = "test-password"
	// unlockKey is the scratch data key; rotatedKey replaces it in the
	// final leg.
	unlockKey  = "murmurops-test-key"
	rotatedKey = "murmurops-rotated-key"
	// opTimeout bounds one client operation.
	opTimeout = 30 * time.Second
	// convergeTimeout bounds every convergence wait.
	convergeTimeout = 120 * time.Second
	// poll is the convergence poll interval.
	poll = 200 * time.Millisecond
)

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
	cfg.Replication.MergePolicies = map[string]string{
		"counters.n":    "counter",
		"counters.lo":   "min",
		"counters.hi":   "max",
		"counters.tags": "set",
	}
	configPath := filepath.Join(dir, "config.yaml")
	if _, err := os.Stat(configPath); err != nil {
		if err := os.WriteFile(configPath, []byte("# murmur-ops fixture config\nreplication:\n  tables: {}\n"), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
	} else {
		// Restart: reload the persisted table flags the way production
		// startup does (SET TABLE saved them into this file).
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

// connect dials and unlocks the session with the given data key.
func connect(t *testing.T, addr, key string) *wireConn {
	t.Helper()
	w, err := dialWire(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	if err := w.exec("MURMUR UNLOCK KEY '" + key + "'"); err != nil {
		w.close()
		t.Fatalf("unlock: %v", err)
	}
	return w
}

// mustFail runs a statement that must fail with the given SQLSTATE code.
func mustFail(t *testing.T, w *wireConn, sql, code, what string) {
	t.Helper()
	_, err := w.run(sql)
	if err == nil {
		t.Fatalf("%s: %q unexpectedly succeeded", what, sql)
	}
	var werr *wireErr
	if !errors.As(err, &werr) {
		t.Fatalf("%s: %q error is not a wire error: %v", what, sql, err)
	}
	if werr.code != code {
		t.Fatalf("%s: %q code = %s (%s), want %s", what, sql, werr.code, werr.message, code)
	}
}

// mustExec runs a statement that must succeed.
func mustExec(t *testing.T, w *wireConn, sql, what string) {
	t.Helper()
	if err := w.exec(sql); err != nil {
		t.Fatalf("%s: %q: %v", what, sql, err)
	}
}

// queryOne runs a query that must return exactly one row.
func queryOne(t *testing.T, w *wireConn, sql, what string) []string {
	t.Helper()
	rows, err := w.run(sql)
	if err != nil {
		t.Fatalf("%s: %q: %v", what, sql, err)
	}
	if len(rows) != 1 {
		t.Fatalf("%s: %q returned %d rows, want 1", what, sql, len(rows))
	}
	return rows[0]
}

// findRow returns the first row whose column c equals want.
func findRow(rows [][]string, c int, want string) []string {
	for _, r := range rows {
		if len(r) > c && r[c] == want {
			return r
		}
	}
	return nil
}

// replAddr returns the replication listen address of one server.
func replAddr(t *testing.T, w *wireConn) string {
	t.Helper()
	row := queryOne(t, w, "MURMUR SHOW REPLICATION", "show replication")
	if len(row) < 2 || row[0] != "true" {
		t.Fatalf("replication not running: %+v", row)
	}
	return row[1]
}

// waitPeersConnected waits until conn reports a live peer session to the
// expected node id (SHOW PEERS columns: node_id address connected ...).
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

func TestMurmurOpsOverWire(t *testing.T) {
	runtimeDir, err := harness.PrepareRuntimeDir("murmur-ops")
	if err != nil {
		t.Fatalf("runtime dir: %v", err)
	}
	failed := true
	defer func() { harness.Finish("murmur-ops", runtimeDir, failed) }()

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

	// Leg 0: lock gate. Before UNLOCK every statement and every MURMUR
	// command fails with 28000; unknown commands fail with a syntax
	// error once unlocked.
	locked, err := dialWire(s0.addr)
	if err != nil {
		t.Fatalf("dial locked: %v", err)
	}
	mustFail(t, locked, "SELECT 1", "28000", "locked select")
	mustFail(t, locked, "MURMUR SHOW STATUS", "28000", "locked show")
	mustFail(t, locked, "MURMUR UNLOCK KEY ''", "28000", "empty unlock")
	locked.close()

	w0 := connect(t, s0.addr, unlockKey)
	defer w0.close()
	w1 := connect(t, s1.addr, unlockKey)
	defer w1.close()
	mustFail(t, w0, "MURMUR UNKNOWN", "42601", "unknown command")
	mustFail(t, w0, "MURMUR FLUSH NOW", "42601", "flush with args")
	mustFail(t, w0, "MURMUR SHOW EVERYTHING", "42601", "bad show")

	// Baseline schema on both nodes. The policy table comes first: the
	// engine validates every configured merge policy on each migrate.
	for i, w := range []*wireConn{w0, w1} {
		mustExec(t, w, countersDDL, "create counters")
		mustExec(t, w, opsDDL, "create ops")
		mustExec(t, w, "INSERT INTO counters(k, n, lo, hi) VALUES('k1', 0, 100, 0)", "seed counters")
		t.Logf("node %d schema ready", i)
	}

	// Leg 1: SHOW surface before replication is opted in.
	status := queryOne(t, w0, "MURMUR SHOW STATUS", "show status")
	if len(status) != 1 || !strings.HasPrefix(strings.TrimSpace(status[0]), "{") {
		t.Fatalf("show status = %+v, want one JSON row", status)
	}
	rows, err := w0.run("MURMUR SHOW TABLES REPLICATION")
	if err != nil {
		t.Fatalf("show tables: %v", err)
	}
	if r := findRow(rows, 0, "ops"); r == nil || r[1] != "false" {
		t.Fatalf("ops replication flag = %+v, want false", r)
	}
	for _, q := range []string{"MURMUR SHOW METRICS", "MURMUR SHOW METRICS SQLITE", "MURMUR SHOW METRICS SPOOL"} {
		mrows, err := w0.run(q)
		if err != nil || len(mrows) == 0 {
			t.Fatalf("%s: %d rows %v", q, len(mrows), err)
		}
		for _, r := range mrows {
			if len(r) != 3 {
				t.Fatalf("%s: row = %+v, want 3 columns", q, r)
			}
		}
	}
	sqliteRows, _ := w0.run("MURMUR SHOW METRICS SQLITE")
	for _, r := range sqliteRows {
		if r[0] != "sqlite" {
			t.Fatalf("sqlite metrics leaked subsystem %q", r[0])
		}
	}
	retRows, err := w0.run("MURMUR SHOW RETENTION")
	if err != nil {
		t.Fatalf("show retention: %v", err)
	}
	t.Logf("retention rows (unpeered): %d", len(retRows))

	// Leg 2: peer the mesh and verify SHOW PEERS / SHOW REPLICATION.
	a0, a1 := replAddr(t, w0), replAddr(t, w1)
	mustExec(t, w0, "MURMUR PEER ADD '"+a1+"'", "peer add 0->1")
	mustExec(t, w1, "MURMUR PEER ADD '"+a0+"'", "peer add 1->0")
	waitPeersConnected(t, w0, creds.nodes[1], "mesh 0->1")
	waitPeersConnected(t, w1, creds.nodes[0], "mesh 1->0")
	mustExec(t, w0, "MURMUR PEER SYNC", "peer sync")
	mustFail(t, w0, "MURMUR PEER ADD 'not-an-addr'", "22023", "bad peer addr")
	mustFail(t, w0, "MURMUR PEER REMOVE 'zzzz'", "22023", "bad node id")
	mustFail(t, w0, "MURMUR PEER RETIRE 'zzzz'", "22023", "bad retire id")

	// Leg 3: SET TABLE opt-in converges a write to the peer.
	for _, w := range []*wireConn{w0, w1} {
		mustExec(t, w, "MURMUR SET TABLE 'ops' REPLICATED true", "opt-in ops")
	}
	rows, _ = w0.run("MURMUR SHOW TABLES REPLICATION")
	if r := findRow(rows, 0, "ops"); r == nil || r[1] != "true" {
		t.Fatalf("ops replication flag after opt-in = %+v, want true", r)
	}
	mustExec(t, w0, "INSERT INTO ops(id, note) VALUES(1, 'converge-me')", "seed ops")
	if ok := harness.WaitUntil(convergeTimeout, poll, func() bool {
		got, err := w1.run("SELECT note FROM ops WHERE id = 1")
		return err == nil && len(got) == 1 && got[0][0] == "converge-me"
	}); !ok {
		t.Fatalf("replicated write never converged to peer")
	}
	mustExec(t, w0, "MURMUR SET TABLE 'ops' REPLICATED false", "opt-out ops")
	rows, _ = w0.run("MURMUR SHOW TABLES REPLICATION")
	if r := findRow(rows, 0, "ops"); r == nil || r[1] != "false" {
		t.Fatalf("ops replication flag after opt-out = %+v, want false", r)
	}
	mustExec(t, w0, "MURMUR SET TABLE 'ops' REPLICATED true", "re-opt-in ops")

	// Leg 4: SET COLUMN search flags round-trip through SHOW COLUMNS.
	ftsRows, err := w0.run("MURMUR SHOW COLUMNS FTS")
	if err != nil || findRow(ftsRows, 1, "note") == nil {
		t.Fatalf("show columns fts: %+v %v", ftsRows, err)
	}
	mustExec(t, w0, "MURMUR SET COLUMN 'ops' 'note' FTS ON", "fts on")
	ftsRows, _ = w0.run("MURMUR SHOW COLUMNS FTS")
	if r := findRow(ftsRows, 1, "note"); r == nil || r[2] != "true" {
		t.Fatalf("fts flag after on = %+v, want true", r)
	}
	mustExec(t, w0, "MURMUR SET COLUMN 'ops' 'note' FTS OFF", "fts off")
	ftsRows, _ = w0.run("MURMUR SHOW COLUMNS FTS")
	if r := findRow(ftsRows, 1, "note"); r == nil || r[2] != "false" {
		t.Fatalf("fts flag after off = %+v, want false", r)
	}
	mustExec(t, w0, "MURMUR SET COLUMN 'ops' 'emb' VECTOR ON DIMENSIONS 3 DISTANCE cosine", "vector on")
	vecRows, err := w0.run("MURMUR SHOW COLUMNS VECTOR")
	if err != nil {
		t.Fatalf("show columns vector: %v", err)
	}
	if r := findRow(vecRows, 1, "emb"); r == nil || r[2] != "true" || r[3] != "3" || r[4] != "cosine" {
		t.Fatalf("vector row after on = %+v, want true/3/cosine", r)
	}
	mustFail(t, w0, "MURMUR SET COLUMN 'ops' 'emb' VECTOR ON DIMENSIONS 0", "22023", "zero dims")
	mustFail(t, w0, "MURMUR SET COLUMN 'ops' 'emb' VECTOR ON DIMENSIONS 8193", "22023", "huge dims")
	mustFail(t, w0, "MURMUR SET COLUMN 'ops' 'emb' VECTOR ON DIMENSIONS 3 DISTANCE bogus", "22023", "bad distance")
	mustFail(t, w0, "MURMUR SET COLUMN 'ops' 'note' VECTOR ON DIMENSIONS 3", "22023", "vector on text")
	mustFail(t, w0, "MURMUR SET COLUMN 'ops' 'note' FTS MAYBE", "22023", "bad boolean")
	mustExec(t, w0, "MURMUR SET COLUMN 'ops' 'emb' VECTOR OFF", "vector off")

	// Leg 5: maintenance commands.
	mustExec(t, w0, "MURMUR FLUSH", "flush")
	mustExec(t, w0, "MURMUR REFRESH INDEXES", "refresh")
	mustExec(t, w0, "MURMUR RETENTION COLLECT", "retention collect")

	// Leg 6: POLICY ops through the configured merge policies.
	mustExec(t, w0, "MURMUR POLICY COUNTER_ADD 'counters' 'n' 'k1' 5", "counter add")
	mustExec(t, w0, "MURMUR POLICY MAX 'counters' 'hi' 'k1' 10", "max")
	mustExec(t, w0, "MURMUR POLICY MIN 'counters' 'lo' 'k1' 7", "min")
	mustExec(t, w0, "MURMUR POLICY SET_ADD 'counters' 'tags' 'k1' 'red'", "set add")
	mustExec(t, w0, "MURMUR POLICY SET_ADD 'counters' 'tags' 'k1' 'blue'", "set add 2")
	row := queryOne(t, w0, "SELECT n, lo, hi FROM counters WHERE k = 'k1'", "policy readback")
	if row[0] != "5" || row[1] != "7" || row[2] != "10" {
		t.Fatalf("counters after policy ops = %+v, want 5/7/10", row)
	}
	tagRow := queryOne(t, w0, "SELECT tags IS NOT NULL AND length(tags) > 0 FROM counters WHERE k = 'k1'", "set readback")
	if tagRow[0] != "true" && tagRow[0] != "t" && tagRow[0] != "1" {
		t.Fatalf("tags cell after set adds = %q, want non-empty", tagRow[0])
	}
	mustExec(t, w0, "MURMUR POLICY SET_REMOVE 'counters' 'tags' 'k1' 'red'", "set remove")
	mustExec(t, w0, "MURMUR POLICY SET_REMOVE 'counters' 'tags' 'k1' 'blue'", "set remove 2")
	mustFail(t, w0, "MURMUR POLICY COUNTER_ADD 'counters' 'n' 'missing' 1", "02000", "policy missing row")
	mustFail(t, w0, "MURMUR POLICY FROBNICATE 'counters' 'n' 'k1' 1", "42601", "bad policy op")

	// Leg 7: unconfigured surfaces fail cleanly over the wire.
	mustFail(t, w0, "MURMUR BRIDGE RELEASE 'counters' 'k1'", "55000", "bridge disabled")
	mustFail(t, w0, "MURMUR BRIDGE RELEASE 'counters'", "42601", "bridge missing key")
	mustFail(t, w0, "MURMUR FILE LIST", "55000", "files disabled")

	// Leg 8: backup round-trip + restore. De-peer first so the
	// restored node cannot re-learn reverted rows from its peer.
	mustExec(t, w0, "MURMUR PEER REMOVE '"+creds.nodes[1]+"'", "de-peer 0")
	mustExec(t, w1, "MURMUR PEER REMOVE '"+creds.nodes[0]+"'", "de-peer 1")
	saveRow := queryOne(t, w0, "MURMUR BACKUP SAVE", "backup save")
	backupName := saveRow[0]
	if backupName == "" {
		t.Fatalf("backup save returned empty name: %+v", saveRow)
	}
	listRows, err := w0.run("MURMUR BACKUP LIST")
	if err != nil || findRow(listRows, 0, backupName) == nil {
		t.Fatalf("backup list missing %q: %+v %v", backupName, listRows, err)
	}
	dlRow := queryOne(t, w0, "MURMUR BACKUP DOWNLOAD '"+backupName+"'", "backup download")
	if len(dlRow) != 3 || dlRow[2] == "" {
		t.Fatalf("backup download row = %+v, want offset/total/chunk", dlRow)
	}
	total, err := strconv.ParseInt(dlRow[1], 10, 64)
	if err != nil || total <= 0 {
		t.Fatalf("backup total = %q, want positive", dlRow[1])
	}
	paged := queryOne(t, w0, "MURMUR BACKUP DOWNLOAD '"+backupName+"' OFFSET 0 LIMIT 16", "backup download page")
	if paged[0] != "0" {
		t.Fatalf("paged download offset = %q, want 0", paged[0])
	}
	mustFail(t, w0, "MURMUR BACKUP DOWNLOAD '"+backupName+"' LIMIT 0", "22023", "zero limit")
	mustFail(t, w0, "MURMUR BACKUP DOWNLOAD '../escape'", "22023", "bad backup name")
	mustFail(t, w0, "MURMUR BACKUP DOWNLOAD 'missing-name'", "58P01", "download missing")
	// Upload the downloaded bytes back under a new name, then restore
	// from the copy: proves the wire bytes are a byte-identical valid
	// backup.
	if !strings.HasPrefix(dlRow[2], "\\x") {
		t.Fatalf("download chunk = %q, want hex bytea", dlRow[2][:16])
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(dlRow[2], "\\x"))
	if err != nil || int64(len(raw)) != total {
		t.Fatalf("chunk bytes = %d total = %d err = %v", len(raw), total, err)
	}
	copyName := "uploaded-copy.murmur-backup"
	mustExec(t, w0, "MURMUR BACKUP UPLOAD BEGIN '"+copyName+"'", "upload begin")
	mustFail(t, w0, "MURMUR BACKUP UPLOAD BEGIN '"+copyName+"'", "23505", "upload duplicate begin")
	mustExec(t, w0, "MURMUR BACKUP UPLOAD APPEND '"+copyName+"' '"+base64.StdEncoding.EncodeToString(raw)+"'", "upload append")
	mustExec(t, w0, "MURMUR BACKUP UPLOAD FINISH '"+copyName+"'", "upload finish")
	mustExec(t, w0, "MURMUR BACKUP UPLOAD BEGIN 'aborted.murmur-backup'", "upload begin abort")
	mustExec(t, w0, "MURMUR BACKUP UPLOAD ABORT 'aborted.murmur-backup'", "upload abort")
	listRows, _ = w0.run("MURMUR BACKUP LIST")
	if findRow(listRows, 0, copyName) == nil {
		t.Fatalf("backup list missing uploaded %q: %+v", copyName, listRows)
	}
	if findRow(listRows, 0, "aborted.murmur-backup") != nil {
		t.Fatalf("aborted upload unexpectedly listed: %+v", listRows)
	}
	mustExec(t, w0, "INSERT INTO ops(id, note) VALUES(999, 'sentinel')", "sentinel")
	mustExec(t, w0, "MURMUR BACKUP SAVE", "backup save 2")
	mustExec(t, w0, "MURMUR RESTORE FROM '"+copyName+"' KEY '"+unlockKey+"'", "restore")
	sentinel, err := w0.run("SELECT id FROM ops WHERE id = 999")
	if err != nil {
		t.Fatalf("sentinel check: %v", err)
	}
	if len(sentinel) != 0 {
		t.Fatalf("sentinel survived restore: %+v", sentinel)
	}
	kept, err := w0.run("SELECT note FROM ops WHERE id = 1")
	if err != nil || len(kept) != 1 || kept[0][0] != "converge-me" {
		t.Fatalf("pre-backup row after restore = %+v %v", kept, err)
	}

	// Leg 9: ROTATE KEY validation, rotation, and restart. The old key
	// must keep failing and the new key must unlock after a full stop.
	mustFail(t, w0, "MURMUR ROTATE KEY '"+unlockKey+"' '"+unlockKey+"'", "22023", "rotate same key")
	mustFail(t, w0, "MURMUR ROTATE KEY 'wrong-previous-key' '"+rotatedKey+"'", "28000", "rotate wrong previous")
	mustExec(t, w0, "MURMUR ROTATE KEY '"+unlockKey+"' '"+rotatedKey+"'", "rotate")
	kept, err = w0.run("SELECT note FROM ops WHERE id = 1")
	if err != nil || len(kept) != 1 {
		t.Fatalf("read after rotate = %+v %v", kept, err)
	}
	w0.close()
	s0.stop(t)
	s0 = serveOne(t, dirs[0], creds, 0, passHash)
	defer s0.cancel()
	stale, err := dialWire(s0.addr)
	if err != nil {
		t.Fatalf("dial after rotate: %v", err)
	}
	if err := stale.exec("MURMUR UNLOCK KEY '" + unlockKey + "'"); err == nil {
		stale.close()
		t.Fatalf("old key unlocked the store after rotation")
	} else {
		t.Logf("old key rejected after rotation: %v", err)
	}
	stale.close()
	w0 = connect(t, s0.addr, rotatedKey)
	defer w0.close()
	kept, err = w0.run("SELECT note FROM ops WHERE id = 1")
	if err != nil || len(kept) != 1 || kept[0][0] != "converge-me" {
		t.Fatalf("read with rotated key = %+v %v", kept, err)
	}

	failed = false
}

// Live scenario: file sync works end to end over the PostgreSQL wire.
//
// Two in-process murmur servers (real server.Server + engine/native over
// separate data dirs, QUIC-replicated, shared file key) are driven by a
// minimal PostgreSQL-wire client using simple-query FILE commands:
//
//   - Upload: BEGIN/APPEND/FINISH stores a file; the finish receipt
//     carries name, size, sha, and locality.
//   - Catalog: LIST + NEXT pages the file; SEARCH matches by pattern and
//     misses cleanly; CLOSE releases the cursor.
//   - Local download: DOWNLOAD + DOWNLOAD NEXT reassembles the exact
//     uploaded bytes.
//   - Remote download: the peer serves the same bytes after mesh sync,
//     proving wire-driven cross-node file fetch.
//   - Negatives: unknown handles/cursors, missing files, wrong handle
//     kinds, and over-1-MiB chunks fail with stable codes; ABORT drops
//     the staged upload.
//   - Restart: the file catalog and bytes survive a full server stop.
//
// Engine-level file behavior has its own suites; this one pins the wire
// framing operators and file clients actually use.
package fileswire

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
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
	// user and password are the scratch admin credential.
	user     = "admin"
	password = "test-password"
	// unlockKey is the scratch data key.
	unlockKey = "fileswire-test-key"
	// fileName is the scratch file path.
	fileName = "wire/hello.txt"
	// opTimeout bounds one client operation.
	opTimeout = 30 * time.Second
	// convergeTimeout bounds every convergence wait.
	convergeTimeout = 180 * time.Second
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
	files string
}

func hexID(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("id entropy: %v", err)
	}
	return hex.EncodeToString(b)
}

// makePairCreds provisions one CA, two node leaves, mutual origin trust,
// and one shared file key, writing each node's PEM files into its dir.
func makePairCreds(t *testing.T, dirs []string) *pairCreds {
	t.Helper()
	ca, err := harness.GenerateCa()
	if err != nil {
		t.Fatalf("ca: %v", err)
	}
	c := &pairCreds{dbHex: hexID(t, 16), trust: map[string]string{}, files: hexID(t, 32)}
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
	filesCfg := config.DefaultReplicationFilesConfig()
	filesCfg.Directory = filepath.Join(dir, "files")
	filesCfg.KeyHex = creds.files
	filesCfg.FetchInterval = config.Duration(100 * time.Millisecond)
	cfg.Replication.Files = &filesCfg
	configPath := filepath.Join(dir, "config.yaml")
	if _, err := os.Stat(configPath); err != nil {
		if err := os.WriteFile(configPath, []byte("# files-wire fixture config\nreplication:\n  tables: {}\n"), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
	} else {
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

// connect dials and unlocks the session.
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
// expected node id.
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

// payload returns deterministic file content of n bytes.
func payload(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte((i*17 + 3) % 251)
	}
	return out
}

// decodeChunk parses a hex-bytea wire field.
func decodeChunk(t *testing.T, field, what string) []byte {
	t.Helper()
	if !strings.HasPrefix(field, "\\x") {
		t.Fatalf("%s: chunk = %q, want hex bytea", what, field)
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(field, "\\x"))
	if err != nil {
		t.Fatalf("%s: chunk hex: %v", what, err)
	}
	return raw
}

// downloadAll opens a download handle and streams it to EOF.
func downloadAll(t *testing.T, w *wireConn, name, what string) []byte {
	t.Helper()
	open := queryOne(t, w, "MURMUR FILE DOWNLOAD '"+name+"'", what+" download open")
	if len(open) != 3 {
		t.Fatalf("%s: download open = %+v, want 3 columns", what, open)
	}
	total, err := strconv.ParseInt(open[2], 10, 64)
	if err != nil || total < 0 {
		t.Fatalf("%s: download size = %q", what, open[2])
	}
	var out bytes.Buffer
	for {
		row := queryOne(t, w, "MURMUR FILE DOWNLOAD NEXT '"+open[0]+"' LIMIT 1024", what+" download next")
		if len(row) != 4 {
			t.Fatalf("%s: download next = %+v, want 4 columns", what, row)
		}
		out.Write(decodeChunk(t, row[2], what))
		if row[3] == "t" || row[3] == "true" {
			break
		}
		if int64(out.Len()) > total {
			t.Fatalf("%s: streamed %d bytes past total %d", what, out.Len(), total)
		}
	}
	if int64(out.Len()) != total {
		t.Fatalf("%s: streamed %d bytes, want total %d", what, out.Len(), total)
	}
	return out.Bytes()
}

func TestFilesOverWire(t *testing.T) {
	runtimeDir, err := harness.PrepareRuntimeDir("files-wire")
	if err != nil {
		t.Fatalf("runtime dir: %v", err)
	}
	failed := true
	defer func() { harness.Finish("files-wire", runtimeDir, failed) }()

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

	w0 := connect(t, s0.addr)
	defer w0.close()
	w1 := connect(t, s1.addr)
	defer w1.close()

	a0, a1 := replAddr(t, w0), replAddr(t, w1)
	mustExec(t, w0, "MURMUR PEER ADD '"+a1+"'", "peer add 0->1")
	mustExec(t, w1, "MURMUR PEER ADD '"+a0+"'", "peer add 1->0")
	waitPeersConnected(t, w0, creds.nodes[1], "mesh 0->1")
	waitPeersConnected(t, w1, creds.nodes[0], "mesh 1->0")

	want := payload(3000)

	// Leg 1: upload in two chunks; the finish receipt carries the file
	// identity (name size modified_ms sha256 local).
	begin := queryOne(t, w0, "MURMUR FILE UPLOAD BEGIN '"+fileName+"'", "upload begin")
	if len(begin) != 1 || begin[0] == "" {
		t.Fatalf("upload begin = %+v, want one handle", begin)
	}
	up := begin[0]
	mustExec(t, w0, "MURMUR FILE UPLOAD APPEND '"+up+"' '\\x"+hex.EncodeToString(want[:1200])+"'", "upload append 1")
	mustExec(t, w0, "MURMUR FILE UPLOAD APPEND '"+up+"' '\\x"+hex.EncodeToString(want[1200:])+"'", "upload append 2")
	receipt := queryOne(t, w0, "MURMUR FILE UPLOAD FINISH '"+up+"'", "upload finish")
	if len(receipt) != 5 || receipt[0] != fileName || receipt[1] != strconv.Itoa(len(want)) {
		t.Fatalf("upload receipt = %+v, want %s/%d", receipt, fileName, len(want))
	}
	if receipt[4] != "t" && receipt[4] != "true" {
		t.Fatalf("upload receipt local = %q, want true", receipt[4])
	}
	t.Logf("uploaded %s sha=%s", receipt[0], receipt[3])

	// Leg 2: catalog paging and search.
	list := queryOne(t, w0, "MURMUR FILE LIST", "file list")
	page, err := w0.run("MURMUR FILE NEXT '" + list[0] + "'")
	if err != nil {
		t.Fatalf("file next: %v", err)
	}
	found := false
	for _, r := range page {
		if len(r) >= 2 && r[0] == fileName && r[1] == strconv.Itoa(len(want)) {
			found = true
		}
	}
	if !found {
		t.Fatalf("file list page missing %s: %+v", fileName, page)
	}
	empty, err := w0.run("MURMUR FILE NEXT '" + list[0] + "'")
	if err != nil || len(empty) != 0 {
		t.Fatalf("second page = %+v %v, want empty", empty, err)
	}
	mustExec(t, w0, "MURMUR FILE CLOSE '"+list[0]+"'", "file close")
	mustFail(t, w0, "MURMUR FILE NEXT '"+list[0]+"'", "34000", "next after close")
	search := queryOne(t, w0, "MURMUR FILE SEARCH 'wire/%'", "file search")
	hits, err := w0.run("MURMUR FILE NEXT '" + search[0] + "' LIMIT 10")
	if err != nil || len(hits) != 1 || hits[0][0] != fileName {
		t.Fatalf("search hits = %+v %v, want %s", hits, err, fileName)
	}
	mustExec(t, w0, "MURMUR FILE CLOSE '"+search[0]+"'", "search close")
	none := queryOne(t, w0, "MURMUR FILE SEARCH 'nomatch-%'", "file search miss")
	miss, err := w0.run("MURMUR FILE NEXT '" + none[0] + "'")
	if err != nil || len(miss) != 0 {
		t.Fatalf("search miss = %+v %v, want empty", miss, err)
	}
	mustExec(t, w0, "MURMUR FILE CLOSE '"+none[0]+"'", "miss close")

	// Leg 3: local download reassembles the exact bytes.
	if got := downloadAll(t, w0, fileName, "local"); !bytes.Equal(got, want) {
		t.Fatalf("local download differs: %d bytes", len(got))
	}

	// Leg 4: the peer serves the same bytes after mesh sync.
	if ok := harness.WaitUntil(convergeTimeout, poll, func() bool {
		_, err := w1.run("MURMUR FILE DOWNLOAD '" + fileName + "'")
		return err == nil
	}); !ok {
		t.Fatalf("peer never served %s", fileName)
	}
	if got := downloadAll(t, w1, fileName, "remote"); !bytes.Equal(got, want) {
		t.Fatalf("remote download differs: %d bytes", len(got))
	}
	t.Logf("remote download matched %d bytes", len(want))

	// Leg 5: negatives and abort. Handles are per-connection, so a
	// handle opened on w0 is unknown on w1.
	badHandle := strings.Repeat("0", 32)
	mustFail(t, w0, "MURMUR FILE DOWNLOAD 'missing.txt'", "55000", "download missing")
	mustFail(t, w0, "MURMUR FILE UPLOAD APPEND '"+badHandle+"' '\\x00'", "34000", "append bad handle")
	mustFail(t, w0, "MURMUR FILE UPLOAD FINISH '"+badHandle+"'", "34000", "finish bad handle")
	mustFail(t, w0, "MURMUR FILE NEXT '"+badHandle+"'", "34000", "next bad cursor")
	mustFail(t, w0, "MURMUR FILE CLOSE '"+badHandle+"'", "34000", "close bad handle")
	other := queryOne(t, w1, "MURMUR FILE LIST", "peer list")
	mustFail(t, w0, "MURMUR FILE NEXT '"+other[0]+"'", "34000", "cross-connection cursor")
	mustExec(t, w1, "MURMUR FILE CLOSE '"+other[0]+"'", "peer list close")
	listCur := queryOne(t, w0, "MURMUR FILE LIST", "kind list")
	mustFail(t, w0, "MURMUR FILE DOWNLOAD NEXT '"+listCur[0]+"'", "42601", "wrong handle kind")
	mustExec(t, w0, "MURMUR FILE CLOSE '"+listCur[0]+"'", "kind list close")
	big := queryOne(t, w0, "MURMUR FILE UPLOAD BEGIN 'big.bin'", "big begin")
	oversize := make([]byte, (1<<20)+1)
	mustFail(t, w0, "MURMUR FILE UPLOAD APPEND '"+big[0]+"' '\\x"+hex.EncodeToString(oversize)+"'", "22023", "oversize chunk")
	mustExec(t, w0, "MURMUR FILE UPLOAD ABORT '"+big[0]+"'", "big abort")
	mustFail(t, w0, "MURMUR FILE UPLOAD FINISH '"+big[0]+"'", "34000", "finish after abort")
	aborted := queryOne(t, w0, "MURMUR FILE SEARCH 'big.bin'", "abort search")
	abortedHits, err := w0.run("MURMUR FILE NEXT '" + aborted[0] + "'")
	if err != nil || len(abortedHits) != 0 {
		t.Fatalf("aborted file listed: %+v %v", abortedHits, err)
	}
	mustExec(t, w0, "MURMUR FILE CLOSE '"+aborted[0]+"'", "abort search close")

	// Leg 6: the catalog and bytes survive a full restart of the peer.
	w1.close()
	s1.stop(t)
	s1 = serveOne(t, dirs[1], creds, 1, passHash)
	defer s1.cancel()
	w1 = connect(t, s1.addr)
	defer w1.close()
	after := queryOne(t, w1, "MURMUR FILE LIST", "post-restart list")
	afterPage, err := w1.run("MURMUR FILE NEXT '" + after[0] + "'")
	if err != nil {
		t.Fatalf("post-restart next: %v", err)
	}
	still := false
	for _, r := range afterPage {
		if len(r) > 0 && r[0] == fileName {
			still = true
		}
	}
	if !still {
		t.Fatalf("post-restart catalog missing %s: %+v", fileName, afterPage)
	}
	mustExec(t, w1, "MURMUR FILE CLOSE '"+after[0]+"'", "post-restart close")
	if got := downloadAll(t, w1, fileName, "post-restart"); !bytes.Equal(got, want) {
		t.Fatalf("post-restart download differs: %d bytes", len(got))
	}

	failed = false
}

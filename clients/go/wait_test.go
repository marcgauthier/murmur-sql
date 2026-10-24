package murmur_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// quoteArg SQL-quotes one MURMUR command argument.
func quoteArg(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// TestWaitForReadYourWrite pins the wire read-your-write recipe end to
// end: flush on the writer, cursor from SHOW HEAD, WAIT FOR on the
// reader, then read.
func TestWaitForReadYourWrite(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	dirs := []string{filepath.Join(base, "n0"), filepath.Join(base, "n1")}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	_, dbs, opts := meshDBs(t, dirs, "")
	node0 := opts[0].NodeID

	peerStatus, err := sqlRows(ctx, dbs[1], "MURMUR SHOW REPLICATION")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if _, err := dbs[0].ExecContext(ctx, "MURMUR PEER ADD "+quoteArg(peerStatus[0]["address"].(string))); err != nil {
		t.Fatalf("add: %v", err)
	}
	for _, db := range dbs {
		if _, err := db.ExecContext(ctx, `CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT NOT NULL)`); err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := db.ExecContext(ctx, "MURMUR SET TABLE docs REPLICATED true"); err != nil {
			t.Fatalf("replicate: %v", err)
		}
	}
	if _, err := dbs[0].ExecContext(ctx, "INSERT INTO docs(id, body) VALUES(1, 'hello')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := dbs[0].ExecContext(ctx, "MURMUR FLUSH"); err != nil {
		t.Fatalf("flush: %v", err)
	}
	heads, err := sqlRows(ctx, dbs[0], "MURMUR SHOW HEAD")
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	var seq int64 = -1
	for _, h := range heads {
		if h["origin"].(string) == node0 {
			seq = h["applied"].(int64)
		}
	}
	if seq <= 0 {
		t.Fatalf("no writer head in %+v", heads)
	}
	waitSQL := "MURMUR WAIT FOR " + quoteArg(node0) + " " + strconv.FormatInt(seq, 10)
	if _, err := dbs[1].ExecContext(ctx, waitSQL); err != nil {
		t.Fatalf("wait: %v", err)
	}
	var body string
	if err := dbs[1].QueryRowContext(ctx, "SELECT body FROM docs WHERE id = 1").Scan(&body); err != nil {
		t.Fatalf("read: %v", err)
	}
	if body != "hello" {
		t.Fatalf("body = %q, want hello", body)
	}
	if _, err := dbs[1].ExecContext(ctx, "MURMUR WAIT FOR LAG 0"); err != nil {
		t.Fatalf("wait lag: %v", err)
	}
}

// TestWaitForErrors pins WAIT failure codes: unreachable cursors time
// out, malformed origins are rejected.
func TestWaitForErrors(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	dirs := []string{filepath.Join(base, "n0"), filepath.Join(base, "n1")}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	_, dbs, opts := meshDBs(t, dirs, "")
	node0 := opts[0].NodeID
	if _, err := dbs[0].ExecContext(ctx,
		"MURMUR WAIT FOR "+quoteArg(node0)+" 999999999 TIMEOUT 1"); codeOf(err) != "57014" {
		t.Fatalf("timeout: code = %q, want 57014 (%v)", codeOf(err), err)
	}
	if _, err := dbs[0].ExecContext(ctx, "MURMUR WAIT FOR 'xyz' 1"); codeOf(err) != "22023" {
		t.Fatalf("bad origin: code = %q, want 22023 (%v)", codeOf(err), err)
	}
	if _, err := dbs[0].ExecContext(ctx, "MURMUR WAIT FOR LAG -1"); codeOf(err) != "22023" {
		t.Fatalf("bad bound: code = %q, want 22023 (%v)", codeOf(err), err)
	}
}

package murmur_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	murmur "github.com/marcgauthier/murmur-sql/clients/go"
	"github.com/marcgauthier/murmur-sql/clients/go/internal/servertest"
)

func filesDB(t *testing.T) (*sql.DB, *servertest.Server, servertest.Options) {
	t.Helper()
	dir := t.TempDir()
	opts := servertest.FileOptions(t, dir)
	s := servertest.Start(t, dir, opts)
	db, err := murmur.Open(context.Background(), murmur.Config{Port: s.Port, User: "admin", Password: "test-password", Key: "files-key"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, s, opts
}

// Fixed-data fixtures drive the SQL protocol; there is no file-transfer API.
func uploadFixture(t *testing.T, db *sql.DB, name string, data []byte) {
	t.Helper()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var token string
	if err := conn.QueryRowContext(ctx, "MURMUR FILE UPLOAD BEGIN $1", name).Scan(&token); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(ctx, "MURMUR FILE UPLOAD ABORT $1", token)
	for offset := 0; offset < len(data); offset += 65536 {
		if _, err := conn.ExecContext(ctx, "MURMUR FILE UPLOAD APPEND $1 $2", token, data[offset:min(offset+65536, len(data))]); err != nil {
			t.Fatal(err)
		}
	}
	var gotName, hash string
	var size, modified int64
	var local bool
	if err := conn.QueryRowContext(ctx, "MURMUR FILE UPLOAD FINISH $1", token).Scan(&gotName, &size, &modified, &hash, &local); err != nil {
		t.Fatal(err)
	}
	if gotName != name || size != int64(len(data)) || !local || hash != fmt.Sprintf("%x", sha256.Sum256(data)) {
		t.Fatal("invalid upload metadata")
	}
}

func TestFileSQLAbortRestartAndErrors(t *testing.T) {
	ctx := context.Background()
	db, server, opts := filesDB(t)
	payload := []byte{0, 255, 39, 92, 128}
	uploadFixture(t, db, "same", payload)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var token string
	if err := conn.QueryRowContext(ctx, "MURMUR FILE UPLOAD BEGIN 'same'").Scan(&token); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "MURMUR FILE UPLOAD APPEND $1 $2", token, []byte("partial")); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "MURMUR FILE UPLOAD ABORT $1", token); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRowContext(ctx, "MURMUR FILE LIST").Scan(&token); err != nil {
		t.Fatal(err)
	}
	other, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.ExecContext(ctx, "MURMUR FILE NEXT $1", token); codeOf(err) != "34000" {
		t.Fatalf("foreign cursor: %v", err)
	}
	other.Close()
	if _, err := conn.ExecContext(ctx, "MURMUR FILE CLOSE $1", token); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "MURMUR FILE SEARCH $1", `x\`); codeOf(err) != "22025" {
		t.Fatalf("bad pattern: %v", err)
	}
	var download, name string
	var size int64
	if err := conn.QueryRowContext(ctx, "MURMUR FILE DOWNLOAD 'same'").Scan(&download, &name, &size); err != nil {
		t.Fatal(err)
	}
	var offset, total int64
	var chunk []byte
	var eof bool
	if err := conn.QueryRowContext(ctx, "MURMUR FILE DOWNLOAD NEXT $1", download).Scan(&offset, &total, &chunk, &eof); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(chunk, payload) || !eof {
		t.Fatal("aborted replacement changed original")
	}
	conn.ExecContext(ctx, "MURMUR FILE CLOSE $1", download)
	for i := 0; i < 16; i++ {
		if err := conn.QueryRowContext(ctx, "MURMUR FILE LIST").Scan(&token); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.QueryRowContext(ctx, "MURMUR FILE LIST").Scan(&token); codeOf(err) != "53400" {
		t.Fatalf("handle limit: %v", err)
	}
	conn.Raw(func(any) error { return driver.ErrBadConn })
	conn.Close()
	if _, err := db.ExecContext(ctx, "MURMUR FLUSH"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	server.Stop()
	restarted := servertest.Start(t, server.Dir, opts)
	db = open(t, murmur.Config{Port: restarted.Port, User: "admin", Password: "test-password", Key: "files-key"})
	if err := db.QueryRowContext(ctx, "MURMUR FILE DOWNLOAD 'same'").Scan(&download, &name, &size); err != nil || size != int64(len(payload)) {
		t.Fatalf("restart: %v", err)
	}
	// Corruption must be rejected before a download handle exposes any bytes.
	entries, err := os.ReadDir(filepath.Join(server.Dir, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".spfo") {
			path := filepath.Join(server.Dir, "objects", e.Name())
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			b[50] ^= 1
			if err := os.WriteFile(path, b, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := db.ExecContext(ctx, "MURMUR FILE DOWNLOAD 'same'"); codeOf(err) != "58030" {
		t.Fatalf("corruption: %v", err)
	}
}

func TestFileSQLPagesPinnedDownloadAndDisconnect(t *testing.T) {
	ctx := context.Background()
	db, server, _ := filesDB(t)
	original := bytes.Repeat([]byte("old\x00\xff"), 20000)
	for _, name := range []string{"c", "a", "b"} {
		uploadFixture(t, db, name, original)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var cursor string
	if err = conn.QueryRowContext(ctx, "MURMUR FILE LIST").Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	uploadFixture(t, db, "d", []byte("new"))
	var names []string
	for {
		rows, err := conn.QueryContext(ctx, "MURMUR FILE NEXT $1 LIMIT 1", cursor)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for rows.Next() {
			var name, hash string
			var size, modified int64
			var local bool
			if err = rows.Scan(&name, &size, &modified, &hash, &local); err != nil {
				t.Fatal(err)
			}
			names = append(names, name)
			n++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	if strings.Join(names, ",") != "a,b,c" {
		t.Fatalf("pagination: %v", names)
	}
	if _, err = conn.ExecContext(ctx, "MURMUR FILE CLOSE $1", cursor); err != nil {
		t.Fatal(err)
	}
	var token, name string
	var size int64
	if err = conn.QueryRowContext(ctx, "MURMUR FILE DOWNLOAD $1", "a").Scan(&token, &name, &size); err != nil {
		t.Fatal(err)
	}
	uploadFixture(t, db, "a", []byte("replacement"))
	var output []byte
	for {
		var offset, total int64
		var chunk []byte
		var eof bool
		if err = conn.QueryRowContext(ctx, "MURMUR FILE DOWNLOAD NEXT $1 LIMIT 7919", token).Scan(&offset, &total, &chunk, &eof); err != nil {
			t.Fatal(err)
		}
		if offset != int64(len(output)) || total != size {
			t.Fatal("bad chunk bounds")
		}
		output = append(output, chunk...)
		if eof {
			break
		}
	}
	if !bytes.Equal(output, original) {
		t.Fatal("download changed version")
	}
	if _, err = conn.ExecContext(ctx, "MURMUR FILE CLOSE $1", token); err != nil {
		t.Fatal(err)
	}
	if err = conn.QueryRowContext(ctx, "MURMUR FILE UPLOAD BEGIN $1", "interrupted").Scan(&token); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.ExecContext(ctx, "MURMUR FILE UPLOAD APPEND $1 $2", token, []byte("not published")); err != nil {
		t.Fatal(err)
	}
	// Force the physical socket closed rather than returning it to the pool.
	conn.Raw(func(any) error { return driver.ErrBadConn })
	deadline := time.Now().Add(5 * time.Second)
	for {
		entries, _ := os.ReadDir(filepath.Join(server.Dir, "objects"))
		staged := false
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), ".upload-") {
				staged = true
			}
		}
		if !staged {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("disconnect did not clean staging")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err = db.ExecContext(ctx, "MURMUR FILE DOWNLOAD 'interrupted'"); codeOf(err) != "55000" {
		t.Fatalf("unfinished metadata published: %v", err)
	}
}

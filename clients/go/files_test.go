package murmur_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/internal/servertest"
)

func filesDB(t *testing.T) (*murmur.DB, *servertest.Server, servertest.Options) {
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
func TestFilesBinaryCursorAndRestart(t *testing.T) {
	ctx := context.Background()
	db, server, opts := filesDB(t)
	payload := bytes.Repeat([]byte{0, 255, 1, 128, 39, 92}, 40000)
	info, err := db.UploadFile(ctx, "dir/a'_%.bin", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(payload)) || !info.Local || info.SHA256 != fmt.Sprintf("%x", sha256.Sum256(payload)) {
		t.Fatalf("bad metadata: %+v", info)
	}
	cursor, err := db.SearchFiles(ctx, "dir/a'\\_\\%.bin")
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next(ctx) || cursor.File().Name != info.Name {
		t.Fatalf("missing file: %v", cursor.Err())
	}
	if _, err = db.UploadFile(ctx, "other", strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if cursor.Next(ctx) || cursor.Err() != nil {
		t.Fatalf("snapshot changed: %v", cursor.Err())
	}
	var out bytes.Buffer
	if err = db.DownloadFile(ctx, info.Name, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), payload) {
		t.Fatal("binary mismatch")
	}
	for _, n := range []string{"empty", "Case.bin"} {
		if _, err = db.UploadFile(ctx, n, strings.NewReader("")); err != nil {
			t.Fatal(err)
		}
	}
	empty := bytes.Buffer{}
	if err = db.DownloadFile(ctx, "empty", &empty); err != nil || empty.Len() != 0 {
		t.Fatalf("empty: %v", err)
	}
	cursor, err = db.SearchFiles(ctx, "case%")
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Next(ctx) || cursor.Err() != nil {
		t.Fatalf("case sensitivity: %v", cursor.Err())
	}
	var count int
	if err = db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("file SQL touched SQLite tables: %d %v", count, err)
	}
	entries, err := os.ReadDir(filepath.Join(server.Dir, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".spfo") {
			found = true
			b, err := os.ReadFile(filepath.Join(server.Dir, "objects", e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(b, payload) {
				t.Fatal("plaintext on disk")
			}
		}
	}
	if !found {
		t.Fatal("no disk object")
	}
	if err = db.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	db.Close()
	server.Stop()
	server = servertest.Start(t, server.Dir, opts)
	db, err = murmur.Open(ctx, murmur.Config{Port: server.Port, User: "admin", Password: "test-password", Key: "files-key"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	out.Reset()
	if err = db.DownloadFile(ctx, info.Name, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), payload) {
		t.Fatal("restart mismatch")
	}
}

type failingFileReader struct{ done bool }

func (r *failingFileReader) Read(b []byte) (int, error) {
	if r.done {
		return 0, errors.New("source failure")
	}
	r.done = true
	return copy(b, []byte("partial secret")), nil
}
func TestFileHandlesAtomicityAndErrors(t *testing.T) {
	ctx := context.Background()
	db, server, _ := filesDB(t)
	_, err := db.UploadFile(ctx, "same", strings.NewReader("old"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.UploadFile(ctx, "same", &failingFileReader{}); err == nil {
		t.Fatal("expected source failure")
	}
	var out bytes.Buffer
	if err = db.DownloadFile(ctx, "same", &out); err != nil || out.String() != "old" {
		t.Fatalf("replacement changed: %q %v", out.String(), err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	other, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	var token string
	if err = conn.QueryRowContext(ctx, "MURMUR FILE LIST").Scan(&token); err != nil {
		t.Fatal(err)
	}
	if _, err = other.QueryContext(ctx, "MURMUR FILE NEXT $1", token); murmur.CodeOf(err) != "34000" {
		t.Fatalf("foreign cursor: %v", err)
	}
	if _, err = conn.ExecContext(ctx, "MURMUR FILE CLOSE $1", token); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.QueryContext(ctx, "MURMUR FILE SEARCH $1", `x\`); murmur.CodeOf(err) != "22025" {
		t.Fatalf("bad pattern: %v", err)
	}
	if _, err = conn.QueryContext(ctx, "MURMUR FILE DOWNLOAD $1", "missing"); murmur.CodeOf(err) != "55000" {
		t.Fatalf("missing: %v", err)
	}
	if err = conn.QueryRowContext(ctx, "MURMUR FILE UPLOAD BEGIN 'hex'").Scan(&token); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.ExecContext(ctx, fmt.Sprintf("MURMUR FILE UPLOAD APPEND '%s' '\\x00ff27'", token)); err != nil {
		t.Fatal(err)
	}
	var info murmur.FileInfo
	if err = conn.QueryRowContext(ctx, "MURMUR FILE UPLOAD FINISH $1", token).Scan(&info.Name, &info.Size, &info.ModifiedMillis, &info.SHA256, &info.Local); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err = db.DownloadFile(ctx, "hex", &out); err != nil || !bytes.Equal(out.Bytes(), []byte{0, 255, 39}) {
		t.Fatalf("hex bytes %x %v", out.Bytes(), err)
	}
	for i := 0; i < 16; i++ {
		if err = conn.QueryRowContext(ctx, "MURMUR FILE LIST").Scan(&token); err != nil {
			t.Fatal(err)
		}
	}
	if err = conn.QueryRowContext(ctx, "MURMUR FILE LIST").Scan(&token); murmur.CodeOf(err) != "53400" {
		t.Fatalf("handle limit: %v", err)
	}
	// Corrupt an immutable object; DOWNLOAD must reject before returning bytes.
	entries, _ := os.ReadDir(filepath.Join(server.Dir, "objects"))
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".spfo") {
			p := filepath.Join(server.Dir, "objects", e.Name())
			b, _ := os.ReadFile(p)
			if len(b) > 50 {
				b[50] ^= 1
				if err = os.WriteFile(p, b, 0o600); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if err = db.DownloadFile(ctx, "same", io.Discard); murmur.CodeOf(err) != "58030" {
		t.Fatalf("corruption: %v", err)
	}
}

func TestFileCursorCancellationReleasesPool(t *testing.T) {
	db, _, _ := filesDB(t)
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithCancel(context.Background())
	cursor, err := db.ListFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	deadline := time.Now().Add(5 * time.Second)
	for db.Stats().InUse != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if db.Stats().InUse != 0 {
		t.Fatal("canceled cursor still owns connection")
	}
	if !errors.Is(cursor.Err(), context.Canceled) {
		t.Fatalf("cursor error: %v", cursor.Err())
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFileSQLPagesPinnedDownloadAndDisconnect(t *testing.T) {
	ctx := context.Background()
	db, server, _ := filesDB(t)
	original := bytes.Repeat([]byte("old\x00\xff"), 20000)
	for _, name := range []string{"c", "a", "b"} {
		if _, err := db.UploadFile(ctx, name, bytes.NewReader(original)); err != nil {
			t.Fatal(err)
		}
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
	if _, err = db.UploadFile(ctx, "d", strings.NewReader("new")); err != nil {
		t.Fatal(err)
	}
	var names []string
	for {
		rows, err := conn.QueryContext(ctx, "MURMUR FILE NEXT $1 LIMIT 1", cursor)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for rows.Next() {
			var f murmur.FileInfo
			if err = rows.Scan(&f.Name, &f.Size, &f.ModifiedMillis, &f.SHA256, &f.Local); err != nil {
				t.Fatal(err)
			}
			names = append(names, f.Name)
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
	if _, err = db.UploadFile(ctx, "a", strings.NewReader("replacement")); err != nil {
		t.Fatal(err)
	}
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
	var out bytes.Buffer
	if err = db.DownloadFile(ctx, "interrupted", &out); murmur.CodeOf(err) != "55000" {
		t.Fatalf("unfinished metadata published: %v", err)
	}
}

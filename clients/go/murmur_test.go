package murmur_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	murmur "github.com/marcgauthier/murmur-sql/clients/go"
	"github.com/marcgauthier/murmur-sql/clients/go/internal/servertest"
)

func caFile(server *servertest.Server) string {
	return filepath.Join(filepath.Dir(server.ConfigPath), "ca.crt")
}

const (
	testPassword = "test-password"
	testKey      = "go-test-key"
)

func testConfig(server *servertest.Server) murmur.Config {
	return murmur.Config{
		Host:     "127.0.0.1",
		Port:     server.Port,
		User:     "admin",
		Password: testPassword,
		Key:      testKey,
	}
}

func open(t *testing.T, cfg murmur.Config) *sql.DB {
	t.Helper()
	db, err := murmur.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestOpenAndUnlock(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	db := open(t, testConfig(server))
	var one int
	if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("select: %v", err)
	}
	if one != 1 {
		t.Fatalf("got %d, want 1", one)
	}
}

func TestAuthFailure(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	cfg := testConfig(server)
	cfg.Password = "wrong"
	if _, err := murmur.Open(context.Background(), cfg); codeOf(err) != "28P01" {
		t.Fatalf("expected authentication SQLSTATE, got %v", err)
	}
	cfg = testConfig(server)
	cfg.User = "nobody"
	if _, err := murmur.Open(context.Background(), cfg); codeOf(err) != "28P01" {
		t.Fatalf("expected unknown-user SQLSTATE, got %v", err)
	}
}

func TestWrongKey(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	first := testConfig(server)
	open(t, first)
	second := testConfig(server)
	second.Key = "another-key"
	_, err := murmur.Open(context.Background(), second)
	if err == nil {
		t.Fatal("expected wrong-key failure")
	}
	if code := codeOf(err); code != "28000" {
		t.Fatalf("code = %q, want 28000 (%v)", code, err)
	}
}

func TestCRUD(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	db := open(t, testConfig(server))
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE items(
		id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, payload BLOB)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO items VALUES ($1, $2, $3, $4)",
		1, "one", 1.5, []byte{1, 2, 3}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO items VALUES ($1, $2, NULL, NULL)",
		2, "two"); err != nil {
		t.Fatalf("insert nulls: %v", err)
	}
	rows, err := db.QueryContext(ctx, "SELECT id, name, score, payload FROM items ORDER BY id")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer rows.Close()
	type item struct {
		id      int64
		name    string
		score   *float64
		payload []byte
	}
	var got []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.id, &it.name, &it.score, &it.payload); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, it)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if len(got) < 2 || got[0].id != 1 || got[0].name != "one" {
		t.Fatalf("unexpected rows: %+v", got)
	}
	if got[0].score == nil || *got[0].score != 1.5 {
		t.Fatalf("unexpected score: %+v", got[0].score)
	}
	if string(got[0].payload) != string([]byte{1, 2, 3}) {
		t.Fatalf("unexpected payload: %v", got[0].payload)
	}
	res, err := db.ExecContext(ctx, "UPDATE items SET score = $1 WHERE id = $2", 2.5, 1)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("rows affected = %d, want 1", n)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM items WHERE id = $1", 1); err != nil {
		t.Fatalf("delete: %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM items").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count < 1 {
		t.Fatalf("count = %d, want >= 1", count)
	}
}

func TestSQLErrorCodes(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	db := open(t, testConfig(server))
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE codes(id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO codes VALUES (1)"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	cases := []struct {
		sql  string
		code string
	}{
		{"SELECT * FROM missing", "42P01"},
		{"INSERT INTO codes VALUES (1)", "23505"},
		{"SELEC oops", "42601"},
	}
	for _, c := range cases {
		_, err := db.ExecContext(ctx, c.sql)
		if err == nil {
			t.Fatalf("%q: expected error", c.sql)
		}
		if code := codeOf(err); code != c.code {
			t.Fatalf("%q: code = %q, want %q (%v)", c.sql, code, c.code, err)
		}
	}
}

func TestVersion(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	db := open(t, testConfig(server))
	var version string
	if err := db.QueryRowContext(context.Background(), "SELECT version()").Scan(&version); err != nil {
		t.Fatalf("version: %v", err)
	}
	if len(version) < 10 || version[:10] != "PostgreSQL" {
		t.Fatalf("unexpected version: %q", version)
	}
}

func TestFlushAndRestart(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	db := open(t, testConfig(server))
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE kept(id INTEGER PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO kept VALUES (1, 'durable')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.ExecContext(ctx, "MURMUR FLUSH"); err != nil {
		t.Fatalf("flush: %v", err)
	}
	db.Close()
	server.Stop()

	restarted := servertest.Start(t, dir, servertest.Options{})
	db2 := open(t, testConfig(restarted))
	var value string
	if err := db2.QueryRowContext(ctx, "SELECT v FROM kept WHERE id = 1").Scan(&value); err != nil {
		t.Fatalf("reopen select: %v", err)
	}
	if value != "durable" {
		t.Fatalf("value = %q, want durable", value)
	}
}

func TestTableFlags(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	db := open(t, testConfig(server))
	ctx := context.Background()
	for _, table := range []string{"shared", "scratch"} {
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, v TEXT NOT NULL)", table)); err != nil {
			t.Fatalf("create %s: %v", table, err)
		}
	}
	flags, err := sqlRows(ctx, db, "MURMUR SHOW TABLES REPLICATION")
	if err != nil {
		t.Fatalf("flags: %v", err)
	}
	if len(flags) != 2 || flags[0]["replicated"] == "true" || flags[1]["replicated"] == "true" {
		t.Fatalf("default flags: %+v", flags)
	}
	if _, err := db.ExecContext(ctx, "MURMUR SET TABLE 'shared' REPLICATED true"); err != nil {
		t.Fatalf("set: %v", err)
	}
	flags, err = sqlRows(ctx, db, "MURMUR SHOW TABLES REPLICATION")
	if err != nil {
		t.Fatalf("flags: %v", err)
	}
	byName := map[string]bool{}
	for _, f := range flags {
		byName[f["table"].(string)] = f["replicated"] == "true"
	}
	if byName["scratch"] || !byName["shared"] {
		t.Fatalf("flags after set: %+v", flags)
	}
	if _, err := db.ExecContext(ctx, "MURMUR SET TABLE 'missing' REPLICATED false"); err == nil {
		t.Fatal("expected unknown-table error")
	}
}

func TestConcurrentUse(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	cfg := testConfig(server)
	cfg.MaxOpenConns = 8
	db := open(t, cfg)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE events(id INTEGER PRIMARY KEY, worker INTEGER NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	const workers, perWorker = 8, 25
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				id := worker*perWorker + i + 1
				if _, err := db.ExecContext(ctx, "INSERT INTO events VALUES ($1, $2)", id, worker); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("insert: %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM events").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != workers*perWorker {
		t.Fatalf("count = %d, want %d", count, workers*perWorker)
	}
}

func TestTLSModes(t *testing.T) {
	dir := t.TempDir()
	pki := servertest.IssuePKI(t)
	// Dev TLS verifies the server only; the server rejects ca_file
	// outside production, where it would falsely imply mTLS.
	tlsFiles := pki.WriteServerFiles(t, dir)
	tlsFiles.CA = ""
	server := servertest.Start(t, dir, servertest.Options{TLSFiles: tlsFiles})

	// Verified TLS against the test CA.
	cfg := testConfig(server)
	cfg.TLS = murmur.TLSConfig{Mode: murmur.TLSVerifyFull, CAFile: caFile(server)}
	db := open(t, cfg)
	var one int
	if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("tls select: %v", err)
	}

	// Dev servers still accept plaintext.
	plain := open(t, testConfig(server))
	if err := plain.QueryRowContext(context.Background(), "SELECT 2").Scan(&one); err != nil {
		t.Fatalf("plaintext select: %v", err)
	}
}

func TestMutualTLS(t *testing.T) {
	dir := t.TempDir()
	pki := servertest.IssuePKI(t)
	server := servertest.Start(t, dir, servertest.Options{
		Production: true,
		TLSFiles:   pki.WriteServerFiles(t, dir),
	})
	clientCert, clientKey := pki.WriteClientFiles(t, dir)

	// Plaintext is rejected in production.
	if _, err := murmur.Open(context.Background(), testConfig(server)); err == nil {
		t.Fatal("expected plaintext rejection")
	} else if code := codeOf(err); code != "28000" {
		t.Fatalf("plaintext code = %q, want 28000 (%v)", code, err)
	}

	// TLS without a client certificate fails the handshake.
	cfg := testConfig(server)
	cfg.TLS = murmur.TLSConfig{Mode: murmur.TLSVerifyFull, CAFile: caFile(server)}
	if _, err := murmur.Open(context.Background(), cfg); err == nil {
		t.Fatal("expected client-cert failure")
	}

	// Full mTLS works.
	cfg.TLS.CertFile, cfg.TLS.KeyFile = clientCert, clientKey
	db := open(t, cfg)
	var three int
	if err := db.QueryRowContext(context.Background(), "SELECT 3").Scan(&three); err != nil {
		t.Fatalf("mtls select: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	ctx := context.Background()
	base := murmur.Config{Host: "127.0.0.1", Port: 5432, User: "u", Password: "p", Key: "k"}
	for name, mutate := range map[string]func(*murmur.Config){
		"user":     func(c *murmur.Config) { c.User = "" },
		"password": func(c *murmur.Config) { c.Password = "" },
		"key":      func(c *murmur.Config) { c.Key = "" },
		"port":     func(c *murmur.Config) { c.Port = 70000 },
		"tls":      func(c *murmur.Config) { c.TLS.Mode = "sometimes" },
		"ca":       func(c *murmur.Config) { c.TLS.Mode = murmur.TLSVerifyFull },
		"certkey":  func(c *murmur.Config) { c.TLS.CertFile = "only-cert" },
	} {
		cfg := base
		mutate(&cfg)
		if _, err := murmur.Open(ctx, cfg); err == nil {
			t.Fatalf("%s: expected validation error", name)
		}
	}
}

func TestBackupSaveListDownload(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	db := open(t, testConfig(server))
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE docs(id INTEGER PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO docs VALUES (1, 'saved')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var saved struct {
		Name string
		Size int64
	}
	err := db.QueryRowContext(ctx, "MURMUR BACKUP SAVE").Scan(&saved.Name, &saved.Size)
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if !strings.HasPrefix(saved.Name, "murmur-") || !strings.HasSuffix(saved.Name, ".murmur-backup") {
		t.Fatalf("name = %q", saved.Name)
	}
	if saved.Size <= 0 {
		t.Fatalf("size = %d", saved.Size)
	}
	files, err := sqlRows(ctx, db, "MURMUR BACKUP LIST")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(files) != 1 || files[0]["name"].(string) != saved.Name || files[0]["size"].(int64) != saved.Size {
		t.Fatalf("list = %+v", files)
	}
	if !strings.HasSuffix(files[0]["modified"].(string), "Z") {
		t.Fatalf("modified = %q", files[0]["modified"].(string))
	}
	var downloaded bytes.Buffer
	for offset := int64(0); ; {
		var actual, total int64
		var chunk []byte
		query := fmt.Sprintf("MURMUR BACKUP DOWNLOAD '%s' OFFSET %d LIMIT 1048576", saved.Name, offset)
		if err := db.QueryRowContext(ctx, query).Scan(&actual, &total, &chunk); err != nil {
			t.Fatal(err)
		}
		if actual != offset {
			t.Fatalf("offset %d, want %d", actual, offset)
		}
		downloaded.Write(chunk)
		offset += int64(len(chunk))
		if offset >= total {
			break
		}
		if len(chunk) == 0 {
			t.Fatal("empty nonfinal chunk")
		}
	}
	onDisk, err := os.ReadFile(filepath.Join(dir, "backups", saved.Name))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(downloaded.Bytes(), onDisk) {
		t.Fatalf("downloaded %d bytes, file has %d", downloaded.Len(), len(onDisk))
	}
	if _, err := db.ExecContext(ctx, "MURMUR BACKUP DOWNLOAD 'absent.murmur-backup'"); err == nil {
		t.Fatal("expected missing-file error")
	} else if codeOf(err) != "58P01" {
		t.Fatalf("code = %q", codeOf(err))
	}
}

func TestBackupUploadRoundtrip(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	db := open(t, testConfig(server))
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE docs(id INTEGER PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	payload := make([]byte, 700_000)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	if _, err := db.ExecContext(ctx, "MURMUR BACKUP UPLOAD BEGIN 'big.murmur-backup'"); err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < len(payload); offset += 256 << 10 {
		end := min(offset+(256<<10), len(payload))
		query := "MURMUR BACKUP UPLOAD APPEND 'big.murmur-backup' '" + base64.StdEncoding.EncodeToString(payload[offset:end]) + "'"
		if _, err := db.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, "MURMUR BACKUP UPLOAD FINISH 'big.murmur-backup'"); err != nil {
		t.Fatal(err)
	}
	var downloaded []byte
	for offset := int64(0); ; {
		var actual, total int64
		var chunk []byte
		query := fmt.Sprintf("MURMUR BACKUP DOWNLOAD 'big.murmur-backup' OFFSET %d LIMIT 131072", offset)
		if err := db.QueryRowContext(ctx, query).Scan(&actual, &total, &chunk); err != nil {
			t.Fatal(err)
		}
		if actual != offset {
			t.Fatalf("offset %d, want %d", actual, offset)
		}
		downloaded = append(downloaded, chunk...)
		offset += int64(len(chunk))
		if offset >= total {
			break
		}
		if len(chunk) == 0 {
			t.Fatal("empty nonfinal chunk")
		}
	}
	if !bytes.Equal(downloaded, payload) {
		t.Fatal("roundtrip mismatch")
	}
	for _, bad := range []string{"", "../evil.murmur-backup", ".hidden", "a b"} {
		for _, verb := range []string{"UPLOAD BEGIN", "DOWNLOAD"} {
			query := "MURMUR BACKUP " + verb + " '" + strings.ReplaceAll(bad, "'", "''") + "'"
			if _, err := db.ExecContext(ctx, query); codeOf(err) != "22023" {
				t.Fatalf("%s: %v", query, err)
			}
		}
	}
}

func TestRestoreCycle(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	db := open(t, testConfig(server))
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE docs(id INTEGER PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO docs VALUES (1, 'before')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var saved struct {
		Name string
		Size int64
	}
	err := db.QueryRowContext(ctx, "MURMUR BACKUP SAVE").Scan(&saved.Name, &saved.Size)
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO docs VALUES (2, 'after')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Wrong key first: the live data must survive.
	if _, err := db.ExecContext(ctx, "MURMUR RESTORE FROM "+"'"+strings.ReplaceAll(saved.Name, "'", "''")+"'"+" KEY 'wrong-key'"); err == nil {
		t.Fatal("expected wrong-key failure")
	} else if codeOf(err) != "28000" {
		t.Fatalf("code = %q", codeOf(err))
	}
	if _, err := db.ExecContext(ctx, "MURMUR RESTORE FROM "+"'"+strings.ReplaceAll(saved.Name, "'", "''")+"'"+" KEY "+"'"+strings.ReplaceAll(testKey, "'", "''")+"'"); err != nil {
		t.Fatalf("restore: %v", err)
	}
	rows, err := db.QueryContext(ctx, "SELECT v FROM docs ORDER BY id")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		values = append(values, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if len(values) != 1 || values[0] != "before" {
		t.Fatalf("values = %q", values)
	}
}

func TestRotateKey(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	db := open(t, testConfig(server))
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE docs(id INTEGER PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO docs VALUES (1, 'kept')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	const newKey = "rotated-go-key"
	if _, err := db.ExecContext(ctx, "MURMUR ROTATE KEY "+"'"+strings.ReplaceAll(testKey, "'", "''")+"'"+" "+"'"+strings.ReplaceAll(newKey, "'", "''")+"'"); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	// Wrong previous key and identical keys fail; the rotation above stands.
	if _, err := db.ExecContext(ctx, "MURMUR ROTATE KEY 'wrong' 'other'"); err == nil {
		t.Fatal("expected wrong-previous failure")
	} else if codeOf(err) != "28000" {
		t.Fatalf("code = %q", codeOf(err))
	}
	if _, err := db.ExecContext(ctx, "MURMUR ROTATE KEY "+"'"+strings.ReplaceAll(newKey, "'", "''")+"'"+" "+"'"+strings.ReplaceAll(newKey, "'", "''")+"'"); err == nil {
		t.Fatal("expected identical-keys failure")
	}
	db.Close()
	// The old key no longer opens; the new key does with data intact.
	stale := testConfig(server)
	if _, err := murmur.Open(ctx, stale); err == nil {
		t.Fatal("expected old-key failure")
	}
	fresh := testConfig(server)
	fresh.Key = newKey
	db2 := open(t, fresh)
	var value string
	if err := db2.QueryRowContext(ctx, "SELECT v FROM docs WHERE id = 1").Scan(&value); err != nil {
		t.Fatalf("select: %v", err)
	}
	if value != "kept" {
		t.Fatalf("value = %q", value)
	}
}

func TestManagedReplicationKeys(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	db := open(t, testConfig(server))
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE identities(tenant TEXT NOT NULL, id TEXT NOT NULL, v TEXT, PRIMARY KEY(tenant,id))"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO identities VALUES($1,$2,$3)", "a", "app-uuid", "local"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "MURMUR SET TABLE 'identities' REPLICATED true"); err != nil {
		t.Fatal(err)
	}
	var first string
	if err := db.QueryRowContext(ctx, "SELECT id_murmur_primary_key FROM identities WHERE tenant=$1 AND id=$2", "a", "app-uuid").Scan(&first); err != nil || len(first) != 36 || first[14] != '5' {
		t.Fatalf("UUIDv5 = %q, error %v", first, err)
	}
	statement, err := db.PrepareContext(ctx, "INSERT INTO identities(tenant,id,v) VALUES($1,$2,$3) RETURNING id_murmur_primary_key")
	if err != nil {
		t.Fatal(err)
	}
	var second string
	if err := statement.QueryRowContext(ctx, "b", "app-uuid", "prepared").Scan(&second); err != nil || first == second {
		t.Fatalf("prepared UUID: %q, %v", second, err)
	}
	statement.Close()
	for _, tc := range []struct{ sql, code string }{
		{"UPDATE identities SET id_murmur_primary_key='forged'", "42501"},
		{"INSERT INTO identities(tenant,id,id_murmur_primary_key) VALUES('c','x','forged')", "42501"},
		{"UPDATE identities SET id='changed'", "42501"},
		{"INSERT INTO identities(tenant,v) VALUES('c','missing')", "23502"},
		{"INSERT INTO identities(tenant,id) VALUES('c',NULL)", "23502"},
		{"INSERT INTO identities(tenant,id) VALUES('a','app-uuid')", "23505"},
	} {
		if _, err := db.ExecContext(ctx, tc.sql); codeOf(err) != tc.code {
			t.Errorf("%s: code %q, want %q: %v", tc.sql, codeOf(err), tc.code, err)
		}
	}
	if _, err := db.ExecContext(ctx, "MURMUR FLUSH"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	server.Stop()
	restarted := servertest.Start(t, dir, servertest.Options{})
	db2 := open(t, testConfig(restarted))
	var recovered string
	if err := db2.QueryRowContext(ctx, "SELECT id_murmur_primary_key FROM identities WHERE tenant='a'").Scan(&recovered); err != nil || recovered != first {
		t.Fatalf("recovered %q, want %q: %v", recovered, first, err)
	}
	if _, err := db2.ExecContext(ctx, "MURMUR SET TABLE 'identities' REPLICATED false"); err != nil {
		t.Fatal(err)
	}
	if _, err := db2.ExecContext(ctx, "UPDATE identities SET id='managed-by-app' WHERE tenant='a'"); err != nil {
		t.Fatalf("local application key update: %v", err)
	}
}

func TestMetrics(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	db := open(t, testConfig(server))
	ctx := context.Background()

	metrics, err := sqlRows(ctx, db, "MURMUR SHOW METRICS")
	if err != nil {
		t.Fatalf("Metrics: %v", err)
	}
	if len(metrics) == 0 {
		t.Fatal("expected non-empty metrics")
	}
	subsystems := map[string]bool{}
	for _, m := range metrics {
		subsystems[m["subsystem"].(string)] = true
	}
	if !subsystems["sqlite"] || !subsystems["spool"] {
		t.Fatalf("expected both sqlite and spool subsystems, got: %v", subsystems)
	}

	sqliteMetrics, err := sqlRows(ctx, db, "MURMUR SHOW METRICS SQLITE")
	if err != nil {
		t.Fatalf("Metrics(sqlite): %v", err)
	}
	if len(sqliteMetrics) == 0 {
		t.Fatal("expected sqlite metrics")
	}
	for _, m := range sqliteMetrics {
		if m["subsystem"].(string) != "sqlite" {
			t.Fatalf("expected subsystem sqlite, got %s", m["subsystem"].(string))
		}
	}

	spoolMetrics, err := sqlRows(ctx, db, "MURMUR SHOW METRICS SPOOL")
	if err != nil {
		t.Fatalf("Metrics(spool): %v", err)
	}
	if len(spoolMetrics) == 0 {
		t.Fatal("expected spool metrics")
	}
	for _, m := range spoolMetrics {
		if m["subsystem"].(string) != "spool" {
			t.Fatalf("expected subsystem spool, got %s", m["subsystem"].(string))
		}
	}

	if _, err := sqlRows(ctx, db, "MURMUR SHOW METRICS INVALID"); err == nil {
		t.Fatal("expected error for invalid scope")
	} else if codeOf(err) != "42601" {
		t.Fatalf("got code %q, want 42601", codeOf(err))
	}
}

func TestDiagnosticStatusUsesOrdinarySQL(t *testing.T) {
	server := servertest.Start(t, t.TempDir(), servertest.Options{})
	db := open(t, testConfig(server))
	ctx := context.Background()
	var document string
	if err := db.QueryRowContext(ctx, "MURMUR SHOW STATUS").Scan(&document); err != nil {
		t.Fatal(err)
	}
	var snapshot map[string]json.RawMessage
	if err := json.Unmarshal([]byte(document), &snapshot); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"committed", "durable", "scheduler", "spool"} {
		if _, present := snapshot[field]; !present {
			t.Fatalf("missing diagnostic field %s", field)
		}
	}
}

func TestIndependentErrorsAcrossPoolOperations(t *testing.T) {
	server := servertest.Start(t, t.TempDir(), servertest.Options{})
	db := open(t, testConfig(server))
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE independent_errors(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	stmt, err := db.PrepareContext(ctx, "INSERT INTO independent_errors VALUES ($1)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	if _, err := stmt.ExecContext(ctx, 1); err != nil {
		t.Fatal(err)
	}
	_, err = stmt.ExecContext(ctx, 1)
	var failure *murmur.Error
	if !errors.As(fmt.Errorf("application: %w", err), &failure) || failure.Code != "23505" || failure.SQLState() != "23505" || failure.Message == "" {
		t.Fatalf("prepared error: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT * FROM missing_independent_table").Scan(new(int)); codeOf(err) != "42P01" {
		t.Fatalf("query row error: %v", err)
	}
	if _, err := db.PrepareContext(ctx, "SELEC broken"); codeOf(err) != "42601" {
		t.Fatalf("prepare error: %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM independent_errors").Scan(&count); err != nil || count != 1 {
		t.Fatalf("pool unusable after rejection: %d %v", count, err)
	}
}

package murmur_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcgauthier/murmur-sql/clients/go"
	"github.com/marcgauthier/murmur-sql/clients/go/internal/servertest"
)

// openFailoverMesh starts two replicated servers on fixed ports (so the
// primary can restart on its address) and returns direct pools plus a
// failover pool pinned to server 0 with server 1 as standby.
func openFailoverMesh(t *testing.T) (servers []*servertest.Server, direct []*sql.DB, pool *sql.DB, opts []servertest.Options) {
	t.Helper()
	ctx := context.Background()
	base := t.TempDir()
	dirs := []string{filepath.Join(base, "n0"), filepath.Join(base, "n1")}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	opts = servertest.MeshOptions(t, dirs, "")
	opts[0].Port = servertest.FreePort(t)
	opts[1].Port = servertest.FreePort(t)
	for i, dir := range dirs {
		servers = append(servers, servertest.Start(t, dir, opts[i]))
		cfg := testConfig(servers[i])
		cfg.Key = "mesh-key"
		direct = append(direct, open(t, cfg))
	}
	addrs := make([]string, 2)
	for i, db := range direct {
		rows, err := sqlRows(ctx, db, "MURMUR SHOW REPLICATION")
		if err != nil {
			t.Fatalf("status: %v", err)
		}
		addrs[i] = rows[0]["address"].(string)
	}
	if _, err := direct[0].ExecContext(ctx, "MURMUR PEER ADD "+quoteArg(addrs[1])); err != nil {
		t.Fatalf("peer 0->1: %v", err)
	}
	if _, err := direct[1].ExecContext(ctx, "MURMUR PEER ADD "+quoteArg(addrs[0])); err != nil {
		t.Fatalf("peer 1->0: %v", err)
	}
	for _, db := range direct {
		if _, err := db.ExecContext(ctx, `CREATE TABLE jobs(id INTEGER PRIMARY KEY, body TEXT NOT NULL)`); err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := db.ExecContext(ctx, "MURMUR SET TABLE jobs REPLICATED true"); err != nil {
			t.Fatalf("replicate: %v", err)
		}
	}
	cfg := testConfig(servers[0])
	cfg.Key = "mesh-key"
	cfg.Standbys = []murmur.HostPort{{Host: "127.0.0.1", Port: servers[1].Port}}
	pool = open(t, cfg)
	return servers, direct, pool, opts
}

// insertIDs writes ids through db, treating unique violations as
// acknowledged: a retried write that already landed is still landed.
func insertIDs(t *testing.T, ctx context.Context, db *sql.DB, ids []int64) {
	t.Helper()
	for _, id := range ids {
		_, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO jobs(id, body) VALUES(%d, 'job-%d')", id, id))
		if err != nil {
			if codeOf(err) == "23505" {
				continue
			}
			t.Fatalf("insert %d: %v", id, err)
		}
	}
}

// flushAndWait flushes the writer and waits for its head on the reader,
// so later kills cannot take unflushed rows with them.
func flushAndWait(t *testing.T, ctx context.Context, writer, reader *sql.DB, writerNode string) {
	t.Helper()
	if _, err := writer.ExecContext(ctx, "MURMUR FLUSH"); err != nil {
		t.Fatalf("flush: %v", err)
	}
	heads, err := sqlRows(ctx, writer, "MURMUR SHOW HEAD")
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	var seq int64 = -1
	for _, h := range heads {
		if h["origin"].(string) == writerNode {
			seq = h["applied"].(int64)
		}
	}
	if seq <= 0 {
		t.Fatalf("no writer head in %+v", heads)
	}
	if _, err := reader.ExecContext(ctx,
		fmt.Sprintf("MURMUR WAIT FOR %s %d TIMEOUT 60", quoteArg(writerNode), seq)); err != nil {
		t.Fatalf("wait: %v", err)
	}
}

func countIDs(t *testing.T, ctx context.Context, db *sql.DB) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

// TestClientFailoverAcrossOutage pins Go client failover: killing the
// primary mid-workload neither fails writes nor loses acknowledged ids;
// the restarted primary reconverges and takes traffic back.
func TestClientFailoverAcrossOutage(t *testing.T) {
	ctx := context.Background()
	servers, direct, pool, opts := openFailoverMesh(t)

	insertIDs(t, ctx, pool, []int64{1, 2, 3, 4, 5})
	flushAndWait(t, ctx, direct[0], direct[1], opts[0].NodeID)
	servers[0].Stop()
	t.Logf("primary stopped; continuing on standby")
	insertIDs(t, ctx, pool, []int64{6, 7, 8, 9, 10})

	if n := countIDs(t, ctx, direct[1]); n != 10 {
		t.Fatalf("survivor has %d rows, want 10", n)
	}
	flushAndWait(t, ctx, direct[1], direct[1], opts[1].NodeID)

	servers[0] = servertest.Start(t, servers[0].Dir, opts[0])
	t.Logf("primary restarted; waiting for reconvergence")
	heads, err := sqlRows(ctx, direct[1], "MURMUR SHOW HEAD")
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	var origin string
	var seq int64
	for _, h := range heads {
		if h["origin"].(string) == opts[1].NodeID {
			origin = opts[1].NodeID
			seq = h["applied"].(int64)
		}
	}
	if origin == "" {
		t.Fatalf("no head for %s in %+v", opts[1].NodeID, heads)
	}
	// The restarted node lost its runtime peers; re-peer both ways, then
	// wait for the survivor's writes to arrive.
	rows, err := sqlRows(ctx, direct[1], "MURMUR SHOW REPLICATION")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	rows0, err := sqlRows(ctx, direct[0], "MURMUR SHOW REPLICATION")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	// direct[0] still points at the old process's pool: fresh pool.
	dbs0 := open(t, testConfigWithKey(t, servers[0], "mesh-key"))
	if _, err := dbs0.ExecContext(ctx, "MURMUR PEER ADD "+quoteArg(rows[0]["address"].(string))); err != nil {
		t.Fatalf("re-peer 0->1: %v", err)
	}
	if _, err := direct[1].ExecContext(ctx, "MURMUR PEER ADD "+quoteArg(rows0[0]["address"].(string))); err != nil {
		t.Fatalf("re-peer 1->0: %v", err)
	}
	if _, err := dbs0.ExecContext(ctx,
		fmt.Sprintf("MURMUR WAIT FOR %s %d TIMEOUT 60", quoteArg(origin), seq)); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if n := countIDs(t, ctx, dbs0); n != 10 {
		t.Fatalf("restarted primary has %d rows, want 10", n)
	}

	// Fail-back: with the primary healthy again, the pool returns to it.
	// Killing the standby must not disturb writes.
	servers[1].Stop()
	t.Logf("standby stopped; primary takes over again")
	insertIDs(t, ctx, pool, []int64{11, 12, 13})
	if n := countIDs(t, ctx, dbs0); n != 13 {
		t.Fatalf("primary has %d rows, want 13", n)
	}
}

// TestOpenStandbyValidation pins standby address validation without
// dialing anything.
func TestOpenStandbyValidation(t *testing.T) {
	cfg := murmur.Config{User: "u", Password: "p", Key: "k", Standbys: []murmur.HostPort{{Port: -1}}}
	if _, err := murmur.Open(context.Background(), cfg); err == nil {
		t.Fatal("invalid standby port accepted, want error")
	}
}

func testConfigWithKey(t *testing.T, server *servertest.Server, key string) murmur.Config {
	t.Helper()
	cfg := testConfig(server)
	cfg.Key = key
	return cfg
}

package murmur_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/clients/go/internal/servertest"
)

func TestReplicationCommandsNeedEnabledReplication(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	db := open(t, testConfig(server))
	ctx := context.Background()
	for _, query := range []string{
		"MURMUR SHOW PEERS", "MURMUR SHOW REPLICATION", "MURMUR SHOW RETENTION",
		"MURMUR RETENTION COLLECT", "MURMUR PEER SYNC", "MURMUR PEER ADD '127.0.0.1:9'",
		"MURMUR POLICY COUNTER_ADD t c 'k' 1", "MURMUR BRIDGE RELEASE t 'k'",
		"MURMUR BRIDGE RELEASE t 'k' COLUMN c",
	} {
		_, err := db.ExecContext(ctx, query)
		if codeOf(err) != "55000" {
			t.Fatalf("%s: expected 55000, got %v", query, err)
		}
	}
	// Server-side usage errors surface before the replication gate.
	if _, err := db.ExecContext(ctx, "MURMUR PEER ADD 'bogus'"); codeOf(err) != "22023" {
		t.Fatalf("raw bad address: code = %q, want 22023 (%v)", codeOf(err), err)
	}
}

func meshDBs(t *testing.T, dirs []string, replicationExtra string) ([]*servertest.Server, []*sql.DB, []servertest.Options) {
	t.Helper()
	opts := servertest.MeshOptions(t, dirs, replicationExtra)
	servers := make([]*servertest.Server, len(dirs))
	dbs := make([]*sql.DB, len(dirs))
	for i, dir := range dirs {
		servers[i] = servertest.Start(t, dir, opts[i])
		cfg := testConfig(servers[i])
		cfg.Key = "mesh-key"
		dbs[i] = open(t, cfg)
	}
	return servers, dbs, opts
}

func TestPeerMeshShowAndRetention(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	dirs := []string{filepath.Join(base, "n0"), filepath.Join(base, "n1")}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	_, dbs, opts := meshDBs(t, dirs, "")
	node0, node1 := opts[0].NodeID, opts[1].NodeID
	status, err := sqlRows(ctx, dbs[0], "MURMUR SHOW REPLICATION")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if (len(status) != 1 || status[0]["running"] != "true") || status[0]["peers"].(int64) != 0 {
		t.Fatalf("status = %+v, want running with 0 peers", status)
	}
	peerStatus, err := sqlRows(ctx, dbs[1], "MURMUR SHOW REPLICATION")
	if err != nil || (len(peerStatus) != 1 || peerStatus[0]["address"] == nil) {
		t.Fatalf("peer status = %+v, %v", peerStatus, err)
	}
	if peers, err := sqlRows(ctx, dbs[0], "MURMUR SHOW PEERS"); err != nil || len(peers) != 0 {
		t.Fatalf("peers = %+v, %v", peers, err)
	}

	if _, err := dbs[0].ExecContext(ctx, "MURMUR PEER ADD "+"'"+strings.ReplaceAll(peerStatus[0]["address"].(string), "'", "''")+"'"); err != nil {
		t.Fatalf("add: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		peers, err := sqlRows(ctx, dbs[0], "MURMUR SHOW PEERS")
		if err != nil {
			t.Fatalf("peers: %v", err)
		}
		connected := false
		for _, p := range peers {
			if p["node_id"].(string) == node1 && p["connected"] == "true" {
				connected = true
				if p["sent_watermark"].(int64) < 0 {
					t.Fatalf("peer = %+v", p)
				}
			}
		}
		if connected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("peer never connected: %+v", peers)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, err := dbs[0].ExecContext(ctx, "MURMUR PEER SYNC"); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if status, err := sqlRows(ctx, dbs[0], "MURMUR SHOW REPLICATION"); err != nil || status[0]["peers"].(int64) < 1 {
		t.Fatalf("status = %+v, %v", status, err)
	}
	var collected int64
	if err := dbs[0].QueryRowContext(ctx, "MURMUR RETENTION COLLECT").Scan(&collected); err != nil || collected < 0 {
		t.Fatalf("collect = %d, %v", collected, err)
	}
	entries, err := sqlRows(ctx, dbs[0], "MURMUR SHOW RETENTION")
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	for _, e := range entries {
		if len(e["node_id"].(string)) != 32 || e["collected_transactions"].(int64) < 0 {
			t.Fatalf("entry = %+v", e)
		}
	}

	if _, err := dbs[0].ExecContext(ctx, "MURMUR PEER REMOVE "+"'"+strings.ReplaceAll(node1, "'", "''")+"'"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := dbs[0].ExecContext(ctx, "MURMUR PEER RETIRE "+"'"+strings.ReplaceAll(node1, "'", "''")+"'"); err != nil {
		t.Fatalf("retire: %v", err)
	}
	entries, err = sqlRows(ctx, dbs[0], "MURMUR SHOW RETENTION")
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	retired := false
	for _, e := range entries {
		if e["node_id"].(string) == node1 && e["retired"] == "true" {
			retired = true
		}
	}
	if !retired {
		t.Fatalf("retired peer missing from retention: %+v", entries)
	}
	if _, err := dbs[0].ExecContext(ctx, "MURMUR PEER RETIRE "+"'"+strings.ReplaceAll(node0, "'", "''")+"'"); codeOf(err) != "22023" {
		t.Fatalf("self-retire: code = %q, want 22023 (%v)", codeOf(err), err)
	}
}

func TestPolicyOperations(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	policies := "  merge_policies:\n" +
		"    \"stats.count\": \"counter\"\n" +
		"    \"stats.tags\": \"set\"\n" +
		"    \"stats.highest\": \"max\"\n" +
		"    \"stats.lowest\": \"min\"\n"
	_, dbs, _ := meshDBs(t, []string{dir}, policies)
	db := dbs[0]
	if _, err := db.ExecContext(ctx, `CREATE TABLE stats(key TEXT NOT NULL PRIMARY KEY,
		count INTEGER NOT NULL, tags BLOB, highest INTEGER NOT NULL, lowest REAL NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO stats VALUES ($1, $2, NULL, $3, $4)",
		"row-1", 10, 5, 2.5); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.ExecContext(ctx, "MURMUR POLICY COUNTER_ADD 'stats' 'count' 'row-1' 5"); err != nil {
		t.Fatalf("counter: %v", err)
	}
	if _, err := db.ExecContext(ctx, "MURMUR POLICY SET_ADD 'stats' 'tags' 'row-1' 'blue'"); err != nil {
		t.Fatalf("set add: %v", err)
	}
	if _, err := db.ExecContext(ctx, "MURMUR POLICY SET_REMOVE 'stats' 'tags' 'row-1' 'blue'"); err != nil {
		t.Fatalf("set remove: %v", err)
	}
	if _, err := db.ExecContext(ctx, "MURMUR POLICY MAX 'stats' 'highest' 'row-1' 42"); err != nil {
		t.Fatalf("max: %v", err)
	}
	if _, err := db.ExecContext(ctx, "MURMUR POLICY MAX 'stats' 'highest' 'row-1' 1"); err != nil {
		t.Fatalf("max: %v", err)
	}
	if _, err := db.ExecContext(ctx, "MURMUR POLICY MIN 'stats' 'lowest' 'row-1' -1.5"); err != nil {
		t.Fatalf("min: %v", err)
	}
	var count, highest int64
	var lowest float64
	if err := db.QueryRowContext(ctx,
		"SELECT count, highest, lowest FROM stats WHERE key = $1", "row-1").Scan(&count, &highest, &lowest); err != nil {
		t.Fatalf("select: %v", err)
	}
	if count != 15 || highest != 42 || lowest != -1.5 {
		t.Fatalf("got %d/%d/%v, want 15/42/-1.5", count, highest, lowest)
	}
	if _, err := db.ExecContext(ctx, "MURMUR POLICY COUNTER_ADD 'stats' 'highest' 'row-1' 1"); codeOf(err) != "22023" {
		t.Fatalf("mismatch: code = %q, want 22023 (%v)", codeOf(err), err)
	}
	if _, err := db.ExecContext(ctx, "MURMUR POLICY COUNTER_ADD 'stats' 'count' 'missing' 1"); codeOf(err) != "02000" {
		t.Fatalf("missing: code = %q, want 02000 (%v)", codeOf(err), err)
	}
}

// bridgeOptions provisions one replication node plus a [replication.bridge]
// section. role is "high-receiver" or "low-exporter"; keyYAML carries the
// role's key lines. No bundles are exchanged, so trust values are arbitrary
// distinct test constants; domain must equal the node's own database id.
func bridgeOptions(t *testing.T, dir, role, keyYAML string) servertest.Options {
	t.Helper()
	opts := servertest.MeshOptions(t, []string{dir}, "")
	o := opts[0]
	o.ExtraYAML += fmt.Sprintf("  bridge:\n    role: %q\n    domain: %q\n"+
		"    trusted_domains:\n      - %q\n    directory: %q\n    stream: \"test-stream\"\n%s",
		role, o.DatabaseID, strings.Repeat("11", 16), filepath.Join(dir, "staging"), keyYAML)
	return o
}

func TestBridgeRelease(t *testing.T) {
	ctx := context.Background()
	highDir := t.TempDir()
	highOpts := bridgeOptions(t, highDir, "high-receiver",
		fmt.Sprintf("    recipient: %q\n    receiver_secret_hex: %q\n",
			strings.Repeat("ee", 32), strings.Repeat("dd", 32)))
	high := open(t, testConfig(servertest.Start(t, highDir, highOpts)))
	if _, err := high.ExecContext(ctx, `CREATE TABLE notes(name TEXT NOT NULL PRIMARY KEY,
		title TEXT NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := high.ExecContext(ctx, "INSERT INTO notes VALUES ($1, $2)", "r1", "v1"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := high.ExecContext(ctx, "MURMUR FLUSH"); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// The High write above claimed ownership of title; the first release
	// reports true and a second finds nothing owned.
	var released bool
	err := high.QueryRowContext(ctx, "MURMUR BRIDGE RELEASE notes 'r1' COLUMN title").Scan(&released)
	if err != nil || !released {
		t.Fatalf("release: released=%v, err=%v", released, err)
	}
	if _, err := high.ExecContext(ctx, "MURMUR BRIDGE RELEASE notes 'r1' COLUMN title"); codeOf(err) != "22023" {
		t.Fatalf("re-release: code = %q, want 22023 (%v)", codeOf(err), err)
	}
	// Row release without COLUMN addresses whole-row ownership, which a
	// plain insert never creates.
	if _, err := high.ExecContext(ctx, "MURMUR BRIDGE RELEASE notes 'r1'"); codeOf(err) != "22023" {
		t.Fatalf("row release: code = %q, want 22023 (%v)", codeOf(err), err)
	}
	if _, err := high.ExecContext(ctx, "MURMUR BRIDGE RELEASE missing 'r1' COLUMN title"); codeOf(err) != "22023" {
		t.Fatalf("unknown table: code = %q, want 22023 (%v)", codeOf(err), err)
	}
	if _, err := high.ExecContext(ctx, "MURMUR BRIDGE RELEASE notes 'r1' COLUMN missing"); codeOf(err) != "22023" {
		t.Fatalf("unknown column: code = %q, want 22023 (%v)", codeOf(err), err)
	}

	// Replication without a bridge is gated with 55000.
	plainDir := t.TempDir()
	plainOpts := servertest.MeshOptions(t, []string{plainDir}, "")
	plain := open(t, testConfig(servertest.Start(t, plainDir, plainOpts[0])))
	if _, err := plain.ExecContext(ctx, "MURMUR BRIDGE RELEASE t 'k'"); codeOf(err) != "55000" {
		t.Fatalf("no bridge: code = %q, want 55000 (%v)", codeOf(err), err)
	}

	// A Low exporter rejects even a well-formed address with 22023.
	lowDir := t.TempDir()
	lowOpts := bridgeOptions(t, lowDir, "low-exporter",
		fmt.Sprintf("    recipient: %q\n    signer_hex: %q\n",
			strings.Repeat("ee", 32), strings.Repeat("bb", 32)))
	low := open(t, testConfig(servertest.Start(t, lowDir, lowOpts)))
	if _, err := low.ExecContext(ctx, "CREATE TABLE notes(name TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)"); err != nil {
		t.Fatalf("low create: %v", err)
	}
	if _, err := low.ExecContext(ctx, "MURMUR BRIDGE RELEASE notes 'r1' COLUMN title"); codeOf(err) != "22023" {
		t.Fatalf("exporter: code = %q, want 22023 (%v)", codeOf(err), err)
	}
}

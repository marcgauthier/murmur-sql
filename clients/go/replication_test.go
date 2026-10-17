package murmur_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/internal/servertest"
)

func TestReplicationCommandsNeedEnabledReplication(t *testing.T) {
	dir := t.TempDir()
	server := servertest.Start(t, dir, servertest.Options{})
	db := open(t, testConfig(server))
	ctx := context.Background()
	cases := map[string]error{}
	var err error
	_, err = db.ShowPeers(ctx)
	cases["peers"] = err
	_, err = db.ShowReplication(ctx)
	cases["replication"] = err
	_, err = db.ShowRetention(ctx)
	cases["retention"] = err
	_, err = db.RetentionCollect(ctx)
	cases["collect"] = err
	cases["sync"] = db.PeerSync(ctx)
	cases["add"] = db.PeerAdd(ctx, "127.0.0.1:9")
	cases["counter"] = db.PolicyCounterAdd(ctx, "t", "c", "k", 1)
	_, err = db.BridgeRelease(ctx, "t", "k")
	cases["release-row"] = err
	_, err = db.BridgeRelease(ctx, "t", "k", "c")
	cases["release-field"] = err
	for name, err := range cases {
		if err == nil {
			t.Fatalf("%s: expected 55000 failure", name)
		}
		if code := murmur.CodeOf(err); code != "55000" {
			t.Fatalf("%s: code = %q, want 55000 (%v)", name, code, err)
		}
	}
	// Client-side validation fails before any SQL is sent.
	if err := db.PeerAdd(ctx, "not-an-addr"); err == nil {
		t.Fatal("PeerAdd(bogus): expected validation error")
	}
	if err := db.PeerRemove(ctx, "short"); err == nil {
		t.Fatal("PeerRemove(short): expected validation error")
	}
	if err := db.PolicyMax(ctx, "s", "c", "k", math.NaN()); err == nil {
		t.Fatal("PolicyMax(NaN): expected validation error")
	}
	if err := db.PolicyMin(ctx, "", "c", "k", 1); err == nil {
		t.Fatal("PolicyMin(empty table): expected validation error")
	}
	if _, err := db.BridgeRelease(ctx, "", "k"); err == nil {
		t.Fatal("BridgeRelease(empty table): expected validation error")
	}
	if _, err := db.BridgeRelease(ctx, "t", "k", " "); err == nil {
		t.Fatal("BridgeRelease(empty column): expected validation error")
	}
	if _, err := db.BridgeRelease(ctx, "t", "k", "a", "b"); err == nil {
		t.Fatal("BridgeRelease(two columns): expected validation error")
	}
	// Server-side usage errors surface before the replication gate.
	if _, err := db.ExecContext(ctx, "MURMUR PEER ADD 'bogus'"); murmur.CodeOf(err) != "22023" {
		t.Fatalf("raw bad address: code = %q, want 22023 (%v)", murmur.CodeOf(err), err)
	}
}

func meshDBs(t *testing.T, dirs []string, replicationTOML string) ([]*servertest.Server, []*murmur.DB, []servertest.Options) {
	t.Helper()
	opts := servertest.MeshOptions(t, dirs, replicationTOML)
	servers := make([]*servertest.Server, len(dirs))
	dbs := make([]*murmur.DB, len(dirs))
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
	status, err := dbs[0].ShowReplication(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !status.Running || status.Peers != 0 {
		t.Fatalf("status = %+v, want running with 0 peers", status)
	}
	peerStatus, err := dbs[1].ShowReplication(ctx)
	if err != nil || peerStatus.Address == nil {
		t.Fatalf("peer status = %+v, %v", peerStatus, err)
	}
	if peers, err := dbs[0].ShowPeers(ctx); err != nil || len(peers) != 0 {
		t.Fatalf("peers = %+v, %v", peers, err)
	}

	if err := dbs[0].PeerAdd(ctx, *peerStatus.Address); err != nil {
		t.Fatalf("add: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		peers, err := dbs[0].ShowPeers(ctx)
		if err != nil {
			t.Fatalf("peers: %v", err)
		}
		connected := false
		for _, p := range peers {
			if p.NodeID == node1 && p.Connected {
				connected = true
				if p.SentWatermark < 0 {
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
	if err := dbs[0].PeerSync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if status, err := dbs[0].ShowReplication(ctx); err != nil || status.Peers < 1 {
		t.Fatalf("status = %+v, %v", status, err)
	}
	if collected, err := dbs[0].RetentionCollect(ctx); err != nil || collected < 0 {
		t.Fatalf("collect = %d, %v", collected, err)
	}
	entries, err := dbs[0].ShowRetention(ctx)
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	for _, e := range entries {
		if len(e.NodeID) != 32 || e.CollectedTransactions < 0 {
			t.Fatalf("entry = %+v", e)
		}
	}

	if err := dbs[0].PeerRemove(ctx, node1); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := dbs[0].PeerRetire(ctx, node1); err != nil {
		t.Fatalf("retire: %v", err)
	}
	entries, err = dbs[0].ShowRetention(ctx)
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	retired := false
	for _, e := range entries {
		if e.NodeID == node1 && e.Retired {
			retired = true
		}
	}
	if !retired {
		t.Fatalf("retired peer missing from retention: %+v", entries)
	}
	if err := dbs[0].PeerRetire(ctx, node0); murmur.CodeOf(err) != "22023" {
		t.Fatalf("self-retire: code = %q, want 22023 (%v)", murmur.CodeOf(err), err)
	}
}

func TestPolicyOperations(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	policies := "merge_policies = { \"stats.count\" = \"counter\", \"stats.tags\" = \"set\", " +
		"\"stats.highest\" = \"max\", \"stats.lowest\" = \"min\" }\n"
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
	if err := db.PolicyCounterAdd(ctx, "stats", "count", "row-1", 5); err != nil {
		t.Fatalf("counter: %v", err)
	}
	if err := db.PolicySetAdd(ctx, "stats", "tags", "row-1", "blue"); err != nil {
		t.Fatalf("set add: %v", err)
	}
	if err := db.PolicySetRemove(ctx, "stats", "tags", "row-1", "blue"); err != nil {
		t.Fatalf("set remove: %v", err)
	}
	if err := db.PolicyMax(ctx, "stats", "highest", "row-1", 42); err != nil {
		t.Fatalf("max: %v", err)
	}
	if err := db.PolicyMax(ctx, "stats", "highest", "row-1", 1); err != nil {
		t.Fatalf("max: %v", err)
	}
	if err := db.PolicyMin(ctx, "stats", "lowest", "row-1", -1.5); err != nil {
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
	if err := db.PolicyCounterAdd(ctx, "stats", "highest", "row-1", 1); murmur.CodeOf(err) != "22023" {
		t.Fatalf("mismatch: code = %q, want 22023 (%v)", murmur.CodeOf(err), err)
	}
	if err := db.PolicyCounterAdd(ctx, "stats", "count", "missing", 1); murmur.CodeOf(err) != "02000" {
		t.Fatalf("missing: code = %q, want 02000 (%v)", murmur.CodeOf(err), err)
	}
}

// bridgeOptions provisions one replication node plus a [replication.bridge]
// section. role is "high-receiver" or "low-exporter"; keyTOML carries the
// role's key lines. No bundles are exchanged, so trust values are arbitrary
// distinct test constants; domain must equal the node's own database id.
func bridgeOptions(t *testing.T, dir, role, keyTOML string) servertest.Options {
	t.Helper()
	opts := servertest.MeshOptions(t, []string{dir}, "")
	o := opts[0]
	o.ExtraTOML += fmt.Sprintf("[replication.bridge]\nrole = %q\ndomain = %q\n"+
		"trusted_domains = [%q]\ndirectory = %q\nstream = \"test-stream\"\n%s",
		role, o.DatabaseID, strings.Repeat("11", 16), filepath.Join(dir, "staging"), keyTOML)
	return o
}

func TestBridgeRelease(t *testing.T) {
	ctx := context.Background()
	highDir := t.TempDir()
	highOpts := bridgeOptions(t, highDir, "high-receiver",
		fmt.Sprintf("recipient = %q\nreceiver_secret_hex = %q\n",
			strings.Repeat("ee", 32), strings.Repeat("dd", 32)))
	high := open(t, testConfig(servertest.Start(t, highDir, highOpts)))
	if _, err := high.ExecContext(ctx, `CREATE TABLE notes(name TEXT NOT NULL PRIMARY KEY,
		title TEXT NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := high.ExecContext(ctx, "INSERT INTO notes VALUES ($1, $2)", "r1", "v1"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := high.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// The High write above claimed ownership of title; the first release
	// reports true and a second finds nothing owned.
	released, err := high.BridgeRelease(ctx, "notes", "r1", "title")
	if err != nil || !released {
		t.Fatalf("release: released=%v, err=%v", released, err)
	}
	if _, err := high.BridgeRelease(ctx, "notes", "r1", "title"); murmur.CodeOf(err) != "22023" {
		t.Fatalf("re-release: code = %q, want 22023 (%v)", murmur.CodeOf(err), err)
	}
	// Row release without COLUMN addresses whole-row ownership, which a
	// plain insert never creates.
	if _, err := high.BridgeRelease(ctx, "notes", "r1"); murmur.CodeOf(err) != "22023" {
		t.Fatalf("row release: code = %q, want 22023 (%v)", murmur.CodeOf(err), err)
	}
	if _, err := high.BridgeRelease(ctx, "missing", "r1", "title"); murmur.CodeOf(err) != "22023" {
		t.Fatalf("unknown table: code = %q, want 22023 (%v)", murmur.CodeOf(err), err)
	}
	if _, err := high.BridgeRelease(ctx, "notes", "r1", "missing"); murmur.CodeOf(err) != "22023" {
		t.Fatalf("unknown column: code = %q, want 22023 (%v)", murmur.CodeOf(err), err)
	}

	// Replication without a bridge is gated with 55000.
	plainDir := t.TempDir()
	plainOpts := servertest.MeshOptions(t, []string{plainDir}, "")
	plain := open(t, testConfig(servertest.Start(t, plainDir, plainOpts[0])))
	if _, err := plain.BridgeRelease(ctx, "t", "k"); murmur.CodeOf(err) != "55000" {
		t.Fatalf("no bridge: code = %q, want 55000 (%v)", murmur.CodeOf(err), err)
	}

	// A Low exporter rejects even a well-formed address with 22023.
	lowDir := t.TempDir()
	lowOpts := bridgeOptions(t, lowDir, "low-exporter",
		fmt.Sprintf("recipient = %q\nsigner_hex = %q\n",
			strings.Repeat("ee", 32), strings.Repeat("bb", 32)))
	low := open(t, testConfig(servertest.Start(t, lowDir, lowOpts)))
	if _, err := low.ExecContext(ctx, "CREATE TABLE notes(name TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)"); err != nil {
		t.Fatalf("low create: %v", err)
	}
	if _, err := low.BridgeRelease(ctx, "notes", "r1", "title"); murmur.CodeOf(err) != "22023" {
		t.Fatalf("exporter: code = %q, want 22023 (%v)", murmur.CodeOf(err), err)
	}
}

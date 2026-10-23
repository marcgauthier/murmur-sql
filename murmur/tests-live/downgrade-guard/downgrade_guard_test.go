// Live scenario: downgrade guard refuses a newer store.
//
// Go port of the Rust `downgrade-guard` scenario
// (/home/marc/MURMUR/tests-live/downgrade-guard/tests/downgrade_guard.rs),
// itself a port of the original Go `downgrade-guard` scenario
// (TestDowngradeGuardRefusesNewStore). That suite builds the
// previous-release daemon from git history and requires it to refuse a
// store written by the current binary: loud version error, nonzero
// exit, byte-untouched store, both compatibility directions still
// working.
//
// There is no previous release to build, so the port inverts the
// mechanism and keeps the pass criteria: it crafts a store one format
// generation NEWER than the current build (the same `00/identity`
// record the product writes, re-encoded with `format + 1` through the
// raw spool API) and requires the current build to refuse it fail-closed
// with the `storage.go` FORMAT guard ("unsupported database format"),
// while the unmodified store reopens with an identical digest and
// identity. Plain SQL replaces the typed-records HTTP API.
package downgradeguard

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/compression"
	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/spool"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// key is the application key for the cluster node and every direct
// open below.
var key = [32]byte{
	0xd6, 0xd6, 0xd6, 0xd6, 0xd6, 0xd6, 0xd6, 0xd6,
	0xd6, 0xd6, 0xd6, 0xd6, 0xd6, 0xd6, 0xd6, 0xd6,
	0xd6, 0xd6, 0xd6, 0xd6, 0xd6, 0xd6, 0xd6, 0xd6,
	0xd6, 0xd6, 0xd6, 0xd6, 0xd6, 0xd6, 0xd6, 0xd6,
}

const (
	// rows is the seeded row count (Rust: `const ROWS = 25`).
	rows = 25
	// firstID is the first row id (Rust: ids `3000+i`).
	firstID = 3000
	// identityKey is the spool key of the store identity record.
	identityKey = "00/identity"
)

// spoolOpts builds raw spool options for a product-written dir: same
// key material as the node's opens plus the zstd read codec the
// product compresses new blocks with. New blocks written through these
// options use the spool default (deflate), which the product decodes
// natively, so the bumped store stays product-shaped.
func spoolOpts(dir string) spool.Options {
	opts := spool.DefaultOptions(dir)
	opts.MasterKey = append([]byte(nil), key[:]...)
	opts.Codecs = []compression.Codec{compression.MustZstd(3)}
	opts.ReclaimInterval = 0
	return opts
}

// readIdentity reads the `00/identity` record of a stopped store
// through the raw spool API.
func readIdentity(t *testing.T, dir string) murmur.Identity {
	t.Helper()
	store, err := spool.Open(spoolOpts(dir))
	if err != nil {
		t.Fatalf("spool open %s: %v", dir, err)
	}
	defer func() { _ = store.Close() }()
	raw, err := store.Get([]byte(identityKey))
	if err != nil {
		t.Fatalf("spool get 00/identity in %s: %v", dir, err)
	}
	if raw == nil {
		t.Fatalf("no 00/identity record in %s", dir)
	}
	ident, err := murmur.DecodeIdentity(raw)
	if err != nil {
		t.Fatalf("decode 00/identity: %v", err)
	}
	return ident
}

// bumpIdentityFormat overwrites the `00/identity` record with
// `format + 1`, returning the bumped identity. The store must be
// stopped (no open `Database`).
func bumpIdentityFormat(t *testing.T, dir string, current murmur.Identity) murmur.Identity {
	t.Helper()
	next := current
	next.Format = current.Format + 1
	store, err := spool.Open(spoolOpts(dir))
	if err != nil {
		t.Fatalf("spool open %s for format bump: %v", dir, err)
	}
	defer func() { _ = store.Close() }()
	if err := store.Commit([]spool.Mutation{{
		Key:   []byte(identityKey),
		Value: murmur.EncodeIdentity(next),
	}}, spool.DurabilitySync); err != nil {
		t.Fatalf("commit bumped identity: %v", err)
	}
	return next
}

// copyDirAll recursively copies a directory tree for the newer-store
// fixture (the pristine node dir must stay immutable).
func copyDirAll(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatalf("create %s: %v", dst, err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		from := filepath.Join(src, entry.Name())
		to := filepath.Join(dst, entry.Name())
		info, err := entry.Info()
		if err != nil {
			t.Fatalf("file info %s: %v", from, err)
		}
		if info.IsDir() {
			copyDirAll(t, from, to)
			continue
		}
		if !info.Mode().IsRegular() {
			continue
		}
		raw, err := os.ReadFile(from)
		if err != nil {
			t.Fatalf("read %s: %v", from, err)
		}
		if err := os.WriteFile(to, raw, info.Mode().Perm()); err != nil {
			t.Fatalf("copy %s: %v", from, err)
		}
	}
}

// hashTree maps every regular file under root as relpath -> sha256 hex.
func hashTree(t *testing.T, root string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(raw)
		out[rel] = fmt.Sprintf("%x", sum)
		return nil
	})
	if err != nil {
		t.Fatalf("hash tree %s: %v", root, err)
	}
	return out
}

// assertStoreUntouched requires the refused store to be
// content-untouched: every file present in both snapshots is
// byte-identical. Additions/removals are logged, not failed -- the
// refused open itself may stage lock files -- and the reopen digest
// below proves the logical data is identical.
func assertStoreUntouched(t *testing.T, before, after map[string]string) {
	t.Helper()
	var violations []string
	var rotation int
	for name, sum := range before {
		afterSum, ok := after[name]
		if !ok {
			rotation++
			t.Logf("  store file change: removed: %s", name)
			continue
		}
		if afterSum != sum {
			violations = append(violations, "modified: "+name)
		}
	}
	for name := range after {
		if _, ok := before[name]; !ok {
			rotation++
			t.Logf("  store file change: added: %s", name)
		}
	}
	if len(violations) > 0 {
		t.Fatalf("refused store was touched:\n%s", strings.Join(violations, "\n"))
	}
	t.Logf("refused store: no content mutated (%d files stable, %d added or removed)",
		len(before), rotation)
}

// rowCount counts rows on one node.
func rowCount(t *testing.T, db *murmur.Database) int {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT COUNT(*) FROM records")
	})
	if err != nil {
		t.Fatalf("row count: %v", err)
	}
	return int(v.([][]sqlite.Value)[0][0].Int)
}

// rowsDigest digests every row ordered by id (Rust: `typedContentionDigest`).
func rowsDigest(t *testing.T, db *murmur.Database) string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name FROM records ORDER BY id")
	})
	if err != nil {
		t.Fatalf("read rows: %v", err)
	}
	h := sha256.New()
	for _, row := range v.([][]sqlite.Value) {
		fmt.Fprintf(h, "%d:%s\n", row[0].Int, row[1].Str)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func hexID(id [16]byte) string {
	return fmt.Sprintf("%x", id)
}

// assertRefusesNewerStore requires the current build to refuse dir with
// the loud format-guard error.
func assertRefusesNewerStore(t *testing.T, dir string) {
	t.Helper()
	db, err := murmur.Open(dir, key[:], murmur.DefaultOptions())
	if err == nil {
		_ = db.Close()
		t.Fatalf("current build opened a newer-format store, want a loud refusal")
	}
	var murmurErr *murmur.Error
	if !errors.As(err, &murmurErr) || murmurErr.Kind != murmur.KindCorrupt ||
		!strings.Contains(murmurErr.Error(), "unsupported database format") {
		t.Fatalf("newer-store open failed with %v, want Corrupt(\"unsupported database format\")", err)
	}
	t.Logf("current build refused newer store: %v", err)
}

// TestDowngradeGuard crafts a one-generation-newer store and requires
// the current build to refuse it fail-closed (Rust: downgrade_guard).
func TestDowngradeGuard(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:        "downgrade-guard",
		NumNodes:    1,
		Key:         key,
		ManualPeers: true,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("downgrade-guard")
		}
		cluster.Finish(failed)
	}()

	if err := cluster.EnsureMigrated(0,
		"CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
		t.Fatalf("migrate records table: %v", err)
	}
	if err := cluster.Node(0).SetReplication("records", true); err != nil {
		t.Fatalf("opt-in records: %v", err)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush schema: %v", err)
	}
	for i := 0; i < rows; i++ {
		id := int64(firstID + i)
		name := fmt.Sprintf("row-%d", i)
		_, err := cluster.Node(0).Transaction(func(s *murmur.Sql) (any, error) {
			_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
			return nil, err
		})
		if err != nil {
			t.Fatalf("seed write %d: %v", id, err)
		}
	}
	if got := rowCount(t, cluster.Node(0)); got != rows {
		t.Fatalf("seeded row count = %d, want %d", got, rows)
	}
	wantDigest := rowsDigest(t, cluster.Node(0))
	wantNode := hexID(cluster.NodeID(0))
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush seeded rows: %v", err)
	}
	cluster.StopNode(0)

	// Identity + format markers of the stopped store.
	dataDir := cluster.NodeInfo(0).DataDir
	identity := readIdentity(t, dataDir)
	if identity.Database != cluster.DatabaseID() {
		t.Fatalf("stored database id mismatch")
	}
	if identity.Node != cluster.NodeID(0) {
		t.Fatalf("stored node id mismatch")
	}
	t.Logf("store identity: db %s node %s format %d",
		hexID(identity.Database), hexID(identity.Node), identity.Format)

	// Positive control: a fresh store created by the current build
	// opens, holds data across a close/reopen, and carries the same
	// current format marker -- so a later refusal can only be caused by
	// the bumped version, not by a broken build.
	freshDir := filepath.Join(cluster.RuntimeDir(), "fresh-store")
	createOpts := murmur.DefaultOptions()
	createOpts.Mode = murmur.OpenCreateNew
	fresh, err := murmur.Open(freshDir, key[:], createOpts)
	if err != nil {
		t.Fatalf("fresh store create: %v", err)
	}
	if err := fresh.Migrate(murmur.NewMigration(
		"CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")); err != nil {
		t.Fatalf("fresh migrate: %v", err)
	}
	if err := fresh.SetReplication("records", true); err != nil {
		t.Fatalf("fresh opt-in records: %v", err)
	}
	_, err = fresh.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", int64(1), "fresh")
		return nil, err
	})
	if err != nil {
		t.Fatalf("fresh insert: %v", err)
	}
	if err := fresh.Flush(); err != nil {
		t.Fatalf("fresh flush: %v", err)
	}
	if err := fresh.Close(); err != nil {
		t.Fatalf("fresh close: %v", err)
	}
	freshReopen, err := murmur.Open(freshDir, key[:], murmur.DefaultOptions())
	if err != nil {
		t.Fatalf("fresh store reopen: %v", err)
	}
	if got := rowCount(t, freshReopen); got != 1 {
		t.Fatalf("fresh reopened count = %d, want 1", got)
	}
	if err := freshReopen.Close(); err != nil {
		t.Fatalf("fresh reopen close: %v", err)
	}
	freshIdentity := readIdentity(t, freshDir)
	if freshIdentity.Format != identity.Format {
		t.Fatalf("fresh store format = %d, want current %d", freshIdentity.Format, identity.Format)
	}
	t.Logf("fresh store booted and reopened at format %d", freshIdentity.Format)

	// Craft the newer store: copy the stopped node dir, then bump its
	// identity format by one generation.
	futureDir := filepath.Join(cluster.RuntimeDir(), "future-store")
	copyDirAll(t, dataDir, futureDir)
	bumped := bumpIdentityFormat(t, futureDir, identity)
	if bumped.Database != identity.Database {
		t.Fatalf("bumped db id mismatch")
	}
	if bumped.Node != identity.Node {
		t.Fatalf("bumped node id mismatch")
	}
	reread := readIdentity(t, futureDir)
	if reread.Format != identity.Format+1 {
		t.Fatalf("bumped format = %d, want %d", reread.Format, identity.Format+1)
	}
	t.Logf("newer store staged at format %d (current %d)", reread.Format, identity.Format)

	// The downgrade attempt: the current build must refuse the newer
	// store loudly, without touching its content.
	before := hashTree(t, futureDir)
	assertRefusesNewerStore(t, futureDir)
	after := hashTree(t, futureDir)
	assertStoreUntouched(t, before, after)
	markers := readIdentity(t, futureDir)
	if markers.Format != reread.Format {
		t.Fatalf("format markers moved: %d != %d", markers.Format, reread.Format)
	}
	if markers.Database != identity.Database {
		t.Fatalf("db id moved after refusal")
	}
	if markers.Node != identity.Node {
		t.Fatalf("node id moved after refusal")
	}
	t.Logf("store format markers still at %d after refusal", markers.Format)

	// The current build reopens the unmodified store with identical data
	// and identity.
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("reopen original store: %v", err)
	}
	if got := rowCount(t, cluster.Node(0)); got != rows {
		t.Fatalf("reopened row count = %d, want %d", got, rows)
	}
	if got := rowsDigest(t, cluster.Node(0)); got != wantDigest {
		t.Fatalf("reopened digest mismatch")
	}
	if got := hexID(cluster.NodeID(0)); got != wantNode {
		t.Fatalf("reopened node id mismatch")
	}
	t.Logf("current build reopened original store: %d rows digest %s", rows, wantDigest)

	// The newer store stays refused while the original works.
	assertRefusesNewerStore(t, futureDir)
	failed = false
}

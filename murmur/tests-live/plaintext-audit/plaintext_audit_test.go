// Live scenario: marker plaintext and key material are absent on disk.
//
// Go port of the Rust `plaintext-audit` scenario
// (/home/marc/MURMUR/tests-live/plaintext-audit/tests/plaintext_audit.rs).
// Random per-run markers are written through the typed table, replicated,
// and crash-cycled; every marker round-trips through the typed API
// (positive control), then both nodes' entire directories and a backup
// artifact are scanned: marker plaintext and key material must be absent
// everywhere.
//
// Plain SQL replaces the typed-records HTTP API; stop/start replaces
// SIGKILL; the backup is an online Database.Backup; and no config file is
// skipped because the harness provisions no key-bearing config file.
package plaintextaudit

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// tableDDL is the typed table (Rust: live_typed_records managed typed
// table).
const tableDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)"

const (
	// numMarkers is the marker row count (Rust: NUM_MARKERS = 8) plus
	// sustained filler (Rust: NUM_FILLER = 200).
	numMarkers = 8
	numFiller  = 200
	totalRows  = numMarkers + numFiller
	// convergeTimeout is the convergence deadline and poll (Rust: 60 s,
	// 200 ms).
	convergeTimeout = 60 * time.Second
	poll            = 200 * time.Millisecond
)

// randHex returns n random bytes as hex (Rust: randHex).
func randHex(t *testing.T, n int) string {
	t.Helper()
	buf := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		t.Fatalf("rand entropy: %v", err)
	}
	return hex.EncodeToString(buf)
}

// typedInsert is a typed insert on one node (Rust: Cluster.TypedInsert).
func typedInsert(t *testing.T, db *murmur.Database, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO live_typed_records(name) VALUES(?)", name)
		return nil, err
	}); err != nil {
		t.Fatalf("typed insert failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after typed insert: %v", err)
	}
}

// typedNames is the canonical ordered names on one node (Rust:
// Cluster.TypedNames).
func typedNames(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM live_typed_records ORDER BY name")
	})
	if err != nil {
		t.Fatalf("read typed names: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

// waitConverged waits until every node holds the same want names (Rust:
// waitConverged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want int) {
	t.Helper()
	ok := harness.WaitUntil(convergeTimeout, poll, func() bool {
		var first string
		for i := 0; i < cluster.Len(); i++ {
			names := typedNames(t, cluster.Node(i))
			if len(names) != want {
				return false
			}
			joined := strings.Join(names, "\x00")
			if i == 0 {
				first = joined
			} else if joined != first {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("nodes did not converge on %d rows within %v", want, convergeTimeout)
	}
}

// fileContains reports whether the file contains the needle, streaming with
// overlap so matches split across chunk boundaries are still found (Rust:
// fileContains).
func fileContains(path string, needle []byte) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	const chunk = 64 * 1024
	var buf []byte
	tmp := make([]byte, chunk)
	for {
		n, err := f.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			if bytes.Contains(buf, needle) {
				return true, nil
			}
			if len(buf) > len(needle) {
				keep := len(needle)
				buf = append([]byte(nil), buf[len(buf)-keep:]...)
			}
		}
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
}

// scanTree returns the first regular file under root containing needle, if
// any (Rust: scanTree; no skip set: the harness provisions no key-bearing
// config file, so every file is audited).
func scanTree(root string, needle []byte) (string, error) {
	var hit string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if hit != "" || !info.Mode().IsRegular() {
			return nil
		}
		found, err := fileContains(path, needle)
		if err != nil {
			return err
		}
		if found {
			hit = path
		}
		return nil
	})
	return hit, err
}

func TestPlaintextAuditMarkersAndKeysAbsentOnDisk(t *testing.T) {
	var key [32]byte
	if _, err := io.ReadFull(rand.Reader, key[:]); err != nil {
		t.Fatalf("key entropy: %v", err)
	}
	keyHex := hex.EncodeToString(key[:])
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "plaintext-audit",
		NumNodes: 2,
		Key:      key,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("plaintext-audit")
		}
		cluster.Finish(failed)
	}()

	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			t.Fatalf("node%d migrate failed: %v", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("live_typed_records", true); err != nil {
			t.Fatalf("opt-in live_typed_records: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}

	// Unique high-entropy markers: random per run, so a stale-file
	// false positive from another run is impossible.
	markers := make([]string, 0, numMarkers)
	for i := 0; i < numMarkers; i++ {
		marker := "MURMUR-AUDIT-" + randHex(t, 32) + "-ROW" + itoa(i)
		typedInsert(t, cluster.Node(0), marker)
		markers = append(markers, marker)
	}

	// Sustained load around the markers to exercise WAL, memtables,
	// compaction, and replication paths.
	for i := 0; i < numFiller; i++ {
		filler := "load-filler-" + itoa(i) + "-padding-" + strings.Repeat("x", 64)
		typedInsert(t, cluster.Node(i%2), filler)
	}
	waitConverged(t, cluster, totalRows)

	// Crash mid-life so tmp/scratch/crash artifacts exist, then restart
	// (Rust: SIGKILL; here in-process stop/start, then re-mesh).
	cluster.StopNode(0)
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart node 1: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	waitConverged(t, cluster, totalRows)

	// Positive control: every marker round-trips byte-identically
	// through the managed typed table, proving the scan below is
	// meaningful.
	present := make(map[string]bool)
	for _, name := range typedNames(t, cluster.Node(0)) {
		present[name] = true
	}
	for i, want := range markers {
		if !present[want] {
			t.Fatalf("marker %d did not round-trip through the typed table", i)
		}
	}
	t.Logf("positive control: all %d markers round-trip intact", len(markers))

	// Backup artifact (Rust: offline open + backup; here the product's
	// online backup on the still-running node 1).
	backupDir := filepath.Join(cluster.RuntimeDir(), "backup-dest")
	if err := cluster.Node(0).Backup(backupDir); err != nil {
		t.Fatalf("backup node 1: %v", err)
	}

	// Stop both nodes so the on-disk image is quiescent, then scan the
	// ENTIRE node directory of BOTH nodes. Node 2 learned the rows via
	// replication apply (not local commit), so its disk image exercises
	// different code paths and must be audited too.
	cluster.StopNode(0)
	cluster.StopNode(1)
	for n := 0; n < cluster.Len(); n++ {
		nodedir := cluster.NodeInfo(n).Dir
		for i, m := range markers {
			hit, err := scanTree(nodedir, []byte(m))
			if err != nil {
				t.Fatalf("scan node%d marker %d: %v", n+1, i, err)
			}
			if hit != "" {
				t.Fatalf("PLAINTEXT LEAK: marker %d found on disk in %s", i, hit)
			}
		}
		t.Logf("markers absent from entire node dir %s", nodedir)
	}

	// Key material must not leak anywhere on disk (Rust additionally
	// skips nothing: the harness writes no key-bearing config file, so
	// the whole tree is audited).
	needles := [][]byte{[]byte(keyHex), []byte(strings.ToUpper(keyHex)), key[:]}
	for n := 0; n < cluster.Len(); n++ {
		nodedir := cluster.NodeInfo(n).Dir
		for _, needle := range needles {
			hit, err := scanTree(nodedir, needle)
			if err != nil {
				t.Fatalf("scan node%d key material: %v", n+1, err)
			}
			if hit != "" {
				t.Fatalf("KEY LEAK: key material found on disk in %s", hit)
			}
		}
	}
	t.Logf("key material absent from entire node dirs")

	// Backup artifacts must not contain marker plaintext (or the key).
	for i, m := range markers {
		hit, err := scanTree(backupDir, []byte(m))
		if err != nil {
			t.Fatalf("scan backup marker %d: %v", i, err)
		}
		if hit != "" {
			t.Fatalf("PLAINTEXT LEAK: marker %d found in backup artifact %s", i, hit)
		}
	}
	if hit, err := scanTree(backupDir, []byte(keyHex)); err != nil {
		t.Fatalf("scan backup key: %v", err)
	} else if hit != "" {
		t.Fatalf("KEY LEAK: key material found in backup artifact %s", hit)
	}
	t.Logf("backup artifacts clean: no marker plaintext, no key material")

	// Restart and reconverge: the audit must not disturb honest behavior.
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart node 1: %v", err)
	}
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart node 2: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after audit: %v", err)
	}
	waitConverged(t, cluster, totalRows)
	failed = false
}

// itoa formats a non-negative int.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [32]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

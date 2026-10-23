// Live scenario: disk-full fails closed, then recovers.
//
// Go port of the Rust `diskfull-live` scenario
// (/home/marc/MURMUR/tests-live/diskfull-live/tests/diskfull_live.rs),
// itself a port of the original Go `diskfull-live` scenario
// (TestDiskFullFailsClosedThenRecovers). One node is stopped, its
// directory is moved onto a tiny tmpfs, and it is restarted there;
// writes past ENOSPC must fail closed (error, no partial row, node
// keeps answering reads) with pre-full data intact, and freeing space
// plus a restart must recover to exact convergence.
//
// Plain SQL replaces the typed-records HTTP API. Env-gated like Rust:
// the test SKIPs when the tmpfs mount fails (no mount privilege) or on
// non-Unix platforms.
package diskfulllive

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// tableDDL is the records table (Rust: typed records with a value field).
const tableDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL)"

const (
	// baselineRows is the pre-failure baseline (Rust: 20).
	baselineRows = 20
	// convergeTimeout bounds every convergence wait (Rust: 30s).
	convergeTimeout = 30 * time.Second
	// poll is the convergence poll interval (Rust: 100ms).
	poll = 100 * time.Millisecond
	// tmpfsEnv sizes the tiny filesystem (Rust: default 8 MiB).
	tmpfsEnv     = "MURMUR_DISKFULL_TMPFS_SIZE_MB"
	defaultTmpfs = 8
	// fillerEnv sizes the fixed ballast (Rust: default 1 MiB).
	fillerEnv     = "MURMUR_DISKFULL_FILLER_SIZE_MB"
	defaultFiller = 1
	// valueEnv sizes fill values (Rust: default 128 KiB).
	valueEnv     = "MURMUR_DISKFULL_VALUE_SIZE_KB"
	defaultValKB = 128
	// maxErrors stops the fill loop (Rust: `maxErrors = 5`).
	maxErrors = 5
)

// envInt reads a positive-integer env knob with fallback (Rust: `envInt`).
func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

// freeBytes reports free bytes on the filesystem holding path.
func freeBytes(t *testing.T, path string) uint64 {
	t.Helper()
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		t.Fatalf("statfs failed: %v", err)
	}
	if st.Bavail < 0 || st.Bsize < 0 {
		return 0
	}
	return uint64(st.Bavail) * uint64(st.Bsize)
}

// queryNames reads ordered names on one node.
func queryNames(db *murmur.Database) ([]string, error) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM live_typed_records ORDER BY name")
	})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out, nil
}

// mustNames reads ordered names, failing the test on error.
func mustNames(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	names, err := queryNames(db)
	if err != nil {
		t.Fatalf("read names failed: %v", err)
	}
	return names
}

// readValue is a point read by name: nil when missing.
func readValue(db *murmur.Database, name string) *string {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT value FROM live_typed_records WHERE name = ?", name)
	})
	if err != nil {
		return nil
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 {
		return nil
	}
	out := rows[0][0].Str
	return &out
}

// tryInsert is a best-effort insert; true when acknowledged.
func tryInsert(db *murmur.Database, name, value string) bool {
	_, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO live_typed_records(name, value) VALUES(?, ?)", name, value)
		return nil, err
	})
	return err == nil
}

// run executes a command to completion, failing unless it succeeds.
func run(t *testing.T, what string, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s failed: %v\n%s", what, err, out)
	}
}

// copyTree copies a directory tree (`cp -a src/. dst`, creating dst).
func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatalf("create copy dest: %v", err)
	}
	run(t, "copy node dir", "cp", "-a", src+"/.", dst)
}

// waitConverged polls exact counts plus ordered-name equality.
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, poll, func() bool {
		var first []string
		for i := 0; i < cluster.Len(); i++ {
			got, err := queryNames(cluster.Node(i))
			if err != nil || len(got) != want {
				return false
			}
			if first == nil {
				first = got
				continue
			}
			if len(got) != len(first) {
				return false
			}
			for j := range got {
				if got[j] != first[j] {
					return false
				}
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("nodes did not converge on %d rows with equal names within %v", want, timeout)
	}
}

// TestDiskFullFailsClosedThenRecovers moves one node onto a tiny tmpfs,
// requires ENOSPC to fail closed, then recovers to exact convergence
// (Rust: disk_full_fails_closed_then_recovers).
func TestDiskFullFailsClosedThenRecovers(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("diskfull-live needs a tmpfs mount (Unix only)")
	}

	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "diskfull-live",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	// Node 0 dir + scenario runtime dir (stable across the tmpfs move).
	nodeDir := cluster.NodeInfo(0).Dir
	scenarioDir := filepath.Dir(nodeDir)
	// Set once the tmpfs is mounted; teardown must not leak it.
	mounted := false
	// Set on the mount-failure path so teardown cleans up without
	// forensics or failure artifacts.
	skipped := false
	failed := true
	defer func() {
		if skipped {
			cluster.Finish(false)
			return
		}
		if failed {
			cluster.DumpForensics("diskfull-live-no-convergence")
			if mounted {
				// Snapshot the tmpfs content aside: `Finish`
				// snapshots runtime/ only after the unmount
				// below hides it again.
				dst := filepath.Join(scenarioDir, "diskfull-tmpfs-content")
				_ = os.MkdirAll(dst, 0o755)
				cmd := exec.Command("cp", "-a", nodeDir+"/.", dst)
				_ = cmd.Run()
			}
		}
		if mounted {
			if out, err := exec.Command("umount", nodeDir).CombinedOutput(); err != nil {
				t.Errorf("unmount tmpfs failed: %v\n%s", err, out)
				failed = true
			}
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}

	// Same schema on every node (Rust: typed fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			t.Fatalf("node%d migrate records table: %v", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("live_typed_records", true); err != nil {
			t.Fatalf("node%d opt-in live_typed_records: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush opt-in: %v", i+1, err)
		}
	}

	// Baseline: 20 rows visible everywhere before any failure.
	for i := 0; i < baselineRows; i++ {
		name := fmt.Sprintf("diskfull-%04d", i)
		if !tryInsert(cluster.Node(0), name, "baseline") {
			t.Fatalf("baseline write failed")
		}
		if err := cluster.Node(0).Flush(); err != nil {
			t.Fatalf("flush baseline: %v", err)
		}
	}
	waitConverged(t, cluster, baselineRows, convergeTimeout)

	// Move the stopped node onto a tiny tmpfs. Mount failure (no
	// privilege in this environment) SKIPs, restoring the dir first.
	cluster.StopNode(0)
	staging := filepath.Join(scenarioDir, "diskfull-staging")
	copyTree(t, nodeDir, staging)
	if err := os.RemoveAll(nodeDir); err != nil {
		t.Fatalf("clear node dir for tmpfs: %v", err)
	}
	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		t.Fatalf("recreate node dir: %v", err)
	}
	tmpfsMB := envInt(tmpfsEnv, defaultTmpfs)
	if err := exec.Command("mount", "-t", "tmpfs", "-o",
		fmt.Sprintf("size=%dM", tmpfsMB), "tmpfs", nodeDir).Run(); err != nil {
		copyTree(t, staging, nodeDir)
		skipped = true
		t.Skip("tmpfs mount failed (needs mount privilege)")
	}
	mounted = true
	copyTree(t, staging, nodeDir)
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart node1 on tmpfs: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	waitConverged(t, cluster, baselineRows, convergeTimeout)

	// Prove the fs really is tiny before filling it (else ENOSPC may
	// never trigger and the test would pass vacuously).
	free := freeBytes(t, nodeDir)
	if free >= 5*1024*1024 {
		t.Fatalf("tmpfs has %d free bytes, want < 5 MiB (test would be vacuous)", free)
	}
	t.Logf("tiny fs proven: %d bytes free", free)

	// Fixed filler first: the fill loop must hit the wall via product
	// writes, not via leftover headroom games.
	fillerMB := envInt(fillerEnv, defaultFiller)
	filler := filepath.Join(nodeDir, "ballast", "filler.bin")
	if err := os.MkdirAll(filepath.Dir(filler), 0o755); err != nil {
		t.Fatalf("ballast dir: %v", err)
	}
	if err := os.WriteFile(filler, make([]byte, fillerMB*1024*1024), 0o644); err != nil {
		t.Fatalf("write filler: %v", err)
	}

	// Fill until the node refuses writes; every outcome is tracked so
	// the post-recovery audit is exact.
	valueSize := envInt(valueEnv, defaultValKB) * 1024
	value := ""
	{
		b := make([]byte, valueSize)
		for i := range b {
			b[i] = 'D'
		}
		value = string(b)
	}
	acked := make(map[string]bool)
	errored := make(map[string]bool)
	for i := 0; ; i++ {
		name := fmt.Sprintf("fill-%08d", i)
		if tryInsert(cluster.Node(0), name, value) {
			_ = cluster.Node(0).Flush()
			acked[name] = true
		} else {
			errored[name] = true
			if len(errored) > maxErrors {
				break
			}
		}
	}
	if len(errored) == 0 {
		t.Fatalf("no write ever failed: ENOSPC never hit (vacuous pass)")
	}
	t.Logf("fill done: %d acked, %d errored", len(acked), len(errored))

	// Fail-closed proof while still full: the node answers reads, and a
	// probe write either acks (tracked, must survive) or errors cleanly.
	// (A panic fails the test by construction, so there is no daemon log
	// to scan.)
	_ = mustNames(t, cluster.Node(0))
	const probe = "liveness-probe"
	if tryInsert(cluster.Node(0), probe, "probe") {
		_ = cluster.Node(0).Flush()
		acked[probe] = true
	}

	// Free space and restart: every acked row intact everywhere, every
	// errored row absent-on-all or whole-on-all (never torn).
	if err := os.Remove(filler); err != nil {
		t.Fatalf("remove filler: %v", err)
	}
	cluster.StopNode(0)
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart node1 after freeing space: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after recovery: %v", err)
	}
	for i := 0; i < cluster.Len(); i++ {
		for name := range acked {
			want := value
			if name == probe {
				want = "probe"
			}
			got := readValue(cluster.Node(i), name)
			if got == nil || *got != want {
				t.Fatalf("node%d lost acked row %q", i+1, name)
			}
		}
		for name := range errored {
			if got := readValue(cluster.Node(i), name); got != nil && *got != value {
				t.Fatalf("node%d has torn row %q (%d of %d bytes)", i+1, name, len(*got), valueSize)
			}
		}
	}
	t.Logf("recovery audit clean: acked intact, errored absent-or-whole")

	// Post-recovery honest writes on both nodes converge everywhere.
	for i, prefix := range []string{"post-recovery-node1", "post-recovery-node2"} {
		if !tryInsert(cluster.Node(i), prefix, "ok") {
			t.Fatalf("post-recovery write node%d failed", i+1)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush post-recovery: %v", i+1, err)
		}
	}
	waitConverged(t, cluster, baselineRows+len(acked)+2, convergeTimeout)
	t.Logf("PASS: ENOSPC failed closed, space freed, mesh reconverged")
	failed = false
}

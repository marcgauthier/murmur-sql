// Live scenario: one node is stopped, its directory is moved onto a tiny
// tmpfs, and it is restarted there; writes past ENOSPC must fail closed
// (error, no partial row, node keeps answering reads) with pre-full data
// intact, and freeing space plus a restart must recover to exact
// convergence.
//
// Go port of tests/diskfull_live.rs using tests-live/goharness.
// Identical behavior: the Unix statvfs probe becomes x/sys/unix.Statvfs,
// the tmpfs mount/umount and tree copies stay external cp/mount commands,
// and the test SKIPs when the tmpfs mount fails (no mount privilege) or
// on non-Unix platforms.
package diskfulllive

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
	"golang.org/x/sys/unix"
)

// Table DDL.
const tableDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL)"

// Baseline rows.
const baselineRows = 20

// Convergence deadline.
const convergeTimeout = 30 * time.Second

// Convergence poll interval.
const poll = 100 * time.Millisecond

// tmpfs-size knob, in MiB.
const tmpfsEnv = "MURMUR_DISKFULL_TMPFS_SIZE_MB"
const defaultTmpfsMB = uint64(8)

// Filler knob, in MiB.
const fillerEnv = "MURMUR_DISKFULL_FILLER_SIZE_MB"
const defaultFillerMB = uint64(1)

// Fill-value knob, in KiB.
const valueEnv = "MURMUR_DISKFULL_VALUE_SIZE_KB"
const defaultValueKB = 128

// Fill loop stops recording after this many errors.
const maxErrors = 5

// envInt is an env-int knob.
func envInt(name string, fallback uint64) uint64 {
	if v, err := strconv.ParseUint(os.Getenv(name), 10, 64); err == nil && v > 0 {
		return v
	}
	return fallback
}

// freeBytes returns the free bytes on the filesystem holding path (Unix
// statfs; the Rust port uses statvfs, which Linux lacks).
func freeBytes(path string) uint64 {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		panic(fmt.Sprintf("statfs failed: %v", err))
	}
	return uint64(stat.Bavail) * uint64(stat.Bsize)
}

// names returns the ordered names on one node.
func names(cluster *goharness.Cluster, idx int) ([]string, error) {
	rows, err := cluster.Node(idx).Query("SELECT name FROM live_typed_records ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("node%d read names failed: %w", idx+1, err)
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		name, err := r.String(0)
		if err != nil {
			return nil, fmt.Errorf("node%d decode name: %w", idx+1, err)
		}
		out = append(out, name)
	}
	return out, nil
}

// readValue is a point read by name: ("", false) when missing.
func readValue(cluster *goharness.Cluster, idx int, name string) (string, bool) {
	rows, err := cluster.Node(idx).Query("SELECT value FROM live_typed_records WHERE name = $1", name)
	if err != nil {
		return "", false
	}
	if len(rows) == 0 {
		return "", false
	}
	value, err := rows[0].String(0)
	if err != nil {
		return "", false
	}
	return value, true
}

// tryInsert is a best-effort insert; true when acknowledged.
func tryInsert(cluster *goharness.Cluster, idx int, name, value string) bool {
	_, err := cluster.Node(idx).Execute(
		"INSERT INTO live_typed_records(name, value) VALUES($1, $2)", name, value)
	return err == nil
}

// run executes a command to completion, failing unless it succeeds.
func run(t *testing.T, cmd *exec.Cmd, what string) {
	t.Helper()
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s failed: %v", what, err)
	}
}

// runErr executes a command to completion, returning the error.
func runErr(cmd *exec.Cmd) error {
	return cmd.Run()
}

// copyTree copies a directory tree (cp -a src/. dst, creating dst).
func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatalf("create copy dest: %v", err)
	}
	run(t, exec.Command("cp", "-a", src+string(filepath.Separator)+".", dst), "copy node dir")
}

// copyTreeErr copies a directory tree, returning the error.
func copyTreeErr(src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	return runErr(exec.Command("cp", "-a", src+string(filepath.Separator)+".", dst))
}

// waitConverged polls exact counts plus ordered-name equality.
func waitConverged(cluster *goharness.Cluster, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var first []string
		hasFirst := false
		converged := true
		for i := 0; i < cluster.Len(); i++ {
			got, err := names(cluster, i)
			if err != nil {
				return err
			}
			if len(got) != want {
				converged = false
				break
			}
			if !hasFirst {
				first, hasFirst = got, true
			} else if !equalStrings(first, got) {
				converged = false
				break
			}
		}
		if converged {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("nodes did not converge on %d rows with equal names within %s", want, timeout)
		}
		time.Sleep(poll)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// runBody is the body of the scenario. It returns skipped=true when the
// run SKIPs (tmpfs mount failed); mounted reports whether the tmpfs is up
// so the caller can tear it down on every exit path.
func runBody(t *testing.T, cluster *goharness.Cluster, nodeDir, scenarioDir string, mounted *bool) (skipped bool, err error) {
	// Same schema on every node, then replicate it: convergence needs
	// the per-table flag on each node.
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			return false, fmt.Errorf("migrate node%d: %w", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE live_typed_records REPLICATED true"); err != nil {
			return false, fmt.Errorf("node%d SET TABLE REPLICATED: %w", i+1, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			return false, fmt.Errorf("node%d flush after SET TABLE: %w", i+1, err)
		}
	}

	// Baseline: 20 rows visible everywhere before any failure.
	for i := 0; i < baselineRows; i++ {
		name := fmt.Sprintf("diskfull-%04d", i)
		if !tryInsert(cluster, 0, name, "baseline") {
			return false, fmt.Errorf("baseline write failed")
		}
		if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
			return false, fmt.Errorf("flush baseline: %w", err)
		}
	}
	if err := waitConverged(cluster, baselineRows, convergeTimeout); err != nil {
		return false, err
	}

	// Move the stopped node onto a tiny tmpfs. Mount failure (no
	// privilege in this environment) SKIPs, restoring the dir first.
	cluster.StopNode(0)
	staging := filepath.Join(scenarioDir, "diskfull-staging")
	if err := copyTreeErr(nodeDir, staging); err != nil {
		return false, fmt.Errorf("stage node dir: %w", err)
	}
	if err := os.RemoveAll(nodeDir); err != nil {
		return false, fmt.Errorf("clear node dir for tmpfs: %w", err)
	}
	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		return false, fmt.Errorf("recreate node dir: %w", err)
	}
	tmpfsMB := envInt(tmpfsEnv, defaultTmpfsMB)
	mountedOK := exec.Command("mount", "-t", "tmpfs", "-o", fmt.Sprintf("size=%dM", tmpfsMB), "tmpfs", nodeDir).Run() == nil
	if !mountedOK {
		copyTree(t, staging, nodeDir)
		fmt.Fprintln(os.Stderr, "SKIP: tmpfs mount failed (needs mount privilege)")
		return true, nil
	}
	*mounted = true
	if err := copyTreeErr(staging, nodeDir); err != nil {
		return false, fmt.Errorf("restore node dir onto tmpfs: %w", err)
	}
	if err := cluster.StartNode(0); err != nil {
		return false, fmt.Errorf("restart node1 on tmpfs: %w", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR SET TABLE live_typed_records REPLICATED true"); err != nil {
		return false, fmt.Errorf("re-set replicated node1 on tmpfs: %w", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		return false, fmt.Errorf("flush node1 on tmpfs: %w", err)
	}
	if err := cluster.Mesh(); err != nil {
		return false, fmt.Errorf("re-mesh after restart: %w", err)
	}
	_ = cluster.SyncAll()
	if err := waitConverged(cluster, baselineRows, convergeTimeout); err != nil {
		return false, err
	}

	// Prove the fs really is tiny before filling it (else ENOSPC may
	// never trigger and the test would pass vacuously).
	free := freeBytes(nodeDir)
	if free >= 5*1024*1024 {
		return false, fmt.Errorf("tmpfs has %d free bytes, want < 5 MiB (test would be vacuous)", free)
	}
	fmt.Fprintf(os.Stderr, "tiny fs proven: %d bytes free\n", free)

	// Fixed filler first: the fill loop must hit the wall via product
	// writes, not via leftover headroom games.
	fillerMB := envInt(fillerEnv, defaultFillerMB)
	filler := filepath.Join(nodeDir, "ballast", "filler.bin")
	if err := os.MkdirAll(filepath.Dir(filler), 0o755); err != nil {
		return false, fmt.Errorf("ballast dir: %w", err)
	}
	if err := os.WriteFile(filler, make([]byte, fillerMB*1024*1024), 0o644); err != nil {
		return false, fmt.Errorf("write filler: %w", err)
	}

	// Fill until the node refuses writes; every outcome is tracked so
	// the post-recovery audit is exact.
	valueSize := int(envInt(valueEnv, defaultValueKB)) * 1024
	value := strings.Repeat("D", valueSize)
	acked := map[string]bool{}
	errored := map[string]bool{}
	for i := 0; ; i++ {
		name := fmt.Sprintf("fill-%08d", i)
		if tryInsert(cluster, 0, name, value) {
			_, _ = cluster.Node(0).Simple("MURMUR FLUSH")
			acked[name] = true
		} else {
			errored[name] = true
			if len(errored) > maxErrors {
				break
			}
		}
	}
	if len(errored) == 0 {
		return false, fmt.Errorf("no write ever failed: ENOSPC never hit (vacuous pass)")
	}
	fmt.Fprintf(os.Stderr, "fill done: %d acked, %d errored\n", len(acked), len(errored))

	// Fail-closed proof while still full: the node answers reads, and
	// a probe write either acks (tracked, must survive) or errors
	// cleanly. (A server panic fails the test via the wire error instead,
	// and the logs are kept as failure artifacts.)
	if _, err := names(cluster, 0); err != nil {
		return false, err
	}
	probe := "liveness-probe"
	if tryInsert(cluster, 0, probe, "probe") {
		_, _ = cluster.Node(0).Simple("MURMUR FLUSH")
		acked[probe] = true
	}

	// Free space and restart: every acked row intact everywhere, every
	// errored row absent-on-all or whole-on-all (never torn).
	if err := os.Remove(filler); err != nil {
		return false, fmt.Errorf("remove filler: %w", err)
	}
	cluster.StopNode(0)
	if err := cluster.StartNode(0); err != nil {
		return false, fmt.Errorf("restart node1 after freeing space: %w", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR SET TABLE live_typed_records REPLICATED true"); err != nil {
		return false, fmt.Errorf("re-set replicated node1 after recovery: %w", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		return false, fmt.Errorf("flush node1 after recovery: %w", err)
	}
	if err := cluster.Mesh(); err != nil {
		return false, fmt.Errorf("re-mesh after recovery: %w", err)
	}
	_ = cluster.SyncAll()
	for i := 0; i < cluster.Len(); i++ {
		for name := range acked {
			want := value
			if name == "liveness-probe" {
				want = "probe"
			}
			got, ok := readValue(cluster, i, name)
			if !ok || got != want {
				return false, fmt.Errorf("node%d lost acked row %q", i+1, name)
			}
		}
		for name := range errored {
			got, ok := readValue(cluster, i, name)
			switch {
			case !ok:
			case got == value:
			default:
				return false, fmt.Errorf("node%d has torn row %q (%d of %d bytes)",
					i+1, name, len(got), valueSize)
			}
		}
	}
	fmt.Fprintln(os.Stderr, "recovery audit clean: acked intact, errored absent-or-whole")

	// Post-recovery honest writes on both nodes converge everywhere.
	for i, prefix := range []string{"post-recovery-node1", "post-recovery-node2"} {
		if !tryInsert(cluster, i, prefix, "ok") {
			return false, fmt.Errorf("post-recovery write node%d failed", i+1)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			return false, fmt.Errorf("flush post-recovery: %w", err)
		}
	}
	if err := waitConverged(cluster, baselineRows+len(acked)+2, convergeTimeout); err != nil {
		return false, err
	}
	fmt.Fprintln(os.Stderr, "PASS: ENOSPC failed closed, space freed, mesh reconverged")
	return false, nil
}

func TestDiskFullFailsClosedThenRecovers(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	if runtime.GOOS == "windows" {
		t.Skip("diskfull-live needs a tmpfs mount (Unix only)")
	}

	opts := goharness.DefaultClusterOptions("diskfull-live")
	opts.NumNodes = 2
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		fmt.Fprintf(os.Stderr, "%s dir=%s sql=%s repl=%s\n",
			info.Label, info.Dir, info.SQLAddr, info.ReplAddr)
	}
	// Node 0 dir + scenario runtime dir (stable across the tmpfs move).
	nodeDir := cluster.NodeInfo(0).Dir
	scenarioDir := filepath.Dir(nodeDir)
	// Set once the tmpfs is mounted; teardown must not leak it.
	mounted := false
	skipped, runErr := runBody(t, cluster, nodeDir, scenarioDir, &mounted)
	failed := runErr != nil
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", runErr)
	}
	if failed {
		cluster.DumpForensics("diskfull-live-no-convergence")
		if mounted {
			// Snapshot the tmpfs content aside: finish snapshots
			// runtime/ only after the unmount below hides it again.
			copyTree(t, nodeDir, filepath.Join(scenarioDir, "diskfull-tmpfs-content"))
		}
	}
	if mounted {
		run(t, exec.Command("umount", nodeDir), "unmount tmpfs")
	}
	cluster.Finish(failed)
	if skipped {
		t.Skip("tmpfs mount failed (needs mount privilege)")
	}
	if failed {
		t.Fatal("scenario failed; artifacts retained under tests-live/failures/")
	}
}

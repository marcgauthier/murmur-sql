// Live SQL test: a database store written by a NEWER release format must
// be refused fail-closed by the server during UNLOCK, leaving the store
// untouched, while the unmodified store reopens cleanly with exact data
// fidelity.
//
// Go port of tests/downgrade_guard.rs using tests-live/gosqlharness.
// Identical behavior: the engine-owned format injector (an external Rust
// test binary via MURMUR_FORMAT_FIXTURE_BIN) bumps the identity format on
// a stopped disposable copy; database assertions still go through SQL to a
// live server. The Rust edit_config closure becomes SetConfig section
// writes and sha2 becomes crypto/sha256.
package downgradeguard

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

// bumpIdentityFormat runs the engine-owned test utility, which mutates
// only the stopped disposable copy.
func bumpIdentityFormat(t *testing.T, dir, keyHex string) {
	t.Helper()
	binary, ok := os.LookupEnv("MURMUR_FORMAT_FIXTURE_BIN")
	if !ok || binary == "" {
		t.Fatal("set MURMUR_FORMAT_FIXTURE_BIN to the engine-owned artifact injector test binary (built from the MURMUR Rust repo)")
	}
	cmd := exec.Command(binary,
		"--ignored",
		"--exact",
		"storage::live_sql_format_fixture::write_future_format_to_copied_store",
		"--nocapture",
	)
	cmd.Env = append(os.Environ(),
		"MURMUR_FORMAT_FIXTURE_PATH="+dir,
		"MURMUR_FORMAT_FIXTURE_KEY_HEX="+keyHex,
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("format injector failed: %s %s (%v)", stdout.String(), stderr.String(), err)
	}
	if !strings.Contains(stdout.String(), "FUTURE_FORMAT_FIXTURE old=") {
		t.Fatalf("format injector missing FUTURE_FORMAT_FIXTURE marker: %s %s",
			stdout.String(), stderr.String())
	}
}

func copyDirAll(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatalf("create copy dest: %v", err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("read copy src: %v", err)
	}
	sort.Slice(entries, func(a, b int) bool { return entries[a].Name() < entries[b].Name() })
	for _, entry := range entries {
		from := filepath.Join(src, entry.Name())
		to := filepath.Join(dst, entry.Name())
		info, err := entry.Info()
		if err != nil {
			t.Fatalf("stat copy entry: %v", err)
		}
		if info.IsDir() {
			copyDirAll(t, from, to)
		} else if info.Mode().IsRegular() {
			raw, err := os.ReadFile(from)
			if err != nil {
				t.Fatalf("read copy file: %v", err)
			}
			if err := os.WriteFile(to, raw, 0o644); err != nil {
				t.Fatalf("write copy file: %v", err)
			}
		}
	}
}

// hashTree maps each regular file under root (relative path) to its
// sha256 hex digest.
func hashTree(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	var walk func(dir string)
	walk = func(dir string) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read hash dir: %v", err)
		}
		sort.Slice(entries, func(a, b int) bool { return entries[a].Name() < entries[b].Name() })
		for _, entry := range entries {
			path := filepath.Join(dir, entry.Name())
			info, err := entry.Info()
			if err != nil {
				t.Fatalf("stat hash entry: %v", err)
			}
			if info.IsDir() {
				walk(path)
			} else if info.Mode().IsRegular() {
				rel, err := filepath.Rel(root, path)
				if err != nil {
					t.Fatalf("rel hash path: %v", err)
				}
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("read hash file: %v", err)
				}
				sum := sha256.Sum256(raw)
				out[rel] = fmt.Sprintf("%x", sum)
			}
		}
	}
	walk(root)
	return out
}

func TestDowngradeGuardRefusesNewStore(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "downgrade-guard",
		NumNodes: 1,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	fixtureKey := strings.Repeat("22", 32)
	if _, err := cluster.Node(0).Simple("MURMUR ROTATE KEY " + gosqlharness.SQLLiteral(gosqlharness.Key) +
		" " + gosqlharness.SQLLiteral(fixtureKey)); err != nil {
		t.Fatalf("rotate key: %v", err)
	}
	cluster.SetKey(0, fixtureKey)
	client := cluster.Node(0)
	if _, err := client.Simple("CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
		t.Fatalf("create records: %v", err)
	}

	for i := int64(0); i < 25; i++ {
		id := 3000 + i
		name := fmt.Sprintf("row-%02d", i)
		if _, err := client.Execute("INSERT INTO records(id, name) VALUES($1, $2)", id, name); err != nil {
			t.Fatalf("insert row-%02d: %v", i, err)
		}
	}
	if _, err := client.Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush rows: %v", err)
	}

	rowsBefore, err := client.Simple("SELECT id, name FROM records ORDER BY id")
	if err != nil {
		t.Fatalf("read rows before: %v", err)
	}
	if len(rowsBefore) != 25 {
		t.Fatalf("rows before = %d, want 25", len(rowsBefore))
	}

	pristineData := cluster.NodeInfo(0).DataDir
	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("stop node: %v", err)
	}

	// Copy to future store and bump format.
	futureData := filepath.Join(cluster.NodeInfo(0).Dir, "future-data")
	copyDirAll(t, pristineData, futureData)
	bumpIdentityFormat(t, futureData, fixtureKey)

	beforeRefusalTree := hashTree(t, futureData)

	// Point node 0 to future store and attempt to start.
	if err := cluster.SetConfig(0, "storage", map[string]any{"data_dir": futureData}); err != nil {
		t.Fatalf("point at future store: %v", err)
	}

	// The server MUST fail to unlock / start against the future store.
	startErr := cluster.StartNode(0)
	if startErr == nil {
		t.Fatal("Expected node start to fail against future store format")
	}
	errText := fmt.Sprintf("%v", startErr)
	if !strings.Contains(errText, "unsupported database format") {
		t.Fatalf("Expected 'unsupported database format' in %s", errText)
	}

	// Future store must remain content-untouched.
	afterRefusalTree := hashTree(t, futureData)
	if !reflect.DeepEqual(beforeRefusalTree, afterRefusalTree) {
		t.Fatal("Future store files were modified despite refusal!")
	}

	// Stop the refused process before restarting with the pristine store.
	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("stop refused node: %v", err)
	}

	// Point back to pristine store and start.
	if err := cluster.SetConfig(0, "storage", map[string]any{"data_dir": pristineData}); err != nil {
		t.Fatalf("point at pristine store: %v", err)
	}

	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("reopen pristine store: %v", err)
	}

	rowsAfter, err := cluster.Node(0).Simple("SELECT id, name FROM records ORDER BY id")
	if err != nil {
		t.Fatalf("read rows after: %v", err)
	}
	if !reflect.DeepEqual(rowsBefore, rowsAfter) {
		t.Fatalf("rows changed across refusal: before=%v after=%v", rowsBefore, rowsAfter)
	}
	failed = false
}

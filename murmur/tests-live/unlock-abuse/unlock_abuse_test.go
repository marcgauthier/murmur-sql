// Live scenario: unlock abuse fails closed without an oracle.
//
// Go port of the Rust `unlock-abuse` scenario
// (/home/marc/MURMUR/tests-live/unlock-abuse/tests/unlock_abuse.rs),
// itself a port of the original Go `unlock-abuse` scenario
// (TestUnlockAbuse). That suite abuses the Remote Unlock HTTP endpoint:
// wrong-key and unknown-key-id attempts must return byte-identical
// generic 401s with no key material, then honest unlock succeeds with an
// audit line. There is no unlock endpoint here (`murmur.Open` takes the
// key directly), so this port proves the same anti-oracle property on
// direct opens of a stopped node's store: distinct wrong keys fail with
// byte-identical generic messages leaking no key bytes, malformed keys
// fail cleanly, and the honest reopen serves intact data.
package unlockabuse

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// tableDDL is the contention table (Rust: typed contention rows).
const tableDDL = "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)"

const (
	// convergeTimeout bounds every convergence wait (Rust: 10s).
	convergeTimeout = 10 * time.Second
	// poll is the convergence poll interval (Rust: 50ms).
	poll = 50 * time.Millisecond
)

// unlockAttempt opens dir with key and returns the failure message. It
// fails the test when the open unexpectedly succeeds.
func unlockAttempt(t *testing.T, dir string, key []byte) string {
	t.Helper()
	db, err := murmur.Open(dir, key, murmur.DefaultOptions())
	if err == nil {
		_ = db.Close()
		t.Fatalf("open with a wrong key succeeded (%d key bytes)", len(key))
	}
	return err.Error()
}

// assertNoOracle requires a failure message to carry no oracle:
// generic (non-empty) and free of every key's hex (tried and real
// alike).
func assertNoOracle(t *testing.T, what, message string, secrets []string) {
	t.Helper()
	if message == "" {
		t.Fatalf("%s: failure message is empty", what)
	}
	lower := strings.ToLower(message)
	for _, secret := range secrets {
		if secret != "" && strings.Contains(lower, secret) {
			t.Fatalf("%s: failure message leaks key material: %q", what, message)
		}
	}
}

// rowPair is one (id, name) contention row.
type rowPair struct {
	id   string
	name string
}

// queryRows reads ordered contention rows on one node.
func queryRows(db *murmur.Database) ([]rowPair, error) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name FROM contention ORDER BY id")
	})
	if err != nil {
		return nil, err
	}
	var out []rowPair
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, rowPair{id: row[0].Str, name: row[1].Str})
	}
	return out, nil
}

// mustRows reads ordered contention rows, failing the test on error.
func mustRows(t *testing.T, db *murmur.Database) []rowPair {
	t.Helper()
	rows, err := queryRows(db)
	if err != nil {
		t.Fatalf("read contention rows failed: %v", err)
	}
	return rows
}

// insertRow inserts one row (Rust: `TypedContentionInsert`).
func insertRow(t *testing.T, db *murmur.Database, node int, id, name string) {
	t.Helper()
	_, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO contention(id, name) VALUES(?, ?)", id, name)
		return nil, err
	})
	if err != nil {
		t.Fatalf("node%d insert failed: %v", node, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after insert failed: %v", node, err)
	}
}

// waitAllCount polls until every node reports exactly `want` rows.
func waitAllCount(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, poll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			rows, err := queryRows(cluster.Node(i))
			if err != nil || len(rows) != want {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("cluster failed to converge to %d rows within %v", want, timeout)
	}
}

// assertEqualDigests requires identical ordered contents on all nodes.
func assertEqualDigests(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	want := mustRows(t, cluster.Node(0))
	for i := 1; i < cluster.Len(); i++ {
		got := mustRows(t, cluster.Node(i))
		if len(got) != len(want) {
			t.Fatalf("node%d contents != node1 contents", i+1)
		}
		for j := range got {
			if got[j] != want[j] {
				t.Fatalf("node%d contents != node1 contents", i+1)
			}
		}
	}
}

// TestUnlockAbuse proves wrong-key opens fail identically with no
// oracle and the honest reopen serves intact data (Rust:
// unlock_abuse).
func TestUnlockAbuse(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "unlock-abuse",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("unlock-abuse-no-convergence")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}

	// Same schema on every node (Rust: typed contention fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			t.Fatalf("node%d migrate contention table: %v", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("contention", true); err != nil {
			t.Fatalf("node%d opt-in contention: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush opt-in: %v", i+1, err)
		}
	}

	// Positive control: honest path works before any abuse.
	insertRow(t, cluster.Node(0), 1, "00000000000000000000000000000001", "baseline")
	waitAllCount(t, cluster, 1, convergeTimeout)
	assertEqualDigests(t, cluster)

	// Lock node 1: stop it; all unlock attempts below open its store
	// directly.
	cluster.StopNode(0)
	dataDir := cluster.NodeInfo(0).DataDir
	realKey := [32]byte{
		9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
		9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
	}
	realHex := hex.EncodeToString(realKey[:])

	// Wrong-key attempts: distinct 32-byte keys must fail with
	// byte-identical generic messages leaking no key bytes.
	var wrongA [32]byte
	for i := range wrongA {
		wrongA[i] = 0xff
	}
	wrongA[31] = 0xf0
	var wrongB, wrongC [32]byte
	for i := range wrongB {
		wrongB[i] = 0x01
		wrongC[i] = 0x02
	}
	wrongKeys := [][]byte{wrongA[:], wrongB[:], wrongC[:]}
	var messages []string
	for _, key := range wrongKeys {
		message := unlockAttempt(t, dataDir, key)
		assertNoOracle(t, "wrong-key", message, []string{hex.EncodeToString(key), realHex})
		messages = append(messages, message)
	}
	if messages[0] != messages[1] || messages[0] != messages[2] {
		t.Fatalf("unlock oracle: wrong-key messages differ:\n%q\n%q\n%q",
			messages[0], messages[1], messages[2])
	}
	t.Logf("wrong-key attempts: identical generic failures, no key material")

	// Malformed credentials: must fail cleanly with no key bytes in the
	// message. Cross-identity with the wrong-key message is NOT
	// asserted: length validation reflects only the attacker's own
	// input length, which cannot oracle key VALUE.
	malformed16 := make([]byte, 16)
	for i := range malformed16 {
		malformed16[i] = 0x03
	}
	malformed64 := make([]byte, 64)
	for i := range malformed64 {
		malformed64[i] = 0x04
	}
	for _, key := range [][]byte{malformed16, malformed64} {
		message := unlockAttempt(t, dataDir, key)
		assertNoOracle(t, "malformed-key", message, []string{hex.EncodeToString(key), realHex})
	}
	t.Logf("malformed-key attempts: clean failures, no key material")

	// Honest unlock succeeds (and proves the failures above reflect
	// real key enforcement, not a wedged store): the node serves
	// intact data and rejoins the mesh.
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("honest reopen: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after reopen: %v", err)
	}
	waitAllCount(t, cluster, 1, convergeTimeout)
	assertEqualDigests(t, cluster)
	insertRow(t, cluster.Node(0), 1, "00000000000000000000000000000002", "post-unlock")
	waitAllCount(t, cluster, 2, convergeTimeout)
	assertEqualDigests(t, cluster)
	t.Logf("PASS: unlock abuse failed closed, honest reopen intact")
	failed = false
}

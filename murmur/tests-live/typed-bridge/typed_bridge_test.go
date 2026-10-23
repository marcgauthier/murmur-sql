// Live scenario: a typed row is exported from the Low domain and imported
// through the High domain's typed durable path.
//
// Go port of the Rust `typed-bridge` scenario
// (/home/marc/MURMUR/tests-live/typed-bridge/tests/typed_bridge.rs). Two
// single-node clusters form the Low (exporter) and High (importer) bridge
// domains sharing one staging directory: a typed row inserted on Low is
// exported, imported through High's typed durable path, overridden by a
// High write (automatic ownership via promote), shielded from a Low
// update, released back to Low, updated again, and still present after a
// High restart.
//
// Plain SQL replaces the typed HTTP API; TypedRename maps to an UPDATE of
// the LWW title field so the primary-key row identity is preserved
// (ownership only covers non-PK cells).
package typedbridge

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/replication"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// lowName is the Low (exporter) cluster name; owns
// runtime/typed-bridge-low/, which also holds the shared staging dir so
// artifacts are retained/cleaned with it.
const lowName = "typed-bridge-low"

// highName is the High (importer) cluster name; owns
// runtime/typed-bridge-high/.
const highName = "typed-bridge-high"

// stream is the bridge stream (Rust: typed-stream).
const stream = "typed-stream"

// table is the typed table (Rust: live_typed_records). title is the LWW
// rename target.
const table = "live_typed_records"

const ddl = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)"

// row is the row identity, fixed for the whole scenario (Rust: one row
// renamed by ID).
const row = "bridge-native-record"

const (
	// wait is the convergence deadline (Rust: 30 s waits).
	wait = 30 * time.Second
	// poll is the convergence poll interval (Rust: 50 ms polls).
	poll = 50 * time.Millisecond
)

// insertRow is a typed insert on Low (Rust: Cluster.TypedInsertWithID).
func insertRow(t *testing.T, db *murmur.Database) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO live_typed_records(name, title) VALUES(?, ?)", row, row)
		return nil, err
	}); err != nil {
		t.Fatalf("typed insert failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after typed insert: %v", err)
	}
}

// updateTitle is a known-field update (Rust: Cluster.TypedRename, applied
// to the LWW title field so the primary-key row identity is preserved).
func updateTitle(t *testing.T, db *murmur.Database, title string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("UPDATE live_typed_records SET title = ? WHERE name = ?", title, row)
		return nil, err
	}); err != nil {
		t.Fatalf("title update failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after title update: %v", err)
	}
}

// titleCount is the row count by title (Rust: Cluster.TypedCount).
func titleCount(t *testing.T, db *murmur.Database, title string) int64 {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT count(*) FROM live_typed_records WHERE title = ?", title)
	})
	if err != nil {
		t.Fatalf("read title count: %v", err)
	}
	return v.([][]sqlite.Value)[0][0].Int
}

func lowBridge(t *testing.T, low *harness.Cluster) *replication.Bridge {
	t.Helper()
	h := low.Node(0).Replication()
	if h == nil {
		t.Fatalf("low replication handle missing")
	}
	b := h.Bridge()
	if b == nil {
		t.Fatalf("low bridge missing")
	}
	return b
}

func highBridge(t *testing.T, high *harness.Cluster) *replication.Bridge {
	t.Helper()
	h := high.Node(0).Replication()
	if h == nil {
		t.Fatalf("high replication handle missing")
	}
	b := h.Bridge()
	if b == nil {
		t.Fatalf("high bridge missing")
	}
	return b
}

// sync is one export+import pump (Rust: BridgeExport + BridgeImport).
// Per-call bundle counts are not asserted: the replication engine also
// drives capture/process in the background, so progress is proven by row
// convergence instead.
func sync(t *testing.T, low, high *harness.Cluster) {
	t.Helper()
	if _, err := lowBridge(t, low).Capture(); err != nil {
		t.Fatalf("low bridge capture: %v", err)
	}
	if _, err := highBridge(t, high).Process(); err != nil {
		t.Fatalf("high bridge process: %v", err)
	}
}

// drain pumps until Low has nothing left to export and High has nothing
// left to import (bounded idle loop; the bridge_files product-test
// pattern). It proves a Low update has arrived before a suppression
// assertion.
func drain(t *testing.T, low, high *harness.Cluster) {
	t.Helper()
	for range 100 {
		exported, err := lowBridge(t, low).Capture()
		if err != nil {
			t.Fatalf("low bridge capture: %v", err)
		}
		imported, err := highBridge(t, high).Process()
		if err != nil {
			t.Fatalf("high bridge process: %v", err)
		}
		if exported == 0 && imported == 0 {
			return
		}
	}
	t.Fatalf("bridge did not become idle")
}

func waitTitle(t *testing.T, low, high *harness.Cluster, title string, want int64) {
	t.Helper()
	ok := harness.WaitUntil(wait, poll, func() bool {
		sync(t, low, high)
		return titleCount(t, high.Node(0), title) == want
	})
	if !ok {
		t.Fatalf("high title count for %q = %d, want %d", title, titleCount(t, high.Node(0), title), want)
	}
}

// releaseTitle releases High ownership of the row's title field (Rust:
// BridgeRelease; the key is the encoded PK value vector).
func releaseTitle(t *testing.T, high *harness.Cluster) {
	t.Helper()
	key := murmur.EncodeValues([]murmur.Value{murmur.TextValue(row)})
	column := "title"
	released, err := highBridge(t, high).Release(table, key, &column)
	if err != nil {
		t.Fatalf("bridge release: %v", err)
	}
	if !released {
		t.Fatalf("bridge release reported no progress")
	}
}

func TestTypedBridgeImportsIntoNativeRIME(t *testing.T) {
	// Independent bridge domains (Rust: two separate clusters).
	var lowDBID, highDBID, lowNode, highNode [16]byte
	for i := range lowDBID {
		lowDBID[i] = 0xb0
		highDBID[i] = 0xb1
		lowNode[i] = 0x10
		highNode[i] = 0x11
	}
	// Low bundle-signer seed (ed25519; High pins the verifying key).
	var lowSigner [32]byte
	for i := range lowSigner {
		lowSigner[i] = 0x21
	}
	// High receiver secret (X25519; Low encrypts to the derived
	// recipient).
	var highSecret [32]byte
	for i := range highSecret {
		highSecret[i] = 0x22
	}

	staging := filepath.Join(harness.RuntimeDir(lowName), "staging")
	low, err := harness.NewCluster(harness.ClusterOptions{
		Name:        lowName,
		NumNodes:    1,
		DatabaseID:  &lowDBID,
		NodeIDs:     [][16]byte{lowNode},
		ManualPeers: true,
		Bridge: harness.BridgeOptions{
			Nodes: map[int]*replication.BridgeConfig{
				0: replication.NewBridgeExporter(lowDBID, staging, stream, lowSigner, replication.RecipientPublic(&highSecret)),
			},
		},
	})
	if err != nil {
		t.Fatalf("low cluster setup: %v", err)
	}
	// High pins Low's origin key so imported transactions verify; the
	// seed itself stays inside the harness.
	lowOrigin := low.NodeOriginPubkey(0)
	high, err := harness.NewCluster(harness.ClusterOptions{
		Name:        highName,
		NumNodes:    1,
		DatabaseID:  &highDBID,
		NodeIDs:     [][16]byte{highNode},
		ManualPeers: true,
		Bridge: harness.BridgeOptions{
			Nodes: map[int]*replication.BridgeConfig{
				0: replication.NewBridgeReceiver(highDBID, staging, stream, highSecret,
					map[[16]byte]bool{lowDBID: true},
					map[[16]byte][32]byte{lowNode: replication.PublicKey(&lowSigner)},
					map[[16]byte][32]byte{lowNode: lowOrigin},
				),
			},
		},
	})
	if err != nil {
		t.Fatalf("high cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			low.DumpForensics("typed-bridge-low")
			high.DumpForensics("typed-bridge-high")
		}
		low.Finish(failed)
		high.Finish(failed)
	}()
	for _, item := range []struct {
		label string
		db    *murmur.Database
	}{
		{"low", low.Node(0)},
		{"high", high.Node(0)},
	} {
		if err := item.db.Migrate(murmur.NewMigration(ddl)); err != nil {
			t.Fatalf("%s migrate failed: %v", item.label, err)
		}
		if err := item.db.SetReplication(table, true); err != nil {
			t.Fatalf("%s opt-in %s: %v", item.label, table, err)
		}
		if err := item.db.Flush(); err != nil {
			t.Fatalf("%s flush after migrate failed: %v", item.label, err)
		}
	}

	// Export + import the native record through the typed durable path.
	insertRow(t, low.Node(0))
	waitTitle(t, low, high, row, 1)
	// High typed field override claims ownership of title.
	updateTitle(t, high.Node(0), "high-owned-record")
	if got := titleCount(t, high.Node(0), "high-owned-record"); got != 1 {
		t.Fatalf("high override not visible locally")
	}
	// Low update while the field is High-owned: imported but suppressed.
	updateTitle(t, low.Node(0), "low-update-before-release")
	drain(t, low, high)
	if got := titleCount(t, high.Node(0), "high-owned-record"); got != 1 {
		t.Fatalf("high-owned value lost to low update")
	}
	if got := titleCount(t, high.Node(0), "low-update-before-release"); got != 0 {
		t.Fatalf("low update leaked through high ownership")
	}
	// Release ownership, then the Low update flows through.
	releaseTitle(t, high)
	updateTitle(t, low.Node(0), "low-after-release")
	waitTitle(t, low, high, "low-after-release", 1)
	// The imported result survives a High-node restart.
	high.StopNode(0)
	if err := high.StartNode(0); err != nil {
		t.Fatalf("high restart: %v", err)
	}
	if got := titleCount(t, high.Node(0), "low-after-release"); got != 1 {
		t.Fatalf("imported row lost across high restart")
	}
	failed = false
}

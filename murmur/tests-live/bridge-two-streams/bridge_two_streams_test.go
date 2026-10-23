// Live scenario: two independent Low domains export into one two-node High
// cluster over separate bridge streams.
//
// Go port of the Rust `bridge-two-streams` scenario
// (/home/marc/MURMUR/tests-live/bridge-two-streams/tests/bridge_two_streams.rs).
// Two independent single-node Low domains (distinct DBIDs, CAs, keys,
// streams, staging dirs) export managed rows; a two-node High cluster
// imports stream-a on node0 and stream-b on node1, then converges both
// domains' rows over its mesh. A stream-a-only second round must arrive
// without disturbing stream-b progress.
//
// Plain SQL replaces the typed HTTP API; BridgeExport/BridgeImport map to
// direct Bridge.Capture() (Low) and Bridge.Process() (High) pumps, with
// progress proven by row convergence instead of per-call bundle counts (the
// replication engine also drives capture/process in the background).
// BridgeStatus progress and Provenance have no public API (spool keys are
// internal), so both legs are proven behaviorally: Low-owned provenance by
// a Low update flowing through to High (a High-owned field would suppress
// it), and stream-b progress isolation by a stream-b-only follow-up round
// delivering exactly once from the undisturbed cursor. A drain-to-idle leg
// covers the duplicate-delivery criterion (receipt idempotence).
//
// TestCrossDomainIdentityCollisionFailsClosed ports the original gate
// (absent from the Rust scenario): the importer fails closed with a
// collision error naming the collision, and first-writer state stands.
// Go-port product extension: murmur.HighLowImport rejects cross-domain
// same-row imports (replication.KindCollision), and Bridge.Process
// surfaces that error instead of holding the bundle. Deviations from the
// original prepareBundle gate: no stream-mismatch or High-owner
// refinement, and file records keep hold semantics.
package bridgetwostreams

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

const (
	// lowAName is the Low-A (exporter) cluster name; owns
	// runtime/bridge-two-streams-low-a/, which also holds staging-a so
	// artifacts are retained/cleaned with it.
	lowAName = "bridge-two-streams-low-a"
	// lowBName is the Low-B (exporter) cluster name; owns
	// runtime/bridge-two-streams-low-b/.
	lowBName = "bridge-two-streams-low-b"
	// highName is the High (importer) cluster name; owns
	// runtime/bridge-two-streams-high/.
	highName = "bridge-two-streams-high"
	// streamA and streamB are the bridge streams (Rust: stream-a,
	// stream-b).
	streamA = "stream-a"
	streamB = "stream-b"

	ddl = "CREATE TABLE live_typed_contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)"
)

const (
	// wait is the convergence deadline (Rust: 60 s waits).
	wait = 60 * time.Second
	// poll is the convergence poll interval (Rust: 200 ms polls).
	poll = 200 * time.Millisecond
)

// bytes16 fills a 16-byte id with one byte (Rust: fixed test constants).
func bytes16(b byte) [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = b
	}
	return id
}

// bytes32 fills a 32-byte key with one byte.
func bytes32(b byte) [32]byte {
	var k [32]byte
	for i := range k {
		k[i] = b
	}
	return k
}

// rowID is the row identity: %032x of a per-domain base plus the index
// (Rust: row_id).
func rowID(base, i uint32) string {
	return fmt.Sprintf("%032x", base+i)
}

// insertRow is a contention insert (Rust: TypedContentionInsert).
func insertRow(t *testing.T, db *murmur.Database, id, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO live_typed_contention(id, name) VALUES(?, ?)", id, name)
		return nil, err
	}); err != nil {
		t.Fatalf("contention insert failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after insert: %v", err)
	}
}

// updateName is a contention rename (Rust: provenance probes use the Name
// field).
func updateName(t *testing.T, db *murmur.Database, id, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("UPDATE live_typed_contention SET name = ? WHERE id = ?", name, id)
		return nil, err
	}); err != nil {
		t.Fatalf("contention update failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after update: %v", err)
	}
}

// sortedNames returns the sorted visible names (Rust: TypedContentionRows +
// digest over names).
func sortedNames(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM live_typed_contention ORDER BY name")
	})
	if err != nil {
		t.Fatalf("read contention names: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

// nodeBridge returns the bridge handle for one node.
func nodeBridge(t *testing.T, db *murmur.Database) *replication.Bridge {
	t.Helper()
	h := db.Replication()
	if h == nil {
		t.Fatalf("replication handle missing")
	}
	b := h.Bridge()
	if b == nil {
		t.Fatalf("bridge missing")
	}
	return b
}

// pump is the export+import acceleration pump (Rust: BridgeExport +
// BridgeImport). Counts are never asserted: the replication engine also
// drives capture/process in the background, so progress is proven by row
// convergence instead.
func pump(t *testing.T, lowA, lowB, high *harness.Cluster) {
	t.Helper()
	if _, err := nodeBridge(t, lowA.Node(0)).Capture(); err != nil {
		t.Fatalf("low-a capture: %v", err)
	}
	if _, err := nodeBridge(t, lowB.Node(0)).Capture(); err != nil {
		t.Fatalf("low-b capture: %v", err)
	}
	if _, err := nodeBridge(t, high.Node(0)).Process(); err != nil {
		t.Fatalf("high-0 process: %v", err)
	}
	if _, err := nodeBridge(t, high.Node(1)).Process(); err != nil {
		t.Fatalf("high-1 process: %v", err)
	}
}

// drain pumps until every bridge is idle (bounded idle loop; the
// bridge_files product-test pattern).
func drain(t *testing.T, lowA, lowB, high *harness.Cluster) {
	t.Helper()
	for range 100 {
		a, err := nodeBridge(t, lowA.Node(0)).Capture()
		if err != nil {
			t.Fatalf("low-a capture: %v", err)
		}
		b, err := nodeBridge(t, lowB.Node(0)).Capture()
		if err != nil {
			t.Fatalf("low-b capture: %v", err)
		}
		h0, err := nodeBridge(t, high.Node(0)).Process()
		if err != nil {
			t.Fatalf("high-0 process: %v", err)
		}
		h1, err := nodeBridge(t, high.Node(1)).Process()
		if err != nil {
			t.Fatalf("high-1 process: %v", err)
		}
		if a == 0 && b == 0 && h0 == 0 && h1 == 0 {
			return
		}
	}
	t.Fatalf("bridges did not become idle")
}

// waitHighNames waits until both High nodes show exactly want (Rust:
// waitHighConverged + assertNames, with the pump inside the poll).
func waitHighNames(t *testing.T, lowA, lowB, high *harness.Cluster, want []string) {
	t.Helper()
	converged := harness.WaitUntil(wait, poll, func() bool {
		pump(t, lowA, lowB, high)
		return slices.Equal(sortedNames(t, high.Node(0)), want) &&
			slices.Equal(sortedNames(t, high.Node(1)), want)
	})
	if !converged {
		t.Fatalf("high nodes did not converge on %d rows: node0=%v node1=%v",
			len(want), sortedNames(t, high.Node(0)), sortedNames(t, high.Node(1)))
	}
}

func TestTwoLowDomainsIntoOneHigh(t *testing.T) {
	// Independent bridge domains (Rust: distinct DBIDs, CAs, keys per
	// domain; each harness cluster already provisions its own CA).
	lowADbid := bytes16(0xA0)
	lowBDbid := bytes16(0xB0)
	highDbid := bytes16(0xC0)
	lowANode := bytes16(0xA1)
	lowBNode := bytes16(0xB1)
	highNode0 := bytes16(0xC1)
	highNode1 := bytes16(0xC2)
	lowAKey := bytes32(0xA2)
	lowBKey := bytes32(0xB2)
	highKey := bytes32(0xC3)
	// Low bundle-signer seeds (ed25519; each High importer pins its domain
	// key).
	lowASigner := bytes32(0x21)
	lowBSigner := bytes32(0x22)
	// High receiver secrets (X25519; one per stream).
	highSecretA := bytes32(0x31)
	highSecretB := bytes32(0x32)

	stagingA := harness.RuntimeDir(lowAName) + "/staging-a"
	stagingB := harness.RuntimeDir(lowBName) + "/staging-b"
	lowA, err := harness.NewCluster(harness.ClusterOptions{
		Name:        lowAName,
		NumNodes:    1,
		DatabaseID:  &lowADbid,
		NodeIDs:     [][16]byte{lowANode},
		Key:         lowAKey,
		ManualPeers: true,
		Bridge: harness.BridgeOptions{
			Nodes: map[int]*replication.BridgeConfig{
				0: replication.NewBridgeExporter(lowADbid, stagingA, streamA, lowASigner,
					replication.RecipientPublic(&highSecretA)),
			},
		},
	})
	if err != nil {
		t.Fatalf("low-a cluster setup: %v", err)
	}
	lowB, err := harness.NewCluster(harness.ClusterOptions{
		Name:        lowBName,
		NumNodes:    1,
		DatabaseID:  &lowBDbid,
		NodeIDs:     [][16]byte{lowBNode},
		Key:         lowBKey,
		ManualPeers: true,
		Bridge: harness.BridgeOptions{
			Nodes: map[int]*replication.BridgeConfig{
				0: replication.NewBridgeExporter(lowBDbid, stagingB, streamB, lowBSigner,
					replication.RecipientPublic(&highSecretB)),
			},
		},
	})
	if err != nil {
		t.Fatalf("low-b cluster setup: %v", err)
	}
	// Each High importer pins its own domain's origin key so imported
	// transactions verify; the seeds themselves stay inside the harness.
	lowAOrigin := lowA.NodeOriginPubkey(0)
	lowBOrigin := lowB.NodeOriginPubkey(0)
	high, err := harness.NewCluster(harness.ClusterOptions{
		Name:        highName,
		NumNodes:    2,
		DatabaseID:  &highDbid,
		NodeIDs:     [][16]byte{highNode0, highNode1},
		Key:         highKey,
		ManualPeers: true,
		Bridge: harness.BridgeOptions{
			Nodes: map[int]*replication.BridgeConfig{
				0: replication.NewBridgeReceiver(highDbid, stagingA, streamA, highSecretA,
					map[[16]byte]bool{lowADbid: true},
					map[[16]byte][32]byte{lowANode: replication.PublicKey(&lowASigner)},
					map[[16]byte][32]byte{lowANode: lowAOrigin},
				),
				1: replication.NewBridgeReceiver(highDbid, stagingB, streamB, highSecretB,
					map[[16]byte]bool{lowBDbid: true},
					map[[16]byte][32]byte{lowBNode: replication.PublicKey(&lowBSigner)},
					map[[16]byte][32]byte{lowBNode: lowBOrigin},
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
			lowA.DumpForensics("bridge-two-streams-low-a")
			lowB.DumpForensics("bridge-two-streams-low-b")
			high.DumpForensics("bridge-two-streams-high")
		}
		lowA.Finish(failed)
		lowB.Finish(failed)
		high.Finish(failed)
	}()

	// Identical DDL on all three domains, then form the High mesh
	// (manual_peers + mesh-after-migrate so a local migrate never races
	// schema auto-adoption of the identical DDL).
	for _, item := range []struct {
		label string
		db    *murmur.Database
	}{
		{"low-a", lowA.Node(0)},
		{"low-b", lowB.Node(0)},
		{"high-0", high.Node(0)},
		{"high-1", high.Node(1)},
	} {
		if err := item.db.Migrate(murmur.NewMigration(ddl)); err != nil {
			t.Fatalf("%s migrate failed: %v", item.label, err)
		}
		if err := item.db.SetReplication("live_typed_contention", true); err != nil {
			t.Fatalf("%s opt-in live_typed_contention: %v", item.label, err)
		}
		if err := item.db.Flush(); err != nil {
			t.Fatalf("%s flush after migrate failed: %v", item.label, err)
		}
	}
	if err := high.Mesh(); err != nil {
		t.Fatalf("high mesh: %v", err)
	}

	// Round 1: disjoint rows from both domains land on both High nodes.
	for i := uint32(0); i < 5; i++ {
		insertRow(t, lowA.Node(0), rowID(0xA000, i), fmt.Sprintf("a-%d", i))
		insertRow(t, lowB.Node(0), rowID(0xB000, i), fmt.Sprintf("b-%d", i))
	}
	var round1 []string
	for i := 0; i < 5; i++ {
		round1 = append(round1, fmt.Sprintf("a-%d", i))
	}
	for i := 0; i < 5; i++ {
		round1 = append(round1, fmt.Sprintf("b-%d", i))
	}
	slices.Sort(round1)
	waitHighNames(t, lowA, lowB, high, round1)

	// Duplicate delivery: pump to idle, then prove the converged set is
	// unchanged (receipt idempotence: redelivery applies nothing twice).
	drain(t, lowA, lowB, high)
	if got := sortedNames(t, high.Node(0)); !slices.Equal(got, round1) {
		t.Fatalf("high-0 changed after idle pumps: %v", got)
	}
	if got := sortedNames(t, high.Node(1)); !slices.Equal(got, round1) {
		t.Fatalf("high-1 changed after idle pumps: %v", got)
	}

	// Low-owned provenance (behavioral): a Low update to one row per
	// domain must flow through to both High nodes (a High-owned field
	// would suppress it); then restore the row names.
	probeA := rowID(0xA000, 0)
	probeB := rowID(0xB000, 0)
	updateName(t, lowA.Node(0), probeA, "a-0-prov")
	updateName(t, lowB.Node(0), probeB, "b-0-prov")
	proved := make([]string, 0, len(round1))
	for _, n := range round1 {
		switch n {
		case "a-0":
			proved = append(proved, "a-0-prov")
		case "b-0":
			proved = append(proved, "b-0-prov")
		default:
			proved = append(proved, n)
		}
	}
	slices.Sort(proved)
	waitHighNames(t, lowA, lowB, high, proved)
	updateName(t, lowA.Node(0), probeA, "a-0")
	updateName(t, lowB.Node(0), probeB, "b-0")
	waitHighNames(t, lowA, lowB, high, round1)

	// Round 2: stream-a only. New A rows arrive on both High nodes.
	for i := uint32(5); i < 8; i++ {
		insertRow(t, lowA.Node(0), rowID(0xA000, i), fmt.Sprintf("a-%d", i))
	}
	var round2 []string
	for i := 0; i < 8; i++ {
		round2 = append(round2, fmt.Sprintf("a-%d", i))
	}
	for i := 0; i < 5; i++ {
		round2 = append(round2, fmt.Sprintf("b-%d", i))
	}
	slices.Sort(round2)
	waitHighNames(t, lowA, lowB, high, round2)

	// Stream-b progress isolation (behavioral): a stream-b-only round
	// must deliver exactly once from the undisturbed cursor (no loss
	// from a skipped sequence, no duplication).
	for i := uint32(5); i < 7; i++ {
		insertRow(t, lowB.Node(0), rowID(0xB000, i), fmt.Sprintf("b-%d", i))
	}
	var round3 []string
	for i := 0; i < 8; i++ {
		round3 = append(round3, fmt.Sprintf("a-%d", i))
	}
	for i := 0; i < 7; i++ {
		round3 = append(round3, fmt.Sprintf("b-%d", i))
	}
	slices.Sort(round3)
	waitHighNames(t, lowA, lowB, high, round3)
	failed = false
}

// TestCrossDomainIdentityCollisionFailsClosed ports the original gate:
// domain A lands first; domain B's same-ID row must fail closed with a
// collision error, leaving first-writer state intact on both High nodes.
//
// Counts are never asserted (the engine also drives capture/process in the
// background); the collision itself is deterministic because a rejected
// bundle is never marked applied, so every Process call surfaces it.
func TestCrossDomainIdentityCollisionFailsClosed(t *testing.T) {
	lowADbid := bytes16(0xA0)
	lowBDbid := bytes16(0xB0)
	highDbid := bytes16(0xC0)
	lowANode := bytes16(0xA1)
	lowBNode := bytes16(0xB1)
	highNode0 := bytes16(0xC1)
	highNode1 := bytes16(0xC2)
	lowAKey := bytes32(0xA2)
	lowBKey := bytes32(0xB2)
	highKey := bytes32(0xC3)
	lowASigner := bytes32(0x21)
	lowBSigner := bytes32(0x22)
	highSecretA := bytes32(0x31)
	highSecretB := bytes32(0x32)

	colAName := lowAName + "-col"
	colBName := lowBName + "-col"
	colHName := highName + "-col"
	stagingA := harness.RuntimeDir(colAName) + "/staging-a"
	stagingB := harness.RuntimeDir(colBName) + "/staging-b"
	lowA, err := harness.NewCluster(harness.ClusterOptions{
		Name: colAName, NumNodes: 1, DatabaseID: &lowADbid,
		NodeIDs: [][16]byte{lowANode}, Key: lowAKey, ManualPeers: true,
		Bridge: harness.BridgeOptions{Nodes: map[int]*replication.BridgeConfig{
			0: replication.NewBridgeExporter(lowADbid, stagingA, streamA, lowASigner,
				replication.RecipientPublic(&highSecretA)),
		}},
	})
	if err != nil {
		t.Fatalf("low-a cluster setup: %v", err)
	}
	lowB, err := harness.NewCluster(harness.ClusterOptions{
		Name: colBName, NumNodes: 1, DatabaseID: &lowBDbid,
		NodeIDs: [][16]byte{lowBNode}, Key: lowBKey, ManualPeers: true,
		Bridge: harness.BridgeOptions{Nodes: map[int]*replication.BridgeConfig{
			0: replication.NewBridgeExporter(lowBDbid, stagingB, streamB, lowBSigner,
				replication.RecipientPublic(&highSecretB)),
		}},
	})
	if err != nil {
		t.Fatalf("low-b cluster setup: %v", err)
	}
	lowAOrigin := lowA.NodeOriginPubkey(0)
	lowBOrigin := lowB.NodeOriginPubkey(0)
	high, err := harness.NewCluster(harness.ClusterOptions{
		Name: colHName, NumNodes: 2, DatabaseID: &highDbid,
		NodeIDs: [][16]byte{highNode0, highNode1}, Key: highKey, ManualPeers: true,
		Bridge: harness.BridgeOptions{Nodes: map[int]*replication.BridgeConfig{
			0: replication.NewBridgeReceiver(highDbid, stagingA, streamA, highSecretA,
				map[[16]byte]bool{lowADbid: true},
				map[[16]byte][32]byte{lowANode: replication.PublicKey(&lowASigner)},
				map[[16]byte][32]byte{lowANode: lowAOrigin},
			),
			1: replication.NewBridgeReceiver(highDbid, stagingB, streamB, highSecretB,
				map[[16]byte]bool{lowBDbid: true},
				map[[16]byte][32]byte{lowBNode: replication.PublicKey(&lowBSigner)},
				map[[16]byte][32]byte{lowBNode: lowBOrigin},
			),
		}},
	})
	if err != nil {
		t.Fatalf("high cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			lowA.DumpForensics(colAName)
			lowB.DumpForensics(colBName)
			high.DumpForensics(colHName)
		}
		lowA.Finish(failed)
		lowB.Finish(failed)
		high.Finish(failed)
	}()

	for _, item := range []struct {
		label string
		db    *murmur.Database
	}{
		{"low-a", lowA.Node(0)},
		{"low-b", lowB.Node(0)},
		{"high-0", high.Node(0)},
		{"high-1", high.Node(1)},
	} {
		if err := item.db.Migrate(murmur.NewMigration(ddl)); err != nil {
			t.Fatalf("%s migrate failed: %v", item.label, err)
		}
		if err := item.db.SetReplication("live_typed_contention", true); err != nil {
			t.Fatalf("%s opt-in live_typed_contention: %v", item.label, err)
		}
		if err := item.db.Flush(); err != nil {
			t.Fatalf("%s flush after migrate failed: %v", item.label, err)
		}
	}
	if err := high.Mesh(); err != nil {
		t.Fatalf("high mesh: %v", err)
	}

	// Domain A lands first.
	row := fmt.Sprintf("%032x", 0xC001)
	insertRow(t, lowA.Node(0), row, "from-a")
	pumpOne := func() {
		t.Helper()
		if _, err := nodeBridge(t, lowA.Node(0)).Capture(); err != nil {
			t.Fatalf("low-a capture: %v", err)
		}
		if _, err := nodeBridge(t, high.Node(0)).Process(); err != nil {
			t.Fatalf("high-0 process: %v", err)
		}
		if _, err := nodeBridge(t, high.Node(1)).Process(); err != nil {
			t.Fatalf("high-1 process: %v", err)
		}
	}
	want := []string{"from-a"}
	converged := harness.WaitUntil(wait, poll, func() bool {
		pumpOne()
		return slices.Equal(sortedNames(t, high.Node(0)), want) &&
			slices.Equal(sortedNames(t, high.Node(1)), want)
	})
	if !converged {
		t.Fatalf("high nodes did not converge on from-a: node0=%v node1=%v",
			sortedNames(t, high.Node(0)), sortedNames(t, high.Node(1)))
	}

	// Domain B's same-ID row must fail closed.
	insertRow(t, lowB.Node(0), row, "from-b")
	if _, err := nodeBridge(t, lowB.Node(0)).Capture(); err != nil {
		t.Fatalf("low-b capture: %v", err)
	}
	_, err = nodeBridge(t, high.Node(1)).Process()
	if err == nil {
		t.Fatal("colliding import succeeded, want identity-collision failure")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "collides") {
		t.Fatalf("collision error %q does not name the collision", err)
	}

	// First-writer state stands untouched on both High nodes.
	for i := range 2 {
		got := sortedNames(t, high.Node(i))
		if !slices.Equal(got, want) {
			t.Fatalf("high-%d state = %v, want intact from-a", i, got)
		}
	}
	failed = false
}

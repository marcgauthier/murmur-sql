// Live scenario: High/Low one-way bridge domains over a directory drop.
//
// Go port of the Rust `highlow` scenario
// (/home/marc/MURMUR/tests-live/highlow/tests/highlow.rs), itself a port of
// the original Go `highlow` + ownership-reorder scenarios. Real Low
// (exporter) and High (importer) domains move logical writes across a
// directory drop: disconnected transfer with one-way role gating, gap
// delivery with duplicate recovery, two-receiver convergence,
// forgery/corruption/misnaming rejection without partial apply, outbox/inbox
// restart resumption, schema-hold survival across restart with release after
// a local High migration (no DDL crosses), signer rotation plus outage
// catch-up, and High-owned-field reorder convergence across two meshed High
// peers with opposite import/override orders (plus release convergence back
// to the latest Low value).
//
// Plain SQL via Database.Transaction/Database.Read replaces the managed
// typed HTTP API; contacts() is the name -> score visible-state map, with
// plain LWW score increments standing in for the sequential single-writer
// TypedCounterAdd legs, which converge identically. BridgeExport/BridgeImport
// map to direct Bridge.Capture() (Low) and Bridge.Process() (High) pumps.
//
// Directory-drop routing: Low and High use SEPARATE staging dirs and the
// test ferries *.bundle files one way via Ferry; only what the test ferries
// is visible to High, which keeps gap/withhold/forgery/outage legs
// deterministic. Bundle filenames (<nodehex>-<seq:020>.bundle) sort in
// export order, so the middle-bundle withhold mirrors the original exactly.
//
// Count/progress mapping: per-call captured/published/received/imported
// counts and BridgeStatus progress have no public equivalent and are racy
// under background driving, so every such assertion is proven behaviorally
// instead: gap holds by the exact visible row set after bounded pumps,
// duplicate redelivery by state stability plus the bundle staying out of
// quarantine, restart resume by Low's bundle file set being unchanged and
// High's rows holding exactly, and outage catch-up by the exact converged
// counter (a lost or doubled event would show a different total).
// Bridge.Process() quarantines undecryptable artifacts into
// staging/quarantine/ and returns nil (the original: import error), so
// forgery legs assert quarantine placement + empty High + no recorded
// progress (the genuine bundle still applies afterwards); only the
// unreadable-file leg surfaces an error. High/Low field provenance has no
// public API, so ownership is proven behaviorally: High-owned by a Low
// update arriving yet staying suppressed, Low-owned by it flowing through.
//
// Signer rotation: the bridge pins a single signer per source node
// (trusted_signers), so overlap is inexpressible: round 1 is imported
// BEFORE the rotation (config edit plus restart on both sides via
// Cluster.SetBridgeConfig), round 2 imports after, and a retired-signer
// replay then fails closed into quarantine while a current-key duplicate
// still stages cleanly.
package highlow

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// stream is the bridge stream (original: `field-records`).
const stream = "field-records"

// table is the contacts table.
const table = "contacts"

const baseDDL = "CREATE TABLE contacts(id TEXT NOT NULL PRIMARY KEY, name TEXT, score INTEGER)"
const emailDDL = "CREATE TABLE contacts(id TEXT NOT NULL PRIMARY KEY, name TEXT, score INTEGER, email TEXT)"
const addEmail = "ALTER TABLE contacts ADD COLUMN email TEXT"

// wait is the convergence deadline (original: 30 s waits).
const wait = 30 * time.Second

// poll is the convergence poll interval (original: 100 ms polls, 50 ms here
// as in Rust).
const poll = 50 * time.Millisecond

func repeat16(b byte) [16]byte {
	var a [16]byte
	for i := range a {
		a[i] = b
	}
	return a
}

func repeat32(b byte) [32]byte {
	var a [32]byte
	for i := range a {
		a[i] = b
	}
	return a
}

// Independent bridge domains. Fixed test vectors shared by every test in
// this file (clusters never interact across tests).
var (
	lowDBID   = repeat16(0xD0)
	highDBID  = repeat16(0xD1)
	rogueDBID = repeat16(0xD2)
	lowNode   = repeat16(0x10)
	highNode0 = repeat16(0x11)
	highNode1 = repeat16(0x12)
	rogueNode = repeat16(0x13)
	// Low bundle-signer seeds (ed25519; High pins the verifying keys).
	lowSignerA  = repeat32(0x21)
	lowSignerB  = repeat32(0x23)
	rogueSigner = repeat32(0x24)
	// Receiver secrets (X25519; Low encrypts to the derived recipients).
	highSecret  = repeat32(0x22)
	otherSecret = repeat32(0x25)
	rogueSecret = repeat32(0x26)
)

// insertContact inserts a contact (original: writeContact).
func insertContact(t *testing.T, db *murmur.Database, id, name string, score int64) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute(
			"INSERT INTO contacts(id, name, score) VALUES(?, ?, ?)",
			id, name, score,
		)
		return nil, err
	}); err != nil {
		t.Fatalf("contact insert failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after contact insert: %v", err)
	}
}

// rename renames a contact (original: TypedRename).
func rename(t *testing.T, db *murmur.Database, id, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		n, err := s.Execute("UPDATE contacts SET name = ? WHERE id = ?", name, id)
		if err != nil {
			return nil, err
		}
		if n != 1 {
			return nil, fmt.Errorf("rename hit %d rows", n)
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("contact rename failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after contact rename: %v", err)
	}
}

// setEmail updates the known-field email (original: TypedSetNote).
func setEmail(t *testing.T, db *murmur.Database, id, email string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		n, err := s.Execute("UPDATE contacts SET email = ? WHERE id = ?", email, id)
		if err != nil {
			return nil, err
		}
		if n != 1 {
			return nil, fmt.Errorf("email update hit %d rows", n)
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("contact email update failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after email update: %v", err)
	}
}

// addScore applies a sequential score increment (original:
// TypedCounterAdd; single-writer legs converge identically under LWW).
func addScore(t *testing.T, db *murmur.Database, id string, delta int64) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		n, err := s.Execute("UPDATE contacts SET score = score + ? WHERE id = ?", delta, id)
		if err != nil {
			return nil, err
		}
		if n != 1 {
			return nil, fmt.Errorf("score update hit %d rows", n)
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("score update failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after score update: %v", err)
	}
}

// queryContacts reads the visible contacts as name -> score (original:
// contacts()). Errors are returned so convergence polls can treat them as
// not-yet-converged.
func queryContacts(db *murmur.Database) (map[string]int64, error) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name, score FROM contacts")
	})
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64)
	for _, row := range v.([][]sqlite.Value) {
		out[row[0].Str] = row[1].Int
	}
	return out, nil
}

// mustContacts reads the visible contacts, failing the test on error.
func mustContacts(t *testing.T, db *murmur.Database) map[string]int64 {
	t.Helper()
	contacts, err := queryContacts(db)
	if err != nil {
		t.Fatalf("read contacts failed: %v", err)
	}
	return contacts
}

// emailOf reads the email field (original: TypedNote); ("", false) when the
// column is absent (pre-migration High), the value is NULL, or the row is
// missing. Tolerant by design for convergence polls.
func emailOf(db *murmur.Database, id string) (string, bool) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT email FROM contacts WHERE id = ?", id)
	})
	if err != nil {
		return "", false
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 || rows[0][0].Type == sqlite.Null {
		return "", false
	}
	return rows[0][0].Str, true
}

// hasColumn reports whether table currently has column, via a tolerant
// SELECT probe (a missing column fails prepare; an empty table with the
// column returns no rows without error).
func hasColumn(db *murmur.Database, table, column string) bool {
	_, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT " + column + " FROM " + table + " LIMIT 1")
	})
	return err == nil
}

func migrate(t *testing.T, db *murmur.Database, ddl, label string) {
	t.Helper()
	if err := db.Migrate(murmur.NewMigration(ddl)); err != nil {
		t.Fatalf("%s migrate failed: %v", label, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("%s flush after migrate failed: %v", label, err)
	}
}

// Opt in after every node migrated: opting in publishes the schema
// immediately, and an early publication lets a peer adopt the table before
// its own migrate runs ("table already exists").
func optIn(t *testing.T, db *murmur.Database, label string) {
	t.Helper()
	// Every DDL here concerns `contacts`; re-opt-in after ALTERs is
	// idempotent.
	if err := db.SetReplication("contacts", true); err != nil {
		t.Fatalf("%s opt-in failed: %v", label, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("%s flush after opt-in failed: %v", label, err)
	}
}

func nodeBridge(t *testing.T, db *murmur.Database) *replication.Bridge {
	t.Helper()
	h := db.Replication()
	if h == nil {
		t.Fatalf("no replication handle")
	}
	b := h.Bridge()
	if b == nil {
		t.Fatalf("no bridge")
	}
	return b
}

// drainLow pumps Low until nothing remains to export (bounded idle loop).
// Sound: capture cursors are durable and only the test writes, so a zero
// return means quiescent.
func drainLow(t *testing.T, low *harness.Cluster) {
	t.Helper()
	for i := 0; i < 200; i++ {
		exported, err := nodeBridge(t, low.Node(0)).Capture()
		if err != nil {
			t.Fatalf("low bridge capture: %v", err)
		}
		if exported == 0 {
			return
		}
	}
	t.Fatalf("low bridge did not become idle")
}

// pumpHigh runs one import pump over the listed High nodes (original:
// BridgeImport).
func pumpHigh(t *testing.T, high *harness.Cluster, idxs ...int) {
	t.Helper()
	for _, i := range idxs {
		if _, err := nodeBridge(t, high.Node(i)).Process(); err != nil {
			t.Fatalf("high node%d bridge process: %v", i, err)
		}
	}
}

// bundleFiles lists sorted *.bundle names directly under dir (original:
// stagingNames).
func bundleFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read dir %s: %v", dir, err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".bundle") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

// clearBundles removes every *.bundle file directly under dir.
func clearBundles(t *testing.T, dir string) {
	t.Helper()
	for _, name := range bundleFiles(t, dir) {
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			t.Fatalf("clear %s: %v", filepath.Join(dir, name), err)
		}
	}
}

// quarantineFiles lists sorted names under staging/quarantine/ (empty when
// no dir yet).
func quarantineFiles(t *testing.T, staging string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(staging, "quarantine"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read quarantine: %v", err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

// clearQuarantine removes every file under staging/quarantine/.
func clearQuarantine(t *testing.T, staging string) {
	t.Helper()
	for _, name := range quarantineFiles(t, staging) {
		if err := os.Remove(filepath.Join(staging, "quarantine", name)); err != nil {
			t.Fatalf("clear quarantine file: %v", err)
		}
	}
}

// ferry is the one-way directory drop: Low exports into low, the test
// ferries *.bundle files into high. Ferries are tracked so ferryAll only
// moves new bundles; targeted withholds use FerryNames.
type ferry struct {
	t    *testing.T
	low  string
	high string
	done map[string]bool
}

func newFerry(t *testing.T, low, high string) *ferry {
	t.Helper()
	return &ferry{t: t, low: low, high: high, done: make(map[string]bool)}
}

// newBundles lists bundles exported since the last ferry, in export order.
func (f *ferry) newBundles() []string {
	f.t.Helper()
	var out []string
	for _, n := range bundleFiles(f.t, f.low) {
		if !f.done[n] {
			out = append(out, n)
		}
	}
	return out
}

// ferryAll ferries every new bundle to High.
func (f *ferry) ferryAll() {
	f.t.Helper()
	f.ferryNames(f.newBundles())
}

// ferryNames ferries exactly names (already-exported bundles) to High.
func (f *ferry) ferryNames(names []string) {
	f.t.Helper()
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(f.low, name))
		if err != nil {
			f.t.Fatalf("ferry %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(f.high, name), raw, 0o600); err != nil {
			f.t.Fatalf("ferry %s: %v", name, err)
		}
		f.done[name] = true
	}
}

// openExporter opens the single-node Low exporter (original: newLowCluster).
func openExporter(t *testing.T, name string, dbid, node [16]byte, staging string, signer, recipient [32]byte) *harness.Cluster {
	t.Helper()
	dbidCopy := dbid
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:        name,
		NumNodes:    1,
		DatabaseID:  &dbidCopy,
		NodeIDs:     [][16]byte{node},
		ManualPeers: true,
		Bridge: harness.BridgeOptions{
			Nodes: map[int]*replication.BridgeConfig{
				0: replication.NewBridgeExporter(dbid, staging, stream, signer, recipient),
			},
		},
	})
	if err != nil {
		t.Fatalf("low cluster setup: %v", err)
	}
	return cluster
}

// openHigh opens the High importer cluster (original: newHighCluster); one
// staging dir per node. Always ManualPeers: the test meshes explicitly
// after migrating.
func openHigh(t *testing.T, name string, dbid [16]byte, nodes [][16]byte, stagings []string, secret [32]byte, domains map[[16]byte]bool, signers, origins map[[16]byte][32]byte) *harness.Cluster {
	t.Helper()
	if len(nodes) != len(stagings) {
		t.Fatalf("nodes/stagings length mismatch")
	}
	dbidCopy := dbid
	receivers := make(map[int]*replication.BridgeConfig, len(nodes))
	for i, staging := range stagings {
		receivers[i] = replication.NewBridgeReceiver(dbid, staging, stream, secret, domains, signers, origins)
	}
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:        name,
		NumNodes:    len(nodes),
		DatabaseID:  &dbidCopy,
		NodeIDs:     nodes,
		ManualPeers: true,
		Bridge:      harness.BridgeOptions{Nodes: receivers},
	})
	if err != nil {
		t.Fatalf("high cluster setup: %v", err)
	}
	return cluster
}

// lowSigners pins Low's bundle signer (the seed itself stays out of the
// receiver config).
func lowSigners(signer [32]byte) map[[16]byte][32]byte {
	return map[[16]byte][32]byte{lowNode: replication.PublicKey(&signer)}
}

func lowOrigins(low *harness.Cluster) map[[16]byte][32]byte {
	return map[[16]byte][32]byte{lowNode: low.NodeOriginPubkey(0)}
}

// waitHigh waits until High node idx shows exactly want (original: contact
// waits plus assertContactsEqual), pumping Low capture + the importer
// inside the poll. The ferry stays explicit (withhold legs must not
// auto-ferry).
func waitHigh(t *testing.T, low, high *harness.Cluster, idx int, want map[string]int64) {
	t.Helper()
	converged := harness.WaitUntil(wait, poll, func() bool {
		if _, err := nodeBridge(t, low.Node(0)).Capture(); err != nil {
			t.Fatalf("low bridge capture: %v", err)
		}
		if _, err := nodeBridge(t, high.Node(idx)).Process(); err != nil {
			t.Fatalf("high bridge process: %v", err)
		}
		got, err := queryContacts(high.Node(idx))
		return err == nil && equalContacts(got, want)
	})
	if !converged {
		t.Fatalf("high node%d contacts = %v, want %v", idx, mustContacts(t, high.Node(idx)), want)
	}
}

// waitMeshed waits until every listed High node shows exactly want over the
// mesh (no bridge traffic remains; plain convergence polling).
func waitMeshed(t *testing.T, high *harness.Cluster, idxs []int, want map[string]int64, what string) {
	t.Helper()
	converged := harness.WaitUntil(wait, poll, func() bool {
		for _, i := range idxs {
			got, err := queryContacts(high.Node(i))
			if err != nil || !equalContacts(got, want) {
				return false
			}
		}
		return true
	})
	if !converged {
		var states []map[string]int64
		for _, i := range idxs {
			states = append(states, mustContacts(t, high.Node(i)))
		}
		t.Fatalf("%s: high nodes did not converge on %v: %v", what, want, states)
	}
}

// releaseName releases High ownership of row's name field on node A
// (original: ReleaseOwnership; the key is the postcard-encoded PK value
// vector), retrying until the cursors align.
func releaseName(t *testing.T, high *harness.Cluster, row string) {
	t.Helper()
	key := murmur.EncodeValues([]murmur.Value{murmur.TextValue(row)})
	column := "name"
	start := time.Now()
	for {
		released, err := nodeBridge(t, high.Node(0)).Release(table, key, &column)
		if err != nil {
			t.Fatalf("bridge release: %v", err)
		}
		if released {
			return
		}
		if time.Since(start) >= wait {
			t.Fatalf("bridge release never made progress")
		}
		time.Sleep(poll)
	}
}

func equalContacts(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func hasKey(m map[string]int64, k string) bool {
	_, ok := m[k]
	return ok
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

// stagingDirs joins the runtime staging dir for a cluster name.
func stagingDir(clusterName string) string {
	return filepath.Join(harness.RuntimeDir(clusterName), "staging")
}

// TestHighLowDisconnectedTransferLive checks disconnected transfer with
// one-way role gating (Rust: disconnected_transfer_with_one_way_role_gating).
func TestHighLowDisconnectedTransferLive(t *testing.T) {
	stagingLow := stagingDir("highlow-transfer-low")
	stagingHigh := stagingDir("highlow-transfer-high")
	low := openExporter(t, "highlow-transfer-low", lowDBID, lowNode, stagingLow, lowSignerA, replication.RecipientPublic(&highSecret))
	high := openHigh(t, "highlow-transfer-high", highDBID, [][16]byte{highNode0}, []string{stagingHigh},
		highSecret, map[[16]byte]bool{lowDBID: true}, lowSigners(lowSignerA), lowOrigins(low))
	migrate(t, low.Node(0), baseDDL, "low")
	migrate(t, high.Node(0), baseDDL, "high")
	optIn(t, low.Node(0), "low")
	optIn(t, high.Node(0), "high")
	fer := newFerry(t, stagingLow, stagingHigh)

	failed := true
	defer func() {
		if failed {
			low.DumpForensics("highlow-transfer-low")
			high.DumpForensics("highlow-transfer-high")
		}
		low.Finish(failed)
		high.Finish(failed)
	}()

	if low.DatabaseID() == high.DatabaseID() {
		t.Fatalf("Low and High share a DBID; domains must differ")
	}
	// One-way role gating at the API boundary: the exporter never imports
	// and the importer never exports.
	if n, err := nodeBridge(t, low.Node(0)).Process(); err != nil || n != 0 {
		t.Fatalf("low exporter imported (n=%d err=%v)", n, err)
	}
	if n, err := nodeBridge(t, high.Node(0)).Capture(); err != nil || n != 0 {
		t.Fatalf("high importer exported (n=%d err=%v)", n, err)
	}

	insertContact(t, low.Node(0), "ann-id", "ann", 1)
	drainLow(t, low)
	if len(fer.newBundles()) == 0 {
		t.Fatalf("low exported no bundles for the contact")
	}
	fer.ferryAll()
	want := map[string]int64{"ann": 1}
	waitHigh(t, low, high, 0, want)
	if !equalContacts(mustContacts(t, low.Node(0)), mustContacts(t, high.Node(0))) {
		t.Fatalf("low/high contact states diverged")
	}
	failed = false
}

// TestHighLowDuplicateReorderRecoveryLive checks gap delivery with duplicate
// recovery (Rust: gap_delivery_with_duplicate_recovery).
func TestHighLowDuplicateReorderRecoveryLive(t *testing.T) {
	stagingLow := stagingDir("highlow-gap-low")
	stagingHigh := stagingDir("highlow-gap-high")
	low := openExporter(t, "highlow-gap-low", lowDBID, lowNode, stagingLow, lowSignerA, replication.RecipientPublic(&highSecret))
	high := openHigh(t, "highlow-gap-high", highDBID, [][16]byte{highNode0}, []string{stagingHigh},
		highSecret, map[[16]byte]bool{lowDBID: true}, lowSigners(lowSignerA), lowOrigins(low))
	migrate(t, low.Node(0), baseDDL, "low")
	migrate(t, high.Node(0), baseDDL, "high")
	optIn(t, low.Node(0), "low")
	optIn(t, high.Node(0), "high")
	fer := newFerry(t, stagingLow, stagingHigh)

	failed := true
	defer func() {
		if failed {
			low.DumpForensics("highlow-gap-low")
			high.DumpForensics("highlow-gap-high")
		}
		low.Finish(failed)
		high.Finish(failed)
	}()

	// Baseline: drain the migration bundles so the three contact rounds
	// below are exactly three new bundles.
	drainLow(t, low)
	fer.ferryAll()
	pumpHigh(t, high, 0)
	if len(mustContacts(t, high.Node(0))) != 0 {
		t.Fatalf("high not empty after baseline")
	}

	insertContact(t, low.Node(0), "ann-id", "ann", 0)
	drainLow(t, low)
	insertContact(t, low.Node(0), "bob-id", "bob", 1)
	drainLow(t, low)
	insertContact(t, low.Node(0), "cid-id", "cid", 2)
	drainLow(t, low)
	fresh := fer.newBundles()
	if len(fresh) != 3 {
		t.Fatalf("expected 3 contact bundles, staged %v", fresh)
	}

	// Withhold the middle bundle: the importer applies the first and
	// records a gap instead of skipping ahead. The gap bundle can never
	// apply (sequence check), so bounded explicit pumps prove the hold
	// without sleeping.
	fer.ferryNames([]string{fresh[0], fresh[2]})
	for i := 0; i < 10; i++ {
		pumpHigh(t, high, 0)
	}
	if got := mustContacts(t, high.Node(0)); !equalContacts(got, map[string]int64{"ann": 0}) {
		t.Fatalf("gap leg applied the wrong set: %v", got)
	}

	// Restoring the bundle fills the gap; all three converge.
	fer.ferryNames([]string{fresh[1]})
	want := map[string]int64{"ann": 0, "bob": 1, "cid": 2}
	waitHigh(t, low, high, 0, want)

	// Duplicate redelivery is a no-op: re-copy an applied bundle and prove
	// the visible state is unchanged and the bundle is not quarantined.
	dup, err := os.ReadFile(filepath.Join(fer.low, fresh[0]))
	if err != nil {
		t.Fatalf("read bundle for redelivery: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fer.high, fresh[0]), dup, 0o600); err != nil {
		t.Fatalf("duplicate redelivery: %v", err)
	}
	for i := 0; i < 10; i++ {
		pumpHigh(t, high, 0)
	}
	if got := mustContacts(t, high.Node(0)); !equalContacts(got, want) {
		t.Fatalf("duplicate redelivery changed state: %v", got)
	}
	if _, err := os.Stat(filepath.Join(fer.high, fresh[0])); err != nil {
		t.Fatalf("duplicate of an applied bundle was quarantined")
	}
	if q := quarantineFiles(t, fer.high); len(q) != 0 {
		t.Fatalf("duplicate redelivery quarantined: %v", q)
	}
	if !equalContacts(mustContacts(t, low.Node(0)), mustContacts(t, high.Node(0))) {
		t.Fatalf("low/high contact states diverged")
	}
	failed = false
}

// TestHighLowTwoReceiversConvergeLive checks that two receivers converge:
// each receiver imports the same staging independently, and the High mesh
// then converges both to identical state (Rust: two_receivers_converge).
func TestHighLowTwoReceiversConvergeLive(t *testing.T) {
	stagingLow := stagingDir("highlow-2r-low")
	stagingHigh := stagingDir("highlow-2r-high")
	low := openExporter(t, "highlow-2r-low", lowDBID, lowNode, stagingLow, lowSignerA, replication.RecipientPublic(&highSecret))
	high := openHigh(t, "highlow-2r-high", highDBID, [][16]byte{highNode0, highNode1}, []string{stagingHigh, stagingHigh},
		highSecret, map[[16]byte]bool{lowDBID: true}, lowSigners(lowSignerA), lowOrigins(low))
	migrate(t, low.Node(0), baseDDL, "low")
	migrate(t, high.Node(0), baseDDL, "high-0")
	migrate(t, high.Node(1), baseDDL, "high-1")
	optIn(t, low.Node(0), "low")
	optIn(t, high.Node(0), "high-0")
	optIn(t, high.Node(1), "high-1")
	if err := high.Mesh(); err != nil {
		t.Fatalf("high mesh: %v", err)
	}
	fer := newFerry(t, stagingLow, stagingHigh)

	failed := true
	defer func() {
		if failed {
			low.DumpForensics("highlow-2r-low")
			high.DumpForensics("highlow-2r-high")
		}
		low.Finish(failed)
		high.Finish(failed)
	}()

	insertContact(t, low.Node(0), "ann-id", "ann", 1)
	drainLow(t, low)
	fer.ferryAll()
	want := map[string]int64{"ann": 1}
	waitHigh(t, low, high, 0, want)
	waitHigh(t, low, high, 1, want)
	if !equalContacts(mustContacts(t, high.Node(0)), mustContacts(t, high.Node(1))) {
		t.Fatalf("receivers diverged")
	}
	if !equalContacts(mustContacts(t, low.Node(0)), mustContacts(t, high.Node(0))) {
		t.Fatalf("low/high contact states diverged")
	}
	failed = false
}

// TestHighLowForgeriesRejectedLive checks that forgeries are rejected
// without partial apply or recorded progress (Rust:
// forgeries_rejected_without_partial_apply).
func TestHighLowForgeriesRejectedLive(t *testing.T) {
	stagingLow := stagingDir("highlow-forge-low")
	stagingHigh := stagingDir("highlow-forge-high")
	stagingRogue := stagingDir("highlow-forge-rogue")
	stagingOther := stagingDir("highlow-forge-other")
	low := openExporter(t, "highlow-forge-low", lowDBID, lowNode, stagingLow, lowSignerA, replication.RecipientPublic(&highSecret))
	high := openHigh(t, "highlow-forge-high", highDBID, [][16]byte{highNode0}, []string{stagingHigh},
		highSecret, map[[16]byte]bool{lowDBID: true}, lowSigners(lowSignerA), lowOrigins(low))
	// Rogue domain: its own signer and recipient, exporting one bundle.
	rogue := openExporter(t, "highlow-forge-rogue", rogueDBID, rogueNode, stagingRogue, rogueSigner, replication.RecipientPublic(&rogueSecret))
	// Wrong-recipient domain: main signer and identity, unknown recipient,
	// so the bundle reaches the recipient check (a distinct domain or node
	// would fail earlier at the domain/signer checks).
	other := openExporter(t, "highlow-forge-other", lowDBID, lowNode, stagingOther, lowSignerA, replication.RecipientPublic(&otherSecret))
	migrate(t, low.Node(0), baseDDL, "low")
	migrate(t, high.Node(0), baseDDL, "high")
	migrate(t, rogue.Node(0), baseDDL, "rogue")
	migrate(t, other.Node(0), baseDDL, "other")
	optIn(t, low.Node(0), "low")
	optIn(t, high.Node(0), "high")
	optIn(t, rogue.Node(0), "rogue")
	optIn(t, other.Node(0), "other")
	fer := newFerry(t, stagingLow, stagingHigh)

	failed := true
	defer func() {
		if failed {
			low.DumpForensics("highlow-forge-low")
			high.DumpForensics("highlow-forge-high")
			rogue.DumpForensics("highlow-forge-rogue")
			other.DumpForensics("highlow-forge-other")
		}
		low.Finish(failed)
		high.Finish(failed)
		rogue.Finish(failed)
		other.Finish(failed)
	}()

	// Baseline the migration bundles, then stage exactly one genuine
	// contact bundle and clear it: each hostile artifact below is
	// presented alone.
	drainLow(t, low)
	fer.ferryAll()
	pumpHigh(t, high, 0)
	clearBundles(t, fer.high)
	insertContact(t, low.Node(0), "ann-id", "ann", 1)
	drainLow(t, low)
	fer.ferryAll()
	genuine := bundleFiles(t, fer.high)
	if len(genuine) != 1 {
		t.Fatalf("want 1 genuine bundle, staged %v", genuine)
	}
	goodRaw, err := os.ReadFile(filepath.Join(fer.high, genuine[0]))
	if err != nil {
		t.Fatalf("read genuine bundle: %v", err)
	}
	if len(goodRaw) == 0 {
		t.Fatalf("genuine bundle is empty")
	}
	clearBundles(t, fer.high)

	drainLow(t, rogue)
	insertContact(t, rogue.Node(0), "mallory-id", "mallory", 9)
	drainLow(t, rogue)
	rogueFiles := bundleFiles(t, stagingRogue)
	if len(rogueFiles) == 0 {
		t.Fatalf("rogue exported no bundles")
	}
	rogueRaw, err := os.ReadFile(filepath.Join(stagingRogue, rogueFiles[len(rogueFiles)-1]))
	if err != nil {
		t.Fatalf("read rogue bundle: %v", err)
	}
	drainLow(t, other)
	insertContact(t, other.Node(0), "carol-id", "carol", 3)
	drainLow(t, other)
	otherFiles := bundleFiles(t, stagingOther)
	if len(otherFiles) == 0 {
		t.Fatalf("wrong-recipient domain exported no bundles")
	}
	otherRaw, err := os.ReadFile(filepath.Join(stagingOther, otherFiles[len(otherFiles)-1]))
	if err != nil {
		t.Fatalf("read wrong-recipient bundle: %v", err)
	}

	tampered := append([]byte(nil), goodRaw...)
	tampered[len(tampered)-1] ^= 0xFF
	oversized := append(append([]byte(nil), goodRaw...), make([]byte, 17<<20)...)
	hostile := []struct {
		name string
		raw  []byte
	}{
		{"rogue.bundle", rogueRaw},
		{"sealed.bundle", otherRaw},
		{"evil.bundle", tampered},
		{"big.bundle", oversized},
	}
	for _, h := range hostile {
		if err := os.WriteFile(filepath.Join(fer.high, h.name), h.raw, 0o600); err != nil {
			t.Fatalf("stage hostile artifact: %v", err)
		}
		// Fail-closed quarantine (the original's import returns an error;
		// here the bridge quarantines and returns nil).
		pumpHigh(t, high, 0)
		if got := mustContacts(t, high.Node(0)); len(got) != 0 {
			t.Fatalf("hostile artifact %s partially applied: %v", h.name, got)
		}
		if q := quarantineFiles(t, fer.high); !equalStrings(q, []string{h.name}) {
			t.Fatalf("hostile artifact %s quarantine = %v", h.name, q)
		}
		if err := os.Remove(filepath.Join(fer.high, "quarantine", h.name)); err != nil {
			t.Fatalf("clear quarantine: %v", err)
		}
	}

	// Wrong extensions are not bridge artifacts at all: ignored in place,
	// never quarantined, never applied.
	for _, name := range []string{"wrongname.txt", ".hidden"} {
		if err := os.WriteFile(filepath.Join(fer.high, name), goodRaw, 0o600); err != nil {
			t.Fatalf("stage misnamed artifact: %v", err)
		}
		pumpHigh(t, high, 0)
		if got := mustContacts(t, high.Node(0)); len(got) != 0 {
			t.Fatalf("misnamed artifact %s applied: %v", name, got)
		}
		if q := quarantineFiles(t, fer.high); len(q) != 0 {
			t.Fatalf("misnamed artifact %s quarantined: %v", name, q)
		}
		if _, err := os.Stat(filepath.Join(fer.high, name)); err != nil {
			t.Fatalf("ignored artifact %s was moved", name)
		}
		if err := os.Remove(filepath.Join(fer.high, name)); err != nil {
			t.Fatalf("clear misnamed: %v", err)
		}
	}

	// An unreadable staging file fails the import without partial apply.
	// Created unreadable atomically so the background importer cannot
	// stage it first.
	unreadable := filepath.Join(fer.high, "unreadable.bundle")
	f, err := os.OpenFile(unreadable, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o000)
	if err != nil {
		t.Fatalf("stage unreadable artifact: %v", err)
	}
	if _, err := f.Write(goodRaw); err != nil {
		_ = f.Close()
		t.Fatalf("write unreadable artifact: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close unreadable artifact: %v", err)
	}
	if _, err := os.ReadFile(unreadable); err == nil {
		t.Log("SKIP unreadable leg: staging file still readable (e.g. running as root)")
	} else {
		if _, err := nodeBridge(t, high.Node(0)).Process(); err == nil {
			t.Fatalf("unreadable artifact accepted")
		} else {
			t.Logf("unreadable import failed closed: %v", err)
		}
		if got := mustContacts(t, high.Node(0)); len(got) != 0 {
			t.Fatalf("unreadable artifact partially applied: %v", got)
		}
	}
	if err := os.Chmod(unreadable, 0o644); err != nil {
		t.Fatalf("restore staging permissions: %v", err)
	}
	if err := os.Remove(unreadable); err != nil {
		t.Fatalf("clear unreadable: %v", err)
	}

	// The importer still accepts the genuine bundle afterwards, which also
	// proves no hostile artifact recorded progress (a skipped sequence
	// would stay skipped).
	if err := os.WriteFile(filepath.Join(fer.high, genuine[0]), goodRaw, 0o600); err != nil {
		t.Fatalf("restore genuine: %v", err)
	}
	want := map[string]int64{"ann": 1}
	waitHigh(t, low, high, 0, want)
	if !equalContacts(mustContacts(t, low.Node(0)), mustContacts(t, high.Node(0))) {
		t.Fatalf("low/high contact states diverged")
	}
	failed = false
}

// TestHighLowRestartResumeLive checks restart resumption on both sides
// (Rust: restart_resumes_without_recapture_or_reapply).
func TestHighLowRestartResumeLive(t *testing.T) {
	stagingLow := stagingDir("highlow-restart-low")
	stagingHigh := stagingDir("highlow-restart-high")
	low := openExporter(t, "highlow-restart-low", lowDBID, lowNode, stagingLow, lowSignerA, replication.RecipientPublic(&highSecret))
	high := openHigh(t, "highlow-restart-high", highDBID, [][16]byte{highNode0}, []string{stagingHigh},
		highSecret, map[[16]byte]bool{lowDBID: true}, lowSigners(lowSignerA), lowOrigins(low))
	migrate(t, low.Node(0), baseDDL, "low")
	migrate(t, high.Node(0), baseDDL, "high")
	optIn(t, low.Node(0), "low")
	optIn(t, high.Node(0), "high")
	fer := newFerry(t, stagingLow, stagingHigh)

	failed := true
	defer func() {
		if failed {
			low.DumpForensics("highlow-restart-low")
			high.DumpForensics("highlow-restart-high")
		}
		low.Finish(failed)
		high.Finish(failed)
	}()

	insertContact(t, low.Node(0), "ann-id", "ann", 1)
	insertContact(t, low.Node(0), "bob-id", "bob", 2)
	drainLow(t, low)
	fer.ferryAll()
	want := map[string]int64{"ann": 1, "bob": 2}
	waitHigh(t, low, high, 0, want)

	// Restarting the exporter loses no capture resume: nothing
	// re-captures, proven by the Low bundle file set being unchanged
	// across the restart.
	before := bundleFiles(t, fer.low)
	low.StopNode(0)
	if err := low.StartNode(0); err != nil {
		t.Fatalf("low restart: %v", err)
	}
	for i := 0; i < 5; i++ {
		drainLow(t, low)
	}
	if after := bundleFiles(t, fer.low); !equalStrings(after, before) {
		t.Fatalf("low re-exported bundles across restart: %v vs %v", after, before)
	}

	// Restarting the importer loses no inbox/cursor resume: the redelivery
	// is a no-op and state holds both contacts exactly once.
	high.StopNode(0)
	if err := high.StartNode(0); err != nil {
		t.Fatalf("high restart: %v", err)
	}
	for i := 0; i < 10; i++ {
		pumpHigh(t, high, 0)
	}
	if got := mustContacts(t, high.Node(0)); !equalContacts(got, want) {
		t.Fatalf("high state changed across restart: %v", got)
	}
	if !equalContacts(mustContacts(t, low.Node(0)), mustContacts(t, high.Node(0))) {
		t.Fatalf("low/high contact states diverged")
	}
	failed = false
}

// TestHighLowSchemaHoldMigrationLive checks schema-hold survival with
// local-migration release: the unknown column holds the bundles (nothing
// applies, no DDL crosses), the hold survives restart, and a local High
// migration releases the held bundles (Rust:
// schema_hold_survives_restart_and_releases_on_local_migration).
func TestHighLowSchemaHoldMigrationLive(t *testing.T) {
	stagingLow := stagingDir("highlow-schema-low")
	stagingHigh := stagingDir("highlow-schema-high")
	low := openExporter(t, "highlow-schema-low", lowDBID, lowNode, stagingLow, lowSignerA, replication.RecipientPublic(&highSecret))
	high := openHigh(t, "highlow-schema-high", highDBID, [][16]byte{highNode0}, []string{stagingHigh},
		highSecret, map[[16]byte]bool{lowDBID: true}, lowSigners(lowSignerA), lowOrigins(low))
	migrate(t, low.Node(0), emailDDL, "low")
	migrate(t, high.Node(0), baseDDL, "high")
	optIn(t, low.Node(0), "low")
	optIn(t, high.Node(0), "high")
	fer := newFerry(t, stagingLow, stagingHigh)

	failed := true
	defer func() {
		if failed {
			low.DumpForensics("highlow-schema-low")
			high.DumpForensics("highlow-schema-high")
		}
		low.Finish(failed)
		high.Finish(failed)
	}()

	insertContact(t, low.Node(0), "ann-id", "ann", 1)
	setEmail(t, low.Node(0), "ann-id", "a@x")
	drainLow(t, low)
	fer.ferryAll()
	// The unknown column holds the bundles: nothing applies (the bridge
	// traps the hold internally and returns nil, so there is no
	// waiting-schema error to assert).
	for i := 0; i < 10; i++ {
		pumpHigh(t, high, 0)
	}
	if got := mustContacts(t, high.Node(0)); len(got) != 0 {
		t.Fatalf("held bundle applied: %v", got)
	}
	if hasColumn(high.Node(0), table, "email") {
		t.Fatalf("DDL crossed the bridge")
	}
	// The hold survives restart and still applies nothing.
	high.StopNode(0)
	if err := high.StartNode(0); err != nil {
		t.Fatalf("high restart: %v", err)
	}
	for i := 0; i < 10; i++ {
		pumpHigh(t, high, 0)
	}
	if got := mustContacts(t, high.Node(0)); len(got) != 0 {
		t.Fatalf("held bundle applied after restart: %v", got)
	}
	if hasColumn(high.Node(0), table, "email") {
		t.Fatalf("DDL crossed the bridge")
	}
	// The local migration releases the bundles; the held email arrives.
	migrate(t, high.Node(0), addEmail, "high")
	arrived := harness.WaitUntil(wait, poll, func() bool {
		if _, err := nodeBridge(t, low.Node(0)).Capture(); err != nil {
			t.Fatalf("low bridge capture: %v", err)
		}
		if _, err := nodeBridge(t, high.Node(0)).Process(); err != nil {
			t.Fatalf("high bridge process: %v", err)
		}
		email, ok := emailOf(high.Node(0), "ann-id")
		return ok && email == "a@x"
	})
	if !arrived {
		email, _ := emailOf(high.Node(0), "ann-id")
		t.Fatalf("held email never arrived after local migration: %q", email)
	}
	if got := mustContacts(t, high.Node(0)); !equalContacts(got, map[string]int64{"ann": 1}) {
		t.Fatalf("released bundle applied the wrong set: %v", got)
	}
	failed = false
}

// TestHighLowKeyRotationAndOutageResumeLive checks signer rotation plus
// outage catch-up (Rust: signer_rotation_and_outage_catch_up).
func TestHighLowKeyRotationAndOutageResumeLive(t *testing.T) {
	stagingLow := stagingDir("highlow-rotate-low")
	stagingHigh := stagingDir("highlow-rotate-high")
	low := openExporter(t, "highlow-rotate-low", lowDBID, lowNode, stagingLow, lowSignerA, replication.RecipientPublic(&highSecret))
	high := openHigh(t, "highlow-rotate-high", highDBID, [][16]byte{highNode0}, []string{stagingHigh},
		highSecret, map[[16]byte]bool{lowDBID: true}, lowSigners(lowSignerA), lowOrigins(low))
	migrate(t, low.Node(0), baseDDL, "low")
	migrate(t, high.Node(0), baseDDL, "high")
	optIn(t, low.Node(0), "low")
	optIn(t, high.Node(0), "high")
	fer := newFerry(t, stagingLow, stagingHigh)
	lowOrigin := low.NodeOriginPubkey(0)

	failed := true
	defer func() {
		if failed {
			low.DumpForensics("highlow-rotate-low")
			high.DumpForensics("highlow-rotate-high")
		}
		low.Finish(failed)
		high.Finish(failed)
	}()

	drainLow(t, low)
	fer.ferryAll()
	pumpHigh(t, high, 0)

	// Round 1 under the predecessor key, imported before rotation (a single
	// signer per source node is pinned, so overlap trust is
	// inexpressible).
	insertContact(t, low.Node(0), "ann-id", "ann", 1)
	drainLow(t, low)
	round1 := fer.newBundles()
	if len(round1) == 0 {
		t.Fatalf("round 1 exported no bundles")
	}
	fer.ferryNames(round1)
	waitHigh(t, low, high, 0, map[string]int64{"ann": 1})

	// Rotate: the successor signs from here on (config edit plus restart
	// on both sides).
	low.StopNode(0)
	if err := low.SetBridgeConfig(0, replication.NewBridgeExporter(
		lowDBID, fer.low, stream, lowSignerB, replication.RecipientPublic(&highSecret),
	)); err != nil {
		t.Fatalf("low signer rotation: %v", err)
	}
	if err := low.StartNode(0); err != nil {
		t.Fatalf("low restart after rotation: %v", err)
	}
	high.StopNode(0)
	if err := high.SetBridgeConfig(0, replication.NewBridgeReceiver(
		highDBID, fer.high, stream, highSecret,
		map[[16]byte]bool{lowDBID: true},
		lowSigners(lowSignerB),
		map[[16]byte][32]byte{lowNode: lowOrigin},
	)); err != nil {
		t.Fatalf("high signer rotation: %v", err)
	}
	if err := high.StartNode(0); err != nil {
		t.Fatalf("high restart after rotation: %v", err)
	}

	// Round 2 under the successor key imports on top of round 1.
	insertContact(t, low.Node(0), "bob-id", "bob", 2)
	drainLow(t, low)
	round2 := fer.newBundles()
	if len(round2) == 0 {
		t.Fatalf("round 2 exported no bundles")
	}
	fer.ferryNames(round2)
	both := map[string]int64{"ann": 1, "bob": 2}
	waitHigh(t, low, high, 0, both)

	// Retire the predecessor: clear the applied bundles, then prove a
	// duplicate of a current bundle still stages cleanly while a
	// retired-signer replay fails closed without disturbing state.
	clearBundles(t, fer.high)
	clearQuarantine(t, fer.high)
	round2Raw, err := os.ReadFile(filepath.Join(fer.low, round2[0]))
	if err != nil {
		t.Fatalf("read round-2 bundle: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fer.high, round2[0]), round2Raw, 0o600); err != nil {
		t.Fatalf("duplicate round-2 bundle: %v", err)
	}
	pumpHigh(t, high, 0)
	if _, err := os.Stat(filepath.Join(fer.high, round2[0])); err != nil {
		t.Fatalf("duplicate of a current bundle was quarantined")
	}
	if q := quarantineFiles(t, fer.high); len(q) != 0 {
		t.Fatalf("current duplicate quarantined: %v", q)
	}
	if got := mustContacts(t, high.Node(0)); !equalContacts(got, both) {
		t.Fatalf("current duplicate changed state: %v", got)
	}
	if err := os.Remove(filepath.Join(fer.high, round2[0])); err != nil {
		t.Fatalf("clear duplicate: %v", err)
	}
	retired, err := os.ReadFile(filepath.Join(fer.low, round1[0]))
	if err != nil {
		t.Fatalf("read round-1 bundle: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fer.high, "replay.bundle"), retired, 0o600); err != nil {
		t.Fatalf("stage replay: %v", err)
	}
	pumpHigh(t, high, 0)
	if q := quarantineFiles(t, fer.high); !equalStrings(q, []string{"replay.bundle"}) {
		t.Fatalf("retired-signer replay not quarantined: %v", q)
	}
	if got := mustContacts(t, high.Node(0)); !equalContacts(got, both) {
		t.Fatalf("retired-signer replay disturbed state: %v", got)
	}
	if err := os.Remove(filepath.Join(fer.high, "quarantine", "replay.bundle")); err != nil {
		t.Fatalf("clear quarantine: %v", err)
	}

	// Prolonged outage: events queue durably on Low (capture-only; the
	// bundles stay in the Low dir), survive a Low restart, then catch up
	// completely. There is no pending-events counter API, so the exact
	// converged total (backlog == 4) proves all five events arrived
	// exactly once.
	insertContact(t, low.Node(0), "backlog-id", "backlog", 0)
	for i := 0; i < 4; i++ {
		addScore(t, low.Node(0), "backlog-id", 1)
	}
	drainLow(t, low)
	low.StopNode(0)
	if err := low.StartNode(0); err != nil {
		t.Fatalf("low restart during outage: %v", err)
	}
	if got := mustContacts(t, high.Node(0)); hasKey(got, "backlog") {
		t.Fatalf("outage leaked to high: %v", got)
	}
	fer.ferryAll()
	caughtUp := map[string]int64{"ann": 1, "bob": 2, "backlog": 4}
	waitHigh(t, low, high, 0, caughtUp)
	if !equalContacts(mustContacts(t, low.Node(0)), mustContacts(t, high.Node(0))) {
		t.Fatalf("low/high contact states diverged")
	}
	failed = false
}

// TestHighLowOwnershipReorderAcrossPeersLive checks High-owned-field reorder
// convergence across two meshed High peers (Rust:
// high_owned_field_reorder_converges_across_peers): peer A imports bundle 1,
// takes a High override, then imports bundle 2 (override-then-import); peer
// B imports both bundles before the mesh delivers the override
// (import-then-override). Both converge on the High bytes with High-owned
// provenance, and an explicit release on A converges both back to the Low
// value.
func TestHighLowOwnershipReorderAcrossPeersLive(t *testing.T) {
	stagingLow := stagingDir("highlow-own-low")
	stagingHigh := stagingDir("highlow-own-high")
	low := openExporter(t, "highlow-own-low", lowDBID, lowNode, stagingLow, lowSignerA, replication.RecipientPublic(&highSecret))
	high := openHigh(t, "highlow-own-high", highDBID, [][16]byte{highNode0, highNode1}, []string{stagingHigh, stagingHigh},
		highSecret, map[[16]byte]bool{lowDBID: true}, lowSigners(lowSignerA), lowOrigins(low))
	migrate(t, low.Node(0), baseDDL, "low")
	migrate(t, high.Node(0), baseDDL, "high-0")
	migrate(t, high.Node(1), baseDDL, "high-1")
	optIn(t, low.Node(0), "low")
	optIn(t, high.Node(0), "high-0")
	optIn(t, high.Node(1), "high-1")
	fer := newFerry(t, stagingLow, stagingHigh)

	failed := true
	defer func() {
		if failed {
			low.DumpForensics("highlow-own-low")
			high.DumpForensics("highlow-own-high")
		}
		low.Finish(failed)
		high.Finish(failed)
	}()

	drainLow(t, low)
	fer.ferryAll()
	pumpHigh(t, high, 0, 1)

	insertContact(t, low.Node(0), "row-0", "low-1", 1)
	drainLow(t, low)
	fer.ferryAll()
	// Peer A: override-then-import. The wait for bundle 1 is the ordering
	// gate: the override lands strictly before bundle 2 exists, so no
	// background pump can collapse the order.
	waitHigh(t, low, high, 0, map[string]int64{"low-1": 1})
	rename(t, high.Node(0), "row-0", "high")
	if got := mustContacts(t, high.Node(0)); !equalContacts(got, map[string]int64{"high": 1}) {
		t.Fatalf("A override not visible locally: %v", got)
	}
	rename(t, low.Node(0), "row-0", "low-2")
	drainLow(t, low)
	fer.ferryAll()
	waitHigh(t, low, high, 0, map[string]int64{"high": 1})

	// Peer B: import-then-override (the override arrives later over the
	// mesh). B has no owner record yet (no mesh), so both bundles show
	// the Low value.
	waitHigh(t, low, high, 1, map[string]int64{"low-2": 1})

	// Mesh the High peers only after both orders are established locally;
	// the mesh is the only override-delivery path.
	if err := high.AddPeer(0, 1); err != nil {
		t.Fatalf("mesh a->b: %v", err)
	}
	if err := high.AddPeer(1, 0); err != nil {
		t.Fatalf("mesh b->a: %v", err)
	}
	owned := map[string]int64{"high": 1}
	waitMeshed(t, high, []int{0, 1}, owned, "ownership reorder")
	if !equalContacts(mustContacts(t, high.Node(0)), mustContacts(t, high.Node(1))) {
		t.Fatalf("peers diverged after mesh")
	}

	// High-owned provenance (behavioral): drain the bridge to idle so the
	// Low update has provably arrived, then prove it stayed suppressed on
	// both peers.
	rename(t, low.Node(0), "row-0", "low-3")
	drainLow(t, low)
	fer.ferryAll()
	for i := 0; i < 100; i++ {
		l, err := nodeBridge(t, low.Node(0)).Capture()
		if err != nil {
			t.Fatalf("low capture: %v", err)
		}
		a, err := nodeBridge(t, high.Node(0)).Process()
		if err != nil {
			t.Fatalf("high-0 process: %v", err)
		}
		b, err := nodeBridge(t, high.Node(1)).Process()
		if err != nil {
			t.Fatalf("high-1 process: %v", err)
		}
		if l == 0 && a == 0 && b == 0 {
			break
		}
	}
	if got := mustContacts(t, high.Node(0)); !equalContacts(got, owned) {
		t.Fatalf("low-3 leaked through A's ownership: %v", got)
	}
	if got := mustContacts(t, high.Node(1)); !equalContacts(got, owned) {
		t.Fatalf("low-3 leaked through B's ownership: %v", got)
	}

	// Explicit release on A converges both peers back to the Low value.
	releaseName(t, high, "row-0")
	released := map[string]int64{"low-3": 1}
	waitMeshed(t, high, []int{0, 1}, released, "ownership release")
	if !equalContacts(mustContacts(t, high.Node(0)), mustContacts(t, high.Node(1))) {
		t.Fatalf("peers diverged after release")
	}
	failed = false
}

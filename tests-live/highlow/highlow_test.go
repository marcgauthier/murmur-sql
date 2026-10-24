// Live scenarios: real Low (exporter) and High (importer) domains move
// logical writes across a directory drop.
//
// Go port of tests/highlow.rs using tests-live/goharness. Identical
// behavior across seven tests: disconnected transfer with one-way role
// gating, gap delivery with duplicate recovery, two-receiver
// convergence, forgery/corruption/misnaming rejection without partial
// apply, outbox/inbox restart resumption, schema-hold survival across
// restart with release after a local High migration (no DDL crosses),
// signer rotation plus outage catch-up, and High-owned-field reorder
// convergence across two meshed High peers with opposite
// import/override orders.
//
// Low and High are separate `murmur-server` clusters driven over the
// PostgreSQL wire protocol. Plain SQL replaces the original Go
// typed-contention API; `contacts` is the `name -> score` visible-state
// map, with plain LWW `score` increments standing in for the sequential
// single-writer counter legs, which converge identically. The
// replication engine drives capture/process automatically every apply
// tick (~1 s), so the test only polls file and row state.
//
// Directory-drop routing: Low and High use SEPARATE staging dirs and
// the test ferries `*.bundle` files one way via ferry; only what the
// test ferries is visible to High, which keeps
// gap/withhold/forgery/outage legs deterministic. Bundle filenames
// (`<nodehex>-<seq:020>.bundle`) sort in export order, so the
// middle-bundle withhold mirrors the original exactly.
//
// Count/progress mapping: per-call capture/import counts and bridge
// progress have no wire equivalent and are racy under background
// driving, so every such assertion is proven behaviorally instead: gap
// holds by the exact visible row set after a background-settle sleep,
// duplicate redelivery by state stability plus the bundle staying out
// of quarantine, restart resume by Low's bundle file set being
// unchanged and High's rows holding exactly, and outage catch-up by
// the exact converged counter (a lost or doubled event would show a
// different total). The bridge quarantines undecryptable artifacts
// into `staging/quarantine/` and returns `Ok`, so forgery legs assert
// quarantine placement + empty High + no recorded progress (the
// genuine bundle still applies afterwards). High/Low field provenance
// has no public API, so ownership is proven behaviorally: High-owned
// by a Low update arriving yet staying suppressed.
//
// Signer rotation: the product pins a single signer per source node
// (`trusted_signers`), so overlap is inexpressible: round 1 is
// imported BEFORE the rotation (config edit plus restart on both
// sides, via Cluster.SetBridgeConfig), round 2 imports after, and a
// retired-signer replay then fails closed into quarantine while a
// current-key duplicate still stages cleanly.
package highlow

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// stream is the bridge stream (Go: `field-records`).
const stream = "field-records"

// table is the contacts table (Go: `liveContactsSchema`).
const table = "contacts"

const baseDDL = "CREATE TABLE contacts(id TEXT NOT NULL PRIMARY KEY, name TEXT, score INTEGER)"
const emailDDL = "CREATE TABLE contacts(id TEXT NOT NULL PRIMARY KEY, name TEXT, score INTEGER, email TEXT)"
const addEmail = "ALTER TABLE contacts ADD COLUMN email TEXT"

// wait is the convergence deadline (Go: 30 s waits).
const wait = 30 * time.Second

// poll is the convergence poll interval (Go: 100 ms polls; the Rust
// port polls every 50 ms).
const poll = 50 * time.Millisecond

// settleFor is the background-settle sleep for negative legs (engine
// capture/process run every ~1 s apply tick; this covers several
// ticks).
const settleFor = 3 * time.Second

// drainQuiet is the export-quiescence observation: the Low bundle set
// must hold still this long (covers >2 apply ticks) before a drain
// returns.
const drainQuiet = 2500 * time.Millisecond

// drainPoll is the export-quiescence poll interval.
const drainPoll = 250 * time.Millisecond

// Independent bridge domains. Fixed test vectors shared by every test
// in this file (clusters never interact across tests, like the
// `tests/bridge_files.rs` fixed-key convention).
var (
	lowDBID   = bytes16(0xD0)
	highDBID  = bytes16(0xD1)
	rogueDBID = bytes16(0xD2)
	lowNode   = bytes16(0x10)
	highNode0 = bytes16(0x11)
	highNode1 = bytes16(0x12)
	rogueNode = bytes16(0x13)
	// Low bundle-signer seeds (ed25519; High pins the verifying keys).
	lowSignerA  = bytes32(0x21)
	lowSignerB  = bytes32(0x23)
	rogueSigner = bytes32(0x24)
	// Receiver secrets (X25519; Low encrypts to the derived recipients).
	highSecret  = bytes32(0x22)
	otherSecret = bytes32(0x25)
	rogueSecret = bytes32(0x26)
)

func bytes16(b byte) [16]byte {
	var out [16]byte
	for i := range out {
		out[i] = b
	}
	return out
}

func bytes32(b byte) [32]byte {
	var out [32]byte
	for i := range out {
		out[i] = b
	}
	return out
}

// insertContact is a contact insert (Go: `writeContact`).
func insertContact(cluster *goharness.Cluster, idx int, id, name string, score int64) error {
	changed, err := cluster.Node(idx).Execute(
		"INSERT INTO contacts(id, name, score) VALUES($1, $2, $3)", id, name, score)
	if err != nil {
		return fmt.Errorf("contact insert failed: %w", err)
	}
	if changed != 1 {
		return fmt.Errorf("contact insert hit %d rows", changed)
	}
	if _, err := cluster.Node(idx).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("flush after contact insert: %w", err)
	}
	return nil
}

// rename is a contact rename (Go: `Cluster.TypedRename`).
func rename(cluster *goharness.Cluster, idx int, id, name string) error {
	changed, err := cluster.Node(idx).Execute(
		"UPDATE contacts SET name = $1 WHERE id = $2", name, id)
	if err != nil {
		return fmt.Errorf("contact rename failed: %w", err)
	}
	if changed != 1 {
		return fmt.Errorf("rename hit %d rows", changed)
	}
	if _, err := cluster.Node(idx).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("flush after contact rename: %w", err)
	}
	return nil
}

// setEmail is a known-field email update (Go: `Cluster.TypedSetNote`).
func setEmail(cluster *goharness.Cluster, idx int, id, email string) error {
	changed, err := cluster.Node(idx).Execute(
		"UPDATE contacts SET email = $1 WHERE id = $2", email, id)
	if err != nil {
		return fmt.Errorf("contact email update failed: %w", err)
	}
	if changed != 1 {
		return fmt.Errorf("email update hit %d rows", changed)
	}
	if _, err := cluster.Node(idx).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("flush after email update: %w", err)
	}
	return nil
}

// addScore is a sequential score increment (Go:
// `Cluster.TypedCounterAdd`; single-writer legs converge identically
// under LWW).
//
// The delta is inlined as a literal (injection-safe: formatted `i64`):
// the server reports expression-position parameters as type `Unknown`
// and the driver refuses to serialize non-string types for them, so
// only string params may sit in positions the server cannot infer from
// a column.
func addScore(cluster *goharness.Cluster, idx int, id string, delta int64) error {
	changed, err := cluster.Node(idx).Execute(
		fmt.Sprintf("UPDATE contacts SET score = score + %d WHERE id = $1", delta), id)
	if err != nil {
		return fmt.Errorf("score update failed: %w", err)
	}
	if changed != 1 {
		return fmt.Errorf("score update hit %d rows", changed)
	}
	if _, err := cluster.Node(idx).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("flush after score update: %w", err)
	}
	return nil
}

// contacts reads the visible contacts as `name -> score` (Go:
// `contacts()`).
func contacts(cluster *goharness.Cluster, idx int) (map[string]int64, error) {
	rows, err := cluster.Node(idx).Query("SELECT name, score FROM contacts")
	if err != nil {
		return nil, fmt.Errorf("contacts read failed: %w", err)
	}
	out := map[string]int64{}
	for _, r := range rows {
		name, err := r.String(0)
		if err != nil {
			return nil, fmt.Errorf("decode contact name: %w", err)
		}
		score, err := r.Int64(1)
		if err != nil {
			return nil, fmt.Errorf("decode contact score: %w", err)
		}
		out[name] = score
	}
	return out, nil
}

func equalContacts(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// emailOf reads the email field (Go: `Cluster.TypedNote`); it reports
// false when the column is absent (pre-migration High) or the value is
// NULL.
func emailOf(cluster *goharness.Cluster, idx int, id string) (string, bool) {
	rows, err := cluster.Node(idx).Query("SELECT email FROM contacts WHERE id = $1", id)
	if err != nil || len(rows) == 0 {
		return "", false
	}
	return rows[0].OptString(0)
}

// hasColumn reports whether `table` currently has `column` (probed via
// a zero-row select; PRAGMA has no guaranteed wire form).
func hasColumn(cluster *goharness.Cluster, idx int, table, column string) bool {
	_, err := cluster.Node(idx).Query(fmt.Sprintf("SELECT %s FROM %s LIMIT 0", column, table))
	return err == nil
}

// migrate runs the fixture DDL on one node plus the replicated flag
// (row convergence and bridge capture need the per-table flag on each
// node).
func migrate(cluster *goharness.Cluster, idx int, ddl, label string) error {
	if err := cluster.EnsureMigrated(idx, ddl); err != nil {
		return fmt.Errorf("%s migrate failed: %w", label, err)
	}
	if _, err := cluster.Node(idx).Simple(
		fmt.Sprintf("MURMUR SET TABLE %s REPLICATED true", table)); err != nil {
		return fmt.Errorf("%s SET TABLE REPLICATED: %w", label, err)
	}
	if _, err := cluster.Node(idx).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("%s flush after migrate: %w", label, err)
	}
	return nil
}

// drainLow waits until Low's export quiesces: the bundle-file set must
// hold still for drainQuiet (the background capture drives every ~1 s
// apply tick). Sound: capture cursors are durable and only the test
// writes, so a still set means quiescent.
func drainLow(f *ferry) error {
	deadline := time.Now().Add(wait)
	last, err := bundleFiles(f.low)
	if err != nil {
		return err
	}
	stillSince := time.Now()
	for {
		time.Sleep(drainPoll)
		now, err := bundleFiles(f.low)
		if err != nil {
			return err
		}
		if equalStrings(now, last) {
			if time.Since(stillSince) >= drainQuiet {
				return nil
			}
		} else {
			last = now
			stillSince = time.Now()
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("low bridge did not become idle")
		}
	}
}

// bundleFiles lists the sorted `*.bundle` names directly under `dir`
// (Go: `stagingNames`).
func bundleFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", dir, err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".bundle") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

// clearBundles removes every `*.bundle` file directly under `dir`.
func clearBundles(dir string) error {
	names, err := bundleFiles(dir)
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			return fmt.Errorf("clear %s: %w", filepath.Join(dir, name), err)
		}
	}
	return nil
}

// quarantineFiles lists the sorted names under `staging/quarantine/`
// (empty when no dir yet).
func quarantineFiles(staging string) []string {
	dir := filepath.Join(staging, "quarantine")
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

// clearQuarantine removes every file under `staging/quarantine/`.
func clearQuarantine(staging string) error {
	for _, name := range quarantineFiles(staging) {
		if err := os.Remove(filepath.Join(staging, "quarantine", name)); err != nil {
			return fmt.Errorf("clear quarantine file: %w", err)
		}
	}
	return nil
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

func copyFile(src, dst string) error {
	raw, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, raw, 0o644)
}

// ferry is the one-way directory drop: Low exports into low, the test
// ferries `*.bundle` files into high. Ferries are tracked so ferryAll
// only moves new bundles; targeted withholds use ferryNames.
type ferry struct {
	low  string
	high string
	done map[string]bool
}

func newFerry(low, high string) *ferry {
	return &ferry{low: low, high: high, done: map[string]bool{}}
}

// newBundles lists bundles exported since the last ferry, in export order.
func (f *ferry) newBundles() ([]string, error) {
	names, err := bundleFiles(f.low)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, n := range names {
		if !f.done[n] {
			out = append(out, n)
		}
	}
	return out, nil
}

// ferryAll ferries every new bundle to High.
func (f *ferry) ferryAll() error {
	names, err := f.newBundles()
	if err != nil {
		return err
	}
	return f.ferryNames(names)
}

// ferryNames ferries exactly `names` (already-exported bundles) to High.
func (f *ferry) ferryNames(names []string) error {
	for _, name := range names {
		if err := copyFile(filepath.Join(f.low, name), filepath.Join(f.high, name)); err != nil {
			return fmt.Errorf("ferry %s: %w", name, err)
		}
		f.done[name] = true
	}
	return nil
}

// openExporter starts a single-node Low exporter (Go: `newLowCluster`).
func openExporter(name string, dbid, node [16]byte, staging string, signer, recipient [32]byte) (*goharness.Cluster, error) {
	signerHex := goharness.Hex(signer[:])
	opts := goharness.DefaultClusterOptions(name)
	opts.NumNodes = 1
	opts.DatabaseID = &dbid
	opts.NodeIDs = [][16]byte{node}
	opts.ManualPeers = true
	opts.Bridge = goharness.BridgeOptions{Nodes: map[int]goharness.BridgeNodeConfig{
		0: {
			Role:      "low-exporter",
			Domain:    goharness.Hex(dbid[:]),
			Directory: filepath.ToSlash(staging),
			Stream:    stream,
			Recipient: goharness.Hex(recipient[:]),
			SignerHex: &signerHex,
		},
	}}
	return goharness.NewCluster(opts)
}

// openHigh starts a High importer cluster (Go: `newHighCluster`); one
// staging dir per node. Always manual peers: the test meshes explicitly
// after migrating.
func openHigh(name string, dbid [16]byte, nodes [][16]byte, stagings []string, secret [32]byte, domains [][16]byte, signers, origins map[[16]byte][32]byte) (*goharness.Cluster, error) {
	if len(nodes) != len(stagings) {
		return nil, fmt.Errorf("nodes/stagings length mismatch")
	}
	recipient := replication.RecipientPublic(&secret)
	secretHex := goharness.Hex(secret[:])
	trustedDomains := make([]string, 0, len(domains))
	for _, id := range domains {
		trustedDomains = append(trustedDomains, goharness.Hex(id[:]))
	}
	trustedOrigins := make(map[string]string, len(origins))
	for n, k := range origins {
		trustedOrigins[goharness.Hex(n[:])] = goharness.Hex(k[:])
	}
	trustedSigners := make(map[string]string, len(signers))
	for n, k := range signers {
		trustedSigners[goharness.Hex(n[:])] = goharness.Hex(k[:])
	}
	receivers := make(map[int]goharness.BridgeNodeConfig, len(nodes))
	for i, staging := range stagings {
		receivers[i] = goharness.BridgeNodeConfig{
			Role:              "high-receiver",
			Domain:            goharness.Hex(dbid[:]),
			TrustedDomains:    trustedDomains,
			TrustedOrigins:    trustedOrigins,
			Directory:         filepath.ToSlash(staging),
			Stream:            stream,
			Recipient:         goharness.Hex(recipient[:]),
			ReceiverSecretHex: &secretHex,
			TrustedSigners:    trustedSigners,
		}
	}
	opts := goharness.DefaultClusterOptions(name)
	opts.NumNodes = len(nodes)
	opts.DatabaseID = &dbid
	opts.NodeIDs = nodes
	opts.ManualPeers = true
	opts.Bridge = goharness.BridgeOptions{Nodes: receivers}
	return goharness.NewCluster(opts)
}

// lowSigners pins Low's bundle signer (the seed itself stays inside the
// harness).
func lowSigners(signer *[32]byte) map[[16]byte][32]byte {
	return map[[16]byte][32]byte{lowNode: replication.PublicKey(signer)}
}

func lowOrigins(low *goharness.Cluster) map[[16]byte][32]byte {
	return map[[16]byte][32]byte{lowNode: low.NodeOriginPubkey(0)}
}

// waitHigh waits until High node `idx` shows exactly `want` (Go:
// contact waits plus `assertContactsEqual`). The engine's background
// capture/process drive the transfer; the ferry stays explicit
// (withhold legs must not auto-ferry).
func waitHigh(high *goharness.Cluster, idx int, want map[string]int64) error {
	deadline := time.Now().Add(wait)
	for {
		got, err := contacts(high, idx)
		if err != nil {
			return err
		}
		if equalContacts(got, want) {
			return nil
		}
		if !time.Now().Before(deadline) {
			got, err := contacts(high, idx)
			if err != nil {
				return err
			}
			return fmt.Errorf("high node%d contacts = %v, want %v", idx, got, want)
		}
		time.Sleep(poll)
	}
}

// waitMeshed waits until every listed High node shows exactly `want`
// over the mesh (no bridge traffic remains; plain convergence polling).
func waitMeshed(high *goharness.Cluster, idxs []int, want map[string]int64, what string) error {
	deadline := time.Now().Add(wait)
	for {
		all := true
		for _, i := range idxs {
			got, err := contacts(high, i)
			if err != nil {
				return err
			}
			if !equalContacts(got, want) {
				all = false
				break
			}
		}
		if all {
			return nil
		}
		if !time.Now().Before(deadline) {
			var states []map[string]int64
			for _, i := range idxs {
				got, err := contacts(high, i)
				if err != nil {
					return err
				}
				states = append(states, got)
			}
			return fmt.Errorf("%s: high nodes did not converge on %v: %v", what, want, states)
		}
		time.Sleep(poll)
	}
}

// settle sleeps for negative legs: background capture/process run every
// ~1 s apply tick, so a still-unchanged state after the settle proves
// the hold without explicit pumps.
func settle() {
	time.Sleep(settleFor)
}

// finishFails dumps forensics and finishes every cluster after a body
// error (Rust: dump_forensics + finish + assert).
func finishFails(t *testing.T, err error, pairs ...any) {
	t.Helper()
	fmt.Fprintf(os.Stderr, "scenario failed: %v\n", err)
	for i := 0; i+1 < len(pairs); i += 2 {
		cluster, _ := pairs[i].(*goharness.Cluster)
		reason, _ := pairs[i+1].(string)
		if cluster != nil {
			cluster.DumpForensics(reason)
		}
	}
	for i := 0; i+1 < len(pairs); i += 2 {
		if cluster, _ := pairs[i].(*goharness.Cluster); cluster != nil {
			cluster.Finish(true)
		}
	}
	t.Fatalf("scenario failed; artifacts retained under tests-live/failures/: %v", err)
}

// finishPass finishes every cluster after success.
func finishPass(clusters ...*goharness.Cluster) {
	for _, c := range clusters {
		c.Finish(false)
	}
}

// TestDisconnectedTransferWithOneWayRoleGating is the disconnected
// transfer with one-way role gating (Go:
// `TestHighLowDisconnectedTransferLive`).
func TestDisconnectedTransferWithOneWayRoleGating(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	stagingLow := filepath.Join(goharness.RuntimeDir("highlow-transfer-low"), "staging")
	stagingHigh := filepath.Join(goharness.RuntimeDir("highlow-transfer-high"), "staging")
	recipient := replication.RecipientPublic(&highSecret)
	low, err := openExporter("highlow-transfer-low", lowDBID, lowNode, stagingLow, lowSignerA, recipient)
	if err != nil {
		t.Fatalf("low cluster setup: %v", err)
	}
	high, err := openHigh("highlow-transfer-high", highDBID, [][16]byte{highNode0},
		[]string{stagingHigh}, highSecret, [][16]byte{lowDBID}, lowSigners(&lowSignerA), lowOrigins(low))
	if err != nil {
		low.Finish(true)
		t.Fatalf("high cluster setup: %v", err)
	}
	if err := migrate(low, 0, baseDDL, "low"); err != nil {
		finishFails(t, err, low, "highlow-transfer-low", high, "highlow-transfer-high")
	}
	if err := migrate(high, 0, baseDDL, "high"); err != nil {
		finishFails(t, err, low, "highlow-transfer-low", high, "highlow-transfer-high")
	}
	f := newFerry(stagingLow, stagingHigh)

	err = func() error {
		if low.DatabaseID() == high.DatabaseID() {
			return fmt.Errorf("Low and High share a DBID; domains must differ")
		}
		// One-way role gating is structural (no wire equivalent for the
		// old API-boundary asserts): nothing is ever ferried back, so
		// High state can only change via ferried Low bundles, and Low
		// never sees High bytes at all.
		if err := insertContact(low, 0, "ann-id", "ann", 1); err != nil {
			return err
		}
		if err := drainLow(f); err != nil {
			return err
		}
		names, err := f.newBundles()
		if err != nil {
			return err
		}
		if len(names) == 0 {
			return fmt.Errorf("low exported no bundles for the contact")
		}
		if err := f.ferryAll(); err != nil {
			return err
		}
		want := map[string]int64{"ann": 1}
		if err := waitHigh(high, 0, want); err != nil {
			return err
		}
		lowContacts, err := contacts(low, 0)
		if err != nil {
			return err
		}
		highContacts, err := contacts(high, 0)
		if err != nil {
			return err
		}
		if !equalContacts(lowContacts, highContacts) {
			return fmt.Errorf("low/high contact states diverged")
		}
		return nil
	}()
	if err != nil {
		finishFails(t, err, low, "highlow-transfer-low", high, "highlow-transfer-high")
	}
	finishPass(low, high)
}

// TestGapDeliveryWithDuplicateRecovery is gap delivery with duplicate
// recovery (Go: `TestHighLowDuplicateReorderRecoveryLive`).
func TestGapDeliveryWithDuplicateRecovery(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	stagingLow := filepath.Join(goharness.RuntimeDir("highlow-gap-low"), "staging")
	stagingHigh := filepath.Join(goharness.RuntimeDir("highlow-gap-high"), "staging")
	recipient := replication.RecipientPublic(&highSecret)
	low, err := openExporter("highlow-gap-low", lowDBID, lowNode, stagingLow, lowSignerA, recipient)
	if err != nil {
		t.Fatalf("low cluster setup: %v", err)
	}
	high, err := openHigh("highlow-gap-high", highDBID, [][16]byte{highNode0},
		[]string{stagingHigh}, highSecret, [][16]byte{lowDBID}, lowSigners(&lowSignerA), lowOrigins(low))
	if err != nil {
		low.Finish(true)
		t.Fatalf("high cluster setup: %v", err)
	}
	if err := migrate(low, 0, baseDDL, "low"); err != nil {
		finishFails(t, err, low, "highlow-gap-low", high, "highlow-gap-high")
	}
	if err := migrate(high, 0, baseDDL, "high"); err != nil {
		finishFails(t, err, low, "highlow-gap-low", high, "highlow-gap-high")
	}
	f := newFerry(stagingLow, stagingHigh)

	err = func() error {
		// Baseline: drain the migration bundles so the three contact
		// rounds below are exactly three new bundles.
		if err := drainLow(f); err != nil {
			return err
		}
		if err := f.ferryAll(); err != nil {
			return err
		}
		settle()
		got, err := contacts(high, 0)
		if err != nil {
			return err
		}
		if len(got) != 0 {
			return fmt.Errorf("high imported baseline bundles as contacts")
		}

		if err := insertContact(low, 0, "ann-id", "ann", 0); err != nil {
			return err
		}
		if err := drainLow(f); err != nil {
			return err
		}
		if err := insertContact(low, 0, "bob-id", "bob", 1); err != nil {
			return err
		}
		if err := drainLow(f); err != nil {
			return err
		}
		if err := insertContact(low, 0, "cid-id", "cid", 2); err != nil {
			return err
		}
		if err := drainLow(f); err != nil {
			return err
		}
		names, err := f.newBundles()
		if err != nil {
			return err
		}
		if len(names) != 3 {
			return fmt.Errorf("expected 3 contact bundles, staged %v", names)
		}

		// Withhold the middle bundle (Go: rename aside): the importer
		// applies the first and records a gap instead of skipping ahead.
		// The gap bundle can never apply (sequence check), so a settle
		// sleep proves the hold.
		if err := f.ferryNames([]string{names[0], names[2]}); err != nil {
			return err
		}
		settle()
		got, err = contacts(high, 0)
		if err != nil {
			return err
		}
		if !equalContacts(got, map[string]int64{"ann": 0}) {
			return fmt.Errorf("gap leg applied the wrong set: %v", got)
		}

		// Restoring the bundle fills the gap; all three converge.
		if err := f.ferryNames([]string{names[1]}); err != nil {
			return err
		}
		want := map[string]int64{"ann": 0, "bob": 1, "cid": 2}
		if err := waitHigh(high, 0, want); err != nil {
			return err
		}

		// Duplicate redelivery is a no-op (Go: imported == 0): re-copy an
		// applied bundle and prove the visible state is unchanged and the
		// bundle is not quarantined.
		if err := copyFile(filepath.Join(f.low, names[0]), filepath.Join(f.high, names[0])); err != nil {
			return fmt.Errorf("duplicate redelivery: %w", err)
		}
		settle()
		got, err = contacts(high, 0)
		if err != nil {
			return err
		}
		if !equalContacts(got, want) {
			return fmt.Errorf("duplicate redelivery changed state")
		}
		if _, err := os.Stat(filepath.Join(f.high, names[0])); err != nil {
			return fmt.Errorf("duplicate of an applied bundle was quarantined")
		}
		if q := quarantineFiles(f.high); len(q) != 0 {
			return fmt.Errorf("duplicate redelivery quarantined: %v", q)
		}
		lowContacts, err := contacts(low, 0)
		if err != nil {
			return err
		}
		highContacts, err := contacts(high, 0)
		if err != nil {
			return err
		}
		if !equalContacts(lowContacts, highContacts) {
			return fmt.Errorf("low/high contact states diverged")
		}
		return nil
	}()
	if err != nil {
		finishFails(t, err, low, "highlow-gap-low", high, "highlow-gap-high")
	}
	finishPass(low, high)
}

// TestTwoReceiversConverge has two receivers converge (Go:
// `TestHighLowTwoReceiversConvergeLive`): each receiver imports the same
// staging independently; the High mesh then converges both to identical
// state.
func TestTwoReceiversConverge(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	stagingLow := filepath.Join(goharness.RuntimeDir("highlow-2r-low"), "staging")
	stagingHigh := filepath.Join(goharness.RuntimeDir("highlow-2r-high"), "staging")
	recipient := replication.RecipientPublic(&highSecret)
	low, err := openExporter("highlow-2r-low", lowDBID, lowNode, stagingLow, lowSignerA, recipient)
	if err != nil {
		t.Fatalf("low cluster setup: %v", err)
	}
	high, err := openHigh("highlow-2r-high", highDBID, [][16]byte{highNode0, highNode1},
		[]string{stagingHigh, stagingHigh}, highSecret, [][16]byte{lowDBID},
		lowSigners(&lowSignerA), lowOrigins(low))
	if err != nil {
		low.Finish(true)
		t.Fatalf("high cluster setup: %v", err)
	}
	if err := migrate(low, 0, baseDDL, "low"); err != nil {
		finishFails(t, err, low, "highlow-2r-low", high, "highlow-2r-high")
	}
	if err := migrate(high, 0, baseDDL, "high-0"); err != nil {
		finishFails(t, err, low, "highlow-2r-low", high, "highlow-2r-high")
	}
	if err := migrate(high, 1, baseDDL, "high-1"); err != nil {
		finishFails(t, err, low, "highlow-2r-low", high, "highlow-2r-high")
	}
	if err := high.Mesh(); err != nil {
		finishFails(t, fmt.Errorf("high mesh: %w", err), low, "highlow-2r-low", high, "highlow-2r-high")
	}
	f := newFerry(stagingLow, stagingHigh)

	err = func() error {
		if err := insertContact(low, 0, "ann-id", "ann", 1); err != nil {
			return err
		}
		if err := drainLow(f); err != nil {
			return err
		}
		if err := f.ferryAll(); err != nil {
			return err
		}
		want := map[string]int64{"ann": 1}
		if err := waitHigh(high, 0, want); err != nil {
			return err
		}
		if err := waitHigh(high, 1, want); err != nil {
			return err
		}
		c0, err := contacts(high, 0)
		if err != nil {
			return err
		}
		c1, err := contacts(high, 1)
		if err != nil {
			return err
		}
		if !equalContacts(c0, c1) {
			return fmt.Errorf("receivers diverged")
		}
		lowContacts, err := contacts(low, 0)
		if err != nil {
			return err
		}
		if !equalContacts(lowContacts, c0) {
			return fmt.Errorf("low/high contact states diverged")
		}
		return nil
	}()
	if err != nil {
		finishFails(t, err, low, "highlow-2r-low", high, "highlow-2r-high")
	}
	finishPass(low, high)
}

// TestForgeriesRejectedWithoutPartialApply rejects forgeries without
// partial apply or recorded progress (Go:
// `TestHighLowForgeriesRejectedLive`).
func TestForgeriesRejectedWithoutPartialApply(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	stagingLow := filepath.Join(goharness.RuntimeDir("highlow-forge-low"), "staging")
	stagingHigh := filepath.Join(goharness.RuntimeDir("highlow-forge-high"), "staging")
	stagingRogue := filepath.Join(goharness.RuntimeDir("highlow-forge-rogue"), "staging")
	stagingOther := filepath.Join(goharness.RuntimeDir("highlow-forge-other"), "staging")
	recipient := replication.RecipientPublic(&highSecret)
	low, err := openExporter("highlow-forge-low", lowDBID, lowNode, stagingLow, lowSignerA, recipient)
	if err != nil {
		t.Fatalf("low cluster setup: %v", err)
	}
	high, err := openHigh("highlow-forge-high", highDBID, [][16]byte{highNode0},
		[]string{stagingHigh}, highSecret, [][16]byte{lowDBID}, lowSigners(&lowSignerA), lowOrigins(low))
	if err != nil {
		low.Finish(true)
		t.Fatalf("high cluster setup: %v", err)
	}
	// Rogue domain: its own signer and recipient, exporting one bundle.
	rogueRecipient := replication.RecipientPublic(&rogueSecret)
	rogue, err := openExporter("highlow-forge-rogue", rogueDBID, rogueNode, stagingRogue, rogueSigner, rogueRecipient)
	if err != nil {
		low.Finish(true)
		high.Finish(true)
		t.Fatalf("rogue cluster setup: %v", err)
	}
	// Wrong-recipient domain: main signer and identity, unknown recipient,
	// so the bundle reaches the recipient check (a distinct domain or node
	// would fail earlier at the domain/signer checks).
	otherRecipient := replication.RecipientPublic(&otherSecret)
	other, err := openExporter("highlow-forge-other", lowDBID, lowNode, stagingOther, lowSignerA, otherRecipient)
	if err != nil {
		low.Finish(true)
		high.Finish(true)
		rogue.Finish(true)
		t.Fatalf("other cluster setup: %v", err)
	}
	forgePairs := func() []any {
		return []any{low, "highlow-forge-low", high, "highlow-forge-high",
			rogue, "highlow-forge-rogue", other, "highlow-forge-other"}
	}
	if err := migrate(low, 0, baseDDL, "low"); err != nil {
		finishFails(t, err, forgePairs()...)
	}
	if err := migrate(high, 0, baseDDL, "high"); err != nil {
		finishFails(t, err, forgePairs()...)
	}
	if err := migrate(rogue, 0, baseDDL, "rogue"); err != nil {
		finishFails(t, err, forgePairs()...)
	}
	if err := migrate(other, 0, baseDDL, "other"); err != nil {
		finishFails(t, err, forgePairs()...)
	}
	f := newFerry(stagingLow, stagingHigh)
	rogueFerry := newFerry(stagingRogue, f.high)
	otherFerry := newFerry(stagingOther, f.high)

	err = func() error {
		// Baseline the migration bundles, then stage exactly one genuine
		// contact bundle and clear it: each hostile artifact below is
		// presented alone.
		if err := drainLow(f); err != nil {
			return err
		}
		if err := f.ferryAll(); err != nil {
			return err
		}
		settle()
		if err := clearBundles(f.high); err != nil {
			return err
		}
		if err := insertContact(low, 0, "ann-id", "ann", 1); err != nil {
			return err
		}
		if err := drainLow(f); err != nil {
			return err
		}
		if err := f.ferryAll(); err != nil {
			return err
		}
		genuine, err := bundleFiles(f.high)
		if err != nil {
			return err
		}
		if len(genuine) != 1 {
			return fmt.Errorf("want 1 genuine bundle, staged %v", genuine)
		}
		goodRaw, err := os.ReadFile(filepath.Join(f.high, genuine[0]))
		if err != nil {
			return fmt.Errorf("read genuine bundle: %w", err)
		}
		if err := clearBundles(f.high); err != nil {
			return err
		}

		if err := drainLow(rogueFerry); err != nil {
			return err
		}
		if err := insertContact(rogue, 0, "mallory-id", "mallory", 9); err != nil {
			return err
		}
		if err := drainLow(rogueFerry); err != nil {
			return err
		}
		rogueFiles, err := bundleFiles(rogueFerry.low)
		if err != nil {
			return err
		}
		if len(rogueFiles) == 0 {
			return fmt.Errorf("no rogue bundle")
		}
		rogueRaw, err := os.ReadFile(filepath.Join(rogueFerry.low, rogueFiles[len(rogueFiles)-1]))
		if err != nil {
			return fmt.Errorf("read rogue bundle: %w", err)
		}
		if err := drainLow(otherFerry); err != nil {
			return err
		}
		if err := insertContact(other, 0, "carol-id", "carol", 3); err != nil {
			return err
		}
		if err := drainLow(otherFerry); err != nil {
			return err
		}
		otherFiles, err := bundleFiles(otherFerry.low)
		if err != nil {
			return err
		}
		if len(otherFiles) == 0 {
			return fmt.Errorf("no other bundle")
		}
		otherRaw, err := os.ReadFile(filepath.Join(otherFerry.low, otherFiles[len(otherFiles)-1]))
		if err != nil {
			return fmt.Errorf("read wrong-recipient bundle: %w", err)
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
			if err := os.WriteFile(filepath.Join(f.high, h.name), h.raw, 0o644); err != nil {
				return fmt.Errorf("stage hostile artifact: %w", err)
			}
			// Fail-closed quarantine (deviation: the original import
			// returns an error; here quarantine happens in the
			// background importer and returns Ok).
			settle()
			got, err := contacts(high, 0)
			if err != nil {
				return err
			}
			if len(got) != 0 {
				return fmt.Errorf("hostile artifact %s partially applied: %v", h.name, got)
			}
			if q := quarantineFiles(f.high); !equalStrings(q, []string{h.name}) {
				return fmt.Errorf("hostile artifact %s quarantine: %v", h.name, q)
			}
			if err := os.Remove(filepath.Join(f.high, "quarantine", h.name)); err != nil {
				return fmt.Errorf("clear quarantine: %w", err)
			}
		}

		// Wrong extensions are not bridge artifacts at all: ignored in
		// place, never quarantined, never applied.
		for _, name := range []string{"wrongname.txt", ".hidden"} {
			if err := os.WriteFile(filepath.Join(f.high, name), goodRaw, 0o644); err != nil {
				return fmt.Errorf("stage misnamed artifact: %w", err)
			}
			settle()
			got, err := contacts(high, 0)
			if err != nil {
				return err
			}
			if len(got) != 0 {
				return fmt.Errorf("misnamed artifact %s applied: %v", name, got)
			}
			if q := quarantineFiles(f.high); len(q) != 0 {
				return fmt.Errorf("misnamed artifact %s quarantined", name)
			}
			if _, err := os.Stat(filepath.Join(f.high, name)); err != nil {
				return fmt.Errorf("ignored artifact %s was moved", name)
			}
			if err := os.Remove(filepath.Join(f.high, name)); err != nil {
				return fmt.Errorf("clear misnamed: %w", err)
			}
		}

		// No unreadable-staging-file leg: the background importer swallows
		// the error with no wire-visible signal (the old in-process port
		// asserted the direct `process()` Err).

		// The importer still accepts the genuine bundle afterwards, which
		// also proves no hostile artifact recorded progress (a skipped
		// sequence would stay skipped).
		if err := os.WriteFile(filepath.Join(f.high, genuine[0]), goodRaw, 0o644); err != nil {
			return fmt.Errorf("restore genuine: %w", err)
		}
		want := map[string]int64{"ann": 1}
		if err := waitHigh(high, 0, want); err != nil {
			return err
		}
		lowContacts, err := contacts(low, 0)
		if err != nil {
			return err
		}
		highContacts, err := contacts(high, 0)
		if err != nil {
			return err
		}
		if !equalContacts(lowContacts, highContacts) {
			return fmt.Errorf("low/high contact states diverged")
		}
		return nil
	}()
	if err != nil {
		finishFails(t, err, forgePairs()...)
	}
	finishPass(low, high, rogue, other)
}

// TestRestartResumesWithoutRecaptureOrReapply is restart resumption on
// both sides (Go: `TestHighLowRestartResumeLive`).
func TestRestartResumesWithoutRecaptureOrReapply(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	stagingLow := filepath.Join(goharness.RuntimeDir("highlow-restart-low"), "staging")
	stagingHigh := filepath.Join(goharness.RuntimeDir("highlow-restart-high"), "staging")
	recipient := replication.RecipientPublic(&highSecret)
	low, err := openExporter("highlow-restart-low", lowDBID, lowNode, stagingLow, lowSignerA, recipient)
	if err != nil {
		t.Fatalf("low cluster setup: %v", err)
	}
	high, err := openHigh("highlow-restart-high", highDBID, [][16]byte{highNode0},
		[]string{stagingHigh}, highSecret, [][16]byte{lowDBID}, lowSigners(&lowSignerA), lowOrigins(low))
	if err != nil {
		low.Finish(true)
		t.Fatalf("high cluster setup: %v", err)
	}
	if err := migrate(low, 0, baseDDL, "low"); err != nil {
		finishFails(t, err, low, "highlow-restart-low", high, "highlow-restart-high")
	}
	if err := migrate(high, 0, baseDDL, "high"); err != nil {
		finishFails(t, err, low, "highlow-restart-low", high, "highlow-restart-high")
	}
	f := newFerry(stagingLow, stagingHigh)

	if err := runRestartBody(low, high, f); err != nil {
		finishFails(t, err, low, "highlow-restart-low", high, "highlow-restart-high")
	}
	finishPass(low, high)
}

func runRestartBody(low, high *goharness.Cluster, f *ferry) error {
	if err := insertContact(low, 0, "ann-id", "ann", 1); err != nil {
		return err
	}
	if err := insertContact(low, 0, "bob-id", "bob", 2); err != nil {
		return err
	}
	if err := drainLow(f); err != nil {
		return err
	}
	if err := f.ferryAll(); err != nil {
		return err
	}
	want := map[string]int64{"ann": 1, "bob": 2}
	if err := waitHigh(high, 0, want); err != nil {
		return err
	}

	// Restarting the exporter loses no capture resume: nothing
	// re-captures (Go: captured == 0), proven by the Low bundle file
	// set being unchanged across the restart.
	before, err := bundleFiles(f.low)
	if err != nil {
		return err
	}
	low.StopNode(0)
	if err := low.StartNode(0); err != nil {
		return fmt.Errorf("low restart: %w", err)
	}
	if err := drainLow(f); err != nil {
		return err
	}
	after, err := bundleFiles(f.low)
	if err != nil {
		return err
	}
	if !equalStrings(after, before) {
		return fmt.Errorf("low re-exported bundles across restart")
	}

	// Restarting the importer loses no inbox/cursor resume: the
	// redelivery is a no-op (Go: received == imported == 0) and state
	// holds both contacts exactly once.
	high.StopNode(0)
	if err := high.StartNode(0); err != nil {
		return fmt.Errorf("high restart: %w", err)
	}
	settle()
	got, err := contacts(high, 0)
	if err != nil {
		return err
	}
	if !equalContacts(got, want) {
		return fmt.Errorf("high state changed across restart")
	}
	lowContacts, err := contacts(low, 0)
	if err != nil {
		return err
	}
	highContacts, err := contacts(high, 0)
	if err != nil {
		return err
	}
	if !equalContacts(lowContacts, highContacts) {
		return fmt.Errorf("low/high contact states diverged")
	}
	return nil
}

// TestSchemaHoldSurvivesRestartAndReleasesOnLocalMigration is
// schema-hold survival with local-migration release (Go:
// `TestHighLowSchemaHoldMigrationLive`): the unknown column holds the
// bundles (nothing applies, no DDL crosses), the hold survives restart,
// and a local High migration releases the held bundles.
func TestSchemaHoldSurvivesRestartAndReleasesOnLocalMigration(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	stagingLow := filepath.Join(goharness.RuntimeDir("highlow-schema-low"), "staging")
	stagingHigh := filepath.Join(goharness.RuntimeDir("highlow-schema-high"), "staging")
	recipient := replication.RecipientPublic(&highSecret)
	low, err := openExporter("highlow-schema-low", lowDBID, lowNode, stagingLow, lowSignerA, recipient)
	if err != nil {
		t.Fatalf("low cluster setup: %v", err)
	}
	high, err := openHigh("highlow-schema-high", highDBID, [][16]byte{highNode0},
		[]string{stagingHigh}, highSecret, [][16]byte{lowDBID}, lowSigners(&lowSignerA), lowOrigins(low))
	if err != nil {
		low.Finish(true)
		t.Fatalf("high cluster setup: %v", err)
	}
	if err := migrate(low, 0, emailDDL, "low"); err != nil {
		finishFails(t, err, low, "highlow-schema-low", high, "highlow-schema-high")
	}
	if err := migrate(high, 0, baseDDL, "high"); err != nil {
		finishFails(t, err, low, "highlow-schema-low", high, "highlow-schema-high")
	}
	f := newFerry(stagingLow, stagingHigh)

	if err := runSchemaBody(low, high, f); err != nil {
		finishFails(t, err, low, "highlow-schema-low", high, "highlow-schema-high")
	}
	finishPass(low, high)
}

func runSchemaBody(low, high *goharness.Cluster, f *ferry) error {
	if err := insertContact(low, 0, "ann-id", "ann", 1); err != nil {
		return err
	}
	if err := setEmail(low, 0, "ann-id", "a@x"); err != nil {
		return err
	}
	if err := drainLow(f); err != nil {
		return err
	}
	if err := f.ferryAll(); err != nil {
		return err
	}
	// The unknown column holds the bundles: nothing applies (the
	// product traps the hold internally and returns Ok, so there is no
	// waiting-schema error to assert like the original Go).
	settle()
	got, err := contacts(high, 0)
	if err != nil {
		return err
	}
	if len(got) != 0 {
		return fmt.Errorf("held bundle applied: %v", got)
	}
	if hasColumn(high, 0, table, "email") {
		return fmt.Errorf("DDL crossed the bridge")
	}
	// The hold survives restart and still applies nothing.
	high.StopNode(0)
	if err := high.StartNode(0); err != nil {
		return fmt.Errorf("high restart: %w", err)
	}
	settle()
	got, err = contacts(high, 0)
	if err != nil {
		return err
	}
	if len(got) != 0 {
		return fmt.Errorf("held bundle applied after restart: %v", got)
	}
	if hasColumn(high, 0, table, "email") {
		return fmt.Errorf("DDL crossed the bridge")
	}
	// The local migration releases the bundles; the held email arrives.
	// (Local DDL only: the replicated flag set at first migrate persists
	// in the server-owned config section.)
	if err := high.EnsureMigrated(0, addEmail); err != nil {
		return fmt.Errorf("high ADD_EMAIL migrate failed: %w", err)
	}
	if _, err := high.Node(0).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("high flush after ADD_EMAIL: %w", err)
	}
	deadline := time.Now().Add(wait)
	for {
		email, ok := emailOf(high, 0, "ann-id")
		if ok && email == "a@x" {
			break
		}
		if !time.Now().Before(deadline) {
			email, _ := emailOf(high, 0, "ann-id")
			return fmt.Errorf("held email never arrived after local migration: %q", email)
		}
		time.Sleep(poll)
	}
	got, err = contacts(high, 0)
	if err != nil {
		return err
	}
	if !equalContacts(got, map[string]int64{"ann": 1}) {
		return fmt.Errorf("released bundle applied the wrong set")
	}
	return nil
}

// TestSignerRotationAndOutageCatchUp is signer rotation plus outage
// catch-up (Go: `TestHighLowKeyRotationAndOutageResumeLive`).
func TestSignerRotationAndOutageCatchUp(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	stagingLow := filepath.Join(goharness.RuntimeDir("highlow-rotate-low"), "staging")
	stagingHigh := filepath.Join(goharness.RuntimeDir("highlow-rotate-high"), "staging")
	recipient := replication.RecipientPublic(&highSecret)
	low, err := openExporter("highlow-rotate-low", lowDBID, lowNode, stagingLow, lowSignerA, recipient)
	if err != nil {
		t.Fatalf("low cluster setup: %v", err)
	}
	high, err := openHigh("highlow-rotate-high", highDBID, [][16]byte{highNode0},
		[]string{stagingHigh}, highSecret, [][16]byte{lowDBID}, lowSigners(&lowSignerA), lowOrigins(low))
	if err != nil {
		low.Finish(true)
		t.Fatalf("high cluster setup: %v", err)
	}
	if err := migrate(low, 0, baseDDL, "low"); err != nil {
		finishFails(t, err, low, "highlow-rotate-low", high, "highlow-rotate-high")
	}
	if err := migrate(high, 0, baseDDL, "high"); err != nil {
		finishFails(t, err, low, "highlow-rotate-low", high, "highlow-rotate-high")
	}
	f := newFerry(stagingLow, stagingHigh)
	lowOrigin := low.NodeOriginPubkey(0)

	if err := runRotationBody(low, high, f, lowOrigin); err != nil {
		finishFails(t, err, low, "highlow-rotate-low", high, "highlow-rotate-high")
	}
	finishPass(low, high)
}

func runRotationBody(low, high *goharness.Cluster, f *ferry, lowOrigin [32]byte) error {
	recipient := replication.RecipientPublic(&highSecret)
	if err := drainLow(f); err != nil {
		return err
	}
	if err := f.ferryAll(); err != nil {
		return err
	}
	settle()

	// Round 1 under the predecessor key, imported before rotation
	// (the product pins a single signer per source node, so overlap
	// trust is inexpressible; see the package docs).
	if err := insertContact(low, 0, "ann-id", "ann", 1); err != nil {
		return err
	}
	if err := drainLow(f); err != nil {
		return err
	}
	round1, err := f.newBundles()
	if err != nil {
		return err
	}
	if err := f.ferryNames(round1); err != nil {
		return err
	}
	if err := waitHigh(high, 0, map[string]int64{"ann": 1}); err != nil {
		return err
	}

	// Rotate: the successor signs from here on (config edit plus
	// restart on both sides).
	low.StopNode(0)
	lowSignerHex := goharness.Hex(lowSignerB[:])
	if err := low.SetBridgeConfig(0, &goharness.BridgeNodeConfig{
		Role:      "low-exporter",
		Domain:    goharness.Hex(lowDBID[:]),
		Directory: filepath.ToSlash(f.low),
		Stream:    stream,
		Recipient: goharness.Hex(recipient[:]),
		SignerHex: &lowSignerHex,
	}); err != nil {
		return fmt.Errorf("low signer rotation: %w", err)
	}
	if err := low.StartNode(0); err != nil {
		return fmt.Errorf("low restart after rotation: %w", err)
	}
	high.StopNode(0)
	receiverSecretHex := goharness.Hex(highSecret[:])
	signerBPub := replication.PublicKey(&lowSignerB)
	if err := high.SetBridgeConfig(0, &goharness.BridgeNodeConfig{
		Role:              "high-receiver",
		Domain:            goharness.Hex(highDBID[:]),
		TrustedDomains:    []string{goharness.Hex(lowDBID[:])},
		TrustedOrigins:    map[string]string{goharness.Hex(lowNode[:]): goharness.Hex(lowOrigin[:])},
		Directory:         filepath.ToSlash(f.high),
		Stream:            stream,
		Recipient:         goharness.Hex(recipient[:]),
		ReceiverSecretHex: &receiverSecretHex,
		TrustedSigners:    map[string]string{goharness.Hex(lowNode[:]): goharness.Hex(signerBPub[:])},
	}); err != nil {
		return fmt.Errorf("high signer rotation: %w", err)
	}
	if err := high.StartNode(0); err != nil {
		return fmt.Errorf("high restart after rotation: %w", err)
	}

	// Round 2 under the successor key imports on top of round 1.
	if err := insertContact(low, 0, "bob-id", "bob", 2); err != nil {
		return err
	}
	if err := drainLow(f); err != nil {
		return err
	}
	round2, err := f.newBundles()
	if err != nil {
		return err
	}
	if err := f.ferryNames(round2); err != nil {
		return err
	}
	both := map[string]int64{"ann": 1, "bob": 2}
	if err := waitHigh(high, 0, both); err != nil {
		return err
	}

	// Retire the predecessor: clear the applied bundles, then prove a
	// duplicate of a current bundle still stages cleanly while a
	// retired-signer replay fails closed without disturbing state.
	if err := clearBundles(f.high); err != nil {
		return err
	}
	if err := clearQuarantine(f.high); err != nil {
		return err
	}
	if err := copyFile(filepath.Join(f.low, round2[0]), filepath.Join(f.high, round2[0])); err != nil {
		return fmt.Errorf("duplicate round-2 bundle: %w", err)
	}
	settle()
	if _, err := os.Stat(filepath.Join(f.high, round2[0])); err != nil {
		return fmt.Errorf("duplicate of a current bundle was quarantined")
	}
	if q := quarantineFiles(f.high); len(q) != 0 {
		return fmt.Errorf("current duplicate quarantined: %v", q)
	}
	got, err := contacts(high, 0)
	if err != nil {
		return err
	}
	if !equalContacts(got, both) {
		return fmt.Errorf("current duplicate changed state")
	}
	if err := os.Remove(filepath.Join(f.high, round2[0])); err != nil {
		return fmt.Errorf("clear duplicate: %w", err)
	}
	retired, err := os.ReadFile(filepath.Join(f.low, round1[0]))
	if err != nil {
		return fmt.Errorf("read round-1 bundle: %w", err)
	}
	if err := os.WriteFile(filepath.Join(f.high, "replay.bundle"), retired, 0o644); err != nil {
		return fmt.Errorf("stage replay: %w", err)
	}
	settle()
	if q := quarantineFiles(f.high); !equalStrings(q, []string{"replay.bundle"}) {
		return fmt.Errorf("retired-signer replay not quarantined: %v", q)
	}
	got, err = contacts(high, 0)
	if err != nil {
		return err
	}
	if !equalContacts(got, both) {
		return fmt.Errorf("retired-signer replay disturbed state")
	}
	if err := os.Remove(filepath.Join(f.high, "quarantine", "replay.bundle")); err != nil {
		return fmt.Errorf("clear quarantine: %w", err)
	}

	// Prolonged outage: events queue durably on Low (capture-only; the
	// bundles stay in the Low dir), survive a Low restart, then catch
	// up completely. There is no pending-events counter API, so the
	// exact converged total (backlog == 4) proves all five events
	// arrived exactly once.
	if err := insertContact(low, 0, "backlog-id", "backlog", 0); err != nil {
		return err
	}
	for range 4 {
		if err := addScore(low, 0, "backlog-id", 1); err != nil {
			return err
		}
	}
	if err := drainLow(f); err != nil {
		return err
	}
	low.StopNode(0)
	if err := low.StartNode(0); err != nil {
		return fmt.Errorf("low restart during outage: %w", err)
	}
	got, err = contacts(high, 0)
	if err != nil {
		return err
	}
	if _, ok := got["backlog"]; ok {
		return fmt.Errorf("outage leaked to high: %v", got)
	}
	if err := f.ferryAll(); err != nil {
		return err
	}
	caughtUp := map[string]int64{"ann": 1, "bob": 2, "backlog": 4}
	if err := waitHigh(high, 0, caughtUp); err != nil {
		return err
	}
	lowContacts, err := contacts(low, 0)
	if err != nil {
		return err
	}
	highContacts, err := contacts(high, 0)
	if err != nil {
		return err
	}
	if !equalContacts(lowContacts, highContacts) {
		return fmt.Errorf("low/high contact states diverged")
	}
	return nil
}

// TestHighOwnedFieldReorderConvergesAcrossPeers is High-owned-field
// reorder convergence across two meshed High peers (Go:
// `TestHighLowOwnershipReorderAcrossPeersLive`): peer A imports bundle
// 1, takes a High override, then imports bundle 2
// (override-then-import); peer B imports both bundles before the mesh
// delivers the override (import-then-override). Both converge on the
// High bytes with High-owned provenance. (Adversarial HLC ordering
// stays pinned at the DB layer by the bridge shadow-reorder coverage,
// as in the original Go. The explicit-release convergence leg has no
// wire equivalent and is omitted.)
func TestHighOwnedFieldReorderConvergesAcrossPeers(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	stagingLow := filepath.Join(goharness.RuntimeDir("highlow-own-low"), "staging")
	stagingHigh := filepath.Join(goharness.RuntimeDir("highlow-own-high"), "staging")
	recipient := replication.RecipientPublic(&highSecret)
	low, err := openExporter("highlow-own-low", lowDBID, lowNode, stagingLow, lowSignerA, recipient)
	if err != nil {
		t.Fatalf("low cluster setup: %v", err)
	}
	high, err := openHigh("highlow-own-high", highDBID, [][16]byte{highNode0, highNode1},
		[]string{stagingHigh, stagingHigh}, highSecret, [][16]byte{lowDBID},
		lowSigners(&lowSignerA), lowOrigins(low))
	if err != nil {
		low.Finish(true)
		t.Fatalf("high cluster setup: %v", err)
	}
	if err := migrate(low, 0, baseDDL, "low"); err != nil {
		finishFails(t, err, low, "highlow-own-low", high, "highlow-own-high")
	}
	if err := migrate(high, 0, baseDDL, "high-0"); err != nil {
		finishFails(t, err, low, "highlow-own-low", high, "highlow-own-high")
	}
	if err := migrate(high, 1, baseDDL, "high-1"); err != nil {
		finishFails(t, err, low, "highlow-own-low", high, "highlow-own-high")
	}
	f := newFerry(stagingLow, stagingHigh)

	err = func() error {
		if err := drainLow(f); err != nil {
			return err
		}
		if err := f.ferryAll(); err != nil {
			return err
		}
		settle()

		if err := insertContact(low, 0, "row-0", "low-1", 1); err != nil {
			return err
		}
		if err := drainLow(f); err != nil {
			return err
		}
		if err := f.ferryAll(); err != nil {
			return err
		}
		// Peer A: override-then-import. The wait for bundle 1 is the
		// ordering gate: the override lands strictly before bundle 2
		// exists, so no background pump can collapse the order.
		if err := waitHigh(high, 0, map[string]int64{"low-1": 1}); err != nil {
			return err
		}
		if err := rename(high, 0, "row-0", "high"); err != nil {
			return err
		}
		got, err := contacts(high, 0)
		if err != nil {
			return err
		}
		if !equalContacts(got, map[string]int64{"high": 1}) {
			return fmt.Errorf("A override not visible locally")
		}
		if err := rename(low, 0, "row-0", "low-2"); err != nil {
			return err
		}
		if err := drainLow(f); err != nil {
			return err
		}
		if err := f.ferryAll(); err != nil {
			return err
		}
		if err := waitHigh(high, 0, map[string]int64{"high": 1}); err != nil {
			return err
		}

		// Peer B: import-then-override (the override arrives later over
		// the mesh). B has no owner record yet (no mesh), so both bundles
		// show the Low value.
		if err := waitHigh(high, 1, map[string]int64{"low-2": 1}); err != nil {
			return err
		}

		// Mesh the High peers only after both orders are established
		// locally; the mesh is the only override-delivery path.
		if err := high.AddPeer(0, 1); err != nil {
			return fmt.Errorf("mesh a->b: %w", err)
		}
		if err := high.AddPeer(1, 0); err != nil {
			return fmt.Errorf("mesh b->a: %w", err)
		}
		owned := map[string]int64{"high": 1}
		if err := waitMeshed(high, []int{0, 1}, owned, "ownership reorder"); err != nil {
			return err
		}
		c0, err := contacts(high, 0)
		if err != nil {
			return err
		}
		c1, err := contacts(high, 1)
		if err != nil {
			return err
		}
		if !equalContacts(c0, c1) {
			return fmt.Errorf("peers diverged after mesh")
		}

		// High-owned provenance (behavioral): drain the bridge to idle so
		// the Low update has provably arrived, then prove it stayed
		// suppressed on both peers.
		if err := rename(low, 0, "row-0", "low-3"); err != nil {
			return err
		}
		if err := drainLow(f); err != nil {
			return err
		}
		if err := f.ferryAll(); err != nil {
			return err
		}
		settle()
		// Settle again after a second drain: the ferry above moved every
		// exported bundle, so the background importer has seen them all.
		if err := drainLow(f); err != nil {
			return err
		}
		if err := f.ferryAll(); err != nil {
			return err
		}
		settle()
		c0, err = contacts(high, 0)
		if err != nil {
			return err
		}
		if !equalContacts(c0, owned) {
			return fmt.Errorf("low-3 leaked through A's ownership")
		}
		c1, err = contacts(high, 1)
		if err != nil {
			return err
		}
		if !equalContacts(c1, owned) {
			return fmt.Errorf("low-3 leaked through B's ownership")
		}
		return nil
	}()
	if err != nil {
		finishFails(t, err, low, "highlow-own-low", high, "highlow-own-high")
	}
	finishPass(low, high)
}

// Live scenario: a typed bridge record imports into the native path,
// honors ownership, and survives restart.
//
// Go port of tests/typed_bridge.rs using tests-live/goharness. Identical
// behavior: two single-node murmur-server clusters form the Low
// (exporter) and High (importer) bridge domains sharing one staging
// directory; a typed row inserted on Low is exported, imported through
// High's typed durable path, overridden by a High write (automatic
// ownership), shielded from a Low update, released over the wire (MURMUR
// BRIDGE RELEASE), updated by Low again, and the imported result's
// survival across a High restart is proven last. TypedRename maps to an
// UPDATE of the LWW title field so the primary-key row identity is
// preserved. ed25519/x25519 keys come from the in-module
// murmur/replication package.
package typedbridge

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

const (
	// lowName owns runtime/typed-bridge-low/, which also holds the
	// shared staging dir so artifacts are retained/cleaned with it.
	lowName = "typed-bridge-low"
	// highName owns runtime/typed-bridge-high/.
	highName = "typed-bridge-high"
	// stream is the bridge stream (Rust: typed-stream).
	stream = "typed-stream"
	// table is the typed table. title is the LWW rename target.
	table = "live_typed_records"
	// ddl creates the typed table.
	ddl = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)"
	// row is the row identity, fixed for the whole scenario.
	row = "bridge-native-record"
	// wait is the convergence deadline (Rust: 30 s).
	wait = 30 * time.Second
	// poll is the convergence poll interval (Rust: 50 ms).
	poll = 50 * time.Millisecond
	// exportTimeout is the export proof deadline: a new bundle file must
	// appear in the staging dir after a Low write (Rust: 30 s).
	exportTimeout = 30 * time.Second
	// suppressionSettle is the settle after the export proof before
	// asserting suppression (Rust: 10 s).
	suppressionSettle = 10 * time.Second
)

// Independent bridge domains.
var (
	lowDBID  = [16]byte{0xb0, 0xb0, 0xb0, 0xb0, 0xb0, 0xb0, 0xb0, 0xb0, 0xb0, 0xb0, 0xb0, 0xb0, 0xb0, 0xb0, 0xb0, 0xb0}
	highDBID = [16]byte{0xb1, 0xb1, 0xb1, 0xb1, 0xb1, 0xb1, 0xb1, 0xb1, 0xb1, 0xb1, 0xb1, 0xb1, 0xb1, 0xb1, 0xb1, 0xb1}
	lowNode  = [16]byte{0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10}
	highNode = [16]byte{0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11}
	// lowSigner is the Low bundle-signer seed (High pins the verifying key).
	lowSigner = [32]byte{
		0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21,
		0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21,
		0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21,
		0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21,
	}
	// highSecret is the High receiver secret (Low encrypts to the
	// derived recipient).
	highSecret = [32]byte{
		0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
		0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
		0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
		0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
	}
)

// insertRow is the typed insert on Low (Rust: insert_row).
func insertRow(low *goharness.Cluster) error {
	if _, err := low.Node(0).Execute(
		"INSERT INTO live_typed_records(name, title) VALUES($1, $2)", row, row); err != nil {
		return fmt.Errorf("low typed insert failed: %w", err)
	}
	if _, err := low.Node(0).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("low flush after typed insert failed: %w", err)
	}
	return nil
}

// updateTitle is a known-field update on the LWW title field (Rust:
// update_title).
func updateTitle(cluster *goharness.Cluster, label, title string) error {
	if _, err := cluster.Node(0).Execute(
		"UPDATE live_typed_records SET title = $1 WHERE name = $2", title, row); err != nil {
		return fmt.Errorf("%s title update failed: %w", label, err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("%s flush after title update failed: %w", label, err)
	}
	return nil
}

// releaseTitle releases High ownership of one field, retrying while the
// engine reports busy (Rust: release_title). Callers pass the column in
// mixed case to cover case-insensitive resolution.
func releaseTitle(high *goharness.Cluster, column string) error {
	deadline := time.Now().Add(wait)
	for {
		got, err := high.Node(0).Simple(
			"MURMUR BRIDGE RELEASE " + table + " '" + row + "' COLUMN " + column)
		if err != nil {
			return fmt.Errorf("high bridge release failed: %w", err)
		}
		if len(got) > 0 && len(got[0]) > 0 && got[0][0] == "true" {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("high bridge release never applied (engine busy)")
		}
		time.Sleep(poll)
	}
}

// titleCount counts rows by title on High (Rust: title_count).
func titleCount(high *goharness.Cluster, title string) (int64, error) {
	got, err := high.Node(0).Query(
		"SELECT count(*) FROM live_typed_records WHERE title = $1", title)
	if err != nil {
		return 0, fmt.Errorf("high title count failed: %w", err)
	}
	if len(got) == 0 {
		return 0, fmt.Errorf("high title count returned no rows")
	}
	n, err := got[0].Int64(0)
	if err != nil {
		return 0, fmt.Errorf("high decode title count: %w", err)
	}
	return n, nil
}

// waitTitle waits until High shows want rows with title (Rust:
// wait_title).
func waitTitle(high *goharness.Cluster, title string, want int64) error {
	deadline := time.Now().Add(wait)
	for {
		n, err := titleCount(high, title)
		if err != nil {
			return err
		}
		if n == want {
			return nil
		}
		if !time.Now().Before(deadline) {
			n, err := titleCount(high, title)
			if err != nil {
				return err
			}
			return fmt.Errorf("high title count for %q = %d, want %d", title, n, want)
		}
		time.Sleep(poll)
	}
}

// bundles lists the bundle files currently in the staging dir.
func bundles(staging string) []string {
	entries, err := os.ReadDir(staging)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".bundle" {
			out = append(out, filepath.Join(staging, e.Name()))
		}
	}
	return out
}

// waitExported waits until the staging dir holds more than before bundle
// files: proves Low exported the latest write (Rust: wait_exported).
func waitExported(staging string, before int) error {
	deadline := time.Now().Add(exportTimeout)
	for {
		count := len(bundles(staging))
		if count > before {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf(
				"staging dir still holds %d bundle files after %v (had %d); low never exported",
				count, exportTimeout, before)
		}
		time.Sleep(poll)
	}
}

func runBody(low, high *goharness.Cluster, staging string) error {
	for _, leg := range []struct {
		label   string
		cluster *goharness.Cluster
	}{{"low", low}, {"high", high}} {
		if err := leg.cluster.EnsureMigrated(0, ddl); err != nil {
			return fmt.Errorf("%s migrate failed: %w", leg.label, err)
		}
		if _, err := leg.cluster.Node(0).Simple(
			"MURMUR SET TABLE " + table + " REPLICATED true"); err != nil {
			return fmt.Errorf("%s SET TABLE REPLICATED: %w", leg.label, err)
		}
		if _, err := leg.cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("%s flush after SET TABLE failed: %w", leg.label, err)
		}
	}

	// Export + import the native record through the typed durable path.
	if err := insertRow(low); err != nil {
		return err
	}
	if err := waitTitle(high, row, 1); err != nil {
		return err
	}
	// High typed field override claims ownership of title.
	if err := updateTitle(high, "high", "high-owned-record"); err != nil {
		return err
	}
	if n, err := titleCount(high, "high-owned-record"); err != nil || n != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("high override not visible locally")
	}
	// Low update while the field is High-owned: exported (bundle proof)
	// and imported, but suppressed by High ownership.
	before := len(bundles(staging))
	if err := updateTitle(low, "low", "low-update-before-release"); err != nil {
		return err
	}
	if err := waitExported(staging, before); err != nil {
		return err
	}
	// The import tick runs every 1 s; settle so the (suppressed) import
	// has provably had time to arrive before asserting it stayed out.
	time.Sleep(suppressionSettle)
	if n, err := titleCount(high, "high-owned-record"); err != nil || n != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("high-owned value lost to low update")
	}
	if n, err := titleCount(high, "low-update-before-release"); err != nil || n != 0 {
		if err != nil {
			return err
		}
		return fmt.Errorf("low update leaked through high ownership")
	}
	// Release High ownership of title: the next Low update flows
	// through and replaces the High-owned value.
	if err := releaseTitle(high, "TITLE"); err != nil {
		return err
	}
	if err := updateTitle(low, "low", "low-update-after-release"); err != nil {
		return err
	}
	if err := waitTitle(high, "low-update-after-release", 1); err != nil {
		return err
	}
	if n, err := titleCount(high, "high-owned-record"); err != nil || n != 0 {
		if err != nil {
			return err
		}
		return fmt.Errorf("high-owned value survived release")
	}
	// The imported result survives a High-node restart.
	high.StopNode(0)
	if err := high.StartNode(0); err != nil {
		return fmt.Errorf("high restart: %w", err)
	}
	if n, err := titleCount(high, "low-update-after-release"); err != nil || n != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("imported row lost across high restart")
	}
	return nil
}

func TestTypedBridgeImportsIntoNativeRime(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	staging := filepath.Join(goharness.RuntimeDir(lowName), "staging")
	recipientPub := replication.RecipientPublic(&highSecret)
	recipient := goharness.Hex(recipientPub[:])
	signerHex := goharness.Hex(lowSigner[:])
	lowOpts := goharness.DefaultClusterOptions(lowName)
	lowOpts.NumNodes = 1
	lowOpts.DatabaseID = &lowDBID
	lowOpts.NodeIDs = [][16]byte{lowNode}
	lowOpts.ManualPeers = true
	lowOpts.Bridge = goharness.BridgeOptions{Nodes: map[int]goharness.BridgeNodeConfig{
		0: {
			Role:      "low-exporter",
			Domain:    goharness.Hex(lowDBID[:]),
			Directory: staging,
			Stream:    stream,
			Recipient: recipient,
			SignerHex: &signerHex,
		},
	}}
	low, err := goharness.NewCluster(lowOpts)
	if err != nil {
		t.Fatalf("low cluster setup: %v", err)
	}
	// High pins Low's origin key so imported transactions verify, and
	// Low's bundle-signer key so bundles verify; the seeds themselves
	// stay inside the test/harness.
	lowOrigin := low.NodeOriginPubkey(0)
	lowVerifier := replication.PublicKey(&lowSigner)
	receiverSecret := goharness.Hex(highSecret[:])
	highOpts := goharness.DefaultClusterOptions(highName)
	highOpts.NumNodes = 1
	highOpts.DatabaseID = &highDBID
	highOpts.NodeIDs = [][16]byte{highNode}
	highOpts.ManualPeers = true
	highOpts.Bridge = goharness.BridgeOptions{Nodes: map[int]goharness.BridgeNodeConfig{
		0: {
			Role:           "high-receiver",
			Domain:         goharness.Hex(highDBID[:]),
			TrustedDomains: []string{goharness.Hex(lowDBID[:])},
			TrustedOrigins: map[string]string{
				goharness.Hex(lowNode[:]): goharness.Hex(lowOrigin[:]),
			},
			Directory:         staging,
			Stream:            stream,
			Recipient:         recipient,
			ReceiverSecretHex: &receiverSecret,
			TrustedSigners: map[string]string{
				goharness.Hex(lowNode[:]): goharness.Hex(lowVerifier[:]),
			},
		},
	}}
	high, err := goharness.NewCluster(highOpts)
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
	if err := runBody(low, high, staging); err != nil {
		t.Logf("scenario failed: %v", err)
		t.Fatal("scenario failed; artifacts retained under tests-live/failures/")
	}
	failed = false
}

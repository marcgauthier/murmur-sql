// Live scenario: two independent single-node Low domains export managed
// rows; a two-node High cluster imports one stream per node, then
// converges both domains' rows over its mesh.
//
// Go port of tests/bridge_two_streams.rs using tests-live/goharness.
// Identical behavior: disjoint rows from both Low domains land on both
// High nodes, identical redelivery changes nothing, Low-owned updates
// flow through, then a stream-a-only round and a stream-b-only round
// arrive exactly once. The engine drives bridge capture/process
// automatically; the test plays the operator delivery leg by copying
// completed .bundle artifacts from each Low outbox to its High inbox.
//
// NOT ported: TestCrossDomainIdentityCollisionFailsClosed, for the same
// reason as the Rust source (no wire-observable import-rejection
// signal); see the scenario README.md.
package bridgetwostreams

import (
	"crypto/ed25519"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/curve25519"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// Low-A (exporter) cluster name; owns runtime/bridge-two-streams-low-a/,
// which also holds its outbox so artifacts are retained/cleaned with it.
const lowAName = "bridge-two-streams-low-a"

// Low-B (exporter) cluster name; owns runtime/bridge-two-streams-low-b/.
const lowBName = "bridge-two-streams-low-b"

// High (importer) cluster name; owns runtime/bridge-two-streams-high/.
const highName = "bridge-two-streams-high"

// Bridge streams.
const (
	streamA = "stream-a"
	streamB = "stream-b"
)

const (
	// wait is the convergence deadline.
	wait = 60 * time.Second
	// poll is the convergence poll interval.
	poll = 200 * time.Millisecond
	// idempotenceWindow is the redelivery stability window: identical
	// bytes are re-copied while the converged set must stay unchanged
	// (receipt idempotence).
	idempotenceWindow = 5 * time.Second
)

// Independent bridge domains (distinct DBIDs, CAs, keys per domain; each
// harness cluster already provisions its own CA).
var (
	lowADbid  = [16]byte{0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0}
	lowBDbid  = [16]byte{0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0}
	highDbid  = [16]byte{0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0}
	lowANode  = [16]byte{0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1}
	lowBNode  = [16]byte{0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1}
	highNode0 = [16]byte{0xC1, 0xC1, 0xC1, 0xC1, 0xC1, 0xC1, 0xC1, 0xC1, 0xC1, 0xC1, 0xC1, 0xC1, 0xC1, 0xC1, 0xC1, 0xC1}
	highNode1 = [16]byte{0xC2, 0xC2, 0xC2, 0xC2, 0xC2, 0xC2, 0xC2, 0xC2, 0xC2, 0xC2, 0xC2, 0xC2, 0xC2, 0xC2, 0xC2, 0xC2}
	lowAKey   = [32]byte{0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2, 0xA2}
	lowBKey   = [32]byte{0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2}
	highKey   = [32]byte{0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3, 0xC3}
	// Low bundle-signer seeds (ed25519; each High importer pins its
	// domain key).
	lowASigner = [32]byte{0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21}
	lowBSigner = [32]byte{0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22}
	// High receiver secrets (X25519; one per stream).
	highSecretA = [32]byte{0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31}
	highSecretB = [32]byte{0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32}
)

func ed25519Public(seed [32]byte) [32]byte {
	priv := ed25519.NewKeyFromSeed(seed[:])
	var out [32]byte
	copy(out[:], priv.Public().(ed25519.PublicKey))
	return out
}

func x25519Public(secret [32]byte) ([32]byte, error) {
	pub, err := curve25519.X25519(secret[:], curve25519.Basepoint)
	if err != nil {
		return [32]byte{}, err
	}
	var out [32]byte
	copy(out[:], pub)
	return out, nil
}

// rowID is the Go row identity: %032x of a per-domain base plus the index.
func rowID(base, i uint32) string {
	return fmt.Sprintf("%032x", base+i)
}

// insertRow is a contention insert.
func insertRow(cluster *goharness.Cluster, id, name string) error {
	if _, err := cluster.Node(0).Execute(
		"INSERT INTO live_typed_contention(id, name) VALUES($1, $2)", id, name); err != nil {
		return fmt.Errorf("insert %q: %w", id, err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("flush after %q: %w", id, err)
	}
	return nil
}

// updateName is a contention rename (provenance probes use the Name field).
func updateName(cluster *goharness.Cluster, id, name string) error {
	if _, err := cluster.Node(0).Execute(
		"UPDATE live_typed_contention SET name = $1 WHERE id = $2", name, id); err != nil {
		return fmt.Errorf("update %q: %w", id, err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("flush after update %q: %w", id, err)
	}
	return nil
}

// sortedNames returns the sorted visible names on one High node.
func sortedNames(high *goharness.Cluster, idx int) ([]string, error) {
	rows, err := high.Node(idx).Query("SELECT name FROM live_typed_contention ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("high-%d SELECT names: %w", idx, err)
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		name, err := row.String(0)
		if err != nil {
			return nil, fmt.Errorf("high-%d decode name: %w", idx, err)
		}
		out = append(out, name)
	}
	return out, nil
}

// deliver is the operator delivery leg: copy completed .bundle artifacts
// from a Low outbox to its High inbox (the engine captures/exports and
// stages/applies automatically in the background). It returns bundles
// copied.
func deliver(outbox, inbox string) (int, error) {
	entries, err := os.ReadDir(outbox)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("read outbox %s: %w", outbox, err)
	}
	if err := os.MkdirAll(inbox, 0o755); err != nil {
		return 0, fmt.Errorf("create inbox %s: %w", inbox, err)
	}
	copied := 0
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".bundle") || strings.HasPrefix(name, ".") {
			continue
		}
		path := filepath.Join(outbox, name)
		dest := filepath.Join(inbox, name)
		srcInfo, srcErr := os.Stat(path)
		dstInfo, dstErr := os.Stat(dest)
		fresh := false
		switch {
		case srcErr == nil && dstErr == nil:
			fresh = srcInfo.Size() != dstInfo.Size()
		case srcErr == nil:
			fresh = true
		}
		if fresh {
			body, err := os.ReadFile(path)
			if err != nil {
				return 0, fmt.Errorf("deliver %s: %w", path, err)
			}
			if err := os.WriteFile(dest, body, 0o644); err != nil {
				return 0, fmt.Errorf("deliver %s: %w", path, err)
			}
			copied++
		}
	}
	return copied, nil
}

type lanes struct {
	outboxA string
	inboxA  string
	outboxB string
	inboxB  string
}

func deliverAll(l *lanes) error {
	if _, err := deliver(l.outboxA, l.inboxA); err != nil {
		return err
	}
	if _, err := deliver(l.outboxB, l.inboxB); err != nil {
		return err
	}
	return nil
}

// waitHighNames waits until both High nodes show exactly want, with the
// delivery leg inside the poll.
func waitHighNames(l *lanes, high *goharness.Cluster, want []string) error {
	deadline := time.Now().Add(wait)
	for {
		if err := deliverAll(l); err != nil {
			return err
		}
		n0, err := sortedNames(high, 0)
		if err != nil {
			return err
		}
		n1, err := sortedNames(high, 1)
		if err != nil {
			return err
		}
		if equalStrings(n0, want) && equalStrings(n1, want) {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("high nodes did not converge on %d rows: node0=%v node1=%v",
				len(want), n0, n1)
		}
		time.Sleep(poll)
	}
}

func exporter(domain [16]byte, directory, stream string, signer, recipient [32]byte) goharness.BridgeNodeConfig {
	signerHex := goharness.Hex(signer[:])
	return goharness.BridgeNodeConfig{
		Role:      "low-exporter",
		Domain:    goharness.Hex(domain[:]),
		Directory: directory,
		Stream:    stream,
		Recipient: goharness.Hex(recipient[:]),
		SignerHex: &signerHex,
	}
}

func receiver(domain [16]byte, directory, stream string, secret [32]byte, trustedDomain, lowNode [16]byte, signerPub, originPub [32]byte) (goharness.BridgeNodeConfig, error) {
	recipient, err := x25519Public(secret)
	if err != nil {
		return goharness.BridgeNodeConfig{}, err
	}
	secretHex := goharness.Hex(secret[:])
	return goharness.BridgeNodeConfig{
		Role:              "high-receiver",
		Domain:            goharness.Hex(domain[:]),
		TrustedDomains:    []string{goharness.Hex(trustedDomain[:])},
		TrustedOrigins:    map[string]string{goharness.Hex(lowNode[:]): goharness.Hex(originPub[:])},
		Directory:         directory,
		Stream:            stream,
		Recipient:         goharness.Hex(recipient[:]),
		ReceiverSecretHex: &secretHex,
		TrustedSigners:    map[string]string{goharness.Hex(lowNode[:]): goharness.Hex(signerPub[:])},
	}, nil
}

func runBody(l *lanes, lowA, lowB, high *goharness.Cluster) error {
	// Identical DDL on all three domains, then replicate it (bridge
	// capture reads the replication log, which only carries replicated
	// tables).
	const ddl = "CREATE TABLE live_typed_contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)"
	for _, leg := range []struct {
		label string
		db    *goharness.Cluster
		idx   int
	}{
		{"low-a", lowA, 0}, {"low-b", lowB, 0}, {"high-0", high, 0}, {"high-1", high, 1},
	} {
		if err := leg.db.EnsureMigrated(leg.idx, ddl); err != nil {
			return fmt.Errorf("%s migrate failed: %w", leg.label, err)
		}
		if _, err := leg.db.Node(leg.idx).Simple("MURMUR SET TABLE live_typed_contention REPLICATED true"); err != nil {
			return fmt.Errorf("%s SET TABLE REPLICATED: %w", leg.label, err)
		}
		if _, err := leg.db.Node(leg.idx).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("%s flush after SET TABLE: %w", leg.label, err)
		}
	}

	// Round 1: disjoint rows from both domains land on both High nodes.
	for i := uint32(0); i < 5; i++ {
		if err := insertRow(lowA, rowID(0xA000, i), fmt.Sprintf("a-%d", i)); err != nil {
			return err
		}
		if err := insertRow(lowB, rowID(0xB000, i), fmt.Sprintf("b-%d", i)); err != nil {
			return err
		}
	}
	var round1 []string
	for i := 0; i < 5; i++ {
		round1 = append(round1, fmt.Sprintf("a-%d", i))
	}
	for i := 0; i < 5; i++ {
		round1 = append(round1, fmt.Sprintf("b-%d", i))
	}
	sort.Strings(round1)
	if err := waitHighNames(l, high, round1); err != nil {
		return err
	}

	// Duplicate delivery: re-copy identical bytes through a stability
	// window; the converged set must stay unchanged (receipt idempotence:
	// redelivery applies nothing twice).
	windowEnd := time.Now().Add(idempotenceWindow)
	for time.Now().Before(windowEnd) {
		if err := deliverAll(l); err != nil {
			return err
		}
		time.Sleep(poll)
	}
	if n0, err := sortedNames(high, 0); err != nil {
		return err
	} else if !equalStrings(n0, round1) {
		return fmt.Errorf("high-0 changed after identical redelivery")
	}
	if n1, err := sortedNames(high, 1); err != nil {
		return err
	} else if !equalStrings(n1, round1) {
		return fmt.Errorf("high-1 changed after identical redelivery")
	}

	// Low-owned provenance (behavioral): a Low update to one row per
	// domain must flow through to both High nodes (a High-owned field
	// would suppress it); then restore the row names.
	probeA := rowID(0xA000, 0)
	probeB := rowID(0xB000, 0)
	if err := updateName(lowA, probeA, "a-0-prov"); err != nil {
		return err
	}
	if err := updateName(lowB, probeB, "b-0-prov"); err != nil {
		return err
	}
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
	sort.Strings(proved)
	if err := waitHighNames(l, high, proved); err != nil {
		return err
	}
	if err := updateName(lowA, probeA, "a-0"); err != nil {
		return err
	}
	if err := updateName(lowB, probeB, "b-0"); err != nil {
		return err
	}
	if err := waitHighNames(l, high, round1); err != nil {
		return err
	}

	// Round 2: stream-a only. New A rows arrive on both High nodes.
	for i := uint32(5); i < 8; i++ {
		if err := insertRow(lowA, rowID(0xA000, i), fmt.Sprintf("a-%d", i)); err != nil {
			return err
		}
	}
	var round2 []string
	for i := 0; i < 8; i++ {
		round2 = append(round2, fmt.Sprintf("a-%d", i))
	}
	for i := 0; i < 5; i++ {
		round2 = append(round2, fmt.Sprintf("b-%d", i))
	}
	sort.Strings(round2)
	if err := waitHighNames(l, high, round2); err != nil {
		return err
	}

	// Stream-b progress isolation (behavioral): a stream-b-only round must
	// deliver exactly once from the undisturbed cursor (no loss from a
	// skipped sequence, no duplication).
	for i := uint32(5); i < 7; i++ {
		if err := insertRow(lowB, rowID(0xB000, i), fmt.Sprintf("b-%d", i)); err != nil {
			return err
		}
	}
	var round3 []string
	for i := 0; i < 8; i++ {
		round3 = append(round3, fmt.Sprintf("a-%d", i))
	}
	for i := 0; i < 7; i++ {
		round3 = append(round3, fmt.Sprintf("b-%d", i))
	}
	sort.Strings(round3)
	return waitHighNames(l, high, round3)
}

func TestTwoLowDomainsIntoOneHigh(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	l := &lanes{
		outboxA: filepath.Join(goharness.RuntimeDir(lowAName), "outbox-a"),
		inboxA:  filepath.Join(goharness.RuntimeDir(highName), "inbox-a"),
		outboxB: filepath.Join(goharness.RuntimeDir(lowBName), "outbox-b"),
		inboxB:  filepath.Join(goharness.RuntimeDir(highName), "inbox-b"),
	}
	recipientA, err := x25519Public(highSecretA)
	if err != nil {
		t.Fatalf("derive recipient A: %v", err)
	}
	recipientB, err := x25519Public(highSecretB)
	if err != nil {
		t.Fatalf("derive recipient B: %v", err)
	}
	lowAOpts := goharness.DefaultClusterOptions(lowAName)
	lowAOpts.NumNodes = 1
	lowAOpts.DatabaseID = &lowADbid
	lowAOpts.NodeIDs = [][16]byte{lowANode}
	lowAOpts.Key = lowAKey
	lowAOpts.ManualPeers = true
	lowAOpts.Bridge = goharness.BridgeOptions{Nodes: map[int]goharness.BridgeNodeConfig{
		0: exporter(lowADbid, l.outboxA, streamA, lowASigner, recipientA),
	}}
	lowA, err := goharness.NewCluster(lowAOpts)
	if err != nil {
		t.Fatalf("low-a cluster setup: %v", err)
	}
	lowBOpts := goharness.DefaultClusterOptions(lowBName)
	lowBOpts.NumNodes = 1
	lowBOpts.DatabaseID = &lowBDbid
	lowBOpts.NodeIDs = [][16]byte{lowBNode}
	lowBOpts.Key = lowBKey
	lowBOpts.ManualPeers = true
	lowBOpts.Bridge = goharness.BridgeOptions{Nodes: map[int]goharness.BridgeNodeConfig{
		0: exporter(lowBDbid, l.outboxB, streamB, lowBSigner, recipientB),
	}}
	lowB, err := goharness.NewCluster(lowBOpts)
	if err != nil {
		t.Fatalf("low-b cluster setup: %v", err)
	}
	// Each High importer pins its own domain's origin key so imported
	// transactions verify.
	lowAOrigin := lowA.NodeOriginPubkey(0)
	lowBOrigin := lowB.NodeOriginPubkey(0)
	receiverA, err := receiver(highDbid, l.inboxA, streamA, highSecretA, lowADbid, lowANode,
		ed25519Public(lowASigner), lowAOrigin)
	if err != nil {
		t.Fatalf("receiver A config: %v", err)
	}
	receiverB, err := receiver(highDbid, l.inboxB, streamB, highSecretB, lowBDbid, lowBNode,
		ed25519Public(lowBSigner), lowBOrigin)
	if err != nil {
		t.Fatalf("receiver B config: %v", err)
	}
	highOpts := goharness.DefaultClusterOptions(highName)
	highOpts.NumNodes = 2
	highOpts.DatabaseID = &highDbid
	highOpts.NodeIDs = [][16]byte{highNode0, highNode1}
	highOpts.Key = highKey
	highOpts.Bridge = goharness.BridgeOptions{Nodes: map[int]goharness.BridgeNodeConfig{
		0: receiverA,
		1: receiverB,
	}}
	high, err := goharness.NewCluster(highOpts)
	if err != nil {
		t.Fatalf("high cluster setup: %v", err)
	}

	resultErr := runBody(l, lowA, lowB, high)
	failed := resultErr != nil
	if resultErr != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", resultErr)
	}
	if failed {
		lowA.DumpForensics("bridge-two-streams-low-a")
		lowB.DumpForensics("bridge-two-streams-low-b")
		high.DumpForensics("bridge-two-streams-high")
	}
	lowA.Finish(failed)
	lowB.Finish(failed)
	high.Finish(failed)
	if failed {
		t.Fatalf("scenario failed: %v; artifacts retained under tests-live/failures/", resultErr)
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

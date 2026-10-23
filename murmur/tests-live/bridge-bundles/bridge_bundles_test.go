// Live scenario: raw sealed bundles cross the Low/High bridge.
//
// The generic bundle API (Bridge.Export on Low; Bridge.Stage and
// Bridge.OpenBundle on High) is exercised end to end with real keys,
// CAs, and sealed envelopes: three payloads export with sequential
// bundle names, stage with verified headers, and open byte-identical.
// A tampered copy fails staging and is quarantined by Process while
// the originals keep staging; role and size negatives are pinned.
package bridgebundles

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// lowName is the Low (exporter) cluster name; owns
// runtime/bridge-bundles-low/, which also holds the shared staging dir.
const lowName = "bridge-bundles-low"

// highName is the High (importer) cluster name; owns
// runtime/bridge-bundles-high/.
const highName = "bridge-bundles-high"

// stream is the bridge stream.
const stream = "field-bundles"

func bytes16(b byte) [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = b
	}
	return id
}

func bytes32(b byte) [32]byte {
	var id [32]byte
	for i := range id {
		id[i] = b
	}
	return id
}

// Independent bridge domains (each harness cluster already provisions
// its own CA).
var (
	lowDBID   = bytes16(0xD0)
	highDBID  = bytes16(0xD1)
	lowNode0  = bytes16(0xD2)
	highNode0 = bytes16(0xD3)
	lowKey    = bytes32(0xD4)
	highKey   = bytes32(0xD5)
	// lowSigner is the Low bundle-signer seed (ed25519; the High
	// importer pins its public key).
	lowSigner = bytes32(0xD6)
	// highSecret is the High receiver secret (X25519; seals the
	// bundles for High).
	highSecret = bytes32(0xD7)
)

func nodeBridge(t *testing.T, cluster *harness.Cluster, idx int) *replication.Bridge {
	t.Helper()
	h := cluster.Node(idx).Replication()
	if h == nil {
		t.Fatalf("node%d replication handle missing", idx+1)
	}
	b := h.Bridge()
	if b == nil {
		t.Fatalf("node%d bridge missing", idx+1)
	}
	return b
}

// payload renders n deterministic bytes.
func payload(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte((i*37 + i/251) % 253)
	}
	return out
}

func TestRawBundlesCrossTheBridge(t *testing.T) {
	staging := filepath.Join(harness.RuntimeDir(lowName), "staging")
	low, err := harness.NewCluster(harness.ClusterOptions{
		Name:       lowName,
		NumNodes:   1,
		DatabaseID: &lowDBID,
		NodeIDs:    [][16]byte{lowNode0},
		Key:        lowKey,
		Bridge: harness.BridgeOptions{
			Nodes: map[int]*replication.BridgeConfig{
				0: replication.NewBridgeExporter(lowDBID, staging, stream, lowSigner,
					replication.RecipientPublic(&highSecret)),
			},
		},
	})
	if err != nil {
		t.Fatalf("low cluster setup: %v", err)
	}
	high, err := harness.NewCluster(harness.ClusterOptions{
		Name:       highName,
		NumNodes:   1,
		DatabaseID: &highDBID,
		NodeIDs:    [][16]byte{highNode0},
		Key:        highKey,
		Bridge: harness.BridgeOptions{
			Nodes: map[int]*replication.BridgeConfig{
				0: replication.NewBridgeReceiver(highDBID, staging, stream, highSecret,
					map[[16]byte]bool{lowDBID: true},
					map[[16]byte][32]byte{lowNode0: replication.PublicKey(&lowSigner)},
					map[[16]byte][32]byte{},
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
			low.DumpForensics("bridge-bundles-low")
			high.DumpForensics("bridge-bundles-high")
		}
		low.Finish(failed)
		high.Finish(failed)
	}()
	lowBridge := nodeBridge(t, low, 0)
	highBridge := nodeBridge(t, high, 0)

	// Export three payloads: empty, one byte, and 100 KiB. Bundle
	// files carry 1-based sequence names.
	want := [][]byte{nil, {0xA5}, payload(100 << 10)}
	var paths []string
	for i, p := range want {
		path, err := lowBridge.Export(p)
		if err != nil {
			t.Fatalf("export %d: %v", i+1, err)
		}
		paths = append(paths, path)
		suffix := "-0000000000000000000" + string(rune('1'+i)) + ".bundle"
		if !strings.HasSuffix(path, suffix) {
			t.Fatalf("bundle %d path %q lacks sequence suffix %q", i+1, path, suffix)
		}
	}

	// Stage each on High: headers carry the Low database, node,
	// stream, and sequence. OpenBundle returns the exact payload.
	for i, path := range paths {
		header, err := highBridge.Stage(path)
		if err != nil {
			t.Fatalf("stage %d: %v", i+1, err)
		}
		if header.Sequence != uint64(i+1) || header.Stream != stream ||
			header.SourceDatabase != replication.NodeId(lowDBID) ||
			header.SourceNode != replication.NodeId(lowNode0) {
			t.Fatalf("bundle %d header = %+v", i+1, header)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		opened, data, err := highBridge.OpenBundle(raw)
		if err != nil {
			t.Fatalf("open %d: %v", i+1, err)
		}
		if opened.Bundle != header.Bundle || !bytes.Equal(data, want[i]) {
			t.Fatalf("bundle %d opened payload differs", i+1)
		}
	}

	// Re-staging the identical artifact is idempotent.
	if _, err := highBridge.Stage(paths[0]); err != nil {
		t.Fatalf("re-stage identical: %v", err)
	}

	// A tampered copy fails staging, and Process quarantines it while
	// the originals still stage.
	raw, err := os.ReadFile(paths[1])
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xFF
	tampered := filepath.Join(staging, "tampered.bundle")
	if err := os.WriteFile(tampered, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := highBridge.Stage(tampered); err == nil {
		t.Fatal("tampered bundle staged")
	}
	n, err := highBridge.Process()
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if n != 0 {
		t.Fatalf("process applied %d raw bundles, want 0 (held, not BridgeContent)", n)
	}
	if _, err := os.Stat(filepath.Join(staging, "quarantine", "tampered.bundle")); err != nil {
		t.Fatalf("tampered bundle not quarantined: %v", err)
	}
	if _, err := highBridge.Stage(paths[1]); err != nil {
		t.Fatalf("original stopped staging after quarantine: %v", err)
	}

	// Role negatives: the exporter neither stages nor opens.
	if _, err := lowBridge.Stage(paths[0]); err == nil {
		t.Fatal("exporter staged a bundle")
	}
	raw, err = os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := lowBridge.OpenBundle(raw); err == nil {
		t.Fatal("exporter opened a bundle")
	}

	// Size negative: past MaxPayloadBytes the export refuses without
	// writing anything.
	before, err := os.ReadDir(staging)
	if err != nil {
		t.Fatal(err)
	}
	huge := make([]byte, (64<<20)+1)
	if _, err := lowBridge.Export(huge); err == nil {
		t.Fatal("oversize payload exported")
	} else if err != replication.ErrOverloaded {
		t.Fatalf("oversize err = %v, want ErrOverloaded", err)
	}
	after, err := os.ReadDir(staging)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatal("oversize export left staging artifacts")
	}
	failed = false
}

// Live scenario: a file crosses the Low/High bridge and back as a tombstone.
//
// Go port of the Rust `files-bridge` scenario
// (/home/marc/MURMUR/tests-live/files-bridge/tests/files_bridge.rs). A
// two-node Low mesh and a two-node High mesh (separate DBIDs, CAs, storage
// keys, and file object keys) move one 384 KiB file across a shared staging
// directory: Low upload, Low mesh metadata + peer fetch, signed export of
// sealed chunks, High import with re-encryption under the High object key,
// High mesh metadata + peer fetch, metadata search, and a delete cascade
// that ends with the file reading deleted on all four nodes.
//
// Uploads, downloads, metadata, search, and deletes use the direct Database
// file API; BridgeExport/BridgeImport map to Bridge.ExportFile plus
// Bridge.Capture (Low) / Bridge.Process (High) pumps, with a pump inside
// every bridge wait poll. Peer fetch is automatic on the configured
// fetch_interval.
package filesbridge

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// lowName is the Low (exporter) cluster name; owns
// runtime/files-bridge-low/, which also holds the shared staging dir so
// artifacts are retained/cleaned with it.
const lowName = "files-bridge-low"

// highName is the High (importer) cluster name; owns
// runtime/files-bridge-high/.
const highName = "files-bridge-high"

// stream is the bridge stream (Rust: field-files).
const stream = "field-files"

// fileName is the bridged file (Rust: shared/field-map.bin).
const fileName = "shared/field-map.bin"

// fileBytes is the upload size (Rust: 384 KiB).
const fileBytes = 384 << 10

const (
	// wait is the convergence deadline (Rust: 30 s waits).
	wait = 30 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
)

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

// Independent bridge domains (Rust: distinct DBIDs, CAs, keys per domain;
// each harness cluster already provisions its own CA).
var (
	lowDBID       = bytes16(0xE0)
	highDBID      = bytes16(0xE1)
	lowNode0      = bytes16(0xE2)
	lowNode1      = bytes16(0xE3)
	highNode0     = bytes16(0xE4)
	highNode1     = bytes16(0xE5)
	lowKey        = bytes32(0xE6)
	highKey       = bytes32(0xE7)
	lowObjectKey  = bytes32(0x11)
	highObjectKey = bytes32(0x22)
	// lowSigner is the Low bundle-signer seed (ed25519; the High importer
	// pins its public key).
	lowSigner = bytes32(0x21)
	// highSecret is the High receiver secret (X25519; seals the file chunks
	// for High).
	highSecret = bytes32(0x31)
)

// payload is the deterministic payload (Rust:
// data[i] = (i*37 + i/251) % 253).
func payload() []byte {
	out := make([]byte, fileBytes)
	for i := range out {
		out[i] = byte((i*37 + i/251) % 253)
	}
	return out
}

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

// pump is the export+import acceleration pump (Rust: BridgeExport +
// BridgeImport). Counts are never asserted: the replication engine also
// drives capture/process in the background, so progress is proven by file
// state instead of published/imported counters.
func pump(t *testing.T, low, high *harness.Cluster) {
	t.Helper()
	if _, err := nodeBridge(t, low, 0).Capture(); err != nil {
		t.Fatalf("low capture: %v", err)
	}
	if _, err := nodeBridge(t, high, 0).Process(); err != nil {
		t.Fatalf("high process: %v", err)
	}
}

// assertNodeBytes downloads the file from cluster node idx and proves full
// integrity: exact bytes plus the uploader's digest carried in the metadata.
func assertNodeBytes(t *testing.T, cluster *harness.Cluster, idx int, want []byte, wantDigest [32]byte) {
	t.Helper()
	var got bytes.Buffer
	meta, err := cluster.Node(idx).GetFile(fileName, &got)
	if err != nil {
		t.Fatalf("node%d download failed: %v", idx+1, err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("node%d bytes differ", idx+1)
	}
	if meta.Digest != wantDigest {
		t.Fatalf("node%d digest differs", idx+1)
	}
}

// waitMetadata waits until cluster node idx carries nondeleted metadata for
// the file with the expected digest (Rust: WaitFileAvailable metadata leg).
func waitMetadata(t *testing.T, cluster *harness.Cluster, idx int, wantDigest [32]byte, what string) {
	t.Helper()
	arrived := harness.WaitUntil(wait, poll, func() bool {
		meta, err := cluster.Node(idx).FileMetadata(fileName)
		return err == nil && meta != nil && meta.Digest == wantDigest
	})
	if !arrived {
		t.Fatalf("%s: metadata never arrived", what)
	}
}

// waitAvailable waits until cluster node idx holds the object locally (Rust:
// fetch leg; the product fetches automatically on fetch_interval).
func waitAvailable(t *testing.T, cluster *harness.Cluster, idx int, what string) {
	t.Helper()
	arrived := harness.WaitUntil(wait, poll, func() bool {
		available, err := cluster.Node(idx).FileAvailable(fileName)
		return err == nil && available
	})
	if !arrived {
		t.Fatalf("%s: object never became available", what)
	}
}

// waitDeleted waits until cluster node idx reads the file deleted (Rust:
// WaitFileDeleted).
func waitDeleted(t *testing.T, cluster *harness.Cluster, idx int, what string) {
	t.Helper()
	deleted := harness.WaitUntil(wait, poll, func() bool {
		meta, err := cluster.Node(idx).FileMetadata(fileName)
		return err == nil && meta == nil
	})
	if !deleted {
		t.Fatalf("%s: tombstone never arrived", what)
	}
}

func contains(haystack, needle []byte) bool {
	return len(needle) == 0 || bytes.Index(haystack, needle) >= 0
}

func TestTwoLowTwoHighFileBridgeLive(t *testing.T) {
	staging := filepath.Join(harness.RuntimeDir(lowName), "staging")
	fetchInterval := 100 * time.Millisecond
	maxBytes := uint64(4 << 20)
	low, err := harness.NewCluster(harness.ClusterOptions{
		Name:       lowName,
		NumNodes:   2,
		DatabaseID: &lowDBID,
		NodeIDs:    [][16]byte{lowNode0, lowNode1},
		Key:        lowKey,
		Files: &harness.FilesOptions{
			Key:           lowObjectKey,
			FetchInterval: &fetchInterval,
			MaxFileBytes:  &maxBytes,
		},
		Bridge: harness.BridgeOptions{
			Nodes: map[int]*replication.BridgeConfig{
				0: replication.NewBridgeExporter(lowDBID, staging, stream, lowSigner, replication.RecipientPublic(&highSecret)),
			},
		},
	})
	if err != nil {
		t.Fatalf("low cluster setup: %v", err)
	}
	// The High importer pins the Low bundle signer and trusts both Low
	// origin keys so imported file transactions verify; the seeds stay
	// inside the harness.
	lowOrigin0 := low.NodeOriginPubkey(0)
	lowOrigin1 := low.NodeOriginPubkey(1)
	high, err := harness.NewCluster(harness.ClusterOptions{
		Name:       highName,
		NumNodes:   2,
		DatabaseID: &highDBID,
		NodeIDs:    [][16]byte{highNode0, highNode1},
		Key:        highKey,
		Files: &harness.FilesOptions{
			Key:           highObjectKey,
			FetchInterval: &fetchInterval,
			MaxFileBytes:  &maxBytes,
		},
		Bridge: harness.BridgeOptions{
			Nodes: map[int]*replication.BridgeConfig{
				0: replication.NewBridgeReceiver(highDBID, staging, stream, highSecret,
					map[[16]byte]bool{lowDBID: true},
					map[[16]byte][32]byte{lowNode0: replication.PublicKey(&lowSigner)},
					map[[16]byte][32]byte{lowNode0: lowOrigin0, lowNode1: lowOrigin1},
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
			low.DumpForensics("files-bridge-low")
			high.DumpForensics("files-bridge-high")
		}
		low.Finish(failed)
		high.Finish(failed)
	}()

	data := payload()

	// Low upload, then Low mesh metadata + peer fetch.
	up, err := low.Node(0).UploadFile(fileName, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("low upload: %v", err)
	}
	if up.Bytes != uint64(len(data)) {
		t.Fatalf("upload byte count %d, want %d", up.Bytes, len(data))
	}
	waitMetadata(t, low, 1, up.Digest, "low mesh metadata")
	waitAvailable(t, low, 1, "low peer fetch")
	assertNodeBytes(t, low, 1, data, up.Digest)

	// Export Low-0 to staging: sealed file-chunk bundles (plus the file
	// metadata transaction bundle). Synchronous, so every chunk bundle
	// exists afterwards; the background engine may add more.
	if err := nodeBridge(t, low, 0).ExportFile(fileName); err != nil {
		t.Fatalf("bridge export: %v", err)
	}
	if _, err := nodeBridge(t, low, 0).Capture(); err != nil {
		t.Fatalf("low capture after export: %v", err)
	}
	entries, err := os.ReadDir(staging)
	if err != nil {
		t.Fatalf("read staging dir: %v", err)
	}
	var staged []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".bundle") {
			staged = append(staged, filepath.Join(staging, e.Name()))
		}
	}
	if len(staged) < 2 {
		t.Fatalf("staged %d bundles, want file chunks plus metadata", len(staged))
	}
	// Staged artifacts must carry no document plaintext: several 1 KiB
	// windows of the payload must be absent from every staged bundle.
	// Only *.bundle files are scanned: the receiver's transient
	// .file-*.part reassembly state lives in the shared dir too once
	// import starts, and it is plaintext by design.
	windows := [][]byte{
		data[0:1024],
		data[100000:101024],
		data[300000:301024],
	}
	for _, path := range staged {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read staged %s: %v", path, err)
		}
		for i, w := range windows {
			if contains(raw, w) {
				t.Fatalf("staged %s contains plaintext window %d", path, i)
			}
		}
	}

	// Import into High-0: metadata applies, chunks install, and the
	// object re-encrypts under the High object key.
	imported := harness.WaitUntil(wait, poll, func() bool {
		pump(t, low, high)
		available, err := high.Node(0).FileAvailable(fileName)
		return err == nil && available
	})
	if !imported {
		t.Fatal("high-0 import never completed")
	}
	assertNodeBytes(t, high, 0, data, up.Digest)
	assertReencrypted(t, low, high)

	// High mesh carries metadata to High-1, which fetches bytes from High-0.
	waitMetadata(t, high, 1, up.Digest, "high mesh metadata")
	waitAvailable(t, high, 1, "high-1 peer fetch")
	assertNodeBytes(t, high, 1, data, up.Digest)

	// Search across the replicated metadata.
	found, err := high.Node(1).SearchFiles("shared/")
	if err != nil {
		t.Fatalf("high search: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("search found %d files, want one", len(found))
	}
	if found[0].Digest != up.Digest {
		t.Fatal("search digest mismatch")
	}
	if found[0].Bytes != uint64(len(data)) {
		t.Fatal("search byte count mismatch")
	}

	// Delete cascade: the Low tombstone replicates, exports, and imports.
	if err := low.Node(0).DeleteFile(fileName); err != nil {
		t.Fatalf("low delete: %v", err)
	}
	waitDeleted(t, low, 1, "low delete mesh")
	tombstoned := harness.WaitUntil(wait, poll, func() bool {
		pump(t, low, high)
		meta, err := high.Node(0).FileMetadata(fileName)
		return err == nil && meta == nil
	})
	if !tombstoned {
		t.Fatal("high-0 delete never arrived")
	}
	waitDeleted(t, high, 1, "high-1 delete")
	if meta, err := low.Node(0).FileMetadata(fileName); err != nil || meta != nil {
		t.Fatal("low-0 still reads the file")
	}
	failed = false
}

// assertReencrypted verifies High at-rest bytes differ from Low at-rest
// bytes for the same verified content (separate domain object keys).
func assertReencrypted(t *testing.T, low, high *harness.Cluster) {
	t.Helper()
	lowObj := firstObjectFile(t, low.NodeInfo(0).Dir)
	highObj := firstObjectFile(t, high.NodeInfo(0).Dir)
	lowRaw, err := os.ReadFile(lowObj)
	if err != nil {
		t.Fatalf("read %s: %v", lowObj, err)
	}
	highRaw, err := os.ReadFile(highObj)
	if err != nil {
		t.Fatalf("read %s: %v", highObj, err)
	}
	if bytes.Equal(lowRaw, highRaw) {
		t.Fatal("high object bytes identical to low object bytes; expected re-encryption")
	}
}

// firstObjectFile is the first *.spfo object file under a node dir.
func firstObjectFile(t *testing.T, root string) string {
	t.Helper()
	var found string
	stack := []string{root}
	for len(stack) > 0 {
		dir := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
		for _, e := range entries {
			path := filepath.Join(dir, e.Name())
			if e.IsDir() {
				stack = append(stack, path)
			} else if found == "" && strings.HasSuffix(e.Name(), ".spfo") {
				found = path
			}
		}
	}
	if found == "" {
		t.Fatalf("no object file under %s", root)
	}
	return found
}

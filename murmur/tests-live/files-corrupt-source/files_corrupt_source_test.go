// Live scenario: fetching from a corrupt source fails verification.
//
// Go port of the Rust `files-corrupt-source` scenario
// (/home/marc/MURMUR/tests-live/files-corrupt-source/tests/files_corrupt_source.rs:
// TestCorruptSourceFetchFailsVerification). A victim object is
// published honestly, then its stored bytes are corrupted on the source
// node while stopped (same length, flipped chunk ciphertext). A peer
// fetch must then fail verification with no partial install, fail
// identically on retry, and leave honest objects fetching afterwards.
//
// There is no HTTP server, so uploads, downloads, metadata, and search
// use the direct Database file API; peer fetch is automatic on the
// configured fetch_interval (no explicit fetch call exists), so the
// fetcher is stopped while the victim is published and corrupted, then
// restarted. Verification failure surfaces as the "object
// authentication" error recorded in LastFetchError.
package filescorruptsource

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// objectKey is the file object key (Rust: 0x42..).
var objectKey = [32]byte{
	0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42,
	0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42,
	0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42,
	0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42,
}

const (
	// maxFileBytes is the file cap (Rust: 4 MiB).
	maxFileBytes = uint64(4 << 20)
	// fetchInterval is the peer fetch cadence (Rust: 100 ms).
	fetchInterval = 100 * time.Millisecond
	// poll is the convergence poll interval (Rust: 50 ms).
	poll = 50 * time.Millisecond
	// wait is the per-step metadata/fetch wait bound (Rust: 30 s).
	wait = 30 * time.Second
	// retryWindow is the retry-persistence pacing sleep (Rust: 500 ms).
	retryWindow = 500 * time.Millisecond
	// errObjectAuth is the verification-failure sentinel (Rust:
	// "object authentication": flipped chunk ciphertext fails AES-GCM
	// authentication during fetch verification).
	errObjectAuth = "object authentication"
)

// payload returns the deterministic payload unique to name (Rust:
// name-keyed bytes, no hash dependency).
func payload(name string, n int) []byte {
	nb := []byte(name)
	out := make([]byte, n)
	for i := range out {
		out[i] = byte((len(nb)*31 + int(nb[i%len(nb)]) + i*17) % 251)
	}
	return out
}

// envUsize reads a positive-integer env knob with fallback.
func envUsize(t *testing.T, name string, fallback int) int {
	t.Helper()
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

// lastFetchError returns the last background-fetch error recorded on
// node idx (the fetch gate; there is no fetch-failed counter).
func lastFetchError(cluster *harness.Cluster, idx int) *string {
	h := cluster.Node(idx).Replication()
	if h == nil {
		return nil
	}
	objects := h.Files()
	if objects == nil {
		return nil
	}
	return objects.LastFetchError()
}

// pollAuthError polls for a fetch-error sample containing the
// verification sentinel. The latch is single-slot last-writer-wins
// shared with benign transport flakes (clean QUIC closes surface as
// recorded errors even on success), so legs match ANY sample in the
// window, never a single sample: a benign flake landing between the
// wait and a one-shot read must not fail the leg.
func pollAuthError(t *testing.T, cluster *harness.Cluster, idx int, timeout time.Duration, what string) string {
	t.Helper()
	var sample string
	if ok := harness.WaitUntil(timeout, 10*time.Millisecond, func() bool {
		e := lastFetchError(cluster, idx)
		if e != nil && strings.Contains(*e, errObjectAuth) {
			sample = *e
			return true
		}
		return false
	}); !ok {
		t.Fatalf("%s (last error: %v)", what, strPtr(lastFetchError(cluster, idx)))
	}
	return sample
}

// findFile returns the first regular file under dir (recursive) whose
// base name matches want.
func findFile(t *testing.T, dir string, want func(string) bool) string {
	t.Helper()
	var hit string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || hit != "" {
			return nil
		}
		if info.IsDir() || !info.Mode().IsRegular() {
			return nil
		}
		if want(info.Name()) {
			hit = path
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return hit
}

// findObjectFile returns the published .spfo object file for digestHex.
func findObjectFile(t *testing.T, dir, digestHex string) string {
	t.Helper()
	return findFile(t, dir, func(base string) bool {
		return strings.HasSuffix(base, ".spfo") && strings.Contains(base, digestHex)
	})
}

// findPartFile returns any fetch-staging *.part file (successful fetches
// rename the .part into place, so any residue is a leak, regardless of
// digest).
func findPartFile(t *testing.T, dir string) string {
	t.Helper()
	return findFile(t, dir, func(base string) bool {
		return strings.HasSuffix(base, ".part")
	})
}

// checkDownload downloads name on node idx and checks bytes and size.
func checkDownload(t *testing.T, cluster *harness.Cluster, idx int, name string, want []byte) {
	t.Helper()
	var got bytes.Buffer
	meta, err := cluster.Node(idx).GetFile(name, &got)
	if err != nil {
		t.Fatalf("node%d download %s: %v", idx, name, err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("node%d %s bytes differ", idx, name)
	}
	if meta.Bytes != uint64(len(want)) {
		t.Fatalf("node%d %s size %d, want %d", idx, name, meta.Bytes, len(want))
	}
}

func TestCorruptSourceFetchFailsVerification(t *testing.T) {
	victimSize := envUsize(t, "MURMUR_FILES_CORRUPT_VICTIM_KB", 256) << 10

	maxBytes := maxFileBytes
	fetch := fetchInterval
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "files-corrupt-source",
		NumNodes: 2,
		Files: &harness.FilesOptions{
			Key:           objectKey,
			FetchInterval: &fetch,
			MaxFileBytes:  &maxBytes,
		},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("files-corrupt-source")
		}
		cluster.Finish(failed)
	}()

	// Positive control: honest upload replicates, fetches, verifies.
	ctrl := payload("ctrl/a.bin", 64<<10)
	st, err := cluster.Node(0).UploadFile("ctrl/a.bin", bytes.NewReader(ctrl))
	if err != nil {
		t.Fatalf("control upload: %v", err)
	}
	if st.Bytes != uint64(len(ctrl)) {
		t.Fatalf("control upload byte count %d, want %d", st.Bytes, len(ctrl))
	}
	if ok := harness.WaitUntil(wait, poll, func() bool {
		meta, err := cluster.Node(1).FileMetadata("ctrl/a.bin")
		return err == nil && meta != nil && meta.Digest == st.Digest
	}); !ok {
		t.Fatal("control metadata never arrived")
	}
	if ok := harness.WaitUntil(wait, poll, func() bool {
		available, err := cluster.Node(1).FileAvailable("ctrl/a.bin")
		return err == nil && available
	}); !ok {
		t.Fatal("control fetch never completed")
	}
	checkDownload(t, cluster, 1, "ctrl/a.bin", ctrl)
	t.Log("positive control: upload/replicate/fetch/verify works")

	// Fetch-error baseline: the clean control must record no
	// verification failure. (Go-only: benign transport flakes share
	// the single-slot latch, so clean fetches may record a close
	// error; the baseline asserts no AUTH error instead of none.)
	if err := lastFetchError(cluster, 1); err != nil && strings.Contains(*err, errObjectAuth) {
		t.Fatalf("clean control recorded a verification failure: %s", *err)
	}

	// Stop the fetcher so no fetch can start before the corruption lands
	// (the product fetches automatically on fetch_interval; there is no
	// disabled-worker mode).
	cluster.StopNode(1)

	// Publish the victim object honestly; metadata only (the fetcher is
	// stopped, so nothing can fetch yet).
	victim := payload("victim/blob.bin", victimSize)
	vst, err := cluster.Node(0).UploadFile("victim/blob.bin", bytes.NewReader(victim))
	if err != nil {
		t.Fatalf("victim upload: %v", err)
	}
	if vst.Bytes != uint64(len(victim)) {
		t.Fatalf("victim upload byte count %d, want %d", vst.Bytes, len(victim))
	}
	victimDigest := vst.Digest
	victimHex := fmt.Sprintf("%x", victimDigest)
	t.Logf("victim published: digest=%s size=%d", victimHex, len(victim))

	// Locate the source-side object file before stopping the node.
	srcObj := findObjectFile(t, cluster.NodeInfo(0).Dir, victimHex)
	if srcObj == "" {
		t.Fatalf("no .spfo object file for digest %s", victimHex)
	}
	before, err := os.ReadFile(srcObj)
	if err != nil {
		t.Fatalf("read source object: %v", err)
	}
	t.Logf("source object: %s (%d container bytes)", srcObj, len(before))
	if len(before) < 1024 {
		t.Fatalf("source object unexpectedly small (%d bytes); refusing to corrupt", len(before))
	}

	// Corrupt the published bytes while the source node is stopped
	// (honest disk-corruption simulation: same length, flipped bytes in
	// the middle of the container, i.e. inside chunk ciphertext).
	cluster.StopNode(0)
	flip := append([]byte(nil), before...)
	for i := 0; i < 16; i++ {
		off := len(flip)/2 + i
		flip[off] ^= 0xFF
	}
	if err := os.WriteFile(srcObj, flip, 0o600); err != nil {
		t.Fatalf("write corrupted object: %v", err)
	}
	after, err := os.ReadFile(srcObj)
	if err != nil {
		t.Fatalf("re-read corrupted object: %v", err)
	}
	if len(after) != len(before) {
		t.Fatal("corrupted object length changed")
	}
	t.Logf("flipped 16 bytes at container offset %d (length preserved)", len(flip)/2)

	// Restart both nodes and re-mesh (fresh ephemeral ports).
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart source: %v", err)
	}
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart fetcher: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}

	// Victim metadata must replicate honestly to the fetcher.
	if ok := harness.WaitUntil(wait, poll, func() bool {
		meta, err := cluster.Node(1).FileMetadata("victim/blob.bin")
		return err == nil && meta != nil && meta.Digest == victimDigest
	}); !ok {
		t.Fatal("victim metadata never arrived")
	}

	// The peer fetch must FAIL verification with the auth sentinel,
	// with no staging residue. Failed verification synchronously
	// discards the .part plus its .identity sidecar, so the joint
	// condition (no .part while the latch holds the auth failure)
	// proves fail-closed AND staging-discard off a single
	// verify-failure; it also excludes the pre-fetch initial state
	// (trivially no .part, latch not yet auth).
	// Go-only window: attempts take seconds here (Rust: ms), so the
	// joint watch spans 60 s to cover several verify-failures; the
	// poll is 10 ms to catch the ~100 ms post-removal gap before the
	// next tick starts a new attempt. Any-match on the latch: benign
	// flakes share it, so the leg passes on any auth sample.
	node1Dir := cluster.NodeInfo(1).Dir
	var fetchErr string
	joint := harness.WaitUntil(60*time.Second, 10*time.Millisecond, func() bool {
		e := lastFetchError(cluster, 1)
		if e == nil || !strings.Contains(*e, errObjectAuth) {
			return false
		}
		if findPartFile(t, node1Dir) != "" {
			return false
		}
		fetchErr = *e
		return true
	})
	if !joint {
		t.Fatalf("corrupt fetch never failed verification with staging discarded (last error: %v)",
			strPtr(lastFetchError(cluster, 1)))
	}
	t.Logf("corrupt fetch failed closed as required: %s", fetchErr)

	// No partial install on the fetching node.
	pst, err := cluster.Node(1).FileMetadata("victim/blob.bin")
	if err != nil {
		t.Fatalf("victim status: %v", err)
	}
	if pst == nil {
		t.Fatal("victim metadata wrongly altered by failed fetch")
	}
	if pst.Digest != victimDigest {
		t.Fatal("victim digest altered by failed fetch")
	}
	if available, _ := cluster.Node(1).FileAvailable("victim/blob.bin"); available {
		t.Fatal("victim reports Available after a failed-verification fetch (partial install)")
	}
	if _, err := cluster.Node(1).GetFile("victim/blob.bin", &bytes.Buffer{}); err == nil {
		t.Fatal("victim downloads after a failed-verification fetch (partial install)")
	}

	// No object file for the unverified digest: failed verification
	// never installs (checked right after the observed post-failure
	// gap above; a .spfo is never created on this path at any time).
	if hit := findObjectFile(t, node1Dir, victimHex); hit != "" {
		t.Fatalf("fetching node holds an object file for an unverified digest (partial install): %s", hit)
	}

	// No phantom metadata: the listing still shows the victim with its
	// original honest digest/size (corruption is bytes-only, metadata
	// untouched) on both nodes.
	for i := 0; i < cluster.Len(); i++ {
		list, err := cluster.Node(i).ListFiles()
		if err != nil {
			t.Fatalf("list files: %v", err)
		}
		found := -1
		for j := range list {
			if list[j].Name == "victim/blob.bin" {
				found = j
				break
			}
		}
		if found < 0 {
			t.Fatalf("node%d lost victim metadata after failed fetch", i+1)
		}
		f := list[found]
		if f.Digest != victimDigest || f.Bytes != uint64(len(victim)) || f.Deleted {
			t.Fatalf("node%d victim metadata altered", i+1)
		}
	}

	// The corruption is persistent, not transient: after more background
	// retry cycles the fetch still fails the same verification.
	// Any-match polling (see pollAuthError): benign flakes share the
	// latch, so the leg passes on any recurring auth sample while the
	// victim stays unavailable. Go-only window: 60 s to cover
	// several slow retry cycles (see the joint watch above).
	time.Sleep(retryWindow)
	again := pollAuthError(t, cluster, 1, 60*time.Second, "post-retry fetch never failed verification again")
	if available, _ := cluster.Node(1).FileAvailable("victim/blob.bin"); available {
		t.Fatal("victim became available across retries (partial install)")
	}
	t.Logf("retried fetch failed closed again: %s", again)

	// Honest objects must still fetch afterwards.
	honest := payload("honest/b.bin", 128<<10)
	hst, err := cluster.Node(0).UploadFile("honest/b.bin", bytes.NewReader(honest))
	if err != nil {
		t.Fatalf("honest upload after corruption: %v", err)
	}
	if ok := harness.WaitUntil(wait, poll, func() bool {
		meta, err := cluster.Node(1).FileMetadata("honest/b.bin")
		return err == nil && meta != nil && meta.Digest == hst.Digest
	}); !ok {
		t.Fatal("honest metadata after corruption never arrived")
	}
	if ok := harness.WaitUntil(wait, poll, func() bool {
		available, err := cluster.Node(1).FileAvailable("honest/b.bin")
		return err == nil && available
	}); !ok {
		t.Fatal("honest fetch after corruption never completed")
	}
	checkDownload(t, cluster, 1, "honest/b.bin", honest)
	checkDownload(t, cluster, 1, "ctrl/a.bin", ctrl)
	t.Log("corrupt-source proven: bad bytes fail verification, honest objects unaffected")

	failed = false
}

func strPtr(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

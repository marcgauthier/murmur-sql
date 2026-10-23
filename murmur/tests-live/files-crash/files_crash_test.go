// Live scenario: a two-node encrypted mesh proves file crash consistency.
// After the uploader is killed mid-upload and the fetcher mid-fetch, every
// listed object downloads byte-complete and digest-verified on both nodes,
// metadata converges exactly, and every on-disk payload is attributable to
// a known attempt.
//
// Go port of the Rust `files-crash` scenario
// (/home/marc/MURMUR/tests-live/files-crash/tests/files_crash.rs).
//
// There is no HTTP server in the product, so uploads, downloads, metadata,
// and search use the direct Database file API; peer fetch is automatic on
// the configured fetch_interval. Signals cannot target one in-process node,
// so the two kill legs are modeled in-process: the mid-upload kill is a
// byte stream that aborts partway (the product must drop the staged temp
// and fail the upload) followed by a close/reopen of the uploader, and the
// mid-fetch kill is a close/reopen of the fetcher while a `.part` transfer
// file is on disk.
package filescrash

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/replication"
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
	// fetchInterval is the peer fetch cadence (Rust: 100 ms; the product
	// has no explicit fetch call, so background fetch must stay enabled).
	fetchInterval = 100 * time.Millisecond
	// poll is the convergence poll interval (Rust: 50 ms).
	poll = 50 * time.Millisecond
	// wait bounds per-step metadata/fetch waits (Rust: 30 s).
	wait = 30 * time.Second
	// settleWait bounds the final metadata settle (Rust: 60 s).
	settleWait = 60 * time.Second
	// killWait bounds mid-fetch kill coordination (Rust: 15 s).
	killWait = 15 * time.Second
	// uploadBytes is the upload size in the uploader-crash rounds
	// (Rust: 1 MiB x8).
	uploadBytes = 1 << 20
	// sabotageBytes is the bytes delivered before the sabotaged upload
	// stream aborts (Rust: 256 KiB).
	sabotageBytes = 256 << 10
	// rounds is the crash rounds per leg (Rust: 3).
	rounds = 3
)

// attempt is one upload try: the digest is known only for acknowledged
// uploads (errored uploads must leave no debris at all).
type attempt struct {
	digest *[32]byte
	size   uint64
	ok     bool
}

// payload is the deterministic payload unique to name (Rust: name-keyed
// byte pattern, no hash dependency).
func payload(name string, n int) []byte {
	nb := []byte(name)
	out := make([]byte, n)
	for i := range out {
		out[i] = byte((len(nb)*31 + int(nb[i%len(nb)]) + i*17) % 251)
	}
	return out
}

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

// failAfter is a byte stream that aborts mid-upload: the in-process
// equivalent of SIGKILLing the uploader while its request body is still
// flowing.
type failAfter struct {
	inner     io.Reader
	remaining int
}

func (f *failAfter) Read(p []byte) (int, error) {
	if f.remaining == 0 {
		return 0, errors.New("simulated mid-upload crash")
	}
	want := len(p)
	if want > f.remaining {
		want = f.remaining
	}
	n, err := f.inner.Read(p[:want])
	f.remaining -= n
	return n, err
}

// parseObjectDigest parses `<hex64>.g<gen>.spfo` -> digest hex.
func parseObjectDigest(base string) (string, bool) {
	stem, ok := strings.CutSuffix(base, ".spfo")
	if !ok {
		return "", false
	}
	// rsplit: digest is the part before the LAST ".g".
	idx := strings.LastIndex(stem, ".g")
	if idx < 0 {
		return "", false
	}
	digest := stem[:idx]
	if len(digest) != 64 {
		return "", false
	}
	for i := 0; i < len(digest); i++ {
		c := digest[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return "", false
		}
	}
	return strings.ToLower(digest), true
}

// parsePartDigest parses `<source>-<digest64>.g<gen>.part` -> digest hex.
func parsePartDigest(base string) (string, bool) {
	stem, ok := strings.CutSuffix(base, ".part")
	if !ok {
		return "", false
	}
	_, rest, ok := strings.Cut(stem, "-")
	if !ok {
		return "", false
	}
	idx := strings.LastIndex(rest, ".g")
	if idx < 0 {
		return "", false
	}
	digest := rest[:idx]
	if len(digest) != 64 {
		return "", false
	}
	for i := 0; i < len(digest); i++ {
		c := digest[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return "", false
		}
	}
	return strings.ToLower(digest), true
}

// collectPayloads gathers on-disk payloads under a node dir: published
// objects (`.spfo`), upload staging (`.upload-*`/`.rotate-*`), and fetch
// staging (`*.part`).
func collectPayloads(t *testing.T, dir string) (spfo, stage, part []string) {
	t.Helper()
	stack := []string{dir}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		entries, err := os.ReadDir(cur)
		if err != nil {
			continue
		}
		for _, e := range entries {
			path := filepath.Join(cur, e.Name())
			if e.IsDir() {
				stack = append(stack, path)
				continue
			}
			if e.Type().IsRegular() || e.Type() == 0 {
				base := e.Name()
				switch {
				case strings.HasSuffix(base, ".spfo"):
					spfo = append(spfo, base)
				case strings.HasPrefix(base, ".upload-") || strings.HasPrefix(base, ".rotate-"):
					stage = append(stage, base)
				case strings.HasSuffix(base, ".part"):
					part = append(part, base)
				}
			}
		}
	}
	return spfo, stage, part
}

// stagingParts lists staged fetch transfers on a node (the in-flight
// signal for the mid-fetch kill leg).
func stagingParts(staging string) []string {
	entries, err := os.ReadDir(staging)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".part") {
			out = append(out, filepath.Join(staging, e.Name()))
		}
	}
	return out
}

// stopWhileFetching closes idx once a fetch transfer is in flight for
// name. It reports (killed, tooSlow): tooSlow means the fetch completed
// first and the round proves nothing (Rust: stop_while_fetching).
func stopWhileFetching(t *testing.T, cluster *harness.Cluster, idx int, name string, timeout time.Duration) (killed, tooSlow bool) {
	t.Helper()
	staging := filepath.Join(cluster.NodeInfo(idx).Dir, "objects", "staging")
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		if available, err := cluster.Node(idx).FileAvailable(name); err == nil && available {
			return false, true
		}
		if inFlight := stagingParts(staging); len(inFlight) > 0 {
			t.Logf("node%d: stopping fetcher with %d staged transfer(s)", idx+1, len(inFlight))
			cluster.StopNode(idx)
			return true, false
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false, false
}

func restartNode(t *testing.T, cluster *harness.Cluster, idx int) {
	t.Helper()
	if err := cluster.StartNode(idx); err != nil {
		t.Fatalf("restart node%d: %v", idx+1, err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
}

func metadataCanonical(list []replication.FileMetadata) string {
	var sb strings.Builder
	// ListFiles already sorts by name; render the canonical listing.
	for _, m := range list {
		fmt.Fprintf(&sb, "%s|%s|%d|%v;", m.Name, hex.EncodeToString(m.Digest[:]), m.Bytes, m.Deleted)
	}
	return sb.String()
}

// waitMetadataSettled polls until both nodes list identical metadata and
// every acknowledged upload is present everywhere (Rust:
// wait_metadata_settled).
func waitMetadataSettled(t *testing.T, cluster *harness.Cluster, attempted map[string]attempt) {
	t.Helper()
	end := time.Now().Add(settleWait)
	for {
		var lists [][]replication.FileMetadata
		ok := true
		for idx := 0; idx < cluster.Len(); idx++ {
			l, err := cluster.Node(idx).ListFiles()
			if err != nil {
				ok = false
				break
			}
			lists = append(lists, l)
		}
		if ok && len(lists) == 2 {
			settled := metadataCanonical(lists[0]) == metadataCanonical(lists[1])
			if settled {
				for _, a := range attempted {
					if !a.ok {
						continue
					}
					found := false
					for _, m := range lists[0] {
						if m.Digest == *a.digest && m.Bytes == a.size {
							found = true
							break
						}
					}
					if !found {
						settled = false
						break
					}
				}
			}
			if settled {
				return
			}
		}
		if !time.Now().Before(end) {
			for idx := 0; idx < cluster.Len(); idx++ {
				n := 0
				if l, err := cluster.Node(idx).ListFiles(); err == nil {
					n = len(l)
				}
				t.Logf("node%d at timeout: %d files", idx, n)
			}
			t.Fatal("file metadata did not settle identically on both nodes")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// verifyAllObjects downloads every listed object on both nodes and requires
// byte-complete, digest-verified content; anything listed must have been
// attempted (no phantoms) and every acked upload must be listed (no lost
// writes). (Rust: verify_all_objects.)
func verifyAllObjects(t *testing.T, cluster *harness.Cluster, attempted map[string]attempt, fetchAttempts *int) {
	t.Helper()
	for idx := 0; idx < cluster.Len(); idx++ {
		list, err := cluster.Node(idx).ListFiles()
		if err != nil {
			t.Fatalf("list files: %v", err)
		}
		for _, f := range list {
			a, ok := attempted[f.Name]
			if !ok {
				t.Fatalf("node%d lists phantom object %q", idx, f.Name)
			}
			if f.Deleted {
				t.Fatalf("node%d object %q unexpectedly deleted", idx, f.Name)
			}
			if a.digest == nil {
				t.Fatalf("node%d %q listed but its upload errored", idx, f.Name)
			}
			if f.Digest != *a.digest || f.Bytes != a.size {
				t.Fatalf("node%d %q metadata digest=%s size=%d, want %s/%d",
					idx, f.Name, hex.EncodeToString(f.Digest[:]), f.Bytes,
					hex.EncodeToString(a.digest[:]), a.size)
			}
			if available, err := cluster.Node(idx).FileAvailable(f.Name); err != nil || !available {
				*fetchAttempts++
				fetched := harness.WaitUntil(wait, poll, func() bool {
					available, err := cluster.Node(idx).FileAvailable(f.Name)
					return err == nil && available
				})
				if !fetched {
					t.Fatalf("node%d fetch %q never completed", idx, f.Name)
				}
			}
			var got bytes.Buffer
			meta, err := cluster.Node(idx).GetFile(f.Name, &got)
			if err != nil {
				t.Fatalf("node%d %q listed but not downloadable (partial?): %v", idx, f.Name, err)
			}
			if uint64(got.Len()) != a.size {
				t.Fatalf("node%d %q length is partial", idx, f.Name)
			}
			if !bytes.Equal(got.Bytes(), payload(f.Name, int(a.size))) {
				t.Fatalf("node%d %q bytes differ (partial/corrupt)", idx, f.Name)
			}
			if meta.Digest != *a.digest {
				t.Fatalf("node%d %q digest mismatch", idx, f.Name)
			}
		}
		for name, a := range attempted {
			if !a.ok {
				continue
			}
			st, err := cluster.Node(idx).FileMetadata(name)
			if err != nil {
				t.Fatalf("node%d stat %q: %v", idx, name, err)
			}
			if st == nil {
				t.Fatalf("node%d lost acked upload %q", idx, name)
			}
		}
	}
	l0, err := cluster.Node(0).ListFiles()
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	l1, err := cluster.Node(1).ListFiles()
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	t.Logf("all %d listed objects byte-complete on both nodes; metadata digests equal", len(l0))
	if len(l0) != len(l1) {
		t.Fatalf("object counts differ: %d vs %d", len(l0), len(l1))
	}
}

// verifyInventory walks each node dir and requires every object payload
// (`.spfo`), upload stage file, and fetch stage file (`*.part`) to be
// attributable to a known upload/fetch attempt and bounded in count (Rust:
// verify_inventory).
func verifyInventory(t *testing.T, cluster *harness.Cluster, attempted map[string]attempt, fetchAttempts int) {
	t.Helper()
	known := make(map[string]bool)
	errored := 0
	for _, a := range attempted {
		if a.ok && a.digest != nil {
			known[hex.EncodeToString(a.digest[:])] = true
		} else {
			errored++
		}
	}
	for idx := 0; idx < cluster.Len(); idx++ {
		spfo, stage, part := collectPayloads(t, cluster.NodeInfo(idx).Dir)
		meta, err := cluster.Node(idx).ListFiles()
		if err != nil {
			t.Fatalf("list files: %v", err)
		}
		referenced := make(map[string]bool)
		for _, m := range meta {
			referenced[hex.EncodeToString(m.Digest[:])] = true
		}
		orphans := 0
		for _, base := range spfo {
			d, ok := parseObjectDigest(base)
			if !ok {
				t.Fatalf("node%d object file %s has a bad name", idx, base)
			}
			if referenced[d] {
				continue
			}
			if known[d] {
				orphans++
				continue
			}
			t.Fatalf("node%d object file %s unattributable to any upload", idx, base)
		}
		if orphans > errored {
			t.Fatalf("node%d has %d unreferenced objects from %d errored uploads", idx, orphans, errored)
		}
		if idx == 1 && len(stage) != 0 {
			t.Fatalf("node 1 never uploaded yet holds %d stage files", len(stage))
		}
		if len(stage) > errored {
			t.Fatalf("node%d holds %d stage files from %d errored uploads", idx, len(stage), errored)
		}
		if idx == 0 && len(part) != 0 {
			t.Fatalf("node 0 never fetched yet holds %d part files", len(part))
		}
		for _, base := range part {
			d, ok := parsePartDigest(base)
			if !ok {
				t.Fatalf("node%d stage file %s has a bad name", idx, base)
			}
			if !known[d] {
				t.Fatalf("node%d stage file %s unattributable to any upload", idx, base)
			}
		}
		if len(part) > fetchAttempts+2 {
			t.Fatalf("node%d holds %d part files from %d fetch attempts", idx, len(part), fetchAttempts)
		}
		t.Logf("node%d inventory: %d objects (%d crash orphans), %d stage, %d part - all attributable",
			idx, len(spfo), orphans, len(stage), len(part))
	}
}

func body(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	bigMB := envInt("MURMUR_FILES_CRASH_BIG_MB", 3)
	attempted := make(map[string]attempt)
	fetchAttempts := 0

	// Positive control: honest upload replicates, fetches, verifies.
	ctrl := payload("ctrl/a.bin", 64<<10)
	st, err := cluster.Node(0).UploadFile("ctrl/a.bin", bytes.NewReader(ctrl))
	if err != nil {
		t.Fatalf("control upload: %v", err)
	}
	if st.Bytes != uint64(len(ctrl)) {
		t.Fatalf("control upload byte count %d, want %d", st.Bytes, len(ctrl))
	}
	ctrlDigest := st.Digest
	attempted["ctrl/a.bin"] = attempt{digest: &ctrlDigest, size: uint64(len(ctrl)), ok: true}
	arrived := harness.WaitUntil(wait, poll, func() bool {
		m, err := cluster.Node(1).FileMetadata("ctrl/a.bin")
		return err == nil && m != nil && m.Digest == st.Digest
	})
	if !arrived {
		t.Fatal("control metadata never arrived")
	}
	fetchAttempts++
	fetched := harness.WaitUntil(wait, poll, func() bool {
		available, err := cluster.Node(1).FileAvailable("ctrl/a.bin")
		return err == nil && available
	})
	if !fetched {
		t.Fatal("control fetch never completed")
	}
	var got bytes.Buffer
	if _, err := cluster.Node(1).GetFile("ctrl/a.bin", &got); err != nil {
		t.Fatalf("control download: %v", err)
	}
	if !bytes.Equal(got.Bytes(), ctrl) {
		t.Fatal("control download bytes differ")
	}
	t.Log("positive control: upload/replicate/fetch/verify works")
	// Baseline inventory before any crash: the clean path must leave
	// zero unattributable payloads (the before half of before/after).
	verifyInventory(t, cluster, attempted, fetchAttempts)

	// Phase 1: kill the uploader mid-upload (stream abort coordinated).
	uploaderKilled := false
	for round := 1; round <= rounds && !uploaderKilled; round++ {
		errored := 0
		for i := 0; i < 8; i++ {
			name := fmt.Sprintf("crash/u-%d-%d.bin", round, i)
			data := payload(name, uploadBytes)
			if i == 3 {
				// The kill lands here: the byte stream aborts partway
				// through this upload.
				sabotaged := &failAfter{inner: bytes.NewReader(data), remaining: sabotageBytes}
				if _, err := cluster.Node(0).UploadFile(name, sabotaged); err == nil {
					t.Fatalf("round %d: sabotaged upload unexpectedly succeeded", round)
				} else {
					errored++
					attempted[name] = attempt{digest: nil, size: uint64(len(data)), ok: false}
				}
				continue
			}
			up, err := cluster.Node(0).UploadFile(name, bytes.NewReader(data))
			if err != nil {
				t.Fatalf("round %d upload %s: %v", round, name, err)
			}
			if up.Bytes != uint64(len(data)) {
				t.Fatalf("round %d upload %s byte count %d", round, name, up.Bytes)
			}
			d := up.Digest
			attempted[name] = attempt{digest: &d, size: uint64(len(data)), ok: true}
		}
		// Crash aftermath: close + reopen the uploader, re-mesh.
		t.Logf("round %d: restarting uploader with %d errored upload(s)", round, errored)
		cluster.StopNode(0)
		restartNode(t, cluster, 0)
		if errored == 0 {
			t.Logf("round %d: kill landed between uploads; retrying", round)
			continue
		}
		uploaderKilled = true
		t.Logf("round %d: uploader killed mid-upload (%d errored uploads)", round, errored)
	}
	if !uploaderKilled {
		t.Fatalf("no valid mid-upload kill in %d rounds", rounds)
	}

	// Phase 2: kill the fetcher mid-fetch (staged-transfer coordinated).
	fetcherKilled := false
	for round := 1; round <= rounds && !fetcherKilled; round++ {
		name := fmt.Sprintf("crash/f-%d.bin", round)
		data := payload(name, bigMB<<20)
		up, err := cluster.Node(0).UploadFile(name, bytes.NewReader(data))
		if err != nil {
			t.Fatalf("round %d big upload: %v", round, err)
		}
		d := up.Digest
		attempted[name] = attempt{digest: &d, size: uint64(len(data)), ok: true}
		arrived := harness.WaitUntil(wait, poll, func() bool {
			m, err := cluster.Node(1).FileMetadata(name)
			return err == nil && m != nil && m.Digest == up.Digest
		})
		if !arrived {
			t.Fatalf("round %d: big metadata never arrived", round)
		}
		fetchAttempts++
		killed, tooSlow := stopWhileFetching(t, cluster, 1, name, killWait)
		if tooSlow {
			t.Logf("round %d: fetch completed before the kill; retrying", round)
			continue
		}
		if !killed {
			t.Fatalf("round %d: fetch never went in flight", round)
		}
		restartNode(t, cluster, 1)
		fetcherKilled = true
		t.Logf("round %d: fetcher killed mid-fetch of %s", round, name)
		// The interrupted fetch must complete cleanly after restart:
		// staged partials never poison the verified install.
		fetchAttempts++
		fetched := harness.WaitUntil(wait, poll, func() bool {
			available, err := cluster.Node(1).FileAvailable(name)
			return err == nil && available
		})
		if !fetched {
			t.Fatal("post-crash fetch never completed")
		}
		var post bytes.Buffer
		meta, err := cluster.Node(1).GetFile(name, &post)
		if err != nil {
			t.Fatalf("post-crash download: %v", err)
		}
		if meta.Digest != up.Digest {
			t.Fatal("post-crash digest mismatch")
		}
		if !bytes.Equal(post.Bytes(), data) {
			t.Fatal("post-crash bytes differ")
		}
	}
	if !fetcherKilled {
		t.Fatalf("no valid mid-fetch kill in %d rounds", rounds)
	}

	// Phase 3: full recovery proof.
	waitMetadataSettled(t, cluster, attempted)
	verifyAllObjects(t, cluster, attempted, &fetchAttempts)
	verifyInventory(t, cluster, attempted, fetchAttempts)
	t.Log("files-crash proven: no partials, no phantoms, inventory attributable")
}

func TestFileCrashLeavesNoPartialOrPhantom(t *testing.T) {
	fetch := fetchInterval
	maxBytes := maxFileBytes
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "files-crash",
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
			cluster.DumpForensics("files-crash")
		}
		cluster.Finish(failed)
	}()
	body(t, cluster)
	failed = false
}

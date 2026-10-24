// Live scenario: a peer fetch of a corrupted source object must fail
// verification with no partial install.
//
// Go port of tests/files_corrupt_source.rs using tests-live/goharness.
// Identical behavior: a victim object is published honestly, then its
// stored bytes are corrupted on the source node while stopped (same
// length, flipped chunk ciphertext). A peer fetch must then fail
// verification with no partial install, fail identically on retry, and
// leave honest objects fetching afterwards. Verification failure is
// proven behaviorally: the victim never becomes locally available while
// its metadata replicates honestly, downloads fail, and no object or
// staging residue lands.
package filescorruptsource

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// objectKey is the file object key (Rust: OBJECT_KEY 0x42..).
var objectKey = [32]byte{
	0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42,
	0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42,
	0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42,
	0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42,
}

const (
	// maxFileBytes is the file cap (4 MiB).
	maxFileBytes uint64 = 4 << 20
	// fetchInterval is the peer fetch cadence.
	fetchInterval = 100 * time.Millisecond
	// appendChunk is the upload wire chunk size (server cap: 1 MiB).
	appendChunk = 256 << 10
	// downloadChunk is the download wire chunk size (server cap: 1 MiB).
	downloadChunk = 256 << 10
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
	// wait bounds each metadata/fetch wait (30 s).
	wait = 30 * time.Second
	// quietWait bounds the quiescent-staging wait (5 s).
	quietWait = 5 * time.Second
	// retryWindow is the retry-persistence observation window (500 ms).
	retryWindow = 500 * time.Millisecond
)

// fileMeta is one file-metadata row (MURMUR FILE NEXT columns: name,
// size, modified_ms, sha256, local).
type fileMeta struct {
	name      string
	size      uint64
	digestHex string
	local     bool
}

// payload is the deterministic payload unique to name.
func payload(name string, n int) []byte {
	nb := []byte(name)
	out := make([]byte, n)
	for i := range out {
		out[i] = byte((len(nb)*31 + int(nb[i%len(nb)]) + i*17) % 251)
	}
	return out
}

func envUsize(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

func sqlQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func parseBool(value string) (bool, error) {
	switch strings.ToLower(value) {
	case "t", "true", "1":
		return true, nil
	case "f", "false", "0":
		return false, nil
	default:
		return false, fmt.Errorf("bad boolean %q", value)
	}
}

func parseMeta(row []string, idx int) (fileMeta, error) {
	var zero fileMeta
	if len(row) != 5 {
		return zero, fmt.Errorf("node%d: metadata row has %d columns, want 5", idx+1, len(row))
	}
	size, err := strconv.ParseUint(row[1], 10, 64)
	if err != nil {
		return zero, fmt.Errorf("node%d: bad size %q: %w", idx+1, row[1], err)
	}
	local, err := parseBool(row[4])
	if err != nil {
		return zero, fmt.Errorf("node%d: %w", idx+1, err)
	}
	return fileMeta{name: row[0], size: size, digestHex: row[3], local: local}, nil
}

// fileUpload uploads data under name on node idx (BEGIN, chunked APPEND
// over the extended protocol, FINISH). Returns the published metadata.
func fileUpload(cluster *goharness.Cluster, idx int, name string, data []byte) (fileMeta, error) {
	var zero fileMeta
	node := cluster.Node(idx)
	rows, err := node.Simple("MURMUR FILE UPLOAD BEGIN " + sqlQuote(name))
	if err != nil {
		return zero, fmt.Errorf("node%d UPLOAD BEGIN %q: %w", idx+1, name, err)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return zero, fmt.Errorf("node%d UPLOAD BEGIN %q: no handle", idx+1, name)
	}
	upload := rows[0][0]
	chunk := appendChunk
	if chunk < 1 {
		chunk = 1
	}
	for off := 0; off < len(data); off += chunk {
		end := off + chunk
		if end > len(data) {
			end = len(data)
		}
		if _, err := node.Execute("MURMUR FILE UPLOAD APPEND $1 $2", upload, data[off:end]); err != nil {
			return zero, fmt.Errorf("node%d UPLOAD APPEND %q: %w", idx+1, name, err)
		}
	}
	rows, err = node.Simple("MURMUR FILE UPLOAD FINISH " + sqlQuote(upload))
	if err != nil {
		return zero, fmt.Errorf("node%d UPLOAD FINISH %q: %w", idx+1, name, err)
	}
	if len(rows) == 0 {
		return zero, fmt.Errorf("node%d UPLOAD FINISH %q: no metadata", idx+1, name)
	}
	return parseMeta(rows[0], idx)
}

// fileDownload downloads the full verified bytes of name from node idx.
func fileDownload(cluster *goharness.Cluster, idx int, name string) ([]byte, error) {
	node := cluster.Node(idx)
	rows, err := node.Simple("MURMUR FILE DOWNLOAD " + sqlQuote(name))
	if err != nil {
		return nil, fmt.Errorf("node%d DOWNLOAD %q: %w", idx+1, name, err)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return nil, fmt.Errorf("node%d DOWNLOAD %q: no handle", idx+1, name)
	}
	handle := rows[0][0]
	var out []byte
	for {
		page, err := node.Query(fmt.Sprintf("MURMUR FILE DOWNLOAD NEXT $1 LIMIT %d", downloadChunk), handle)
		if err != nil {
			return nil, fmt.Errorf("node%d DOWNLOAD NEXT %q: %w", idx+1, name, err)
		}
		if len(page) == 0 {
			return nil, fmt.Errorf("node%d DOWNLOAD NEXT %q: empty page", idx+1, name)
		}
		chunk, err := page[0].Bytes(2)
		if err != nil {
			return nil, fmt.Errorf("node%d DOWNLOAD NEXT %q: bad chunk: %w", idx+1, name, err)
		}
		eof, err := page[0].Bool(3)
		if err != nil {
			return nil, fmt.Errorf("node%d DOWNLOAD NEXT %q: bad eof: %w", idx+1, name, err)
		}
		out = append(out, chunk...)
		if eof {
			break
		}
	}
	if _, err := node.Simple("MURMUR FILE CLOSE " + sqlQuote(handle)); err != nil {
		return nil, fmt.Errorf("node%d CLOSE download %q: %w", idx+1, name, err)
	}
	return out, nil
}

// fileList returns the full metadata listing on node idx.
func fileList(cluster *goharness.Cluster, idx int) ([]fileMeta, error) {
	node := cluster.Node(idx)
	rows, err := node.Simple("MURMUR FILE LIST")
	if err != nil {
		return nil, fmt.Errorf("node%d FILE LIST: %w", idx+1, err)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return nil, fmt.Errorf("node%d FILE LIST: no cursor", idx+1)
	}
	cursor := rows[0][0]
	var out []fileMeta
	for {
		page, err := node.Simple("MURMUR FILE NEXT " + sqlQuote(cursor) + " LIMIT 256")
		if err != nil {
			return nil, fmt.Errorf("node%d FILE NEXT: %w", idx+1, err)
		}
		if len(page) == 0 {
			break
		}
		for _, row := range page {
			meta, err := parseMeta(row, idx)
			if err != nil {
				return nil, err
			}
			out = append(out, meta)
		}
	}
	if _, err := node.Simple("MURMUR FILE CLOSE " + sqlQuote(cursor)); err != nil {
		return nil, fmt.Errorf("node%d CLOSE cursor: %w", idx+1, err)
	}
	return out, nil
}

// fileMetadata returns metadata for exactly name on node idx (nil when
// unlisted).
func fileMetadata(cluster *goharness.Cluster, idx int, name string) (*fileMeta, error) {
	list, err := fileList(cluster, idx)
	if err != nil {
		return nil, err
	}
	for i := range list {
		if list[i].name == name {
			return &list[i], nil
		}
	}
	return nil, nil
}

// findFile returns the first regular file under dir (recursive) whose
// name matches want.
func findFile(dir string, want func(string) bool) (string, bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", false
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if entry.IsDir() {
			if hit, ok := findFile(path, want); ok {
				return hit, true
			}
			continue
		}
		if !entry.Type().IsRegular() {
			continue
		}
		if want(entry.Name()) {
			return path, true
		}
	}
	return "", false
}

// findObjectFile locates the published .spfo object file for digestHex.
func findObjectFile(dir, digestHex string) (string, bool) {
	return findFile(dir, func(base string) bool {
		return strings.HasSuffix(base, ".spfo") && strings.Contains(base, digestHex)
	})
}

// findPartFile locates any fetch-staging *.part file.
func findPartFile(dir string) (string, bool) {
	return findFile(dir, func(base string) bool { return strings.HasSuffix(base, ".part") })
}

func checkDownload(cluster *goharness.Cluster, idx int, name string, want []byte) error {
	got, err := fileDownload(cluster, idx, name)
	if err != nil {
		return fmt.Errorf("node%d download %s: %w", idx+1, name, err)
	}
	if !equalBytes(got, want) {
		return fmt.Errorf("node%d %s bytes differ", idx+1, name)
	}
	meta, err := fileMetadata(cluster, idx, name)
	if err != nil {
		return err
	}
	if meta == nil {
		return fmt.Errorf("node%d %s metadata missing", idx+1, name)
	}
	if meta.size != uint64(len(want)) {
		return fmt.Errorf("node%d %s size %d", idx+1, name, meta.size)
	}
	return nil
}

// waitMetadata waits until node idx carries metadata for name with the
// expected digest.
func waitMetadata(cluster *goharness.Cluster, idx int, name, wantDigest, what string) error {
	deadline := time.Now().Add(wait)
	for {
		meta, err := fileMetadata(cluster, idx, name)
		if err != nil {
			return err
		}
		if meta != nil && meta.digestHex == wantDigest {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%s: metadata never arrived", what)
		}
		time.Sleep(poll)
	}
}

// waitAvailable waits until node idx holds the object locally.
func waitAvailable(cluster *goharness.Cluster, idx int, name, what string) error {
	deadline := time.Now().Add(wait)
	for {
		meta, err := fileMetadata(cluster, idx, name)
		if err != nil {
			return err
		}
		if meta != nil && meta.local {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%s: object never became available", what)
		}
		time.Sleep(poll)
	}
}

// assertNeverLocal asserts node idx never holds the object locally for a
// full wait (the corrupt leg: background fetch cycles every 100 ms, so a
// full window of unavailability with present metadata proves every
// attempt failed verification).
func assertNeverLocal(cluster *goharness.Cluster, idx int, name, what string) error {
	deadline := time.Now().Add(wait)
	for {
		meta, err := fileMetadata(cluster, idx, name)
		if err != nil {
			return err
		}
		if meta != nil && meta.local {
			return fmt.Errorf("%s: corrupt object became available (partial install)", what)
		}
		if !time.Now().Before(deadline) {
			return nil
		}
		time.Sleep(poll)
	}
}

func runBody(cluster *goharness.Cluster) error {
	victimSize := envUsize("MURMUR_FILES_CORRUPT_VICTIM_KB", 256) << 10

	// Positive control: honest upload replicates, fetches, verifies.
	ctrl := payload("ctrl/a.bin", 64<<10)
	st, err := fileUpload(cluster, 0, "ctrl/a.bin", ctrl)
	if err != nil {
		return fmt.Errorf("control upload: %w", err)
	}
	if st.size != uint64(len(ctrl)) {
		return fmt.Errorf("control upload byte count mismatch")
	}
	if err := waitMetadata(cluster, 1, "ctrl/a.bin", st.digestHex, "control metadata"); err != nil {
		return err
	}
	if err := waitAvailable(cluster, 1, "ctrl/a.bin", "control fetch"); err != nil {
		return err
	}
	if err := checkDownload(cluster, 1, "ctrl/a.bin", ctrl); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "positive control: upload/replicate/fetch/verify works")

	// Stop the fetcher so no fetch can start before the corruption lands.
	cluster.StopNode(1)

	// Publish the victim object honestly; metadata only (the fetcher is
	// stopped, so nothing can fetch yet).
	victim := payload("victim/blob.bin", victimSize)
	vst, err := fileUpload(cluster, 0, "victim/blob.bin", victim)
	if err != nil {
		return fmt.Errorf("victim upload: %w", err)
	}
	if vst.size != uint64(len(victim)) {
		return fmt.Errorf("victim upload byte count mismatch")
	}
	victimDigest := vst.digestHex
	fmt.Fprintf(os.Stderr, "victim published: digest=%s size=%d\n", victimDigest, len(victim))

	// Locate the source-side object file before stopping the node.
	srcObj, ok := findObjectFile(cluster.NodeInfo(0).Dir, victimDigest)
	if !ok {
		return fmt.Errorf("no .spfo object file for digest %s", victimDigest)
	}
	before, err := os.ReadFile(srcObj)
	if err != nil {
		return fmt.Errorf("read source object: %w", err)
	}
	fmt.Fprintf(os.Stderr, "source object: %s (%d container bytes)\n", srcObj, len(before))
	if len(before) < 1024 {
		return fmt.Errorf("source object unexpectedly small (%d bytes); refusing to corrupt", len(before))
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
	if err := os.WriteFile(srcObj, flip, 0o644); err != nil {
		return fmt.Errorf("write corrupted object: %w", err)
	}
	after, err := os.ReadFile(srcObj)
	if err != nil {
		return fmt.Errorf("re-read corrupted object: %w", err)
	}
	if len(after) != len(before) {
		return fmt.Errorf("corrupted object length changed")
	}
	fmt.Fprintf(os.Stderr, "flipped 16 bytes at container offset %d (length preserved)\n", len(flip)/2)

	// Restart both nodes and re-mesh (fresh ephemeral ports).
	if err := cluster.StartNode(0); err != nil {
		return fmt.Errorf("restart source: %w", err)
	}
	if err := cluster.StartNode(1); err != nil {
		return fmt.Errorf("restart fetcher: %w", err)
	}
	if err := cluster.Mesh(); err != nil {
		return fmt.Errorf("re-mesh after restart: %w", err)
	}

	// Victim metadata must replicate honestly to the fetcher.
	if err := waitMetadata(cluster, 1, "victim/blob.bin", victimDigest, "victim metadata"); err != nil {
		return err
	}

	// The peer fetch must FAIL verification with no partial install.
	if err := assertNeverLocal(cluster, 1, "victim/blob.bin", "corrupt fetch"); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "corrupt fetch failed closed as required: never available across a full window")

	// No partial install on the fetching node.
	pst, err := fileMetadata(cluster, 1, "victim/blob.bin")
	if err != nil {
		return err
	}
	if pst == nil {
		return fmt.Errorf("victim metadata wrongly altered by failed fetch")
	}
	if pst.digestHex != victimDigest {
		return fmt.Errorf("victim digest altered by failed fetch")
	}
	if pst.local {
		return fmt.Errorf("victim reports local after a failed-verification fetch (partial install)")
	}
	if _, err := fileDownload(cluster, 1, "victim/blob.bin"); err == nil {
		return fmt.Errorf("victim downloads after a failed-verification fetch (partial install)")
	}

	// Background retries run every fetch_interval, so wait for a
	// quiescent moment (no transfer in flight) and require no staging
	// residue and no object file for the unverified digest then.
	node1Dir := cluster.NodeInfo(1).Dir
	quietDeadline := time.Now().Add(quietWait)
	for {
		if _, ok := findPartFile(node1Dir); !ok {
			break
		}
		if !time.Now().Before(quietDeadline) {
			return fmt.Errorf("fetch staging never quiesced")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := findObjectFile(node1Dir, victimDigest); ok {
		return fmt.Errorf("fetching node holds an object file for an unverified digest (partial install)")
	}
	if _, ok := findPartFile(node1Dir); ok {
		return fmt.Errorf("fetching node holds a staging file after failed verification (staging not discarded)")
	}

	// No phantom metadata: the listing still shows the victim with its
	// original honest digest/size on both nodes.
	for i := 0; i < cluster.Len(); i++ {
		list, err := fileList(cluster, i)
		if err != nil {
			return err
		}
		var f *fileMeta
		for j := range list {
			if list[j].name == "victim/blob.bin" {
				f = &list[j]
			}
		}
		if f == nil {
			return fmt.Errorf("node%d lost victim metadata after failed fetch", i+1)
		}
		if !(f.digestHex == victimDigest && f.size == uint64(len(victim))) {
			return fmt.Errorf("node%d victim metadata altered: %s %s %d", i+1, f.name, f.digestHex, f.size)
		}
	}

	// The corruption is persistent, not transient: after more background
	// retry cycles the victim is still unavailable.
	time.Sleep(retryWindow)
	meta, err := fileMetadata(cluster, 1, "victim/blob.bin")
	if err != nil {
		return err
	}
	if meta != nil && meta.local {
		return fmt.Errorf("victim became available across retries (partial install)")
	}
	fmt.Fprintln(os.Stderr, "retried fetch failed closed again: still unavailable")

	// Honest objects must still fetch afterwards.
	honest := payload("honest/b.bin", 128<<10)
	hst, err := fileUpload(cluster, 0, "honest/b.bin", honest)
	if err != nil {
		return fmt.Errorf("honest upload after corruption: %w", err)
	}
	if err := waitMetadata(cluster, 1, "honest/b.bin", hst.digestHex, "honest metadata after corruption"); err != nil {
		return err
	}
	if err := waitAvailable(cluster, 1, "honest/b.bin", "honest fetch after corruption"); err != nil {
		return err
	}
	if err := checkDownload(cluster, 1, "honest/b.bin", honest); err != nil {
		return err
	}
	if err := checkDownload(cluster, 1, "ctrl/a.bin", ctrl); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "corrupt-source proven: bad bytes fail verification, honest objects unaffected")
	return nil
}

func TestCorruptSourceFetchFailsVerification(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	fetch := fetchInterval
	maxBytes := maxFileBytes
	opts := goharness.DefaultClusterOptions("files-corrupt-source")
	opts.NumNodes = 2
	opts.Files = &goharness.FilesOptions{Key: objectKey, FetchInterval: &fetch, MaxFileBytes: &maxBytes}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	result := runBody(cluster)
	failed := result != nil
	if result != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", result)
	}
	if failed {
		cluster.DumpForensics("files-corrupt-source")
	}
	cluster.Finish(failed)
	if failed {
		t.Fatal("scenario failed; artifacts retained under tests-live/failures/")
	}
}

func equalBytes(a, b []byte) bool {
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

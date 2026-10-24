// Live scenario: file crash consistency across mid-upload and
// mid-fetch kills.
//
// Go port of tests/files_crash.rs using tests-live/goharness. Identical
// behavior: after the uploader is killed mid-upload and the fetcher
// mid-fetch, every listed object downloads byte-complete and
// digest-verified on both nodes, metadata converges exactly, and every
// on-disk payload is attributable to a known attempt. Both kill legs are
// real process kills: the mid-upload kill lands between chunked UPLOAD
// APPENDs (the product must drop the staged upload and never publish
// it), and the mid-fetch kill stops the fetcher while a .part transfer
// file is on disk.
package filescrash

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
	// postCrashFetchWait is the post-crash fetch bound (90 s; the
	// product's recovery tail after zombie serve tasks hold the
	// uploader's serve semaphore until QUIC idle timeout).
	postCrashFetchWait = 90 * time.Second
	// settleWait is the final metadata-settle bound (60 s).
	settleWait = 60 * time.Second
	// killWait bounds mid-fetch kill coordination (15 s).
	killWait = 15 * time.Second
	// uploadBytes is the upload size in the uploader-crash rounds (1 MiB).
	uploadBytes = 1 << 20
	// sabotageBytes are appended before the mid-upload kill lands.
	sabotageBytes = 256 << 10
	// rounds is the crash rounds per leg (3).
	rounds = 3
)

// fileMeta is one file-metadata row (MURMUR FILE NEXT columns: name,
// size, modified_ms, sha256, local). Wire LIST shows nondeleted files
// only.
type fileMeta struct {
	name      string
	size      uint64
	digestHex string
	local     bool
}

// attempt is one upload try: the digest is known only for acknowledged
// uploads (errored uploads must leave no debris at all).
type attempt struct {
	digest *string
	size   uint64
	ok     bool
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

// uploadBegin opens an upload handle for name on node idx.
func uploadBegin(cluster *goharness.Cluster, idx int, name string) (string, error) {
	rows, err := cluster.Node(idx).Simple("MURMUR FILE UPLOAD BEGIN " + sqlQuote(name))
	if err != nil {
		return "", fmt.Errorf("node%d UPLOAD BEGIN %q: %w", idx+1, name, err)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return "", fmt.Errorf("node%d UPLOAD BEGIN %q: no handle", idx+1, name)
	}
	return rows[0][0], nil
}

// uploadAppend appends one chunk to an open upload handle (extended
// protocol, raw BYTEA).
func uploadAppend(cluster *goharness.Cluster, idx int, upload string, chunk []byte) error {
	if _, err := cluster.Node(idx).Execute("MURMUR FILE UPLOAD APPEND $1 $2", upload, chunk); err != nil {
		return fmt.Errorf("node%d UPLOAD APPEND: %w", idx+1, err)
	}
	return nil
}

// uploadFinish finishes an open upload handle; returns the published
// metadata.
func uploadFinish(cluster *goharness.Cluster, idx int, upload string) (fileMeta, error) {
	var zero fileMeta
	rows, err := cluster.Node(idx).Simple("MURMUR FILE UPLOAD FINISH " + sqlQuote(upload))
	if err != nil {
		return zero, fmt.Errorf("node%d UPLOAD FINISH: %w", idx+1, err)
	}
	if len(rows) == 0 {
		return zero, fmt.Errorf("node%d UPLOAD FINISH: no metadata", idx+1)
	}
	return parseMeta(rows[0], idx)
}

// fileUpload uploads data under name on node idx (BEGIN, chunked
// APPEND, FINISH). Returns the published metadata.
func fileUpload(cluster *goharness.Cluster, idx int, name string, data []byte) (fileMeta, error) {
	var zero fileMeta
	upload, err := uploadBegin(cluster, idx, name)
	if err != nil {
		return zero, err
	}
	chunk := appendChunk
	if chunk < 1 {
		chunk = 1
	}
	for off := 0; off < len(data); off += chunk {
		end := off + chunk
		if end > len(data) {
			end = len(data)
		}
		if err := uploadAppend(cluster, idx, upload, data[off:end]); err != nil {
			return zero, err
		}
	}
	return uploadFinish(cluster, idx, upload)
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

// parseObjectDigest parses `<hex64>.g<gen>.spfo` to its digest hex.
func parseObjectDigest(base string) (string, bool) {
	stem, ok := strings.CutSuffix(base, ".spfo")
	if !ok {
		return "", false
	}
	// rsplit_once(".g"): cut at the LAST ".g".
	idx := strings.LastIndex(stem, ".g")
	if idx < 0 {
		return "", false
	}
	if !isHex64(stem[:idx]) {
		return "", false
	}
	return stem[:idx], true
}

// parsePartDigest parses `<source>-<digest64>.g<gen>.part` to its digest
// hex.
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
	if !isHex64(rest[:idx]) {
		return "", false
	}
	return rest[:idx], true
}

func isHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

// collectPayloads collects on-disk payloads under a node dir: published
// objects (.spfo), upload staging (.upload-*/.rotate-*), and fetch
// staging (*.part).
func collectPayloads(dir string, spfo, stage, part *[]string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if entry.IsDir() {
			collectPayloads(path, spfo, stage, part)
			continue
		}
		if !entry.Type().IsRegular() {
			continue
		}
		base := entry.Name()
		switch {
		case strings.HasSuffix(base, ".spfo"):
			*spfo = append(*spfo, base)
		case strings.HasPrefix(base, ".upload-") || strings.HasPrefix(base, ".rotate-"):
			*stage = append(*stage, base)
		case strings.HasSuffix(base, ".part"):
			*part = append(*part, base)
		}
	}
}

// stagingParts lists staged fetch transfers in a staging dir (the
// in-flight signal for the mid-fetch kill leg).
func stagingParts(staging string) []string {
	var out []string
	entries, err := os.ReadDir(staging)
	if err != nil {
		return out
	}
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".part" {
			out = append(out, filepath.Join(staging, entry.Name()))
		}
	}
	return out
}

// killWhileFetching kills idx once a fetch transfer is in flight for
// name. It reports (killed, tooSlow): tooSlow means the fetch completed
// first and the round proves nothing.
func killWhileFetching(cluster *goharness.Cluster, idx int, name string, timeout time.Duration) (killed, tooSlow bool, err error) {
	staging := filepath.Join(cluster.NodeInfo(idx).Dir, "objects", "staging")
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		meta, err := fileMetadata(cluster, idx, name)
		if err != nil {
			return false, false, err
		}
		if meta != nil && meta.local {
			return false, true, nil
		}
		if inFlight := stagingParts(staging); len(inFlight) > 0 {
			fmt.Fprintf(os.Stderr, "node%d: killing fetcher with %d staged transfer(s)\n", idx+1, len(inFlight))
			cluster.KillNode(idx)
			return true, false, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false, false, nil
}

func restartNode(cluster *goharness.Cluster, idx int) error {
	if err := cluster.StartNode(idx); err != nil {
		return fmt.Errorf("restart node%d: %w", idx+1, err)
	}
	if err := cluster.Mesh(); err != nil {
		return fmt.Errorf("re-mesh after restart: %w", err)
	}
	return nil
}

func metadataCanonical(list []fileMeta) string {
	sorted := append([]fileMeta(nil), list...)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j].name < sorted[j-1].name; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	var sb strings.Builder
	for _, m := range sorted {
		fmt.Fprintf(&sb, "%s|%s|%d;", m.name, m.digestHex, m.size)
	}
	return sb.String()
}

// waitMetadataSettled polls until both nodes list identical metadata and
// every acknowledged upload is present everywhere.
func waitMetadataSettled(cluster *goharness.Cluster, attempted map[string]attempt) error {
	end := time.Now().Add(settleWait)
	for {
		var lists [][]fileMeta
		ok := true
		for idx := 0; idx < cluster.Len(); idx++ {
			l, err := fileList(cluster, idx)
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
						if a.digest != nil && m.digestHex == *a.digest && m.size == a.size {
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
				return nil
			}
		}
		if !time.Now().Before(end) {
			for idx := 0; idx < cluster.Len(); idx++ {
				n := 0
				if l, err := fileList(cluster, idx); err == nil {
					n = len(l)
				}
				fmt.Fprintf(os.Stderr, "node%d at timeout: %d files\n", idx, n)
			}
			return fmt.Errorf("file metadata did not settle identically on both nodes")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// verifyAllObjects downloads every listed object on both nodes and
// requires byte-complete, digest-verified content; anything listed must
// have been attempted (no phantoms) and every acked upload must be
// listed (no lost writes).
func verifyAllObjects(cluster *goharness.Cluster, attempted map[string]attempt, fetchAttempts *int) error {
	for idx := 0; idx < cluster.Len(); idx++ {
		list, err := fileList(cluster, idx)
		if err != nil {
			return err
		}
		for _, f := range list {
			a, ok := attempted[f.name]
			if !ok {
				return fmt.Errorf("node%d lists phantom object %q", idx, f.name)
			}
			if a.digest == nil {
				return fmt.Errorf("node%d %q listed but its upload errored", idx, f.name)
			}
			if !(f.digestHex == *a.digest && f.size == a.size) {
				return fmt.Errorf("node%d %q metadata digest=%s size=%d, want %s/%d",
					idx, f.name, f.digestHex, f.size, *a.digest, a.size)
			}
			meta, err := fileMetadata(cluster, idx, f.name)
			if err != nil {
				return err
			}
			if meta == nil || !meta.local {
				*fetchAttempts++
				deadline := time.Now().Add(wait)
				for {
					fetched, err := fileMetadata(cluster, idx, f.name)
					if err != nil {
						return err
					}
					if fetched != nil && fetched.local {
						break
					}
					if !time.Now().Before(deadline) {
						return fmt.Errorf("node%d fetch %q never completed", idx, f.name)
					}
					time.Sleep(poll)
				}
			}
			got, err := fileDownload(cluster, idx, f.name)
			if err != nil {
				return fmt.Errorf("node%d %q listed but not downloadable (partial?): %w", idx, f.name, err)
			}
			if uint64(len(got)) != a.size {
				return fmt.Errorf("node%d %q length is partial", idx, f.name)
			}
			if !equalBytes(got, payload(f.name, int(a.size))) {
				return fmt.Errorf("node%d %q bytes differ (partial/corrupt)", idx, f.name)
			}
		}
		for name, a := range attempted {
			if !a.ok {
				continue
			}
			st, err := fileMetadata(cluster, idx, name)
			if err != nil {
				return err
			}
			if st == nil {
				return fmt.Errorf("node%d lost acked upload %q", idx, name)
			}
		}
	}
	l0, err := fileList(cluster, 0)
	if err != nil {
		return err
	}
	l1, err := fileList(cluster, 1)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "all %d listed objects byte-complete on both nodes; metadata digests equal\n", len(l0))
	if len(l0) != len(l1) {
		return fmt.Errorf("object counts differ")
	}
	return nil
}

// verifyInventory walks each node dir and requires every object payload
// (.spfo), upload stage file, and fetch stage file (*.part) to be
// attributable to a known upload/fetch attempt and bounded in count.
func verifyInventory(cluster *goharness.Cluster, attempted map[string]attempt, fetchAttempts int) error {
	known := map[string]bool{}
	errored := 0
	for _, a := range attempted {
		if a.ok && a.digest != nil {
			known[*a.digest] = true
		} else {
			errored++
		}
	}
	// Note: Rust counts (ok=true, digest=None) as errored too; the match
	// arm `(true, Some(d))` inserts, everything else increments errored.
	// The loop above matches: only ok-with-digest inserts.
	for idx := 0; idx < cluster.Len(); idx++ {
		var spfo, stage, part []string
		collectPayloads(cluster.NodeInfo(idx).Dir, &spfo, &stage, &part)
		meta, err := fileList(cluster, idx)
		if err != nil {
			return err
		}
		referenced := map[string]bool{}
		for _, m := range meta {
			referenced[m.digestHex] = true
		}
		orphans := 0
		for _, base := range spfo {
			digest, ok := parseObjectDigest(base)
			if !ok {
				return fmt.Errorf("node%d object file %s has a bad name", idx, base)
			}
			if referenced[digest] {
				continue
			}
			if known[digest] {
				orphans++
				continue
			}
			return fmt.Errorf("node%d object file %s unattributable to any upload", idx, base)
		}
		if orphans > errored {
			return fmt.Errorf("node%d has %d unreferenced objects from %d errored uploads", idx, orphans, errored)
		}
		if idx == 1 && len(stage) != 0 {
			return fmt.Errorf("node 1 never uploaded yet holds %d stage files", len(stage))
		}
		if len(stage) > errored {
			return fmt.Errorf("node%d holds %d stage files from %d errored uploads", idx, len(stage), errored)
		}
		if idx == 0 && len(part) != 0 {
			return fmt.Errorf("node 0 never fetched yet holds %d part files", len(part))
		}
		for _, base := range part {
			digest, ok := parsePartDigest(base)
			if !ok {
				return fmt.Errorf("node%d stage file %s has a bad name", idx, base)
			}
			if !known[digest] {
				return fmt.Errorf("node%d stage file %s unattributable to any upload", idx, base)
			}
		}
		if len(part) > fetchAttempts+2 {
			return fmt.Errorf("node%d holds %d part files from %d fetch attempts", idx, len(part), fetchAttempts)
		}
		fmt.Fprintf(os.Stderr, "node%d inventory: %d objects (%d crash orphans), %d stage, %d part - all attributable\n",
			idx, len(spfo), orphans, len(stage), len(part))
	}
	return nil
}

func runBody(cluster *goharness.Cluster) error {
	bigMB := envUsize("MURMUR_FILES_CRASH_BIG_MB", 3)
	attempted := map[string]attempt{}
	fetchAttempts := 0

	// Positive control: honest upload replicates, fetches, verifies.
	ctrl := payload("ctrl/a.bin", 64<<10)
	st, err := fileUpload(cluster, 0, "ctrl/a.bin", ctrl)
	if err != nil {
		return fmt.Errorf("control upload: %w", err)
	}
	if st.size != uint64(len(ctrl)) {
		return fmt.Errorf("control upload byte count mismatch")
	}
	ctrlDigest := st.digestHex
	attempted["ctrl/a.bin"] = attempt{digest: &ctrlDigest, size: uint64(len(ctrl)), ok: true}
	deadline := time.Now().Add(wait)
	for {
		meta, err := fileMetadata(cluster, 1, "ctrl/a.bin")
		if err != nil {
			return err
		}
		if meta != nil && meta.digestHex == st.digestHex {
			break
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("control metadata never arrived")
		}
		time.Sleep(poll)
	}
	fetchAttempts++
	deadline = time.Now().Add(wait)
	for {
		meta, err := fileMetadata(cluster, 1, "ctrl/a.bin")
		if err != nil {
			return err
		}
		if meta != nil && meta.local {
			break
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("control fetch never completed")
		}
		time.Sleep(poll)
	}
	got, err := fileDownload(cluster, 1, "ctrl/a.bin")
	if err != nil {
		return fmt.Errorf("control download: %w", err)
	}
	if !equalBytes(got, ctrl) {
		return fmt.Errorf("control download bytes differ")
	}
	fmt.Fprintln(os.Stderr, "positive control: upload/replicate/fetch/verify works")
	// Baseline inventory before any crash: the clean path must leave
	// zero unattributable payloads (the before half of before/after).
	if err := verifyInventory(cluster, attempted, fetchAttempts); err != nil {
		return err
	}

	// Phase 1: kill the uploader mid-upload (a real process kill between
	// chunked appends; the unfinished upload must never publish).
	uploaderKilled := false
	for round := 1; round <= rounds && !uploaderKilled; round++ {
		errored := 0
		for i := 0; i < 8; i++ {
			name := fmt.Sprintf("crash/u-%d-%d.bin", round, i)
			data := payload(name, uploadBytes)
			if i == 3 {
				// The kill lands here: one chunk is appended, then the
				// uploader is killed before FINISH.
				upload, err := uploadBegin(cluster, 0, name)
				if err != nil {
					return fmt.Errorf("round %d: sabotaged BEGIN: %w", round, err)
				}
				if err := uploadAppend(cluster, 0, upload, data[:min(sabotageBytes, len(data))]); err != nil {
					return fmt.Errorf("round %d: sabotaged APPEND: %w", round, err)
				}
				cluster.KillNode(0)
				if err := restartNode(cluster, 0); err != nil {
					return err
				}
				// The unfinished upload must never publish.
				meta, err := fileMetadata(cluster, 0, name)
				if err != nil {
					return err
				}
				if meta != nil {
					return fmt.Errorf("round %d: killed upload %s published anyway", round, name)
				}
				errored++
				fmt.Fprintf(os.Stderr, "round %d: uploader killed mid-upload of %s\n", round, name)
				attempted[name] = attempt{digest: nil, size: uint64(len(data)), ok: false}
				continue
			}
			up, err := fileUpload(cluster, 0, name, data)
			if err != nil {
				return fmt.Errorf("round %d upload %s: %w", round, name, err)
			}
			if up.size != uint64(len(data)) {
				return fmt.Errorf("round %d upload %s byte count mismatch", round, name)
			}
			d := up.digestHex
			attempted[name] = attempt{digest: &d, size: uint64(len(data)), ok: true}
		}
		if errored == 0 {
			fmt.Fprintf(os.Stderr, "round %d: kill landed between uploads; retrying\n", round)
			continue
		}
		uploaderKilled = true
		fmt.Fprintf(os.Stderr, "round %d: uploader killed mid-upload (%d errored uploads)\n", round, errored)
	}
	if !uploaderKilled {
		return fmt.Errorf("no valid mid-upload kill in %d rounds", rounds)
	}

	// Phase 2: kill the fetcher mid-fetch (staged-transfer coordinated).
	fetcherKilled := false
	for round := 1; round <= rounds && !fetcherKilled; round++ {
		name := fmt.Sprintf("crash/f-%d.bin", round)
		data := payload(name, bigMB<<20)
		up, err := fileUpload(cluster, 0, name, data)
		if err != nil {
			return fmt.Errorf("round %d big upload: %w", round, err)
		}
		d := up.digestHex
		attempted[name] = attempt{digest: &d, size: uint64(len(data)), ok: true}
		deadline := time.Now().Add(wait)
		for {
			meta, err := fileMetadata(cluster, 1, name)
			if err != nil {
				return err
			}
			if meta != nil && meta.digestHex == up.digestHex {
				break
			}
			if !time.Now().Before(deadline) {
				return fmt.Errorf("round %d: big metadata never arrived", round)
			}
			time.Sleep(poll)
		}
		fetchAttempts++
		killed, tooSlow, err := killWhileFetching(cluster, 1, name, killWait)
		if err != nil {
			return err
		}
		if tooSlow {
			fmt.Fprintf(os.Stderr, "round %d: fetch completed before the kill; retrying\n", round)
			continue
		}
		if !killed {
			return fmt.Errorf("round %d: fetch never went in flight", round)
		}
		if err := restartNode(cluster, 1); err != nil {
			return err
		}
		fetcherKilled = true
		fmt.Fprintf(os.Stderr, "round %d: fetcher killed mid-fetch of %s\n", round, name)
		// The interrupted fetch must complete cleanly after restart:
		// staged partials never poison the verified install.
		fetchAttempts++
		deadline = time.Now().Add(postCrashFetchWait)
		for {
			meta, err := fileMetadata(cluster, 1, name)
			if err != nil {
				return err
			}
			if meta != nil && meta.local {
				break
			}
			if !time.Now().Before(deadline) {
				return fmt.Errorf("post-crash fetch never completed")
			}
			time.Sleep(poll)
		}
		got, err := fileDownload(cluster, 1, name)
		if err != nil {
			return fmt.Errorf("post-crash download: %w", err)
		}
		meta, err := fileMetadata(cluster, 1, name)
		if err != nil {
			return err
		}
		if meta == nil {
			return fmt.Errorf("post-crash metadata missing")
		}
		if meta.digestHex != up.digestHex {
			return fmt.Errorf("post-crash digest mismatch")
		}
		if !equalBytes(got, data) {
			return fmt.Errorf("post-crash bytes differ")
		}
	}
	if !fetcherKilled {
		return fmt.Errorf("no valid mid-fetch kill in %d rounds", rounds)
	}

	// Phase 3: full recovery proof.
	if err := waitMetadataSettled(cluster, attempted); err != nil {
		return err
	}
	if err := verifyAllObjects(cluster, attempted, &fetchAttempts); err != nil {
		return err
	}
	if err := verifyInventory(cluster, attempted, fetchAttempts); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "files-crash proven: no partials, no phantoms, inventory attributable")
	return nil
}

func TestFileCrashLeavesNoPartialOrPhantom(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	fetch := fetchInterval
	maxBytes := maxFileBytes
	opts := goharness.DefaultClusterOptions("files-crash")
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
		cluster.DumpForensics("files-crash")
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

// Live scenario: a two-node encrypted mesh soaks 64 KiB file uploads.
//
// Go port of tests/files_soak.rs using tests-live/goharness. Identical
// behavior: upload to node 1, wait for replicated metadata on node 2,
// fetch, download, verify the digest, and search the replicated names,
// one upload every MURMUR_FILES_SOAK_INTERVAL_SECONDS (default 1 s) for
// MURMUR_FILES_SOAK_DURATION_SECONDS (default 5 s). Upload p95 must stay
// under 5 s and metadata-to-verified-fetch p95 under 20 s, and the fetch
// gate must show every file completed with zero failures. Peer fetch is
// automatic on the configured fetch_interval.
package filessoak

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// objectKey is the file object key (Rust: OBJECT_KEY 0x51..).
var objectKey = [32]byte{
	0x51, 0x51, 0x51, 0x51, 0x51, 0x51, 0x51, 0x51,
	0x51, 0x51, 0x51, 0x51, 0x51, 0x51, 0x51, 0x51,
	0x51, 0x51, 0x51, 0x51, 0x51, 0x51, 0x51, 0x51,
	0x51, 0x51, 0x51, 0x51, 0x51, 0x51, 0x51, 0x51,
}

const (
	// fileBytes is the upload size per round (64 KiB).
	fileBytes = 64 << 10
	// maxFileBytes is the file cap (4 MiB).
	maxFileBytes uint64 = 4 << 20
	// appendChunk is the upload wire chunk size (server cap: 1 MiB).
	appendChunk = 256 << 10
	// downloadChunk is the download wire chunk size (server cap: 1 MiB).
	downloadChunk = 256 << 10
	// wait bounds each metadata/fetch wait (20 s).
	wait = 20 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
	// uploadP95SLO bounds upload p95 (5 s).
	uploadP95SLO = 5 * time.Second
	// transferP95SLO bounds metadata-to-verified-fetch p95 (20 s).
	transferP95SLO = 20 * time.Second
)

// fileMeta is one file-metadata row (MURMUR FILE NEXT columns: name,
// size, modified_ms, sha256, local).
type fileMeta struct {
	name      string
	size      uint64
	digestHex string
	local     bool
}

// payload is the deterministic round payload.
func payload(round int) []byte {
	out := make([]byte, fileBytes)
	for i := range out {
		out[i] = byte((round*31 + i*17) % 251)
	}
	return out
}

// p95 returns the p95 element of a sorted latency sample.
func p95(sorted []time.Duration) time.Duration {
	n := len(sorted)
	return sorted[min(n-1, (95*n+99)/100-1)]
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

// fileUpload uploads data under name on node idx: BEGIN, chunked APPEND
// (extended protocol, raw BYTEA), FINISH. Returns the published metadata.
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

// fileList lists metadata on node idx, optionally restricted by a LIKE
// pattern (MURMUR FILE SEARCH); "" lists everything.
func fileList(cluster *goharness.Cluster, idx int, like string, hasLike bool) ([]fileMeta, error) {
	node := cluster.Node(idx)
	open := "MURMUR FILE LIST"
	if hasLike {
		open = "MURMUR FILE SEARCH " + sqlQuote(like)
	}
	rows, err := node.Simple(open)
	if err != nil {
		return nil, fmt.Errorf("node%d %s: %w", idx+1, open, err)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return nil, fmt.Errorf("node%d %s: no cursor", idx+1, open)
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

// fileMetadata returns metadata for exactly name on node idx (nil when
// unlisted).
func fileMetadata(cluster *goharness.Cluster, idx int, name string) (*fileMeta, error) {
	list, err := fileList(cluster, idx, "", false)
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

func runBody(cluster *goharness.Cluster, duration, interval time.Duration) error {
	deadline := time.Now().Add(duration)
	var uploadLatencies []time.Duration
	var endToEndLatencies []time.Duration
	expected := map[string]string{}
	round := 0
	for time.Now().Before(deadline) {
		name := fmt.Sprintf("soak/file-%04d.bin", round)
		data := payload(round)

		start := time.Now()
		uploaded, err := fileUpload(cluster, 0, name, data)
		if err != nil {
			return fmt.Errorf("upload round %d: %w", round, err)
		}
		if uploaded.size != uint64(len(data)) {
			return fmt.Errorf("round %d: upload byte count %d, want %d", round, uploaded.size, len(data))
		}
		uploadLatencies = append(uploadLatencies, time.Since(start))
		expected[name] = uploaded.digestHex

		endToEndStart := time.Now()
		// Metadata leg.
		metaDeadline := time.Now().Add(wait)
		for {
			meta, err := fileMetadata(cluster, 1, name)
			if err != nil {
				return err
			}
			if meta != nil && meta.digestHex == uploaded.digestHex {
				break
			}
			if !time.Now().Before(metaDeadline) {
				return fmt.Errorf("round %d: metadata never arrived", round)
			}
			time.Sleep(poll)
		}
		// Fetch leg: the product fetches automatically on
		// fetch_interval, so this is an availability wait on the local
		// flag.
		fetchDeadline := time.Now().Add(wait)
		for {
			meta, err := fileMetadata(cluster, 1, name)
			if err != nil {
				return err
			}
			if meta != nil && meta.local {
				break
			}
			if !time.Now().Before(fetchDeadline) {
				return fmt.Errorf("round %d: object never fetched", round)
			}
			time.Sleep(poll)
		}
		got, err := fileDownload(cluster, 1, name)
		if err != nil {
			return fmt.Errorf("round %d: download failed: %w", round, err)
		}
		if !equalBytes(got, data) {
			return fmt.Errorf("round %d: downloaded bytes differ", round)
		}
		endToEndLatencies = append(endToEndLatencies, time.Since(endToEndStart))

		// Search across the replicated metadata (LIKE `%file-%`: the
		// wire SEARCH matches whole names).
		found, err := fileList(cluster, 1, "%file-%", true)
		if err != nil {
			return err
		}
		if len(found) != round+1 {
			return fmt.Errorf("round %d: search returned %d files, want %d", round, len(found), round+1)
		}

		round++
		if time.Now().Before(deadline) {
			time.Sleep(interval)
		}
	}
	if len(uploadLatencies) == 0 {
		return fmt.Errorf("soak completed no uploads")
	}

	// Fetch gate: every uploaded file must be locally available with its
	// upload digest on the peer.
	for name, digest := range expected {
		meta, err := fileMetadata(cluster, 1, name)
		if err != nil {
			return err
		}
		if meta == nil {
			return fmt.Errorf("file %s: metadata missing after the soak", name)
		}
		if !meta.local {
			return fmt.Errorf("file %s not available on the peer after the soak", name)
		}
		if meta.digestHex != digest {
			return fmt.Errorf("file %s: peer digest differs from the upload digest", name)
		}
	}

	// First-file status.
	first, err := fileMetadata(cluster, 1, "soak/file-0000.bin")
	if err != nil {
		return err
	}
	if first == nil {
		return fmt.Errorf("first file metadata missing")
	}
	if !first.local {
		return fmt.Errorf("first file not available")
	}

	sort.Slice(uploadLatencies, func(a, b int) bool { return uploadLatencies[a] < uploadLatencies[b] })
	sort.Slice(endToEndLatencies, func(a, b int) bool { return endToEndLatencies[a] < endToEndLatencies[b] })
	uploadP95 := p95(uploadLatencies)
	transferP95 := p95(endToEndLatencies)
	fmt.Fprintf(os.Stderr, "file soak duration=%v rounds=%d upload_p95=%v metadata_fetch_verify_p95=%v\n",
		duration, len(expected), uploadP95, transferP95)
	if uploadP95 > uploadP95SLO {
		return fmt.Errorf("upload p95 %v exceeded 5s SLO", uploadP95)
	}
	if transferP95 > transferP95SLO {
		return fmt.Errorf("end-to-end p95 %v exceeded 20s SLO", transferP95)
	}
	return nil
}

func TestEncryptedFileFetchSoak(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	duration := goharness.EnvSeconds("MURMUR_FILES_SOAK_DURATION_SECONDS", 5)
	interval := goharness.EnvSeconds("MURMUR_FILES_SOAK_INTERVAL_SECONDS", 1)
	fetchInterval := 100 * time.Millisecond
	maxBytes := maxFileBytes
	opts := goharness.DefaultClusterOptions("files-soak")
	opts.NumNodes = 2
	opts.Files = &goharness.FilesOptions{Key: objectKey, FetchInterval: &fetchInterval, MaxFileBytes: &maxBytes}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	result := runBody(cluster, duration, interval)
	failed := result != nil
	if result != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", result)
	}
	if failed {
		cluster.DumpForensics("files-soak")
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

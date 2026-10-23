// Live scenario: encrypted file fetch soak with latency SLOs.
//
// Go port of the Rust `files-soak` scenario
// (/home/marc/MURMUR/tests-live/files-soak/tests/files_soak.rs:
// TestEncryptedFileFetchSoak). A two-node encrypted mesh soaks 64 KiB file
// uploads for MURMUR_FILES_SOAK_DURATION_SECONDS (default 5 s), one upload
// every MURMUR_FILES_SOAK_INTERVAL_SECONDS (default 1 s): upload to node 1,
// wait for replicated metadata on node 2, fetch, download, verify the
// digest, and search the replicated names. Upload p95 must stay under 5 s
// and metadata-to-verified-fetch p95 under 20 s, and the fetch gate must
// show every file completed with zero failures.
//
// Uploads, downloads, metadata, and search use the direct Database file API;
// peer fetch is automatic on the configured fetch_interval (no explicit fetch
// call exists).
package filessoak

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// objectKey is the file object key (Rust: soakObjectKey 0x51..).
var objectKey = [32]byte{
	0x51, 0x51, 0x51, 0x51, 0x51, 0x51, 0x51, 0x51,
	0x51, 0x51, 0x51, 0x51, 0x51, 0x51, 0x51, 0x51,
	0x51, 0x51, 0x51, 0x51, 0x51, 0x51, 0x51, 0x51,
	0x51, 0x51, 0x51, 0x51, 0x51, 0x51, 0x51, 0x51,
}

const (
	// fileBytes is the upload size per round (Rust: 64 KiB).
	fileBytes = 64 << 10
	// maxFileBytes is the file cap (Rust: 4 MiB).
	maxFileBytes = uint64(4 << 20)
	// wait is the per-round metadata/fetch wait bound (Rust: 20 s).
	wait = 20 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
	// uploadP95SLO is the upload p95 SLO (Rust: 5 s).
	uploadP95SLO = 5 * time.Second
	// transferP95SLO is the metadata-to-verified-fetch p95 SLO (Rust: 20 s).
	transferP95SLO = 20 * time.Second
)

// payload is the deterministic round payload (Rust:
// data[i] = (round*31 + i*17) % 251).
func payload(round int) []byte {
	out := make([]byte, fileBytes)
	for i := range out {
		out[i] = byte((round*31 + i*17) % 251)
	}
	return out
}

// p95 returns the p95 element of a sorted latency sample (Rust:
// min(n-1, (95*n+99)/100-1)).
func p95(sorted []time.Duration) time.Duration {
	n := len(sorted)
	idx := (95*n+99)/100 - 1
	if idx > n-1 {
		idx = n - 1
	}
	return sorted[idx]
}

func TestEncryptedFileFetchSoak(t *testing.T) {
	duration := harness.EnvSeconds("MURMUR_FILES_SOAK_DURATION_SECONDS", 5)
	interval := harness.EnvSeconds("MURMUR_FILES_SOAK_INTERVAL_SECONDS", 1)
	fetchInterval := 100 * time.Millisecond
	maxBytes := maxFileBytes
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "files-soak",
		NumNodes: 2,
		Files: &harness.FilesOptions{
			Key:           objectKey,
			FetchInterval: &fetchInterval,
			MaxFileBytes:  &maxBytes,
		},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("files-soak")
		}
		cluster.Finish(failed)
	}()

	deadline := time.Now().Add(duration)
	var uploadLatencies []time.Duration
	var endToEndLatencies []time.Duration
	expected := make(map[string][32]byte)
	round := 0
	for time.Now().Before(deadline) {
		name := fmt.Sprintf("soak/file-%04d.bin", round)
		data := payload(round)

		start := time.Now()
		uploaded, err := cluster.Node(0).UploadFile(name, bytes.NewReader(data))
		if err != nil {
			t.Fatalf("upload round %d: %v", round, err)
		}
		if uploaded.Bytes != uint64(len(data)) {
			t.Fatalf("round %d: upload byte count %d, want %d", round, uploaded.Bytes, len(data))
		}
		uploadLatencies = append(uploadLatencies, time.Since(start))
		expected[name] = uploaded.Digest

		endToEndStart := time.Now()
		// Metadata leg (Rust: WaitFileAvailable metadata wait).
		metadataArrived := harness.WaitUntil(wait, poll, func() bool {
			meta, err := cluster.Node(1).FileMetadata(name)
			return err == nil && meta != nil && meta.Digest == uploaded.Digest
		})
		if !metadataArrived {
			t.Fatalf("round %d: metadata never arrived", round)
		}
		// Fetch leg (Rust: explicit FetchFile; the product fetches
		// automatically on fetch_interval, so this is an availability wait).
		fetched := harness.WaitUntil(wait, poll, func() bool {
			available, err := cluster.Node(1).FileAvailable(name)
			return err == nil && available
		})
		if !fetched {
			t.Fatalf("round %d: object never fetched", round)
		}
		var got bytes.Buffer
		downloaded, err := cluster.Node(1).GetFile(name, &got)
		if err != nil {
			t.Fatalf("round %d: download failed: %v", round, err)
		}
		if !bytes.Equal(got.Bytes(), data) {
			t.Fatalf("round %d: downloaded bytes differ", round)
		}
		if downloaded.Digest != uploaded.Digest {
			t.Fatalf("round %d: digest mismatch", round)
		}
		endToEndLatencies = append(endToEndLatencies, time.Since(endToEndStart))

		found, err := cluster.Node(1).SearchFiles("file-")
		if err != nil {
			t.Fatalf("round %d: search failed: %v", round, err)
		}
		if len(found) != round+1 {
			t.Fatalf("round %d: search returned %d files, want %d", round, len(found), round+1)
		}

		round++
		if time.Now().Before(deadline) {
			time.Sleep(interval)
		}
	}
	if len(uploadLatencies) == 0 {
		t.Fatal("soak completed no uploads")
	}

	// Fetch gate (Rust: FetchStats shows every file completed with zero
	// failures; the product exposes no fetch counters): every uploaded
	// file must be locally available with its upload digest on the peer,
	// and the fetcher must have recorded no error.
	for name, digest := range expected {
		available, err := cluster.Node(1).FileAvailable(name)
		if err != nil || !available {
			t.Fatalf("file %s not available on the peer after the soak", name)
		}
		meta, err := cluster.Node(1).FileMetadata(name)
		if err != nil {
			t.Fatalf("file %s: metadata lookup failed: %v", name, err)
		}
		if meta == nil {
			t.Fatalf("file %s: metadata missing after the soak", name)
		}
		if meta.Digest != digest {
			t.Fatalf("file %s: peer digest differs from the upload digest", name)
		}
	}
	fetchErr := cluster.Node(1).Replication().Files().LastFetchError()
	if fetchErr != nil {
		t.Fatalf("fetch errors: %s", *fetchErr)
	}

	// First-file status (Rust: StatFile exists + available).
	first, err := cluster.Node(1).FileMetadata("soak/file-0000.bin")
	if err != nil {
		t.Fatalf("first file status: %v", err)
	}
	if first == nil {
		t.Fatal("first file metadata missing")
	}
	if available, err := cluster.Node(1).FileAvailable("soak/file-0000.bin"); err != nil || !available {
		t.Fatal("first file not available")
	}

	sort.Slice(uploadLatencies, func(i, j int) bool { return uploadLatencies[i] < uploadLatencies[j] })
	sort.Slice(endToEndLatencies, func(i, j int) bool { return endToEndLatencies[i] < endToEndLatencies[j] })
	uploadP95 := p95(uploadLatencies)
	transferP95 := p95(endToEndLatencies)
	t.Logf("file soak duration=%v rounds=%d upload_p95=%v metadata_fetch_verify_p95=%v",
		duration, len(expected), uploadP95, transferP95)
	if uploadP95 > uploadP95SLO {
		t.Fatalf("upload p95 %v exceeded 5s SLO", uploadP95)
	}
	if transferP95 > transferP95SLO {
		t.Fatalf("end-to-end p95 %v exceeded 20s SLO", transferP95)
	}
	failed = false
}

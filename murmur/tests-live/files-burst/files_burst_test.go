// Live scenario: concurrent multi-file fetch burst.
//
// New Go scenario (no Rust original): a two-node encrypted mesh proves the
// file-fetch path sustains concurrent load. Node 1 uploads a burst of N
// distinct files back-to-back; node 2 must fetch every object with
// byte-identical content. A second burst then proves fetching still works
// afterwards (no wedged permits, no leaked fetch state): the exact
// accounting is pinned by the TestFetchMissingPermitExhaustion unit test,
// this scenario proves sustained end-to-end throughput.
package filesburst

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// objectKey is the file object key (0x53..).
var objectKey = [32]byte{
	0x53, 0x53, 0x53, 0x53, 0x53, 0x53, 0x53, 0x53,
	0x53, 0x53, 0x53, 0x53, 0x53, 0x53, 0x53, 0x53,
	0x53, 0x53, 0x53, 0x53, 0x53, 0x53, 0x53, 0x53,
	0x53, 0x53, 0x53, 0x53, 0x53, 0x53, 0x53, 0x53,
}

const (
	// defaultCount is the files per burst (MURMUR_FILES_BURST_COUNT).
	defaultCount = 12
	// maxFileBytes is the file cap.
	maxFileBytes = uint64(4 << 20)
	// wait is the per-wave fetch bound.
	wait = 60 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
)

// burstCount reads MURMUR_FILES_BURST_COUNT (default 12, clamped to 3..64).
func burstCount(t *testing.T) int {
	t.Helper()
	n := defaultCount
	if raw := os.Getenv("MURMUR_FILES_BURST_COUNT"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("MURMUR_FILES_BURST_COUNT: %v", err)
		}
		n = v
	}
	if n < 3 {
		n = 3
	}
	if n > 64 {
		n = 64
	}
	return n
}

// payload is the deterministic wave/index payload; sizes vary from 1 KiB
// to ~200 KiB so the burst mixes single- and multi-chunk objects.
func payload(wave, index int) []byte {
	size := 1024 + index*20*1024 + wave*7
	out := make([]byte, size)
	for i := range out {
		out[i] = byte((wave*131 + index*37 + i*17) % 251)
	}
	return out
}

func TestFilesBurstFetchesConcurrently(t *testing.T) {
	count := burstCount(t)
	fetchInterval := 100 * time.Millisecond
	maxBytes := maxFileBytes
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "files-burst",
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
			cluster.DumpForensics("files-burst")
		}
		cluster.Finish(failed)
	}()

	for wave := 0; wave < 2; wave++ {
		prefix := fmt.Sprintf("burst/w%d-", wave)
		want := make(map[string][]byte, count)
		for i := 0; i < count; i++ {
			name := fmt.Sprintf("%sfile-%04d.bin", prefix, i)
			data := payload(wave, i)
			uploaded, err := cluster.Node(0).UploadFile(name, bytes.NewReader(data))
			if err != nil {
				t.Fatalf("wave %d upload %s: %v", wave, name, err)
			}
			if uploaded.Bytes != uint64(len(data)) {
				t.Fatalf("wave %d %s: upload byte count %d, want %d", wave, name, uploaded.Bytes, len(data))
			}
			want[name] = data
		}
		// Every burst object must become available on the peer: with the
		// default two fetch permits, a 12-file burst needs several
		// throttled rounds.
		allArrived := harness.WaitUntil(wait, poll, func() bool {
			for name := range want {
				available, err := cluster.Node(1).FileAvailable(name)
				if err != nil || !available {
					return false
				}
			}
			return true
		})
		if !allArrived {
			missing := 0
			for name := range want {
				available, _ := cluster.Node(1).FileAvailable(name)
				if !available {
					missing++
				}
			}
			t.Fatalf("wave %d: %d of %d burst objects never fetched", wave, missing, count)
		}
		for name, data := range want {
			meta, err := cluster.Node(1).FileMetadata(name)
			if err != nil || meta == nil {
				t.Fatalf("wave %d %s: metadata missing after fetch", wave, name)
			}
			var got bytes.Buffer
			downloaded, err := cluster.Node(1).GetFile(name, &got)
			if err != nil {
				t.Fatalf("wave %d %s: download failed: %v", wave, name, err)
			}
			if !bytes.Equal(got.Bytes(), data) {
				t.Fatalf("wave %d %s: downloaded bytes differ", wave, name)
			}
			if downloaded.Digest != meta.Digest {
				t.Fatalf("wave %d %s: digest mismatch", wave, name)
			}
		}
		found, err := cluster.Node(1).SearchFiles(prefix + "file-")
		if err != nil {
			t.Fatalf("wave %d: search failed: %v", wave, err)
		}
		if len(found) != count {
			t.Fatalf("wave %d: search returned %d files, want %d", wave, len(found), count)
		}
		t.Logf("wave %d: %d burst files fetched and verified", wave, count)
	}
	if fetchErr := cluster.Node(1).Replication().Files().LastFetchError(); fetchErr != nil {
		t.Fatalf("fetch errors: %s", *fetchErr)
	}
	failed = false
}

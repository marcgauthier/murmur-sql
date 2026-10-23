// Live scenario: one-to-many file fanout.
//
// New Go scenario (no Rust original): an encrypted mesh with one uploader
// and several simultaneous fetchers proves the file-serve path sustains
// concurrent load. Node 1 uploads two multi-chunk files; every other node
// must fetch both with byte-identical content. Serve and fetch share two
// permits, so fetchers queue and retry across several rounds.
package filesfanout

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// objectKey is the file object key (0x54..).
var objectKey = [32]byte{
	0x54, 0x54, 0x54, 0x54, 0x54, 0x54, 0x54, 0x54,
	0x54, 0x54, 0x54, 0x54, 0x54, 0x54, 0x54, 0x54,
	0x54, 0x54, 0x54, 0x54, 0x54, 0x54, 0x54, 0x54,
	0x54, 0x54, 0x54, 0x54, 0x54, 0x54, 0x54, 0x54,
}

const (
	// defaultNodes is the mesh size (MURMUR_FILES_FANOUT_NODES).
	defaultNodes = 5
	// maxFileBytes is the file cap.
	maxFileBytes = uint64(4 << 20)
	// wait is the fanout convergence bound.
	wait = 90 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
)

// fileSizes covers a single-chunk and a multi-chunk object.
var fileSizes = []int{48 << 10, 256 << 10}

// fanoutNodes reads MURMUR_FILES_FANOUT_NODES (default 5, clamped to 3..8).
func fanoutNodes(t *testing.T) int {
	t.Helper()
	n := defaultNodes
	if raw := os.Getenv("MURMUR_FILES_FANOUT_NODES"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("MURMUR_FILES_FANOUT_NODES: %v", err)
		}
		n = v
	}
	if n < 3 {
		n = 3
	}
	if n > 8 {
		n = 8
	}
	return n
}

// payload is the deterministic file content for index i.
func payload(i int) []byte {
	out := make([]byte, fileSizes[i])
	for j := range out {
		out[j] = byte((i*53 + j*17) % 251)
	}
	return out
}

func TestFilesFanoutServesConcurrentFetchers(t *testing.T) {
	numNodes := fanoutNodes(t)
	fetchInterval := 100 * time.Millisecond
	maxBytes := maxFileBytes
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "files-fanout",
		NumNodes: numNodes,
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
			cluster.DumpForensics("files-fanout")
		}
		cluster.Finish(failed)
	}()

	var names []string
	want := make(map[string][]byte)
	for i := range fileSizes {
		name := fmt.Sprintf("fanout/file-%d.bin", i)
		data := payload(i)
		uploaded, err := cluster.Node(0).UploadFile(name, bytes.NewReader(data))
		if err != nil {
			t.Fatalf("upload %s: %v", name, err)
		}
		if uploaded.Bytes != uint64(len(data)) {
			t.Fatalf("%s: upload byte count %d, want %d", name, uploaded.Bytes, len(data))
		}
		names = append(names, name)
		want[name] = data
	}
	// Every fetcher converges on both objects. Refused permits surface
	// as transient fetch errors and retries, so only convergence and
	// content are asserted, not an error-free run.
	converged := harness.WaitUntil(wait, poll, func() bool {
		for node := 1; node < cluster.Len(); node++ {
			for _, name := range names {
				available, err := cluster.Node(node).FileAvailable(name)
				if err != nil || !available {
					return false
				}
			}
		}
		return true
	})
	if !converged {
		for node := 1; node < cluster.Len(); node++ {
			for _, name := range names {
				available, _ := cluster.Node(node).FileAvailable(name)
				if !available {
					t.Logf("node%d missing %s", node+1, name)
				}
			}
		}
		t.Fatalf("fanout to %d fetchers never converged", cluster.Len()-1)
	}
	for node := 1; node < cluster.Len(); node++ {
		for _, name := range names {
			meta, err := cluster.Node(node).FileMetadata(name)
			if err != nil || meta == nil {
				t.Fatalf("node%d %s: metadata missing after fanout", node+1, name)
			}
			var got bytes.Buffer
			downloaded, err := cluster.Node(node).GetFile(name, &got)
			if err != nil {
				t.Fatalf("node%d %s: download failed: %v", node+1, name, err)
			}
			if !bytes.Equal(got.Bytes(), want[name]) {
				t.Fatalf("node%d %s: downloaded bytes differ", node+1, name)
			}
			if downloaded.Digest != meta.Digest {
				t.Fatalf("node%d %s: digest mismatch", node+1, name)
			}
		}
		// Staging drains once every fetch completes: no stranded
		// partials or identity sidecars on a quiet fetcher.
		if ok := harness.WaitUntil(wait, poll, func() bool {
			status, err := cluster.Node(node).FileKeyRotationStatus()
			return err == nil && status.StagedTransfers == 0
		}); !ok {
			t.Fatalf("node%d: staging never drained after fanout", node+1)
		}
	}
	t.Logf("fanout: %d files served to %d fetchers", len(names), cluster.Len()-1)
	failed = false
}

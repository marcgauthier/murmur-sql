// Live scenario: file tombstone pruning at scale.
//
// New Go scenario (no Rust original): a two-node encrypted mesh proves
// deleted file objects are reclaimed. Sixty files replicate; forty are
// deleted and their tombstones replicate; forcing collection reclaims
// exactly the forty orphaned objects on both nodes while the twenty
// survivors keep downloading byte-identical.
package filesprune

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// objectKey is the file object key (0x59..).
var objectKey = [32]byte{
	0x59, 0x59, 0x59, 0x59, 0x59, 0x59, 0x59, 0x59,
	0x59, 0x59, 0x59, 0x59, 0x59, 0x59, 0x59, 0x59,
	0x59, 0x59, 0x59, 0x59, 0x59, 0x59, 0x59, 0x59,
	0x59, 0x59, 0x59, 0x59, 0x59, 0x59, 0x59, 0x59,
}

const (
	// fileCount is the uploaded set size; deleteCount of them are pruned.
	fileCount   = 60
	deleteCount = 40
	// fileBytes is the per-file size.
	fileBytes = 8 << 10
	// maxFileBytes is the file cap.
	maxFileBytes = uint64(4 << 20)
	// wait is the convergence bound.
	wait = 90 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
)

// payload is the deterministic file content for index i.
func payload(i int) []byte {
	out := make([]byte, fileBytes)
	for j := range out {
		out[j] = byte((i*31 + j*17) % 251)
	}
	return out
}

// objectCount counts stored file objects (*.spfo) in node idx's store.
func objectCount(t *testing.T, cluster *harness.Cluster, idx int) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(cluster.NodeInfo(idx).Dir, "objects"))
	if err != nil {
		t.Fatalf("node%d objects listing: %v", idx+1, err)
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".spfo") {
			count++
		}
	}
	return count
}

func TestFilesPruneReclaimsDeletedObjects(t *testing.T) {
	fetchInterval := 100 * time.Millisecond
	maxBytes := maxFileBytes
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "files-prune",
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
			cluster.DumpForensics("files-prune")
		}
		cluster.Finish(failed)
	}()

	var names []string
	want := make(map[string][]byte, fileCount)
	for i := 0; i < fileCount; i++ {
		name := fmt.Sprintf("prune/file-%04d.bin", i)
		data := payload(i)
		if _, err := cluster.Node(0).UploadFile(name, bytes.NewReader(data)); err != nil {
			t.Fatalf("upload %s: %v", name, err)
		}
		names = append(names, name)
		want[name] = data
	}
	if ok := harness.WaitUntil(wait, poll, func() bool {
		for _, name := range names {
			available, err := cluster.Node(1).FileAvailable(name)
			if err != nil || !available {
				return false
			}
		}
		return true
	}); !ok {
		t.Fatal("peer never fetched every file")
	}

	// Delete the first forty; every tombstone must replicate.
	deleted := names[:deleteCount]
	survivors := names[deleteCount:]
	for _, name := range deleted {
		if err := cluster.Node(0).DeleteFile(name); err != nil {
			t.Fatalf("delete %s: %v", name, err)
		}
	}
	if ok := harness.WaitUntil(wait, poll, func() bool {
		for _, name := range deleted {
			meta, err := cluster.Node(1).FileMetadata(name)
			if err != nil || meta != nil {
				return false
			}
		}
		return true
	}); !ok {
		t.Fatal("tombstones never replicated to the peer")
	}

	// Forced collection reclaims exactly the orphaned objects on both
	// nodes; survivors are untouched.
	for _, node := range []int{0, 1} {
		files := cluster.Node(node).Replication().Files()
		if files == nil {
			t.Fatalf("node%d has no file store", node+1)
		}
		if _, err := files.Collect(0); err != nil {
			t.Fatalf("node%d collect: %v", node+1, err)
		}
		if got := objectCount(t, cluster, node); got != len(survivors) {
			t.Fatalf("node%d stores %d objects, want %d survivors", node+1, got, len(survivors))
		}
		for _, name := range survivors {
			var got bytes.Buffer
			if _, err := cluster.Node(node).GetFile(name, &got); err != nil {
				t.Fatalf("node%d %s: download failed after prune: %v", node+1, name, err)
			}
			if !bytes.Equal(got.Bytes(), want[name]) {
				t.Fatalf("node%d %s: bytes differ after prune", node+1, name)
			}
		}
		found, err := cluster.Node(node).SearchFiles("prune/file-")
		if err != nil {
			t.Fatalf("node%d search failed: %v", node+1, err)
		}
		if len(found) != len(survivors) {
			t.Fatalf("node%d search returned %d files, want %d", node+1, len(found), len(survivors))
		}
	}
	failed = false
}

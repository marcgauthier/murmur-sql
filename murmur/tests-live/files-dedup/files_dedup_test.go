// Live scenario: identical file content deduplicates to one object.
//
// New Go scenario (no Rust original): a two-node encrypted mesh proves
// content-addressed file storage. The same bytes uploaded under three
// names share one digest and one stored object; every name fetches
// byte-identical on the peer. Deleting one name replicates its tombstone
// while the surviving names keep downloading: the shared object is only
// collected once unreferenced.
package filesdedup

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// objectKey is the file object key (0x56..).
var objectKey = [32]byte{
	0x56, 0x56, 0x56, 0x56, 0x56, 0x56, 0x56, 0x56,
	0x56, 0x56, 0x56, 0x56, 0x56, 0x56, 0x56, 0x56,
	0x56, 0x56, 0x56, 0x56, 0x56, 0x56, 0x56, 0x56,
	0x56, 0x56, 0x56, 0x56, 0x56, 0x56, 0x56, 0x56,
}

const (
	// nameCount is the names sharing one content.
	nameCount = 3
	// fileBytes is the shared content size.
	fileBytes = 32 << 10
	// maxFileBytes is the file cap.
	maxFileBytes = uint64(4 << 20)
	// wait is the convergence bound.
	wait = 60 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
)

// shared returns the deterministic content uploaded under every name.
func shared() []byte {
	out := make([]byte, fileBytes)
	for i := range out {
		out[i] = byte((i * 17) % 251)
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

func TestFilesDeduplicateIdenticalContent(t *testing.T) {
	fetchInterval := 100 * time.Millisecond
	maxBytes := maxFileBytes
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "files-dedup",
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
			cluster.DumpForensics("files-dedup")
		}
		cluster.Finish(failed)
	}()

	data := shared()
	var names []string
	var digest [32]byte
	for i := 0; i < nameCount; i++ {
		name := fmt.Sprintf("dedup/copy-%d.bin", i)
		uploaded, err := cluster.Node(0).UploadFile(name, bytes.NewReader(data))
		if err != nil {
			t.Fatalf("upload %s: %v", name, err)
		}
		if i == 0 {
			digest = uploaded.Digest
		} else if uploaded.Digest != digest {
			t.Fatalf("%s digest differs from the shared content digest", name)
		}
		names = append(names, name)
	}
	if got := objectCount(t, cluster, 0); got != 1 {
		t.Fatalf("uploader stores %d objects for identical content, want 1", got)
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
		t.Fatalf("peer never fetched every name")
	}
	for _, name := range names {
		var got bytes.Buffer
		downloaded, err := cluster.Node(1).GetFile(name, &got)
		if err != nil {
			t.Fatalf("%s: peer download failed: %v", name, err)
		}
		if !bytes.Equal(got.Bytes(), data) {
			t.Fatalf("%s: peer bytes differ", name)
		}
		if downloaded.Digest != digest {
			t.Fatalf("%s: peer digest differs", name)
		}
	}
	if got := objectCount(t, cluster, 1); got != 1 {
		t.Fatalf("peer stores %d objects for identical content, want 1", got)
	}

	// Deleting one name replicates its tombstone; the surviving names
	// keep downloading from the shared object.
	if err := cluster.Node(0).DeleteFile(names[0]); err != nil {
		t.Fatalf("delete %s: %v", names[0], err)
	}
	if ok := harness.WaitUntil(wait, poll, func() bool {
		meta, err := cluster.Node(1).FileMetadata(names[0])
		return err == nil && meta == nil
	}); !ok {
		t.Fatalf("tombstone for %s never replicated", names[0])
	}
	for _, name := range names[1:] {
		var got bytes.Buffer
		if _, err := cluster.Node(1).GetFile(name, &got); err != nil {
			t.Fatalf("%s: download failed after sibling delete: %v", name, err)
		}
		if !bytes.Equal(got.Bytes(), data) {
			t.Fatalf("%s: bytes differ after sibling delete", name)
		}
	}
	// The digest stays usable: the same content uploads fine under a new
	// name after the delete.
	if _, err := cluster.Node(0).UploadFile("dedup/copy-new.bin", bytes.NewReader(data)); err != nil {
		t.Fatalf("re-upload after delete: %v", err)
	}
	failed = false
}

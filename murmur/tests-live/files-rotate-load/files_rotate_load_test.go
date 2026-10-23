// Live scenario: file key rotation amid concurrent writes.
//
// New Go scenario (no Rust original): a two-node encrypted mesh proves
// file generation rotation converges while writes keep landing. Three
// upload batches interleave with a rolling rotation (batch 1 pre-rotation
// on node 1, batch 2 on the rotated node 1 while node 2 still runs the
// old generation, batch 3 after both rotated). Every file must settle at
// generation 1 on both nodes, available and byte-identical, with clean
// rotation status.
package filesrotateload

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// fileKeys are the generation-0 (retired) and generation-1 (current) file
// object keys.
var fileKeys = [2][32]byte{
	{
		0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
		0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
		0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
		0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
	},
	{
		0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
		0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
		0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
		0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
	},
}

const (
	// batches is the upload batch count; batchSize the files per batch.
	batches   = 3
	batchSize = 3
	// maxFileBytes is the file cap.
	maxFileBytes = uint64(4 << 20)
	// wait is the convergence bound per leg.
	wait = 60 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
)

// payload is the deterministic file content for batch b, index i.
func payload(b, i int) []byte {
	size := 1024 + (b*batchSize+i)*16*1024
	out := make([]byte, size)
	for j := range out {
		out[j] = byte((b*67 + i*41 + j*17) % 251)
	}
	return out
}

// fileName is the replicated name for batch b, index i.
func fileName(b, i int) string {
	return fmt.Sprintf("rotload/b%d-file-%d.bin", b, i)
}

// waitAvailable waits until every named file is locally available on node.
func waitAvailable(t *testing.T, cluster *harness.Cluster, node int, names []string, what string) {
	t.Helper()
	if ok := harness.WaitUntil(wait, poll, func() bool {
		for _, name := range names {
			available, err := cluster.Node(node).FileAvailable(name)
			if err != nil || !available {
				return false
			}
		}
		return true
	}); !ok {
		t.Fatalf("%s: objects never available on node%d", what, node+1)
	}
}

// waitGeneration waits until every named file's replicated metadata shows
// the given generation on node.
func waitGeneration(t *testing.T, cluster *harness.Cluster, node int, names []string, generation uint64, what string) {
	t.Helper()
	if ok := harness.WaitUntil(wait, poll, func() bool {
		for _, name := range names {
			meta, err := cluster.Node(node).FileMetadata(name)
			if err != nil || meta == nil || meta.Generation != generation {
				return false
			}
		}
		return true
	}); !ok {
		t.Fatalf("%s: metadata never reached generation %d on node%d", what, generation, node+1)
	}
}

// verifyDownloads checks every named file downloads byte-identical on node.
func verifyDownloads(t *testing.T, cluster *harness.Cluster, node int, want map[string][]byte, what string) {
	t.Helper()
	for name, data := range want {
		var got bytes.Buffer
		downloaded, err := cluster.Node(node).GetFile(name, &got)
		if err != nil {
			t.Fatalf("%s: %s download failed on node%d: %v", what, name, node+1, err)
		}
		if !bytes.Equal(got.Bytes(), data) {
			t.Fatalf("%s: %s bytes differ on node%d", what, name, node+1)
		}
		meta, err := cluster.Node(node).FileMetadata(name)
		if err != nil || meta == nil {
			t.Fatalf("%s: %s metadata missing on node%d", what, name, node+1)
		}
		if downloaded.Digest != meta.Digest {
			t.Fatalf("%s: %s digest mismatch on node%d", what, name, node+1)
		}
	}
}

// waitRotationClean waits until rotation status shows the given current
// generation with no old-generation metadata outstanding and no staged
// transfers in flight on node.
func waitRotationClean(t *testing.T, cluster *harness.Cluster, node int, generation uint64, what string) {
	t.Helper()
	if ok := harness.WaitUntil(wait, poll, func() bool {
		status, err := cluster.Node(node).FileKeyRotationStatus()
		return err == nil && status.CurrentGeneration == generation &&
			status.OldGenerationMetadataMissingLocally == 0 && status.StagedTransfers == 0
	}); !ok {
		status, err := cluster.Node(node).FileKeyRotationStatus()
		t.Fatalf("%s: rotation status never clean on node%d: %+v, %v", what, node+1, status, err)
	}
}

// rotate rolls node idx to generation 1 and waits until its objects are
// re-encrypted and converged.
func rotate(t *testing.T, cluster *harness.Cluster, idx int, names []string, want map[string][]byte) {
	t.Helper()
	what := fmt.Sprintf("rotation node%d", idx+1)
	cluster.StopNode(idx)
	oldKey := fileKeys[0]
	if err := cluster.SetFilesConfig(idx, fileKeys[1], 1, &oldKey); err != nil {
		t.Fatalf("%s: set files config: %v", what, err)
	}
	if err := cluster.StartNode(idx); err != nil {
		t.Fatalf("%s: restart: %v", what, err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("%s: re-mesh: %v", what, err)
	}
	waitGeneration(t, cluster, idx, names, 1, what)
	waitAvailable(t, cluster, idx, names, what)
	verifyDownloads(t, cluster, idx, want, what)
}

// uploadBatch uploads batch b to node and records names and contents.
func uploadBatch(t *testing.T, cluster *harness.Cluster, node, b int, names *[]string, want map[string][]byte) {
	t.Helper()
	for i := 0; i < batchSize; i++ {
		name := fileName(b, i)
		data := payload(b, i)
		if _, err := cluster.Node(node).UploadFile(name, bytes.NewReader(data)); err != nil {
			t.Fatalf("batch %d upload %s to node%d: %v", b, name, node+1, err)
		}
		*names = append(*names, name)
		want[name] = data
	}
}

func TestFileKeyRotationAmidWrites(t *testing.T) {
	fetchInterval := 100 * time.Millisecond
	maxBytes := maxFileBytes
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "files-rotate-load",
		NumNodes: 2,
		Files: &harness.FilesOptions{
			Key:           fileKeys[0],
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
			cluster.DumpForensics("files-rotate-load")
		}
		cluster.Finish(failed)
	}()

	var names []string
	want := make(map[string][]byte)

	// Batch 0 lands pre-rotation; then node 1 rolls.
	uploadBatch(t, cluster, 0, 0, &names, want)
	rotate(t, cluster, 0, names, want)

	// Batch 1 uploads at generation 1 on node 1 while node 2 still runs
	// generation 0; then node 2 rolls and everything converges.
	uploadBatch(t, cluster, 0, 1, &names, want)
	rotate(t, cluster, 1, names, want)

	// Batch 2 lands post-rotation on node 2.
	uploadBatch(t, cluster, 1, 2, &names, want)

	// Every file settles at generation 1 on both nodes, available and
	// byte-identical, with clean rotation status.
	for _, node := range []int{0, 1} {
		waitGeneration(t, cluster, node, names, 1, "final")
		waitAvailable(t, cluster, node, names, "final")
		verifyDownloads(t, cluster, node, want, "final")
		waitRotationClean(t, cluster, node, 1, "final")
	}
	failed = false
}

// Live scenario: file object-key rotation across a mesh.
//
// New Go scenario (no Rust original): a two-node encrypted mesh proves
// file generation rotation end to end. Both nodes upload and replicate
// files under key generation 0, then each node rolls to generation 1
// (new key plus the retired key), the engine re-encrypts its objects,
// and the rotated mesh replicates and fetches normally. A final leg
// drops the retired key once rotation status reports it unused.
package filesrekey

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// fileKeys are the generation-0 (retired) and generation-1 (current) file
// object keys.
var fileKeys = [2][32]byte{
	{
		0x60, 0x60, 0x60, 0x60, 0x60, 0x60, 0x60, 0x60,
		0x60, 0x60, 0x60, 0x60, 0x60, 0x60, 0x60, 0x60,
		0x60, 0x60, 0x60, 0x60, 0x60, 0x60, 0x60, 0x60,
		0x60, 0x60, 0x60, 0x60, 0x60, 0x60, 0x60, 0x60,
	},
	{
		0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
		0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
		0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
		0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
	},
}

const (
	// maxFileBytes is the file cap.
	maxFileBytes = uint64(4 << 20)
	// wait is the convergence bound per leg.
	wait = 60 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
)

// fileSizes covers a tiny, a single-chunk, and a multi-chunk object.
var fileSizes = []int{100, 48 << 10, 200 << 10}

// payload is the deterministic file content for index i.
func payload(i int) []byte {
	out := make([]byte, fileSizes[i])
	for j := range out {
		out[j] = byte((i*41 + j*17) % 251)
	}
	return out
}

// fileName is the replicated name for index i.
func fileName(i int) string {
	return fmt.Sprintf("rekey/file-%d.bin", i)
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

// waitRotationClean waits until rotation status shows the given current
// generation with no old-generation metadata outstanding and no staged
// transfers in flight. Staging drains microseconds after objects become
// available, so this must poll rather than assert once.
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

// verifyDownloads checks every named file downloads byte-identical with a
// stable digest on node.
func verifyDownloads(t *testing.T, cluster *harness.Cluster, node int, want map[string][]byte, what string) {
	t.Helper()
	for name, data := range want {
		meta, err := cluster.Node(node).FileMetadata(name)
		if err != nil || meta == nil {
			t.Fatalf("%s: %s metadata missing on node%d", what, name, node+1)
		}
		var got bytes.Buffer
		downloaded, err := cluster.Node(node).GetFile(name, &got)
		if err != nil {
			t.Fatalf("%s: %s download failed on node%d: %v", what, name, node+1, err)
		}
		if !bytes.Equal(got.Bytes(), data) {
			t.Fatalf("%s: %s bytes differ on node%d", what, name, node+1)
		}
		if downloaded.Digest != meta.Digest {
			t.Fatalf("%s: %s digest mismatch on node%d", what, name, node+1)
		}
	}
}

func TestFileKeyRotationAcrossMesh(t *testing.T) {
	fetchInterval := 100 * time.Millisecond
	maxBytes := maxFileBytes
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "files-rekey",
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
			cluster.DumpForensics("files-rekey")
		}
		cluster.Finish(failed)
	}()

	var names []string
	want := make(map[string][]byte)
	for i := range fileSizes {
		names = append(names, fileName(i))
	}
	digests := make(map[string][32]byte)

	// Baseline: replicate and fetch everything under generation 0.
	for i, name := range names {
		data := payload(i)
		uploaded, err := cluster.Node(0).UploadFile(name, bytes.NewReader(data))
		if err != nil {
			t.Fatalf("baseline upload %s: %v", name, err)
		}
		if uploaded.Generation != 0 {
			t.Fatalf("baseline %s: generation %d, want 0", name, uploaded.Generation)
		}
		want[name] = data
		digests[name] = uploaded.Digest
	}
	waitAvailable(t, cluster, 1, names, "baseline")
	verifyDownloads(t, cluster, 1, want, "baseline")
	for _, node := range []int{0, 1} {
		waitRotationClean(t, cluster, node, 0, "baseline")
	}

	// Rolling rotation: one node at a time reopens at generation 1 with
	// the retired key available; the engine re-encrypts its objects.
	oldKey := fileKeys[0]
	for _, node := range []int{0, 1} {
		cluster.StopNode(node)
		if err := cluster.SetFilesConfig(node, fileKeys[1], 1, &oldKey); err != nil {
			t.Fatalf("rotation set files config node%d: %v", node+1, err)
		}
		if err := cluster.StartNode(node); err != nil {
			t.Fatalf("rotation restart node%d: %v", node+1, err)
		}
		if err := cluster.Mesh(); err != nil {
			t.Fatalf("rotation re-mesh after node%d: %v", node+1, err)
		}
		waitGeneration(t, cluster, node, names, 1, fmt.Sprintf("rotation node%d", node+1))
		waitAvailable(t, cluster, node, names, fmt.Sprintf("rotation node%d", node+1))
		verifyDownloads(t, cluster, node, want, fmt.Sprintf("rotation node%d", node+1))
		for _, name := range names {
			meta, err := cluster.Node(node).FileMetadata(name)
			if err != nil || meta == nil {
				t.Fatalf("rotation node%d %s: metadata missing", node+1, name)
			}
			if meta.Digest != digests[name] {
				t.Fatalf("rotation node%d %s: digest changed across rotation", node+1, name)
			}
		}
	}

	// Rotated mesh proof: generation-1 metadata everywhere, no old
	// metadata outstanding, and post-rotation uploads replicate and fetch.
	for _, node := range []int{0, 1} {
		waitGeneration(t, cluster, node, names, 1, "rotated mesh")
		verifyDownloads(t, cluster, node, want, "rotated mesh")
		waitRotationClean(t, cluster, node, 1, "rotated mesh")
	}
	postName := "rekey/post-rotation.bin"
	postData := bytes.Repeat([]byte{0xAB}, 64<<10)
	if _, err := cluster.Node(1).UploadFile(postName, bytes.NewReader(postData)); err != nil {
		t.Fatalf("post-rotation upload: %v", err)
	}
	waitAvailable(t, cluster, 0, []string{postName}, "post-rotation")
	verifyDownloads(t, cluster, 0, map[string][]byte{postName: postData}, "post-rotation")
	if meta, err := cluster.Node(0).FileMetadata(postName); err != nil || meta == nil || meta.Generation != 1 {
		t.Fatalf("post-rotation metadata: %+v, %v", meta, err)
	}

	// Drop the retired key: with no old-generation metadata left, both
	// nodes reopen at generation 1 with no previous key.
	want[postName] = postData
	for _, node := range []int{0, 1} {
		cluster.StopNode(node)
		if err := cluster.SetFilesConfig(node, fileKeys[1], 1, nil); err != nil {
			t.Fatalf("drop-previous set files config node%d: %v", node+1, err)
		}
		if err := cluster.StartNode(node); err != nil {
			t.Fatalf("drop-previous restart node%d (old metadata remains?): %v", node+1, err)
		}
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("drop-previous re-mesh: %v", err)
	}
	for _, node := range []int{0, 1} {
		verifyDownloads(t, cluster, node, want, "drop-previous")
	}
	failed = false
}

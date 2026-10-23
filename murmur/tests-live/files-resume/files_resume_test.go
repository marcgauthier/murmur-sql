// Live scenario: interrupted file burst resumes cleanly.
//
// New Go scenario (no Rust original): a two-node encrypted mesh proves
// an interrupted burst of larger files converges without stranded
// state. Node 1 uploads ten 1 MiB files; once node 2 has partials in
// flight, node 2 restarts and re-meshes. Every file must converge
// byte-identical with drained staging and clean rotation status: no
// orphaned partials or identity sidecars survive the interruption.
package filesresume

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// objectKey is the file object key (0x5B..).
var objectKey = [32]byte{
	0x5B, 0x5B, 0x5B, 0x5B, 0x5B, 0x5B, 0x5B, 0x5B,
	0x5B, 0x5B, 0x5B, 0x5B, 0x5B, 0x5B, 0x5B, 0x5B,
	0x5B, 0x5B, 0x5B, 0x5B, 0x5B, 0x5B, 0x5B, 0x5B,
	0x5B, 0x5B, 0x5B, 0x5B, 0x5B, 0x5B, 0x5B, 0x5B,
}

const (
	// fileCount is the burst size.
	fileCount = 10
	// fileBytes is the per-file size: multi-chunk, slow enough to catch
	// in flight.
	fileBytes = 1 << 20
	// maxFileBytes is the file cap.
	maxFileBytes = uint64(4 << 20)
	// wait is the convergence bound.
	wait = 120 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
	// flightPoll catches in-flight partials.
	flightPoll = 10 * time.Millisecond
)

// payload is the deterministic file content for index i.
func payload(i int) []byte {
	out := make([]byte, fileBytes)
	for j := range out {
		out[j] = byte((i*37 + j*17) % 251)
	}
	return out
}

// stagingBusy reports whether node idx has any staging files.
func stagingBusy(cluster *harness.Cluster, idx int) bool {
	entries, err := os.ReadDir(filepath.Join(cluster.NodeInfo(idx).Dir, "objects", "staging"))
	if err != nil {
		return false
	}
	return len(entries) > 0
}

func TestFilesInterruptedBurstResumesCleanly(t *testing.T) {
	fetchInterval := 100 * time.Millisecond
	maxBytes := maxFileBytes
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "files-resume",
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
			cluster.DumpForensics("files-resume")
		}
		cluster.Finish(failed)
	}()

	var names []string
	want := make(map[string][]byte, fileCount)
	for i := 0; i < fileCount; i++ {
		name := fmt.Sprintf("resume/file-%04d.bin", i)
		data := payload(i)
		if _, err := cluster.Node(0).UploadFile(name, bytes.NewReader(data)); err != nil {
			t.Fatalf("upload %s: %v", name, err)
		}
		names = append(names, name)
		want[name] = data
	}
	// Catch partials in flight, then restart the fetcher under them.
	if ok := harness.WaitUntil(wait, flightPoll, func() bool {
		return stagingBusy(cluster, 1)
	}); !ok {
		t.Fatal("partials never observed in flight on node 2")
	}
	cluster.StopNode(1)
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart fetcher: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh: %v", err)
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
		t.Fatal("burst never converged after the fetcher restart")
	}
	for _, name := range names {
		meta, err := cluster.Node(1).FileMetadata(name)
		if err != nil || meta == nil {
			t.Fatalf("%s: metadata missing after convergence", name)
		}
		var got bytes.Buffer
		downloaded, err := cluster.Node(1).GetFile(name, &got)
		if err != nil {
			t.Fatalf("%s: download failed: %v", name, err)
		}
		if !bytes.Equal(got.Bytes(), want[name]) {
			t.Fatalf("%s: downloaded bytes differ", name)
		}
		if downloaded.Digest != meta.Digest {
			t.Fatalf("%s: digest mismatch", name)
		}
	}
	// Nothing stranded: staging drains and rotation status is clean.
	if ok := harness.WaitUntil(wait, poll, func() bool {
		status, err := cluster.Node(1).FileKeyRotationStatus()
		return err == nil && status.StagedTransfers == 0 && status.PreviousKeyUnusedLocally
	}); !ok {
		t.Fatal("staging never drained after convergence")
	}
	failed = false
}

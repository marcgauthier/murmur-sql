// Live scenario: file source restarts mid-transfer.
//
// New Go scenario (no Rust original): a two-node encrypted mesh proves a
// fetcher survives its source restarting mid-transfer. Node 1 uploads a
// large file; once node 2's fetch is observably in flight (staging
// non-empty), node 1 restarts (new replication and file-endpoint ports)
// and re-meshes. Node 2 must re-resolve the source and converge on
// byte-identical content with drained staging.
package filessourcerestart

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// objectKey is the file object key (0x58..).
var objectKey = [32]byte{
	0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58,
	0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58,
	0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58,
	0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58,
}

const (
	// fileName is the transferred file.
	fileName = "source-restart/big.bin"
	// fileBytes is the transfer size: large enough that the fetch is
	// observably in flight for many poll intervals.
	fileBytes = 32 << 20
	// maxFileBytes is the file cap.
	maxFileBytes = uint64(64 << 20)
	// wait is the convergence bound.
	wait = 120 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
	// flightPoll catches the in-flight window.
	flightPoll = 10 * time.Millisecond
	// restartDowntime is the source outage between stop and start.
	restartDowntime = 2 * time.Second
)

// payload is the deterministic transfer content.
func payload() []byte {
	out := make([]byte, fileBytes)
	for i := range out {
		out[i] = byte((i * 17) % 251)
	}
	return out
}

// stagingBusy reports whether node idx has any staging files (a fetch is
// in flight or a partial awaits resume).
func stagingBusy(cluster *harness.Cluster, idx int) bool {
	entries, err := os.ReadDir(filepath.Join(cluster.NodeInfo(idx).Dir, "objects", "staging"))
	if err != nil {
		return false
	}
	return len(entries) > 0
}

func TestFilesSourceRestartMidTransfer(t *testing.T) {
	fetchInterval := 100 * time.Millisecond
	maxBytes := maxFileBytes
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "files-source-restart",
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
			cluster.DumpForensics("files-source-restart")
		}
		cluster.Finish(failed)
	}()

	data := payload()
	uploaded, err := cluster.Node(0).UploadFile(fileName, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if uploaded.Bytes != uint64(len(data)) {
		t.Fatalf("upload byte count %d, want %d", uploaded.Bytes, len(data))
	}
	if ok := harness.WaitUntil(wait, poll, func() bool {
		meta, err := cluster.Node(1).FileMetadata(fileName)
		return err == nil && meta != nil
	}); !ok {
		t.Fatal("metadata never arrived on node 2")
	}
	// Catch the fetch in flight, then restart the source under it.
	if ok := harness.WaitUntil(wait, flightPoll, func() bool {
		return stagingBusy(cluster, 1)
	}); !ok {
		t.Fatal("fetch never observed in flight on node 2")
	}
	cluster.StopNode(0)
	time.Sleep(restartDowntime)
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart source: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh: %v", err)
	}
	if ok := harness.WaitUntil(wait, poll, func() bool {
		available, err := cluster.Node(1).FileAvailable(fileName)
		return err == nil && available
	}); !ok {
		t.Fatal("object never converged after the source restart")
	}
	meta, err := cluster.Node(1).FileMetadata(fileName)
	if err != nil || meta == nil {
		t.Fatal("metadata missing after convergence")
	}
	var got bytes.Buffer
	downloaded, err := cluster.Node(1).GetFile(fileName, &got)
	if err != nil {
		t.Fatalf("download failed: %v", err)
	}
	if !bytes.Equal(got.Bytes(), data) {
		t.Fatal("downloaded bytes differ")
	}
	if downloaded.Digest != meta.Digest || meta.Digest != uploaded.Digest {
		t.Fatal("digest mismatch")
	}
	if ok := harness.WaitUntil(wait, poll, func() bool {
		status, err := cluster.Node(1).FileKeyRotationStatus()
		return err == nil && status.StagedTransfers == 0
	}); !ok {
		t.Fatal("staging never drained after convergence")
	}
	failed = false
}

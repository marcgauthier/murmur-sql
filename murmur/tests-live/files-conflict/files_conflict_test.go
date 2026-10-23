// Live scenario: concurrent same-name file upload conflict.
//
// New Go scenario (no Rust original): a two-node encrypted mesh proves
// same-name file conflicts converge. Partitioned, each node uploads
// different bytes under one name; after healing, both nodes must agree
// on one winning digest whose content downloads byte-identical on both,
// with no phantom second file.
package filesconflict

import (
	"bytes"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// objectKey is the file object key (0x57..).
var objectKey = [32]byte{
	0x57, 0x57, 0x57, 0x57, 0x57, 0x57, 0x57, 0x57,
	0x57, 0x57, 0x57, 0x57, 0x57, 0x57, 0x57, 0x57,
	0x57, 0x57, 0x57, 0x57, 0x57, 0x57, 0x57, 0x57,
	0x57, 0x57, 0x57, 0x57, 0x57, 0x57, 0x57, 0x57,
}

const (
	// fileName is the contested name.
	fileName = "conflict/file.bin"
	// fileBytes is the per-side content size.
	fileBytes = 8 << 10
	// maxFileBytes is the file cap.
	maxFileBytes = uint64(4 << 20)
	// wait is the convergence bound.
	wait = 60 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
)

// payload is the deterministic per-node content (seed distinguishes the
// two sides).
func payload(seed int) []byte {
	out := make([]byte, fileBytes)
	for i := range out {
		out[i] = byte((seed*101 + i*17) % 251)
	}
	return out
}

func TestFilesConflictConvergesToOneWinner(t *testing.T) {
	fetchInterval := 100 * time.Millisecond
	maxBytes := maxFileBytes
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "files-conflict",
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
			cluster.DumpForensics("files-conflict")
		}
		cluster.Finish(failed)
	}()

	if err := cluster.RemovePeer(0, 1); err != nil {
		t.Fatalf("partition node1->node2: %v", err)
	}
	if err := cluster.RemovePeer(1, 0); err != nil {
		t.Fatalf("partition node2->node1: %v", err)
	}
	// Removal is asynchronous: wait until both sessions are actually
	// down, otherwise the first upload replicates before the partition
	// holds and no conflict ever exists.
	partitioned := harness.WaitUntil(10*time.Second, poll, func() bool {
		return cluster.Node(0).Replication().Status().Peers == 0 &&
			cluster.Node(1).Replication().Status().Peers == 0
	})
	if !partitioned {
		t.Fatal("partition never took effect")
	}
	dataA := payload(1)
	dataB := payload(2)
	metaA, err := cluster.Node(0).UploadFile(fileName, bytes.NewReader(dataA))
	if err != nil {
		t.Fatalf("node1 upload: %v", err)
	}
	// Distinct timestamps order the versions; the test accepts either
	// winner and proves agreement, not which side wins.
	time.Sleep(20 * time.Millisecond)
	metaB, err := cluster.Node(1).UploadFile(fileName, bytes.NewReader(dataB))
	if err != nil {
		t.Fatalf("node2 upload: %v", err)
	}
	if metaA.Digest == metaB.Digest {
		t.Fatal("conflicting uploads share a digest")
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("heal: %v", err)
	}

	// Both nodes converge on one winning digest.
	agreed := harness.WaitUntil(wait, poll, func() bool {
		metaA, errA := cluster.Node(0).FileMetadata(fileName)
		metaB, errB := cluster.Node(1).FileMetadata(fileName)
		return errA == nil && errB == nil && metaA != nil && metaB != nil &&
			metaA.Digest == metaB.Digest
	})
	if !agreed {
		t.Fatal("nodes never agreed on one winning digest")
	}
	// The losing side must still fetch the winner's object.
	fetched := harness.WaitUntil(wait, poll, func() bool {
		for node := 0; node < 2; node++ {
			available, err := cluster.Node(node).FileAvailable(fileName)
			if err != nil || !available {
				return false
			}
		}
		return true
	})
	if !fetched {
		t.Fatal("winning object never available on both nodes")
	}
	// The winner downloads byte-identical on both nodes and matches one
	// of the two uploaded contents.
	var winner []byte
	for node := 0; node < 2; node++ {
		var got bytes.Buffer
		downloaded, err := cluster.Node(node).GetFile(fileName, &got)
		if err != nil {
			t.Fatalf("node%d download failed: %v", node+1, err)
		}
		if winner == nil {
			winner = got.Bytes()
		} else if !bytes.Equal(got.Bytes(), winner) {
			t.Fatal("nodes downloaded different winners")
		}
		meta, err := cluster.Node(node).FileMetadata(fileName)
		if err != nil || meta == nil {
			t.Fatalf("node%d metadata missing after agreement", node+1)
		}
		if downloaded.Digest != meta.Digest {
			t.Fatalf("node%d digest mismatch", node+1)
		}
	}
	if !bytes.Equal(winner, dataA) && !bytes.Equal(winner, dataB) {
		t.Fatal("winner matches neither uploaded content")
	}
	winnerSide := "node1"
	if bytes.Equal(winner, dataB) {
		winnerSide = "node2"
	}
	t.Logf("conflict resolved: %s wins", winnerSide)
	// No phantom second file.
	for node := 0; node < 2; node++ {
		found, err := cluster.Node(node).SearchFiles("conflict/")
		if err != nil {
			t.Fatalf("node%d search failed: %v", node+1, err)
		}
		if len(found) != 1 || found[0].Name != fileName {
			t.Fatalf("node%d search returned %d files, want exactly the winner", node+1, len(found))
		}
	}
	failed = false
}

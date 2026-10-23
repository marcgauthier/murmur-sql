// Live scenario: multi-source file fetch failover.
//
// New Go scenario (no Rust original): a three-node encrypted mesh proves
// a fetcher fails over across sources. Node 2 fetches only from node 1
// while node 1 serves poisoned objects (corrupted at rest): every attempt
// must fail verification with nothing published. Reconnecting node 3 —
// the honest source — then lets every fetch complete byte-identical.
package filesfailover

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// objectKey is the file object key (0x55..).
var objectKey = [32]byte{
	0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
	0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
	0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
	0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
}

const (
	// fileCount is the failover set size.
	fileCount = 6
	// fileBytes is the per-file size (fast attempts, one chunk each).
	fileBytes = 4 << 10
	// maxFileBytes is the file cap.
	maxFileBytes = uint64(4 << 20)
	// wait is the convergence bound.
	wait = 60 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
	// poisonWindow is the corrupt-only phase: long enough for dozens of
	// fetch attempts against the poisoned source.
	poisonWindow = 5 * time.Second
)

// payload is the deterministic file content for index i.
func payload(i int) []byte {
	out := make([]byte, fileBytes)
	for j := range out {
		out[j] = byte((i*29 + j*17) % 251)
	}
	return out
}

// corrupt flips 16 bytes in the middle of buf, preserving its length.
func corrupt(buf []byte) {
	start := len(buf) / 2
	if start+16 > len(buf) {
		start = len(buf) - 16
	}
	for i := 0; i < 16; i++ {
		buf[start+i] ^= 0xFF
	}
}

func TestFilesFailoverAcrossSources(t *testing.T) {
	fetchInterval := 100 * time.Millisecond
	maxBytes := maxFileBytes
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:       "files-failover",
		NumNodes:   3,
		ManualPeers: true,
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
			cluster.DumpForensics("files-failover")
		}
		cluster.Finish(failed)
	}()

	var names []string
	want := make(map[string][]byte, fileCount)
	// Node 2 joins only after the poisoning: until the heal, its sole
	// file source is the poisoned node 1.
	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatalf("mesh node1->node2: %v", err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		t.Fatalf("mesh node2->node1: %v", err)
	}
	for i := 0; i < fileCount; i++ {
		name := fmt.Sprintf("failover/file-%d.bin", i)
		data := payload(i)
		if _, err := cluster.Node(0).UploadFile(name, bytes.NewReader(data)); err != nil {
			t.Fatalf("upload %s: %v", name, err)
		}
		names = append(names, name)
		want[name] = data
	}
	// Node 1 holds good copies.
	if ok := harness.WaitUntil(wait, poll, func() bool {
		for _, name := range names {
			available, err := cluster.Node(1).FileAvailable(name)
			if err != nil || !available {
				return false
			}
		}
		return true
	}); !ok {
		t.Fatalf("node 2 never fetched the baseline objects")
	}

	// Poison node 1's stored objects while stopped (same length, flipped
	// ciphertext). Node 1 keeps serving them: present objects are never
	// re-verified or refetched locally.
	digests := make(map[string][32]byte, len(names))
	metas := make(map[string]uint64, len(names))
	for _, name := range names {
		meta, err := cluster.Node(1).FileMetadata(name)
		if err != nil || meta == nil {
			t.Fatalf("baseline metadata %s: %+v, %v", name, meta, err)
		}
		digests[name] = meta.Digest
		metas[name] = meta.Generation
	}
	cluster.StopNode(1)
	for _, name := range names {
		digest := digests[name]
		path := filepath.Join(cluster.NodeInfo(1).Dir, "objects",
			fmt.Sprintf("%s.g%d.spfo", hex.EncodeToString(digest[:]), metas[name]))
		buf, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s object: %v", name, err)
		}
		corrupt(buf)
		if err := os.WriteFile(path, buf, 0o600); err != nil {
			t.Fatalf("poison %s object: %v", name, err)
		}
	}
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart poisoned node: %v", err)
	}
	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatalf("re-mesh node1->node2: %v", err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		t.Fatalf("re-mesh node2->node1: %v", err)
	}
	// Node 2 joins against the poisoned source only: node 0 is
	// administratively excluded first, otherwise membership gossip plus
	// bootstrap auto-dial would link node 2 straight to the honest
	// source. It learns metadata but every fetch attempt must fail
	// verification.
	if err := cluster.RemovePeer(2, 0); err != nil {
		t.Fatalf("exclude node1 from node3: %v", err)
	}
	if err := cluster.RemovePeer(0, 2); err != nil {
		t.Fatalf("exclude node3 from node1: %v", err)
	}
	if err := cluster.AddPeer(2, 1); err != nil {
		t.Fatalf("join node3->node2: %v", err)
	}
	if err := cluster.AddPeer(1, 2); err != nil {
		t.Fatalf("join node2->node3: %v", err)
	}
	if ok := harness.WaitUntil(wait, poll, func() bool {
		for _, name := range names {
			meta, err := cluster.Node(2).FileMetadata(name)
			if err != nil || meta == nil {
				return false
			}
		}
		return true
	}); !ok {
		t.Fatalf("node 3 never received the baseline metadata")
	}

	// Corrupt-only phase: every fetch attempt must fail verification and
	// publish nothing.
	time.Sleep(poisonWindow)
	for _, name := range names {
		if available, err := cluster.Node(2).FileAvailable(name); err != nil || available {
			t.Fatalf("%s available from the poisoned source alone", name)
		}
		var probe bytes.Buffer
		if _, err := cluster.Node(2).GetFile(name, &probe); err == nil {
			t.Fatalf("%s downloads from the poisoned source alone", name)
		}
	}
	fetchErr := cluster.Node(2).Replication().Files().LastFetchError()
	if fetchErr == nil || !strings.Contains(*fetchErr, "object authentication") {
		errStr := "<nil>"
		if fetchErr != nil {
			errStr = *fetchErr
		}
		t.Fatalf("poisoned fetch error = %q, want object authentication", errStr)
	}
	var selfProbe bytes.Buffer
	if _, err := cluster.Node(1).GetFile(names[0], &selfProbe); err == nil {
		t.Fatal("poisoned node downloads its own corrupt object")
	}

	// Heal: explicit re-admit clears the exclusion and every fetch
	// completes from the honest source.
	if err := cluster.AddPeer(2, 0); err != nil {
		t.Fatalf("heal node3->node1: %v", err)
	}
	if err := cluster.AddPeer(0, 2); err != nil {
		t.Fatalf("heal node1->node3: %v", err)
	}
	if ok := harness.WaitUntil(wait, poll, func() bool {
		for _, name := range names {
			available, err := cluster.Node(2).FileAvailable(name)
			if err != nil || !available {
				return false
			}
		}
		return true
	}); !ok {
		t.Fatalf("failover never completed after the heal")
	}
	for _, name := range names {
		var got bytes.Buffer
		downloaded, err := cluster.Node(2).GetFile(name, &got)
		if err != nil {
			t.Fatalf("%s: post-heal download failed: %v", name, err)
		}
		if !bytes.Equal(got.Bytes(), want[name]) {
			t.Fatalf("%s: post-heal bytes differ", name)
		}
		if downloaded.Digest != digests[name] {
			t.Fatalf("%s: post-heal digest mismatch", name)
		}
	}
	// Poisoned partials are discarded, never stranded: staging drains.
	if ok := harness.WaitUntil(wait, poll, func() bool {
		status, err := cluster.Node(2).FileKeyRotationStatus()
		return err == nil && status.StagedTransfers == 0
	}); !ok {
		t.Fatalf("staging never drained after failover")
	}
	failed = false
}

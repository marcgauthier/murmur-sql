// Live scenario: boundary-size files replicate exactly.
//
// New Go scenario (no Rust original): a two-node encrypted mesh proves
// file size boundaries behave: empty, 1-byte, header-sized, one-byte
// below/above/at the 64 KiB chunk edge, and exactly the file cap all
// replicate byte-identical, while one byte over the cap fails cleanly
// with nothing published.
package filesboundary

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// objectKey is the file object key (0x5C..).
var objectKey = [32]byte{
	0x5C, 0x5C, 0x5C, 0x5C, 0x5C, 0x5C, 0x5C, 0x5C,
	0x5C, 0x5C, 0x5C, 0x5C, 0x5C, 0x5C, 0x5C, 0x5C,
	0x5C, 0x5C, 0x5C, 0x5C, 0x5C, 0x5C, 0x5C, 0x5C,
	0x5C, 0x5C, 0x5C, 0x5C, 0x5C, 0x5C, 0x5C, 0x5C,
}

const (
	// maxFileBytes is the file cap for this scenario.
	maxFileBytes = uint64(1 << 20)
	// wait is the convergence bound.
	wait = 60 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
)

// boundarySizes covers empty, 1-byte, header-sized (40), chunk-edge
// (64 KiB -/+1 and exact), and exactly the cap.
var boundarySizes = []int{0, 1, 40, 64<<10 - 1, 64 << 10, 64<<10 + 1, 1 << 20}

// payload is the deterministic file content of n bytes.
func payload(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte((n + i*17) % 251)
	}
	return out
}

func TestFilesBoundarySizesReplicateExactly(t *testing.T) {
	fetchInterval := 100 * time.Millisecond
	maxBytes := maxFileBytes
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "files-boundary",
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
			cluster.DumpForensics("files-boundary")
		}
		cluster.Finish(failed)
	}()

	var names []string
	want := make(map[string][]byte, len(boundarySizes))
	for _, size := range boundarySizes {
		name := fmt.Sprintf("boundary/size-%07d.bin", size)
		data := payload(size)
		uploaded, err := cluster.Node(0).UploadFile(name, bytes.NewReader(data))
		if err != nil {
			t.Fatalf("upload %d bytes: %v", size, err)
		}
		if uploaded.Bytes != uint64(size) {
			t.Fatalf("size %d: upload byte count %d", size, uploaded.Bytes)
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
		t.Fatal("boundary files never all available on the peer")
	}
	for _, name := range names {
		meta, err := cluster.Node(1).FileMetadata(name)
		if err != nil || meta == nil {
			t.Fatalf("%s: metadata missing", name)
		}
		if meta.Bytes != uint64(len(want[name])) {
			t.Fatalf("%s: metadata bytes %d, want %d", name, meta.Bytes, len(want[name]))
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

	// One byte over the cap fails cleanly with nothing published.
	overName := "boundary/over-cap.bin"
	if _, err := cluster.Node(0).UploadFile(overName, bytes.NewReader(payload(int(maxFileBytes)+1))); err == nil {
		t.Fatal("over-cap upload accepted")
	}
	if meta, err := cluster.Node(0).FileMetadata(overName); err != nil || meta != nil {
		t.Fatalf("over-cap metadata published: %+v, %v", meta, err)
	}
	failed = false
}

// Live scenario: file endpoint refuses foreign database ids.
//
// New Go scenario (no Rust original): a two-node encrypted mesh plus a
// raw file-protocol client proves the file endpoint enforces database
// identity. A request carrying the cluster's own id streams the object;
// the same request with a foreign id is refused before any object
// lookup (no header, no FileMissing); a same-database request for a
// missing object still reports FileMissing.
package filesdbid

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
	"github.com/quic-go/quic-go"
)

// objectKey is the file object key (0x5A..).
var objectKey = [32]byte{
	0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A,
	0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A,
	0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A,
	0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A,
}

const (
	// fileALPN is the file-fetch endpoint ALPN. A mismatch breaks every
	// probe loudly, which is the point: the wire identity is load-bearing.
	fileALPN = "murmur-sqlite/files/1"
	// fileName is the probed file.
	fileName = "dbid/report.bin"
	// fileBytes is the probed content size.
	fileBytes = 48 << 10
	// maxFileBytes is the file cap.
	maxFileBytes = uint64(4 << 20)
	// wait is the mesh convergence bound.
	wait = 60 * time.Second
	// poll is the convergence poll interval.
	poll = 50 * time.Millisecond
	// probeTimeout bounds each raw-protocol probe.
	probeTimeout = 30 * time.Second
)

// payload is the deterministic file content.
func payload() []byte {
	out := make([]byte, fileBytes)
	for i := range out {
		out[i] = byte((i * 17) % 251)
	}
	return out
}

// storedContainer returns the raw stored object bytes for the single
// uploaded file on node idx.
func storedContainer(t *testing.T, cluster *harness.Cluster, idx int) []byte {
	t.Helper()
	dir := filepath.Join(cluster.NodeInfo(idx).Dir, "objects")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("node%d objects listing: %v", idx+1, err)
	}
	var found string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".spfo") {
			if found != "" {
				t.Fatalf("node%d stores more than one object", idx+1)
			}
			found = e.Name()
		}
	}
	if found == "" {
		t.Fatalf("node%d stores no object", idx+1)
	}
	raw, err := os.ReadFile(filepath.Join(dir, found))
	if err != nil {
		t.Fatalf("node%d object read: %v", idx+1, err)
	}
	return raw
}

// fileClient dials node idx's file endpoint with a freshly issued leaf
// certificate from the cluster CA and returns the open connection.
func fileClient(t *testing.T, cluster *harness.Cluster, idx int) *quic.Conn {
	t.Helper()
	endpoint := cluster.Node(idx).Replication().Status().FileEndpoint
	if endpoint == nil {
		t.Fatalf("node%d has no file endpoint", idx+1)
	}
	var probeID [16]byte
	for i := range probeID {
		probeID[i] = byte(0xF0 + i)
	}
	issued, err := cluster.IssueCert(probeID)
	if err != nil {
		t.Fatalf("issue probe cert: %v", err)
	}
	key, err := x509.ParsePKCS8PrivateKey(issued.KeyDER)
	if err != nil {
		t.Fatalf("parse probe key: %v", err)
	}
	tlsConf := &tls.Config{
		Certificates:       []tls.Certificate{{Certificate: issued.Credentials.Chain, PrivateKey: key}},
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{fileALPN},
		InsecureSkipVerify: true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	conn, err := quic.DialAddr(ctx, endpoint.String(), tlsConf, &quic.Config{})
	if err != nil {
		t.Fatalf("dial file endpoint: %v", err)
	}
	return conn
}

// request opens one stream, sends a file request, and returns the first
// response frame. The caller owns conn.
func request(t *testing.T, conn *quic.Conn, frame *replication.FileFrame) replication.FileFrame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open file stream: %v", err)
	}
	defer stream.Close()
	if err := replication.WriteFileFrame(stream, frame); err != nil {
		t.Fatalf("write file request: %v", err)
	}
	response, err := replication.ReadFileFrame(stream)
	if err != nil {
		t.Fatalf("read file response: %v", err)
	}
	return response
}

func TestFileEndpointRefusesForeignDatabase(t *testing.T) {
	fetchInterval := 100 * time.Millisecond
	maxBytes := maxFileBytes
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "files-dbid",
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
			cluster.DumpForensics("files-dbid")
		}
		cluster.Finish(failed)
	}()

	data := payload()
	if _, err := cluster.Node(0).UploadFile(fileName, bytes.NewReader(data)); err != nil {
		t.Fatalf("upload: %v", err)
	}
	// Product-path positive control: ordinary mesh fetch works.
	if ok := harness.WaitUntil(wait, poll, func() bool {
		available, err := cluster.Node(1).FileAvailable(fileName)
		return err == nil && available
	}); !ok {
		t.Fatal("mesh fetch never completed")
	}
	meta, err := cluster.Node(0).FileMetadata(fileName)
	if err != nil || meta == nil {
		t.Fatalf("metadata missing: %v", err)
	}
	database := cluster.DatabaseID()
	container := storedContainer(t, cluster, 0)

	// Same-database request streams the stored container bytes.
	conn := fileClient(t, cluster, 0)
	func() {
		defer conn.CloseWithError(0, "probe done")
		ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
		defer cancel()
		stream, err := conn.OpenStreamSync(ctx)
		if err != nil {
			t.Fatalf("open file stream: %v", err)
		}
		defer stream.Close()
		if err := replication.WriteFileFrame(stream, &replication.FileFrame{
			Kind: replication.FileRequest, Database: database,
			Digest: meta.Digest, Generation: meta.Generation,
		}); err != nil {
			t.Fatalf("write file request: %v", err)
		}
		header, err := replication.ReadFileFrame(stream)
		if err != nil {
			t.Fatalf("read file header: %v", err)
		}
		if header.Kind != replication.FileHeader {
			t.Fatalf("same-database response = %d, want FileHeader", header.Kind)
		}
		var streamed []byte
		for {
			frame, err := replication.ReadFileFrame(stream)
			if err != nil {
				t.Fatalf("read file stream: %v", err)
			}
			if frame.Kind == replication.FileDone {
				break
			}
			if frame.Kind != replication.FileChunkFrame {
				t.Fatalf("stream frame = %d, want chunk or done", frame.Kind)
			}
			streamed = append(streamed, frame.Data...)
		}
		if !bytes.Equal(streamed, container) {
			t.Fatal("streamed container differs from the stored object")
		}
	}()

	// Foreign-database request is refused before any object lookup: no
	// header and no FileMissing, just a torn-down stream. The digest is
	// bogus too, pinning refusal-ahead-of-lookup ordering.
	foreignConn := fileClient(t, cluster, 0)
	func() {
		defer foreignConn.CloseWithError(0, "probe done")
		var foreignDB [16]byte
		for i := range foreignDB {
			foreignDB[i] = byte(0xE0 + i)
		}
		var bogus [32]byte
		for i := range bogus {
			bogus[i] = byte(i)
		}
		ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
		defer cancel()
		stream, err := foreignConn.OpenStreamSync(ctx)
		if err != nil {
			t.Fatalf("open file stream: %v", err)
		}
		defer stream.Close()
		if err := replication.WriteFileFrame(stream, &replication.FileFrame{
			Kind: replication.FileRequest, Database: foreignDB,
			Digest: bogus,
		}); err != nil {
			t.Fatalf("write foreign request: %v", err)
		}
		response, err := replication.ReadFileFrame(stream)
		if err == nil {
			t.Fatalf("foreign-database response = %d, want connection teardown", response.Kind)
		}
	}()

	// Same-database request for a missing object still reports FileMissing.
	missingConn := fileClient(t, cluster, 0)
	func() {
		defer missingConn.CloseWithError(0, "probe done")
		var bogus [32]byte
		for i := range bogus {
			bogus[i] = byte(0xFF - i)
		}
		response := request(t, missingConn, &replication.FileFrame{
			Kind: replication.FileRequest, Database: database, Digest: bogus,
		})
		if response.Kind != replication.FileMissing {
			t.Fatalf("missing-object response = %d, want FileMissing", response.Kind)
		}
	}()
	failed = false
}

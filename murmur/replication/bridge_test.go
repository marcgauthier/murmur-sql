package replication

import (
	"bytes"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
)

type bridgeBackend struct {
	*testBackend
	imported [][]byte
}

func (b *bridgeBackend) ImportBridge(_ Header, payload []byte) (bool, error) {
	b.imported = append(b.imported, append([]byte(nil), payload...))
	return true, nil
}

func testReceiver(t *testing.T, dir string, backend Backend) *Bridge {
	t.Helper()
	cfg := NewBridgeReceiver(idOf(1), dir, "s", fill32(40),
		map[NodeId]bool{idOf(1): true},
		map[NodeId][32]byte{idOf(2): PublicKey(mustSeed(30))},
		map[NodeId][32]byte{})
	b, err := OpenBridge(*cfg, backend)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustSeed(b byte) *[32]byte {
	s := fill32(b)
	return &s
}

// snapshotBackend records ownership releases and serves a fixed snapshot.
type snapshotBackend struct {
	Backend
	snapshot []byte
	released int
}

func (b *snapshotBackend) Snapshot() ([]byte, error) { return b.snapshot, nil }

func (b *snapshotBackend) ReleaseOwnership(string, []byte, *string) (bool, error) {
	b.released++
	return true, nil
}

func TestBridgeRelease(t *testing.T) {
	backend := &snapshotBackend{Backend: newTestBackend()}
	receiver := testReceiver(t, t.TempDir(), backend)
	released, err := receiver.Release("t", []byte("k"), nil)
	if err != nil || !released || backend.released != 1 {
		t.Fatalf("release = %v, %v (count %d)", released, err, backend.released)
	}
	column := "c"
	released, err = receiver.Release("t", []byte("k"), &column)
	if err != nil || !released || backend.released != 2 {
		t.Fatalf("column release = %v, %v (count %d)", released, err, backend.released)
	}
	exporter := testExporter(t, t.TempDir())
	if _, err := exporter.Release("t", []byte("k"), nil); err == nil {
		t.Fatal("low-side release accepted")
	}
}

func TestBridgeExportSnapshot(t *testing.T) {
	exportDir := t.TempDir()
	receiveDir := t.TempDir()
	payload := bytesRepeat(0xA5, 5000)
	cfg := NewBridgeExporter(idOf(1), exportDir, "s", fill32(30), RecipientPublicForBridge(fill32(40)))
	exporter, err := OpenBridge(*cfg, &snapshotBackend{Backend: newTestBackend(), snapshot: payload})
	if err != nil {
		t.Fatal(err)
	}
	backend := &bridgeBackend{testBackend: newTestBackend()}
	receiver := testReceiver(t, receiveDir, backend)
	path, err := exporter.ExportSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) == 0 {
		t.Fatalf("snapshot bundle = %d bytes, %v", len(raw), err)
	}
	incoming := filepath.Join(receiveDir, filepath.Base(path))
	if err := os.WriteFile(incoming, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	header, err := receiver.Stage(incoming)
	if err != nil {
		t.Fatal(err)
	}
	if header.Sequence != 1 || header.Stream != "s" {
		t.Fatalf("header = %+v", header)
	}
	n, err := receiver.Process()
	if err != nil || n != 1 {
		t.Fatalf("process = %d, %v", n, err)
	}
	if len(backend.imported) != 1 || string(backend.imported[0]) != string(payload) {
		t.Fatal("snapshot payload did not import intact")
	}
}

func TestBridgeCaptureFileEdges(t *testing.T) {
	// No file store attached: capture is a no-op.
	bare := testExporter(t, t.TempDir())
	if n, err := bare.Capture(); err != nil || n != 0 {
		t.Fatalf("bare capture = %d, %v", n, err)
	}

	// Listed but unavailable objects are skipped, not exported.
	shared := newTestBackend()
	objectsA, err := OpenFileObjects(*NewFilesConfig(t.TempDir(), fill32(3), netip.MustParseAddrPort("127.0.0.1:0")), shared)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := objectsA.UploadOne("doc", bytes.NewReader(bytesRepeat(9, 1000))); err != nil {
		t.Fatal(err)
	}
	objectsB, err := OpenFileObjects(*NewFilesConfig(t.TempDir(), fill32(3), netip.MustParseAddrPort("127.0.0.1:0")), shared)
	if err != nil {
		t.Fatal(err)
	}
	exportDir := t.TempDir()
	exporter := testExporter(t, exportDir)
	exporter.backend = shared
	exporter.AttachFiles(objectsB)
	bundles := func() int {
		entries, _ := os.ReadDir(exportDir)
		n := 0
		for _, e := range entries {
			if filepath.Ext(e.Name()) == ".bundle" {
				n++
			}
		}
		return n
	}
	if n, err := exporter.Capture(); err != nil || n != 0 {
		t.Fatalf("unavailable capture = %d, %v", n, err)
	}
	if bundles() != 0 {
		t.Fatal("unavailable object exported")
	}

	// Already-exported objects are skipped on later captures.
	exporter.AttachFiles(objectsA)
	if err := exporter.ExportFile("doc"); err != nil {
		t.Fatal(err)
	}
	before := bundles()
	if before == 0 {
		t.Fatal("no bundle exported")
	}
	if n, err := exporter.Capture(); err != nil || n != 0 {
		t.Fatalf("re-capture = %d, %v", n, err)
	}
	if bundles() != before {
		t.Fatal("exported object re-exported")
	}

	// A corrupt progress record fails the capture loudly.
	sum := sha256.Sum256([]byte("doc"))
	progressKey := []byte("09/bridge/file-export/s/" + Hex(sum[:]))
	if err := shared.Commit([]Record{{Key: progressKey, Value: []byte{1, 2, 3}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := exporter.Capture(); err == nil {
		t.Fatal("corrupt progress accepted")
	}
}

func TestBridgeDecodeHeader(t *testing.T) {
	exportDir := t.TempDir()
	receiveDir := t.TempDir()
	exporter := testExporter(t, exportDir)
	backend := &bridgeBackend{testBackend: newTestBackend()}
	receiver := testReceiver(t, receiveDir, backend)
	path, err := exporter.Export(bytesRepeat(0x5A, 100))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	incoming := filepath.Join(receiveDir, filepath.Base(path))
	if err := os.WriteFile(incoming, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	header, err := receiver.Stage(incoming)
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeHeader(EncodeHeader(header))
	if err != nil {
		t.Fatal(err)
	}
	if string(EncodeHeader(&back)) != string(EncodeHeader(header)) {
		t.Fatal("header roundtrip mismatch")
	}
	if _, err := DecodeHeader(nil); err == nil {
		t.Fatal("empty header accepted")
	}
	enc := EncodeHeader(header)
	if _, err := DecodeHeader(enc[:len(enc)-1]); err == nil {
		t.Fatal("truncated header accepted")
	}
}

func TestBridgeRoundtrip(t *testing.T) {
	exportDir := t.TempDir()
	receiveDir := t.TempDir()
	exporter := testExporter(t, exportDir)
	backend := &bridgeBackend{testBackend: newTestBackend()}
	receiver := testReceiver(t, receiveDir, backend)
	payload := bytesRepeat(0x5A, 1000)
	path, err := exporter.Export(payload)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate the one-way transfer.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	incoming := filepath.Join(receiveDir, filepath.Base(path))
	if err := os.WriteFile(incoming, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	header, err := receiver.Stage(incoming)
	if err != nil {
		t.Fatal(err)
	}
	if header.Sequence != 1 || header.Stream != "s" {
		t.Fatalf("header = %+v", header)
	}
	n, err := receiver.Process()
	if err != nil || n != 1 {
		t.Fatalf("process = %d, %v", n, err)
	}
	if len(backend.imported) != 1 || !bytes.Equal(backend.imported[0], payload) {
		t.Fatal("payload mismatch")
	}
	n, err = receiver.Process()
	if err != nil || n != 0 {
		t.Fatalf("reprocess = %d, %v", n, err)
	}
}

func TestBridgeTamper(t *testing.T) {
	exportDir := t.TempDir()
	receiveDir := t.TempDir()
	exporter := testExporter(t, exportDir)
	backend := &bridgeBackend{testBackend: newTestBackend()}
	receiver := testReceiver(t, receiveDir, backend)
	path, err := exporter.Export([]byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	raw[len(raw)-1] ^= 1
	incoming := filepath.Join(receiveDir, filepath.Base(path))
	if err := os.WriteFile(incoming, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.Stage(incoming); err == nil {
		t.Fatal("tampered bundle staged")
	}
	n, err := receiver.Process()
	if err != nil || n != 0 {
		t.Fatalf("process = %d, %v", n, err)
	}
	if _, err := os.Stat(incoming); !os.IsNotExist(err) {
		t.Fatal("rejected artifact not quarantined")
	}
	entries, _ := os.ReadDir(filepath.Join(receiveDir, "quarantine"))
	if len(entries) != 1 {
		t.Fatalf("quarantine = %d files", len(entries))
	}
	keys, _ := backend.Keys([]byte("09/bridge/quarantine/"))
	if len(keys) != 1 {
		t.Fatalf("quarantine records = %d", len(keys))
	}
}

func TestBridgeFiles(t *testing.T) {
	exportDir := t.TempDir()
	receiveDir := t.TempDir()
	exportBackend := newTestBackend()
	exporter := testExporter(t, exportDir)
	// Reopen exporter over its own backend with file storage.
	exporter.backend = exportBackend
	objects, err := OpenFileObjects(*NewFilesConfig(t.TempDir(), fill32(3), netip.MustParseAddrPort("127.0.0.1:0")), exportBackend)
	if err != nil {
		t.Fatal(err)
	}
	exporter.AttachFiles(objects)
	data := bytesRepeat(11, 100_000)
	if _, err := objects.UploadOne("doc", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err := exporter.ExportFile("doc"); err != nil {
		t.Fatal(err)
	}
	// Second export is a no-op (durable progress).
	if err := exporter.ExportFile("doc"); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(exportDir)
	var bundles []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".bundle" {
			bundles = append(bundles, e.Name())
		}
	}
	if len(bundles) == 0 {
		t.Fatal("no file bundles exported")
	}
	for _, name := range bundles {
		raw, _ := os.ReadFile(filepath.Join(exportDir, name))
		if err := os.WriteFile(filepath.Join(receiveDir, name), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	receiveBackend := &bridgeBackend{testBackend: newTestBackend()}
	receiver := testReceiver(t, receiveDir, receiveBackend)
	robjects, err := OpenFileObjects(*NewFilesConfig(t.TempDir(), fill32(3), netip.MustParseAddrPort("127.0.0.1:0")), receiveBackend)
	if err != nil {
		t.Fatal(err)
	}
	receiver.AttachFiles(robjects)
	for i := 0; i < 10; i++ {
		n, err := receiver.Process()
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	var out bytes.Buffer
	if _, err := robjects.Read("doc", &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Fatal("bridged file mismatch")
	}
}

func TestBridgeCapture(t *testing.T) {
	dir := t.TempDir()
	backend := newTestBackend()
	exporter := testExporter(t, dir)
	exporter.backend = backend
	tx := Transaction{Database: idOf(1), Origin: idOf(3), Sequence: 1, ID: idOf(8),
		Millis: 10, Payload: []byte{1, 2, 3}}
	seed := fill32(21)
	tx.Sign(&seed)
	if err := backend.Commit([]Record{PutRecord(LogKey(idOf(3), 1), EncodeTransaction(&tx))}); err != nil {
		t.Fatal(err)
	}
	n, err := exporter.Capture()
	if err != nil || n != 1 {
		t.Fatalf("capture = %d, %v", n, err)
	}
	n, err = exporter.Capture()
	if err != nil || n != 0 {
		t.Fatalf("recapture = %d, %v", n, err)
	}
	// The captured bundle carries an origin proof the receiver checks.
	entries, _ := os.ReadDir(dir)
	var bundle string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".bundle" {
			bundle = filepath.Join(dir, e.Name())
		}
	}
	if bundle == "" {
		t.Fatal("no bundle captured")
	}
	receiveDir := t.TempDir()
	raw, _ := os.ReadFile(bundle)
	incoming := filepath.Join(receiveDir, "x.bundle")
	if err := os.WriteFile(incoming, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	receiveBackend := &bridgeBackend{testBackend: newTestBackend()}
	receiver := testReceiver(t, receiveDir, receiveBackend)
	receiver.config.TrustedOrigins[idOf(3)] = PublicKey(&seed)
	n, err = receiver.Process()
	if err != nil || n != 1 {
		t.Fatalf("process = %d, %v", n, err)
	}
	if len(receiveBackend.imported) != 1 || !bytes.Equal(receiveBackend.imported[0], []byte{1, 2, 3}) {
		t.Fatalf("imported = %q", receiveBackend.imported)
	}
}

func TestBridgePublishHTTP(t *testing.T) {
	dir := t.TempDir()
	bridge := testExporter(t, dir)
	var got []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		buf := new(bytes.Buffer)
		buf.ReadFrom(r.Body)
		got = buf.Bytes()
	}))
	defer server.Close()
	file := filepath.Join(dir, "a.bundle")
	if err := os.WriteFile(file, []byte("bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := bridge.PublishHTTP(file, server.URL); err != nil {
		t.Fatal(err)
	}
	if string(got) != "bytes" {
		t.Fatalf("got %q", got)
	}
	if err := bridge.PublishHTTP(file, "ftp://x"); err == nil {
		t.Fatal("bad scheme accepted")
	}
}

func TestBridgeRoles(t *testing.T) {
	dir := t.TempDir()
	exporter := testExporter(t, dir)
	if _, _, err := exporter.OpenBundle([]byte("x")); err == nil {
		t.Fatal("exporter opened bundle")
	}
	receiver := testReceiver(t, t.TempDir(), newTestBackend())
	if _, err := receiver.Export([]byte("x")); err == nil {
		t.Fatal("receiver exported")
	}
	if n, err := receiver.Capture(); err != nil || n != 0 {
		t.Fatalf("receiver capture = %d, %v", n, err)
	}
	if n, err := exporter.Process(); err != nil || n != 0 {
		t.Fatalf("exporter process = %d, %v", n, err)
	}
}

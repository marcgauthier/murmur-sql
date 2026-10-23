package replication

import (
	"bytes"
	"context"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileFrames(t *testing.T) {
	frames := []FileFrame{
		{Kind: FileRequest, Database: idOf(7), Digest: fill32(8), Generation: 2, Offset: 10},
		{Kind: FileHeader, Bytes: 100, Identity: fill32(9)},
		{Kind: FileChunkFrame, Data: []byte{1, 2, 3}},
		{Kind: FileDone},
		{Kind: FileMissing},
	}
	var buf bytes.Buffer
	for i := range frames {
		if err := WriteFileFrame(&buf, &frames[i]); err != nil {
			t.Fatal(err)
		}
	}
	for i := range frames {
		back, err := ReadFileFrame(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if back.Kind != frames[i].Kind || !bytes.Equal(EncodeFileFrame(&back), EncodeFileFrame(&frames[i])) {
			t.Fatalf("frame %d mismatch", i)
		}
	}
	if _, err := ReadFileFrame(&bytes.Buffer{}); err == nil {
		t.Fatal("empty read accepted")
	}
	bad := append([]byte("XX\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"), 0)
	if _, err := ReadFileFrame(bytes.NewReader(bad)); err == nil {
		t.Fatal("bad magic accepted")
	}
}

func TestFilePoisoned(t *testing.T) {
	b := newTestBackend()
	o, dir := openTestObjects(t, b)
	staging := filepath.Join(dir, "staging")
	if err := os.MkdirAll(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	meta := &FileMetadata{Name: "f", Digest: fill32(8), Bytes: 10, Millis: 1, Node: idOf(2)}
	path := filepath.Join(staging, "x.part")
	identity := filepath.Join(staging, "x.identity")
	if err := os.WriteFile(path, bytesRepeat(9, 64), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(identity, bytesRepeat(1, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := publishStagedFile(o, path, identity, meta); err == nil {
		t.Fatal("poisoned container published")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("poisoned container kept")
	}
	if _, err := os.Stat(identity); !os.IsNotExist(err) {
		t.Fatal("poisoned identity kept")
	}
	// Crash without identity sidecar: discard and restart.
	if err := os.WriteFile(path, bytesRepeat(9, 64), 0o600); err != nil {
		t.Fatal(err)
	}
	off, err := fileResumeOffset(path)
	if err != nil || off != 0 {
		t.Fatalf("offset = %d, %v", off, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("unsafe bytes kept")
	}
}

func TestStagingIdentityPath(t *testing.T) {
	staging := filepath.Join("staging", "aa.g0.part")
	if got := stagingIdentityPath(staging); got != filepath.Join("staging", "aa.g0.identity") {
		t.Fatalf("sidecar = %q", got)
	}
	if got := stagingIdentityPath("noext"); got != "noext.identity" {
		t.Fatalf("extensionless sidecar = %q", got)
	}
	// Resume round-trip: a partial plus the sidecar fetch writes resumes
	// instead of restarting.
	dir := t.TempDir()
	path := filepath.Join(dir, "aa.g0.part")
	if err := os.WriteFile(path, bytesRepeat(9, 100), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stagingIdentityPath(path), bytesRepeat(1, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	off, err := fileResumeOffset(path)
	if err != nil || off != 100 {
		t.Fatalf("offset = %d, %v", off, err)
	}
	// Staging protection pins the same sidecar fetch and resume use.
	b := newTestBackend()
	o, _ := openTestObjects(t, b)
	guard := o.ProtectStaging(path)
	o.stagedPaths.Lock()
	_, ok := o.staged[stagingIdentityPath(path)]
	o.stagedPaths.Unlock()
	guard.Release()
	if !ok {
		t.Fatal("sidecar not pinned")
	}
}

// A served file must arrive whole on the first attempt: the server must
// not close the connection before the client holds the terminal frame.
func TestFileFetchServeRoundTrip(t *testing.T) {
	cfg := transferConfigs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newService := func(t *testing.T, i int, node NodeId) (*fetchService, *FileObjects) {
		t.Helper()
		backend := transferBackend(node)
		dir := t.TempDir()
		fcfg := NewFilesConfig(dir, fill32(3), netip.MustParseAddrPort("127.0.0.1:0"))
		cfg[i].Files = fcfg
		objects, err := OpenFileObjects(*fcfg, backend)
		if err != nil {
			t.Fatal(err)
		}
		service, err := startFetchService(cfg[i], backend, objects, ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { service.endpoint.Close() })
		return service, objects
	}
	server, objectsA := newService(t, 0, idOf(1))
	client, objectsB := newService(t, 1, idOf(2))

	data := bytesRepeat(9, 200_000)
	meta, err := objectsA.UploadOne("big", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	source := fileSource{node: idOf(1), addr: server.endpoint.LocalAddr()}
	fetchCtx, fetchCancel := context.WithTimeout(ctx, 30*time.Second)
	defer fetchCancel()
	if err := client.fetch(fetchCtx, source, meta); err != nil {
		t.Fatalf("first-attempt fetch: %v", err)
	}
	var out bytes.Buffer
	if err := objectsB.Verify(objectsB.path(meta.Digest, meta.Generation), meta, &out); err != nil {
		t.Fatalf("verify fetched object: %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Fatal("fetched bytes differ")
	}
	// A missing object reports FileMissing, not a torn-down connection.
	missing := *meta
	missing.Digest = fill32(99)
	missCtx, missCancel := context.WithTimeout(ctx, 30*time.Second)
	defer missCancel()
	if err := client.fetch(missCtx, source, &missing); err == nil {
		t.Fatal("missing object fetched")
	}
}

// Exhausted fetch permits must defer the round, not spawn unthrottled:
// every fetch releases one permit on exit, so spawning without holding
// one corrupts the accounting and wedges later transfers.
func TestFetchMissingPermitExhaustion(t *testing.T) {
	cfg := transferConfigs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backend := transferBackend(idOf(2))
	dir := t.TempDir()
	fcfg := NewFilesConfig(dir, fill32(3), netip.MustParseAddrPort("127.0.0.1:0"))
	cfg[1].Files = fcfg
	objects, err := OpenFileObjects(*fcfg, backend)
	if err != nil {
		t.Fatal(err)
	}
	service, err := startFetchService(cfg[1], backend, objects, ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { service.endpoint.Close() })
	// Two remote-only files: metadata present, objects absent.
	for _, name := range []string{"a", "b"} {
		if _, err := objects.UploadOne(name, bytes.NewReader([]byte("data-"+name))); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".spfo" {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Fill every permit: no fetch may start until one frees up.
	for i := 0; i < fcfg.ConcurrentFetches; i++ {
		service.permits <- struct{}{}
	}
	if err := service.fetchMissing(nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := len(service.permits); got != fcfg.ConcurrentFetches {
		t.Fatalf("permits = %d, want %d (fetch spawned without one)", got, fcfg.ConcurrentFetches)
	}
}

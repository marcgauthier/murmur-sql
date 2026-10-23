package replication

import (
	"bytes"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func openTestObjects(t *testing.T, backend Backend) (*FileObjects, string) {
	t.Helper()
	dir := t.TempDir()
	o, err := OpenFileObjects(*NewFilesConfig(dir, fill32(3), netip.MustParseAddrPort("127.0.0.1:0")), backend)
	if err != nil {
		t.Fatal(err)
	}
	return o, dir
}

func TestFilesRoundtrip(t *testing.T) {
	b := newTestBackend()
	o, _ := openTestObjects(t, b)
	data := bytesRepeat(7, FileChunk*2+100)
	meta, err := o.UploadOne("big", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if meta.Bytes != uint64(len(data)) || meta.Deleted {
		t.Fatalf("meta = %+v", meta)
	}
	var out bytes.Buffer
	back, err := o.Read("big", &out)
	if err != nil || !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("read = %d bytes, %v", out.Len(), err)
	}
	if back.Digest != meta.Digest {
		t.Fatal("digest mismatch")
	}
	// Streaming download.
	dl, err := o.Download("big")
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	for {
		_, chunk, done, err := dl.Next(1000)
		if err != nil {
			dl.Close()
			t.Fatal(err)
		}
		got = append(got, chunk...)
		if done {
			break
		}
	}
	dl.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("download mismatch")
	}
	// Plain-chunk streaming with skip.
	var total uint64
	var pieces [][]byte
	_, total, err = o.StreamPlainChunks("big", 7, 4096, func(_ uint64, b []byte) error {
		pieces = append(pieces, append([]byte(nil), b...))
		return nil
	})
	if err != nil || total != uint64(len(data)) {
		t.Fatalf("stream = %d, %v", total, err)
	}
	if !bytes.Equal(bytes.Join(pieces, nil), data[7:]) {
		t.Fatal("stream mismatch")
	}
	// Delete tombstones.
	if err := o.Delete("big"); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Read("big", &bytes.Buffer{}); err == nil {
		t.Fatal("deleted file readable")
	}
	listed, err := o.List()
	if err != nil || len(listed) != 0 {
		t.Fatalf("list = %+v, %v", listed, err)
	}
}

func TestFilesTamper(t *testing.T) {
	b := newTestBackend()
	o, dir := openTestObjects(t, b)
	if _, err := o.UploadOne("f", bytes.NewReader(bytesRepeat(1, 100))); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		p := filepath.Join(dir, e.Name())
		buf, _ := os.ReadFile(p)
		buf[len(buf)-1] ^= 1
		if err := os.WriteFile(p, buf, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := o.Read("f", &bytes.Buffer{}); err == nil {
		t.Fatal("tampered file verified")
	}
}

func TestFilesStaging(t *testing.T) {
	b := newTestBackend()
	o, dir := openTestObjects(t, b)
	if err := os.MkdirAll(filepath.Join(dir, "staging"), 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "staging", "file.part")
	active := o.ProtectStaging(path)
	if err := os.WriteFile(path, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	// with_extension replaces ".part".
	sidecar := filepath.Join(dir, "staging", "file.identity")
	if err := os.WriteFile(sidecar, bytesRepeat(1, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	if n, err := o.Collect(0); err != nil || n != 0 {
		t.Fatalf("collect = %d, %v", n, err)
	}
	st, err := o.KeyRotationStatus()
	if err != nil || st.PreviousKeyUnusedLocally {
		t.Fatalf("status = %+v, %v", st, err)
	}
	active.Release()
	if n, err := o.Collect(0); err != nil || n != 2 {
		t.Fatalf("collect = %d, %v", n, err)
	}
	st, err = o.KeyRotationStatus()
	if err != nil || !st.PreviousKeyUnusedLocally {
		t.Fatalf("status = %+v, %v", st, err)
	}
}

func TestStagingReconciledAtOpen(t *testing.T) {
	b := newTestBackend()
	dir := t.TempDir()
	addr := netip.MustParseAddrPort("127.0.0.1:0")
	o, err := OpenFileObjects(*NewFilesConfig(dir, fill32(3), addr), b)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := o.UploadOne("done", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatal(err)
	}
	staging := filepath.Join(dir, "staging")
	if err := os.MkdirAll(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	nine := idOf(9)
	stem := func(digest [32]byte, generation uint64) string {
		return Hex(nine[:]) + "-" + Hex(digest[:]) + ".g" + strconv.FormatUint(generation, 10)
	}
	write := func(name string, n int) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(staging, name), bytesRepeat(7, n), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// Orphan sidecar: publish completed, sidecar removal never ran.
	write(stem(fill32(77), 0)+".identity", 32)
	// Orphan partial: no sidecar, unresumable by the resume rule.
	write(stem(fill32(78), 0)+".part", 100)
	// Completed pair: object already published, resume pointless.
	write(stem(meta.Digest, meta.Generation)+".part", 100)
	write(stem(meta.Digest, meta.Generation)+".identity", 32)
	// Valid resumable pair: partial plus sidecar, object absent.
	write(stem(fill32(79), 0)+".part", 100)
	write(stem(fill32(79), 0)+".identity", 32)

	if _, err := OpenFileObjects(*NewFilesConfig(dir, fill32(3), addr), b); err != nil {
		t.Fatal(err)
	}
	left, err := os.ReadDir(staging)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 2 {
		names := make([]string, 0, len(left))
		for _, e := range left {
			names = append(names, e.Name())
		}
		t.Fatalf("staging holds %d files after open: %v", len(left), names)
	}
	seventyNine := fill32(79)
	for _, e := range left {
		if !strings.HasPrefix(e.Name(), Hex(nine[:])+"-"+Hex(seventyNine[:])) {
			t.Fatalf("unexpected survivor: %s", e.Name())
		}
	}
	// The survivor still resumes.
	off, err := fileResumeOffset(filepath.Join(staging, stem(fill32(79), 0)+".part"))
	if err != nil || off != 100 {
		t.Fatalf("offset = %d, %v", off, err)
	}
}

func TestReapStagingOrphans(t *testing.T) {
	b := newTestBackend()
	o, dir := openTestObjects(t, b)
	meta, err := o.UploadOne("done", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatal(err)
	}
	staging := filepath.Join(dir, "staging")
	if err := os.MkdirAll(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	nine := idOf(9)
	stem := func(digest [32]byte, generation uint64) string {
		return Hex(nine[:]) + "-" + Hex(digest[:]) + ".g" + strconv.FormatUint(generation, 10)
	}
	write := func(name string, n int) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(staging, name), bytesRepeat(7, n), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	done := stem(meta.Digest, meta.Generation)
	// Reaped: lone sidecar with a published object (publish completed,
	// sidecar removal skipped: the files-resume strand shape).
	write(done+".identity", 32)
	// Reaped: lone sidecar, object absent (fetch died pre-partial).
	write(stem(fill32(71), 0)+".identity", 32)
	// Reaped: completed pair, object published.
	meta72, err := o.UploadOne("done72", bytes.NewReader([]byte("data72")))
	if err != nil {
		t.Fatal(err)
	}
	write(stem(meta72.Digest, meta72.Generation)+".part", 100)
	write(stem(meta72.Digest, meta72.Generation)+".identity", 32)
	// Reaped: lone partial, object published.
	meta2, err := o.UploadOne("done2", bytes.NewReader([]byte("data2")))
	if err != nil {
		t.Fatal(err)
	}
	write(stem(meta2.Digest, meta2.Generation)+".part", 100)
	// Kept: valid resumable pair, object absent.
	write(stem(fill32(79), 0)+".part", 100)
	write(stem(fill32(79), 0)+".identity", 32)
	// Kept: lone partial, object absent (the resume rule owns it).
	write(stem(fill32(80), 0)+".part", 100)
	// Kept: protected in-flight lone sidecar and pair.
	write(stem(fill32(81), 0)+".identity", 32)
	protSidecar := o.ProtectStaging(filepath.Join(staging, stem(fill32(81), 0)+".part"))
	defer protSidecar.Release()
	write(stem(fill32(82), 0)+".part", 100)
	write(stem(fill32(82), 0)+".identity", 32)
	protPair := o.ProtectStaging(filepath.Join(staging, stem(fill32(82), 0)+".part"))
	defer protPair.Release()

	if err := o.reapStagingOrphans(); err != nil {
		t.Fatal(err)
	}
	left, err := os.ReadDir(staging)
	if err != nil {
		t.Fatal(err)
	}
	names := make(map[string]bool, len(left))
	for _, e := range left {
		names[e.Name()] = true
	}
	for _, gone := range []string{
		done + ".identity",
		stem(fill32(71), 0) + ".identity",
		stem(meta72.Digest, meta72.Generation) + ".part",
		stem(meta72.Digest, meta72.Generation) + ".identity",
		stem(meta2.Digest, meta2.Generation) + ".part",
	} {
		if names[gone] {
			t.Fatalf("orphan survived: %s", gone)
		}
	}
	for _, kept := range []string{
		stem(fill32(79), 0) + ".part",
		stem(fill32(79), 0) + ".identity",
		stem(fill32(80), 0) + ".part",
		stem(fill32(81), 0) + ".identity",
		stem(fill32(82), 0) + ".part",
		stem(fill32(82), 0) + ".identity",
	} {
		if !names[kept] {
			t.Fatalf("live staging reaped: %s", kept)
		}
	}
	if len(names) != 6 {
		t.Fatalf("staging holds %d files, want 6 survivors", len(names))
	}
}

func TestFilesBeginFinishUpload(t *testing.T) {
	b := newTestBackend()
	o, dir := openTestObjects(t, b)
	// Incremental roundtrip: odd-sized appends across chunk edges.
	upload, err := o.BeginUpload("inc")
	if err != nil {
		t.Fatal(err)
	}
	pieces := [][]byte{bytesRepeat(1, 1000), bytesRepeat(2, 70000), bytesRepeat(3, 5)}
	for _, p := range pieces {
		if err := upload.Append(p); err != nil {
			t.Fatal(err)
		}
	}
	meta, err := o.FinishUpload(upload)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Join(pieces, nil)
	if meta.Bytes != uint64(len(want)) {
		t.Fatalf("bytes = %d, want %d", meta.Bytes, len(want))
	}
	var out bytes.Buffer
	if _, err := o.Read("inc", &out); err != nil || !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("read = %d bytes, %v", out.Len(), err)
	}
	// A finished upload belongs to its own store.
	o2, _ := openTestObjects(t, newTestBackend())
	stray, err := o.BeginUpload("stray")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o2.FinishUpload(stray); err == nil {
		t.Fatal("cross-store finish accepted")
	}
	stray.Abort()
	// Empty names are rejected.
	if _, err := o.BeginUpload(""); err == nil {
		t.Fatal("empty name accepted")
	}
	// Abort discards without publishing.
	aborted, err := o.BeginUpload("aborted")
	if err != nil {
		t.Fatal(err)
	}
	if err := aborted.Append([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	aborted.Abort()
	if meta, err := o.Metadata("aborted"); err != nil || meta != nil {
		t.Fatalf("aborted metadata = %+v, %v", meta, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".upload-") {
			t.Fatalf("aborted upload left %s", e.Name())
		}
	}
	// Abandoned (crashed) uploads are swept by collection: simulate the
	// crash by reopening, which drops the in-memory pin.
	crashed, err := o.BeginUpload("crashed")
	if err != nil {
		t.Fatal(err)
	}
	if err := crashed.Append([]byte("orphan")); err != nil {
		t.Fatal(err)
	}
	entries, err = os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var abandoned string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".upload-") {
			abandoned = filepath.Join(dir, e.Name())
		}
	}
	if abandoned == "" {
		t.Fatal("no abandoned upload on disk")
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(abandoned, old, old); err != nil {
		t.Fatal(err)
	}
	o3, err := OpenFileObjects(*NewFilesConfig(dir, fill32(3), netip.MustParseAddrPort("127.0.0.1:0")), b)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := o3.Collect(0); err != nil || n != 1 {
		t.Fatalf("collect = %d, %v", n, err)
	}
	if _, err := os.Stat(abandoned); !os.IsNotExist(err) {
		t.Fatal("abandoned upload kept")
	}
	// Over-cap appends fail.
	small := NewFilesConfig(t.TempDir(), fill32(3), netip.MustParseAddrPort("127.0.0.1:0"))
	small.MaxFileBytes = 100
	capped, err := OpenFileObjects(*small, newTestBackend())
	if err != nil {
		t.Fatal(err)
	}
	big, err := capped.BeginUpload("big")
	if err != nil {
		t.Fatal(err)
	}
	if err := big.Append(bytesRepeat(9, 200)); err == nil {
		t.Fatal("over-cap append accepted")
	}
	big.Abort()
}

func TestFileAvailabilityCodec(t *testing.T) {
	cases := []FileAvailability{
		{Kind: FileLocal},
		{Kind: FileRemote, Remote: nil},
		{Kind: FileRemote, Remote: []NodeId{idOf(3), idOf(9)}},
		{Kind: FileUnavailable},
	}
	for i, c := range cases {
		back, err := DecodeFileAvailability(EncodeFileAvailability(&c))
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if back.Kind != c.Kind || len(back.Remote) != len(c.Remote) {
			t.Fatalf("case %d: %+v", i, back)
		}
		for j := range c.Remote {
			if back.Remote[j] != c.Remote[j] {
				t.Fatalf("case %d: remote %d differs", i, j)
			}
		}
	}
	if _, err := DecodeFileAvailability(nil); err == nil {
		t.Fatal("empty availability accepted")
	}
	if _, err := DecodeFileAvailability([]byte{0xFF}); err == nil {
		t.Fatal("bad discriminant accepted")
	}
	truncated := EncodeFileAvailability(&cases[2])[:len(EncodeFileAvailability(&cases[2]))-1]
	if _, err := DecodeFileAvailability(truncated); err == nil {
		t.Fatal("truncated availability accepted")
	}
}

func TestFileKeyRotationStatusCodec(t *testing.T) {
	s := &FileKeyRotationStatus{
		CurrentGeneration: 3, OldGenerationObjects: 7,
		OldGenerationMetadataMissingLocally: 11, StagedTransfers: 13,
		PreviousKeyUnusedLocally: true,
	}
	back, err := DecodeFileKeyRotationStatus(EncodeFileKeyRotationStatus(s))
	if err != nil {
		t.Fatal(err)
	}
	if back != *s {
		t.Fatalf("roundtrip = %+v", back)
	}
	plain := *s
	plain.PreviousKeyUnusedLocally = false
	back, err = DecodeFileKeyRotationStatus(EncodeFileKeyRotationStatus(&plain))
	if err != nil || back != plain {
		t.Fatalf("roundtrip = %+v, %v", back, err)
	}
	if _, err := DecodeFileKeyRotationStatus(nil); err == nil {
		t.Fatal("empty status accepted")
	}
	enc := EncodeFileKeyRotationStatus(s)
	if _, err := DecodeFileKeyRotationStatus(enc[:len(enc)-1]); err == nil {
		t.Fatal("truncated status accepted")
	}
}

func TestFileDownloadMeta(t *testing.T) {
	b := newTestBackend()
	o, _ := openTestObjects(t, b)
	data := bytesRepeat(7, 5000)
	meta, err := o.UploadOne("f", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	dl, err := o.Download("f")
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()
	got := dl.Meta()
	if got == nil || got.Name != "f" || got.Digest != meta.Digest || got.Bytes != uint64(len(data)) {
		t.Fatalf("meta = %+v", got)
	}
}

func TestFilesAvailability(t *testing.T) {
	b := newTestBackend()
	o, _ := openTestObjects(t, b)
	a, err := o.Availability("missing")
	if err != nil || a.Kind != FileUnavailable {
		t.Fatalf("missing = %+v, %v", a, err)
	}
	meta, err := o.UploadOne("f", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatal(err)
	}
	a, err = o.Availability("f")
	if err != nil || a.Kind != FileLocal {
		t.Fatalf("local = %+v, %v", a, err)
	}
	// Simulate a remote-only file by deleting the object.
	entries, _ := os.ReadDir(o.config.Directory)
	for _, e := range entries {
		os.Remove(filepath.Join(o.config.Directory, e.Name()))
	}
	if err := o.recordPeerAvailability(meta.Digest, meta.Generation, idOf(9), true); err != nil {
		t.Fatal(err)
	}
	a, err = o.Availability("f")
	if err != nil || a.Kind != FileRemote || len(a.Remote) != 1 || a.Remote[0] != idOf(9) {
		t.Fatalf("remote = %+v, %v", a, err)
	}
}

func TestFilesRotate(t *testing.T) {
	b := newTestBackend()
	dir := t.TempDir()
	addr := netip.MustParseAddrPort("127.0.0.1:0")
	o, err := OpenFileObjects(*NewFilesConfig(dir, fill32(3), addr), b)
	if err != nil {
		t.Fatal(err)
	}
	data := bytesRepeat(5, 1000)
	if _, err := o.UploadOne("f", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	// Rotate to generation 1 with the old key available.
	cfg := NewFilesConfig(dir, fill32(4), addr)
	cfg.Generation = 1
	prev := fill32(3)
	cfg.PreviousKey = &prev
	o2, err := OpenFileObjects(*cfg, b)
	if err != nil {
		t.Fatal(err)
	}
	n, err := o2.RotateKeys(10)
	if err != nil || n != 1 {
		t.Fatalf("rotated = %d, %v", n, err)
	}
	meta, err := o2.Metadata("f")
	if err != nil || meta.Generation != 1 {
		t.Fatalf("meta = %+v, %v", meta, err)
	}
	var out bytes.Buffer
	if _, err := o2.Read("f", &out); err != nil || !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("reread = %d, %v", out.Len(), err)
	}
	// Opening without the previous key while old metadata exists fails.
	cfg2 := NewFilesConfig(dir, fill32(9), addr)
	cfg2.Generation = 2
	if _, err := OpenFileObjects(*cfg2, b); err == nil {
		t.Fatal("missing previous key accepted")
	}
}

func TestFilesListBounded(t *testing.T) {
	b := newTestBackend()
	o, _ := openTestObjects(t, b)
	if _, err := o.UploadOne("b", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatal(err)
	}
	if _, err := o.UploadOne("a", bytes.NewReader([]byte("y"))); err != nil {
		t.Fatal(err)
	}
	listed, err := o.ListBounded(1 << 20)
	if err != nil || len(listed) != 2 || listed[0].Name != "a" {
		t.Fatalf("listed = %+v, %v", listed, err)
	}
	if _, err := o.ListBounded(10); err == nil {
		t.Fatal("tiny budget accepted")
	}
}

func TestFilesCollect(t *testing.T) {
	b := newTestBackend()
	o, dir := openTestObjects(t, b)
	if _, err := o.UploadOne("keep", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(dir, "00.identity")
	_ = orphan
	digest := fill32(1)
	orphanPath := filepath.Join(dir, Hex(digest[:])+".g0.spfo")
	if err := os.WriteFile(orphanPath, []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(orphanPath, old, old); err != nil {
		t.Fatal(err)
	}
	n, err := o.Collect(time.Hour)
	if err != nil || n != 1 {
		t.Fatalf("collect = %d, %v", n, err)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatal("orphan kept")
	}
}

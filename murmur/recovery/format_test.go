package recovery

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/compression"
)

func testKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return key
}

func testCipher(t *testing.T) *Cipher {
	t.Helper()
	c, err := NewCipher(testKey())
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	return c
}

func testPlainCipher(t *testing.T) *Cipher {
	t.Helper()
	c, err := NewCipher(nil)
	if err != nil {
		t.Fatalf("plain cipher: %v", err)
	}
	return c
}

func testDBID() [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = byte(0xa0 + i)
	}
	return id
}

func testDigest() [32]byte {
	var d [32]byte
	for i := range d {
		d[i] = byte(i)
	}
	return d
}

func testManifest() *Manifest {
	return &Manifest{
		DatabaseID:      testDBID(),
		GenerationID:    3,
		CaptureCursor:   100,
		CaptureSequence: 200,
		SchemaDigest:    testDigest(),
		CreatedUnixNano: 123456789,
		CodecID:         compression.IDNone,
		Encrypted:       true,
		KeyRef:          "test-key",
		RowCount:        10,
		TableCount:      2,
		Files: []FileDescriptor{
			{Name: "table-abc-00000.rrun", Type: FileRun, Size: 100, Rows: 6,
				MinKey: []byte("a"), MaxKey: []byte("m")},
			{Name: IndexName, Type: FileIndex, Size: 50, Rows: 10},
		},
	}
}

func TestCipherRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cipher func(*testing.T) *Cipher
	}{
		{"encrypted", testCipher},
		{"plain", testPlainCipher},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.cipher(t)
			aad := []byte("test-aad")
			nonce, sealed, err := c.Seal([]byte("hello world"), aad)
			if err != nil {
				t.Fatalf("seal: %v", err)
			}
			if len(nonce) != NonceLen {
				t.Fatalf("nonce len = %d", len(nonce))
			}
			plain, err := c.Open(nonce, sealed, aad)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if string(plain) != "hello world" {
				t.Fatalf("round trip = %q", plain)
			}
			// Fresh nonces in encrypted mode.
			if c.Encrypted() {
				n2, _, err := c.Seal([]byte("hello world"), aad)
				if err != nil {
					t.Fatalf("seal2: %v", err)
				}
				if bytes.Equal(nonce, n2) {
					t.Fatal("nonce reused")
				}
			}
			// Tampered payload fails.
			bad := append([]byte(nil), sealed...)
			bad[len(bad)-1] ^= 0xff
			if _, err := c.Open(nonce, bad, aad); !IsCorrupt(err) {
				t.Fatalf("tampered payload err = %v, want corrupt", err)
			}
			// Wrong AAD fails in encrypted mode.
			if _, err := c.Open(nonce, sealed, []byte("wrong")); c.Encrypted() && !IsCorrupt(err) {
				t.Fatalf("wrong aad err = %v, want corrupt", err)
			}
		})
	}
}

func TestCipherKeySizes(t *testing.T) {
	if _, err := NewCipher(make([]byte, 16)); err == nil {
		t.Fatal("16-byte key accepted")
	}
	if _, err := NewCipher(make([]byte, 64)); err == nil {
		t.Fatal("64-byte key accepted")
	}
	c, err := NewCipher(testKey())
	if err != nil || !c.Encrypted() {
		t.Fatalf("32-byte key: %v %v", c, err)
	}
	p, err := NewCipher(nil)
	if err != nil || p.Encrypted() {
		t.Fatalf("empty key: %v %v", p, err)
	}
	// Wrong key cannot open.
	c2, _ := NewCipher(bytes.Repeat([]byte{9}, 32))
	nonce, sealed, err := c.Seal([]byte("secret"), []byte("aad"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if _, err := c2.Open(nonce, sealed, []byte("aad")); !IsCorrupt(err) {
		t.Fatalf("wrong key err = %v, want corrupt", err)
	}
}

func TestManifestRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cipher func(*testing.T) *Cipher
	}{
		{"encrypted", testCipher},
		{"plain", testPlainCipher},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.cipher(t)
			m := testManifest()
			m.Encrypted = c.Encrypted()
			sealed, err := sealManifest(c, m)
			if err != nil {
				t.Fatalf("seal: %v", err)
			}
			got, err := openManifest(c, testDBID(), sealed)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if got.GenerationID != m.GenerationID || got.RowCount != m.RowCount ||
				len(got.Files) != 2 || got.Files[0].Name != m.Files[0].Name {
				t.Fatalf("manifest mismatch: %+v", got)
			}
			// Wrong database rejected.
			var other [16]byte
			if _, err := openManifest(c, other, sealed); !IsCorrupt(err) {
				t.Fatalf("wrong db err = %v, want corrupt", err)
			}
			// Flipped body byte rejected.
			bad := append([]byte(nil), sealed...)
			bad[len(bad)-1] ^= 0x01
			if _, err := openManifest(c, testDBID(), bad); !IsCorrupt(err) {
				t.Fatalf("tampered body err = %v, want corrupt", err)
			}
			// Truncation rejected.
			if _, err := openManifest(c, testDBID(), sealed[:len(sealed)/2]); !IsCorrupt(err) {
				t.Fatalf("truncated err = %v, want corrupt", err)
			}
			// Header/body mismatch rejected.
			badHead := append([]byte(nil), sealed...)
			flagOff := len(MagicManifest) + 2
			badHead[flagOff] ^= 0x01
			if _, err := openManifest(c, testDBID(), badHead); !IsCorrupt(err) {
				t.Fatalf("header mismatch err = %v, want corrupt", err)
			}
		})
	}
}

func TestManifestFileNames(t *testing.T) {
	for _, bad := range []string{"", "../evil", "a/b", "..", ".hidden", "a\\b", "x\x00y"} {
		if err := checkFileName(bad); !IsCorrupt(err) {
			t.Fatalf("name %q accepted", bad)
		}
	}
	for _, ok := range []string{IndexName, ManifestName, "table-00-00000.rrun", "CURRENT"} {
		if err := checkFileName(ok); err != nil {
			t.Fatalf("name %q rejected: %v", ok, err)
		}
	}
	// Traversal inside a sealed manifest fails decode.
	c := testPlainCipher(t)
	m := testManifest()
	m.Encrypted = false
	m.Files[0].Name = "../../evil"
	sealed, err := sealManifest(c, m)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if _, err := openManifest(c, testDBID(), sealed); !IsCorrupt(err) {
		t.Fatalf("traversal manifest err = %v, want corrupt", err)
	}
}

func TestCurrentFormat(t *testing.T) {
	raw := FormatCurrent("generation-00000007", HashBytes([]byte("manifest")))
	name, hash, err := ParseCurrent(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if name != "generation-00000007" || hash != HashBytes([]byte("manifest")) {
		t.Fatalf("current = %q %x", name, hash)
	}
	for _, bad := range []string{
		"", "generation-1\n", "generation-00000007",
		"generation-00000007\nzzzz\n", "generation-00000007\n00\n",
		"../generation-00000007\n" + fmt.Sprintf("%064x", 0) + "\n",
		"generation-0000000x\n" + fmt.Sprintf("%064x", 0) + "\n",
	} {
		if _, _, err := ParseCurrent([]byte(bad)); !IsCorrupt(err) {
			t.Fatalf("CURRENT %q accepted", bad)
		}
	}
}

func TestGenerationDirNames(t *testing.T) {
	if got := GenerationDirName(42); got != "generation-00000042" {
		t.Fatalf("name = %q", got)
	}
	for _, tc := range []struct {
		name string
		id   uint64
		ok   bool
	}{
		{"generation-00000042", 42, true},
		{"generation-00000000", 0, true},
		{"generation-42", 0, false},
		{"generation-0000000x", 0, false},
		{"generation-000000421", 0, false},
		{".building-abc", 0, false},
		{"CURRENT", 0, false},
	} {
		id, ok := ParseGenerationDir(tc.name)
		if ok != tc.ok || (ok && id != tc.id) {
			t.Fatalf("parse %q = (%d, %v)", tc.name, id, ok)
		}
	}
}

func TestBlockRoundTrip(t *testing.T) {
	c := testCipher(t)
	var buf bytes.Buffer
	aad := func(block uint32) []byte {
		return BlockAAD(testDBID(), 1, testDigest(), "test", block)
	}
	w := newBlockWriter(&buf, c, compression.MustZstd(3), "test", aad, 1024)
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		payload := appendBytes(nil, []byte(k))
		if err := w.Add([]byte(k), payload); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	r := newBlockReader(&buf, c, compression.MustZstd(3), "test", aad, MaxBlockPlain)
	first, last, plain, rows, err := r.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if string(first) != "a" || string(last) != "e" || rows != 5 || len(plain) == 0 {
		t.Fatalf("block = %q %q rows=%d plain=%d", first, last, rows, len(plain))
	}
	if _, _, _, _, err := r.Next(); err != io.EOF {
		t.Fatalf("second block err = %v, want EOF", err)
	}
}

func TestBlockCorruption(t *testing.T) {
	build := func(t *testing.T) []byte {
		c := testCipher(t)
		var buf bytes.Buffer
		aad := func(block uint32) []byte {
			return BlockAAD(testDBID(), 1, testDigest(), "test", block)
		}
		w := newBlockWriter(&buf, c, compression.MustZstd(3), "test", aad, 1024)
		if err := w.Add([]byte("k"), appendBytes(nil, []byte("v"))); err != nil {
			t.Fatalf("add: %v", err)
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		return buf.Bytes()
	}
	read := func(t *testing.T, raw []byte) error {
		c := testCipher(t)
		aad := func(block uint32) []byte {
			return BlockAAD(testDBID(), 1, testDigest(), "test", block)
		}
		r := newBlockReader(bytes.NewReader(raw), c, compression.MustZstd(3), "test", aad, MaxBlockPlain)
		_, _, _, _, err := r.Next()
		return err
	}
	t.Run("flip-tail", func(t *testing.T) {
		raw := build(t)
		raw[len(raw)-1] ^= 0xff
		if err := read(t, raw); !IsCorrupt(err) {
			t.Fatalf("err = %v, want corrupt", err)
		}
	})
	t.Run("truncate", func(t *testing.T) {
		raw := build(t)
		if err := read(t, raw[:len(raw)/2]); !IsCorrupt(err) {
			t.Fatalf("err = %v, want corrupt", err)
		}
	})
	t.Run("huge-claim", func(t *testing.T) {
		// min key length claims 2^32-1.
		raw := []byte{0xff, 0xff, 0xff, 0xff, 0x0f}
		if err := read(t, raw); !IsCorrupt(err) {
			t.Fatalf("err = %v, want corrupt", err)
		}
	})
	t.Run("empty", func(t *testing.T) {
		if err := read(t, nil); !errors.Is(err, io.EOF) {
			t.Fatalf("err = %v, want EOF", err)
		}
	})
}

func testRows(n int) []Row {
	rows := make([]Row, 0, n)
	for i := 0; i < n; i++ {
		table := "t1"
		if i%3 == 0 {
			table = "t2"
		}
		rows = append(rows, Row{
			Table: table,
			Sort:  []byte(fmt.Sprintf("sort-%06d", i)),
			Key:   []byte(fmt.Sprintf("04/meta/row/%s/%064x", table, i*2654435761)),
			Seq:   uint64(i + 1),
			Data:  []byte(fmt.Sprintf("payload-%d-%s", i, string(make([]byte, i%64)))),
		})
	}
	return rows
}

func testWriterConfig(dir string, cipher *Cipher, codec compression.Codec) WriterConfig {
	return WriterConfig{
		Dir:               dir,
		DatabaseID:        testDBID(),
		GenerationID:      1,
		CaptureCursor:     7,
		CaptureSequence:   9,
		SchemaDigest:      testDigest(),
		Codec:             codec,
		Cipher:            cipher,
		KeyRef:            "test-key",
		TargetRunBytes:    1 << 20,
		TargetBlockBytes:  4096,
		MaxMemory:         1 << 20,
		RetainGenerations: 2,
		Less:              ByteOrder,
	}
}

func testOpener(gcm, plain *Cipher) *Opener {
	return &Opener{CipherFor: func(keyRef string, encrypted bool) (*Cipher, error) {
		if keyRef != "test-key" {
			return nil, fmt.Errorf("unknown key ref %q", keyRef)
		}
		if encrypted {
			return gcm, nil
		}
		return plain, nil
	}}
}

func collectRows(t *testing.T, s *RunStream) []*Row {
	t.Helper()
	var out []*Row
	for {
		r, err := s.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		out = append(out, r)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return out
}

func collectIndex(t *testing.T, s *IndexStream) []*IndexEntry {
	t.Helper()
	var out []*IndexEntry
	for {
		e, err := s.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("index stream: %v", err)
		}
		out = append(out, e)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return out
}

func TestSorterMemoryAndSpill(t *testing.T) {
	c := testCipher(t)
	for _, maxBytes := range []int64{1 << 30, 2048} {
		s, err := NewSorter("test", ByteOrder, maxBytes, t.TempDir(), c, compression.MustZstd(3),
			testDBID(), 1, testDigest())
		if err != nil {
			t.Fatalf("sorter: %v", err)
		}
		rows := testRows(500)
		// Feed in reverse to prove sorting (not input order).
		for i := len(rows) - 1; i >= 0; i-- {
			if err := s.Add(rows[i]); err != nil {
				t.Fatalf("add: %v", err)
			}
		}
		if maxBytes < 1<<30 && s.Spills() == 0 {
			t.Fatal("expected spill files under tiny budget")
		}
		if maxBytes >= 1<<30 && s.Spills() != 0 {
			t.Fatal("unexpected spills with huge budget")
		}
		var got []*Row
		if err := s.Drain(func(r *Row) error {
			got = append(got, &Row{Table: r.Table, Sort: append([]byte(nil), r.Sort...),
				Key: append([]byte(nil), r.Key...), Seq: r.Seq, Data: append([]byte(nil), r.Data...)})
			return nil
		}); err != nil {
			t.Fatalf("drain: %v", err)
		}
		if len(got) != len(rows) {
			t.Fatalf("drained %d rows, want %d", len(got), len(rows))
		}
		for i := 1; i < len(got); i++ {
			if ByteOrder(got[i], got[i-1]) {
				t.Fatalf("order violation at %d (budget %d)", i, maxBytes)
			}
		}
		// Payloads survive the round trip.
		want := map[string]string{}
		for _, r := range rows {
			want[string(r.Key)] = string(r.Data)
		}
		for _, r := range got {
			if want[string(r.Key)] != string(r.Data) {
				t.Fatalf("payload mismatch for %q", r.Key)
			}
		}
	}
}

func TestSorterSpillsAreEncrypted(t *testing.T) {
	c := testCipher(t)
	dir := t.TempDir()
	s, err := NewSorter("test", ByteOrder, 1024, dir, c, compression.MustZstd(3),
		testDBID(), 1, testDigest())
	if err != nil {
		t.Fatalf("sorter: %v", err)
	}
	secret := bytes.Repeat([]byte("s3cret!"), 64)
	for i := 0; i < 100; i++ {
		if err := s.Add(Row{Table: "t", Sort: []byte{byte(i)}, Key: []byte{byte(i)}, Data: secret}); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	if s.Spills() == 0 {
		t.Fatal("expected spills")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("read spill: %v", err)
		}
		if bytes.Contains(raw, secret) {
			t.Fatalf("plaintext secret in %s", e.Name())
		}
	}
	count := 0
	if err := s.Drain(func(*Row) error { count++; return nil }); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if count != 100 {
		t.Fatalf("drained %d, want 100", count)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("spills left behind: %v", entries)
	}
}

func buildTestGeneration(t *testing.T, dir string, cipher *Cipher, codec compression.Codec, gen uint64, rows []Row, mutate func(*WriterConfig)) *Manifest {
	t.Helper()
	cfg := testWriterConfig(dir, cipher, codec)
	cfg.GenerationID = gen
	if mutate != nil {
		mutate(&cfg)
	}
	w, err := NewWriter(cfg)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	for _, r := range rows {
		if err := w.Add(r); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	m, err := w.Finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	return m
}

func TestWriterReaderRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cipher func(*testing.T) *Cipher
		codec  func() compression.Codec
	}{
		{"encrypted-zstd", testCipher, func() compression.Codec { return compression.MustZstd(3) }},
		{"encrypted-none", testCipher, func() compression.Codec { return compression.None }},
		{"plain-zstd", testPlainCipher, func() compression.Codec { return compression.MustZstd(3) }},
		{"plain-none", testPlainCipher, func() compression.Codec { return compression.None }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.cipher(t)
			dir := t.TempDir()
			rows := testRows(500)
			m := buildTestGeneration(t, dir, c, tc.codec(), 1, rows, nil)
			if m.RowCount != uint64(len(rows)) || m.TableCount != 2 {
				t.Fatalf("manifest counts = %d/%d", m.RowCount, m.TableCount)
			}
			g, err := Discover(dir, testDBID(), testOpener(testCipher(t), testPlainCipher(t)))
			if err != nil {
				t.Fatalf("discover: %v", err)
			}
			if g.Manifest.GenerationID != 1 {
				t.Fatalf("generation = %d", g.Manifest.GenerationID)
			}
			rs, err := g.OpenRuns()
			if err != nil {
				t.Fatalf("open runs: %v", err)
			}
			got := collectRows(t, rs)
			if len(got) != len(rows) {
				t.Fatalf("streamed %d rows, want %d", len(got), len(rows))
			}
			want := map[string]Row{}
			for _, r := range rows {
				want[string(r.Key)] = r
			}
			for i, r := range got {
				w, ok := want[string(r.Key)]
				if !ok {
					t.Fatalf("unknown key %q", r.Key)
				}
				if r.Table != w.Table || !bytes.Equal(r.Sort, w.Sort) ||
					r.Seq != w.Seq || !bytes.Equal(r.Data, w.Data) {
					t.Fatalf("row %d mismatch", i)
				}
				if i > 0 && ByteOrder(r, got[i-1]) {
					t.Fatalf("order violation at %d", i)
				}
			}
			is, err := g.OpenIndex()
			if err != nil {
				t.Fatalf("open index: %v", err)
			}
			entries := collectIndex(t, is)
			if len(entries) != len(rows) {
				t.Fatalf("index has %d entries, want %d", len(entries), len(rows))
			}
			for i, e := range entries {
				w, ok := want[string(e.Key)]
				if !ok || e.Seq != w.Seq {
					t.Fatalf("index entry %d mismatch", i)
				}
				if i > 0 && bytes.Compare(e.Key, entries[i-1].Key) < 0 {
					t.Fatalf("index order violation at %d", i)
				}
			}
		})
	}
}

func TestPublishedFilesHaveNoPlaintext(t *testing.T) {
	c := testCipher(t)
	dir := t.TempDir()
	secret := bytes.Repeat([]byte("topsecret"), 64)
	rows := []Row{{Table: "t", Sort: []byte("s"), Key: []byte("k"), Seq: 1, Data: secret}}
	buildTestGeneration(t, dir, c, compression.MustZstd(3), 1, rows, nil)
	count := 0
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		count++
		if bytes.Contains(raw, secret) {
			t.Errorf("plaintext secret in %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if count == 0 {
		t.Fatal("no files written")
	}
}

func TestEmptyGeneration(t *testing.T) {
	c := testCipher(t)
	dir := t.TempDir()
	m := buildTestGeneration(t, dir, c, compression.MustZstd(3), 1, nil, nil)
	if m.RowCount != 0 {
		t.Fatalf("row count = %d", m.RowCount)
	}
	g, err := Discover(dir, testDBID(), testOpener(c, testPlainCipher(t)))
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	rs, err := g.OpenRuns()
	if err != nil {
		t.Fatalf("open runs: %v", err)
	}
	if got := collectRows(t, rs); len(got) != 0 {
		t.Fatalf("rows = %d", len(got))
	}
	is, err := g.OpenIndex()
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	if got := collectIndex(t, is); len(got) != 0 {
		t.Fatalf("index entries = %d", len(got))
	}
}

func TestRunSplitting(t *testing.T) {
	c := testCipher(t)
	dir := t.TempDir()
	m := buildTestGeneration(t, dir, c, compression.None, 1, testRows(200), func(cfg *WriterConfig) {
		cfg.TargetRunBytes = 2048
		cfg.TargetBlockBytes = 512
	})
	runs := 0
	for _, f := range m.Files {
		if f.Type == FileRun {
			runs++
		}
	}
	if runs < 2 {
		t.Fatalf("runs = %d, want >= 2", runs)
	}
	g, err := Discover(dir, testDBID(), testOpener(c, testPlainCipher(t)))
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	rs, err := g.OpenRuns()
	if err != nil {
		t.Fatalf("open runs: %v", err)
	}
	if got := collectRows(t, rs); len(got) != 200 {
		t.Fatalf("rows = %d", len(got))
	}
}

func TestRetention(t *testing.T) {
	c := testCipher(t)
	p := testPlainCipher(t)
	dir := t.TempDir()
	for gen := uint64(1); gen <= 3; gen++ {
		buildTestGeneration(t, dir, c, compression.None, gen, testRows(10), nil)
	}
	g, err := Discover(dir, testDBID(), testOpener(c, p))
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if g.Manifest.GenerationID != 3 {
		t.Fatalf("current generation = %d, want 3", g.Manifest.GenerationID)
	}
	for _, want := range []string{"generation-00000002", "generation-00000003"} {
		if _, err := os.Stat(filepath.Join(dir, want)); err != nil {
			t.Fatalf("missing %s: %v", want, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "generation-00000001")); !os.IsNotExist(err) {
		t.Fatal("generation 1 not pruned")
	}
}

func TestDiscoverFallbacks(t *testing.T) {
	c := testCipher(t)
	p := testPlainCipher(t)
	t.Run("missing-dir", func(t *testing.T) {
		if _, err := Discover(filepath.Join(t.TempDir(), "nope"), testDBID(), testOpener(c, p)); !errors.Is(err, ErrNoCache) {
			t.Fatalf("err = %v, want ErrNoCache", err)
		}
	})
	t.Run("wrong-db", func(t *testing.T) {
		dir := t.TempDir()
		buildTestGeneration(t, dir, c, compression.None, 1, testRows(10), nil)
		var other [16]byte
		if _, err := Discover(dir, other, testOpener(c, p)); !errors.Is(err, ErrNoCache) {
			t.Fatalf("err = %v, want ErrNoCache", err)
		}
	})
	t.Run("corrupt-current-falls-back-to-scan", func(t *testing.T) {
		dir := t.TempDir()
		buildTestGeneration(t, dir, c, compression.None, 1, testRows(10), nil)
		if err := os.WriteFile(filepath.Join(dir, CurrentName), []byte("garbage"), 0o600); err != nil {
			t.Fatalf("corrupt current: %v", err)
		}
		g, err := Discover(dir, testDBID(), testOpener(c, p))
		if err != nil {
			t.Fatalf("discover: %v", err)
		}
		if g.Manifest.GenerationID != 1 {
			t.Fatalf("generation = %d", g.Manifest.GenerationID)
		}
	})
	t.Run("missing-file-skips-generation", func(t *testing.T) {
		dir := t.TempDir()
		buildTestGeneration(t, dir, c, compression.None, 1, testRows(10), nil)
		buildTestGeneration(t, dir, c, compression.None, 2, testRows(10), func(cfg *WriterConfig) {
			cfg.RetainGenerations = 5
		})
		// Delete a run from generation 2 (CURRENT target).
		g2, err := Discover(dir, testDBID(), testOpener(c, p))
		if err != nil {
			t.Fatalf("discover: %v", err)
		}
		removed := false
		for _, f := range g2.Manifest.Files {
			if f.Type == FileRun {
				if err := os.Remove(filepath.Join(dir, GenerationDirName(2), f.Name)); err != nil {
					t.Fatalf("remove: %v", err)
				}
				removed = true
				break
			}
		}
		if !removed {
			t.Fatal("no run to remove")
		}
		g, err := Discover(dir, testDBID(), testOpener(c, p))
		if err != nil {
			t.Fatalf("discover: %v", err)
		}
		if g.Manifest.GenerationID != 1 {
			t.Fatalf("generation = %d, want 1", g.Manifest.GenerationID)
		}
	})
	t.Run("unknown-key-skips-generation", func(t *testing.T) {
		dir := t.TempDir()
		buildTestGeneration(t, dir, c, compression.None, 1, testRows(10), nil)
		opener := &Opener{CipherFor: func(string, bool) (*Cipher, error) {
			return nil, fmt.Errorf("key pruned")
		}}
		if _, err := Discover(dir, testDBID(), opener); !errors.Is(err, ErrNoCache) {
			t.Fatalf("err = %v, want ErrNoCache", err)
		}
	})
}

func TestStreamCorruption(t *testing.T) {
	c := testCipher(t)
	p := testPlainCipher(t)
	flip := func(t *testing.T, path string, off int) {
		t.Helper()
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		raw[off] ^= 0xff
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	t.Run("run-byte-flip", func(t *testing.T) {
		dir := t.TempDir()
		buildTestGeneration(t, dir, c, compression.MustZstd(3), 1, testRows(100), nil)
		g, err := Discover(dir, testDBID(), testOpener(c, p))
		if err != nil {
			t.Fatalf("discover: %v", err)
		}
		var runName string
		for _, f := range g.Manifest.Files {
			if f.Type == FileRun {
				runName = f.Name
				break
			}
		}
		// Flip a byte late in the file (past headers) to exercise the
		// streaming path rather than open-time checks.
		flip(t, filepath.Join(dir, GenerationDirName(1), runName), 300)
		rs, err := g.OpenRuns()
		if err != nil {
			t.Fatalf("open runs: %v", err)
		}
		defer rs.Close()
		for {
			_, err := rs.Next()
			if err == io.EOF {
				t.Fatal("corrupt run streamed clean")
			}
			if err != nil {
				if !IsCorrupt(err) {
					t.Fatalf("err = %v, want corrupt", err)
				}
				return
			}
		}
	})
	t.Run("run-truncate", func(t *testing.T) {
		dir := t.TempDir()
		buildTestGeneration(t, dir, c, compression.None, 1, testRows(100), nil)
		g, err := Discover(dir, testDBID(), testOpener(c, p))
		if err != nil {
			t.Fatalf("discover: %v", err)
		}
		for _, f := range g.Manifest.Files {
			if f.Type == FileRun {
				path := filepath.Join(dir, GenerationDirName(1), f.Name)
				if err := os.Truncate(path, int64(f.Size-10)); err != nil {
					t.Fatalf("truncate: %v", err)
				}
			}
		}
		// Size mismatch fails at discovery (older generations absent).
		if _, err := Discover(dir, testDBID(), testOpener(c, p)); !errors.Is(err, ErrNoCache) {
			t.Fatalf("err = %v, want ErrNoCache", err)
		}
	})
	t.Run("index-byte-flip", func(t *testing.T) {
		dir := t.TempDir()
		buildTestGeneration(t, dir, c, compression.None, 1, testRows(100), nil)
		g, err := Discover(dir, testDBID(), testOpener(c, p))
		if err != nil {
			t.Fatalf("discover: %v", err)
		}
		flip(t, filepath.Join(dir, GenerationDirName(1), IndexName), 100)
		is, err := g.OpenIndex()
		if err == nil {
			defer is.Close()
			for {
				_, err := is.Next()
				if err == io.EOF {
					t.Fatal("corrupt index streamed clean")
				}
				if err != nil {
					if !IsCorrupt(err) {
						t.Fatalf("err = %v, want corrupt", err)
					}
					return
				}
			}
		}
		if !IsCorrupt(err) {
			t.Fatalf("open index err = %v, want corrupt", err)
		}
	})
	t.Run("manifest-tamper-skips-generation", func(t *testing.T) {
		dir := t.TempDir()
		buildTestGeneration(t, dir, c, compression.None, 1, testRows(10), nil)
		flip(t, filepath.Join(dir, GenerationDirName(1), ManifestName), 40)
		if _, err := Discover(dir, testDBID(), testOpener(c, p)); !errors.Is(err, ErrNoCache) {
			t.Fatalf("err = %v, want ErrNoCache", err)
		}
	})
}

func TestReaderComparatorMismatch(t *testing.T) {
	// Builders and readers must share the row comparator; a mismatch
	// fails the stream instead of silently reordering.
	c := testCipher(t)
	p := testPlainCipher(t)
	dir := t.TempDir()
	buildTestGeneration(t, dir, c, compression.None, 1, testRows(100), nil)
	opener := testOpener(c, p)
	opener.Less = func(a, b *Row) bool { return ByteOrder(b, a) } // reversed
	g, err := Discover(dir, testDBID(), opener)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	rs, err := g.OpenRuns()
	if err != nil {
		t.Fatalf("open runs: %v", err)
	}
	defer rs.Close()
	for {
		_, err := rs.Next()
		if err == io.EOF {
			t.Fatal("mismatched comparator streamed clean")
		}
		if err != nil {
			if !IsCorrupt(err) {
				t.Fatalf("err = %v, want corrupt", err)
			}
			return
		}
	}
}

func TestCrashHooks(t *testing.T) {
	c := testCipher(t)
	p := testPlainCipher(t)
	opener := testOpener(c, p)
	t.Run("abort-before-publish", func(t *testing.T) {
		dir := t.TempDir()
		cfg := testWriterConfig(dir, c, compression.None)
		cfg.Hook = func(PublishStage) error { return fmt.Errorf("simulated crash") }
		w, err := NewWriter(cfg)
		if err != nil {
			t.Fatalf("writer: %v", err)
		}
		staging := w.Staging()
		for _, r := range testRows(50) {
			if err := w.Add(r); err != nil {
				t.Fatalf("add: %v", err)
			}
		}
		if _, err := w.Finish(); err == nil {
			t.Fatal("finish succeeded despite crash hook")
		}
		// Staging debris remains; no generation is discoverable.
		if _, err := os.Stat(staging); err != nil {
			t.Fatalf("staging missing: %v", err)
		}
		if _, err := Discover(dir, testDBID(), opener); !errors.Is(err, ErrNoCache) {
			t.Fatalf("err = %v, want ErrNoCache", err)
		}
		CleanupStaging(dir)
		if entries, _ := os.ReadDir(dir); len(entries) != 0 {
			t.Fatalf("debris left: %v", entries)
		}
		// A retry with a fresh generation succeeds.
		buildTestGeneration(t, dir, c, compression.None, 1, testRows(50), nil)
		if _, err := Discover(dir, testDBID(), opener); err != nil {
			t.Fatalf("rediscover: %v", err)
		}
	})
	t.Run("abort-after-publish", func(t *testing.T) {
		dir := t.TempDir()
		cfg := testWriterConfig(dir, c, compression.None)
		cfg.Hook = func(s PublishStage) error {
			if s == StagePublished {
				return fmt.Errorf("simulated crash")
			}
			return nil
		}
		w, err := NewWriter(cfg)
		if err != nil {
			t.Fatalf("writer: %v", err)
		}
		for _, r := range testRows(50) {
			if err := w.Add(r); err != nil {
				t.Fatalf("add: %v", err)
			}
		}
		if _, err := w.Finish(); err == nil {
			t.Fatal("finish succeeded despite crash hook")
		}
		// Complete generation, no CURRENT: discoverable by scan.
		if _, err := os.Stat(filepath.Join(dir, CurrentName)); !os.IsNotExist(err) {
			t.Fatalf("current status = %v", err)
		}
		g, err := Discover(dir, testDBID(), opener)
		if err != nil {
			t.Fatalf("discover: %v", err)
		}
		if g.Manifest.GenerationID != 1 {
			t.Fatalf("generation = %d", g.Manifest.GenerationID)
		}
	})
	t.Run("abort-after-current", func(t *testing.T) {
		dir := t.TempDir()
		buildTestGeneration(t, dir, c, compression.None, 1, testRows(10), nil)
		cfg := testWriterConfig(dir, c, compression.None)
		cfg.GenerationID = 2
		cfg.RetainGenerations = 1
		cfg.Hook = func(s PublishStage) error {
			if s == StageCurrentUpdated {
				return fmt.Errorf("simulated crash")
			}
			return nil
		}
		w, err := NewWriter(cfg)
		if err != nil {
			t.Fatalf("writer: %v", err)
		}
		for _, r := range testRows(10) {
			if err := w.Add(r); err != nil {
				t.Fatalf("add: %v", err)
			}
		}
		if _, err := w.Finish(); err == nil {
			t.Fatal("finish succeeded despite crash hook")
		}
		// CURRENT points at 2; pruning skipped; both survive.
		g, err := Discover(dir, testDBID(), opener)
		if err != nil {
			t.Fatalf("discover: %v", err)
		}
		if g.Manifest.GenerationID != 2 {
			t.Fatalf("generation = %d, want 2", g.Manifest.GenerationID)
		}
		if _, err := os.Stat(filepath.Join(dir, GenerationDirName(1))); err != nil {
			t.Fatalf("gen 1 missing: %v", err)
		}
		// Next build prunes normally.
		buildTestGeneration(t, dir, c, compression.None, 3, testRows(10), func(cfg *WriterConfig) {
			cfg.RetainGenerations = 1
		})
		if _, err := os.Stat(filepath.Join(dir, GenerationDirName(1))); !os.IsNotExist(err) {
			t.Fatal("gen 1 not pruned by next build")
		}
	})
}

func TestFilePermissions(t *testing.T) {
	c := testCipher(t)
	dir := filepath.Join(t.TempDir(), "recovery")
	buildTestGeneration(t, dir, c, compression.None, 1, testRows(10), nil)
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		mode := info.Mode().Perm()
		if info.IsDir() {
			if mode != 0o700 {
				t.Errorf("dir %s mode %o", path, mode)
			}
			return nil
		}
		if mode != 0o600 {
			t.Errorf("file %s mode %o", path, mode)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}

func TestNextGenerationID(t *testing.T) {
	dir := t.TempDir()
	if id, err := NextGenerationID(filepath.Join(dir, "missing")); err != nil || id != 1 {
		t.Fatalf("missing dir = (%d, %v)", id, err)
	}
	if id, err := NextGenerationID(dir); err != nil || id != 1 {
		t.Fatalf("empty dir = (%d, %v)", id, err)
	}
	for _, name := range []string{"generation-00000001", "generation-00000007", "CURRENT", ".building-x"} {
		if name == "CURRENT" {
			if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := os.Mkdir(filepath.Join(dir, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if id, err := NextGenerationID(dir); err != nil || id != 8 {
		t.Fatalf("populated dir = (%d, %v)", id, err)
	}
}

func TestBlockRecordBound(t *testing.T) {
	c := testCipher(t)
	var buf bytes.Buffer
	aad := func(block uint32) []byte {
		return BlockAAD(testDBID(), 1, testDigest(), "test", block)
	}
	w := newBlockWriter(&buf, c, compression.None, "test", aad, MaxBlockPlain)
	if err := w.Add([]byte("k"), make([]byte, MaxBlockPlain+1)); err == nil {
		t.Fatal("oversized record accepted")
	}
	// Two over-half-max records split across blocks, both readable.
	rec := make([]byte, MaxBlockPlain/2+16)
	if err := w.Add([]byte("a"), rec); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := w.Add([]byte("b"), rec); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	r := newBlockReader(&buf, c, compression.None, "test", aad, MaxBlockPlain)
	blocks := 0
	for {
		_, _, _, _, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		blocks++
	}
	if blocks != 2 {
		t.Fatalf("blocks = %d, want 2", blocks)
	}
}

func TestWriterValidation(t *testing.T) {
	c := testCipher(t)
	bad := func(mut func(*WriterConfig)) WriterConfig {
		cfg := testWriterConfig(t.TempDir(), c, compression.None)
		mut(&cfg)
		return cfg
	}
	for i, cfg := range []WriterConfig{
		bad(func(c *WriterConfig) { c.Dir = "" }),
		bad(func(c *WriterConfig) { c.Codec = nil }),
		bad(func(c *WriterConfig) { c.Cipher = nil }),
		bad(func(c *WriterConfig) { c.Less = nil }),
		bad(func(c *WriterConfig) { c.TargetRunBytes = 0 }),
		bad(func(c *WriterConfig) { c.TargetBlockBytes = 0 }),
		bad(func(c *WriterConfig) { c.TargetBlockBytes = MaxBlockPlain + 1 }),
		bad(func(c *WriterConfig) { c.MaxMemory = 0 }),
		bad(func(c *WriterConfig) { c.RetainGenerations = 0 }),
		bad(func(c *WriterConfig) { c.KeyRef = string(make([]byte, MaxKeyRefLen+1)) }),
	} {
		if _, err := NewWriter(cfg); err == nil {
			t.Fatalf("config %d accepted", i)
		}
	}
	// Duplicate generation id fails instead of clobbering.
	dir := t.TempDir()
	buildTestGeneration(t, dir, c, compression.None, 1, testRows(5), nil)
	cfg := testWriterConfig(dir, c, compression.None)
	w, err := NewWriter(cfg)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := w.Add(testRows(1)[0]); err != nil {
		t.Fatalf("add: %v", err)
	}
	if _, err := w.Finish(); err == nil {
		t.Fatal("duplicate generation id accepted")
	}
}

package recovery

import (
	"bytes"
	"io"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/compression"
)

// FuzzDecodeManifest feeds arbitrary bytes to the manifest decoder: no
// panic, no hang, and every error must be corruption.
func FuzzDecodeManifest(f *testing.F) {
	c := mustTestCipher()
	m := testManifest()
	m.Encrypted = true
	sealed, err := sealManifest(c, m)
	if err != nil {
		f.Fatalf("seal: %v", err)
	}
	f.Add(sealed)
	f.Add([]byte("MRRC"))
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xff}, 64))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			t.Skip("input too large")
		}
		got, err := openManifest(c, testDBID(), raw)
		if err != nil {
			if !IsCorrupt(err) {
				t.Fatalf("non-corrupt error: %v", err)
			}
			return
		}
		// Valid decodes re-encode deterministically.
		rt, err := openManifest(c, testDBID(), mustSeal(t, c, got))
		if err != nil {
			t.Fatalf("re-open: %v", err)
		}
		if rt.GenerationID != got.GenerationID || len(rt.Files) != len(got.Files) {
			t.Fatal("round trip mismatch")
		}
	})
}

// FuzzBlockStream feeds arbitrary bytes to the block reader: no panic,
// bounded allocation, corruption-only errors.
func FuzzBlockStream(f *testing.F) {
	c := mustTestCipher()
	var buf bytes.Buffer
	aad := func(block uint32) []byte {
		return BlockAAD(testDBID(), 1, testDigest(), "fuzz", block)
	}
	w := newBlockWriter(&buf, c, compression.None, "fuzz", aad, 256)
	_ = w.Add([]byte("k"), appendBytes(nil, []byte("v")))
	_ = w.Flush()
	f.Add(buf.Bytes())
	f.Add([]byte{})
	f.Add([]byte{0x01})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			t.Skip("input too large")
		}
		r := newBlockReader(bytes.NewReader(raw), c, compression.None, "fuzz", aad, MaxBlockPlain)
		for {
			_, _, _, _, err := r.Next()
			if err == io.EOF {
				return
			}
			if err != nil {
				if !IsCorrupt(err) {
					t.Fatalf("non-corrupt error: %v", err)
				}
				return
			}
		}
	})
}

// FuzzRunRecords feeds arbitrary block plaintext to the run-record
// splitter: no panic, corruption-only errors, exact consumption.
func FuzzRunRecords(f *testing.F) {
	f.Add(encodeRunRecord(&Row{Table: "t", Sort: []byte("s"), Key: []byte("k"), Seq: 7, Data: []byte("d")}))
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			t.Skip("input too large")
		}
		r := &byteReader{buf: raw}
		for len(r.buf) > 0 && r.err == nil {
			if _, err := decodeRunRecord(r, "t"); err != nil {
				if !IsCorrupt(err) {
					t.Fatalf("non-corrupt error: %v", err)
				}
				return
			}
		}
		if r.err != nil && !IsCorrupt(r.err) {
			t.Fatalf("non-corrupt error: %v", r.err)
		}
	})
}

// FuzzCurrent feeds arbitrary bytes to the CURRENT parser.
func FuzzCurrent(f *testing.F) {
	f.Add(FormatCurrent("generation-00000001", HashBytes([]byte("m"))))
	f.Add([]byte("generation-00000001\n"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 4096 {
			t.Skip("input too large")
		}
		name, hash, err := ParseCurrent(raw)
		if err != nil {
			if !IsCorrupt(err) {
				t.Fatalf("non-corrupt error: %v", err)
			}
			return
		}
		if _, ok := ParseGenerationDir(name); !ok {
			t.Fatalf("bad generation %q accepted", name)
		}
		_ = hash
	})
}

func mustTestCipher() *Cipher {
	c, err := NewCipher(testKey())
	if err != nil {
		panic(err)
	}
	return c
}

func mustSeal(t *testing.T, c *Cipher, m *Manifest) []byte {
	t.Helper()
	sealed, err := sealManifest(c, m)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	return sealed
}

package spool_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/spool"
)

// testMasterKey is a fixed 32-byte key for deterministic tests
// (salts and nonces stay random per write).
var testMasterKey = []byte("0123456789abcdef0123456789abcdef")

// testOptions returns small, fast options using the default cipher.
func testOptions(tb testing.TB, dir string) spool.Options {
	tb.Helper()
	o := spool.DefaultOptions(dir)
	o.MasterKey = testMasterKey
	o.Flush.MaxDelay = -1 // deterministic: flush only on demand
	o.ReclaimInterval = -1
	return o
}

// loadAll collects a full load into a last-write-wins map.
func loadAll(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	got := make(map[string][]byte)
	seqs := make(map[string]uint64)
	err := spool.Load(dir, testMasterKey, func(recs []spool.Record) error {
		for _, r := range recs {
			if r.Sequence < seqs[string(r.Key)] {
				continue
			}
			seqs[string(r.Key)] = r.Sequence
			if r.Deleted {
				delete(got, string(r.Key))
			} else {
				got[string(r.Key)] = append([]byte(nil), r.Value...)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return got
}

func TestRoundTripAllModes(t *testing.T) {
	encs := []spool.Encryption{spool.EncryptionNone, spool.EncryptionAES256GCM}
	comps := []spool.Compression{spool.CompressionNone, spool.CompressionDeflate}
	for _, enc := range encs {
		for _, comp := range comps {
			name := enc.String() + "/" + comp.String()
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				o := testOptions(t, dir)
				o.Encryption = enc
				o.Compression = comp
				if enc == spool.EncryptionNone {
					o.MasterKey = nil
				}
				st, err := spool.Open(o)
				if err != nil {
					t.Fatalf("Open: %v", err)
				}
				want := make(map[string]string)
				for i := 0; i < 500; i++ {
					k := fmt.Sprintf("key-%04d", i)
					// Compressible values exercise the codec.
					v := fmt.Sprintf("value-%04d-%s", i, string(bytes.Repeat([]byte("x"), i%97)))
					want[k] = v
					if err := st.Put([]byte(k), []byte(v)); err != nil {
						t.Fatalf("Put: %v", err)
					}
				}
				// Overwrite every 7th key to exercise replacement.
				for i := 0; i < 500; i += 7 {
					k := fmt.Sprintf("key-%04d", i)
					want[k] = "replaced"
					if err := st.Put([]byte(k), []byte("replaced")); err != nil {
						t.Fatalf("Put: %v", err)
					}
				}
				if err := st.Flush(); err != nil {
					t.Fatalf("Flush: %v", err)
				}
				stats := st.Stats()
				if stats.Keys != 500 {
					t.Fatalf("Keys = %d, want 500", stats.Keys)
				}
				if stats.BlocksWritten == 0 || stats.BytesWritten == 0 {
					t.Fatalf("missing write stats: %+v", stats)
				}
				if err := st.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				// Close is idempotent.
				if err := st.Close(); err != nil {
					t.Fatalf("second Close: %v", err)
				}

				var got map[string][]byte
				if enc == spool.EncryptionNone {
					got = make(map[string][]byte)
					seqs := make(map[string]uint64)
					err := spool.LoadWithOptions(spool.LoadOptions{Path: dir}, func(recs []spool.Record) error {
						for _, r := range recs {
							if r.Sequence < seqs[string(r.Key)] {
								continue
							}
							seqs[string(r.Key)] = r.Sequence
							got[string(r.Key)] = append([]byte(nil), r.Value...)
						}
						return nil
					})
					if err != nil {
						t.Fatalf("Load: %v", err)
					}
				} else {
					got = loadAll(t, dir)
				}
				if len(got) != len(want) {
					t.Fatalf("loaded %d keys, want %d", len(got), len(want))
				}
				for k, v := range want {
					if string(got[k]) != v {
						t.Fatalf("key %q = %q, want %q", k, got[k], v)
					}
				}
			})
		}
	}
}

func TestOpenAndLoad(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 100; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Close(); err != nil { // Close flushes.
		t.Fatalf("Close: %v", err)
	}
	count := 0
	st2, err := spool.OpenAndLoad(testOptions(t, dir), func(recs []spool.Record) error {
		count += len(recs)
		for _, r := range recs {
			if r.Deleted {
				t.Errorf("unexpected tombstone for %q", r.Key)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("OpenAndLoad: %v", err)
	}
	defer st2.Close()
	if count != 100 {
		t.Fatalf("callback saw %d records, want 100", count)
	}
	if got := st2.Stats().Keys; got != 100 {
		t.Fatalf("Keys = %d, want 100", got)
	}
}

func TestPassphraseMode(t *testing.T) {
	dir := t.TempDir()
	o := spool.DefaultOptions(dir)
	o.Passphrase = "correct horse battery staple"
	o.Flush.MaxDelay = -1
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := make(map[string]string)
	err = spool.LoadWithOptions(spool.LoadOptions{Path: dir, Passphrase: o.Passphrase}, func(recs []spool.Record) error {
		for _, r := range recs {
			got[string(r.Key)] = string(r.Value)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got["k"] != "v" {
		t.Fatalf("got %q, want v", got["k"])
	}
	// Wrong passphrase fails.
	err = spool.LoadWithOptions(spool.LoadOptions{Path: dir, Passphrase: "wrong"}, func([]spool.Record) error {
		return nil
	})
	if !errors.Is(err, spool.ErrWrongKey) {
		t.Fatalf("wrong passphrase err = %v, want ErrWrongKey", err)
	}
}

func TestOpenValidation(t *testing.T) {
	key := testMasterKey
	cases := []struct {
		name   string
		mutate func(*spool.Options)
	}{
		{"no path", func(o *spool.Options) { o.Path = "" }},
		{"no key", func(o *spool.Options) { o.MasterKey = nil }},
		{"short key", func(o *spool.Options) { o.MasterKey = []byte("short") }},
		{"key and passphrase", func(o *spool.Options) { o.Passphrase = "x" }},
		{"key with none", func(o *spool.Options) { o.Encryption = spool.EncryptionNone }},
		{"non-pow2 write shards", func(o *spool.Options) { o.WriteShards = 100 }},
		{"non-pow2 index shards", func(o *spool.Options) { o.IndexShards = 3 }},
		{"threshold over one", func(o *spool.Options) { o.CompactionThreshold = 1.5 }},
		{"negative threshold", func(o *spool.Options) { o.CompactionThreshold = -0.5 }},
		{"oversize key limit", func(o *spool.Options) { o.MaxKeySize = o.MaxBlockBytes }},
		{"negative free-space reserve", func(o *spool.Options) { o.CompactionMinFreeBytes = -1 }},
		{"unknown encryption", func(o *spool.Options) { o.Encryption = spool.Encryption(99) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := spool.DefaultOptions(t.TempDir())
			o.MasterKey = key
			tc.mutate(&o)
			st, err := spool.Open(o)
			if err == nil {
				st.Close()
				t.Fatalf("Open succeeded, want error")
			}
		})
	}
}

func TestWrongKeyFails(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	o := testOptions(t, dir)
	o.MasterKey = bytes.Repeat([]byte{0x42}, 32)
	if _, err := spool.Open(o); !errors.Is(err, spool.ErrWrongKey) {
		t.Fatalf("Open with wrong key err = %v, want ErrWrongKey", err)
	}
	// Encrypted store refuses plaintext options and vice versa.
	o2 := testOptions(t, dir)
	o2.Encryption = spool.EncryptionNone
	o2.MasterKey = nil
	if _, err := spool.Open(o2); err == nil {
		t.Fatalf("Open encrypted store with EncryptionNone succeeded")
	}
	plainDir := t.TempDir()
	po := spool.DefaultOptions(plainDir)
	po.Encryption = spool.EncryptionNone
	ps, err := spool.Open(po)
	if err != nil {
		t.Fatalf("Open plain: %v", err)
	}
	ps.Close()
	po2 := spool.DefaultOptions(plainDir)
	po2.MasterKey = testMasterKey
	if _, err := spool.Open(po2); err == nil {
		t.Fatalf("Open plain store with key succeeded")
	}
}

func TestDoubleOpenLocked(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if _, err := spool.Open(testOptions(t, dir)); !errors.Is(err, spool.ErrLocked) {
		t.Fatalf("second Open err = %v, want ErrLocked", err)
	}
}

func TestUseAfterClose(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); !errors.Is(err, spool.ErrClosed) {
		t.Fatalf("Put err = %v, want ErrClosed", err)
	}
	if err := st.Delete([]byte("k")); !errors.Is(err, spool.ErrClosed) {
		t.Fatalf("Delete err = %v, want ErrClosed", err)
	}
	if err := st.Flush(); !errors.Is(err, spool.ErrClosed) {
		t.Fatalf("Flush err = %v, want ErrClosed", err)
	}
	if err := st.Reclaim(); !errors.Is(err, spool.ErrClosed) {
		t.Fatalf("Reclaim err = %v, want ErrClosed", err)
	}
}

func TestSizeLimits(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.MaxKeySize = 16
	o.MaxValueSize = 32
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if err := st.Put(bytes.Repeat([]byte("k"), 17), []byte("v")); !errors.Is(err, spool.ErrKeyTooLarge) {
		t.Fatalf("oversize key err = %v, want ErrKeyTooLarge", err)
	}
	if err := st.Put([]byte("k"), bytes.Repeat([]byte("v"), 33)); !errors.Is(err, spool.ErrValueTooLarge) {
		t.Fatalf("oversize value err = %v, want ErrValueTooLarge", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
}

func TestLargeValueOwnBlock(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.TargetBlockBytes = 64 << 10 // 5MB value dwarfs the target
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	big := bytes.Repeat([]byte("0123456789abcdef"), 5<<20/16)
	if err := st.Put([]byte("small"), []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Put([]byte("big"), big); err != nil {
		t.Fatalf("Put big: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if !bytes.Equal(got["big"], big) {
		t.Fatalf("big value mismatch (len %d)", len(got["big"]))
	}
	if string(got["small"]) != "x" {
		t.Fatalf("small value = %q", got["small"])
	}
}

func TestDurabilityModes(t *testing.T) {
	for _, d := range []spool.Durability{spool.DurabilityAsync, spool.DurabilityFlush, spool.DurabilitySync} {
		t.Run(d.String(), func(t *testing.T) {
			dir := t.TempDir()
			o := testOptions(t, dir)
			o.Durability = d
			st, err := spool.Open(o)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			for i := 0; i < 50; i++ {
				if err := st.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
					t.Fatalf("Put: %v", err)
				}
			}
			if err := st.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if err := st.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if got := loadAll(t, dir); len(got) != 50 {
				t.Fatalf("loaded %d keys, want 50", len(got))
			}
		})
	}
}

func TestStatsBasic(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	// Highly compressible values: expect a real ratio.
	val := bytes.Repeat([]byte("a"), 1000)
	for i := 0; i < 200; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%04d", i)), val); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	s := st.Stats()
	if s.Keys != 200 || s.LiveRecords != 200 {
		t.Fatalf("Keys=%d Live=%d, want 200/200", s.Keys, s.LiveRecords)
	}
	if s.BlocksWritten == 0 || s.BytesWritten == 0 {
		t.Fatalf("missing write stats: %+v", s)
	}
	if s.CompressionRatio <= 1.5 {
		t.Fatalf("CompressionRatio = %f, want > 1.5 for repetitive data", s.CompressionRatio)
	}
	if s.PendingRecords != 0 || s.PendingBytes != 0 {
		t.Fatalf("pending not drained: %+v", s)
	}
	if s.Segments == 0 || s.DiskBytes == 0 || s.LiveBytes == 0 {
		t.Fatalf("missing disk stats: %+v", s)
	}
}

func TestLoadStandaloneMissing(t *testing.T) {
	if err := spool.Load(filepath.Join(t.TempDir(), "nope"), testMasterKey, func([]spool.Record) error {
		return nil
	}); err == nil {
		t.Fatalf("Load of missing dir succeeded")
	}
}

func TestLoadFuncErrorAborts(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	sentinel := errors.New("stop")
	err = spool.Load(dir, testMasterKey, func([]spool.Record) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("Load err = %v, want sentinel", err)
	}
}

func TestManifestAndKeysLayout(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, name := range []string{"manifest", "keys.enc", "LOCK"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
	}
	segs, err := os.ReadDir(filepath.Join(dir, "segments"))
	if err != nil {
		t.Fatalf("segments dir: %v", err)
	}
	if len(segs) != 1 || segs[0].Name() != "000000000001.spool" {
		t.Fatalf("segments = %v, want single 000000000001.spool", segs)
	}
}

// TestLoadDeliversCurrentOnce builds three generations across
// flushed files and requires standalone Load to deliver each key
// exactly once with its current version, tombstones included.
func TestLoadDeliversCurrentOnce(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 60; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v1")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	for i := 0; i < 60; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v2")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	for i := 0; i < 20; i++ {
		if err := st.Delete([]byte(fmt.Sprintf("k%02d", i))); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	seen := make(map[string]int)
	var tombs, callbacks int
	err = spool.Load(dir, testMasterKey, func(recs []spool.Record) error {
		if len(recs) == 0 {
			t.Fatalf("empty callback delivered")
		}
		callbacks++
		for _, r := range recs {
			seen[string(r.Key)]++
			if r.Deleted {
				tombs++
				continue
			}
			if string(r.Value) != "v2" {
				t.Fatalf("key %q delivered stale value %q", r.Key, r.Value)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(seen) != 60 {
		t.Fatalf("delivered %d distinct keys, want 60", len(seen))
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("key %q delivered %d times, want once", k, n)
		}
	}
	if tombs != 20 {
		t.Fatalf("delivered %d tombstones, want 20", tombs)
	}
	if callbacks == 0 {
		t.Fatalf("no callbacks fired")
	}
}

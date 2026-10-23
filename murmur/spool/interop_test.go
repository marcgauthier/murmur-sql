package spool_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// Rust <-> Go byte-compatibility: block v3, manifest v3, keys.enc v3.
//
//   - testdata/gostore-{enc,plain}: written by the original Go spool
//     (interop dataset, compat key 32 x 0x42), also read by the Rust
//     crate's interop suite.
//   - testdata/ruststore-enc: written by the Rust crate's writestore
//     example (same dataset and key).
//
// The Go port must decode identical record sets from all three, over
// every read path (offline load, ordered open, point Get, key scans).

var compatKey = bytes.Repeat([]byte{0x42}, 32)

// interopDataset is the expected final state: 52 live keys plus the
// key-000/key-001 tombstones.
func interopDataset() (live map[string]string, tombs []string) {
	live = map[string]string{
		"batch-a": "1", "batch-b": "22",
		"commit-x": "xxx", "empty-val": "",
	}
	for i := 2; i < 50; i++ {
		if i == 10 {
			continue
		}
		live[fmt.Sprintf("key-%03d", i)] = fmt.Sprintf("value-%d", i)
	}
	live["key-010"] = "v3-final"
	return live, []string{"key-000", "key-001"}
}

func copyFixture(t *testing.T, name string) string {
	t.Helper()
	src := filepath.Join("testdata", name)
	dst := t.TempDir()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("fixture %s: %v", name, err)
	}
	copyDir(t, src, dst, entries)
	// Segments live one level down; copyDir handles nesting.
	return dst
}

func copyDir(t *testing.T, src, dst string, entries []os.DirEntry) {
	t.Helper()
	for _, e := range entries {
		sp, dp := filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())
		if e.IsDir() {
			if err := os.Mkdir(dp, 0o700); err != nil {
				t.Fatalf("mkdir: %v", err)
			}
			sub, err := os.ReadDir(sp)
			if err != nil {
				t.Fatalf("readdir: %v", err)
			}
			copyDir(t, sp, dp, sub)
			continue
		}
		raw, err := os.ReadFile(sp)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if err := os.WriteFile(dp, raw, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
}

func checkDataset(t *testing.T, live map[string][]byte, tombs map[string]bool) {
	t.Helper()
	wantLive, wantTombs := interopDataset()
	if len(live) != len(wantLive) {
		t.Fatalf("live keys = %d, want %d", len(live), len(wantLive))
	}
	for k, want := range wantLive {
		if got, ok := live[k]; !ok || string(got) != want {
			t.Fatalf("key %q = %q, want %q", k, got, want)
		}
	}
	if len(tombs) != len(wantTombs) {
		t.Fatalf("tombs = %d, want %d", len(tombs), len(wantTombs))
	}
	for _, k := range wantTombs {
		if !tombs[k] {
			t.Fatalf("missing tomb %q", k)
		}
	}
}

func TestInteropFixtures(t *testing.T) {
	for name, key := range map[string][]byte{
		"gostore-enc":   compatKey,
		"gostore-plain": nil,
		"ruststore-enc": compatKey,
	} {
		t.Run(name, func(t *testing.T) {
			dir := copyFixture(t, name)
			// Offline load sees every winner exactly once.
			live := make(map[string][]byte)
			tombs := make(map[string]bool)
			if err := spool.Load(dir, key, func(recs []spool.Record) error {
				for _, r := range recs {
					if r.Deleted {
						tombs[string(r.Key)] = true
					} else {
						live[string(r.Key)] = append([]byte(nil), r.Value...)
					}
				}
				return nil
			}); err != nil {
				t.Fatalf("load: %v", err)
			}
			checkDataset(t, live, tombs)

			// Ordered open delivers the same set, sorted.
			var keys []string
			o := spool.DefaultOptions(dir)
			o.MasterKey = key
			o.ReclaimInterval = -1
			if key == nil {
				o.Encryption = spool.EncryptionNone
			}
			s, err := spool.OpenAndLoadOrdered(o, func(recs []spool.Record) error {
				for _, r := range recs {
					keys = append(keys, string(r.Key))
				}
				return nil
			})
			if err != nil {
				t.Fatalf("ordered open: %v", err)
			}
			defer s.Close()
			if len(keys) == 0 {
				t.Fatalf("no records delivered")
			}
			for i := 1; i < len(keys); i++ {
				if keys[i-1] >= keys[i] {
					t.Fatalf("unordered delivery at %d", i)
				}
			}
			// Point reads and scans over foreign-written data.
			got, err := s.Get([]byte("key-010"))
			if err != nil || string(got) != "v3-final" {
				t.Fatalf("get = %q, %v", got, err)
			}
			if v, err := s.Get([]byte("key-000")); err != nil || v != nil {
				t.Fatalf("deleted get = %q, %v", v, err)
			}
			if all := s.KeysWithPrefix([]byte("key-")); len(all) != 48 {
				t.Fatalf("key- scan = %d keys, want 48", len(all))
			}
			var pages int
			var after []byte
			for {
				page := s.KeysPage(nil, after, 10)
				if len(page) == 0 {
					break
				}
				pages += len(page)
				after = page[len(page)-1]
			}
			if pages != 52 {
				t.Fatalf("paged scan = %d keys, want 52", pages)
			}
		})
	}
}

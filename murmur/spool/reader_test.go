package spool_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/spool"
)

func openReaderStore(t *testing.T, dir string, plain bool) *spool.Store {
	t.Helper()
	o := testOptions(t, dir)
	if plain {
		o.Encryption = spool.EncryptionNone
		o.MasterKey = nil
	}
	s, err := spool.Open(o)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestGetRoundTrip(t *testing.T) {
	for _, plain := range []bool{false, true} {
		t.Run(fmt.Sprintf("plain=%v", plain), func(t *testing.T) {
			dir := t.TempDir()
			s := openReaderStore(t, dir, plain)
			pairs := map[string][]byte{
				"a/b/1":    []byte("one"),
				"a/b/2":    []byte(""),
				"a/c/deep": {0, 1, 2, 255},
				"zzz":      bytes.Repeat([]byte{7}, 100000),
			}
			for k, v := range pairs {
				if err := s.Put([]byte(k), v); err != nil {
					t.Fatalf("put %s: %v", k, err)
				}
			}
			// Pending puts are invisible until flushed.
			if v, err := s.Get([]byte("a/b/1")); err != nil || v != nil {
				t.Fatalf("unflushed get = %q, %v", v, err)
			}
			if err := s.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			for k, want := range pairs {
				got, err := s.Get([]byte(k))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("get %s = %d bytes, %v", k, len(got), err)
				}
				// Second read exercises the hot cache path.
				again, err := s.Get([]byte(k))
				if err != nil || !bytes.Equal(again, want) {
					t.Fatalf("cached get %s failed: %v", k, err)
				}
			}
			if v, err := s.Get([]byte("missing")); err != nil || v != nil {
				t.Fatalf("missing = %q, %v", v, err)
			}
			if err := s.Delete([]byte("a/b/1")); err != nil {
				t.Fatalf("delete: %v", err)
			}
			if err := s.Flush(); err != nil {
				t.Fatalf("flush2: %v", err)
			}
			if v, err := s.Get([]byte("a/b/1")); err != nil || v != nil {
				t.Fatalf("deleted = %q, %v", v, err)
			}
			if err := s.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			s2 := openReaderStore(t, dir, plain)
			got, err := s2.Get([]byte("zzz"))
			if err != nil || len(got) != 100000 {
				t.Fatalf("reopened get = %d bytes, %v", len(got), err)
			}
			if v, err := s2.Get([]byte("a/b/1")); err != nil || v != nil {
				t.Fatalf("reopened deleted = %q, %v", v, err)
			}
		})
	}
}

func TestGetClosed(t *testing.T) {
	dir := t.TempDir()
	s := openReaderStore(t, dir, false)
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := s.Get([]byte("k")); !errors.Is(err, spool.ErrClosed) {
		t.Fatalf("get after close = %v, want ErrClosed", err)
	}
}

func TestKeysPrefixAndPages(t *testing.T) {
	dir := t.TempDir()
	s := openReaderStore(t, dir, false)
	var want []string
	for i := range 50 {
		k := fmt.Sprintf("a/b/%02d/x", i)
		want = append(want, k)
		if err := s.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	for _, k := range []string{"a/c/1", "zzz", "a/b/1"} {
		if err := s.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	full := s.KeysWithPrefix([]byte("a/b/"))
	if len(full) != 51 {
		t.Fatalf("prefix keys = %d, want 51", len(full))
	}
	for i := 1; i < len(full); i++ {
		if bytes.Compare(full[i-1], full[i]) >= 0 {
			t.Fatalf("keys not sorted at %d", i)
		}
	}
	var pages [][]byte
	var after []byte
	for {
		page := s.KeysPage([]byte("a/b/"), after, 7)
		if len(page) == 0 {
			break
		}
		after = page[len(page)-1]
		pages = append(pages, page...)
	}
	if len(pages) != len(full) {
		t.Fatalf("paged = %d keys, want %d", len(pages), len(full))
	}
	for i := range full {
		if !bytes.Equal(pages[i], full[i]) {
			t.Fatalf("page key %d mismatch", i)
		}
	}
	if got := s.KeysPage([]byte("a/b/"), nil, 0); len(got) != 0 {
		t.Fatalf("limit 0 = %d keys", len(got))
	}
	if got := s.KeysWithPrefix([]byte("nope/")); len(got) != 0 {
		t.Fatalf("missing prefix = %d keys", len(got))
	}
	// Deletions disappear from scans after flush.
	if err := s.Delete([]byte("a/b/00/x")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("flush2: %v", err)
	}
	if got := s.KeysWithPrefix([]byte("a/b/")); len(got) != 50 {
		t.Fatalf("after delete = %d keys, want 50", len(got))
	}
}

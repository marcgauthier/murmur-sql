package spool

import (
	"fmt"
	"hash/crc32"
	"math"
	"sync"
	"testing"
)

// Pinned against the Rust google-crc32c vectors: the Go port uses the
// standard library Castagnoli table and must produce identical values.
func TestCRC32CStandardVectors(t *testing.T) {
	if got := crc32.Checksum(nil, castagnoli); got != 0 {
		t.Fatalf("empty = %#x", got)
	}
	if got := crc32.Checksum([]byte("123456789"), castagnoli); got != 0xe3069283 {
		t.Fatalf("digits = %#x", got)
	}
	zeros := make([]byte, 32)
	if got := crc32.Checksum(zeros, castagnoli); got != 0x8a9136aa {
		t.Fatalf("zeros = %#x", got)
	}
	ones := bytesRepeat(0xff, 32)
	if got := crc32.Checksum(ones, castagnoli); got != 0x62a8ab43 {
		t.Fatalf("ones = %#x", got)
	}
	buf := []byte("x123456789padding")
	if got := crc32.Checksum(buf[1:10], castagnoli); got != 0xe3069283 {
		t.Fatalf("unaligned = %#x", got)
	}
}

func testLocation(seq uint64) Location {
	return Location{FileID: 1, BlockID: seq, BlockOffset: 0, RecordIndex: 0, Sequence: seq, Size: 16}
}

func scannedMemory(ix *index) uint64 {
	var total uint64
	for i := range ix.shards {
		sh := &ix.shards[i]
		sh.mu.RLock()
		for k := range sh.values {
			total += uint64(len(k)) + perEntryBytes
		}
		for k := range sh.tombs {
			total += uint64(len(k)) + perEntryBytes
		}
		sh.mu.RUnlock()
	}
	return total
}

func scannedKeys(ix *index, prefix string) [][]byte {
	var keys [][]byte
	for i := range ix.shards {
		sh := &ix.shards[i]
		sh.mu.RLock()
		for k := range sh.values {
			if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
				keys = append(keys, []byte(k))
			}
		}
		sh.mu.RUnlock()
	}
	sortBytes(keys)
	return keys
}

func sortBytes(keys [][]byte) {
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && string(keys[j]) < string(keys[j-1]); j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
}

func equalKeySets(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if string(a[i]) != string(b[i]) {
			return false
		}
	}
	return true
}

func TestIndexPrefixHintsAndPages(t *testing.T) {
	ix := newIndex(256)
	fs := newFileStats()
	for seq := uint64(1); seq <= 500; seq++ {
		key := fmt.Sprintf("a/b/%d/%d", seq%7, seq%53)
		ix.commitRecord(fs, key, seq, seq%3 == 0, testLocation(seq), 16)
	}
	specials := []string{"\xff/\x00/\xfe/one", "a//b/", "a/b/", "plain", ""}
	for i, key := range specials {
		seq := uint64(1000 + i)
		ix.commitRecord(fs, key, seq, false, testLocation(seq), 16)
	}
	if got, want := ix.memoryEstimate(), scannedMemory(ix); got != want {
		t.Fatalf("memory = %d, scanned %d", got, want)
	}
	prefixes := []string{"", "a", "a/", "a/b", "a/b/", "a/b/1/", "a/b/missing/", "a//b/", "\xff/\x00/\xfe/", "plain/"}
	for _, prefix := range prefixes {
		expected := scannedKeys(ix, prefix)
		if got := ix.keysWithPrefix([]byte(prefix)); !equalKeySets(got, expected) {
			t.Fatalf("prefix %q: %d keys, want %d", prefix, len(got), len(expected))
		}
		var actual [][]byte
		var after []byte
		for {
			page := ix.keysPage([]byte(prefix), after, 5)
			if len(page) == 0 {
				break
			}
			after = page[len(page)-1]
			actual = append(actual, page...)
		}
		if !equalKeySets(actual, expected) {
			t.Fatalf("prefix %q pages: %d keys, want %d", prefix, len(actual), len(expected))
		}
	}
	// Saturated/colliding hints degrade to searching, never wrong results.
	for i := range ix.shards {
		for w := range ix.shards[i].namespaces {
			ix.shards[i].namespaces[w].Store(math.MaxUint64)
		}
	}
	if got := ix.keysWithPrefix([]byte("a/b/1/")); !equalKeySets(got, scannedKeys(ix, "a/b/1/")) {
		t.Fatalf("saturated hints changed results")
	}
	if got := ix.keysWithPrefix([]byte("a/b/missing/")); len(got) != 0 {
		t.Fatalf("saturated missing prefix = %d keys", len(got))
	}
}

func TestIndexConcurrentInsertsVisible(t *testing.T) {
	ix := newIndex(256)
	fs := newFileStats()
	var wg sync.WaitGroup
	for thread := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := uint64(1); seq <= 200; seq++ {
				key := fmt.Sprintf("a/b/%d/%d", thread, seq)
				ix.commitRecord(fs, key, seq, false, testLocation(seq), 16)
				found := false
				for _, k := range ix.keysWithPrefix([]byte(fmt.Sprintf("a/b/%d/", thread))) {
					if string(k) == key {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("inserted key %q not found", key)
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := ix.keysWithPrefix([]byte("a/b/")); !equalKeySets(got, scannedKeys(ix, "a/b/")) {
		t.Fatalf("concurrent scan mismatch")
	}
}

func TestIndexMemoryAccounting(t *testing.T) {
	ix := newIndex(4)
	fs := newFileStats()
	ix.commitRecord(fs, "k1", 1, false, testLocation(1), 16)
	ix.commitRecord(fs, "k2", 1, true, testLocation(1), 16)
	want := 2*(2+perEntryBytes) + 0
	if got := ix.memoryEstimate(); got != uint64(want) {
		t.Fatalf("memory = %d, want %d", got, want)
	}
	// Replacement keeps the charge.
	ix.commitRecord(fs, "k1", 2, false, testLocation(2), 16)
	ix.commitRecord(fs, "k2", 2, false, testLocation(2), 16)
	if got := ix.memoryEstimate(); got != uint64(want) {
		t.Fatalf("after replace memory = %d, want %d", got, want)
	}
	// Stale records add nothing.
	ix.commitRecord(fs, "k1", 1, false, testLocation(1), 16)
	if got := ix.memoryEstimate(); got != uint64(want) {
		t.Fatalf("after stale memory = %d, want %d", got, want)
	}
	// Dropping a tomb releases its charge.
	ix.commitRecord(fs, "k3", 3, true, testLocation(3), 16)
	if _, ok := ix.dropTomb("k3", 3, 1); !ok {
		t.Fatalf("dropTomb failed")
	}
	if got := ix.memoryEstimate(); got != uint64(want) {
		t.Fatalf("after drop memory = %d, want %d", got, want)
	}
}

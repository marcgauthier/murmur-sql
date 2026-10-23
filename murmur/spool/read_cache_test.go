package spool

import "testing"

func testBlock(size int) *cacheBlock {
	return &cacheBlock{
		frame:   bytesRepeat(1, 32),
		plain:   bytesRepeat(7, size),
		records: nil,
	}
}

func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func admitCache(c *readCache, key cacheKey, b *cacheBlock) {
	c.insert(key, b)
	c.insert(key, b)
}

func TestCacheDisabledAndOversized(t *testing.T) {
	b := testBlock(100)
	for _, capacity := range []int{0, b.bytes() - 1} {
		c := newReadCache(capacity)
		admitCache(c, cacheKey{1, 0}, b)
		if got := c.get(cacheKey{1, 0}); got != nil {
			t.Fatalf("capacity %d: cached disabled/oversized block", capacity)
		}
		if c.bytes != 0 {
			t.Fatalf("capacity %d: bytes = %d", capacity, c.bytes)
		}
	}
}

func TestCacheEvictionBoundedLRU(t *testing.T) {
	b := testBlock(100)
	c := newReadCache(b.bytes() * 2)
	admitCache(c, cacheKey{1, 0}, b)
	admitCache(c, cacheKey{2, 0}, b)
	if c.get(cacheKey{1, 0}) == nil {
		t.Fatalf("missing admitted block")
	}
	admitCache(c, cacheKey{3, 0}, b)
	if c.get(cacheKey{2, 0}) != nil {
		t.Fatalf("LRU victim survived")
	}
	if c.get(cacheKey{1, 0}) == nil || c.get(cacheKey{3, 0}) == nil {
		t.Fatalf("hot blocks evicted")
	}
	if c.bytes != b.bytes()*2 {
		t.Fatalf("bytes = %d, want %d", c.bytes, b.bytes()*2)
	}
}

func TestCacheScanProbation(t *testing.T) {
	b := testBlock(100)
	c := newReadCache(b.bytes())
	admitCache(c, cacheKey{1, 0}, b)
	for seq := uint64(2); seq < 10000; seq++ {
		c.insert(cacheKey{seq, 0}, b)
		if c.bytes > c.limit {
			t.Fatalf("bytes %d exceeds limit %d", c.bytes, c.limit)
		}
	}
	if c.get(cacheKey{1, 0}) == nil {
		t.Fatalf("scan displaced hot block")
	}
	if len(c.probation) != 4096 || len(c.seen) != 4096 {
		t.Fatalf("probation=%d seen=%d, want 4096/4096", len(c.probation), len(c.seen))
	}
}

func TestCacheClearKeepsReaders(t *testing.T) {
	b := testBlock(100)
	c := newReadCache(b.bytes())
	admitCache(c, cacheKey{1, 0}, b)
	reader := c.get(cacheKey{1, 0})
	if reader == nil {
		t.Fatalf("missing admitted block")
	}
	c.clear()
	if len(reader.plain) != 100 || reader.plain[0] != 7 {
		t.Fatalf("clear invalidated active reader")
	}
	if c.bytes != 0 || len(c.entries) != 0 || len(c.seen) != 0 {
		t.Fatalf("clear left bytes=%d entries=%d seen=%d", c.bytes, len(c.entries), len(c.seen))
	}
}

func TestCacheDuplicateInsert(t *testing.T) {
	b := testBlock(100)
	c := newReadCache(b.bytes())
	admitCache(c, cacheKey{1, 0}, b)
	for range 100 {
		c.insert(cacheKey{1, 0}, b)
	}
	if c.bytes != b.bytes() || len(c.entries) != 1 {
		t.Fatalf("bytes=%d entries=%d after duplicates", c.bytes, len(c.entries))
	}
}

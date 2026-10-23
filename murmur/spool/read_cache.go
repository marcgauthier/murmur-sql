package spool

import "container/list"

// Byte-bounded hot block cache. A bounded probation list keeps
// sequential scans from replacing repeatedly accessed blocks. No
// application value index: entries are disposable decoded blocks.
//
// Port of the Rust read_cache module (which itself follows this
// package's eviction design): two-touch admission, clock-ordered
// eviction, byte accounting. One deliberate deviation: evicted
// plaintext is not wiped (Go shares the block with active readers,
// so wiping would corrupt them; collection is left to the GC).

// cacheKey identifies a cached block: segment file plus block offset.
type cacheKey struct {
	file   uint64
	offset int64
}

// cacheBlock keeps the sealed frame, the plaintext body, and the
// decoded records aliasing the plaintext.
type cacheBlock struct {
	frame   []byte
	plain   []byte
	records []Record
}

// bytes estimates the block's memory: buffers plus decoded-record
// overhead plus a per-entry allowance.
func (b *cacheBlock) bytes() int {
	return cap(b.frame) + cap(b.plain) + cap(b.records)*64 + 128
}

type cacheEntry struct {
	key   cacheKey
	block *cacheBlock
	size  int
}

// probationCap bounds the first-touch admission list.
const probationCap = 4096

type readCache struct {
	limit     int
	bytes     int
	order     *list.List // front is most recently used
	entries   map[cacheKey]*list.Element
	seen      map[cacheKey]struct{}
	probation []cacheKey
}

func newReadCache(limit int) *readCache {
	return &readCache{
		limit:   limit,
		order:   list.New(),
		entries: make(map[cacheKey]*list.Element),
		seen:    make(map[cacheKey]struct{}),
	}
}

// get returns the cached block and marks it most recently used.
func (c *readCache) get(key cacheKey) *cacheBlock {
	if e, ok := c.entries[key]; ok {
		c.order.MoveToFront(e)
		return e.Value.(*cacheEntry).block
	}
	return nil
}

// insert admits a block on second touch, evicting least recently
// used entries until it fits. Disabled (zero limit) and oversized
// blocks are never cached; duplicate inserts are no-ops.
func (c *readCache) insert(key cacheKey, block *cacheBlock) {
	size := block.bytes()
	if c.limit == 0 || size > c.limit {
		return
	}
	if _, ok := c.entries[key]; ok {
		return
	}
	if _, ok := c.seen[key]; !ok {
		if len(c.probation) == probationCap {
			old := c.probation[0]
			copy(c.probation, c.probation[1:])
			c.probation = c.probation[:len(c.probation)-1]
			delete(c.seen, old)
		}
		c.seen[key] = struct{}{}
		c.probation = append(c.probation, key)
		return
	}
	for size > c.limit-c.bytes {
		back := c.order.Back()
		if back == nil {
			break
		}
		c.order.Remove(back)
		ent := back.Value.(*cacheEntry)
		delete(c.entries, ent.key)
		c.bytes -= ent.size
	}
	c.entries[key] = c.order.PushFront(&cacheEntry{key: key, block: block, size: size})
	c.bytes += size
}

// clear drops every entry. Blocks already handed out stay valid.
func (c *readCache) clear() {
	c.order.Init()
	c.entries = make(map[cacheKey]*list.Element)
	c.seen = make(map[cacheKey]struct{})
	c.probation = nil
	c.bytes = 0
}

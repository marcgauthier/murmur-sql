package spool

import (
	"encoding/binary"
	"hash/crc32"
	"sync"
	"time"
)

// writeShard buffers puts behind its own lock so concurrent writers
// rarely contend.
type writeShard struct {
	mu      sync.Mutex
	records []pendingRecord
}

// commitGroup is one admitted atomic commit: owned sequenced records
// with their durability and an optional exact-identity recheck for
// compaction rewrites. done receives the terminal result exactly
// once (cap 1, never blocks); async callers ignore it.
type commitGroup struct {
	id    uint64
	recs  []pendingRecord
	bytes int64
	d     Durability
	done  chan error
}

// put appends a mutation to a write shard. When block is false the
// call returns ErrBackpressure instead of waiting for buffer space.
func (s *Store) put(key, value []byte, tomb, block bool) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return err
	}
	if err := s.checkMaint(); err != nil {
		return err
	}
	if len(key) > s.opts.MaxKeySize {
		return ErrKeyTooLarge
	}
	if !tomb && len(value) > s.opts.MaxValueSize {
		return ErrValueTooLarge
	}
	size := len(key) + len(value) + recordOverhead
	if size > s.opts.MaxBlockBytes {
		return ErrRecordTooLarge
	}
	// Copy outside the shard lock to keep the critical section tiny.
	k := cloneBytes(key)
	var v []byte
	if !tomb {
		v = cloneBytes(value)
	}
	// Backpressure: wait while over budget. Overshoot by in-flight
	// reservations is bounded and harmless.
	if int64(size) <= s.opts.MaxPendingBytes {
		s.bpMu.Lock()
		for s.pendingBytes.Load()+int64(size) > s.opts.MaxPendingBytes && !s.closed.Load() && !s.terminal.Load() {
			if !block {
				s.bpMu.Unlock()
				return ErrBackpressure
			}
			s.bpCond.Wait()
		}
		closed := s.closed.Load()
		s.bpMu.Unlock()
		if closed {
			return ErrClosed
		}
		if err := s.checkTerminal(); err != nil {
			return err
		}
	} else if !block && s.pendingBytes.Load() >= s.opts.MaxPendingBytes {
		return ErrBackpressure
	}
	s.pendingBytes.Add(int64(size))
	s.pendingRecords.Add(1)

	sh := &s.shards[shardForKey(k, s.shardMask)]
	sh.mu.Lock()
	// Sequences are assigned at seal time (see sealLocked), keeping
	// the hot path to one lock plus one append. The staged counter
	// increments inside the section, before the bytes land, so a
	// seal that observes the count also collects the bytes.
	s.stagedCount.Add(1)
	sh.records = append(sh.records, pendingRecord{key: k, value: v, tomb: tomb, size: size})
	sh.mu.Unlock()

	s.maybeSignalFlusher()
	return nil
}

// bufferItems admits validated, owned batch records: one
// backpressure wait for the whole batch, then one lock per touched
// shard. A batch larger than MaxPendingBytes bypasses the wait like
// an oversized single record (each entry is already size-bounded).
func (s *Store) bufferItems(items []pendingRecord, total int64) error {
	if err := s.checkTerminal(); err != nil {
		return err
	}
	if err := s.checkMaint(); err != nil {
		return err
	}
	if total <= s.opts.MaxPendingBytes {
		s.bpMu.Lock()
		for s.pendingBytes.Load()+total > s.opts.MaxPendingBytes && !s.closed.Load() && !s.terminal.Load() {
			s.bpCond.Wait()
		}
		closed := s.closed.Load()
		s.bpMu.Unlock()
		if closed {
			return ErrClosed
		}
		if err := s.checkTerminal(); err != nil {
			return err
		}
	}
	s.pendingBytes.Add(total)
	s.pendingRecords.Add(int64(len(items)))

	groups := make(map[uint64][]pendingRecord)
	for i := range items {
		sh := shardForKey(items[i].key, s.shardMask)
		groups[sh] = append(groups[sh], items[i])
	}
	for sh, recs := range groups {
		s.shards[sh].mu.Lock()
		// Count inside the section, before the bytes land (see
		// put): per-shard coupling keeps the seal's dirty check
		// sound for multi-shard batches.
		s.stagedCount.Add(uint64(len(recs)))
		s.shards[sh].records = append(s.shards[sh].records, recs...)
		s.shards[sh].mu.Unlock()
	}
	s.maybeSignalFlusher()
	return nil
}

// maybeSignalFlusher wakes the background writer when a count or byte
// threshold trips. The send never blocks.
func (s *Store) maybeSignalFlusher() {
	fp := s.opts.Flush
	hit := (fp.MaxRecords > 0 && s.pendingRecords.Load() >= int64(fp.MaxRecords)) ||
		(fp.MaxBytes > 0 && s.pendingBytes.Load() >= fp.MaxBytes)
	if hit {
		select {
		case s.flushCh <- struct{}{}:
		default:
		}
	}
}

// sealLocked swaps out every shard buffer and assigns sequences in
// swap order. The caller must hold admissionMu (ordering) and sealMu
// (shard swap atomicity).
//
// Seal-time assignment keeps sequences strictly increasing with
// append order (one admission completes fully before the next
// begins), which group assembly and the tombstone age rule rely on.
func (s *Store) sealLocked() ([]pendingRecord, error) {
	var out []pendingRecord
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		if n := len(sh.records); n > 0 {
			if s.seqNext.Load()+uint64(n) >= seqCounterMax {
				sh.mu.Unlock()
				return nil, ErrSeqExhausted
			}
			for j := range sh.records {
				n := s.seqNext.Add(1)
				sh.records[j].seq = s.seqBase | n
			}
			out = append(out, sh.records...)
			sh.records = nil
		}
		sh.mu.Unlock()
	}
	return out, nil
}

// admitLocked enqueues an internal group of owned records,
// assigning a group id and any unassigned sequences in order.
// Sealed records arrive sequenced; Commit groups arrive with seq 0.
// The caller must hold admissionMu.
func (s *Store) admitLocked(recs []pendingRecord, d Durability) *commitGroup {
	id := s.groupSeq.Add(1)
	for i := range recs {
		if recs[i].seq == 0 {
			recs[i].seq = s.seqBase | s.seqNext.Add(1)
		}
		recs[i].group = id
	}
	var bytes int64
	for i := range recs {
		bytes += int64(recs[i].size)
	}
	g := &commitGroup{id: id, recs: recs, bytes: bytes, d: d, done: make(chan error, 1)}
	s.queueMu.Lock()
	s.queue = append(s.queue, g)
	s.lastGroup.Store(id)
	s.commitCond.Broadcast()
	s.queueMu.Unlock()
	s.groupsAccepted.Add(1)
	s.pendingGroups.Add(1)
	return g
}

// popQueue removes the head group or returns nil when empty.
func (s *Store) popQueue() *commitGroup {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if len(s.queue) == 0 {
		return nil
	}
	g := s.queue[0]
	s.queue[0] = nil
	s.queue = s.queue[1:]
	return g
}

// waitForGroup blocks until group id completes, the store goes
// terminal, or (unless drain) the store closes. Completed groups
// always report success: post-admission failures are terminal,
// never per-group. Close's drain waits with drain=true: the
// committer stays alive until drained is set.
func (s *Store) waitForGroup(id uint64, drain bool) error {
	for {
		s.queueMu.Lock()
		for s.completedThrough < id && (drain || !s.closed.Load()) && !s.terminal.Load() {
			s.commitCond.Wait()
		}
		done := s.completedThrough >= id
		s.queueMu.Unlock()
		if err := s.checkTerminal(); err != nil {
			return err
		}
		if done {
			return nil
		}
		if s.closed.Load() {
			return ErrClosed
		}
	}
}

// commitLoop is the single ordered committer: groups append in
// admission order, so logical sequence order always matches physical
// append order. On terminal failure it releases every queued waiter
// and exits; on close it drains the queue first.
func (s *Store) commitLoop() {
	defer s.closeWG.Done()
	for {
		s.queueMu.Lock()
		for len(s.queue) == 0 && (!s.closed.Load() || !s.drained) {
			s.commitCond.Wait()
		}
		empty := len(s.queue) == 0
		s.queueMu.Unlock()
		if empty {
			return
		}
		if err := s.checkTerminal(); err != nil {
			s.drainQueue(err)
			return
		}
		if empty {
			continue
		}
		g := s.popQueue()
		if g == nil {
			continue
		}
		err := s.processGroup(g)
		if err != nil {
			s.setTerminal(err)
		}
		s.queueMu.Lock()
		if err == nil && g.id > s.completedThrough {
			s.completedThrough = g.id
		}
		s.commitCond.Broadcast()
		s.queueMu.Unlock()
		s.pendingGroups.Add(-1)
		g.done <- err
	}
}

// noteAppended records the highest appended group id. The committer
// and replacement compaction append concurrently, so the maximum is
// resolved with compare-and-swap rather than a plain store.
func (s *Store) noteAppended(id uint64) {
	for {
		if cur := s.appendedGroup.Load(); id <= cur || s.appendedGroup.CompareAndSwap(cur, id) {
			return
		}
	}
}

// drainQueue releases every queued group waiter with the terminal
// cause. Completed groups keep their success; queued groups never
// ran, so completedThrough does not advance.
func (s *Store) drainQueue(cause error) {
	s.queueMu.Lock()
	queued := s.queue
	s.queue = nil
	s.commitCond.Broadcast()
	s.queueMu.Unlock()
	for _, g := range queued {
		s.pendingBytes.Add(-g.bytes)
		s.pendingRecords.Add(-int64(len(g.recs)))
		s.pendingGroups.Add(-1)
		g.done <- cause
	}
}

// splitBlocks groups records into block-sized batches. A record
// larger than the target gets a dedicated block; every block fits
// maxBody bytes of body.
func splitBlocks(records []pendingRecord, target, maxBody, maxRecs int) [][]pendingRecord {
	var out [][]pendingRecord
	var cur []pendingRecord
	curSize := 0
	flush := func() {
		if len(cur) > 0 {
			out = append(out, cur)
			cur = nil
			curSize = 0
		}
	}
	for _, r := range records {
		rs := r.size + 32 // framing + margin
		if rs > target && len(cur) == 0 {
			out = append(out, []pendingRecord{r})
			continue
		}
		if len(cur) >= maxRecs || curSize+rs > maxBody || (curSize > 0 && curSize+rs > target) {
			flush()
		}
		cur = append(cur, r)
		curSize += rs
	}
	flush()
	return out
}

// plaintextGroupSize computes the exact plaintext data-body bytes for
// records split into blocks, for pre-admission bound checks.
func plaintextGroupSize(records []pendingRecord, target, maxBody, maxRecs int) uint64 {
	var total uint64
	for _, b := range splitBlocks(records, target, maxBody, maxRecs) {
		n := uint64(groupFramingLen + 4)
		for i := range b {
			n += uint64(8 + 1 + 4 + 4 + len(b[i].key) + len(b[i].value))
		}
		total += n
	}
	return total
}

// chunkForBound splits records into bound-fitting chunks for flush
// seals. Estimates are conservative (per-record slack covers block
// framing), so chunks always fit exactly.
func chunkForBound(records []pendingRecord, bound int64) [][]pendingRecord {
	var out [][]pendingRecord
	var cur []pendingRecord
	var curSize int64
	for _, r := range records {
		rs := int64(r.size + 32)
		if len(cur) > 0 && curSize+rs > bound {
			out = append(out, cur)
			cur = nil
			curSize = 0
		}
		cur = append(cur, r)
		curSize += rs
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}

// buildFrames compresses and encrypts every data block with bounded
// parallelism, returning frames in input order. On the first worker
// error it drains the rest and reports the error.
func (s *Store) buildFrames(groupID uint64, blocks [][]pendingRecord, keyID uint32, key [32]byte, firstBlockID uint64, ctx [16]byte) ([][]byte, []int, error) {
	type result struct {
		idx     int
		frame   []byte
		bodyLen int
		err     error
	}
	results := make(chan result, len(blocks))
	sem := make(chan struct{}, s.workers)
	var wg sync.WaitGroup
	for i, recs := range blocks {
		wg.Add(1)
		go func(idx int, recs []pendingRecord) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			frame, bodyLen, err := s.buildOneBlock(groupID, uint32(idx), uint32(len(blocks)), recs, keyID, key, firstBlockID+uint64(idx), ctx)
			results <- result{idx: idx, frame: frame, bodyLen: bodyLen, err: err}
		}(i, recs)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	frames := make([][]byte, len(blocks))
	bodyLens := make([]int, len(blocks))
	var firstErr error
	for r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		frames[r.idx] = r.frame
		bodyLens[r.idx] = r.bodyLen
	}
	if firstErr != nil {
		return nil, nil, firstErr
	}
	return frames, bodyLens, nil
}

// buildOneBlock encodes, compresses and seals a single data block.
func (s *Store) buildOneBlock(groupID uint64, blockIndex, blockCount uint32, recs []pendingRecord, keyID uint32, key [32]byte, blockID uint64, ctx [16]byte) ([]byte, int, error) {
	body := encodeBody(groupID, blockIndex, blockCount, recs)
	return s.sealBlockWith(body, false, uint32(len(recs)), keyID, key, blockID, ctx)
}

// buildCompletionBlock seals the group's completion record over the
// finished data frames.
func (s *Store) buildCompletionBlock(groupID uint64, dataFrames [][]byte, bodyLens []int, totalRecords uint32, keyID uint32, key [32]byte, blockID uint64, ctx [16]byte) ([]byte, int, error) {
	seqs := make([]uint64, len(dataFrames))
	sealed := make([][]byte, len(dataFrames))
	var totalPlain uint64
	for i, f := range dataFrames {
		seqs[i] = blockID - uint64(len(dataFrames)) + uint64(i)
		sealed[i] = f[blockHeaderLen:]
		totalPlain += uint64(bodyLens[i])
	}
	digest := groupDigest(groupID, uint32(len(dataFrames)), seqs, sealed)
	body := encodeCompletion(completion{
		groupID:      groupID,
		blockCount:   uint32(len(dataFrames)),
		totalRecords: totalRecords,
		totalPlain:   totalPlain,
		blockSeqs:    seqs,
		digest:       digest,
	})
	return s.sealBlockWith(body, true, 0, keyID, key, blockID, ctx)
}

// sealBlock compresses and seals one plaintext body with a fresh
// nonce. The header doubles as AEAD associated data (bound to the
// store id and database context), sealed with a zero sealed-length
// field patched afterwards along with the CRC.
func (s *Store) sealBlock(body []byte, completion bool, recordCount uint32, keyID uint32, key [32]byte, blockID uint64) ([]byte, int, error) {
	return s.sealBlockWith(body, completion, recordCount, keyID, key, blockID, s.ctx)
}

// sealBlockWith seals under an explicit database context; sealBlock
// uses the store's live context. Maintenance re-sealing passes the
// target context.
func (s *Store) sealBlockWith(body []byte, completion bool, recordCount uint32, keyID uint32, key [32]byte, blockID uint64, ctx [16]byte) ([]byte, int, error) {
	return sealFrame(s.comp, s.crypt, s.opts.Compression, s.man.storeID, body, completion, recordCount, keyID, key, blockID, ctx)
}

// sealFrame compresses and seals one plaintext body with a fresh
// nonce under explicit parameters so maintenance staging can seal
// without a live Store.
func sealFrame(comp *compressor, crypt blockCrypt, compression Compression, storeID [16]byte, body []byte, completion bool, recordCount uint32, keyID uint32, key [32]byte, blockID uint64, ctx [16]byte) ([]byte, int, error) {
	compBody, err := comp.compressWith(compression, body)
	if err != nil {
		return nil, 0, err
	}
	nonce, err := crypt.newNonce()
	if err != nil {
		return nil, 0, err
	}
	hdr, err := encodeBlockHeader(crypt.algo, compressionID(compression), completion, keyID, blockID, recordCount, uint32(len(body)), 0, nonce)
	if err != nil {
		return nil, 0, err
	}
	sealed, err := crypt.seal(key, nonce, compBody, blockAAD(hdr[:59], storeID[:], ctx[:]))
	if err != nil {
		return nil, 0, err
	}
	binary.LittleEndian.PutUint32(hdr[30:34], uint32(len(sealed)))
	binary.LittleEndian.PutUint32(hdr[59:63], crc32.Checksum(hdr[:59], castagnoli))
	frame := make([]byte, 0, len(hdr)+len(sealed))
	frame = append(frame, hdr...)
	frame = append(frame, sealed...)
	return frame, len(body), nil
}

// flushInternal seals buffered individual writes into bound-fitting
// internal groups and admits them in order. It returns the fenced
// group id: after it returns, every record buffered before its seal
// is admitted. allowClosed serves Close's drain; terminal state
// always fails.
func (s *Store) flushInternal(d Durability, allowClosed bool) (uint64, error) {
	if !allowClosed && s.closed.Load() {
		return s.lastGroup.Load(), ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return s.lastGroup.Load(), err
	}
	if err := s.checkMaint(); err != nil {
		return s.lastGroup.Load(), err
	}
	if err := s.ensureFreshKey(); err != nil {
		return s.lastGroup.Load(), err
	}
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	return s.flushLocked(d)
}

// flushLocked seals buffered writes into admitted groups. The caller
// must hold admissionMu. Key freshness is the caller's business:
// checkpoint capture cannot rotate mid-section and seals under the
// current key instead. A seal that finds the staged counter
// unchanged since the last seal skips the shard walk: nothing new
// arrived. The recorded count predates collection, so writes that
// race the walk are picked up by the next seal.
func (s *Store) flushLocked(d Durability) (uint64, error) {
	c0 := s.stagedCount.Load()
	if c0 == s.lastSealed {
		return s.lastGroup.Load(), nil
	}
	start := time.Now()
	defer func() {
		s.flushes.Add(1)
		s.flushNanos.Add(uint64(time.Since(start)))
	}()
	s.sealMu.Lock()
	records, err := s.sealLocked()
	s.sealMu.Unlock()
	if err != nil {
		return s.lastGroup.Load(), err
	}
	s.lastSealed = c0
	if len(records) == 0 {
		return s.lastGroup.Load(), nil
	}
	var id uint64
	for _, chunk := range chunkForBound(records, s.opts.MaxAtomicBatchBytes) {
		g := s.admitLocked(chunk, d)
		id = g.id
	}
	return id, nil
}

// processGroup builds, appends and commits one group. The group
// lands wholly in one segment; any failure is terminal (no retry,
// no replacement sequences) because disk state is uncertain.
func (s *Store) processGroup(g *commitGroup) error {
	keyID, key, ok := s.sealDataKey()
	if !ok {
		return errNoCurrentKey
	}
	defer s.releaseSealKey(keyID)
	maxBody := s.opts.MaxBlockBytes - blockHeaderLen - s.crypt.overhead() - 1024
	groups := splitBlocks(g.recs, s.opts.TargetBlockBytes, maxBody, s.opts.MaxRecordsPerBlock)
	nData := len(groups)
	firstBlock := s.nextBlockID.Add(uint64(nData + 1))
	firstBlock -= uint64(nData + 1)
	firstBlock++
	frames, bodyLens, err := s.buildFrames(g.id, groups, keyID, key, firstBlock, s.ctx)
	if err != nil {
		return err
	}
	compFrame, compLen, err := s.buildCompletionBlock(g.id, frames, bodyLens, uint32(len(g.recs)), keyID, key, firstBlock+uint64(nData), s.ctx)
	if err != nil {
		return err
	}
	all := append(frames, compFrame)
	var total int64
	for _, f := range all {
		total += int64(len(f))
	}
	// Reserve the whole group in one segment: rotate first when it
	// would overflow a non-empty active file, and seal the file
	// after an oversized group completes.
	if off := s.seg.endOffset(); off > 0 && off+total > s.opts.MaxSegmentSize {
		if err := s.seg.rotateTo(s.allocateFileID()); err != nil {
			return err
		}
	}
	fileID, offs, err := s.seg.appendGroup(all)
	if err != nil {
		return err
	}
	if total > s.opts.MaxSegmentSize {
		if err := s.seg.rotateTo(s.allocateFileID()); err != nil {
			return err
		}
	}
	if err := s.seg.commit(g.d); err != nil {
		return err
	}
	// Durability reached: install index entries and file statistics.
	// Records keep seal/admission order; repeats resolve by sequence.
	var totalSize int64
	for i, recs := range groups {
		for j := range recs {
			r := &recs[j]
			size := uint32(len(r.key) + len(r.value) + 17)
			loc := Location{
				FileID:      fileID,
				BlockID:     firstBlock + uint64(i),
				BlockOffset: offs[i],
				RecordIndex: uint32(j),
				Sequence:    r.seq,
				Size:        size,
			}
			s.idx.commitRecord(s.fstats, string(r.key), r.seq, r.tomb, loc, size)
			totalSize += int64(r.size)
			// Release value bytes for GC; the index holds no copy.
			recs[j].key, recs[j].value = nil, nil
		}
	}
	s.blocksWritten.Add(uint64(len(all)))
	s.bytesWritten.Add(uint64(total))
	for _, l := range bodyLens {
		s.bytesUncompressed.Add(uint64(l))
	}
	s.bytesUncompressed.Add(uint64(compLen))
	s.groupsWritten.Add(1)
	if g.d == DurabilitySync {
		s.groupsSynced.Add(1)
	}
	s.noteAppended(g.id)
	s.pendingBytes.Add(-totalSize)
	s.pendingRecords.Add(-int64(len(g.recs)))
	s.lastFlush.Store(nowUnixNano())
	s.bpCond.Broadcast()
	return nil
}

// flushLoop is the background writer: threshold signals plus the
// MaxDelay timer. Sealed writes become internal groups; terminal
// state stops the loop's attempts but Close still reaps it.
func (s *Store) flushLoop() {
	defer s.closeWG.Done()
	var tick <-chan time.Time
	if s.opts.Flush.MaxDelay > 0 {
		ticker := time.NewTicker(s.opts.Flush.MaxDelay)
		defer ticker.Stop()
		tick = ticker.C
	}
	for {
		select {
		case <-s.stopCh:
			return
		case <-s.flushCh:
			s.backgroundFlush()
		case <-tick:
			if s.pendingRecords.Load() > 0 {
				s.backgroundFlush()
			}
		}
	}
}

// backgroundFlush seals one batch and records (never drops) errors.
// It skips ticks while maintenance holds the store.
func (s *Store) backgroundFlush() {
	if s.maintActive.Load() {
		return
	}
	if _, err := s.flushInternal(s.opts.Durability, false); err != nil {
		s.flushErrors.Add(1)
		s.recordFlushErr(err)
		if s.checkTerminal() != nil {
			return
		}
		// Wake promptly for retry; pending counts stayed high.
		select {
		case s.flushCh <- struct{}{}:
		default:
		}
	}
}

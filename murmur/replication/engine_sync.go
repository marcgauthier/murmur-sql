package replication

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/postcard"
)

// Retransmit unacked ranges once pushing an origin goes quiet for this
// long. `sent` dedupes the stream while new transactions flow, but without
// a resend timer a follower that fell behind would only ever trickle back
// via 10 s anti-entropy Needs (minutes of tail for seconds of backlog).
const resendAfterQuiet = 5 * time.Second

// Cap unacknowledged in-flight sequences per origin per peer. Without
// backpressure the sender floods a slow joiner with its whole unacked
// range; fresh transactions then queue behind hundreds of stale
// duplicates and catch-up stalls for seconds per cycle. The window holds
// ~6s of catch-up flow at measured apply rates; quiet resends keep the
// head (acked+1..) fresh while in-flight-full.
const pushInflightWindow = 1024

const snapBulkLagTx = 1000
const snapBulkLagStable = 30 * time.Second

var (
	needServeCtr atomic.Uint64
	needServedN  atomic.Uint64
	stageCtr     atomic.Uint64
	stageNew     atomic.Uint64
	stageApplied atomic.Uint64
	stageStaged  atomic.Uint64
	stageFloor   atomic.Uint64
	pushCtr      atomic.Uint64
)

// Transfers with a live chunk-serve task. Manifest retransmits make the
// receiver re-Need aggressively; without dedupe every Need spawns a task
// that re-sends the whole transfer, clogging the data lane with
// duplicate chunks. Transfer ids are globally unique, so no donor key.
var snapshotServing = struct {
	sync.Mutex
	entries map[NodeId]bool
}{entries: make(map[NodeId]bool)}

// snapshotBuildGate paces snapshot builds process-wide: colocated nodes
// racing to serve the same rejoin would otherwise dogpile the CPU.
var snapshotBuildGate = make(chan struct{}, 2)

// Stage one transaction chunk with O(1) backend operations. The proof
// signature verifies once per transfer (first chunk); later chunks bind
// to the durable manifest by digest, and reassembly reads each chunk
// exactly once at completion.
func (c *engineCore) stageChunk(chunk Chunk) error {
	// Cheap structural checks first: no crypto, no I/O. Saturating
	// math: the bounds below reject bogus indexes either way.
	lastLen := int64(chunk.Total) - int64(chunk.Index)*ChunkBytes
	if lastLen < 0 {
		lastLen = 0
	}
	wantLen := ChunkBytes
	if chunk.Index+1 == chunk.Count {
		wantLen = int(lastLen)
	}
	count := 0
	if chunk.Total > 0 {
		count = int((chunk.Total + ChunkBytes - 1) / ChunkBytes)
	}
	if int(chunk.Total) > c.config.MaxTransactionBytes+4096 || chunk.Count == 0 ||
		int(chunk.Count) != count || chunk.Index >= chunk.Count || len(chunk.Data) != wantLen {
		return protocolErr("transaction chunk bounds")
	}
	if len(chunk.Proof.Payload) != 0 || chunk.Proof.Origin != chunk.Origin ||
		chunk.Proof.Sequence != chunk.Sequence || chunk.Proof.ID != chunk.ID {
		return authErr("chunk proof identity")
	}
	db, _ := c.backend.Identity()
	floor, err := c.readFloor(chunk.Origin)
	if err != nil {
		return err
	}
	if chunk.Sequence <= floor {
		return nil
	}
	c.stageGate.Lock()
	mkey := ChunkManifestKey(chunk.Origin, chunk.Sequence)
	raw, err := c.backend.Get(mkey)
	if err != nil {
		c.stageGate.Unlock()
		return err
	}
	var manifest ChunkManifest
	manifestIsNew := false
	if raw != nil {
		m, err := DecodeChunkManifest(raw)
		if err != nil {
			c.stageGate.Unlock()
			return err
		}
		manifest = m
	} else {
		// Duplicate chunks of a completed transaction are dropped
		// without re-verifying or re-staging anything.
		receipt, err := c.backend.Get(ReceiptKey(chunk.Origin, chunk.Sequence))
		if err != nil {
			c.stageGate.Unlock()
			return err
		}
		staged, err := c.backend.Get(StageKey(chunk.Origin, chunk.Sequence))
		if err != nil {
			c.stageGate.Unlock()
			return err
		}
		if receipt != nil || staged != nil {
			c.stageGate.Unlock()
			return nil
		}
		if err := chunk.Proof.VerifyHeader(db, c.config.TrustedOrigins); err != nil {
			c.stageGate.Unlock()
			return err
		}
		manifest = ChunkManifest{ID: chunk.ID, Count: chunk.Count, Total: chunk.Total, Digest: chunk.Digest}
		manifestIsNew = true
	}
	if manifest.ID != chunk.ID || manifest.Count != chunk.Count ||
		manifest.Total != chunk.Total || manifest.Digest != chunk.Digest {
		c.stageGate.Unlock()
		return authErr("chunk manifest mismatch")
	}
	key := ChunkKey(chunk.Origin, chunk.Sequence, chunk.Index)
	isNew := false
	old, err := c.backend.Get(key)
	if err != nil {
		c.stageGate.Unlock()
		return err
	}
	if old != nil {
		if string(old) != string(EncodeChunk(&chunk)) {
			c.stageGate.Unlock()
			return authErr("conflicting transaction chunk")
		}
	} else {
		isNew = true
	}
	manifestBytes := EncodeChunkManifest(&manifest)
	if manifestIsNew {
		if err := c.chargeChunks(1, len(manifestBytes)); err != nil {
			c.stageGate.Unlock()
			return err
		}
	}
	if isNew {
		if err := c.chargeChunks(1, len(chunk.Data)); err != nil {
			if manifestIsNew {
				c.releaseChunks(1, len(manifestBytes))
			}
			c.stageGate.Unlock()
			return err
		}
	}
	var records []Record
	if manifestIsNew {
		records = append(records, PutRecord(mkey, manifestBytes))
	}
	if isNew {
		records = append(records, PutRecord(key, EncodeChunk(&chunk)))
	}
	if len(records) > 0 {
		if err := c.backend.Commit(records); err != nil {
			c.stageGate.Unlock()
			return err
		}
	}
	func() {
		now := time.Now()
		ck := chunkKey{origin: chunk.Origin, seq: chunk.Sequence}
		c.chunkSeenMu.Lock()
		defer c.chunkSeenMu.Unlock()
		if p, ok := c.chunkSeen[ck]; ok {
			if isNew {
				p.received++
			}
			p.updated = now
			return
		}
		// First sighting this process: reseed the count from the
		// durable keys (covers restart mid-transfer). A transfer
		// already underway elsewhere counts as stale so the repair
		// sweep picks it up promptly.
		have := uint32(0)
		if n, err := c.countStagedChunks(chunk.Origin, chunk.Sequence); err == nil {
			have = uint32(n)
		}
		updated := now
		if !manifestIsNew {
			updated = now.Add(-time.Hour)
		}
		c.chunkSeen[ck] = &chunkProgress{received: have, updated: updated, repaired: now.Add(-time.Hour)}
	}()
	ck := chunkKey{origin: chunk.Origin, seq: chunk.Sequence}
	c.chunkSeenMu.Lock()
	received := uint32(0)
	if p, ok := c.chunkSeen[ck]; ok {
		received = p.received
	}
	c.chunkSeenMu.Unlock()
	if received < manifest.Count {
		c.stageGate.Unlock()
		return nil
	}
	// Complete: single reassembly pass, then hand off to staging.
	var data []byte
	var cleanup []Record
	complete := true
	for index := uint32(0); index < manifest.Count; index++ {
		raw, err := c.backend.Get(ChunkKey(chunk.Origin, chunk.Sequence, index))
		if err != nil {
			c.stageGate.Unlock()
			return err
		}
		if raw == nil {
			// Lost a chunk between counting and reading; recount and
			// wait for repair instead of failing the transfer.
			if n, err := c.countStagedChunks(chunk.Origin, chunk.Sequence); err == nil {
				c.chunkSeenMu.Lock()
				if p, ok := c.chunkSeen[ck]; ok {
					p.received = uint32(n)
				}
				c.chunkSeenMu.Unlock()
			}
			complete = false
			break
		}
		part, err := DecodeChunk(raw)
		if err != nil {
			c.stageGate.Unlock()
			return err
		}
		if part.Digest != manifest.Digest || part.ID != manifest.ID ||
			part.Total != manifest.Total || part.Count != manifest.Count {
			c.stageGate.Unlock()
			return authErr("mixed transaction chunks")
		}
		data = append(data, part.Data...)
		cleanup = append(cleanup, DeleteRecord(ChunkKey(chunk.Origin, chunk.Sequence, index)))
	}
	if !complete {
		c.stageGate.Unlock()
		return nil
	}
	if uint64(len(data)) != manifest.Total || sha256.Sum256(data) != manifest.Digest {
		c.stageGate.Unlock()
		return authErr("transaction chunk digest")
	}
	tx, err := DecodeTransaction(data)
	if err != nil {
		c.stageGate.Unlock()
		return err
	}
	if tx.Origin != chunk.Origin || tx.Sequence != chunk.Sequence || tx.ID != chunk.ID {
		c.stageGate.Unlock()
		return authErr("chunk transaction identity")
	}
	cleanup = append(cleanup, DeleteRecord(ChunkManifestKey(chunk.Origin, chunk.Sequence)))
	manifestLen := len(EncodeChunkManifest(&manifest))
	c.stageGate.Unlock()
	if err := c.stage(tx); err != nil {
		return err
	}
	if err := c.backend.Commit(cleanup); err != nil {
		return err
	}
	c.releaseChunks(int(manifest.Count)+1, int(manifest.Total)+manifestLen)
	c.chunkSeenMu.Lock()
	delete(c.chunkSeen, ck)
	c.chunkSeenMu.Unlock()
	c.chunkHoldersMu.Lock()
	delete(c.chunkHolders, ck)
	c.chunkHoldersMu.Unlock()
	return nil
}

// countStagedChunks counts staged chunk keys (not the manifest) for one transfer.
func (c *engineCore) countStagedChunks(origin NodeId, seq uint64) (int, error) {
	keys, err := c.backend.Keys([]byte(ChunkPrefix(origin, seq)))
	if err != nil {
		return 0, err
	}
	n := 0
	for _, k := range keys {
		if !strings.HasSuffix(string(k), "manifest") {
			n++
		}
	}
	return n, nil
}

// chargeChunks reserves staging capacity for chunk bytes. Counters
// rebuild from a single key scan on first use after (re)start,
// conservatively estimating values at one full chunk each
// (fail-closed direction).
func (c *engineCore) chargeChunks(keys, bytes int) error {
	c.chunkCountersMu.Lock()
	defer c.chunkCountersMu.Unlock()
	if c.chunkCounters == nil {
		n, err := c.backend.Keys([]byte("07/replication/chunks/"))
		if err != nil {
			return err
		}
		c.chunkCounters = &chunkCounters{count: len(n), total: len(n) * ChunkBytes}
	}
	c.chunkCounters.count += keys
	c.chunkCounters.total += bytes
	if c.chunkCounters.total > c.config.StagingBytes || c.chunkCounters.count > c.config.StagingTransfers*1024 {
		c.chunkCounters.count -= keys
		c.chunkCounters.total -= bytes
		return ErrOverloaded
	}
	return nil
}

func (c *engineCore) releaseChunks(keys, bytes int) {
	c.chunkCountersMu.Lock()
	defer c.chunkCountersMu.Unlock()
	if c.chunkCounters != nil {
		c.chunkCounters.count -= keys
		if c.chunkCounters.count < 0 {
			c.chunkCounters.count = 0
		}
		c.chunkCounters.total -= bytes
		if c.chunkCounters.total < 0 {
			c.chunkCounters.total = 0
		}
	}
}

// serveChunkNeed serves requested chunk indexes, from the complete
// transaction when retained, else from staged chunks as a repair relay.
// Holding nothing is a quiet miss: repair requests are broadcast, so
// most peers won't have the data.
func (c *engineCore) serveChunkNeed(origin NodeId, sequence uint64, indexes []uint32, send *queueSender) error {
	if len(indexes) > 128 {
		return ErrOverloaded
	}
	if raw, err := c.backend.Get(LogKey(origin, sequence)); err != nil {
		return err
	} else if raw != nil {
		tx, err := DecodeTransaction(raw)
		if err != nil {
			return err
		}
		data := EncodeTransaction(&tx)
		sum := sha256.Sum256(data)
		count := (len(data) + ChunkBytes - 1) / ChunkBytes
		for _, index := range indexes {
			if int(index) >= count {
				return protocolErr("chunk index")
			}
			start := int(index) * ChunkBytes
			end := start + ChunkBytes
			if end > len(data) {
				end = len(data)
			}
			proof := tx
			proof.Payload = nil
			_ = send.TrySend(Message{Kind: MsgTransactionChunk, Chunk: Chunk{
				Proof: proof, Origin: origin, Sequence: sequence, ID: tx.ID,
				Index: index, Count: uint32(count), Total: uint64(len(data)),
				Digest: sum, Data: append([]byte(nil), data[start:end]...),
			}})
		}
		c.mutateStatus(func(s *Status) { s.Metrics.ChunkNeedServed += uint64(len(indexes)) })
		return nil
	}
	mkey := ChunkManifestKey(origin, sequence)
	manifestBytes, err := c.backend.Get(mkey)
	if err != nil {
		return err
	}
	if manifestBytes == nil {
		return nil
	}
	manifest, err := DecodeChunkManifest(manifestBytes)
	if err != nil {
		return err
	}
	var served uint64
	for _, index := range indexes {
		if index >= manifest.Count {
			return protocolErr("chunk index")
		}
		if raw, err := c.backend.Get(ChunkKey(origin, sequence, index)); err != nil {
			return err
		} else if raw != nil {
			part, err := DecodeChunk(raw)
			if err != nil {
				return err
			}
			_ = send.TrySend(Message{Kind: MsgTransactionChunk, Chunk: part})
			served++
		}
	}
	c.mutateStatus(func(s *Status) { s.Metrics.ChunkNeedServed += served })
	return nil
}

// stagedTransfers lists transfers with a durable chunk manifest
// (partially received). Key form:
// `07/replication/chunks/<origin-hex>/<seq20>/manifest`.
func (c *engineCore) stagedTransfers() ([]chunkKey, error) {
	keys, err := c.backend.Keys([]byte("07/replication/chunks/"))
	if err != nil {
		return nil, err
	}
	var out []chunkKey
	for _, key := range keys {
		parts := strings.Split(string(key), "/")
		if len(parts) == 6 && parts[5] == "manifest" {
			origin, oerr := parseID(parts[3])
			seq, serr := strconv.ParseUint(parts[4], 10, 64)
			if oerr == nil && serr == nil {
				out = append(out, chunkKey{origin: origin, seq: seq})
			}
		}
	}
	return out, nil
}

// presentIndexes lists indexes currently staged for one transfer.
func (c *engineCore) presentIndexes(origin NodeId, seq uint64) (map[uint32]bool, error) {
	keys, err := c.backend.Keys([]byte(ChunkPrefix(origin, seq)))
	if err != nil {
		return nil, err
	}
	out := make(map[uint32]bool)
	for _, key := range keys {
		if strings.HasSuffix(string(key), "manifest") {
			continue
		}
		suffix := string(key)
		if i := strings.LastIndexByte(suffix, '/'); i >= 0 {
			suffix = suffix[i+1:]
		}
		if index, err := strconv.ParseUint(suffix, 10, 32); err == nil {
			out[uint32(index)] = true
		}
	}
	return out, nil
}

// serveChunkAvailability answers "what chunks do you hold": staged
// partial transfers with their present indexes, 64 transfers per page.
// Complete transactions are omitted by design: holders serve broadcast
// repair requests from the log without advertising.
func (c *engineCore) serveChunkAvailability(offset uint32, send *queueSender) error {
	transfers, err := c.stagedTransfers()
	if err != nil {
		return err
	}
	sort.Slice(transfers, func(i, j int) bool {
		if transfers[i].origin != transfers[j].origin {
			return nodeLess(transfers[i].origin, transfers[j].origin)
		}
		return transfers[i].seq < transfers[j].seq
	})
	if int(offset) < len(transfers) {
		transfers = transfers[offset:]
	} else {
		transfers = nil
	}
	total := (len(transfers) + 63) / 64
	for i := 0; i < total; i++ {
		page := transfers[i*64 : min((i+1)*64, len(transfers))]
		entries := make([]ChunkAvailability, 0, len(page))
		for _, t := range page {
			present, err := c.presentIndexes(t.origin, t.seq)
			if err != nil {
				return err
			}
			indexes := make([]uint32, 0, len(present))
			for index := range present {
				indexes = append(indexes, index)
			}
			sort.Slice(indexes, func(a, b int) bool { return indexes[a] < indexes[b] })
			entries = append(entries, ChunkAvailability{Origin: t.origin, Sequence: t.seq, Indexes: indexes})
		}
		_ = send.TrySend(Message{Kind: MsgChunkAvailabilityPage, Availability: entries, More: i+1 < total})
	}
	if total == 0 {
		_ = send.TrySend(Message{Kind: MsgChunkAvailabilityPage, More: false})
	}
	return nil
}

// mergeChunkAvailability merges a holder bitmap page. Entries are
// capped: a 64 MiB transaction has 1024 chunks, so 4096 indexes per
// entry is ample headroom.
func (c *engineCore) mergeChunkAvailability(from NodeId, entries []ChunkAvailability) error {
	if len(entries) > 64 {
		return ErrOverloaded
	}
	now := time.Now()
	c.chunkHoldersMu.Lock()
	defer c.chunkHoldersMu.Unlock()
	for _, e := range entries {
		if len(e.Indexes) > 4096 {
			return ErrOverloaded
		}
		ck := chunkKey{origin: e.Origin, seq: e.Sequence}
		holder, ok := c.chunkHolders[ck]
		if !ok {
			holder = &chunkHolders{holders: make(map[NodeId]map[uint32]bool)}
			c.chunkHolders[ck] = holder
		}
		set := make(map[uint32]bool, len(e.Indexes))
		for _, index := range e.Indexes {
			set[index] = true
		}
		holder.holders[from] = set
		holder.at = now
	}
	return nil
}

// cleanupTransfer drops staged chunks plus manifest for one transfer
// and releases the reserved capacity.
func (c *engineCore) cleanupTransfer(origin NodeId, seq uint64) error {
	mkey := ChunkManifestKey(origin, seq)
	raw, err := c.backend.Get(mkey)
	if err != nil {
		return err
	}
	var manifest *ChunkManifest
	if raw != nil {
		m, err := DecodeChunkManifest(raw)
		if err != nil {
			return err
		}
		manifest = &m
	}
	keys, err := c.backend.Keys([]byte(ChunkPrefix(origin, seq)))
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		records := make([]Record, len(keys))
		for i, key := range keys {
			records[i] = DeleteRecord(key)
		}
		if err := c.backend.Commit(records); err != nil {
			return err
		}
	}
	if manifest != nil {
		c.releaseChunks(int(manifest.Count)+1, int(manifest.Total)+len(EncodeChunkManifest(manifest)))
	}
	ck := chunkKey{origin: origin, seq: seq}
	c.chunkSeenMu.Lock()
	delete(c.chunkSeen, ck)
	c.chunkSeenMu.Unlock()
	c.chunkHoldersMu.Lock()
	delete(c.chunkHolders, ck)
	c.chunkHoldersMu.Unlock()
	return nil
}

// repairChunks runs periodic gap repair for incomplete chunked
// transfers. It recovers restart state from durable manifests,
// re-requests missing indexes (directed by holder bitmaps when known,
// broadcast otherwise), and drops chunk garbage for transactions that
// completed another way.
func (c *engineCore) repairChunks() error {
	now := time.Now()
	// Seed in-memory progress from durable manifests. Only needed when
	// the map is empty (restart): live transfers insert on arrival, and
	// the insert sits in the same critical section as the manifest
	// commit with no await between, so no other gap is possible.
	c.chunkSeenMu.Lock()
	empty := len(c.chunkSeen) == 0
	c.chunkSeenMu.Unlock()
	if empty {
		transfers, err := c.stagedTransfers()
		if err != nil {
			return err
		}
		for _, t := range transfers {
			have := 0
			if n, err := c.countStagedChunks(t.origin, t.seq); err == nil {
				have = n
			}
			stale := now.Add(-time.Hour)
			c.chunkSeenMu.Lock()
			c.chunkSeen[t] = &chunkProgress{received: uint32(have), updated: stale, repaired: stale}
			c.chunkSeenMu.Unlock()
		}
	}
	c.chunkHoldersMu.Lock()
	for key, holder := range c.chunkHolders {
		if now.Sub(holder.at) >= time.Minute {
			delete(c.chunkHolders, key)
		}
	}
	c.chunkHoldersMu.Unlock()
	c.chunkSeenMu.Lock()
	incomplete := make([]chunkKey, 0, len(c.chunkSeen))
	for key := range c.chunkSeen {
		incomplete = append(incomplete, key)
	}
	c.chunkSeenMu.Unlock()
	for _, t := range incomplete {
		manifestBytes, err := c.backend.Get(ChunkManifestKey(t.origin, t.seq))
		if err != nil {
			return err
		}
		if manifestBytes == nil {
			c.chunkSeenMu.Lock()
			delete(c.chunkSeen, t)
			c.chunkSeenMu.Unlock()
			continue
		}
		manifest, err := DecodeChunkManifest(manifestBytes)
		if err != nil {
			return err
		}
		// Completed through another path (full re-send won the race)?
		// Drop the leftover chunks.
		receipt, err := c.backend.Get(ReceiptKey(t.origin, t.seq))
		if err != nil {
			return err
		}
		staged, err := c.backend.Get(StageKey(t.origin, t.seq))
		if err != nil {
			return err
		}
		logged, err := c.backend.Get(LogKey(t.origin, t.seq))
		if err != nil {
			return err
		}
		if receipt != nil || staged != nil || logged != nil {
			if err := c.cleanupTransfer(t.origin, t.seq); err != nil {
				return err
			}
			continue
		}
		c.chunkSeenMu.Lock()
		p, ok := c.chunkSeen[t]
		var updated, repaired time.Time
		if ok {
			updated, repaired = p.updated, p.repaired
		}
		c.chunkSeenMu.Unlock()
		if !ok {
			continue
		}
		// Chunks still flowing, or recently asked: leave it alone.
		if now.Sub(updated) < 5*time.Second || now.Sub(repaired) < 10*time.Second {
			continue
		}
		present, err := c.presentIndexes(t.origin, t.seq)
		if err != nil {
			return err
		}
		c.chunkSeenMu.Lock()
		if p, ok := c.chunkSeen[t]; ok {
			p.received = uint32(len(present))
		}
		c.chunkSeenMu.Unlock()
		var missing []uint32
		for i := uint32(0); i < manifest.Count; i++ {
			if !present[i] {
				missing = append(missing, i)
			}
		}
		if len(missing) == 0 {
			// Bitmap complete but reassembly never ran (completion
			// crashed): ask for a full re-send via a transaction Need
			// and let the duplicate chunks trigger reassembly.
			c.peersMu.Lock()
			if len(c.peers) == 0 {
				c.peersMu.Unlock()
				continue
			}
			for _, peer := range c.peers {
				_ = peer.send.TrySend(Message{Kind: MsgNeed, Origin: t.origin, From: t.seq, Through: t.seq})
			}
			c.peersMu.Unlock()
			c.chunkSeenMu.Lock()
			if p, ok := c.chunkSeen[t]; ok {
				p.repaired = now
			}
			c.chunkSeenMu.Unlock()
			continue
		}
		sent := c.requestMissingChunks(t.origin, t.seq, missing)
		// Only re-arm the flood gate when a request actually went out;
		// otherwise the next sweep must retry (e.g. peers not yet
		// connected) instead of sleeping through the full interval.
		if sent > 0 {
			c.chunkSeenMu.Lock()
			if p, ok := c.chunkSeen[t]; ok {
				p.repaired = now
			}
			c.chunkSeenMu.Unlock()
		}
	}
	return nil
}

// selectHolders directs each missing index at a holder round-robin;
// indexes with no known holder fall back to broadcast.
func selectHolders(missing []uint32, holders map[NodeId]map[uint32]bool) (map[NodeId][]uint32, []uint32) {
	directed := make(map[NodeId][]uint32)
	var orphaned []uint32
	for i, index := range missing {
		var having []NodeId
		for id, set := range holders {
			if set[index] {
				having = append(having, id)
			}
		}
		sort.Slice(having, func(a, b int) bool { return nodeLess(having[a], having[b]) })
		if len(having) == 0 {
			orphaned = append(orphaned, index)
		} else {
			holder := having[i%len(having)]
			directed[holder] = append(directed[holder], index)
		}
	}
	return directed, orphaned
}

// requestMissingChunks asks for missing chunk indexes, directed at
// known holders when holder bitmaps exist, broadcast otherwise (with an
// availability query attached so the next round can be directed).
func (c *engineCore) requestMissingChunks(origin NodeId, seq uint64, missing []uint32) int {
	type peerSend struct {
		id   NodeId
		send *queueSender
	}
	c.peersMu.Lock()
	peers := make([]peerSend, 0, len(c.peers))
	for id, p := range c.peers {
		peers = append(peers, peerSend{id: id, send: p.send})
	}
	c.peersMu.Unlock()
	if len(peers) == 0 {
		return 0
	}
	c.chunkHoldersMu.Lock()
	holders := make(map[NodeId]map[uint32]bool)
	if holder, ok := c.chunkHolders[chunkKey{origin: origin, seq: seq}]; ok {
		for id, set := range holder.holders {
			holders[id] = set
		}
	}
	c.chunkHoldersMu.Unlock()
	want := missing
	if len(want) > 128 {
		want = want[:128]
	}
	sent := 0
	if len(holders) == 0 {
		for _, peer := range peers {
			_ = peer.send.TrySend(Message{Kind: MsgChunkNeed, Origin: origin, Sequence: seq, Indexes: append([]uint32(nil), want...)})
			_ = peer.send.TrySend(Message{Kind: MsgChunkAvailabilityRequest})
			c.mutateStatus(func(s *Status) { s.Metrics.ChunkNeedSent++ })
			sent++
		}
		return sent
	}
	directed, orphaned := selectHolders(want, holders)
	for peer, indexes := range directed {
		for _, p := range peers {
			if p.id == peer {
				_ = p.send.TrySend(Message{Kind: MsgChunkNeed, Origin: origin, Sequence: seq, Indexes: indexes})
				c.mutateStatus(func(s *Status) { s.Metrics.ChunkNeedSent++ })
				sent++
				break
			}
		}
	}
	if len(orphaned) > 0 {
		for _, peer := range peers {
			_ = peer.send.TrySend(Message{Kind: MsgChunkNeed, Origin: origin, Sequence: seq, Indexes: append([]uint32(nil), orphaned...)})
			c.mutateStatus(func(s *Status) { s.Metrics.ChunkNeedSent++ })
			sent++
		}
	}
	return sent
}

func (c *engineCore) handleProgress(id NodeId, entries []Progress, send *queueSender) error {
	if len(entries) > 128 {
		return ErrOverloaded
	}
	for _, p := range entries {
		if p.Observed < p.Applied || (p.RetainedFrom > p.RetainedThrough && p.RetainedThrough != 0) {
			return protocolErr("invalid progress")
		}
	}
	// In-memory peer progress (drives push windows) updates on every Ack
	// without touching the retention gate. Durable peer-ACK floors
	// persist at most once per second per peer: a Spool commit per Ack
	// would serialize behind the worker and lump ACK processing into
	// multi-second bursts. Unknown peers always touch (retire check).
	now := time.Now()
	due := true
	c.peersMu.Lock()
	if peer, ok := c.peers[id]; ok {
		for _, p := range entries {
			if p.Applied > peer.progress[p.Origin] {
				peer.ackAnyAt = &now
			}
			peer.progress[p.Origin] = p.Applied
		}
		peer.lastSeen = now
		if peer.lastTouch != nil && now.Sub(*peer.lastTouch) < time.Second {
			due = false
		} else {
			peer.lastTouch = &now
		}
	}
	c.peersMu.Unlock()
	if due {
		c.retentionGate.Lock()
		err := Touch(c.backend, id, entries)
		c.retentionGate.Unlock()
		if err != nil {
			return err
		}
	}
	keys := make([][]byte, len(entries))
	for i, p := range entries {
		keys[i] = ProgressKey(p.Origin)
	}
	values, err := readMany(c.backend, keys)
	if err != nil {
		return err
	}
	var maxLag uint64
	for i, p := range entries {
		if p.Observed < p.Applied || (p.RetainedFrom > p.RetainedThrough && p.RetainedThrough != 0) {
			return protocolErr("invalid progress")
		}
		var applied uint64
		if values[i] != nil {
			v, err := decodeOne(values[i], func(d *postcard.Decoder) uint64 { return d.U64() })
			if err != nil {
				return err
			}
			applied = v
		}
		next := applied + 1
		if lag := p.RetainedThrough - applied; p.RetainedThrough >= applied && lag > maxLag {
			maxLag = lag
		}
		missingHistory := p.RetainedFrom > next || (p.Applied > applied && p.RetainedThrough <= applied)
		if missingHistory {
			if c.config.TrustedSnapshotSources[id] {
				c.requestSnapshotDamped(id, send)
			} else {
				c.recordError(unavailableErr("catch-up requires an explicitly trusted snapshot source"))
			}
		} else if p.RetainedThrough > applied {
			through := p.RetainedThrough
			if cap := satAdd(applied, 16); cap < through {
				through = cap
			}
			_ = send.TrySend(Message{Kind: MsgNeed, Origin: p.Origin, From: next, Through: through})
		}
	}
	// Bulk catch-up: a large STABLE per-origin lag is cheaper via
	// snapshot than per-tx repair even when all history is retained
	// (outage/partition victims face 10k+ gaps that grind for many
	// minutes transaction by transaction). Stability (30s) filters
	// transient bursts; trust gates acceptance; the request itself is
	// 5s-damped and donors reuse fresh per-recipient builds, so
	// repeats are cheap. Silent when untrusted (opportunistic).
	if maxLag > snapBulkLagTx {
		fire := false
		c.peersMu.Lock()
		if peer, ok := c.peers[id]; ok {
			if peer.bulkLagSince == nil {
				peer.bulkLagSince = &now
			} else if now.Sub(*peer.bulkLagSince) >= snapBulkLagStable {
				fire = true
			}
		}
		c.peersMu.Unlock()
		if fire && c.config.TrustedSnapshotSources[id] {
			incoming, err := c.cachedIncoming()
			if err != nil {
				return err
			}
			if incoming == nil {
				fmt.Fprintf(os.Stderr, "[snap-bulk] requesting from %02x%02x lag=%d\n", id[0], id[1], maxLag)
				c.requestSnapshotDamped(id, send)
			}
		}
	} else {
		c.peersMu.Lock()
		if peer, ok := c.peers[id]; ok {
			peer.bulkLagSince = nil
		}
		c.peersMu.Unlock()
	}
	return nil
}

func (c *engineCore) sendTransaction(origin NodeId, seq uint64, send *queueSender) error {
	raw, err := c.backend.Get(LogKey(origin, seq))
	if err != nil {
		return err
	}
	if raw == nil {
		return unavailableErr("retained transaction missing")
	}
	tx, err := DecodeTransaction(raw)
	if err != nil {
		return err
	}
	return c.sendLoadedTransaction(origin, seq, &outgoing{data: raw, tx: &tx}, send)
}

func (c *engineCore) sendLoadedTransaction(origin NodeId, seq uint64, value *outgoing, send *queueSender) error {
	data := value.data
	tx := value.tx
	if len(data) < 1<<20 {
		msg := Message{Kind: MsgBatches, Transactions: []Transaction{*tx}}
		if c.config.Dissemination == DisseminationPlumtree {
			msg = Message{Kind: MsgPlumtreeData, Tx: *tx}
		}
		if err := send.TrySend(msg); err != nil {
			return ErrOverloaded
		}
		return nil
	}
	sum := sha256.Sum256(data)
	count := (len(data) + ChunkBytes - 1) / ChunkBytes
	// One stream per transaction: duplicate requests while a
	// transfer is in flight are acknowledged without re-sending.
	key := transferKey{sender: send.ID(), origin: origin, seq: seq}
	c.transfersActiveMu.Lock()
	if c.transfersActive[key] {
		c.transfersActiveMu.Unlock()
		return nil
	}
	c.transfersActive[key] = true
	c.transfersActiveMu.Unlock()
	select {
	case c.transfers <- struct{}{}:
	default:
		c.transfersActiveMu.Lock()
		delete(c.transfersActive, key)
		c.transfersActiveMu.Unlock()
		return ErrOverloaded
	}
	c.spawn(func(ctx context.Context) {
		defer func() { <-c.transfers }()
		defer func() {
			c.transfersActiveMu.Lock()
			delete(c.transfersActive, key)
			c.transfersActiveMu.Unlock()
		}()
		proof := *tx
		proof.Payload = nil
		for index, start := 0, 0; start < len(data); index, start = index+1, start+ChunkBytes {
			end := start + ChunkBytes
			if end > len(data) {
				end = len(data)
			}
			chunk := Message{Kind: MsgTransactionChunk, Chunk: Chunk{
				Proof: proof, Origin: origin, Sequence: seq, ID: tx.ID,
				Index: uint32(index), Count: uint32(count), Total: uint64(len(data)),
				Digest: sum, Data: append([]byte(nil), data[start:end]...),
			}}
			if err := send.Send(chunk); err != nil {
				return
			}
		}
	})
	return nil
}

func (c *engineCore) pushLogs() error {
	pushStart := time.Now()
	pushTiming := os.Getenv("MURMUR_PUSH_TIMING") != ""
	progress, err := c.progress()
	if err != nil {
		return err
	}
	sort.Slice(progress, func(i, j int) bool { return nodeLess(progress[i].Origin, progress[j].Origin) })
	type target struct {
		id         NodeId
		send       *queueSender
		known      map[NodeId]uint64
		sent       map[NodeId]uint64
		sentAt     map[NodeId]time.Time
		resentAt   map[NodeId]time.Time
		resent     map[NodeId]uint64
		ackAnyAt   *time.Time
		lazy       bool
		pushOrigin *NodeId
	}
	c.peersMu.Lock()
	targets := make([]target, 0, len(c.peers))
	for id, p := range c.peers {
		if !p.push {
			continue
		}
		// Snapshot everything read after unlock: session goroutines
		// keep updating the live peer maps under peersMu.
		targets = append(targets, target{id: id, send: p.send, known: cloneCounters(p.progress), sent: cloneCounters(p.sent),
			sentAt: cloneStamps(p.sentAt), resentAt: cloneStamps(p.resentAt), resent: cloneCounters(p.resent),
			ackAnyAt: cloneStamp(p.ackAnyAt), lazy: p.lazy, pushOrigin: cloneNode(p.pushOrigin)})
	}
	c.peersMu.Unlock()
	pushLog := os.Getenv("MURMUR_PUSH_LOG") != ""
	reuse := &outgoingRound{}
	pushSentTotal := 0
	pushTargets := len(targets)
	pushStreamed, pushWalked, pushGated := 0, 0, 0
	for _, t := range targets {
		var batch []Transaction
		batchBytes := 0
		var sentEntries []chunkKey
		var quietSent []NodeId
		var resentWalk []chunkKey
		for _, p := range pushOrder(progress, t.pushOrigin) {
			// Retransmit unacked ranges once pushing goes quiet: `sent`
			// dedupes the stream while new transactions flow, but without
			// a resend timer a follower that fell behind would only ever
			// trickle back via 10 s anti-entropy Needs (minutes of tail
			// for seconds of backlog). Resends are idempotent downstream.
			// A quiet resend walks forward through the unacked range at
			// full tick rate (fast recovery); only a full-range retry
			// waits out the interval, and resends never refresh the
			// quiet timer (a partial resend would defer the rest).
			// Repeating the same 16 every tick instead buries receivers
			// in duplicates and saturates small boxes.
			acked := t.known[p.Origin]
			now := time.Now()
			var sentAt *time.Time
			if at, ok := t.sentAt[p.Origin]; ok {
				at := at
				sentAt = &at
			}
			quiet := pushQuiet(sentAt, now)
			restarted := false
			var from uint64
			if !quiet {
				pushStreamed++
				from = pushFrom(acked, t.sent[p.Origin], sentAt, now)
			} else {
				// Resend only into total ack silence: while the peer
				// applies anything at all, per-origin recovery runs via
				// explicit Needs (holes) and streaming (new data), and
				// re-walking idle origins just buries the receiver in
				// already-applied duplicates. Those duplicates congest
				// the session enough to drop Acks, which would trigger
				// more walks: gating on peer-level progress breaks that
				// collapse. A fresh peer (no acks yet) still walks.
				if t.ackAnyAt != nil && now.Sub(*t.ackAnyAt) <= resendAfterQuiet {
					pushGated++
					continue
				}
				pushWalked++
				start := acked + 1
				if r, ok := t.resent[p.Origin]; ok {
					start = acked
					if r > start {
						start = r
					}
					start++
				}
				if start > p.RetainedThrough {
					if at, ok := t.resentAt[p.Origin]; ok && now.Sub(at) <= resendAfterQuiet {
						continue
					}
					start = acked + 1
					restarted = true
				}
				from = start
			}
			before := len(sentEntries)
			// One backend round-trip for the whole lookahead: per-key reads
			// here saturate a worker-threaded backend at catch-up scale.
			// Reads may run past a hole (bounded 16); iteration below
			// keeps the exact stop-at-first-missing semantics.
			end := p.RetainedThrough
			if cap := satAdd(from, 15); cap < end {
				end = cap
			}
			if cap := satAdd(acked, pushInflightWindow); cap < end {
				end = cap
			}
			if pushLog && from <= end {
				fmt.Fprintf(os.Stderr, "[push] peer=%02x%02x origin=%02x%02x from=%d end=%d acked=%d sent=%d quiet=%d resent=%d retained=%d\n",
					t.id[0], t.id[1], p.Origin[0], p.Origin[1], from, end, acked, t.sent[p.Origin], boolToU8(quiet), t.resent[p.Origin], p.RetainedThrough)
			}
			var seqs []uint64
			for seq := from; seq <= end && len(sentEntries)+len(seqs) < 64; seq++ {
				seqs = append(seqs, seq)
				if seq == end {
					break
				}
			}
			bullet, err := c.loadOutgoing(p.Origin, seqs)
			if err != nil {
				return err
			}
			for i, seq := range seqs {
				if len(sentEntries) >= 64 {
					break
				}
				value := bullet[i]
				if value == nil {
					break
				}
				data := value.data
				switch {
				case len(data) >= 1<<20:
					switch err := c.sendLoadedTransaction(p.Origin, seq, value, t.send); {
					case err == nil:
						sentEntries = append(sentEntries, chunkKey{origin: p.Origin, seq: seq})
					case isOverloaded(err):
						goto nextOrigin
					default:
						return err
					}
				case t.lazy && c.config.Dissemination == DisseminationPlumtree:
					if err := t.send.TrySend(Message{Kind: MsgIHave, Origin: p.Origin, Sequence: seq}); err != nil {
						goto nextOrigin
					}
					sentEntries = append(sentEntries, chunkKey{origin: p.Origin, seq: seq})
				case c.config.Dissemination == DisseminationPlumtree:
					msg := Message{Kind: MsgPlumtreeData, Tx: *value.tx}
					frame, err := reuse.frame([]chunkKey{{origin: p.Origin, seq: seq}}, &msg)
					if err != nil {
						return err
					}
					if err := t.send.TrySendPrepared(msg, frame); err != nil {
						goto nextOrigin
					}
					sentEntries = append(sentEntries, chunkKey{origin: p.Origin, seq: seq})
				default:
					if batchBytes+len(data) > 1<<20 {
						goto nextOrigin
					}
					batchBytes += len(data)
					batch = append(batch, *value.tx)
					sentEntries = append(sentEntries, chunkKey{origin: p.Origin, seq: seq})
				}
			}
		nextOrigin:
			if quiet && len(sentEntries) > before {
				top := sentEntries[before].seq
				for _, entry := range sentEntries[before:] {
					if entry.seq > top {
						top = entry.seq
					}
				}
				resentWalk = append(resentWalk, chunkKey{origin: p.Origin, seq: top})
				if restarted {
					quietSent = append(quietSent, p.Origin)
				}
			}
			if len(sentEntries) >= 64 || batchBytes >= 1<<20 {
				break
			}
		}
		if len(batch) > 0 {
			queued := make([]chunkKey, len(batch))
			queuedSet := make(map[chunkKey]bool, len(batch))
			for i, tx := range batch {
				queued[i] = chunkKey{origin: tx.Origin, seq: tx.Sequence}
				queuedSet[queued[i]] = true
			}
			msg := Message{Kind: MsgBatches, Transactions: batch}
			frame, err := reuse.frame(queued, &msg)
			if err != nil {
				return err
			}
			if err := t.send.TrySendPrepared(msg, frame); err != nil {
				kept := sentEntries[:0]
				for _, entry := range sentEntries {
					if !queuedSet[entry] {
						kept = append(kept, entry)
					}
				}
				sentEntries = kept
			}
		}
		if len(sentEntries) > 0 {
			pushSentTotal += len(sentEntries)
			c.mutateStatus(func(s *Status) { s.Metrics.PushedTransactions += uint64(len(sentEntries)) })
			// Only advancing pushes refresh the quiet timer; resends of
			// already-sent ranges must not suppress further resends.
			// Quiet resends stamp their own timer so the next resend
			// waits a full interval instead of firing every tick.
			c.peersMu.Lock()
			if peer, ok := c.peers[t.id]; ok && peer.send.ID() == t.send.ID() {
				last := sentEntries[len(sentEntries)-1].origin
				peer.pushOrigin = &last
				now := time.Now()
				live := make(map[NodeId]bool)
				for _, entry := range sentEntries {
					live[entry.origin] = true
				}
				for _, origin := range quietSent {
					if live[origin] {
						peer.resentAt[origin] = now
					}
				}
				for _, walk := range resentWalk {
					if live[walk.origin] && walk.seq > peer.resent[walk.origin] {
						peer.resent[walk.origin] = walk.seq
					}
				}
				for _, entry := range sentEntries {
					advanced := entry.seq > peer.sent[entry.origin]
					if entry.seq > peer.sent[entry.origin] {
						peer.sent[entry.origin] = entry.seq
					}
					if advanced {
						peer.sentAt[entry.origin] = now
					}
				}
			}
			c.peersMu.Unlock()
		}
	}
	if pushTiming && pushCtr.Add(1)%50 == 0 {
		fmt.Fprintf(os.Stderr, "[push-timing] targets=%d sent=%d stream=%d walk=%d gated=%d elapsed=%dms\n",
			pushTargets, pushSentTotal, pushStreamed, pushWalked, pushGated, time.Since(pushStart).Milliseconds())
	}
	return nil
}

func cloneCounters(m map[NodeId]uint64) map[NodeId]uint64 {
	out := make(map[NodeId]uint64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneStamps(m map[NodeId]time.Time) map[NodeId]time.Time {
	out := make(map[NodeId]time.Time, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneStamp(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	out := *t
	return &out
}

func cloneNode(n *NodeId) *NodeId {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

func boolToU8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

func isOverloaded(err error) bool {
	var rerr *Error
	return err != nil && errors.As(err, &rerr) && rerr.Kind == KindOverloaded
}

func (c *engineCore) rotate(legacyDue bool) {
	// Catch-up targeting: a behind peer that never holds a push slot
	// repairs only via 10 s anti-entropy (a minute of tail for seconds
	// of backlog), and the legacy single swap per 30 s cycles five
	// starved peers far too slowly for short verdicts. Promote every
	// behind non-push peer immediately, evicting caught-up push peers
	// first (peer send state persists across flag flips, so churn is
	// free: no replay blasts). The legacy availability swap stays for
	// steady-state coverage when nobody is behind.
	retained := make(map[NodeId]uint64)
	if progress, err := c.progress(); err == nil {
		for _, p := range progress {
			retained[p.Origin] = p.RetainedThrough
		}
	}
	lag := func(p *enginePeer) uint64 {
		var max uint64
		for origin, through := range retained {
			if l := through - p.progress[origin]; through >= p.progress[origin] && l > max {
				max = l
			}
		}
		return max
	}
	c.peersMu.Lock()
	defer c.peersMu.Unlock()
	var incoming []NodeId
	for id, p := range c.peers {
		if !p.push && !connClosed(p.conn) && lag(p) > 16 {
			incoming = append(incoming, id)
		}
	}
	rand.Shuffle(len(incoming), func(i, j int) { incoming[i], incoming[j] = incoming[j], incoming[i] })
	sort.Slice(incoming, func(i, j int) bool { return lag(c.peers[incoming[i]]) > lag(c.peers[incoming[j]]) })
	var outgoing []NodeId
	for id, p := range c.peers {
		if p.push {
			outgoing = append(outgoing, id)
		}
	}
	sort.Slice(outgoing, func(i, j int) bool { return lag(c.peers[outgoing[i]]) < lag(c.peers[outgoing[j]]) })
	swapped := false
	for i := 0; i < len(incoming) && i < len(outgoing); i++ {
		// Never evict a more-behind peer for a less-behind one.
		lagNew, lagOld := lag(c.peers[incoming[i]]), lag(c.peers[outgoing[i]])
		if lagOld >= lagNew && lagOld > 16 {
			break
		}
		c.peers[outgoing[i]].push = false
		c.peers[incoming[i]].push = true
		swapped = true
	}
	if swapped || !legacyDue {
		if swapped {
			return
		}
		if !legacyDue {
			return
		}
	}
	type candidate struct {
		id        NodeId
		available int64
	}
	var candidates []candidate
	for id, p := range c.peers {
		if !p.push && !connClosed(p.conn) {
			candidates = append(candidates, candidate{id: id, available: p.send.Available()})
		}
	}
	rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
	var newPeer *NodeId
	var bestAvail int64 = -1
	seen := 0
	for _, cand := range candidates {
		if seen >= 2 {
			break
		}
		seen++
		if newPeer == nil || cand.available > bestAvail {
			id := cand.id
			newPeer, bestAvail = &id, cand.available
		}
	}
	var oldPeer *NodeId
	var worstAvail int64
	for id, p := range c.peers {
		if p.push && (oldPeer == nil || p.send.Available() < worstAvail) {
			id := id
			oldPeer, worstAvail = &id, p.send.Available()
		}
	}
	if newPeer != nil && oldPeer != nil {
		c.peers[*oldPeer].push = false
		c.peers[*newPeer].push = true
	}
}

func (c *engineCore) probe() {
	db, node := c.backend.Identity()
	c.viewMu.Lock()
	lost := c.view.Expire()
	c.viewMu.Unlock()
	c.peersMu.Lock()
	var ids []NodeId
	for id := range c.peers {
		ids = append(ids, id)
	}
	c.peersMu.Unlock()
	if len(ids) > 0 {
		target := ids[rand.IntN(len(ids))]
		nonce := rand.Uint64()
		c.viewMu.Lock()
		if _, ok := c.view.Pending[target]; !ok {
			c.view.Pending[target] = pendingProbe{At: time.Now(), Nonce: nonce}
		}
		members := c.view.Gossip()
		c.viewMu.Unlock()
		c.peersMu.Lock()
		peer, ok := c.peers[target]
		c.peersMu.Unlock()
		if ok {
			if data := EncodePacket(&Packet{Kind: PacketPing, Database: db, Sender: node, Nonce: nonce, Members: members}); len(data) <= 1000 {
				_ = peer.conn.SendDatagram(data)
			}
		}
	}
	for _, probe := range lost {
		c.peersMu.Lock()
		count := 0
		for _, peer := range c.peers {
			if count >= 3 {
				break
			}
			count++
			if data := EncodePacket(&Packet{Kind: PacketPingRequest, Database: db, Sender: node, Target: probe.Node, Nonce: probe.Nonce}); true {
				_ = peer.conn.SendDatagram(data)
			}
		}
		c.peersMu.Unlock()
	}
}

func (c *engineCore) gossip() {
	db, node := c.backend.Identity()
	c.viewMu.Lock()
	members := c.view.Gossip()
	c.viewMu.Unlock()
	data := EncodePacket(&Packet{Kind: PacketGossip, Database: db, Sender: node, Members: members})
	if len(data) > 1000 {
		return
	}
	c.peersMu.Lock()
	conns := make([]quicConn, 0, len(c.peers))
	for _, peer := range c.peers {
		conns = append(conns, peer.conn)
	}
	c.peersMu.Unlock()
	rand.Shuffle(len(conns), func(i, j int) { conns[i], conns[j] = conns[j], conns[i] })
	for i := 0; i < len(conns) && i < 3; i++ {
		_ = conns[i].SendDatagram(data)
	}
}

func (c *engineCore) packet(id NodeId, conn quicConn, data []byte) error {
	actual, err := peerID(conn, c.config)
	if err != nil {
		return err
	}
	if actual != id {
		return authErr("membership identity")
	}
	packet, err := DecodePacket(data)
	if err != nil {
		return err
	}
	db, node := c.backend.Identity()
	switch packet.Kind {
	case PacketGossip:
		if packet.Database != db || packet.Sender != id || len(packet.Members) > 4 {
			return authErr("membership envelope")
		}
		c.viewMu.Lock()
		for _, member := range packet.Members {
			c.view.Merge(member)
		}
		c.viewMu.Unlock()
	case PacketIndirectAck:
		if packet.Database != db || packet.Sender != id {
			return authErr("indirect acknowledgement")
		}
		c.viewMu.Lock()
		c.view.Acknowledged(packet.Target, packet.Nonce)
		c.viewMu.Unlock()
	case PacketPing, PacketAck:
		if packet.Database != db || packet.Sender != id || len(packet.Members) > 4 {
			return authErr("membership envelope")
		}
		c.viewMu.Lock()
		for _, member := range packet.Members {
			c.view.Merge(member)
		}
		c.view.Acknowledged(id, packet.Nonce)
		reply := c.view.Gossip()
		c.viewMu.Unlock()
		c.relaysMu.Lock()
		relay, ok := c.relays[relayKey{target: id, nonce: packet.Nonce}]
		if ok {
			delete(c.relays, relayKey{target: id, nonce: packet.Nonce})
		}
		c.relaysMu.Unlock()
		if ok {
			c.peersMu.Lock()
			requester, rok := c.peers[relay.requester]
			c.peersMu.Unlock()
			if rok {
				bytes := EncodePacket(&Packet{Kind: PacketIndirectAck, Database: db, Sender: node, Target: id, Nonce: packet.Nonce})
				_ = requester.conn.SendDatagram(bytes)
			}
		}
		if packet.Kind == PacketPing {
			bytes := EncodePacket(&Packet{Kind: PacketAck, Database: db, Sender: node, Nonce: packet.Nonce, Members: reply})
			if len(bytes) <= 1000 {
				_ = conn.SendDatagram(bytes)
			}
		}
	case PacketPingRequest:
		if packet.Database != db || packet.Sender != id {
			return authErr("membership envelope")
		}
		now := time.Now()
		c.relaysMu.Lock()
		for key, entry := range c.relays {
			if now.Sub(entry.at) >= 5*time.Second {
				delete(c.relays, key)
			}
		}
		if len(c.relays) < 4096 {
			c.relays[relayKey{target: packet.Target, nonce: packet.Nonce}] = relayEntry{requester: packet.Sender, at: now}
		}
		c.relaysMu.Unlock()
		c.peersMu.Lock()
		relay, ok := c.peers[packet.Target]
		c.peersMu.Unlock()
		if ok {
			c.viewMu.Lock()
			members := c.view.Gossip()
			c.viewMu.Unlock()
			bytes := EncodePacket(&Packet{Kind: PacketPing, Database: db, Sender: node, Nonce: packet.Nonce, Members: members})
			if len(bytes) <= 1000 {
				_ = relay.conn.SendDatagram(bytes)
			}
		}
	default:
		return authErr("membership envelope")
	}
	return nil
}

// Snapshot builds checkpoint and re-scan the spool (seconds under load),
// so they run as background tasks: awaiting a build inline would stall
// this connection's read loop until it finishes, wedging control
// traffic exactly when catch-up needs it. Builds are paced
// process-wide: colocated nodes racing to serve the same rejoin would
// otherwise dogpile the CPU and slow every build several-fold. Queued
// builds run their reuse check after waking, so losers reuse the
// winner's committed build instead of rescanning. Failures are silent;
// the requester retries damped.
func (c *engineCore) sendSnapshot(id NodeId, send *queueSender) {
	fmt.Fprintf(os.Stderr, "[snap-donor] request from %02x%02x\n", id[0], id[1])
	c.spawn(func(ctx context.Context) {
		select {
		case snapshotBuildGate <- struct{}{}:
			defer func() { <-snapshotBuildGate }()
		case <-ctx.Done():
			fmt.Fprintf(os.Stderr, "[snap-donor] gate closed\n")
			return
		}
		fmt.Fprintf(os.Stderr, "[snap-donor] building for %02x%02x\n", id[0], id[1])
		_ = Maintain(c.backend, NowMillis())
		manifest, err := c.backend.CreateSnapshot(id, c.config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[snap-donor] build failed: %v\n", err)
			return
		}
		fmt.Fprintf(os.Stderr, "[snap-donor] manifest built chunks=%d transfer=%02x%02x\n",
			len(manifest.Chunks), manifest.Transfer[0], manifest.Transfer[1])
		_ = send.TrySend(Message{Kind: MsgSnapshotManifest, Manifest: manifest})
	})
}

// requestSnapshotDamped sends SnapshotRequest with per-peer damping:
// missing history fires on every progress round, and undamped repeats
// make every donor rebuild and re-manifest continuously while the victim
// spawns a duplicate chunk serve task per manifest, clogging the data
// lane with duplicate chunks. First requests stay immediate (failover
// across donors is unaffected); repeats to the same peer wait 5s.
func (c *engineCore) requestSnapshotDamped(id NodeId, send *queueSender) {
	now := time.Now()
	sendIt := true
	c.peersMu.Lock()
	if p, ok := c.peers[id]; ok {
		if p.snapshotReqAt != nil && now.Sub(*p.snapshotReqAt) < 5*time.Second {
			sendIt = false
		} else {
			p.snapshotReqAt = &now
		}
	}
	c.peersMu.Unlock()
	if sendIt {
		_ = send.TrySend(Message{Kind: MsgSnapshotRequest})
	}
}

func (c *engineCore) requestSnapshotChunks(id NodeId, transfer NodeId, send *queueSender) error {
	now := NowMillis()
	if raw, err := c.backend.Get(SnapshotReceiptKey(transfer)); err != nil {
		return err
	} else if raw != nil {
		receipt, err := DecodeIncoming(raw)
		if err != nil {
			return err
		}
		if receipt.Manifest.Donor != id {
			return authErr("snapshot receipt identity")
		}
		return send.TrySend(Message{Kind: MsgSnapshotApplied, Transfer: transfer, Digest: receipt.Manifest.Digest})
	}
	r, err := c.cachedIncoming()
	if err != nil {
		return err
	}
	if r == nil {
		return nil
	}
	if r.Manifest.Transfer != transfer || r.Manifest.Donor != id {
		return authErr("snapshot request identity")
	}
	schema, err := c.backend.Schema()
	if err != nil {
		return err
	}
	if r.Manifest.Schema != SchemaDigest(schema) {
		if err := send.TrySend(Message{Kind: MsgSnapshotCancel, Transfer: transfer}); err != nil {
			return err
		}
		if err := c.backend.Commit([]Record{DeleteRecord(SnapshotHeaderKey(SnapshotIn, transfer))}); err != nil {
			return err
		}
		// Existing signed schema negotiation must complete before a fresh capture.
		_ = send.TrySend(Message{Kind: MsgSchemaRequest})
		return nil
	}
	missing, err := c.cachedMissing(r)
	if err != nil {
		return err
	}
	installKey := snapshotInstallKey(transfer)
	before, err := c.backend.Get(installKey)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		fmt.Fprintf(os.Stderr, "[snap-rx] transfer %02x%02x missing %d/%d chunks\n",
			transfer[0], transfer[1], len(missing), len(r.Manifest.Chunks))
		return send.TrySend(Message{Kind: MsgSnapshotNeed, Transfer: transfer, Indexes: missing})
	}
	installed, err := Finish(c.backend, c.config, id, transfer, now)
	if err != nil {
		return err
	}
	if installed {
		fmt.Fprintf(os.Stderr, "[snap-rx] transfer %02x%02x INSTALLED\n", transfer[0], transfer[1])
		c.mutateStatus(func(s *Status) { s.Metrics.SnapshotsApplied++ })
		return send.TrySend(Message{Kind: MsgSnapshotApplied, Transfer: transfer, Digest: r.Manifest.Digest})
	}
	after, err := c.backend.Get(installKey)
	if err != nil {
		return err
	}
	if string(after) != string(before) {
		c.notifyApply()
	}
	return nil
}

func (c *engineCore) sendSnapshotChunks(id NodeId, transfer NodeId, indexes []uint32, send *queueSender) error {
	if len(indexes) == 0 || len(indexes) > 128 {
		return protocolErr("snapshot request indexes")
	}
	seen := make(map[uint32]bool, len(indexes))
	for _, i := range indexes {
		if seen[i] {
			return protocolErr("snapshot request indexes")
		}
		seen[i] = true
	}
	manifest, err := Outgoing(c.backend, id, transfer, NowMillis())
	if err != nil {
		return err
	}
	if manifest == nil {
		return send.TrySend(Message{Kind: MsgSnapshotUnavailable, Transfer: transfer})
	}
	for _, i := range indexes {
		if int(i) >= len(manifest.Chunks) {
			return protocolErr("snapshot chunk index")
		}
	}
	snapshotServing.Lock()
	if snapshotServing.entries[transfer] {
		snapshotServing.Unlock()
		return nil
	}
	snapshotServing.entries[transfer] = true
	snapshotServing.Unlock()
	defer func() {
		snapshotServing.Lock()
		delete(snapshotServing.entries, transfer)
		snapshotServing.Unlock()
	}()
	// Duplicate page requests or transaction transfers can temporarily use
	// the shared budget. The receiver's durable retry loop will ask again.
	select {
	case c.transfers <- struct{}{}:
	default:
		return nil
	}
	indexesCopy := append([]uint32(nil), indexes...)
	c.spawn(func(ctx context.Context) {
		defer func() { <-c.transfers }()
		for _, index := range indexesCopy {
			raw, err := c.backend.Get(SnapshotChunkKey(SnapshotOut, transfer, index))
			if err != nil || raw == nil {
				_ = send.Send(Message{Kind: MsgSnapshotUnavailable, Transfer: transfer})
				return
			}
			if err := send.Send(Message{Kind: MsgSnapshotChunk, Transfer: transfer, Index: index, Data: raw}); err != nil {
				return
			}
		}
		_ = send.Send(Message{Kind: MsgSnapshotDone, Transfer: transfer})
	})
	return nil
}

// Outcome of vetting a received schema manifest, without adopting it.
// Every verified manifest is recorded (per signer) so payload filtering can
// drop tables the sender has retired; only the configured master (or any
// trusted origin in legacy mode) is adopted. Stale duplicates stay idempotent
// success so retransmits never kill sessions.
type schemaDecision int

const (
	schemaCurrent schemaDecision = iota
	schemaAdopt
	schemaStale
	schemaRecord
)

func recordSchemaAccepted(backend Backend, signed *SignedSchema) error {
	return backend.Commit([]Record{PutRecord(SchemaAcceptedKey(signed.Signer), encode(func(e *postcard.Encoder) {
		e.U64(signed.Sequence)
		e.ByteString(signed.Schema)
	}))})
}

func checkSchemaManifest(config *Config, backend Backend, signed *SignedSchema) (schemaDecision, error) {
	db, node := backend.Identity()
	if err := signed.Verify(db, signed.Signer, config.TrustedOrigins, config.MaxTransactionBytes); err != nil {
		return schemaStale, err
	}
	adoptable := true
	if config.SchemaMaster != nil {
		adoptable = signed.Signer == *config.SchemaMaster && !(config.Master && *config.SchemaMaster == node)
	}
	key := SchemaAcceptedKey(signed.Signer)
	var acceptedSeq uint64
	if raw, err := backend.Get(key); err != nil {
		return schemaStale, err
	} else if raw != nil {
		v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 {
			seq := d.U64()
			_ = d.Bytes()
			return seq
		})
		if err != nil {
			return schemaStale, err
		}
		acceptedSeq = v
	}
	if signed.Sequence <= acceptedSeq {
		return schemaStale, nil
	}
	ours, err := backend.Schema()
	if err != nil {
		return schemaStale, err
	}
	if string(signed.Schema) == string(ours) {
		if err := recordSchemaAccepted(backend, signed); err != nil {
			return schemaStale, err
		}
		return schemaCurrent, nil
	}
	if !adoptable || !config.AcceptRemoteSchema {
		// Recorded for filtering, never adopted.
		if err := recordSchemaAccepted(backend, signed); err != nil {
			return schemaStale, err
		}
		return schemaRecord, nil
	}
	return schemaAdopt, nil
}

func proofTx(tx *Transaction) Transaction {
	header := *tx
	header.Payload = nil
	return header
}

func parseID(s string) (NodeId, error) {
	var zero NodeId
	if len(s) != 32 {
		return zero, protocolErr("node id")
	}
	for i := range zero {
		v, err := strconv.ParseUint(s[i*2:i*2+2], 16, 8)
		if err != nil {
			return zero, protocolErr("node id")
		}
		zero[i] = byte(v)
	}
	return zero, nil
}

func compatibleCaps(capabilities, required uint64) bool {
	return required&^Capabilities == 0 && capabilities&Required == Required
}

// Next sequence to push for an origin: past the sent frontier while the
// stream advances (dedup), back at the acked frontier once quiet (ARQ).
// Sorted durable heads, rotated independently for each peer. A bounded round
// must not let permanently backlogged early origins consume every batch slot.
func pushOrder(progress []Progress, after *NodeId) []Progress {
	split := 0
	if after != nil {
		split = sort.Search(len(progress), func(i int) bool {
			return !nodeLess(progress[i].Origin, *after) && progress[i].Origin != *after
		})
	}
	out := make([]Progress, 0, len(progress))
	out = append(out, progress[split:]...)
	out = append(out, progress[:split]...)
	return out
}

func pushQuiet(sentAt *time.Time, now time.Time) bool {
	if sentAt == nil {
		return true
	}
	return now.Sub(*sentAt) > resendAfterQuiet
}

func pushFrom(acked, sent uint64, sentAt *time.Time, now time.Time) uint64 {
	base := acked
	if !pushQuiet(sentAt, now) && sent > base {
		base = sent
	}
	return satAdd(base, 1)
}

func (c *engineCore) readFloor(origin NodeId) (uint64, error) {
	raw, err := c.backend.Get(FloorKey(origin))
	if err != nil {
		return 0, err
	}
	if raw == nil {
		return 0, nil
	}
	return decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
}

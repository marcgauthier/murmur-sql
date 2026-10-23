package replication

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/postcard"
)

// Disposable per-push working set; Spool remains authoritative. Separate byte
// limits cover decoded transactions and prepared frames, with bounded key counts.
const outgoingCacheBytes = 4 << 20

// Disposable admission accounting for durable staged keys. Spool owns their
// contents; mutations update the count under the existing stage/cache locks.
type stagedSizes struct {
	sizes map[string]int
	bytes int
}

func (s *stagedSizes) insert(key []byte, size int) error {
	old := s.sizes[string(key)]
	next := s.bytes - old + size
	if next < 0 || (size > 0 && next < s.bytes-old) {
		return ErrOverloaded
	}
	if s.sizes == nil {
		s.sizes = make(map[string]int)
	}
	s.sizes[string(key)] = size
	s.bytes = next
	return nil
}

func (s *stagedSizes) remove(key []byte) {
	if size, ok := s.sizes[string(key)]; ok {
		delete(s.sizes, string(key))
		s.bytes -= size
	}
}

type outgoing struct {
	data []byte
	tx   *Transaction
}

// Shared retained-log cache across push rounds. Log records are immutable,
// so entries are disposable (Spool authoritative) and need only a TTL
// against retention trimming plus a byte bound. FIFO eviction suits the
// sliding unacked window (oldest entries are acked past and never reused).
// Without this every push tick re-reads the whole window from Spool
// (~400 reads/s), queueing the worker behind itself.
const outgoingSharedBytes = 8 << 20

var outgoingSharedTTL = 10 * time.Second

type outgoingEntry struct {
	value  *outgoing
	at     time.Time
	charge uint64
}

type outgoingShared struct {
	values map[chunkKey]outgoingEntry
	order  []chunkKey
	head   int
	bytes  int
}

func (s *outgoingShared) get(key chunkKey, now time.Time) *outgoing {
	entry, ok := s.values[key]
	if !ok {
		return nil
	}
	if now.Sub(entry.at) < outgoingSharedTTL {
		return entry.value
	}
	delete(s.values, key)
	s.bytes -= int(entry.charge)
	if s.bytes < 0 {
		s.bytes = 0
	}
	return nil
}

func (s *outgoingShared) evictOne() bool {
	for s.head < len(s.order) {
		old := s.order[s.head]
		s.head++
		if entry, ok := s.values[old]; ok {
			delete(s.values, old)
			s.bytes -= int(entry.charge)
			if s.bytes < 0 {
				s.bytes = 0
			}
		}
		if s.head > 4096 && s.head*2 > len(s.order) {
			s.order = append([]chunkKey(nil), s.order[s.head:]...)
			s.head = 0
		}
		return true
	}
	return false
}

func (s *outgoingShared) insert(key chunkKey, value *outgoing, charge uint64, now time.Time) {
	if s.values == nil {
		s.values = make(map[chunkKey]outgoingEntry)
	}
	if _, ok := s.values[key]; ok {
		return
	}
	if charge > outgoingSharedBytes {
		return
	}
	for s.bytes+int(charge) > outgoingSharedBytes {
		if !s.evictOne() {
			break
		}
	}
	s.order = append(s.order, key)
	s.bytes += int(charge)
	s.values[key] = outgoingEntry{value: value, at: now, charge: charge}
	for len(s.order)-s.head > 16384 {
		if !s.evictOne() {
			break
		}
	}
}

type outgoingRound struct {
	frames     map[string]*Prepared
	frameBytes int
}

func outgoingFrameKey(keys []chunkKey) string {
	buf := make([]byte, 0, len(keys)*24)
	for _, k := range keys {
		buf = append(buf, k.origin[:]...)
		var tmp [8]byte
		for i := 0; i < 8; i++ {
			tmp[i] = byte(k.seq >> (8 * i))
		}
		buf = append(buf, tmp[:]...)
	}
	return string(buf)
}

func readMany(backend Backend, keys [][]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	values, err := backend.GetMany(keys)
	if err != nil {
		return nil, err
	}
	if len(values) != len(keys) {
		return nil, storageErr("get_many returned the wrong number of values")
	}
	return values, nil
}

func readBounded(backend Backend, keys [][]byte, maxBytes int) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	values, err := backend.GetManyBounded(keys, maxBytes)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 || len(values) > len(keys) {
		return nil, storageErr("bounded read returned invalid prefix length")
	}
	if len(values) > 1 {
		total := 0
		for _, v := range values {
			total += len(v)
		}
		if total > maxBytes {
			return nil, storageErr("bounded read exceeded byte budget")
		}
	}
	return values, nil
}

func (c *engineCore) loadOutgoing(origin NodeId, seqs []uint64) ([]*outgoing, error) {
	now := time.Now()
	out := make([]*outgoing, len(seqs))
	var missing []int
	func() {
		c.outgoingMu.Lock()
		defer c.outgoingMu.Unlock()
		for i, seq := range seqs {
			if v := c.outgoing.get(chunkKey{origin: origin, seq: seq}, now); v != nil {
				out[i] = v
			} else {
				missing = append(missing, i)
			}
		}
	}()
	if len(missing) > 0 {
		keys := make([][]byte, len(missing))
		for j, i := range missing {
			keys[j] = LogKey(origin, seqs[i])
		}
		values, err := readBounded(c.backend, keys, 1<<20)
		if err != nil {
			return nil, err
		}
		c.outgoingMu.Lock()
		defer c.outgoingMu.Unlock()
		for j, i := range missing {
			if j >= len(values) {
				break
			}
			data := values[j]
			seq := seqs[i]
			if data == nil {
				continue
			}
			tx, err := DecodeTransaction(data)
			if err != nil {
				return nil, err
			}
			if tx.Origin != origin || tx.Sequence != seq {
				return nil, protocolErr("retained transaction key mismatch")
			}
			charge := uint64(len(data) + len(tx.Payload) + len(tx.Signature) + 512)
			value := &outgoing{data: data, tx: &tx}
			c.outgoing.insert(chunkKey{origin: origin, seq: seq}, value, charge, now)
			out[i] = value
		}
	}
	return out, nil
}

func (r *outgoingRound) frame(keys []chunkKey, message *Message) (*Prepared, error) {
	if r.frames == nil {
		r.frames = make(map[string]*Prepared)
	}
	key := outgoingFrameKey(keys)
	if frame, ok := r.frames[key]; ok {
		return frame, nil
	}
	frame, err := NewPrepared(message)
	if err != nil {
		return nil, err
	}
	charge := len(frame.Bytes) + len(keys)*24 + 256
	if len(r.frames) < 1024 && charge <= outgoingCacheBytes-r.frameBytes {
		r.frameBytes += charge
		r.frames[key] = frame
	}
	return frame, nil
}

func (c *engineCore) stagedTotals() (int, int, error) {
	c.stagedMu.Lock()
	defer c.stagedMu.Unlock()
	if c.staged == nil {
		loaded := &stagedSizes{}
		keys, err := c.backend.Keys([]byte("07/replication/staged/"))
		if err != nil {
			return 0, 0, err
		}
		for w := 0; w < len(keys); w += 64 {
			window := keys[w:min(w+64, len(keys))]
			cursor := 0
			for cursor < len(window) {
				values, err := readBounded(c.backend, window[cursor:], 1<<20)
				if err != nil {
					return 0, 0, err
				}
				count := len(values)
				for j, value := range values {
					if value != nil {
						if err := loaded.insert(window[cursor+j], len(value)); err != nil {
							return 0, 0, err
						}
					}
				}
				cursor += count
			}
		}
		c.staged = loaded
	}
	return len(c.staged.sizes), c.staged.bytes, nil
}

func (c *engineCore) stagedMap() (map[string]int, error) {
	if _, _, err := c.stagedTotals(); err != nil {
		return nil, err
	}
	c.stagedMu.Lock()
	defer c.stagedMu.Unlock()
	out := make(map[string]int, len(c.staged.sizes))
	for k, v := range c.staged.sizes {
		out[k] = v
	}
	return out, nil
}

func (c *engineCore) stage(tx Transaction) error {
	return c.stageBatch([]Transaction{tx})
}

type stagedPending struct {
	tx     Transaction
	digest [32]byte
	bytes  []byte
}

func (c *engineCore) stageBatch(transactions []Transaction) error {
	// Authenticate the complete bounded envelope before publishing anything.
	// Keep individual signatures and reject conflicting identities within it.
	db, _ := c.backend.Identity()
	pending := make(map[string]stagedPending)
	for _, tx := range transactions {
		tx := tx
		c.verifyMu.Lock()
		verified, err := c.verification.Verify(&tx, db, c.config.TrustedOrigins, c.config.MaxTransactionBytes)
		c.verifyMu.Unlock()
		if err != nil {
			return err
		}
		key := string(StageKey(tx.Origin, tx.Sequence))
		if old, ok := pending[key]; ok {
			if old.digest != verified.Digest {
				return authErr("conflicting staged transaction")
			}
			continue
		}
		pending[key] = stagedPending{tx: tx, digest: verified.Digest, bytes: verified.Bytes}
	}
	// Verification, existence probes and filtering run without holding
	// any gate: staging must not wait on applies, retention or
	// snapshots (that stalls the connection read loop and wedges
	// control traffic). Only the commit section below serializes,
	// briefly, against other staging commits, so the observed-max
	// read-modify-write, admission totals and conflict recheck stay
	// atomic. Stale pre-reads only admit work the recheck or the apply
	// loop drops.
	if _, _, err := c.stagedTotals(); err != nil {
		return err
	}
	order := make([]stagedPending, 0, len(pending))
	keys := make([]string, 0, len(pending))
	for key := range pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		order = append(order, pending[key])
	}
	if len(order) == 0 {
		return nil
	}
	probe := make([][]byte, 0, len(order)*2)
	for _, p := range order {
		probe = append(probe, ReceiptKey(p.tx.Origin, p.tx.Sequence))
		probe = append(probe, StageKey(p.tx.Origin, p.tx.Sequence))
	}
	found, err := readMany(c.backend, probe)
	if err != nil {
		return err
	}
	originSet := make(map[NodeId]bool)
	for _, p := range order {
		originSet[p.tx.Origin] = true
	}
	origins := make([]NodeId, 0, len(originSet))
	for origin := range originSet {
		origins = append(origins, origin)
	}
	sort.Slice(origins, func(i, j int) bool { return nodeLess(origins[i], origins[j]) })
	floorKeys := make([][]byte, len(origins))
	for i, origin := range origins {
		floorKeys[i] = FloorKey(origin)
	}
	floors := make(map[NodeId]uint64)
	floorValues, err := readMany(c.backend, floorKeys)
	if err != nil {
		return err
	}
	for i, origin := range origins {
		var floor uint64
		if floorValues[i] != nil {
			v, err := decodeOne(floorValues[i], func(d *postcard.Decoder) uint64 { return d.U64() })
			if err != nil {
				return err
			}
			floor = v
		}
		floors[origin] = floor
	}
	var candidates []stagedPending
	stageTiming := os.Getenv("MURMUR_STAGE_TIMING") != ""
	var cFloor, cApplied, cStaged int
	for i, p := range order {
		var receipt, old []byte
		if i*2 < len(found) {
			receipt = found[i*2]
		}
		if i*2+1 < len(found) {
			old = found[i*2+1]
		}
		floor := floors[p.tx.Origin]
		if p.tx.Sequence <= floor {
			cFloor++
			continue
		}
		if receipt != nil {
			if string(receipt) != string(p.digest[:]) {
				return authErr("conflicting transaction identity")
			}
			cApplied++
			continue
		}
		if old != nil {
			prior, err := DecodeTransaction(old)
			if err != nil {
				return err
			}
			if prior.Digest() != p.digest {
				return authErr("conflicting staged transaction")
			}
			cStaged++
			continue
		}
		candidates = append(candidates, p)
	}
	_ = stageTiming
	_ = cFloor
	_ = cApplied
	_ = cStaged
	if os.Getenv("MURMUR_STAGE_TIMING") != "" {
		stageNew.Add(uint64(len(candidates)))
		stageApplied.Add(uint64(cApplied))
		stageStaged.Add(uint64(cStaged))
		stageFloor.Add(uint64(cFloor))
		if stageCtr.Add(1)%200 == 0 {
			fmt.Fprintf(os.Stderr, "[stage-timing] new=%d dup_applied=%d dup_staged=%d floor=%d\n",
				stageNew.Swap(0), stageApplied.Swap(0), stageStaged.Swap(0), stageFloor.Swap(0))
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	var finalAdded [][2]any
	{
		c.stageCommit.Lock()
		defer c.stageCommit.Unlock()
		// Recheck against concurrent staging commits: receipts or staged
		// entries may have appeared since the first read.
		re := make([][]byte, 0, len(candidates)*2)
		for _, p := range candidates {
			re = append(re, ReceiptKey(p.tx.Origin, p.tx.Sequence))
			re = append(re, StageKey(p.tx.Origin, p.tx.Sequence))
		}
		fresh, err := readMany(c.backend, re)
		if err != nil {
			return err
		}
		var records []Record
		stagedMax := make(map[NodeId]uint64)
		var kept []stagedPending
		for i, p := range candidates {
			var receipt, old []byte
			if i*2 < len(fresh) {
				receipt = fresh[i*2]
			}
			if i*2+1 < len(fresh) {
				old = fresh[i*2+1]
			}
			if receipt != nil {
				if string(receipt) != string(p.digest[:]) {
					return authErr("conflicting transaction identity")
				}
				continue
			}
			if old != nil {
				prior, err := DecodeTransaction(old)
				if err != nil {
					return err
				}
				if prior.Digest() != p.digest {
					return authErr("conflicting staged transaction")
				}
				continue
			}
			if p.tx.Sequence > stagedMax[p.tx.Origin] {
				stagedMax[p.tx.Origin] = p.tx.Sequence
			}
			key := StageKey(p.tx.Origin, p.tx.Sequence)
			finalAdded = append(finalAdded, [2]any{key, len(p.bytes)})
			records = append(records, PutRecord(key, p.bytes))
			kept = append(kept, p)
		}
		_ = kept
		if len(finalAdded) == 0 {
			return nil
		}
		// Fresh admission totals: concurrent commits may have filled the
		// cache since our first read.
		c.stagedMu.Lock()
		freshCount, freshTotal := len(c.staged.sizes), c.staged.bytes
		c.stagedMu.Unlock()
		if freshCount+len(finalAdded) >= c.config.StagingTransfers {
			return ErrOverloaded
		}
		total := freshTotal
		for _, added := range finalAdded {
			total += added[1].(int)
			if total < freshTotal {
				return ErrOverloaded
			}
		}
		if total > c.config.StagingBytes {
			return ErrOverloaded
		}
		// Fresh observed maxima merged with this batch's sequences.
		maxOrigins := make([]NodeId, 0, len(stagedMax))
		for origin := range stagedMax {
			maxOrigins = append(maxOrigins, origin)
		}
		sort.Slice(maxOrigins, func(i, j int) bool { return nodeLess(maxOrigins[i], maxOrigins[j]) })
		obsKeys := make([][]byte, len(maxOrigins))
		for i, origin := range maxOrigins {
			obsKeys[i] = ObservedKey(origin)
		}
		obsValues, err := readMany(c.backend, obsKeys)
		if err != nil {
			return err
		}
		for i, origin := range maxOrigins {
			var before uint64
			if obsValues[i] != nil {
				v, err := decodeOne(obsValues[i], func(d *postcard.Decoder) uint64 { return d.U64() })
				if err != nil {
					return err
				}
				before = v
			}
			merged := before
			if stagedMax[origin] > merged {
				merged = stagedMax[origin]
			}
			records = append(records, PutRecord(ObservedKey(origin), encode(func(e *postcard.Encoder) { e.U64(merged) })))
		}
		if err := c.backend.Commit(records); err != nil {
			return err
		}
		var clockMillis uint64
		var clockLogical uint32
		for _, p := range candidates {
			if p.tx.Millis > clockMillis || (p.tx.Millis == clockMillis && p.tx.Logical > clockLogical) {
				clockMillis, clockLogical = p.tx.Millis, p.tx.Logical
			}
		}
		if err := c.backend.Observe(clockMillis, clockLogical); err != nil {
			return err
		}
	}
	count := len(finalAdded)
	c.stagedMu.Lock()
	for _, added := range finalAdded {
		if err := c.staged.insert(added[0].([]byte), added[1].(int)); err != nil {
			c.stagedMu.Unlock()
			return err
		}
	}
	c.stagedMu.Unlock()
	c.mutateStatus(func(s *Status) { s.Metrics.ReceivedTransactions += uint64(count) })
	c.notifyApply()
	return nil
}

func (c *engineCore) resendSnapshotReceipts(cursor *int) error {
	// Completion receipts survive lost ACKs and either process restarting.
	keys, err := c.backend.Keys(SnapshotReceipts)
	if err != nil {
		return err
	}
	if *cursor > len(keys) {
		*cursor = 0
	}
	end := *cursor + 128
	if end > len(keys) {
		end = len(keys)
	}
	for _, key := range keys[*cursor:end] {
		raw, err := c.backend.Get(key)
		if err != nil {
			return err
		}
		if raw == nil {
			continue
		}
		r, err := DecodeIncoming(raw)
		if err != nil {
			return err
		}
		c.peersMu.Lock()
		peer, ok := c.peers[r.Manifest.Donor]
		c.peersMu.Unlock()
		if ok {
			_ = peer.send.TrySend(Message{Kind: MsgSnapshotApplied, Transfer: r.Manifest.Transfer, Digest: r.Manifest.Digest})
		}
	}
	*cursor = end
	return nil
}

// cachedIncoming reads the incoming snapshot header without a full key
// scan on every apply. `ReadIncoming` lists every key under the incoming
// prefix (thousands of chunk keys while a snapshot is active), which
// costs seconds per apply under load. Accepts enforce a single incoming
// transfer, so a transfer hint plus one validating point read is
// equivalent: a live header decodes fresh from the read, a
// finished/cancelled/expired one misses and falls back to a rescan
// (which also re-arms the hint). The empty hint always rescans, so a
// newly accepted transfer is never missed.
func (c *engineCore) cachedIncoming() (*Incoming, error) {
	c.incomingHintMu.Lock()
	hint := c.incomingHint
	c.incomingHintMu.Unlock()
	if hint != nil {
		if raw, err := c.backend.Get(SnapshotHeaderKey(SnapshotIn, *hint)); err != nil {
			return nil, err
		} else if raw != nil {
			r, err := DecodeIncoming(raw)
			if err != nil {
				return nil, err
			}
			return &r, nil
		}
	}
	// Negative cache: with no transfer active (the common case) every
	// apply batch would otherwise re-scan the snapshot keyspace (~100ms
	// under contention). Accept hints the new transfer directly, so the
	// TTL here is only a backstop and never delays a real transfer.
	now := time.Now()
	c.incomingNoneAtMu.Lock()
	noneAt := c.incomingNoneAt
	c.incomingNoneAtMu.Unlock()
	if noneAt != nil && now.Sub(*noneAt) < 5*time.Second {
		return nil, nil
	}
	found, err := ReadIncoming(c.backend)
	if err != nil {
		return nil, err
	}
	c.incomingHintMu.Lock()
	if found != nil {
		transfer := found.Manifest.Transfer
		c.incomingHint = &transfer
	} else {
		c.incomingHint = nil
	}
	c.incomingHintMu.Unlock()
	if found == nil {
		c.incomingNoneAtMu.Lock()
		c.incomingNoneAt = &now
		c.incomingNoneAtMu.Unlock()
	}
	return found, nil
}

// cachedMissing lists missing snapshot chunks without a key scan per
// apply. Every other caller consults this only after `cachedIncoming`
// validates the header live, and maintenance never strips chunks under a
// live header (header and chunks expire in the same commit), so the only
// mutation that can stale the entry is a chunk receive, which clears
// it. Stale entries for finished transfers simply miss on the
// transfer key and recompute.
func (c *engineCore) cachedMissing(r *Incoming) ([]uint32, error) {
	c.missingMu.Lock()
	cached := c.missingCache
	c.missingMu.Unlock()
	if cached != nil && cached.transfer == r.Manifest.Transfer {
		return cached.missing, nil
	}
	missing, err := Missing(c.backend, r)
	if err != nil {
		return nil, err
	}
	c.missingMu.Lock()
	c.missingCache = &missingEntry{transfer: r.Manifest.Transfer, missing: missing}
	c.missingMu.Unlock()
	return missing, nil
}

// requestHole asks ONE peer for an apply-blocking hole, damped per
// range. The old broadcast asked every peer on every blocked batch;
// concurrent streamers stage out of order constantly, so every spurious
// hole fanned out N-way and the responses (which bypass the push
// cursor) arrived as already-applied duplicates, saturating CPU and
// queues. Out-of-order batches land within milliseconds; a hole that
// persists past the damping window is re-asked (possibly of another
// peer).
func (c *engineCore) requestHole(origin NodeId, from, through uint64) {
	now := time.Now()
	c.needAtMu.Lock()
	if len(c.needAt) > 4096 {
		for key, at := range c.needAt {
			if now.Sub(at) >= time.Minute {
				delete(c.needAt, key)
			}
		}
	}
	if prev, ok := c.needAt[chunkKey{origin: origin, seq: from}]; ok && now.Sub(prev) < time.Second {
		c.needAtMu.Unlock()
		return
	}
	c.needAt[chunkKey{origin: origin, seq: from}] = now
	c.needAtMu.Unlock()
	// Prefer a peer that reports having the range; fall back to the
	// best-known holder so a stale view still retries somewhere.
	c.peersMu.Lock()
	defer c.peersMu.Unlock()
	var best *queueSender
	var bestHave uint64
	for _, peer := range c.peers {
		have := peer.progress[origin]
		if have >= through {
			best = peer.send
			break
		}
		if have >= bestHave {
			bestHave = have
			best = peer.send
		}
	}
	if best != nil {
		_ = best.TrySend(Message{Kind: MsgNeed, Origin: origin, From: from, Through: through})
	}
}

func (c *engineCore) applyPending() (bool, error) {
	started := time.Now()
	timing := os.Getenv("MURMUR_APPLY_TIMING") != ""
	limit := c.config.BatchTransactions
	if c.config.AdaptiveSync {
		if adaptive := int(c.adaptiveBatch.Load()); adaptive < limit {
			limit = adaptive
		}
		if limit < 1 {
			limit = 1
		}
	}
	// Serialize against retention collection (shared retained heads),
	// but never against staging or snapshots: applies do slow SQLite
	// merge work and must not stall the connection read loop.
	c.retainedGate.Lock()
	defer c.retainedGate.Unlock()
	tGate := time.Since(started)
	{
		r, err := c.cachedIncoming()
		if err != nil {
			return false, err
		}
		if r != nil {
			c.peersMu.Lock()
			var sender *queueSender
			if peer, ok := c.peers[r.Manifest.Donor]; ok {
				sender = peer.send
			}
			c.peersMu.Unlock()
			if sender != nil {
				missing, err := c.cachedMissing(r)
				if err != nil {
					return false, err
				}
				if len(missing) == 0 {
					if err := c.requestSnapshotChunks(r.Manifest.Donor, r.Manifest.Transfer, sender); err != nil {
						return false, err
					}
				} else {
					_ = sender.TrySend(Message{Kind: MsgSnapshotResume, Transfer: r.Manifest.Transfer})
				}
			}
		}
	}
	tSnap := time.Since(started)
	sizes, err := c.stagedMap()
	if err != nil {
		return false, err
	}
	// Publish to Metrics at the source: consumers poll Metrics, and the
	// run-loop copy lags up to a tick behind the state it describes.
	c.mutateStatus(func(s *Status) {
		s.StagedTransactions = len(sizes)
		s.Metrics.StagedTransactions = len(sizes)
	})
	groups := make(map[NodeId][]string)
	for key := range sizes {
		parts := strings.Split(key, "/")
		if len(parts) < 4 {
			return false, protocolErr("staged origin")
		}
		origin, err := parseID(parts[3])
		if err != nil {
			return false, err
		}
		groups[origin] = append(groups[origin], key)
	}
	for origin := range groups {
		sort.Strings(groups[origin])
	}
	c.lastOriginMu.Lock()
	last := c.lastOrigin
	c.lastOriginMu.Unlock()
	origins := make([]NodeId, 0, len(groups))
	for origin := range groups {
		origins = append(origins, origin)
	}
	sort.Slice(origins, func(i, j int) bool { return nodeLess(origins[i], origins[j]) })
	split := sort.Search(len(origins), func(i int) bool { return !nodeLess(origins[i], last) && origins[i] != last })
	rotated := append(append([]NodeId(nil), origins[split:]...), origins[:split]...)
	type stagedRef struct {
		origin NodeId
		key    string
	}
	var refs []stagedRef
	for {
		more := false
		for _, origin := range rotated {
			if len(groups[origin]) > 0 {
				key := groups[origin][0]
				groups[origin] = groups[origin][1:]
				refs = append(refs, stagedRef{origin: origin, key: key})
				more = true
			}
		}
		if !more {
			break
		}
	}
	tParse := time.Since(started)
	var tData, tVerify time.Duration
	next := make(map[NodeId]uint64)
	blocked := make(map[NodeId]bool)
	type selectedTx struct {
		tx       Transaction
		verified *Verified
		raw      []byte
	}
	var transactions []selectedTx
	size := 0
	var purges []Record
	// Chunked batched reads: one backend round-trip per 64 staged keys
	// instead of one per key. Keys are still processed in strict
	// round-robin order with identical blocked/progress/limit logic;
	// only the reads move ahead in bounded chunks.
	progressKeys := make([][]byte, len(origins))
	for i, origin := range origins {
		progressKeys[i] = ProgressKey(origin)
	}
	progressValues, err := readMany(c.backend, progressKeys)
	if err != nil {
		return false, err
	}
	for i, origin := range origins {
		var applied uint64
		if progressValues[i] != nil {
			v, err := decodeOne(progressValues[i], func(d *postcard.Decoder) uint64 { return d.U64() })
			if err != nil {
				return false, err
			}
			applied = v
		}
		next[origin] = applied
	}
	tProg := time.Since(started)
	db, _ := c.backend.Identity()
	cursor := 0
selectLoop:
	for cursor < len(refs) {
		if len(transactions) >= limit {
			break
		}
		var idx []int
		var batch [][]byte
		j := cursor
		readBytes := 0
		planned := make(map[NodeId]uint64, len(next))
		for origin, n := range next {
			planned[origin] = n
		}
		for j < len(refs) && len(batch) < 64 {
			ref := refs[j]
			if !blocked[ref.origin] {
				sequence, serr := parseStagedSequence(ref.key)
				if serr == nil {
					expected := planned[ref.origin] + 1
					if sequence > expected {
						blocked[ref.origin] = true
						c.requestHole(ref.origin, expected, sequence-1)
						j++
						continue
					}
					if sequence == expected {
						planned[ref.origin] = sequence
					}
				}
				bytes := sizes[ref.key]
				if len(batch) > 0 && readBytes+bytes > c.config.BatchBytes {
					break
				}
				readBytes += bytes
				idx = append(idx, j)
				batch = append(batch, []byte(ref.key))
			}
			j++
		}
		cursor = j
		if len(batch) == 0 {
			break
		}
		tR := time.Now()
		found, err := readMany(c.backend, batch)
		tData += time.Since(tR)
		if err != nil {
			return false, err
		}
		for k, b := range idx {
			if len(transactions) >= limit {
				break selectLoop
			}
			ref := refs[b]
			applied, ok := next[ref.origin]
			if !ok {
				applied, err = c.readApplied(ref.origin)
				if err != nil {
					return false, err
				}
				next[ref.origin] = applied
			}
			// Fast stale path: staged keys name their zero-padded
			// sequence, so already-applied keys drop without decoding
			// or verifying them. Unparseable keys fall through to the
			// decoded path; spool-missing keys (phantoms) evict.
			stale := false
			if seq, serr := parseStagedSequence(ref.key); serr == nil && seq <= applied {
				stale = true
			}
			value := found[k]
			if value == nil {
				purges = append(purges, DeleteRecord([]byte(ref.key)))
				continue
			}
			if stale {
				purges = append(purges, DeleteRecord([]byte(ref.key)))
				continue
			}
			tx, err := DecodeTransaction(value)
			if err != nil {
				return false, err
			}
			tV := time.Now()
			verified, err := Bind(&tx, db, c.config.MaxTransactionBytes)
			tVerify += time.Since(tV)
			if err != nil {
				return false, err
			}
			if tx.Origin != ref.origin {
				applied, ok = next[tx.Origin]
				if !ok {
					applied, err = c.readApplied(tx.Origin)
					if err != nil {
						return false, err
					}
					next[tx.Origin] = applied
				}
			}
			if tx.Sequence <= applied {
				purges = append(purges, DeleteRecord([]byte(ref.key)))
				continue
			}
			if tx.Sequence != applied+1 {
				blocked[tx.Origin] = true
				c.requestHole(tx.Origin, applied+1, tx.Sequence-1)
				continue
			}
			if len(transactions) > 0 && size+len(value) > c.config.BatchBytes {
				break selectLoop
			}
			size += len(value)
			next[tx.Origin] = tx.Sequence
			transactions = append(transactions, selectedTx{tx: tx, verified: verified, raw: value})
		}
	}
	var records []Record
	// Batched stale purge: one commit for every dropped key (piggybacked
	// on the apply below when applying) plus one cache pass, instead of
	// a commit and a lock per key.
	if len(purges) > 0 {
		if len(transactions) == 0 {
			if err := c.backend.Commit(purges); err != nil {
				return false, err
			}
		} else {
			records = append(records, purges...)
		}
		c.stagedMu.Lock()
		for _, purge := range purges {
			c.staged.remove(purge.Key)
		}
		c.stagedMu.Unlock()
	}
	if len(transactions) == 0 {
		if timing && time.Since(started) >= 100*time.Millisecond {
			fmt.Fprintf(os.Stderr, "[apply-timing] n=0 staged=%d select=%dms backend=0ms gate=%dms snap=%dms parse=%dms prog=%dms data=%dms verify=%dms\n",
				len(sizes), time.Since(started).Milliseconds(), tGate.Milliseconds(),
				(tSnap - tGate).Milliseconds(), (tParse - tSnap).Milliseconds(),
				(tProg - tParse).Milliseconds(), tData.Milliseconds(), tVerify.Milliseconds())
		}
		return false, nil
	}
	headSet := make(map[NodeId]bool)
	for _, t := range transactions {
		headSet[t.tx.Origin] = true
	}
	headOrigins := make([]NodeId, 0, len(headSet))
	for origin := range headSet {
		headOrigins = append(headOrigins, origin)
	}
	sort.Slice(headOrigins, func(i, j int) bool { return nodeLess(headOrigins[i], headOrigins[j]) })
	headKeys := make([][]byte, len(headOrigins))
	for i, origin := range headOrigins {
		headKeys[i] = RetainedKey(origin)
	}
	retained, err := readMany(c.backend, headKeys)
	if err != nil {
		return false, err
	}
	heads := make(map[NodeId]*Progress)
	for i, origin := range headOrigins {
		if retained[i] != nil {
			head, err := DecodeProgress(retained[i])
			if err != nil {
				return false, err
			}
			h := head
			heads[origin] = &h
		}
	}
	nowMillis := NowMillis()
	nowBytes := encode(func(e *postcard.Encoder) { e.U64(nowMillis) })
	for _, t := range transactions {
		records = append(records, PutRecord(LogKey(t.tx.Origin, t.tx.Sequence), t.verified.Bytes))
		records = append(records, PutRecord(TimeKey(t.tx.Origin, t.tx.Sequence), nowBytes))
		head, ok := heads[t.tx.Origin]
		if !ok {
			raw, err := c.backend.Get(RetainedKey(t.tx.Origin))
			if err != nil {
				return false, err
			}
			if raw != nil {
				h, err := DecodeProgress(raw)
				if err != nil {
					return false, err
				}
				head = &h
			} else {
				head = &Progress{Origin: t.tx.Origin, Applied: t.tx.Sequence, Observed: t.tx.Sequence, RetainedFrom: t.tx.Sequence, RetainedThrough: t.tx.Sequence}
			}
			heads[t.tx.Origin] = head
		}
		head.Applied = t.tx.Sequence
		if t.tx.Sequence > head.Observed {
			head.Observed = t.tx.Sequence
		}
		head.RetainedThrough = t.tx.Sequence
		records = append(records, PutRecord(ReceiptKey(t.tx.Origin, t.tx.Sequence), append([]byte(nil), t.verified.Digest[:]...)))
		records = append(records, DeleteRecord(StageKey(t.tx.Origin, t.tx.Sequence)))
	}
	var acknowledged []Progress
	headList := make([]NodeId, 0, len(heads))
	for origin := range heads {
		headList = append(headList, origin)
	}
	sort.Slice(headList, func(i, j int) bool { return nodeLess(headList[i], headList[j]) })
	for _, origin := range headList {
		head := heads[origin]
		acknowledged = append(acknowledged, *head)
		records = append(records, PutRecord(ProgressKey(origin), encode(func(e *postcard.Encoder) { e.U64(head.Applied) })))
		records = append(records, PutRecord(RetainedKey(origin), EncodeProgress(head)))
	}
	payloads := make([][]byte, len(transactions))
	for i, t := range transactions {
		payloads[i] = t.tx.Payload
	}
	selectMs := time.Since(started).Milliseconds()
	applied, err := c.backend.Apply(payloads, records)
	if err != nil {
		return false, err
	}
	c.mutateStatus(func(s *Status) { s.Deferred = !applied })
	if timing && time.Since(started) >= 100*time.Millisecond {
		totalMs := time.Since(started).Milliseconds()
		fmt.Fprintf(os.Stderr, "[apply-timing] n=%d staged=%d select=%dms backend=%dms gate=%dms snap=%dms parse=%dms prog=%dms data=%dms verify=%dms\n",
			len(transactions), len(sizes), selectMs, totalMs-selectMs, tGate.Milliseconds(),
			(tSnap - tGate).Milliseconds(), (tParse - tSnap).Milliseconds(),
			(tProg - tParse).Milliseconds(), tData.Milliseconds(), tVerify.Milliseconds())
	}
	if applied {
		elapsed := time.Since(started)
		c.mutateStatus(func(s *Status) {
			s.Metrics.ApplyBatches++
			s.Metrics.ApplyBytes += uint64(size)
			s.Metrics.ApplyNanos += uint64(elapsed.Nanoseconds())
		})
		if c.config.AdaptiveSync {
			nextLimit := limit
			switch {
			case elapsed > 10*time.Millisecond:
				nextLimit = limit / 2
				if nextLimit < 1 {
					nextLimit = 1
				}
			case elapsed < 2*time.Millisecond:
				nextLimit = limit * 2
				if nextLimit > c.config.BatchTransactions {
					nextLimit = c.config.BatchTransactions
				}
			}
			c.adaptiveBatch.Store(int64(nextLimit))
		}
		c.stagedMu.Lock()
		for _, t := range transactions {
			c.staged.remove(StageKey(t.tx.Origin, t.tx.Sequence))
		}
		c.stagedMu.Unlock()
		c.lastOriginMu.Lock()
		c.lastOrigin = transactions[len(transactions)-1].tx.Origin
		c.lastOriginMu.Unlock()
		c.mutateStatus(func(s *Status) {
			s.AppliedTransactions += uint64(len(transactions))
			s.Metrics.AppliedTransactions = s.AppliedTransactions
		})
		c.peersMu.Lock()
		for _, peer := range c.peers {
			for a := 0; a < len(acknowledged); a += 128 {
				end := a + 128
				if end > len(acknowledged) {
					end = len(acknowledged)
				}
				page := append([]Progress(nil), acknowledged[a:end]...)
				_ = peer.send.TrySend(Message{Kind: MsgAck, Ack: page})
			}
		}
		c.peersMu.Unlock()
	}
	return applied, nil
}

func (c *engineCore) readApplied(origin NodeId) (uint64, error) {
	raw, err := c.backend.Get(ProgressKey(origin))
	if err != nil {
		return 0, err
	}
	if raw == nil {
		return 0, nil
	}
	return decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
}

// parseStagedSequence reads the zero-padded sequence suffix of a staged key.
func parseStagedSequence(key string) (uint64, error) {
	if len(key) < 20 {
		return 0, protocolErr("staged key")
	}
	suffix := key[len(key)-20:]
	for i := 0; i < len(suffix); i++ {
		if suffix[i] < '0' || suffix[i] > '9' {
			return 0, protocolErr("staged key")
		}
	}
	return strconv.ParseUint(suffix, 10, 64)
}

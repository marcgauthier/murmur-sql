package replication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net/netip"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/postcard"
)

var (
	stubMasterSeed = fill32(11)
	stubOtherSeed  = fill32(22)
	stubMaster     = idOf(1)
	stubOther      = idOf(2)
	stubDatabase   = idOf(7)
)

type stubBackend struct {
	mu      sync.Mutex
	store   map[string][]byte
	node    NodeId
	schema  []byte
	commits int
	reads   int
	clocks  [][2]uint64
	busy    atomic.Bool
}

func newStubBackend(node NodeId, schema []byte) *stubBackend {
	return &stubBackend{store: make(map[string][]byte), node: node, schema: schema}
}

func (b *stubBackend) Identity() (NodeId, NodeId) { return stubDatabase, b.node }
func (b *stubBackend) CancelPending()             {}
func (b *stubBackend) Get(key []byte) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reads++
	v, ok := b.store[string(key)]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), v...), nil
}
func (b *stubBackend) GetMany(keys [][]byte) ([][]byte, error) {
	out := make([][]byte, len(keys))
	for i, key := range keys {
		v, err := b.Get(key)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}
func (b *stubBackend) GetManyBounded(keys [][]byte, maxBytes int) ([][]byte, error) {
	var out [][]byte
	used := 0
	for _, key := range keys {
		v, err := b.Get(key)
		if err != nil {
			return nil, err
		}
		if len(out) > 0 && used+len(v) > maxBytes {
			break
		}
		used += len(v)
		out = append(out, v)
		if used >= maxBytes {
			break
		}
	}
	return out, nil
}
func (b *stubBackend) Keys(prefix []byte) ([][]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out [][]byte
	for k := range b.store {
		if bytes.HasPrefix([]byte(k), prefix) {
			out = append(out, []byte(k))
		}
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i]) < string(out[j]) })
	return out, nil
}
func (b *stubBackend) KeysPage(prefix, after []byte, limit int) ([][]byte, error) {
	keys, err := b.Keys(prefix)
	if err != nil {
		return nil, err
	}
	var out [][]byte
	for _, k := range keys {
		if after != nil && bytes.Compare(k, after) <= 0 {
			continue
		}
		out = append(out, k)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}
func (b *stubBackend) Progress() ([]Progress, error) {
	origins := make(map[NodeId]*Progress)
	retained, err := b.Keys([]byte("07/replication/retained/"))
	if err != nil {
		return nil, err
	}
	for _, key := range retained {
		raw, err := b.Get(key)
		if err != nil || raw == nil {
			continue
		}
		p, err := DecodeProgress(raw)
		if err != nil {
			return nil, err
		}
		pc := p
		origins[pc.Origin] = &pc
	}
	appliedKeys, err := b.Keys([]byte("07/replication/applied/"))
	if err != nil {
		return nil, err
	}
	for _, key := range appliedKeys {
		raw, err := b.Get(key)
		if err != nil || raw == nil {
			continue
		}
		parts := bytes.Split(key, []byte("/"))
		origin, err := parseID(string(parts[len(parts)-1]))
		if err != nil {
			return nil, err
		}
		p, ok := origins[origin]
		if !ok {
			p = &Progress{Origin: origin}
			origins[origin] = p
		}
		v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
		if err != nil {
			return nil, err
		}
		p.Applied = v
	}
	observedKeys, err := b.Keys([]byte("07/replication/observed/"))
	if err != nil {
		return nil, err
	}
	for _, key := range observedKeys {
		raw, err := b.Get(key)
		if err != nil || raw == nil {
			continue
		}
		parts := bytes.Split(key, []byte("/"))
		origin, err := parseID(string(parts[len(parts)-1]))
		if err != nil {
			return nil, err
		}
		p, ok := origins[origin]
		if !ok {
			p = &Progress{Origin: origin}
			origins[origin] = p
		}
		v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
		if err != nil {
			return nil, err
		}
		p.Observed = v
	}
	var out []Progress
	for _, p := range origins {
		if p.Applied > p.Observed {
			p.Observed = p.Applied
		}
		out = append(out, *p)
	}
	return out, nil
}
func (b *stubBackend) Commit(records []Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.commits++
	for _, r := range records {
		if r.Value == nil {
			delete(b.store, string(r.Key))
		} else {
			b.store[string(r.Key)] = append([]byte(nil), r.Value...)
		}
	}
	return nil
}
func (b *stubBackend) ImportBridge(Header, []byte) (bool, error)              { return false, nil }
func (b *stubBackend) ReleaseOwnership(string, []byte, *string) (bool, error) { return false, nil }
func (b *stubBackend) Publish(records []Record) error                         { return b.Commit(records) }
func (b *stubBackend) Snapshot() ([]byte, error)                              { return []byte{}, nil }
func (b *stubBackend) SnapshotWithProgress() ([]byte, []Progress, error) {
	return []byte{}, nil, nil
}
func (b *stubBackend) CreateSnapshot(NodeId, *Config) (Manifest, error) {
	return Manifest{}, unavailableErr("durable snapshots unsupported by backend")
}
func (b *stubBackend) AcknowledgeSnapshot(NodeId, NodeId, [32]byte) error {
	return unavailableErr("durable snapshots unsupported by backend")
}
func (b *stubBackend) RetentionCheckpoint() error                  { return nil }
func (b *stubBackend) PruneHistory(NodeId, uint64) (uint64, error) { return 0, nil }
func (b *stubBackend) ReclaimRetention() error                     { return nil }
func (b *stubBackend) Schema() ([]byte, error)                     { return append([]byte(nil), b.schema...), nil }
func (b *stubBackend) Apply(_ [][]byte, records []Record) (bool, error) {
	return true, b.Commit(records)
}
func (b *stubBackend) Observe(millis uint64, logical uint32) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.clocks = append(b.clocks, [2]uint64{millis, uint64(logical)})
	return nil
}
func (b *stubBackend) AcceptSchema([]byte) (bool, error) { return !b.busy.Swap(false), nil }

func (b *stubBackend) counts() (commits, reads int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.commits, b.reads
}

func stubTrust() map[NodeId][32]byte {
	return map[NodeId][32]byte{
		stubMaster: PublicKey(&stubMasterSeed),
		stubOther:  PublicKey(&stubOtherSeed),
	}
}

func engineTestConfig(trust map[NodeId][32]byte) *Config {
	c := NewConfig(netip.MustParseAddrPort("127.0.0.1:0"),
		Credentials{Chain: [][]byte{{1}}, PrivateKey: []byte{}, Roots: [][]byte{{2}}},
		stubOtherSeed)
	c.TrustedOrigins = trust
	return c
}

func chunkRepairCore(backend *stubBackend) *engineCore {
	ctx, cancel := context.WithCancel(context.Background())
	_ = cancel
	control := &engineControl{
		backend: backend, commands: make(chan engineCommand, 1),
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
	}
	core := newEngineCore(engineTestConfig(stubTrust()), backend, nil, nil, control)
	core.viewMu.Lock()
	core.view.Local.Node = stubOther
	core.viewMu.Unlock()
	return core
}

func signedChunkedTx(sequence uint64, payloadLen int) (Transaction, []byte) {
	tx := Transaction{
		Database: stubDatabase, Origin: stubMaster, Sequence: sequence, ID: idOf(8),
		Millis: 10, Payload: bytesRepeat(0xAB, payloadLen),
	}
	tx.Sign(&stubMasterSeed)
	return tx, EncodeTransaction(&tx)
}

func chunkMessage(tx *Transaction, data []byte, index uint32) Message {
	count := uint32((len(data) + ChunkBytes - 1) / ChunkBytes)
	start := int(index) * ChunkBytes
	end := start + ChunkBytes
	if end > len(data) {
		end = len(data)
	}
	proof := *tx
	proof.Payload = nil
	sum := sha256.Sum256(data)
	return Message{Kind: MsgTransactionChunk, Chunk: Chunk{
		Proof: proof, Origin: tx.Origin, Sequence: tx.Sequence, ID: tx.ID,
		Index: index, Count: count, Total: uint64(len(data)),
		Digest: sum, Data: append([]byte(nil), data[start:end]...),
	}}
}

func stubManifest(seed *[32]byte, signer NodeId, sequence uint64, schema []byte) SignedSchema {
	m := SignedSchema{Database: stubDatabase, Signer: signer, Sequence: sequence, Schema: schema}
	m.Sign(seed)
	return m
}

func recvTimeout(t *testing.T, r *queueReceiver, d time.Duration) *queued {
	t.Helper()
	type result struct {
		q  *queued
		ok bool
	}
	ch := make(chan result, 1)
	go func() {
		q, ok := r.Recv()
		ch <- result{q, ok}
	}()
	select {
	case res := <-ch:
		if !res.ok {
			t.Fatal("queue closed")
		}
		return res.q
	case <-time.After(d):
		t.Fatal("timed out waiting for queued message")
		return nil
	}
}

func TestPushOrderSharesRounds(t *testing.T) {
	var progress []Progress
	for i := 0; i < 10; i++ {
		progress = append(progress, Progress{Origin: idOf(byte(i)), Applied: 10_000,
			Observed: 10_000, RetainedFrom: 1, RetainedThrough: 10_000})
	}
	var cursor *NodeId
	counts := make(map[NodeId]int)
	for i := 0; i < 10; i++ {
		served := pushOrder(progress, cursor)
		if len(served) > 4 {
			served = served[:4]
		}
		for _, p := range served {
			counts[p.Origin]++
		}
		last := served[len(served)-1].Origin
		cursor = &last
	}
	if len(counts) != 10 {
		t.Fatalf("counts = %d origins", len(counts))
	}
	for origin, n := range counts {
		if n != 4 {
			t.Fatalf("origin %x served %d times", origin, n)
		}
	}
	if got := pushOrder(progress, nil)[0].Origin; got != idOf(0) {
		t.Fatalf("fresh cursor starts at %x", got)
	}
	wrap := idOf(255)
	if got := pushOrder(progress, &wrap)[0].Origin; got != idOf(0) {
		t.Fatalf("retired cursor starts at %x", got)
	}
	mid := idOf(4)
	if got := pushOrder(progress, &mid)[0].Origin; got != idOf(5) {
		t.Fatalf("cursor resumes at %x", got)
	}
	if len(pushOrder(nil, &mid)) != 0 {
		t.Fatal("empty progress yields entries")
	}
}

func TestPushFromDedupesAndRetransmits(t *testing.T) {
	now := time.Now()
	fresh := now.Add(-time.Second)
	stale := now.Add(-time.Minute)
	if got := pushFrom(100, 150, &fresh, now); got != 151 {
		t.Fatalf("streaming from = %d", got)
	}
	if got := pushFrom(100, 150, &stale, now); got != 101 {
		t.Fatalf("quiet from = %d", got)
	}
	if got := pushFrom(100, 0, nil, now); got != 101 {
		t.Fatalf("fresh from = %d", got)
	}
	if got := pushFrom(150, 150, &fresh, now); got != 151 {
		t.Fatalf("caught-up streaming from = %d", got)
	}
	if got := pushFrom(150, 150, &stale, now); got != 151 {
		t.Fatalf("caught-up quiet from = %d", got)
	}
	if got := pushFrom(160, 150, &fresh, now); got != 161 {
		t.Fatalf("peer-ahead from = %d", got)
	}
}

func TestEnvelopeVerifyAcceptsMasterRejectsForgery(t *testing.T) {
	trust := stubTrust()
	good := stubManifest(&stubMasterSeed, stubMaster, 3, []byte("schema-bytes"))
	if err := good.Verify(stubDatabase, stubMaster, trust, 64<<20); err != nil {
		t.Fatal(err)
	}
	bad := good
	bad.Schema = append(append([]byte(nil), bad.Schema...), 'x')
	if err := bad.Verify(stubDatabase, stubMaster, trust, 64<<20); err == nil {
		t.Fatal("tampered manifest verified")
	}
	if err := good.Verify(stubDatabase, stubOther, trust, 64<<20); err == nil {
		t.Fatal("wrong master verified")
	}
	if err := good.Verify(idOf(8), stubMaster, trust, 64<<20); err == nil {
		t.Fatal("wrong database verified")
	}
	if err := good.Verify(stubDatabase, stubMaster, map[NodeId][32]byte{}, 64<<20); err == nil {
		t.Fatal("unknown signer verified")
	}
	zero := good
	zero.Sequence = 0
	zero.Sign(&stubMasterSeed)
	if err := zero.Verify(stubDatabase, stubMaster, trust, 64<<20); err == nil {
		t.Fatal("zero sequence verified")
	}
	if err := good.Verify(stubDatabase, stubMaster, trust, 4); err == nil {
		t.Fatal("oversize manifest verified")
	}
}

func TestCheckAdoptsCurrentMasterDedupsReplays(t *testing.T) {
	c := engineTestConfig(stubTrust())
	c.SchemaMaster = &stubMaster
	backend := newStubBackend(stubOther, []byte("old"))
	fresh := stubManifest(&stubMasterSeed, stubMaster, 1, []byte("new"))
	decision, err := checkSchemaManifest(c, backend, &fresh)
	if err != nil || decision != schemaAdopt {
		t.Fatalf("decision = %v, %v", decision, err)
	}
	if err := recordSchemaAccepted(backend, &fresh); err != nil {
		t.Fatal(err)
	}
	decision, err = checkSchemaManifest(c, backend, &fresh)
	if err != nil || decision != schemaStale {
		t.Fatalf("replay decision = %v, %v", decision, err)
	}
	converged := newStubBackend(stubOther, []byte("new"))
	if err := recordSchemaAccepted(converged, &fresh); err != nil {
		t.Fatal(err)
	}
	newer := stubManifest(&stubMasterSeed, stubMaster, 2, []byte("new"))
	decision, err = checkSchemaManifest(c, converged, &newer)
	if err != nil || decision != schemaCurrent {
		t.Fatalf("converged decision = %v, %v", decision, err)
	}
	decision, err = checkSchemaManifest(c, converged, &newer)
	if err != nil || decision != schemaStale {
		t.Fatalf("converged replay = %v, %v", decision, err)
	}
}

func TestCheckRecordsButNeverAdoptsNonMaster(t *testing.T) {
	c := engineTestConfig(stubTrust())
	c.SchemaMaster = &stubMaster
	backend := newStubBackend(stubOther, []byte("old"))
	rogue := stubManifest(&stubOtherSeed, stubOther, 1, []byte("new"))
	decision, err := checkSchemaManifest(c, backend, &rogue)
	if err != nil || decision != schemaRecord {
		t.Fatalf("decision = %v, %v", decision, err)
	}
	raw, err := backend.Get(SchemaAcceptedKey(stubOther))
	if err != nil || raw == nil {
		t.Fatal("acceptance not recorded")
	}
	v, err := decodeOne(raw, func(d *postcard.Decoder) [2]any {
		return [2]any{d.U64(), d.Bytes()}
	})
	if err != nil {
		t.Fatal(err)
	}
	if v[0].(uint64) != 1 || string(v[1].([]byte)) != "new" {
		t.Fatalf("recorded = %v", v)
	}
	decision, err = checkSchemaManifest(c, backend, &rogue)
	if err != nil || decision != schemaStale {
		t.Fatalf("replay = %v, %v", decision, err)
	}
	masterCfg := engineTestConfig(stubTrust())
	masterCfg.Master = true
	masterCfg.SchemaMaster = &stubMaster
	masterBackend := newStubBackend(stubMaster, []byte("old"))
	m := stubManifest(&stubMasterSeed, stubMaster, 9, []byte("new"))
	decision, err = checkSchemaManifest(masterCfg, masterBackend, &m)
	if err != nil || decision != schemaRecord {
		t.Fatalf("master self decision = %v, %v", decision, err)
	}
	forged := stubManifest(&stubMasterSeed, stubMaster, 10, []byte("new"))
	forged.Schema = append(forged.Schema, 'x')
	var rerr *Error
	if _, err := checkSchemaManifest(c, backend, &forged); !errors.As(err, &rerr) || rerr.Kind != KindAuth {
		t.Fatalf("forged err = %v", err)
	}
}

func TestCheckLegacyModeAcceptsAnyTrustedOrigin(t *testing.T) {
	c := engineTestConfig(stubTrust())
	backend := newStubBackend(stubOther, []byte("old"))
	m := stubManifest(&stubOtherSeed, stubOther, 1, []byte("new"))
	decision, err := checkSchemaManifest(c, backend, &m)
	if err != nil || decision != schemaAdopt {
		t.Fatalf("decision = %v, %v", decision, err)
	}
	strangerSeed := fill32(33)
	stranger := stubManifest(&strangerSeed, idOf(3), 1, []byte("new"))
	var rerr *Error
	if _, err := checkSchemaManifest(c, backend, &stranger); !errors.As(err, &rerr) || rerr.Kind != KindAuth {
		t.Fatalf("stranger err = %v", err)
	}
}

func TestBusySchemaAdoptionReRequests(t *testing.T) {
	backend := newStubBackend(stubOther, []byte("old"))
	backend.busy.Store(true)
	core := chunkRepairCore(backend)
	send, recv := newQueue(newByteLease(1<<20), 1<<20)
	m := stubManifest(&stubOtherSeed, stubOther, 1, []byte("new"))
	if err := core.adoptManifest(stubOther, &m, send); err != nil {
		t.Fatal(err)
	}
	if q, ok := recv.TryRecv(); ok {
		t.Fatalf("sent inline: %+v", q.Message)
	}
	q := recvTimeout(t, recv, 10*time.Second)
	if q.Message.Kind != MsgSchemaRequest {
		t.Fatalf("retry = %+v", q.Message)
	}
	if err := core.adoptManifest(stubOther, &m, send); err != nil {
		t.Fatal(err)
	}
	q = recvTimeout(t, recv, 5*time.Second)
	if q.Message.Kind != MsgSchemaAck || q.Message.Sequence != 1 {
		t.Fatalf("ack = %+v", q.Message)
	}
	core.control.statusMu.Lock()
	adoptions := core.control.status.Metrics.SchemaAdoptions
	core.control.statusMu.Unlock()
	if adoptions != 1 {
		t.Fatalf("adoptions = %d", adoptions)
	}
}

func testChunkFor(t *testing.T, tx *Transaction, data []byte) Chunk {
	t.Helper()
	c := chunkMessage(tx, data, 0)
	return c.Chunk
}

func TestDuplicateFloorDiscards(t *testing.T) {
	backend := newStubBackend(stubOther, nil)
	floor := encode(func(e *postcard.Encoder) { e.U64(7) })
	if err := backend.Commit([]Record{PutRecord(FloorKey(stubMaster), floor)}); err != nil {
		t.Fatal(err)
	}
	core := chunkRepairCore(backend)
	for _, sequence := range []uint64{1, 7} {
		tx, data := signedChunkedTx(sequence, 3)
		tx.Payload = []byte{1, 2, 3}
		tx.Sign(&stubMasterSeed)
		data = EncodeTransaction(&tx)
		if err := core.stage(tx); err != nil {
			t.Fatal(err)
		}
		if err := core.stageChunk(testChunkFor(t, &tx, data)); err != nil {
			t.Fatal(err)
		}
	}
	if keys, _ := backend.Keys([]byte("07/replication/stage/")); len(keys) != 0 {
		t.Fatalf("staged = %d", len(keys))
	}
	if keys, _ := backend.Keys([]byte("07/replication/chunks/")); len(keys) != 0 {
		t.Fatalf("chunks = %d", len(keys))
	}
	core.control.statusMu.Lock()
	applied := core.control.status.AppliedTransactions
	core.control.statusMu.Unlock()
	if applied != 0 {
		t.Fatalf("applied = %d", applied)
	}
}

func TestServeChunkNeedFromLog(t *testing.T) {
	backend := newStubBackend(stubOther, nil)
	tx, data := signedChunkedTx(3, 200_000)
	if err := backend.Commit([]Record{PutRecord(LogKey(stubMaster, 3), data)}); err != nil {
		t.Fatal(err)
	}
	core := chunkRepairCore(backend)
	send, recv := newQueue(newByteLease(64<<20), 4<<20)
	if err := core.serveChunkNeed(stubMaster, 3, []uint32{0, 2}, send); err != nil {
		t.Fatal(err)
	}
	type got struct {
		index uint32
		size  int
	}
	var served []got
	for {
		q, ok := recv.TryRecv()
		if !ok {
			break
		}
		if q.Message.Kind != MsgTransactionChunk {
			t.Fatalf("kind = %v", q.Message.Kind)
		}
		served = append(served, got{q.Message.Chunk.Index, len(q.Message.Chunk.Data)})
	}
	if len(served) != 2 || served[0].index != 0 || served[1].index != 2 {
		t.Fatalf("served = %+v", served)
	}
	core.control.statusMu.Lock()
	n := core.control.status.Metrics.ChunkNeedServed
	core.control.statusMu.Unlock()
	if n != 2 {
		t.Fatalf("metric = %d", n)
	}
	count := uint32((len(data) + ChunkBytes - 1) / ChunkBytes)
	var rerr *Error
	if err := core.serveChunkNeed(stubMaster, 3, []uint32{count}, send); !errors.As(err, &rerr) || rerr.Kind != KindProtocol {
		t.Fatalf("err = %v", err)
	}
	_ = tx
}

func TestServeChunkNeedRelaysStaged(t *testing.T) {
	backend := newStubBackend(stubOther, nil)
	tx, data := signedChunkedTx(5, 200_000)
	count := uint32((len(data) + ChunkBytes - 1) / ChunkBytes)
	sum := sha256.Sum256(data)
	manifest := ChunkManifest{ID: tx.ID, Count: count, Total: uint64(len(data)), Digest: sum}
	records := []Record{PutRecord(ChunkManifestKey(stubMaster, 5), EncodeChunkManifest(&manifest))}
	for _, index := range []uint32{0, 2} {
		c := chunkMessage(&tx, data, index).Chunk
		records = append(records, PutRecord(ChunkKey(stubMaster, 5, index), EncodeChunk(&c)))
	}
	if err := backend.Commit(records); err != nil {
		t.Fatal(err)
	}
	core := chunkRepairCore(backend)
	send, recv := newQueue(newByteLease(64<<20), 4<<20)
	if err := core.serveChunkNeed(stubMaster, 5, []uint32{0, 1, 2}, send); err != nil {
		t.Fatal(err)
	}
	var got []uint32
	for {
		q, ok := recv.TryRecv()
		if !ok {
			break
		}
		got = append(got, q.Message.Chunk.Index)
	}
	if len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatalf("served = %v", got)
	}
	core.control.statusMu.Lock()
	n := core.control.status.Metrics.ChunkNeedServed
	core.control.statusMu.Unlock()
	if n != 2 {
		t.Fatalf("metric = %d", n)
	}
}

func TestServeChunkNeedMissingQuiet(t *testing.T) {
	backend := newStubBackend(stubOther, nil)
	core := chunkRepairCore(backend)
	send, recv := newQueue(newByteLease(64<<20), 4<<20)
	if err := core.serveChunkNeed(stubMaster, 9, []uint32{0}, send); err != nil {
		t.Fatal(err)
	}
	if q, ok := recv.TryRecv(); ok {
		t.Fatalf("quiet miss sent %+v", q.Message)
	}
	core.control.statusMu.Lock()
	n := core.control.status.Metrics.ChunkNeedServed
	core.control.statusMu.Unlock()
	if n != 0 {
		t.Fatalf("metric = %d", n)
	}
}

func TestServeChunkAvailabilityListsHoldings(t *testing.T) {
	backend := newStubBackend(stubOther, nil)
	tx, data := signedChunkedTx(5, 200_000)
	count := uint32((len(data) + ChunkBytes - 1) / ChunkBytes)
	sum := sha256.Sum256(data)
	manifest := ChunkManifest{ID: tx.ID, Count: count, Total: uint64(len(data)), Digest: sum}
	records := []Record{PutRecord(ChunkManifestKey(stubMaster, 5), EncodeChunkManifest(&manifest))}
	for _, index := range []uint32{0, 3} {
		c := chunkMessage(&tx, data, index).Chunk
		records = append(records, PutRecord(ChunkKey(stubMaster, 5, index), EncodeChunk(&c)))
	}
	if err := backend.Commit(records); err != nil {
		t.Fatal(err)
	}
	core := chunkRepairCore(backend)
	send, recv := newQueue(newByteLease(64<<20), 4<<20)
	if err := core.serveChunkAvailability(0, send); err != nil {
		t.Fatal(err)
	}
	q, ok := recv.TryRecv()
	if !ok || q.Message.Kind != MsgChunkAvailabilityPage {
		t.Fatalf("page = %+v", q)
	}
	if q.Message.More || len(q.Message.Availability) != 1 {
		t.Fatalf("page = %+v", q.Message)
	}
	entry := q.Message.Availability[0]
	if entry.Origin != stubMaster || entry.Sequence != 5 || len(entry.Indexes) != 2 || entry.Indexes[0] != 0 || entry.Indexes[1] != 3 {
		t.Fatalf("entry = %+v", entry)
	}
}

func TestMergeChunkAvailabilityCaps(t *testing.T) {
	backend := newStubBackend(stubOther, nil)
	core := chunkRepairCore(backend)
	if err := core.mergeChunkAvailability(stubOther, []ChunkAvailability{{Origin: stubMaster, Sequence: 5, Indexes: []uint32{0, 1}}}); err != nil {
		t.Fatal(err)
	}
	core.chunkHoldersMu.Lock()
	holder := core.chunkHolders[chunkKey{origin: stubMaster, seq: 5}]
	core.chunkHoldersMu.Unlock()
	if holder == nil || !holder.holders[stubOther][0] || !holder.holders[stubOther][1] {
		t.Fatalf("holders = %+v", holder)
	}
	tooMany := make([]ChunkAvailability, 65)
	for i := range tooMany {
		tooMany[i] = ChunkAvailability{Origin: stubMaster, Sequence: uint64(i), Indexes: []uint32{0}}
	}
	if err := core.mergeChunkAvailability(stubOther, tooMany); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("err = %v", err)
	}
	tooWide := []ChunkAvailability{{Origin: stubMaster, Sequence: 5, Indexes: make([]uint32, 4097)}}
	if err := core.mergeChunkAvailability(stubOther, tooWide); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("err = %v", err)
	}
}

func TestBulkStagingObservedHeads(t *testing.T) {
	backend := newStubBackend(stubOther, nil)
	core := chunkRepairCore(backend)
	old, _ := signedChunkedTx(1, 32)
	digest := old.Digest()
	if err := backend.Commit([]Record{PutRecord(ReceiptKey(stubMaster, 1), digest[:])}); err != nil {
		t.Fatal(err)
	}
	fresh, _ := signedChunkedTx(1, 32)
	fresh.Origin = stubOther
	fresh.Sign(&stubOtherSeed)
	if err := core.stageBatch([]Transaction{old, fresh}); err != nil {
		t.Fatal(err)
	}
	if raw, _ := backend.Get(ObservedKey(stubMaster)); raw != nil {
		t.Fatal("duplicate origin rewrote observed head")
	}
	raw, _ := backend.Get(ObservedKey(stubOther))
	if raw == nil {
		t.Fatal("fresh origin missing observed head")
	}
	v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
	if err != nil || v != 1 {
		t.Fatalf("observed = %d, %v", v, err)
	}
}

func TestOutgoingRoundReuse(t *testing.T) {
	backend := newStubBackend(stubOther, nil)
	var seqs []uint64
	for seq := uint64(1); seq <= 16; seq++ {
		seqs = append(seqs, seq)
		_, data := signedChunkedTx(seq, 256)
		if err := backend.Commit([]Record{PutRecord(LogKey(stubMaster, seq), data)}); err != nil {
			t.Fatal(err)
		}
	}
	core := chunkRepairCore(backend)
	first, err := core.loadOutgoing(stubMaster, seqs)
	if err != nil {
		t.Fatal(err)
	}
	_, reads := backend.counts()
	second, err := core.loadOutgoing(stubMaster, seqs)
	if err != nil {
		t.Fatal(err)
	}
	_, readsAfter := backend.counts()
	if readsAfter != reads {
		t.Fatalf("cached load read backend (%d -> %d)", reads, readsAfter)
	}
	for i := range first {
		if first[i] == nil || second[i] == nil || first[i] != second[i] {
			t.Fatal("cache did not reuse entries")
		}
	}
	var txs []Transaction
	var keys []chunkKey
	for _, seq := range seqs {
		txs = append(txs, *first[seq-1].tx)
		keys = append(keys, chunkKey{origin: stubMaster, seq: seq})
	}
	message := Message{Kind: MsgBatches, Transactions: txs}
	round := &outgoingRound{}
	a, err := round.frame(keys, &message)
	if err != nil {
		t.Fatal(err)
	}
	b, err := round.frame(keys, &message)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("frame not reused")
	}
	direct, err := NewPrepared(&message)
	if err != nil {
		t.Fatal(err)
	}
	if string(a.Bytes) != string(direct.Bytes) {
		t.Fatal("reused frame differs")
	}
	core.outgoingMu.Lock()
	sharedBytes := core.outgoing.bytes
	core.outgoingMu.Unlock()
	if sharedBytes > outgoingSharedBytes || round.frameBytes > outgoingCacheBytes {
		t.Fatalf("bounds exceeded: %d %d", sharedBytes, round.frameBytes)
	}
}

func TestOutgoingBoundedAllowsOneOversized(t *testing.T) {
	backend := newStubBackend(stubOther, nil)
	for seq := uint64(1); seq <= 3; seq++ {
		_, data := signedChunkedTx(seq, 4<<20)
		if err := backend.Commit([]Record{PutRecord(LogKey(stubMaster, seq), data)}); err != nil {
			t.Fatal(err)
		}
	}
	core := chunkRepairCore(backend)
	values, err := core.loadOutgoing(stubMaster, []uint64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	if values[0] == nil || values[1] != nil || values[2] != nil {
		t.Fatal("bounded prefix wrong")
	}
	core.outgoingMu.Lock()
	cached := len(core.outgoing.values)
	core.outgoingMu.Unlock()
	if cached != 0 {
		t.Fatalf("oversized transaction cached (%d)", cached)
	}
}

func TestOutgoingSharedEviction(t *testing.T) {
	shared := &outgoingShared{}
	now := time.Now()
	mk := func(seq uint64) *outgoing {
		tx, _ := signedChunkedTx(seq, 0)
		return &outgoing{data: make([]byte, 1024), tx: &tx}
	}
	var seq uint64
	for shared.bytes <= outgoingSharedBytes {
		seq++
		shared.insert(chunkKey{origin: idOf(9), seq: seq}, mk(seq), 2048, now)
		if seq > 100000 {
			break
		}
	}
	if shared.bytes > outgoingSharedBytes {
		t.Fatalf("bytes = %d", shared.bytes)
	}
	if shared.get(chunkKey{origin: idOf(9), seq: 1}, now) != nil {
		t.Fatal("oldest entry survived")
	}
	if shared.get(chunkKey{origin: idOf(9), seq: seq}, now) == nil {
		t.Fatal("newest entry evicted")
	}
	stale := now.Add(outgoingSharedTTL + time.Second)
	if shared.get(chunkKey{origin: idOf(9), seq: seq}, stale) != nil {
		t.Fatal("stale entry served")
	}
}

func TestSelectHoldersRoundRobin(t *testing.T) {
	holders := map[NodeId]map[uint32]bool{
		idOf(1): {0: true, 1: true, 2: true},
		idOf(2): {1: true, 2: true, 3: true},
	}
	directed, orphaned := selectHolders([]uint32{0, 1, 2, 3, 9}, holders)
	if len(directed[idOf(1)]) != 2 || directed[idOf(1)][0] != 0 || directed[idOf(1)][1] != 2 {
		t.Fatalf("directed[1] = %v", directed[idOf(1)])
	}
	if len(directed[idOf(2)]) != 2 || directed[idOf(2)][0] != 1 || directed[idOf(2)][1] != 3 {
		t.Fatalf("directed[2] = %v", directed[idOf(2)])
	}
	if len(orphaned) != 1 || orphaned[0] != 9 {
		t.Fatalf("orphaned = %v", orphaned)
	}
}

func TestGapDoesNotScanBacklog(t *testing.T) {
	backend := newStubBackend(stubOther, nil)
	core := chunkRepairCore(backend)
	var records []Record
	for sequence := uint64(2); sequence <= 101; sequence++ {
		_, data := signedChunkedTx(sequence, 10)
		records = append(records, PutRecord(StageKey(stubMaster, sequence), data))
	}
	healthy, _ := signedChunkedTx(1, 10)
	healthy.Origin = stubOther
	healthy.Sign(&stubOtherSeed)
	records = append(records, PutRecord(StageKey(stubOther, 1), EncodeTransaction(&healthy)))
	if err := backend.Commit(records); err != nil {
		t.Fatal(err)
	}
	if _, err := core.stagedMap(); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	backend.reads = 0
	backend.mu.Unlock()
	applied, err := core.applyPending()
	if err != nil || !applied {
		t.Fatalf("apply = %v, %v", applied, err)
	}
	_, reads := backend.counts()
	if reads >= 12 {
		t.Fatalf("gap caused %d reads", reads)
	}
	raw, _ := backend.Get(ProgressKey(stubOther))
	v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
	if err != nil || v != 1 {
		t.Fatalf("progress = %d, %v", v, err)
	}
	sizes, err := core.stagedMap()
	if err != nil || len(sizes) != 100 {
		t.Fatalf("staged = %d, %v", len(sizes), err)
	}
	applied, err = core.applyPending()
	if err != nil || applied {
		t.Fatalf("reapply = %v, %v", applied, err)
	}
}

func TestChangedDurableBytesRejected(t *testing.T) {
	backend := newStubBackend(stubOther, nil)
	core := chunkRepairCore(backend)
	tx, _ := signedChunkedTx(1, 10)
	if err := core.stage(tx); err != nil {
		t.Fatal(err)
	}
	changed := tx
	changed.Payload[0] ^= 1
	if err := backend.Commit([]Record{PutRecord(StageKey(stubMaster, 1), EncodeTransaction(&changed))}); err != nil {
		t.Fatal(err)
	}
	var rerr *Error
	if _, err := core.applyPending(); !errors.As(err, &rerr) || rerr.Kind != KindAuth {
		t.Fatalf("err = %v", err)
	}
	if raw, _ := backend.Get(ProgressKey(stubMaster)); raw != nil {
		t.Fatal("progress advanced on corrupt bytes")
	}
	restarted := chunkRepairCore(backend)
	if _, err := restarted.applyPending(); !errors.As(err, &rerr) || rerr.Kind != KindAuth {
		t.Fatalf("restart err = %v", err)
	}
	if raw, _ := backend.Get(ProgressKey(stubMaster)); raw != nil {
		t.Fatal("progress advanced on corrupt bytes after restart")
	}
}

func TestStagedSizesAccounting(t *testing.T) {
	sizes := &stagedSizes{}
	for i := uint32(0); i < 500; i++ {
		var key [4]byte
		key[0], key[1], key[2], key[3] = byte(i>>24), byte(i>>16), byte(i>>8), byte(i)
		if err := sizes.insert(key[:], int(i)+1); err != nil {
			t.Fatal(err)
		}
	}
	for i := uint32(0); i < 500; i++ {
		var key [4]byte
		key[0], key[1], key[2], key[3] = byte(i>>24), byte(i>>16), byte(i>>8), byte(i)
		if i%3 == 0 {
			sizes.remove(key[:])
		} else if err := sizes.insert(key[:], 7); err != nil {
			t.Fatal(err)
		}
		total := 0
		for _, v := range sizes.sizes {
			total += v
		}
		if sizes.bytes != total {
			t.Fatalf("bytes = %d, sum = %d", sizes.bytes, total)
		}
	}
	sizes.remove([]byte("missing"))
	overflow := &stagedSizes{}
	if err := overflow.insert([]byte{1}, int(^uint(0)>>1)); err != nil {
		t.Fatal(err)
	}
	if err := overflow.insert([]byte{2}, 1); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("err = %v", err)
	}
	if len(overflow.sizes) != 1 || overflow.bytes != int(^uint(0)>>1) {
		t.Fatal("overflow state changed")
	}
	if err := overflow.insert([]byte{1}, 10); err != nil {
		t.Fatal(err)
	}
	if overflow.bytes != 10 {
		t.Fatalf("bytes = %d", overflow.bytes)
	}
}

func TestStagingBatchCommitsOnce(t *testing.T) {
	backend := newStubBackend(stubOther, nil)
	core := chunkRepairCore(backend)
	first, _ := signedChunkedTx(1, 10)
	second, _ := signedChunkedTx(2, 10)
	second.Millis = 20
	second.Logical = 3
	second.Sign(&stubMasterSeed)
	if err := core.stageBatch([]Transaction{second, first, first}); err != nil {
		t.Fatal(err)
	}
	commits, _ := backend.counts()
	if commits != 1 {
		t.Fatalf("commits = %d", commits)
	}
	backend.mu.Lock()
	clocks := append([][2]uint64(nil), backend.clocks...)
	backend.mu.Unlock()
	if len(clocks) != 1 || clocks[0] != [2]uint64{20, 3} {
		t.Fatalf("clocks = %v", clocks)
	}
	sizes, err := core.stagedMap()
	if err != nil || len(sizes) != 2 {
		t.Fatalf("staged = %d, %v", len(sizes), err)
	}
	expectedBytes := len(EncodeTransaction(&first)) + len(EncodeTransaction(&second))
	count, total, err := core.stagedTotals()
	if err != nil || count != 2 || total != expectedBytes {
		t.Fatalf("totals = %d/%d, %v", count, total, err)
	}
	restarted := chunkRepairCore(backend)
	count, total, err = restarted.stagedTotals()
	if err != nil || count != 2 || total != expectedBytes {
		t.Fatalf("restart totals = %d/%d, %v", count, total, err)
	}
	raw, _ := backend.Get(ObservedKey(stubMaster))
	v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
	if err != nil || v != 2 {
		t.Fatalf("observed = %d, %v", v, err)
	}
	if err := core.stageBatch([]Transaction{first, second}); err != nil {
		t.Fatal(err)
	}
	commits, _ = backend.counts()
	if commits != 1 {
		t.Fatalf("commits = %d", commits)
	}
	conflict := first
	conflict.Payload[0] ^= 1
	conflict.Sign(&stubMasterSeed)
	var rerr *Error
	if err := core.stageBatch([]Transaction{first, conflict}); !errors.As(err, &rerr) || rerr.Kind != KindAuth {
		t.Fatalf("err = %v", err)
	}
	commits, _ = backend.counts()
	if commits != 1 {
		t.Fatalf("commits = %d", commits)
	}
	applied, err := core.applyPending()
	if err != nil || !applied {
		t.Fatalf("apply = %v, %v", applied, err)
	}
	raw, _ = backend.Get(ProgressKey(stubMaster))
	v, err = decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
	if err != nil || v != 2 {
		t.Fatalf("progress = %d, %v", v, err)
	}
	count, total, err = core.stagedTotals()
	if err != nil || count != 0 || total != 0 {
		t.Fatalf("totals = %d/%d, %v", count, total, err)
	}
}

func TestStagingBatchOverBudget(t *testing.T) {
	backend := newStubBackend(stubOther, nil)
	core := chunkRepairCore(backend)
	core.config.StagingTransfers = 1
	first, _ := signedChunkedTx(1, 10)
	second, _ := signedChunkedTx(2, 10)
	if err := core.stageBatch([]Transaction{first, second}); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("err = %v", err)
	}
	commits, _ := backend.counts()
	if commits != 0 {
		t.Fatalf("commits = %d", commits)
	}
	backend.mu.Lock()
	clocks := len(backend.clocks)
	backend.mu.Unlock()
	if clocks != 0 {
		t.Fatal("clock observed on rejected batch")
	}
	sizes, err := core.stagedMap()
	if err != nil || len(sizes) != 0 {
		t.Fatalf("staged = %d, %v", len(sizes), err)
	}
	core.config.StagingTransfers = 2
	core.config.StagingBytes = len(EncodeTransaction(&first))
	if err := core.stageBatch([]Transaction{first, second}); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("err = %v", err)
	}
	commits, _ = backend.counts()
	if commits != 0 {
		t.Fatalf("commits = %d", commits)
	}
	sizes, err = core.stagedMap()
	if err != nil || len(sizes) != 0 {
		t.Fatalf("staged = %d, %v", len(sizes), err)
	}
}

func TestContinuousApplyDrains(t *testing.T) {
	backend := newStubBackend(stubOther, nil)
	core := chunkRepairCore(backend)
	core.config.BatchTransactions = 1
	var records []Record
	for sequence := uint64(1); sequence <= 16; sequence++ {
		_, data := signedChunkedTx(sequence, 10)
		records = append(records, PutRecord(StageKey(stubMaster, sequence), data))
	}
	if err := backend.Commit(records); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go core.applyLoop(ctx)
	core.notifyApply()
	deadline := time.Now().Add(3 * time.Second)
	for {
		core.control.statusMu.Lock()
		applied := core.control.status.AppliedTransactions
		core.control.statusMu.Unlock()
		if applied == 16 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("applied = %d, still tick-limited", applied)
		}
		time.Sleep(time.Millisecond)
	}
	raw, _ := backend.Get(ProgressKey(stubMaster))
	v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
	if err != nil || v != 16 {
		t.Fatalf("progress = %d, %v", v, err)
	}
}

func TestLargeTxMultipleRecipients(t *testing.T) {
	backend := newStubBackend(stubOther, nil)
	_, data := signedChunkedTx(1, 2<<20)
	if err := backend.Commit([]Record{PutRecord(LogKey(stubMaster, 1), data)}); err != nil {
		t.Fatal(err)
	}
	core := chunkRepairCore(backend)
	first, a := newQueue(newByteLease(64<<20), 4<<20)
	second, b := newQueue(newByteLease(64<<20), 4<<20)
	if err := core.sendTransaction(stubMaster, 1, first); err != nil {
		t.Fatal(err)
	}
	if err := core.sendTransaction(stubMaster, 1, second); err != nil {
		t.Fatal(err)
	}
	qa := recvTimeout(t, a, 2*time.Second)
	if qa.Message.Kind != MsgTransactionChunk {
		t.Fatalf("kind = %v", qa.Message.Kind)
	}
	qb := recvTimeout(t, b, 2*time.Second)
	if qb.Message.Kind != MsgTransactionChunk {
		t.Fatalf("kind = %v", qb.Message.Kind)
	}
	a.Close()
	b.Close()
}

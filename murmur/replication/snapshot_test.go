package replication

import (
	"bytes"
	"errors"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/postcard"
)

type snapshotMemory struct {
	*testBackend
	failCommit atomic.Bool
	deferApply atomic.Bool
	applied    atomic.Int64
}

func newSnapshotMemory() *snapshotMemory {
	return &snapshotMemory{testBackend: newTestBackend()}
}

func (m *snapshotMemory) Commit(records []Record) error {
	if m.failCommit.Swap(false) {
		return storageErr("injected commit")
	}
	return m.testBackend.Commit(records)
}

func (m *snapshotMemory) Apply(payloads [][]byte, records []Record) (bool, error) {
	if m.deferApply.Load() {
		return false, nil
	}
	if len(payloads) != 1 {
		panic("expected one payload")
	}
	if err := m.Commit(records); err != nil {
		return false, err
	}
	m.applied.Add(1)
	return true, nil
}

func (m *snapshotMemory) Schema() ([]byte, error) { return []byte{5}, nil }

func snapshotConfig() *Config {
	c := NewConfig(netip.MustParseAddrPort("127.0.0.1:0"), Credentials{}, fill32(7))
	seed := fill32(7)
	c.TrustedSnapshotSources = map[NodeId]bool{idOf(3): true}
	c.TrustedOrigins = map[NodeId][32]byte{idOf(3): PublicKey(&seed)}
	c.SnapshotTransferLifetime = 10 * time.Second
	return c
}

func snapshotTransfer(t *testing.T, size int) (*Config, *Manifest, []Record) {
	t.Helper()
	c := snapshotConfig()
	m, r, err := Build([2]NodeId{idOf(1), idOf(3)}, idOf(2), bytesRepeat(8, size),
		[]Progress{{Origin: idOf(3), Applied: 7, Observed: 7, RetainedFrom: 1, RetainedThrough: 7}},
		SchemaDigest([]byte{5}), c, 100)
	if err != nil {
		t.Fatal(err)
	}
	return c, m, r
}

func snapshotParts(records []Record) [][]byte {
	var out [][]byte
	for _, r := range records {
		if bytes.Contains(r.Key, []byte("/chunk/")) {
			out = append(out, r.Value)
		}
	}
	return out
}

func TestSnapshotValidates(t *testing.T) {
	b := newSnapshotMemory()
	c, m, _ := snapshotTransfer(t, ChunkBytes+1)
	clone := func() *Manifest { cp := *m; return &cp }
	wrong := clone()
	wrong.Recipient = idOf(9)
	if err := Accept(b, c, idOf(3), wrong, 100); err == nil {
		t.Fatal("wrong recipient accepted")
	}
	wrong = clone()
	wrong.Database = idOf(9)
	if err := Accept(b, c, idOf(3), wrong, 100); err == nil {
		t.Fatal("wrong database accepted")
	}
	wrong = clone()
	wrong.Signature = append([]byte(nil), wrong.Signature...)
	wrong.Signature[0] ^= 1
	if err := Accept(b, c, idOf(3), wrong, 100); err == nil {
		t.Fatal("bad signature accepted")
	}
	wrong = clone()
	wrong.Progress = append([]Progress(nil), wrong.Progress...)
	wrong.Progress[0].Applied = 6
	if err := Accept(b, c, idOf(3), wrong, 100); err == nil {
		t.Fatal("bad progress accepted")
	}
	if err := Accept(b, c, idOf(4), clone(), 100); err == nil {
		t.Fatal("untrusted source accepted")
	}
	if err := Accept(b, c, idOf(3), clone(), m.Expires); err == nil {
		t.Fatal("expired accepted")
	}
	small := *c
	small.SnapshotStagingBytes = ChunkBytes
	if err := Accept(b, &small, idOf(3), clone(), 100); err == nil {
		t.Fatal("oversize accepted")
	}
	if err := Accept(b, c, idOf(3), clone(), 100); err != nil {
		t.Fatal(err)
	}
	original, _ := b.Get(SnapshotHeaderKey(SnapshotIn, m.Transfer))
	if err := Accept(b, c, idOf(3), clone(), 200); err != nil {
		t.Fatal(err)
	}
	again, _ := b.Get(SnapshotHeaderKey(SnapshotIn, m.Transfer))
	if !bytes.Equal(original, again) {
		t.Fatal("retransmission extended deadline")
	}
	_, other, _ := snapshotTransfer(t, 20)
	if err := Accept(b, c, idOf(3), other, 100); err == nil {
		t.Fatal("second transfer accepted")
	}
}

func TestSnapshotResume(t *testing.T) {
	b := newSnapshotMemory()
	c, m, r := snapshotTransfer(t, ChunkBytes*130+7)
	data := snapshotParts(r)
	if err := Accept(b, c, idOf(3), m, 100); err != nil {
		t.Fatal(err)
	}
	in, _ := ReadIncoming(b)
	missing, err := Missing(b, in)
	if err != nil || len(missing) != 128 {
		t.Fatalf("missing = %d, %v", len(missing), err)
	}
	for _, i := range []uint32{130, 2, 0} {
		for range 2 {
			if err := Receive(b, idOf(3), m.Transfer, i, data[i], 100); err != nil {
				t.Fatal(err)
			}
		}
	}
	if done, err := Finish(b, c, idOf(3), m.Transfer, 100); err != nil || done {
		t.Fatalf("early finish = %v, %v", done, err)
	}
	in, _ = ReadIncoming(b)
	missing, _ = Missing(b, in)
	if len(missing) != 128 {
		t.Fatalf("missing = %d", len(missing))
	}
	for i := len(data) - 1; i >= 0; i-- {
		if err := Receive(b, idOf(3), m.Transfer, uint32(i), data[i], 100); err != nil {
			t.Fatal(err)
		}
	}
	in, _ = ReadIncoming(b)
	missing, _ = Missing(b, in)
	if len(missing) != 0 {
		t.Fatalf("missing = %d", len(missing))
	}
	if done, err := Finish(b, c, idOf(3), m.Transfer, 100); err != nil || !done {
		t.Fatalf("finish = %v, %v", done, err)
	}
	if b.applied.Load() != 1 {
		t.Fatalf("applied = %d", b.applied.Load())
	}
}

func TestSnapshotRejectsCorrupt(t *testing.T) {
	b := newSnapshotMemory()
	c, m, r := snapshotTransfer(t, ChunkBytes+3)
	data := snapshotParts(r)
	if err := Accept(b, c, idOf(3), m, 100); err != nil {
		t.Fatal(err)
	}
	if err := Receive(b, idOf(3), m.Transfer, 0, data[0], 100); err != nil {
		t.Fatal(err)
	}
	for _, p := range [][]byte{nil, bytesRepeat(8, ChunkBytes-1), bytesRepeat(8, ChunkBytes+1), bytesRepeat(9, ChunkBytes)} {
		if err := Receive(b, idOf(3), m.Transfer, 0, p, 100); err == nil {
			t.Fatal("corrupt chunk accepted")
		}
	}
	if err := Receive(b, idOf(4), m.Transfer, 0, data[0], 100); err == nil {
		t.Fatal("wrong source accepted")
	}
	if err := Receive(b, idOf(3), m.Transfer, 2, bytesRepeat(8, 3), 100); err == nil {
		t.Fatal("bad index accepted")
	}
	if err := Receive(b, idOf(3), m.Transfer, 1, bytesRepeat(8, 4), 100); err == nil {
		t.Fatal("bad length accepted")
	}
	raw, _ := b.Get(SnapshotChunkKey(SnapshotIn, m.Transfer, 0))
	if !bytes.Equal(raw, data[0]) {
		t.Fatal("saved chunk clobbered")
	}
}

func TestSnapshotDeferred(t *testing.T) {
	b := newSnapshotMemory()
	c, m, r := snapshotTransfer(t, 20)
	if err := Accept(b, c, idOf(3), m, 100); err != nil {
		t.Fatal(err)
	}
	if err := Receive(b, idOf(3), m.Transfer, 0, snapshotParts(r)[0], 100); err != nil {
		t.Fatal(err)
	}
	b.deferApply.Store(true)
	if done, err := Finish(b, c, idOf(3), m.Transfer, 100); err != nil || done {
		t.Fatalf("deferred = %v, %v", done, err)
	}
	if raw, _ := b.Get(SnapshotReceiptKey(m.Transfer)); raw != nil {
		t.Fatal("receipt without apply")
	}
	b.deferApply.Store(false)
	b.failCommit.Store(true)
	if _, err := Finish(b, c, idOf(3), m.Transfer, 100); err == nil {
		t.Fatal("failed commit accepted")
	}
	if in, _ := ReadIncoming(b); in == nil {
		t.Fatal("incoming lost")
	}
	if err := b.Commit([]Record{PutRecord(ProgressKey(idOf(3)),
		encode(func(e *postcard.Encoder) { e.U64(9) }))}); err != nil {
		t.Fatal(err)
	}
	if done, err := Finish(b, c, idOf(3), m.Transfer, 100); err != nil || !done {
		t.Fatalf("finish = %v, %v", done, err)
	}
	if done, err := Finish(b, c, idOf(3), m.Transfer, 200); err != nil || !done {
		t.Fatalf("refinish = %v, %v", done, err)
	}
	if b.applied.Load() != 1 {
		t.Fatalf("applied = %d", b.applied.Load())
	}
	raw, _ := b.Get(ProgressKey(idOf(3)))
	v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
	if err != nil || v != 9 {
		t.Fatalf("progress = %d, %v", v, err)
	}
	if _, err := Finish(b, c, idOf(4), m.Transfer, 200); err == nil {
		t.Fatal("wrong source finish accepted")
	}
	if err := Maintain(b, 200); err != nil {
		t.Fatal(err)
	}
	if keys, _ := b.Keys(SnapshotIn); len(keys) != 0 {
		t.Fatalf("in keys = %d", len(keys))
	}
	if raw, _ := b.Get(SnapshotReceiptKey(m.Transfer)); raw == nil {
		t.Fatal("receipt swept early")
	}
	if err := Maintain(b, SnapshotDeadline(c, 100)); err != nil {
		t.Fatal(err)
	}
	if keys, _ := b.Keys(SnapshotReceipts); len(keys) != 0 {
		t.Fatalf("receipts = %d", len(keys))
	}
}

func TestSnapshotDigestSchema(t *testing.T) {
	b := newSnapshotMemory()
	c, m, r := snapshotTransfer(t, 20)
	m.Digest = [32]byte{}
	seed := fill32(7)
	m.Signature = Sign(&seed, m.signedBytes())
	if err := Accept(b, c, idOf(3), m, 100); err != nil {
		t.Fatal(err)
	}
	if err := Receive(b, idOf(3), m.Transfer, 0, snapshotParts(r)[0], 100); err != nil {
		t.Fatal(err)
	}
	if _, err := Finish(b, c, idOf(3), m.Transfer, 100); err == nil {
		t.Fatal("bad digest finished")
	}
	if b.applied.Load() != 0 {
		t.Fatal("bad digest applied")
	}
	b = newSnapshotMemory()
	_, m, r = snapshotTransfer(t, 20)
	m.Schema = [32]byte{}
	m.Signature = Sign(&seed, m.signedBytes())
	if err := Accept(b, c, idOf(3), m, 100); err != nil {
		t.Fatal(err)
	}
	if err := Receive(b, idOf(3), m.Transfer, 0, snapshotParts(r)[0], 100); err != nil {
		t.Fatal(err)
	}
	if _, err := Finish(b, c, idOf(3), m.Transfer, 100); err == nil {
		t.Fatal("bad schema finished")
	}
	if b.applied.Load() != 0 {
		t.Fatal("bad schema applied")
	}
}

func TestSnapshotPins(t *testing.T) {
	b := newSnapshotMemory()
	_, m, r := snapshotTransfer(t, 20)
	if err := b.Commit(r); err != nil {
		t.Fatal(err)
	}
	if v, _ := PruneLimit(b, idOf(3), 100); v != 7 {
		t.Fatalf("limit = %d", v)
	}
	if v, _ := PruneLimit(b, idOf(8), 100); v != 0 {
		t.Fatalf("unknown limit = %d", v)
	}
	if err := Retire(b, idOf(2)); err != nil {
		t.Fatal(err)
	}
	if err := Maintain(b, 200); err != nil {
		t.Fatal(err)
	}
	if raw, _ := b.Get(SnapshotPinKey(m.Transfer)); raw == nil {
		t.Fatal("pin swept before expiry")
	}
	if err := Maintain(b, m.Expires); err != nil {
		t.Fatal(err)
	}
	if v, _ := PruneLimit(b, idOf(3), m.Expires); v != ^uint64(0) {
		t.Fatalf("expired limit = %d", v)
	}
	if keys, _ := b.Keys(SnapshotOut); len(keys) != 0 {
		t.Fatalf("out keys = %d", len(keys))
	}
}

func TestSnapshotPinTarget(t *testing.T) {
	b := newSnapshotMemory()
	_, m, r := snapshotTransfer(t, 20)
	if err := b.Commit(r); err != nil {
		t.Fatal(err)
	}
	raw, _ := b.Get(SnapshotPinKey(m.Transfer))
	p, err := DecodePin(raw)
	if err != nil {
		t.Fatal(err)
	}
	p.HasTarget = true
	p.Target = map[NodeId]uint64{idOf(3): 9, idOf(8): 2}
	if err := b.Commit([]Record{PutRecord(SnapshotPinKey(m.Transfer), EncodePin(&p))}); err != nil {
		t.Fatal(err)
	}
	if err := Touch(b, idOf(2), []Progress{{Origin: idOf(3), Applied: 9, Observed: 9}}); err != nil {
		t.Fatal(err)
	}
	if err := Maintain(b, 200); err != nil {
		t.Fatal(err)
	}
	if keys, _ := b.Keys(SnapshotOut); len(keys) != 0 {
		t.Fatalf("out keys = %d", len(keys))
	}
	if v, _ := PruneLimit(b, idOf(3), 200); v != 7 {
		t.Fatalf("limit = %d", v)
	}
	if err := Touch(b, idOf(2), []Progress{{Origin: idOf(8), Applied: 2, Observed: 2}}); err != nil {
		t.Fatal(err)
	}
	if err := Maintain(b, 200); err != nil {
		t.Fatal(err)
	}
	if keys, _ := b.Keys(Pins); len(keys) != 0 {
		t.Fatalf("pins = %d", len(keys))
	}
}

func TestSnapshotExpiryBounded(t *testing.T) {
	b := newSnapshotMemory()
	c, m, r := snapshotTransfer(t, ChunkBytes*300)
	if err := Accept(b, c, idOf(3), m, 100); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(r); err != nil {
		t.Fatal(err)
	}
	if err := Maintain(b, m.Expires); err != nil {
		t.Fatal(err)
	}
	if keys, _ := b.Keys(SnapshotOut); len(keys) != 45 {
		t.Fatalf("out keys = %d", len(keys))
	}
	if err := Maintain(b, m.Expires); err != nil {
		t.Fatal(err)
	}
	if keys, _ := b.Keys(SnapshotOut); len(keys) != 0 {
		t.Fatalf("out keys = %d", len(keys))
	}
	if in, _ := ReadIncoming(b); in != nil {
		t.Fatal("incoming remains")
	}
	if err := b.Commit([]Record{PutRecord(SnapshotChunkKey(SnapshotIn, m.Transfer, 0), bytesRepeat(1, 10))}); err != nil {
		t.Fatal(err)
	}
	if err := Maintain(b, m.Expires); err != nil {
		t.Fatal(err)
	}
	if keys, _ := b.Keys(SnapshotIn); len(keys) != 0 {
		t.Fatalf("in keys = %d", len(keys))
	}
}

func TestSnapshotStreamBuild(t *testing.T) {
	c := snapshotConfig()
	pages := make([][]byte, 20)
	for i := range pages {
		pages[i] = bytesRepeat(7, ChunkBytes)
	}
	var emitted [][]Record
	next := func() ([]byte, error, bool) {
		if len(pages) == 0 {
			return nil, nil, true
		}
		p := pages[0]
		pages = pages[1:]
		return p, nil, false
	}
	manifest, err := BuildStream([2]NodeId{idOf(1), idOf(3)}, idOf(2), next, nil, SchemaDigest([]byte{5}), c, 100,
		func(records []Record) error {
			emitted = append(emitted, records)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(emitted) < 2 || len(manifest.Chunks) <= 16 {
		t.Fatalf("emitted %d batches, %d chunks", len(emitted), len(manifest.Chunks))
	}
	b := newSnapshotMemory()
	for _, batch := range emitted[:2] {
		if err := b.Commit(batch); err != nil {
			t.Fatal(err)
		}
	}
	keys, _ := b.Keys(SnapshotOut)
	if len(keys) <= 2 {
		t.Fatalf("staged keys = %d", len(keys))
	}
	if err := Maintain(b, 100+60_000); err != nil {
		t.Fatal(err)
	}
	after, _ := b.Keys(SnapshotOut)
	if len(after) != len(keys) {
		t.Fatalf("maintain swept build: %d -> %d", len(keys), len(after))
	}
	c2 := newSnapshotMemory()
	if err := c2.Commit([]Record{PutRecord(SnapshotChunkKey(SnapshotOut, idOf(8), 0), bytesRepeat(1, 10))}); err != nil {
		t.Fatal(err)
	}
	if err := Maintain(c2, 100+60_000); err != nil {
		t.Fatal(err)
	}
	if keys, _ := c2.Keys(SnapshotOut); len(keys) != 0 {
		t.Fatal("orphan kept")
	}
}

func TestSnapshotEmptyManifest(t *testing.T) {
	b := newSnapshotMemory()
	c, m, _ := snapshotTransfer(t, 0)
	if len(m.Chunks) != 0 {
		t.Fatal("empty build has chunks")
	}
	if err := Accept(b, c, idOf(3), m, 100); err == nil {
		t.Fatal("chunkless accepted")
	} else {
		var rerr *Error
		if !errors.As(err, &rerr) || rerr.Kind != KindUnavailable {
			t.Fatalf("err = %v", err)
		}
	}
	if in, _ := ReadIncoming(b); in != nil {
		t.Fatal("chunkless staged")
	}
}

func TestSnapshotCancel(t *testing.T) {
	b := newSnapshotMemory()
	_, m, r := snapshotTransfer(t, 20)
	if err := b.Commit(r); err != nil {
		t.Fatal(err)
	}
	if err := Cancel(b, idOf(9), m.Transfer); err == nil {
		t.Fatal("wrong recipient cancel accepted")
	}
	if raw, _ := b.Get(SnapshotPinKey(m.Transfer)); raw == nil {
		t.Fatal("pin lost")
	}
	if err := Cancel(b, idOf(2), m.Transfer); err != nil {
		t.Fatal(err)
	}
	if err := Maintain(b, 200); err != nil {
		t.Fatal(err)
	}
	if keys, _ := b.Keys(SnapshotOut); len(keys) != 0 {
		t.Fatalf("out = %d", len(keys))
	}
	if keys, _ := b.Keys(Pins); len(keys) != 0 {
		t.Fatalf("pins = %d", len(keys))
	}
	_, m, r = snapshotTransfer(t, 20)
	if err := b.Commit(r); err != nil {
		t.Fatal(err)
	}
	raw, _ := b.Get(SnapshotPinKey(m.Transfer))
	p, _ := DecodePin(raw)
	p.HasTarget = true
	p.Target = map[NodeId]uint64{idOf(3): 9}
	if err := b.Commit([]Record{PutRecord(SnapshotPinKey(m.Transfer), EncodePin(&p))}); err != nil {
		t.Fatal(err)
	}
	if err := Cancel(b, idOf(2), m.Transfer); err != nil {
		t.Fatal(err)
	}
	if v, _ := PruneLimit(b, idOf(3), 200); v != 7 {
		t.Fatalf("limit = %d", v)
	}
}

func TestSnapshotHistoryMissing(t *testing.T) {
	b := newSnapshotMemory()
	origin := idOf(9)
	if v, _ := historyDefinitivelyMissing(b, origin, 1); v {
		t.Fatal("unknown origin missing")
	}
	if err := b.Commit([]Record{PutRecord(ProgressKey(origin),
		encode(func(e *postcard.Encoder) { e.U64(21) }))}); err != nil {
		t.Fatal(err)
	}
	prog := Progress{Origin: origin, Applied: 21, Observed: 21, RetainedFrom: 1, RetainedThrough: 21}
	if err := b.Commit([]Record{PutRecord(RetainedKey(origin), EncodeProgress(&prog))}); err != nil {
		t.Fatal(err)
	}
	if v, _ := historyDefinitivelyMissing(b, origin, 30); v {
		t.Fatal("ahead missing")
	}
	if v, _ := historyDefinitivelyMissing(b, origin, 21); !v {
		t.Fatal("pruned not missing")
	}
	b = newSnapshotMemory()
	if err := b.Commit([]Record{PutRecord(ProgressKey(origin),
		encode(func(e *postcard.Encoder) { e.U64(62) }))}); err != nil {
		t.Fatal(err)
	}
	if v, _ := historyDefinitivelyMissing(b, origin, 40); !v {
		t.Fatal("covered not missing")
	}
	if v, _ := historyDefinitivelyMissing(b, origin, 70); v {
		t.Fatal("ahead missing")
	}
}

func targetlessPin(t *testing.T, recipient NodeId, boundary uint64) (*snapshotMemory, NodeId) {
	t.Helper()
	b := newSnapshotMemory()
	transfer := idOf(7)
	pin := &Pin{Recipient: recipient, Boundary: map[NodeId]uint64{idOf(3): boundary},
		Expires: NowMillis() + 7*86400_000}
	if err := b.Commit([]Record{PutRecord(SnapshotPinKey(transfer), EncodePin(pin))}); err != nil {
		t.Fatal(err)
	}
	return b, transfer
}

func touchApplied(t *testing.T, b Backend, peer, origin NodeId, applied uint64) {
	t.Helper()
	if err := Touch(b, peer, []Progress{{Origin: origin, Applied: applied, Observed: applied, RetainedFrom: 1, RetainedThrough: applied}}); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotReapFulfilled(t *testing.T) {
	b, transfer := targetlessPin(t, idOf(2), 21)
	touchApplied(t, b, idOf(2), idOf(3), 62)
	if err := reapForgottenPins(b, NowMillis(), 20_000, map[NodeId]bool{idOf(2): true}); err != nil {
		t.Fatal(err)
	}
	if raw, _ := b.Get(SnapshotPinKey(transfer)); raw != nil {
		t.Fatal("fulfilled pin kept")
	}
}

func TestSnapshotReapUnfulfilled(t *testing.T) {
	b, transfer := targetlessPin(t, idOf(2), 21)
	touchApplied(t, b, idOf(2), idOf(3), 10)
	if err := reapForgottenPins(b, NowMillis(), 20_000, map[NodeId]bool{idOf(2): true}); err != nil {
		t.Fatal(err)
	}
	if raw, _ := b.Get(SnapshotPinKey(transfer)); raw == nil {
		t.Fatal("unfulfilled pin reaped")
	}
}

func TestSnapshotReapNoRecord(t *testing.T) {
	b, transfer := targetlessPin(t, idOf(2), 21)
	if err := reapForgottenPins(b, NowMillis(), 20_000, map[NodeId]bool{idOf(2): true}); err != nil {
		t.Fatal(err)
	}
	if raw, _ := b.Get(SnapshotPinKey(transfer)); raw == nil {
		t.Fatal("record-less pin reaped")
	}
}

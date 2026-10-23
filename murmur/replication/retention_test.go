package replication

import (
	"bytes"
	"errors"
	"fmt"
	"net/netip"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/postcard"
)

type testBackend struct {
	mu             sync.Mutex
	records        map[string][]byte
	events         []string
	checkpointFail atomic.Bool
}

func newTestBackend() *testBackend { return &testBackend{records: make(map[string][]byte)} }

func (b *testBackend) Identity() (NodeId, NodeId) { return idOf(1), idOf(2) }

func (b *testBackend) Get(key []byte) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.records[string(key)]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), v...), nil
}

func (b *testBackend) GetMany(keys [][]byte) ([][]byte, error) {
	var out [][]byte
	for _, k := range keys {
		v, err := b.Get(k)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func (b *testBackend) GetManyBounded(keys [][]byte, maxBytes int) ([][]byte, error) {
	var out [][]byte
	total := 0
	for _, k := range keys {
		v, err := b.Get(k)
		if err != nil {
			return nil, err
		}
		if len(out) > 0 && total+len(v) > maxBytes {
			break
		}
		total += len(v)
		out = append(out, v)
		if total >= maxBytes {
			break
		}
	}
	return out, nil
}

func (b *testBackend) Keys(prefix []byte) ([][]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out [][]byte
	for k := range b.records {
		if bytes.HasPrefix([]byte(k), prefix) {
			out = append(out, []byte(k))
		}
	}
	return out, nil
}

func (b *testBackend) KeysPage(prefix, after []byte, limit int) ([][]byte, error) {
	keys, err := b.Keys(prefix)
	if err != nil {
		return nil, err
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
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

func (b *testBackend) Progress() ([]Progress, error) { panic("unreachable") }
func (b *testBackend) Commit(records []Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range records {
		if r.Value == nil {
			delete(b.records, string(r.Key))
		} else {
			b.records[string(r.Key)] = append([]byte(nil), r.Value...)
		}
	}
	return nil
}
func (b *testBackend) ImportBridge(Header, []byte) (bool, error) { panic("unreachable") }
func (b *testBackend) ReleaseOwnership(string, []byte, *string) (bool, error) {
	panic("unreachable")
}
func (b *testBackend) Publish(records []Record) error                     { return b.Commit(records) }
func (b *testBackend) Snapshot() ([]byte, error)                          { panic("unreachable") }
func (b *testBackend) SnapshotWithProgress() ([]byte, []Progress, error)  { panic("unreachable") }
func (b *testBackend) CreateSnapshot(NodeId, *Config) (Manifest, error)   { panic("unreachable") }
func (b *testBackend) AcknowledgeSnapshot(NodeId, NodeId, [32]byte) error { panic("unreachable") }
func (b *testBackend) RetentionCheckpoint() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, "snapshot")
	if b.checkpointFail.Load() {
		return storageErr("snapshot failed")
	}
	return nil
}
func (b *testBackend) PruneHistory(origin NodeId, through uint64) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, fmt.Sprintf("prune:%d:%d", origin[0], through))
	return through, nil
}
func (b *testBackend) ReclaimRetention() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, "reclaim")
	return nil
}
func (b *testBackend) Schema() ([]byte, error)                { panic("unreachable") }
func (b *testBackend) Apply([][]byte, []Record) (bool, error) { panic("unreachable") }
func (b *testBackend) Observe(uint64, uint32) error           { panic("unreachable") }
func (b *testBackend) AcceptSchema([]byte) (bool, error)      { panic("unreachable") }
func (b *testBackend) CancelPending()                         {}

const testNow = 100_000

func testConfig() *Config {
	c := NewConfig(netip.MustParseAddrPort("127.0.0.1:0"), Credentials{}, fill32(1))
	c.MinimumRetained = 2
	c.Retention = time.Second
	c.MaximumOffline = 5 * time.Second
	return c
}

func testLogs(t *testing.T, b *testBackend, count, age uint64) {
	t.Helper()
	origin := idOf(3)
	prog := Progress{Origin: origin, Observed: count, Applied: count, RetainedFrom: 1, RetainedThrough: count}
	if err := b.Commit([]Record{PutRecord(RetainedKey(origin), EncodeProgress(&prog))}); err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= count; seq++ {
		stamp := encode(func(e *postcard.Encoder) { e.U64(testNow - age) })
		err := b.Commit([]Record{
			PutRecord(LogKey(origin, seq), []byte{0xff}),
			PutRecord(TimeKey(origin, seq), stamp),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func testPutPeer(t *testing.T, b *testBackend, lastSeen, ack uint64, retired bool) {
	t.Helper()
	p := &PeerRetention{Node: idOf(4), LastSeenMillis: lastSeen, Retired: retired,
		Acknowledged: map[NodeId]uint64{idOf(3): ack}}
	if err := b.Commit([]Record{PutRecord(peerKey(p.Node), EncodePeerRetention(p))}); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionAgeAndCount(t *testing.T) {
	for _, tc := range [][3]uint64{{2, 2000, 0}, {6, 999, 0}, {6, 1000, 4}} {
		b := newTestBackend()
		testLogs(t, b, tc[0], tc[1])
		got, err := collectAt(b, testConfig(), testNow, map[NodeId]bool{}, 0)
		if err != nil || got != tc[2] {
			t.Fatalf("count=%d age=%d: got %d, %v", tc[0], tc[1], got, err)
		}
		if tc[2] > 0 {
			if !reflect.DeepEqual(b.events, []string{"snapshot", "prune:3:4", "reclaim"}) {
				t.Fatalf("events = %q", b.events)
			}
		} else if !reflect.DeepEqual(b.events, []string{"reclaim"}) {
			t.Fatalf("quiet events = %q", b.events)
		}
	}
}

func TestRetentionOfflineWindow(t *testing.T) {
	for _, tc := range []struct {
		lastSeen uint64
		ack      uint64
		retired  bool
		want     uint64
	}{
		{testNow - 5000, 0, false, 0},
		{testNow - 5000, 2, false, 2},
		{testNow - 5001, 0, false, 4},
		{testNow, 0, true, 4},
	} {
		b := newTestBackend()
		testLogs(t, b, 6, 2000)
		testPutPeer(t, b, tc.lastSeen, tc.ack, tc.retired)
		got, err := collectAt(b, testConfig(), testNow, map[NodeId]bool{}, 0)
		if err != nil || got != tc.want {
			t.Fatalf("%+v: got %d, %v", tc, got, err)
		}
	}
}

func TestRetentionConnectedPeers(t *testing.T) {
	for _, tc := range []struct {
		ack     uint64
		retired bool
		online  bool
		want    uint64
	}{
		{0, false, true, 0},
		{2, false, true, 2},
		{0, true, true, 4},
	} {
		b := newTestBackend()
		testLogs(t, b, 6, 2000)
		testPutPeer(t, b, testNow-6000, tc.ack, tc.retired)
		online := map[NodeId]bool{}
		if tc.online {
			online[idOf(4)] = true
		}
		got, err := collectAt(b, testConfig(), testNow, online, 0)
		if err != nil || got != tc.want {
			t.Fatalf("%+v: got %d, %v", tc, got, err)
		}
	}
}

func TestRetentionStartupGrace(t *testing.T) {
	b := newTestBackend()
	testLogs(t, b, 6, 2000)
	c := testConfig()
	c.Bootstrap = []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:9")}
	got, err := collectAt(b, c, testNow, map[NodeId]bool{}, testNow)
	if err != nil || got != 0 {
		t.Fatalf("young = %d, %v", got, err)
	}
	got, err = collectAt(b, c, testNow, map[NodeId]bool{}, 0)
	if err != nil || got != 4 {
		t.Fatalf("old = %d, %v", got, err)
	}
}

func TestRetentionSeedless(t *testing.T) {
	b := newTestBackend()
	testLogs(t, b, 6, 2000)
	if len(testConfig().Bootstrap) != 0 {
		t.Fatal("default config has seeds")
	}
	got, err := collectAt(b, testConfig(), testNow, map[NodeId]bool{}, testNow)
	if err != nil || got != 4 {
		t.Fatalf("got %d, %v", got, err)
	}
}

func TestRetentionUnrecordedOnline(t *testing.T) {
	b := newTestBackend()
	testLogs(t, b, 6, 2000)
	got, err := collectAt(b, testConfig(), testNow, map[NodeId]bool{idOf(9): true}, 0)
	if err != nil || got != 0 {
		t.Fatalf("got %d, %v", got, err)
	}
}

func TestRetentionBridgeFloor(t *testing.T) {
	for _, tc := range []struct {
		exported *uint64
		want     uint64
	}{
		{nil, 0},
		{uintPtr(2), 2},
		{uintPtr(6), 4},
	} {
		b := newTestBackend()
		testLogs(t, b, 6, 2000)
		c := testConfig()
		c.Bridge = NewBridgeExporter(idOf(1), "", "low", fill32(5), fill32(6))
		if tc.exported != nil {
			stamp := encode(func(e *postcard.Encoder) { e.U64(*tc.exported) })
			o3 := idOf(3)
			key := []byte("09/bridge/exported/low/" + Hex(o3[:]))
			if err := b.Commit([]Record{PutRecord(key, stamp)}); err != nil {
				t.Fatal(err)
			}
		}
		got, err := collectAt(b, c, testNow, map[NodeId]bool{}, 0)
		if err != nil || got != tc.want {
			t.Fatalf("exported=%v: got %d, %v", tc.exported, got, err)
		}
	}
}

func uintPtr(v uint64) *uint64 { return &v }

func TestRetentionFailedSnapshot(t *testing.T) {
	b := newTestBackend()
	testLogs(t, b, 6, 2000)
	b.checkpointFail.Store(true)
	if _, err := collectAt(b, testConfig(), testNow, map[NodeId]bool{}, 0); err == nil {
		t.Fatal("failure accepted")
	}
	if !reflect.DeepEqual(b.events, []string{"snapshot"}) {
		t.Fatalf("events = %q", b.events)
	}
	keys, _ := b.Keys([]byte("07/replication/log/"))
	if len(keys) != 6 {
		t.Fatalf("logs pruned: %d", len(keys))
	}
	b.checkpointFail.Store(false)
	got, err := collectAt(b, testConfig(), testNow, map[NodeId]bool{}, 0)
	if err != nil || got != 4 {
		t.Fatalf("retry = %d, %v", got, err)
	}
	if !reflect.DeepEqual(b.events, []string{"snapshot", "snapshot", "prune:3:4", "reclaim"}) {
		t.Fatalf("events = %q", b.events)
	}
}

func TestRetentionQuiet(t *testing.T) {
	b := newTestBackend()
	got, err := collectAt(b, testConfig(), testNow, map[NodeId]bool{}, testNow)
	if err != nil || got != 0 {
		t.Fatalf("empty = %d, %v", got, err)
	}
	if !reflect.DeepEqual(b.events, []string{"reclaim"}) {
		t.Fatalf("events = %q", b.events)
	}
	b = newTestBackend()
	testLogs(t, b, 6, 0)
	got, err = collectAt(b, testConfig(), testNow, map[NodeId]bool{}, 0)
	if err != nil || got != 0 {
		t.Fatalf("young = %d, %v", got, err)
	}
	for _, e := range b.events {
		if e != "reclaim" {
			t.Fatalf("events = %q", b.events)
		}
	}
}

func TestRetentionSparse(t *testing.T) {
	b := newTestBackend()
	origin := idOf(3)
	prog := Progress{Origin: origin, Observed: 62, Applied: 62, RetainedFrom: 1, RetainedThrough: 62}
	if err := b.Commit([]Record{PutRecord(RetainedKey(origin), EncodeProgress(&prog))}); err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 61; seq++ {
		if seq >= 11 && seq <= 19 {
			continue
		}
		stamp := encode(func(e *postcard.Encoder) { e.U64(testNow - 2000) })
		if err := b.Commit([]Record{
			PutRecord(LogKey(origin, seq), []byte{0xff}),
			PutRecord(TimeKey(origin, seq), stamp),
		}); err != nil {
			t.Fatal(err)
		}
	}
	testPutPeer(t, b, testNow, 62, false)
	got, err := collectAt(b, testConfig(), testNow, map[NodeId]bool{idOf(4): true}, 0)
	if err != nil || got != 60 {
		t.Fatalf("got %d, %v", got, err)
	}
	if !reflect.DeepEqual(b.events, []string{"snapshot", "prune:3:60", "reclaim"}) {
		t.Fatalf("events = %q", b.events)
	}
}

func TestRetentionMissingTime(t *testing.T) {
	b := newTestBackend()
	testLogs(t, b, 6, 2000)
	if err := b.Commit([]Record{DeleteRecord(TimeKey(idOf(3), 1))}); err != nil {
		t.Fatal(err)
	}
	got, err := collectAt(b, testConfig(), testNow, map[NodeId]bool{}, 0)
	if err != nil || got != 0 {
		t.Fatalf("blocked = %d, %v", got, err)
	}
	raw, _ := b.Get(TimeKey(idOf(3), 1))
	v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
	if err != nil || v != testNow {
		t.Fatalf("stamp = %d, %v", v, err)
	}
	got, err = collectAt(b, testConfig(), testNow+999, map[NodeId]bool{}, 0)
	if err != nil || got != 0 {
		t.Fatalf("young stamp = %d, %v", got, err)
	}
	got, err = collectAt(b, testConfig(), testNow+1000, map[NodeId]bool{}, 0)
	if err != nil || got != 4 {
		t.Fatalf("aged stamp = %d, %v", got, err)
	}
}

func TestRetentionRetire(t *testing.T) {
	b := newTestBackend()
	testPutPeer(t, b, testNow, 2, false)
	if err := Retire(b, idOf(4)); err != nil {
		t.Fatal(err)
	}
	p, err := Peer(b, idOf(4))
	if err != nil || p == nil || !p.Retired {
		t.Fatalf("peer = %+v, %v", p, err)
	}
	n4b := idOf(4)
	raw, _ := b.Get([]byte("07/replication/excluded/" + Hex(n4b[:])))
	if raw == nil {
		t.Fatal("exclusion missing")
	}
	if err := Touch(b, idOf(4), nil); err == nil {
		t.Fatal("touch of retired accepted")
	} else {
		var rerr *Error
		if !errors.As(err, &rerr) || rerr.Kind != KindAuth {
			t.Fatalf("touch err = %v", err)
		}
	}
}

func TestRetentionMigrate(t *testing.T) {
	b := newTestBackend()
	testPutPeer(t, b, testNow, 7, true)
	n4, o3 := idOf(4), idOf(3)
	key := []byte("07/replication/peer/" + Hex(n4[:]) + "/" + Hex(o3[:]))
	pair := encode(func(e *postcard.Encoder) { e.U64(2); e.U64(testNow - 1) })
	if err := b.Commit([]Record{PutRecord(key, pair)}); err != nil {
		t.Fatal(err)
	}
	if err := MigratePeers(b); err != nil {
		t.Fatal(err)
	}
	if err := MigratePeers(b); err != nil {
		t.Fatal(err)
	}
	p, err := Peer(b, idOf(4))
	if err != nil || p == nil || !p.Retired || p.Acknowledged[idOf(3)] != 7 || p.LastSeenMillis != testNow {
		t.Fatalf("peer = %+v, %v", p, err)
	}
	keys, _ := b.Keys([]byte("07/replication/peer/"))
	if len(keys) != 0 {
		t.Fatalf("legacy keys remain: %d", len(keys))
	}
}

func TestRetentionBounded(t *testing.T) {
	b := newTestBackend()
	testLogs(t, b, 600, 2000)
	got, err := collectAt(b, testConfig(), testNow, map[NodeId]bool{}, 0)
	if err != nil || got != 256 {
		t.Fatalf("got %d, %v", got, err)
	}
	found := false
	for _, e := range b.events {
		if e == "prune:3:256" {
			found = true
		}
	}
	if !found {
		t.Fatalf("events = %q", b.events)
	}
}

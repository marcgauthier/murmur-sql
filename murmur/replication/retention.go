package replication

import (
	"bytes"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/postcard"
)

// Replication history lifecycle. All authoritative state is in the backend.

var retentionPeers = []byte("07/replication/retention/peers/")

// CoverageKey marks snapshot coverage.
var CoverageKey = []byte("07/replication/retention/coverage")

// PeerRetention tracks one peer's acknowledgement watermark.
type PeerRetention struct {
	Node           NodeId
	LastSeenMillis uint64
	Retired        bool
	Acknowledged   map[NodeId]uint64
}

// RetentionStatus snapshots retention state.
type RetentionStatus struct {
	Peers                 []PeerRetention
	CollectedTransactions uint64
	LastCollectionMillis  uint64
}

// TimeKey renders a receipt-time key.
func TimeKey(origin NodeId, sequence uint64) []byte {
	return []byte("07/replication/retention/time/" + Hex(origin[:]) + "/" + zeroPad(sequence, 20))
}

// FloorKey renders a duplicate-floor key.
func FloorKey(origin NodeId) []byte {
	return []byte("07/replication/retention/floor/" + Hex(origin[:]))
}

func peerKey(node NodeId) []byte {
	return append(append([]byte(nil), retentionPeers...), Hex(node[:])...)
}

// NowMillis reports wall-clock milliseconds.
func NowMillis() uint64 {
	return uint64(time.Now().UnixMilli())
}

func durationMillis(d time.Duration) uint64 {
	ms := d.Milliseconds()
	if ms < 0 {
		return 0
	}
	return uint64(ms)
}

func zeroPad(n uint64, width int) string {
	s := strconv.FormatUint(n, 10)
	if len(s) >= width {
		return s
	}
	return strings.Repeat("0", width-len(s)) + s
}

func encodeU64(e *postcard.Encoder, v uint64) { e.U64(v) }

func encodePeerRetention(e *postcard.Encoder, p *PeerRetention) {
	e.Raw(p.Node[:])
	e.U64(p.LastSeenMillis)
	e.Bool(p.Retired)
	encodeIdMap(e, p.Acknowledged)
}

func decodePeerRetention(d *postcard.Decoder) PeerRetention {
	var p PeerRetention
	copy(p.Node[:], d.Raw(16))
	p.LastSeenMillis = d.U64()
	p.Retired = d.Bool()
	p.Acknowledged = decodeIdMap(d)
	return p
}

// EncodePeerRetention serializes peer retention.
func EncodePeerRetention(p *PeerRetention) []byte {
	return encode(func(e *postcard.Encoder) { encodePeerRetention(e, p) })
}

// DecodePeerRetention parses peer retention.
func DecodePeerRetention(b []byte) (PeerRetention, error) {
	return decodeOne(b, decodePeerRetention)
}

// Peer reads one peer's retention record.
func Peer(backend Backend, node NodeId) (*PeerRetention, error) {
	raw, err := backend.Get(peerKey(node))
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	p, err := DecodePeerRetention(raw)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// Touch records a peer sighting with fresh acknowledgements.
func Touch(backend Backend, node NodeId, progress []Progress) error {
	state, err := Peer(backend, node)
	if err != nil {
		return err
	}
	if state == nil {
		state = &PeerRetention{Node: node, Acknowledged: make(map[NodeId]uint64)}
	}
	if state.Retired {
		return authErr("retired peer")
	}
	now := NowMillis()
	changed := state.LastSeenMillis == 0
	for _, entry := range progress {
		if entry.Applied > state.Acknowledged[entry.Origin] {
			changed = true
		}
		if state.Acknowledged[entry.Origin] < entry.Applied {
			state.Acknowledged[entry.Origin] = entry.Applied
		}
	}
	if len(state.Acknowledged) > 4096 {
		return ErrOverloaded
	}
	if !changed && satSub(now, state.LastSeenMillis) < 10_000 {
		return nil
	}
	if now > state.LastSeenMillis {
		state.LastSeenMillis = now
	}
	return backend.Commit([]Record{PutRecord(peerKey(node), EncodePeerRetention(state))})
}

// Retire excludes a peer and removes its retention obligation atomically.
func Retire(backend Backend, node NodeId) error {
	state, err := Peer(backend, node)
	if err != nil {
		return err
	}
	if state == nil {
		state = &PeerRetention{Node: node, LastSeenMillis: NowMillis(), Acknowledged: make(map[NodeId]uint64)}
	}
	state.Retired = true
	nodeBytes := encode(func(e *postcard.Encoder) { e.Raw(node[:]) })
	return backend.Commit([]Record{
		PutRecord(peerKey(node), EncodePeerRetention(state)),
		PutRecord([]byte("07/replication/excluded/"+Hex(node[:])), nodeBytes),
	})
}

// Status reads retention status.
func RetentionReport(backend Backend) (*RetentionStatus, error) {
	st := &RetentionStatus{}
	keys, err := backend.Keys(retentionPeers)
	if err != nil {
		return nil, err
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	for _, key := range keys {
		raw, err := backend.Get(key)
		if err != nil {
			return nil, err
		}
		if raw == nil {
			continue
		}
		p, err := DecodePeerRetention(raw)
		if err != nil {
			return nil, err
		}
		st.Peers = append(st.Peers, p)
	}
	if raw, err := backend.Get([]byte("07/replication/retention/collected")); err != nil {
		return nil, err
	} else if raw != nil {
		v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
		if err != nil {
			return nil, err
		}
		st.CollectedTransactions = v
	}
	if raw, err := backend.Get([]byte("07/replication/retention/last")); err != nil {
		return nil, err
	} else if raw != nil {
		v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
		if err != nil {
			return nil, err
		}
		st.LastCollectionMillis = v
	}
	return st, nil
}

// startupGraceMillis pins history on young seeded nodes while peers connect.
const startupGraceMillis = 60_000

func satSub(a, b uint64) uint64 {
	if a < b {
		return 0
	}
	return a - b
}

// Collect runs one bounded retention pass: at most 256 deletions per
// origin, 2048 per pass.
func Collect(backend Backend, config *Config, online map[NodeId]bool, bootMillis uint64) (uint64, error) {
	return collectAt(backend, config, NowMillis(), online, bootMillis)
}

func collectAt(backend Backend, config *Config, now uint64, online map[NodeId]bool, bootMillis uint64) (uint64, error) {
	st, err := RetentionReport(backend)
	if err != nil {
		return 0, err
	}
	peers := st.Peers
	type plan struct {
		origin  NodeId
		through uint64
	}
	var plans []plan
	budget := 2048
	var timestamps []Record
	cursorKey := []byte("07/replication/retention/origin-cursor")
	previous, err := backend.Get(cursorKey)
	if err != nil {
		return 0, err
	}
	origins, err := backend.Keys([]byte("07/replication/retained/"))
	if err != nil {
		return 0, err
	}
	sort.Slice(origins, func(i, j int) bool { return bytes.Compare(origins[i], origins[j]) < 0 })
	if previous != nil {
		split := sort.Search(len(origins), func(i int) bool { return bytes.Compare(origins[i], previous) > 0 })
		rotated := append(append([][]byte(nil), origins[split:]...), origins[:split]...)
		origins = rotated
	}
	var cursor []byte
	for _, key := range origins {
		cursor = key
		if budget == 0 {
			break
		}
		raw, err := backend.Get(key)
		if err != nil {
			return 0, err
		}
		if raw == nil {
			continue
		}
		head, err := DecodeProgress(raw)
		if err != nil {
			return 0, err
		}
		logKeys, err := backend.KeysPage([]byte("07/replication/log/"+Hex(head.Origin[:])+"/"), nil, 256)
		if err != nil {
			return 0, err
		}
		retained := satSub(head.RetainedThrough, head.RetainedFrom)
		if retained < ^uint64(0) {
			retained++
		}
		eligible := budget
		if sub := satSub(retained, config.MinimumRetained); sub < uint64(eligible) {
			eligible = int(sub)
		}
		if len(logKeys) < eligible {
			eligible = len(logKeys)
		}
		if eligible == 0 {
			continue
		}
		recordedFloor := ^uint64(0)
		haveFloor := false
		for i := range peers {
			p := &peers[i]
			fresh := satSub(now, p.LastSeenMillis) <= durationMillis(config.MaximumOffline)
			if p.Retired || !(online[p.Node] || fresh) {
				continue
			}
			floor := p.Acknowledged[head.Origin]
			if !haveFloor || floor < recordedFloor {
				recordedFloor = floor
				haveFloor = true
			}
		}
		// A live session without a retention record yet (dial/accept race)
		// pins the floor: pruning before its first ack would strand it on
		// an untrusted snapshot.
		unrecordedOnline := false
		for id := range online {
			found := false
			for i := range peers {
				if peers[i].Node == id {
					found = true
					break
				}
			}
			if !found {
				unrecordedOnline = true
				break
			}
		}
		// Before any peer record exists, a young node with configured seeds
		// cannot distinguish "single-node" from "seeds still connecting":
		// pin the floor until the startup grace elapses, then fall back to
		// MAX. Seedless nodes prune immediately (late joiners take the
		// snapshot path by design); mid-handshake races stay covered by
		// unrecorded_online at any age.
		young := len(config.Bootstrap) > 0 && satSub(now, bootMillis) < startupGraceMillis
		var peerFloor uint64
		switch {
		case unrecordedOnline:
			peerFloor = 0
		case haveFloor:
			peerFloor = recordedFloor
		case young:
			peerFloor = 0
		default:
			peerFloor = ^uint64(0)
		}
		bridgeFloor := ^uint64(0)
		if config.Bridge != nil && config.Bridge.Role == BridgeLowExporter {
			raw, err := backend.Get([]byte("09/bridge/exported/" + config.Bridge.Stream + "/" + Hex(head.Origin[:])))
			if err != nil {
				return 0, err
			}
			bridgeFloor = 0
			if raw != nil {
				v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
				if err != nil {
					return 0, err
				}
				bridgeFloor = v
			}
		}
		snapshotFloor, err := PruneLimit(backend, head.Origin, now)
		if err != nil {
			return 0, err
		}
		var through *uint64
		prefixBlocked := false
		take := eligible
		if take > len(logKeys) {
			take = len(logKeys)
		}
		for _, lkey := range logKeys[:take] {
			// Only inspect indexed keys and the small persistence-time record.
			// Cleanup must not decode a potentially huge transaction payload.
			parts := strings.Split(string(lkey), "/")
			sequence, perr := strconv.ParseUint(parts[len(parts)-1], 10, 64)
			if perr != nil {
				return 0, protocolErr("retention log sequence")
			}
			if sequence == 0 || string(lkey) != string(LogKey(head.Origin, sequence)) {
				return 0, protocolErr("retention log identity")
			}
			traw, err := backend.Get(TimeKey(head.Origin, sequence))
			if err != nil {
				return 0, err
			}
			if traw == nil {
				prefixBlocked = true
				nowBytes := encode(func(e *postcard.Encoder) { encodeU64(e, now) })
				timestamps = append(timestamps, PutRecord(TimeKey(head.Origin, sequence), nowBytes))
				if len(timestamps) >= 256 {
					break
				}
				continue
			}
			stamp, err := decodeOne(traw, func(d *postcard.Decoder) uint64 { return d.U64() })
			if err != nil {
				return 0, err
			}
			if prefixBlocked {
				continue
			}
			aged := satSub(now, stamp) >= durationMillis(config.Retention)
			if !aged || sequence > head.Applied || sequence > peerFloor ||
				sequence > bridgeFloor || sequence > snapshotFloor {
				break
			}
			through = &sequence
		}
		if through != nil {
			// The key loop above counts indexed keys, but out-of-order
			// receive leaves sparse logs: the eligible-th present key can
			// sit at or past the tail, leaving less than the minimum and
			// tripping the prune_history guard. Cap explicitly so the plan
			// always keeps the minimum tail.
			capped := *through
			if sub := satSub(head.RetainedThrough, config.MinimumRetained); sub < capped {
				capped = sub
			}
			if capped >= head.RetainedFrom {
				plans = append(plans, plan{head.Origin, capped})
			}
			budget -= eligible
		}
		if len(timestamps) >= 256 {
			break
		}
	}
	if len(timestamps) > 0 {
		if err := backend.Commit(timestamps); err != nil {
			return 0, err
		}
	}
	if cursor != nil {
		if err := backend.Commit([]Record{PutRecord(cursorKey, cursor)}); err != nil {
			return 0, err
		}
	}
	var count uint64
	if len(plans) > 0 {
		// Backend must snapshot durable logical state and its applied progress
		// on the serialized storage worker before allowing any deletion.
		if err := backend.RetentionCheckpoint(); err != nil {
			return 0, err
		}
		for _, p := range plans {
			n, err := backend.PruneHistory(p.origin, p.through)
			if err != nil {
				return 0, err
			}
			if count += n; count < n {
				count = ^uint64(0)
			}
		}
	}
	nowBytes := encode(func(e *postcard.Encoder) { encodeU64(e, now) })
	if err := backend.Commit([]Record{PutRecord([]byte("07/replication/retention/last"), nowBytes)}); err != nil {
		return 0, err
	}
	// Reclaim after every pass, including quiet ones: deleting a sealed
	// file can unblock tombstone drops (and further file deletions)
	// whose older values lived in that file, so a cross-file delete
	// cascade needs reclaims with no fresh prunes to drain. Skipping
	// quiet passes strands that debt forever once pruning completes.
	// Backends rate-limit physical reclaims; see reclaim_retention.
	if err := backend.ReclaimRetention(); err != nil {
		return 0, err
	}
	return count, nil
}

// MigratePeers upgrades acknowledgements recorded by the previous
// replication worker. Do not forget offline peers when retention is
// first enabled on an existing store.
func MigratePeers(backend Backend) error {
	keys, err := backend.Keys([]byte("07/replication/peer/"))
	if err != nil {
		return err
	}
	peers := make(map[NodeId]*PeerRetention)
	for _, key := range keys {
		parts := strings.Split(string(key), "/")
		if len(parts) != 5 {
			return protocolErr("legacy peer key")
		}
		node, err := parseNode(parts[3])
		if err != nil {
			return err
		}
		origin, err := parseNode(parts[4])
		if err != nil {
			return err
		}
		raw, err := backend.Get(key)
		if err != nil {
			return err
		}
		if raw == nil {
			continue
		}
		var applied, seen uint64
		_, err = decodeOne(raw, func(d *postcard.Decoder) uint64 {
			applied = d.U64()
			seen = d.U64()
			return 0
		})
		if err != nil {
			return err
		}
		state, ok := peers[node]
		if !ok {
			existing, err := Peer(backend, node)
			if err != nil {
				return err
			}
			if existing == nil {
				existing = &PeerRetention{Node: node, LastSeenMillis: seen, Acknowledged: make(map[NodeId]uint64)}
			}
			state = existing
			peers[node] = state
		}
		if seen > state.LastSeenMillis {
			state.LastSeenMillis = seen
		}
		if applied > state.Acknowledged[origin] {
			state.Acknowledged[origin] = applied
		}
	}
	for _, state := range peers {
		if err := backend.Commit([]Record{PutRecord(peerKey(state.Node), EncodePeerRetention(state))}); err != nil {
			return err
		}
	}
	// New records are durable before the old representation is removed.
	for i := 0; i < len(keys); i += 128 {
		end := i + 128
		if end > len(keys) {
			end = len(keys)
		}
		var batch []Record
		for _, k := range keys[i:end] {
			batch = append(batch, DeleteRecord(k))
		}
		if err := backend.Commit(batch); err != nil {
			return err
		}
	}
	return nil
}

func parseNode(name string) (NodeId, error) {
	var zero NodeId
	if len(name) != 32 {
		return zero, protocolErr("node id")
	}
	var node NodeId
	for i := range node {
		v, err := strconv.ParseUint(name[i*2:i*2+2], 16, 8)
		if err != nil {
			return zero, protocolErr("node id")
		}
		node[i] = byte(v)
	}
	return node, nil
}

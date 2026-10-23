package murmur

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/postcard"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

func storageNetErr(err error) *replication.Error {
	return &replication.Error{Kind: replication.KindStorage, Msg: err.Error()}
}

func storageNetErr2(msg string) *replication.Error {
	return &replication.Error{Kind: replication.KindStorage, Msg: msg}
}

func unavailableNetErr(msg string) *replication.Error {
	return &replication.Error{Kind: replication.KindUnavailable, Msg: msg}
}

// Adapter is the Spool-backed replication backend. Network workers never
// own a SQLite connection.
type Adapter struct {
	mergeDeferrals      atomic.Int64
	cancelled           atomic.Bool
	inner               *Inner
	worker              chan<- Work
	identity            [2][16]byte
	lastReclaimMs       atomic.Uint64
	lastApplyBarrierMs  atomic.Uint64
	batchesSinceBarrier atomic.Uint64
	progressMu          sync.Mutex
	progressCache       *progressEntry
}

type progressEntry struct {
	at    time.Time
	heads []replication.Progress
}

// Minimum spacing between physical reclaims.
const reclaimMinIntervalMillis = 60000

var workerReadsForced = sync.OnceValue(func() bool {
	_, ok := os.LookupEnv("MURMUR_WORKER_READS")
	return ok
})

// directStore bypasses the storage worker queue for reads. Commits stay
// on the worker; stale or torn reads fall back to the serialized worker.
func (a *Adapter) directStore() *LogicalStore {
	if workerReadsForced() {
		return nil
	}
	a.inner.status.Mu.Lock()
	closed := a.inner.status.Status.Closed
	a.inner.status.Mu.Unlock()
	if closed {
		return nil
	}
	return NewLogicalStore(a.inner.store)
}

// Call runs f on the writer thread, aborting on cancel.
func (a *Adapter) Call(f func(*Core) (any, error)) (any, error) {
	database, err := a.Database()
	if err != nil {
		return nil, storageErr("%v", err)
	}
	return database.CallCancel(f, &a.cancelled)
}

// Database returns a handle, failing when closed.
func (a *Adapter) Database() (*Database, error) {
	a.inner.status.Mu.Lock()
	closed := a.inner.status.Status.Closed
	a.inner.status.Mu.Unlock()
	if closed {
		return nil, unavailableNetErr("database closed")
	}
	return &Database{inner: a.inner}, nil
}

// Spool runs f on the storage worker.
func (a *Adapter) Spool(f func(*Opened) (any, error)) (any, error) {
	reply := make(chan spoolResult, 1)
	work := Work{Kind: WorkNetwork, Network: func(o *Opened) {
		v, err := f(o)
		reply <- spoolResult{v: v, err: err}
	}}
	select {
	case a.worker <- work:
	case <-time.After(5 * time.Minute):
		return nil, unavailableNetErr("storage worker stopped")
	}
	result := <-reply
	if result.err != nil {
		// Fail-closed bridge rejections keep their identity so the
		// importer can surface them instead of holding the bundle.
		if errors.Is(result.err, replication.ErrBridgeIdentityCollision) {
			return nil, result.err
		}
		return nil, storageNetErr(result.err)
	}
	return result.v, nil
}

type spoolResult struct {
	v   any
	err error
}

func isClosedOrCorrupt(err error) bool {
	var merr *Error
	if errors.As(err, &merr) {
		return merr.Kind == KindClosed || merr.Kind == KindCorrupt
	}
	return false
}

// CancelPending interrupts pending admission waits.
func (a *Adapter) CancelPending() { a.cancelled.Store(true) }

// Identity returns the database/node identity.
func (a *Adapter) Identity() ([16]byte, [16]byte) { return a.identity[0], a.identity[1] }

// Get reads one key.
func (a *Adapter) Get(key []byte) ([]byte, error) {
	if store := a.directStore(); store != nil {
		value, err := store.Get(key)
		if err == nil {
			return value, nil
		}
		if !isClosedOrCorrupt(err) {
			return nil, storageNetErr(err)
		}
	}
	keyCopy := append([]byte(nil), key...)
	v, err := a.Spool(func(o *Opened) (any, error) { return o.Store.Get(keyCopy) })
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

// GetMany reads keys in order.
func (a *Adapter) GetMany(keys [][]byte) ([][]byte, error) {
	if store := a.directStore(); store != nil {
		out := make([][]byte, 0, len(keys))
		fallback := false
		for _, key := range keys {
			value, err := store.Get(key)
			if err != nil {
				if isClosedOrCorrupt(err) {
					fallback = true
					break
				}
				return nil, storageNetErr(err)
			}
			out = append(out, value)
		}
		if !fallback {
			return out, nil
		}
	}
	v, err := a.Spool(func(o *Opened) (any, error) {
		out := make([][]byte, 0, len(keys))
		for _, key := range keys {
			value, err := o.Store.Get(key)
			if err != nil {
				return nil, err
			}
			out = append(out, value)
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([][]byte), nil
}

// GetManyBounded reads a byte-bounded prefix of keys.
func (a *Adapter) GetManyBounded(keys [][]byte, maxBytes int) ([][]byte, error) {
	read := func(store *LogicalStore) ([][]byte, bool, error) {
		var out [][]byte
		used := 0
		for _, key := range keys {
			value, err := store.Get(key)
			if err != nil {
				if isClosedOrCorrupt(err) {
					return nil, true, nil
				}
				return nil, false, storageNetErr(err)
			}
			if len(out) > 0 && used+len(value) > maxBytes {
				break
			}
			used += len(value)
			out = append(out, value)
			if used >= maxBytes {
				break
			}
		}
		return out, false, nil
	}
	if store := a.directStore(); store != nil {
		out, fallback, err := read(store)
		if err != nil || !fallback {
			return out, err
		}
	}
	v, err := a.Spool(func(o *Opened) (any, error) {
		out, _, err := read(o.Store)
		return out, err
	})
	if err != nil {
		return nil, err
	}
	return v.([][]byte), nil
}

// Keys lists keys with a prefix.
func (a *Adapter) Keys(prefix []byte) ([][]byte, error) {
	v, err := a.Spool(func(o *Opened) (any, error) { return o.Store.KeysWithPrefix(prefix), nil })
	if err != nil {
		return nil, err
	}
	return v.([][]byte), nil
}

// KeysPage lists a page of keys.
func (a *Adapter) KeysPage(prefix, after []byte, limit int) ([][]byte, error) {
	v, err := a.Spool(func(o *Opened) (any, error) { return o.Store.KeysPage(prefix, after, limit), nil })
	if err != nil {
		return nil, err
	}
	return v.([][]byte), nil
}

// Progress returns memoized replication heads.
func (a *Adapter) Progress() ([]replication.Progress, error) {
	a.progressMu.Lock()
	cached := a.progressCache
	a.progressMu.Unlock()
	if cached != nil && time.Since(cached.at) < time.Second {
		// The cache is shared across engine goroutines; callers
		// sort the result, so hand out a private copy.
		return append([]replication.Progress(nil), cached.heads...), nil
	}
	v, err := a.Spool(func(o *Opened) (any, error) {
		origins := make(map[[16]byte]*replication.Progress)
		for _, key := range o.Store.KeysWithPrefix([]byte("07/replication/retained/")) {
			raw, err := o.Store.Get(key)
			if err != nil {
				return nil, err
			}
			if raw == nil {
				continue
			}
			head, err := replication.DecodeProgress(raw)
			if err != nil {
				return nil, corruptErr("%v", err)
			}
			headCopy := head
			origins[head.Origin] = &headCopy
		}
		for _, probe := range []struct {
			prefix   []byte
			observed bool
		}{{[]byte("07/replication/applied/"), false}, {[]byte("07/replication/observed/"), true}} {
			for _, key := range o.Store.KeysWithPrefix(probe.prefix) {
				raw, err := o.Store.Get(key)
				if err != nil {
					return nil, err
				}
				if raw == nil {
					continue
				}
				origin, err := parseProgressOrigin(key)
				if err != nil {
					return nil, err
				}
				head, ok := origins[origin]
				if !ok {
					head = &replication.Progress{Origin: origin}
					origins[origin] = head
				}
				v, err := DecodeVersion(raw)
				_ = v
				value, verr := decodeU64(raw)
				if verr != nil {
					return nil, verr
				}
				if probe.observed {
					head.Observed = value
				} else {
					head.Applied = value
				}
			}
		}
		var heads []replication.Progress
		for _, head := range origins {
			if head.Applied > head.Observed {
				head.Observed = head.Applied
			}
			heads = append(heads, *head)
		}
		return heads, nil
	})
	if err != nil {
		return nil, err
	}
	heads := v.([]replication.Progress)
	a.progressMu.Lock()
	a.progressCache = &progressEntry{at: time.Now(), heads: heads}
	a.progressMu.Unlock()
	return append([]replication.Progress(nil), heads...), nil
}

// Commit writes records durably.
func (a *Adapter) Commit(records []replication.Record) error {
	_, err := a.Spool(func(o *Opened) (any, error) {
		mutations := make([]spool.Mutation, len(records))
		for i, r := range records {
			if r.Value == nil {
				mutations[i] = spool.Mutation{Key: r.Key, Deleted: true}
			} else {
				mutations[i] = spool.Mutation{Key: r.Key, Value: r.Value}
			}
		}
		return nil, o.Store.Commit(mutations, spool.DurabilitySync)
	})
	return err
}

// ImportBridge ingests a sealed bridge bundle.
func (a *Adapter) ImportBridge(header replication.Header, payload []byte) (bool, error) {
	return HighLowImport(a, header, payload)
}

// ReleaseOwnership drops High ownership.
func (a *Adapter) ReleaseOwnership(table string, key []byte, column *string) (bool, error) {
	return HighLowRelease(a, table, key, column)
}

// Publish signs and stores metadata-only content.
func (a *Adapter) Publish(records []replication.Record) error {
	_, err := a.Spool(func(o *Opened) (any, error) {
		if o.ReplicationConfig == nil {
			return nil, invalidErr("replication disabled")
		}
		config := o.ReplicationConfig
		if err := validateAuxiliary(records); err != nil {
			return nil, err
		}
		var changes []spool.Mutation
		for _, r := range records {
			if r.Value == nil {
				changes = append(changes, spool.Mutation{Key: r.Key, Deleted: true})
			} else {
				changes = append(changes, spool.Mutation{Key: r.Key, Value: r.Value})
			}
		}
		clock, err := o.Clock.Next(o.Identity.Node)
		if err != nil {
			return nil, err
		}
		signed, err := signContent(o, config, &replication.Content{Records: records, Origin: o.Identity.Node}, clock, &o.Schema)
		if err != nil {
			return nil, err
		}
		changes = append(changes, signed...)
		changes = append(changes, spool.Mutation{Key: []byte("06/clock"), Value: EncodeVersion(clock)})
		if err := o.Store.Commit(changes, spool.DurabilitySync); err != nil {
			return nil, err
		}
		o.Clock = clock
		return nil, nil
	})
	return err
}

// Snapshot renders the filtered state snapshot.
func (a *Adapter) Snapshot() ([]byte, error) {
	v, err := a.Spool(func(o *Opened) (any, error) { return snapshotContent(o) })
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

// SnapshotWithProgress renders snapshot plus durable progress.
func (a *Adapter) SnapshotWithProgress() ([]byte, []replication.Progress, error) {
	v, err := a.Spool(func(o *Opened) (any, error) {
		content, err := snapshotContent(o)
		if err != nil {
			return nil, err
		}
		progress, err := durableProgress(o)
		if err != nil {
			return nil, err
		}
		return &snapshotAndProgress{content: content, progress: progress}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	out := v.(*snapshotAndProgress)
	return out.content, out.progress, nil
}

type snapshotAndProgress struct {
	content  []byte
	progress []replication.Progress
}

// CreateSnapshot builds or reuses a streaming snapshot.
func (a *Adapter) CreateSnapshot(recipient [16]byte, config *replication.Config) (replication.Manifest, error) {
	var zero replication.Manifest
	streamConfig := *config
	v, err := a.Spool(func(o *Opened) (any, error) {
		now := replication.NowMillis()
		schema, err := shaSchema(&o.Schema)
		if err != nil {
			return nil, err
		}
		for _, key := range o.Store.KeysWithPrefix(replication.SnapshotOut) {
			if !bytes.HasSuffix(key, []byte("/header")) {
				continue
			}
			raw, err := o.Store.Get(key)
			if err != nil {
				return nil, err
			}
			if raw == nil {
				continue
			}
			m, err := replication.DecodeManifest(raw)
			if err != nil {
				return nil, corruptErr("%v", err)
			}
			pinRaw, err := o.Store.Get(replication.SnapshotPinKey(m.Transfer))
			if err != nil {
				return nil, err
			}
			var pin *replication.Pin
			if pinRaw != nil {
				p, err := replication.DecodePin(pinRaw)
				if err != nil {
					return nil, corruptErr("%v", err)
				}
				pin = &p
			}
			if m.Recipient == recipient && m.Expires > now && m.Schema == schema && pin != nil && !pin.HasTarget {
				if len(m.Chunks) == 0 {
					if m.Expires-now > replication.BuildFreshMarginMillis {
						return nil, ErrBusy
					}
					continue
				}
				return &snapshotCaptured{manifest: &m}, nil
			}
		}
		if len(o.Store.KeysWithPrefix(replication.Pins)) >= config.StagingTransfers {
			return nil, limitErr("snapshot transfer count")
		}
		used := 0
		for _, key := range o.Store.KeysWithPrefix(replication.SnapshotOut) {
			if !bytes.HasSuffix(key, []byte("/header")) {
				used++
			}
		}
		usedBytes := used * replication.ChunkBytes
		if usedBytes/replication.ChunkBytes != used {
			return nil, limitErr("snapshot staging bytes")
		}
		budget := config.SnapshotStagingBytes - usedBytes
		if config.SnapshotStagingBytes < usedBytes {
			budget = 0
		}
		if budget == 0 {
			return nil, limitErr("snapshot staging bytes")
		}
		parent := filepath.Dir(string(o.Opts.Path))
		var rnd [16]byte
		if _, err := rand.Read(rnd[:]); err != nil {
			return nil, ioErr(err)
		}
		path := filepath.Join(parent, ".murmur-snapshot-"+replication.Hex(rnd[:]))
		checkpoint, err := o.Store.Checkpoint(path)
		if err != nil {
			return nil, err
		}
		options := o.Opts
		options.Path = path
		options.ReclaimInterval = 0
		// The source outlives this closure on engine goroutines; it
		// must own its key copy so Open/Close hygiene never touches
		// the live store key.
		options.MasterKey = append([]byte(nil), o.Opts.MasterKey...)
		progress, err := durableProgress(o)
		if err != nil {
			return nil, err
		}
		return &snapshotCaptured{source: &SnapshotSource{
			Checkpoint: checkpoint, Options: options, Schema: o.Schema,
			Clock: o.Clock, Identity: [2][16]byte{o.Identity.Database, o.Identity.Node},
			Progress: progress, Budget: budget,
		}}, nil
	})
	if err != nil {
		return zero, err
	}
	captured := v.(*snapshotCaptured)
	if captured.manifest != nil {
		return *captured.manifest, nil
	}
	pages, err := OpenSnapshotPages(captured.source)
	if err != nil {
		return zero, storageNetErr(err)
	}
	defer pages.Close()
	defer captured.source.Close()
	identity := pages.Source().Identity
	schema, err := shaSchema(&pages.Source().Schema)
	if err != nil {
		return zero, storageNetErr(err)
	}
	streamConfig.SnapshotStagingBytes = min(streamConfig.SnapshotStagingBytes, pages.Source().Budget)
	next := func() (page []byte, err error, done bool) {
		data, finished, err := pages.Next()
		if err != nil {
			return nil, err, false
		}
		if finished {
			return nil, nil, true
		}
		return data, nil, false
	}
	manifest, err := replication.BuildStream(identity, recipient, next, pages.Source().Progress, schema, &streamConfig, replication.NowMillis(), func(records []replication.Record) error {
		return a.Commit(records)
	})
	if err != nil {
		return zero, storageNetErr(err)
	}
	return *manifest, nil
}

type snapshotCaptured struct {
	manifest *replication.Manifest
	source   *SnapshotSource
}

// AcknowledgeSnapshot records a recipient acknowledgement.
func (a *Adapter) AcknowledgeSnapshot(recipient, transfer [16]byte, digest [32]byte) error {
	_, err := a.Spool(func(o *Opened) (any, error) {
		key := replication.SnapshotPinKey(transfer)
		raw, err := o.Store.Get(key)
		if err != nil {
			return nil, err
		}
		if raw == nil {
			return nil, nil
		}
		pin, err := replication.DecodePin(raw)
		if err != nil {
			return nil, corruptErr("%v", err)
		}
		if pin.Recipient != recipient || pin.Digest != digest {
			return nil, invalidErr("snapshot acknowledgement identity")
		}
		if pin.Expires <= replication.NowMillis() {
			return nil, invalidErr("snapshot acknowledgement expired")
		}
		if !pin.HasTarget {
			progress, err := durableProgress(o)
			if err != nil {
				return nil, err
			}
			pin.Target = make(map[[16]byte]uint64)
			for _, p := range progress {
				pin.Target[p.Origin] = p.Applied
			}
			pin.HasTarget = true
			if err := o.Store.Commit([]spool.Mutation{{Key: key, Value: replication.EncodePin(&pin)}}, spool.DurabilitySync); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	return err
}

// RetentionCheckpoint snapshots storage and records coverage.
func (a *Adapter) RetentionCheckpoint() error {
	_, err := a.Spool(func(o *Opened) (any, error) {
		if err := SnapshotStore(o); err != nil {
			return nil, err
		}
		progress, err := durableProgress(o)
		if err != nil {
			return nil, err
		}
		coverage := make(map[[16]byte]uint64)
		for _, p := range progress {
			coverage[p.Origin] = p.Applied
		}
		encoded := encodeOne(func(e *postcard.Encoder) {
			e.U64(o.Sequence)
			keys := make([][16]byte, 0, len(coverage))
			for k := range coverage {
				keys = append(keys, k)
			}
			sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
			e.Len(len(keys))
			for _, k := range keys {
				e.Raw(k[:])
				e.U64(coverage[k])
			}
		})
		return nil, o.Store.Commit([]spool.Mutation{{Key: replication.CoverageKey, Value: encoded}}, spool.DurabilitySync)
	})
	return err
}

// PruneHistory prunes covered origin history through a sequence.
func (a *Adapter) PruneHistory(origin [16]byte, through uint64) (uint64, error) {
	database, err := a.Database()
	if err != nil {
		return 0, err
	}
	ticket, err := database.inner.scheduler.AdmitTimeout(WriterMaintenance, 100*time.Millisecond)
	if err != nil {
		return 0, storageNetErr(err)
	}
	defer ticket.Release()
	v, err := a.Spool(func(o *Opened) (any, error) {
		old := uint64(0)
		if raw, err := o.Store.Get(replication.FloorKey(origin)); err != nil {
			return nil, err
		} else if raw != nil {
			v, err := decodeU64(raw)
			if err != nil {
				return nil, err
			}
			old = v
		}
		if through <= old {
			return uint64(0), nil
		}
		for _, key := range o.Store.KeysWithPrefix(replication.Pins) {
			raw, err := o.Store.Get(key)
			if err != nil {
				return nil, err
			}
			if raw == nil {
				continue
			}
			pin, err := replication.DecodePin(raw)
			if err != nil {
				return nil, corruptErr("%v", err)
			}
			if pin.Expires > replication.NowMillis() && through > pin.Boundary[origin] {
				return nil, ErrBusy
			}
		}
		coverageRaw, err := o.Store.Get(replication.CoverageKey)
		if err != nil {
			return nil, err
		}
		if coverageRaw == nil {
			return nil, invalidErr("retention snapshot missing")
		}
		sequence, coverage, err := decodeCoverage(coverageRaw)
		if err != nil {
			return nil, err
		}
		if sequence > o.Sequence || coverage[origin] < through {
			return nil, invalidErr("history not covered by durable snapshot")
		}
		headRaw, err := o.Store.Get(replication.RetainedKey(origin))
		if err != nil {
			return nil, err
		}
		if headRaw == nil {
			return nil, corruptErr("retention head missing")
		}
		head, err := replication.DecodeProgress(headRaw)
		if err != nil {
			return nil, corruptErr("%v", err)
		}
		applied := uint64(0)
		if raw, err := o.Store.Get(replication.ProgressKey(origin)); err != nil {
			return nil, err
		} else if raw != nil {
			v, err := decodeU64(raw)
			if err != nil {
				return nil, err
			}
			applied = v
		}
		if through > applied || through >= head.RetainedThrough {
			return nil, invalidErr("retention would remove current history")
		}
		keys := o.Store.KeysPage([]byte("07/replication/log/"+replication.Hex(origin[:])+"/"), nil, 257)
		remaining := head.RetainedThrough - through
		if o.ReplicationConfig == nil {
			return nil, invalidErr("replication disabled")
		}
		if remaining < o.ReplicationConfig.MinimumRetained {
			return nil, invalidErr("retention minimum would be violated")
		}
		var changes []spool.Mutation
		var count uint64
		for _, key := range keys {
			seq, err := keySequence(key)
			if err != nil {
				return nil, err
			}
			if seq > through {
				break
			}
			if count >= 256 {
				return nil, limitErr("retention deletion batch")
			}
			changes = append(changes, spool.Mutation{Key: key, Deleted: true})
			changes = append(changes, spool.Mutation{Key: replication.TimeKey(origin, seq), Deleted: true})
			count++
		}
		receipts := o.Store.KeysPage([]byte("07/replication/receipt/"+replication.Hex(origin[:])+"/"), nil, 257)
		receiptCount := 0
		for _, key := range receipts {
			seq, err := keySequence(key)
			if err != nil {
				return nil, err
			}
			if seq > through {
				break
			}
			if receiptCount >= 256 {
				return nil, limitErr("retention receipt batch")
			}
			changes = append(changes, spool.Mutation{Key: key, Deleted: true})
			receiptCount++
		}
		if through == ^uint64(0) {
			return nil, invalidErr("sequence exhausted")
		}
		if through+1 > head.RetainedFrom {
			head.RetainedFrom = through + 1
		}
		changes = append(changes, spool.Mutation{Key: replication.RetainedKey(origin), Value: replication.EncodeProgress(&head)})
		changes = append(changes, spool.Mutation{Key: replication.FloorKey(origin), Value: encodeOne(func(e *postcard.Encoder) { e.U64(through) })})
		total := uint64(0)
		if raw, err := o.Store.Get([]byte("07/replication/retention/collected")); err != nil {
			return nil, err
		} else if raw != nil {
			v, err := decodeU64(raw)
			if err != nil {
				return nil, err
			}
			total = v
		}
		total += count
		changes = append(changes, spool.Mutation{Key: []byte("07/replication/retention/collected"),
			Value: encodeOne(func(e *postcard.Encoder) { e.U64(total) })})
		if err := o.Store.Commit(changes, spool.DurabilitySync); err != nil {
			return nil, err
		}
		return count, nil
	})
	if err != nil {
		return 0, err
	}
	return v.(uint64), nil
}

// ReclaimRetention runs gated physical reclamation.
func (a *Adapter) ReclaimRetention() error {
	now := replication.NowMillis()
	last := a.lastReclaimMs.Load()
	if now-last < reclaimMinIntervalMillis {
		return nil
	}
	if !a.lastReclaimMs.CompareAndSwap(last, now) {
		return nil
	}
	database, err := a.Database()
	if err != nil {
		return err
	}
	ticket, err := database.inner.scheduler.AdmitTimeout(WriterMaintenance, 100*time.Millisecond)
	if err != nil {
		return storageNetErr(err)
	}
	defer ticket.Release()
	_, err = a.Spool(func(o *Opened) (any, error) { return nil, o.Store.Reclaim() })
	return err
}

// Schema renders the replicated schema bytes.
func (a *Adapter) Schema() ([]byte, error) {
	v, err := a.Spool(func(o *Opened) (any, error) {
		filtered := FilterSchema(&o.Schema)
		canonical, err := CanonicalSchema(&filtered)
		if err != nil {
			return nil, err
		}
		return EncodeSchema(*canonical), nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

// Observe advances the collection clock.
func (a *Adapter) Observe(millis uint64, logical uint32) error {
	_, err := a.Call(func(c *Core) (any, error) {
		c.collectionClock = c.collectionClock.Max(Version{Millis: millis, Logical: logical, Node: [16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}})
		return nil, nil
	})
	if err != nil {
		return storageNetErr(err)
	}
	return nil
}

// AcceptSchema adopts an incoming schema when idle.
func (a *Adapter) AcceptSchema(schemaBytes []byte) (bool, error) {
	incoming, err := DecodeSchema(schemaBytes)
	if err != nil {
		return false, storageNetErr(err)
	}
	database, err := a.Database()
	if err != nil {
		return false, err
	}
	ticket, err := database.inner.scheduler.AdmitTimeout(WriterRemote, 100*time.Millisecond)
	if err != nil {
		if err == ErrBusy {
			return false, nil
		}
		return false, storageNetErr(err)
	}
	defer ticket.Release()
	v, err := a.Call(func(c *Core) (any, error) { return c.adoptSchema(incoming) })
	if err != nil {
		return false, storageNetErr(err)
	}
	return v.(bool), nil
}

// Apply merges payloads plus auxiliary records.
func (a *Adapter) Apply(payloads [][]byte, records []replication.Record) (bool, error) {
	start := time.Now()
	timing := os.Getenv("MURMUR_APPLY_TIMING") != ""
	database, err := a.Database()
	if err != nil {
		return false, err
	}
	reserved := a.mergeDeferrals.Load() >= 2
	type decoded struct {
		state  ReplicaState
		origin [16]byte
	}
	var states []decoded
	var auxiliary []replication.Record
	for _, payload := range payloads {
		content, err := DecodeBridgeContent(payload)
		if err != nil {
			return false, storageNetErr(err)
		}
		if err := validateAuxiliary(content.Records); err != nil {
			return false, storageNetErr(err)
		}
		if content.SQLite != nil {
			state, err := DecodeReplicaState(content.SQLite)
			if err != nil {
				return false, storageNetErr(err)
			}
			states = append(states, decoded{state: state, origin: content.Origin})
		}
		auxiliary = append(auxiliary, content.Records...)
	}
	tDecode := time.Since(start)
	for attempt := 0; attempt < 8; attempt++ {
		attemptStates := make([]decoded, len(states))
		copy(attemptStates, states)
		attemptAux := append([]replication.Record(nil), auxiliary...)
		ticket, err := database.inner.scheduler.AdmitTimeout(WriterRemote, 100*time.Millisecond)
		if err != nil {
			if err == ErrBusy {
				return false, nil
			}
			return false, storageNetErr(err)
		}
		releaseTicket := true
		defer func() {
			if releaseTicket {
				ticket.Release()
			}
		}()
		tAdmit1 := time.Since(start)
		v, err := a.Call(func(c *Core) (any, error) {
			c.status.Mu.Lock()
			needsCapture := c.status.Status.Committed != c.capturedCursor
			c.status.Mu.Unlock()
			if needsCapture {
				if err := c.checkpoint(); err != nil {
					return nil, err
				}
			}
			c.status.Mu.Lock()
			defer c.status.Mu.Unlock()
			if c.status.Status.StorageError != nil {
				return nil, nil
			}
			committed := c.status.Status.Committed
			return &committed, nil
		})
		if err != nil {
			return false, storageNetErr(err)
		}
		if v == nil {
			a.mergeDeferrals.Add(1)
			return false, nil
		}
		cursor := *(v.(*uint64))
		if !reserved {
			ticket.Release()
			releaseTicket = false
			ticket = nil
		}
		prepared, err := a.Spool(func(o *Opened) (any, error) {
			if o.Cursor != cursor {
				return nil, nil
			}
			var incoming []ReplicaState
			for _, d := range attemptStates {
				state := d.state
				if err := dropRetiredTables(o, &state, d.origin); err != nil {
					return nil, err
				}
				incoming = append(incoming, state)
			}
			filtered, err := FilterRecords(attemptAux, &o.Schema)
			if err != nil {
				return nil, err
			}
			attemptAux = filtered
			disjoint := len(attemptAux) == 0
			affected, err := HighLowAffectedRecords(attemptAux)
			if err != nil {
				return nil, err
			}
			keys := make(map[RowID]bool)
			for _, ref := range affected {
				keys[RowID{Table: ref.Table, Key: string(ref.Key)}] = true
			}
			for i := range incoming {
				state := &incoming[i]
				for _, row := range state.Rows {
					if ReplicationEnabled(&o.Schema, row.Table) {
						keys[RowID{Table: row.Table, Key: string(EncodeValues(row.Key))}] = true
					}
				}
				for _, tomb := range state.Tombstones {
					if ReplicationEnabled(&o.Schema, tomb.Table) {
						keys[RowID{Table: tomb.Table, Key: string(tomb.Key)}] = true
					}
				}
			}
			baseline, err := AffectedState(o.Store, o.Schema, o.Identity.Database, o.Clock, keys)
			if err != nil {
				return nil, err
			}
			for i := range incoming {
				incomingState := &incoming[i]
				for _, table := range incomingState.Schema.Tables {
					if !ReplicationEnabled(&baseline.Schema, table.ID) {
						continue
					}
					local := findSchemaTable(&baseline.Schema, table.ID)
					if local == nil {
						return nil, ErrSchemaMismatch
					}
					if local.Name != table.Name {
						return nil, ErrSchemaMismatch
					}
					for _, c := range table.Columns {
						found := false
						for _, l := range local.Columns {
							historical := c
							historical.Fts = l.Fts
							historical.Vector = l.Vector
							if columnsEqualValue(l, historical) {
								found = true
								break
							}
						}
						if !found {
							return nil, ErrSchemaMismatch
						}
					}
				}
				for j := range incomingState.Rows {
					row := &incomingState.Rows[j]
					if !ReplicationEnabled(&baseline.Schema, row.Table) {
						continue
					}
					table := findSchemaTable(&baseline.Schema, row.Table)
					if table == nil {
						return nil, ErrSchemaMismatch
					}
					for _, column := range table.Columns {
						found := false
						for _, cell := range row.Cells {
							if cell.Column == column.ID {
								found = true
								break
							}
						}
						if !found {
							if column.NotNull || column.PrimaryKey > 0 {
								return nil, ErrSchemaMismatch
							}
							row.Cells = append(row.Cells, Cell{Column: column.ID})
						}
					}
				}
				FilterState(incomingState, &baseline.Schema)
				incomingState.Schema = baseline.Schema
			}
			joined, err := JoinBatch(baseline, incoming)
			if err != nil {
				return nil, err
			}
			attemptAux, err = FilterRecords(attemptAux, &o.Schema)
			if err != nil {
				return nil, err
			}
			attemptAux, err = HighLowJoin(o, attemptAux, joined)
			if err != nil {
				return nil, err
			}
			attemptAux, err = JoinPolicyStates(o, attemptAux, joined)
			if err != nil {
				return nil, err
			}
			attemptAux, err = joinAuxiliary(o, attemptAux)
			if err != nil {
				return nil, err
			}
			PreserveLocal(joined, baseline)
			patch, err := PrepareDelta(baseline, joined, cursor)
			if err != nil {
				return nil, err
			}
			return &applyPrepared{patch: patch, auxiliary: attemptAux, keys: keys, disjoint: disjoint}, nil
		})
		if err != nil {
			return false, err
		}
		if prepared == nil {
			continue
		}
		prep := prepared.(*applyPrepared)
		tSpool := time.Since(start)
		if ticket == nil {
			ticket, err = database.inner.scheduler.AdmitTimeout(WriterRemote, 100*time.Millisecond)
			if err != nil {
				if err == ErrBusy {
					return false, nil
				}
				return false, storageNetErr(err)
			}
			releaseTicket = true
		}
		tAdmit2 := time.Since(start)
		snapshotCompletion := false
		for _, r := range records {
			if bytes.HasPrefix(r.Key, replication.SnapshotReceipts) {
				snapshotCompletion = true
				break
			}
		}
		perAttempt := append(append([]replication.Record(nil), records...), prep.auxiliary...)
		v, err = a.Call(func(c *Core) (any, error) {
			patch := prep.patch
			if prep.disjoint {
				advanced, err := c.AdvanceDisjointPatch(patch, prep.keys)
				if err != nil || !advanced {
					return false, err
				}
			}
			return c.PublishNetwork(patch, perAttempt, false)
		})
		if err != nil {
			return false, storageNetErr(err)
		}
		if !v.(bool) {
			continue
		}
		ticket.Release()
		releaseTicket = false
		a.mergeDeferrals.Store(0)
		tPublish := time.Since(start)
		nowMs := replication.NowMillis()
		last := a.lastApplyBarrierMs.Load()
		since := a.batchesSinceBarrier.Add(1)
		if snapshotCompletion || (since >= 4 && nowMs-last >= 1000 && a.lastApplyBarrierMs.CompareAndSwap(last, nowMs)) {
			a.batchesSinceBarrier.Store(0)
			reply := make(chan error, 1)
			select {
			case a.worker <- Work{Kind: WorkBarrier, BarrierReply: reply}:
			case <-time.After(5 * time.Minute):
				return false, unavailableNetErr("storage worker stopped")
			}
			if err := <-reply; err != nil {
				return false, storageNetErr(err)
			}
		}
		if timing && time.Since(start) >= 100*time.Millisecond {
			total := time.Since(start)
			fmt.Fprintf(os.Stderr, "[backend-timing] total=%dms decode=%dms admit1=%dms spool=%dms admit2=%dms publish=%dms barrier=%dms\n",
				total.Milliseconds(), tDecode.Milliseconds(),
				(tAdmit1 - tDecode).Milliseconds(), (tSpool - tAdmit1).Milliseconds(),
				(tAdmit2 - tSpool).Milliseconds(), (tPublish - tAdmit2).Milliseconds(),
				(total - tPublish).Milliseconds())
		}
		return true, nil
	}
	a.mergeDeferrals.Add(1)
	return false, nil
}

type applyPrepared struct {
	patch     *Patch
	auxiliary []replication.Record
	keys      map[RowID]bool
	disjoint  bool
}

// Attach starts replication for a database.
func Attach(database *Database, config *replication.Config) (*replication.Handle, error) {
	adapter := &Adapter{
		inner: database.inner, worker: database.inner.worker,
		identity: [2][16]byte{database.DatabaseID(), database.NodeID()},
	}
	handle, err := replication.Start(config, adapter)
	if err != nil {
		return nil, invalidErr("%v", err)
	}
	return handle, nil
}

func columnsEqualValue(a, b Column) bool {
	if a.ID != b.ID || a.Name != b.Name || a.PrimaryKey != b.PrimaryKey ||
		a.DeclaredType != b.DeclaredType || a.NotNull != b.NotNull || a.Policy != b.Policy ||
		a.Fts != b.Fts {
		return false
	}
	if (a.Vector == nil) != (b.Vector == nil) {
		return false
	}
	if a.Vector != nil && *a.Vector != *b.Vector {
		return false
	}
	return true
}

// SignLocal signs filtered local content into spool mutations.
func SignLocal(opened *Opened, config *replication.Config, state ReplicaState, records []replication.Record) ([]spool.Mutation, error) {
	schema := state.Schema
	FilterState(&state, &schema)
	filtered, err := FilterRecords(records, &schema)
	if err != nil {
		return nil, err
	}
	state.Schema = FilterSchema(&schema)
	encoded := EncodeReplicaState(state)
	return signContent(opened, config, &replication.Content{SQLite: encoded, Records: filtered, Origin: opened.Identity.Node}, state.Clock, &state.Schema)
}

func signContent(opened *Opened, config *replication.Config, content *replication.Content, clock Version, schema *Schema) ([]spool.Mutation, error) {
	origin := opened.Identity.Node
	previous := uint64(0)
	if raw, err := opened.Store.Get(replication.SequenceKey(origin)); err != nil {
		return nil, err
	} else if raw != nil {
		v, err := decodeU64(raw)
		if err != nil {
			return nil, err
		}
		previous = v
	}
	if previous == ^uint64(0) {
		return nil, corruptErr("origin sequence exhausted")
	}
	sequence := previous + 1
	schemaDigest, err := shaSchema(schema)
	if err != nil {
		return nil, err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, ioErr(err)
	}
	payload := replication.EncodeContent(content)
	if len(payload) > config.MaxTransactionBytes {
		return nil, limitErr("signed replication content exceeds max_transaction_bytes; reduce capture size or raise the configured limit")
	}
	tx := &replication.Transaction{Database: opened.Identity.Database, Origin: origin, Sequence: sequence,
		ID: id, Millis: clock.Millis, Logical: clock.Logical, Schema: schemaDigest, Payload: payload}
	seed := config.SigningKey
	tx.Sign(&seed)
	headKey := []byte("07/replication/retained/" + replication.Hex(origin[:]))
	head := replication.Progress{Origin: origin, Applied: sequence, Observed: sequence, RetainedFrom: sequence, RetainedThrough: sequence}
	if raw, err := opened.Store.Get(headKey); err != nil {
		return nil, err
	} else if raw != nil {
		h, err := replication.DecodeProgress(raw)
		if err != nil {
			return nil, corruptErr("%v", err)
		}
		head = h
	}
	head.Applied, head.Observed, head.RetainedThrough = sequence, sequence, sequence
	digest := tx.Digest()
	now := encodeOne(func(e *postcard.Encoder) { e.U64(replication.NowMillis()) })
	seq := encodeOne(func(e *postcard.Encoder) { e.U64(sequence) })
	return []spool.Mutation{
		{Key: headKey, Value: replication.EncodeProgress(&head)},
		{Key: replication.LogKey(origin, sequence), Value: replication.EncodeTransaction(tx)},
		{Key: replication.TimeKey(origin, sequence), Value: now},
		{Key: replication.SequenceKey(origin), Value: seq},
		{Key: replication.ProgressKey(origin), Value: seq},
		{Key: replication.ReceiptKey(origin, sequence), Value: append([]byte(nil), digest[:]...)},
	}, nil
}

func shaSchema(schema *Schema) ([32]byte, error) {
	filtered := FilterSchema(schema)
	canonical, err := CanonicalSchema(&filtered)
	if err != nil {
		return [32]byte{}, err
	}
	return replication.SchemaDigest(EncodeSchema(*canonical)), nil
}

func snapshotContent(o *Opened) ([]byte, error) {
	state, err := ReadState(o.Store, o.Schema, o.Identity.Database, o.Clock)
	if err != nil {
		return nil, err
	}
	FilterState(state, &o.Schema)
	state.Schema = FilterSchema(&o.Schema)
	var records []replication.Record
	for _, prefix := range [][]byte{[]byte("08/files/meta/"), []byte("10/policy/"), []byte("09/bridge/owner/"), []byte("09/bridge/shadow/"), []byte("09/bridge/provenance/")} {
		for _, key := range o.Store.KeysWithPrefix(prefix) {
			value, err := o.Store.Get(key)
			if err != nil {
				return nil, err
			}
			if value != nil {
				records = append(records, replication.PutRecord(key, value))
			}
		}
	}
	records, err = FilterRecords(records, &o.Schema)
	if err != nil {
		return nil, err
	}
	encoded := EncodeReplicaState(*state)
	return replication.EncodeContent(&replication.Content{SQLite: encoded, Records: records, Origin: o.Identity.Node}), nil
}

func keySequence(key []byte) (uint64, error) {
	name := string(key)
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		name = name[i+1:]
	}
	seq, err := strconv.ParseUint(name, 10, 64)
	if err != nil {
		return 0, corruptErr("replication sequence key")
	}
	return seq, nil
}

// MonotonicProgress resolves applied watermarks at commit time.
func MonotonicProgress(o *Opened, records []EventRecord) error {
	seen := make(map[string]uint64)
	for i := range records {
		key, value := records[i].Key, records[i].Value
		if !bytes.HasPrefix(key, []byte("07/replication/applied/")) || value == nil {
			continue
		}
		old, ok := seen[string(key)]
		if !ok {
			raw, err := o.Store.Get(key)
			if err != nil {
				return err
			}
			if raw != nil {
				v, err := decodeU64(raw)
				if err != nil {
					return err
				}
				old = v
			}
		}
		next, err := decodeU64(value)
		if err != nil {
			return err
		}
		if old > next {
			next = old
		}
		records[i].Value = encodeOne(func(e *postcard.Encoder) { e.U64(next) })
		seen[string(key)] = next
	}
	return nil
}

func durableProgress(o *Opened) ([]replication.Progress, error) {
	var entries []replication.Progress
	for _, key := range o.Store.KeysWithPrefix([]byte("07/replication/applied/")) {
		origin, err := parseProgressOrigin(key)
		if err != nil {
			return nil, err
		}
		raw, err := o.Store.Get(key)
		if err != nil {
			return nil, err
		}
		if raw == nil {
			return nil, corruptErr("progress value")
		}
		applied, err := decodeU64(raw)
		if err != nil {
			return nil, err
		}
		head := replication.Progress{Origin: origin, Applied: applied, Observed: applied}
		if hraw, err := o.Store.Get(replication.RetainedKey(origin)); err != nil {
			return nil, err
		} else if hraw != nil {
			h, err := replication.DecodeProgress(hraw)
			if err != nil {
				return nil, corruptErr("%v", err)
			}
			head = h
		}
		head.Applied = applied
		if applied > head.Observed {
			head.Observed = applied
		}
		entries = append(entries, head)
	}
	return entries, nil
}

func parseProgressOrigin(key []byte) ([16]byte, error) {
	var zero [16]byte
	name := string(key)
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		name = name[i+1:]
	}
	if len(name) != 32 {
		return zero, corruptErr("progress origin")
	}
	for i := range zero {
		v, err := strconv.ParseUint(name[i*2:i*2+2], 16, 8)
		if err != nil {
			return zero, corruptErr("progress origin")
		}
		zero[i] = byte(v)
	}
	return zero, nil
}

func decodeCoverage(b []byte) (uint64, map[[16]byte]uint64, error) {
	d := postcard.NewDecoder(b)
	sequence := d.U64()
	n := d.Len()
	coverage := make(map[[16]byte]uint64, n)
	for i := 0; i < n; i++ {
		var k [16]byte
		copy(k[:], d.Raw(16))
		coverage[k] = d.U64()
	}
	if err := d.Err(); err != nil {
		return 0, nil, corruptErr("%v", err)
	}
	if d.Offset() != len(b) {
		return 0, nil, corruptErr("trailing coverage bytes")
	}
	return sequence, coverage, nil
}

func decodeU64(b []byte) (uint64, error) {
	return decodeOne(b, func(d *postcard.Decoder) uint64 { return d.U64() })
}

func validateAuxiliary(records []replication.Record) error {
	if len(records) > 10000 {
		return limitErr("auxiliary records")
	}
	for _, record := range records {
		if !bytes.HasPrefix(record.Key, []byte("08/files/meta/")) &&
			!bytes.HasPrefix(record.Key, []byte("10/policy/")) &&
			!bytes.HasPrefix(record.Key, []byte("09/bridge/owner/")) &&
			!bytes.HasPrefix(record.Key, []byte("09/bridge/shadow/")) &&
			!bytes.HasPrefix(record.Key, []byte("09/bridge/provenance/")) {
			return corruptErr("unauthorized auxiliary record")
		}
		if record.Value == nil {
			return corruptErr("auxiliary tombstones must be logical")
		}
	}
	return nil
}

// DecodeBridgeContent decodes a payload, accepting pre-origin payloads.
func DecodeBridgeContent(payload []byte) (replication.Content, error) {
	if content, err := replication.DecodeContent(payload); err == nil {
		return content, nil
	}
	legacy, err := decodeOne(payload, func(d *postcard.Decoder) replication.Content {
		var c replication.Content
		if d.Present() {
			sqlite := d.Bytes()
			c.SQLite = sqlite
		}
		n := d.Len()
		for i := 0; i < n; i++ {
			var r replication.Record
			r.Key = d.Bytes()
			if d.Present() {
				r.Value = d.Bytes()
			}
			c.Records = append(c.Records, r)
		}
		return c
	})
	if err != nil {
		return replication.Content{}, corruptErr("%v", err)
	}
	return legacy, nil
}

func dropRetiredTables(o *Opened, incoming *ReplicaState, origin [16]byte) error {
	raw, err := o.Store.Get(replication.SchemaAcceptedKey(origin))
	if err != nil {
		return err
	}
	if raw == nil {
		return nil
	}
	manifest, err := decodeOne(raw, func(d *postcard.Decoder) []byte {
		_ = d.U64()
		return d.Bytes()
	})
	if err != nil {
		return corruptErr("%v", err)
	}
	declared, err := DecodeSchema(manifest)
	if err != nil {
		return err
	}
	live := make(map[string]bool)
	for _, t := range declared.Tables {
		live[t.ID] = true
	}
	local := make(map[string]bool)
	for _, t := range o.Schema.Tables {
		local[t.ID] = true
	}
	var retired []string
	for _, t := range incoming.Schema.Tables {
		if !live[t.ID] && !local[t.ID] {
			retired = append(retired, t.ID)
		}
	}
	if len(retired) == 0 {
		return nil
	}
	retiredSet := make(map[string]bool)
	for _, id := range retired {
		retiredSet[id] = true
	}
	kept := incoming.Schema.Tables[:0]
	for _, t := range incoming.Schema.Tables {
		if live[t.ID] {
			kept = append(kept, t)
		}
	}
	incoming.Schema.Tables = kept
	keptRows := incoming.Rows[:0]
	for _, r := range incoming.Rows {
		if !retiredSet[r.Table] {
			keptRows = append(keptRows, r)
		}
	}
	incoming.Rows = keptRows
	keptTombs := incoming.Tombstones[:0]
	for _, t := range incoming.Tombstones {
		if !retiredSet[t.Table] {
			keptTombs = append(keptTombs, t)
		}
	}
	incoming.Tombstones = keptTombs
	return nil
}

func joinAuxiliary(o *Opened, records []replication.Record) ([]replication.Record, error) {
	joined := make(map[string][]byte)
	var order []string
	for _, record := range records {
		if record.Value == nil {
			return nil, corruptErr("auxiliary tombstones must be logical")
		}
		previous, ok := joined[string(record.Key)]
		if !ok {
			stored, err := o.Store.Get(record.Key)
			if err != nil {
				return nil, err
			}
			previous = stored
		}
		if bytes.HasPrefix(record.Key, []byte("08/files/meta/")) {
			incoming, err := replication.DecodeFileMetadata(record.Value)
			if err != nil {
				return nil, corruptErr("%v", err)
			}
			replace := true
			if previous != nil {
				prior, err := replication.DecodeFileMetadata(previous)
				if err != nil {
					return nil, corruptErr("%v", err)
				}
				replace = fileMetaGreater(&incoming, &prior)
			}
			if replace {
				if _, seen := joined[string(record.Key)]; !seen {
					order = append(order, string(record.Key))
				}
				joined[string(record.Key)] = record.Value
			} else if _, seen := joined[string(record.Key)]; !seen && previous != nil {
				order = append(order, string(record.Key))
				joined[string(record.Key)] = previous
			}
		} else {
			if _, seen := joined[string(record.Key)]; !seen {
				order = append(order, string(record.Key))
			}
			joined[string(record.Key)] = record.Value
		}
	}
	sort.Strings(order)
	out := make([]replication.Record, 0, len(order))
	for _, k := range order {
		out = append(out, replication.PutRecord([]byte(k), joined[k]))
	}
	return out, nil
}

func fileMetaGreater(a, b *replication.FileMetadata) bool {
	if a.Millis != b.Millis {
		return a.Millis > b.Millis
	}
	if c := bytes.Compare(a.Node[:], b.Node[:]); c != 0 {
		return c > 0
	}
	if c := bytes.Compare(a.Digest[:], b.Digest[:]); c != 0 {
		return c > 0
	}
	return !b.Deleted && a.Deleted
}

// CanonicalSchema renders the network identity of a schema.
func CanonicalSchema(schema *Schema) (*Schema, error) {
	canonical := *schema
	tables := append([]Table(nil), schema.Tables...)
	sort.Slice(tables, func(i, j int) bool { return tables[i].ID < tables[j].ID })
	canonical.Tables = tables
	for i := range canonical.Tables {
		t := &canonical.Tables[i]
		columns := append([]Column(nil), t.Columns...)
		sort.Slice(columns, func(a, b int) bool { return columns[a].ID < columns[b].ID })
		t.Columns = columns
		sql, err := canonicalTableDefinition(t)
		if err != nil {
			return nil, err
		}
		t.SQL = sql
	}
	indexes := append([]string(nil), schema.Indexes...)
	sort.Strings(indexes)
	canonical.Indexes = indexes
	canonical.History = [32]byte{}
	canonical.History = hash(EncodeSchema(canonical))
	return &canonical, nil
}

func canonicalColumnDefinition(column *Column) (string, error) {
	if column.Name == "" || len(column.DeclaredType) > 256 {
		return "", ErrSchemaMismatch
	}
	for i := 0; i < len(column.DeclaredType); i++ {
		c := column.DeclaredType[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == ' ' || c == '(' || c == ')' || c == ',' || c == '_') {
			return "", ErrSchemaMismatch
		}
	}
	notNull := ""
	if column.NotNull {
		notNull = " NOT NULL"
	}
	return quote(column.Name) + " " + column.DeclaredType + notNull, nil
}

func canonicalTableDefinition(table *Table) (string, error) {
	if table.Name == "" || len(table.Columns) == 0 || len(table.Columns) > 2000 {
		return "", ErrSchemaMismatch
	}
	if err := ValidateTable(table); err != nil {
		return "", err
	}
	source, err := ColumnDefinitions(table.SQL)
	if err != nil {
		return "", err
	}
	var definitions []string
	for i := range table.Columns {
		column := &table.Columns[i]
		if _, err := canonicalColumnDefinition(column); err != nil {
			return "", err
		}
		definition, ok := source[column.Name]
		if !ok {
			return "", ErrSchemaMismatch
		}
		definitions = append(definitions, definition)
	}
	keys := PkColumns(table)
	if len(keys) == 0 {
		return "", ErrSchemaMismatch
	}
	quoted := make([]string, len(keys))
	for i, c := range keys {
		quoted[i] = quote(c.Name)
	}
	definitions = append(definitions, "PRIMARY KEY ("+strings.Join(quoted, ",")+")")
	if key := table.ApplicationKey; key != nil {
		parts := make([]string, len(key.Columns))
		for i, n := range key.Columns {
			direction := "ASC"
			if key.Descending[i] {
				direction = "DESC"
			}
			parts[i] = quote(n) + " COLLATE " + quote(key.Collations[i]) + " " + direction
		}
		definitions = append(definitions, "UNIQUE ("+strings.Join(parts, ",")+")"+Conflict(key))
	}
	suffix := ""
	if key := table.ApplicationKey; key != nil {
		switch {
		case key.WithoutRow && key.Strict:
			suffix = " WITHOUT ROWID, STRICT"
		case key.WithoutRow:
			suffix = " WITHOUT ROWID"
		case key.Strict:
			suffix = " STRICT"
		}
	}
	return "CREATE TABLE " + quote(table.Name) + " (" + strings.Join(definitions, ",") + ")" + suffix, nil
}

// PublishNetwork applies a network patch plus auxiliary records,
// returning false when the cursor moved (caller retries).
func (c *Core) PublishNetwork(patch *Patch, records []replication.Record, remint bool) (bool, error) {
	c.status.Mu.Lock()
	committed, storageError := c.status.Status.Committed, c.status.Status.StorageError
	c.status.Mu.Unlock()
	if committed != patch.Cursor || committed != c.capturedCursor || storageError != nil {
		return false, nil
	}
	size := len(EncodeRowStates(patch.Rows)) + len(EncodeRowStates(patch.Deleted)) + len(encodeTombs(patch.Tombstones)) + len(EncodeVersion(patch.Clock))
	for _, r := range records {
		size += len(r.Key) + len(r.Value)
	}
	if err := c.Admit(size); err != nil {
		return false, err
	}
	// Destroy the clean session: remote publication must never feed
	// local capture.
	if c.capture != nil {
		c.capture.Close()
		c.capture = nil
	}
	if err := c.connection.ExecBatch("BEGIN IMMEDIATE"); err != nil {
		return false, sqlErr(err)
	}
	result := func() error {
		if err := DeleteUserRows(c.connection, &c.schema, patch.Deleted); err != nil {
			return err
		}
		if err := PutUserRows(c.connection, &c.schema, patch.Rows); err != nil {
			return err
		}
		if err := c.connection.ExecBatch("COMMIT"); err != nil {
			return sqlErr(err)
		}
		return nil
	}()
	if result != nil {
		_ = c.connection.ExecBatch("ROLLBACK")
	}
	capture, cerr := CaptureSchema(c.connection, &c.schema)
	if cerr != nil {
		return false, cerr
	}
	c.capture = capture
	if result != nil {
		return false, result
	}
	c.search.SearchRows(&c.schema, patch.Rows)
	c.search.SearchRows(&c.schema, patch.Deleted)
	c.status.Mu.Lock()
	c.status.Status.SearchIndexes = c.search.SearchStatus()
	c.status.Mu.Unlock()
	cursor, err := c.bump()
	if err != nil {
		return false, err
	}
	c.collectionClock = c.collectionClock.Max(patch.Clock)
	affectedSet := make(map[string]bool)
	for _, r := range append(append([]RowState(nil), patch.Rows...), patch.Deleted...) {
		for _, t := range c.schema.Tables {
			if t.ID == r.Table {
				affectedSet[t.Name] = true
			}
		}
	}
	affected := make([]string, 0, len(affectedSet))
	for name := range affectedSet {
		affected = append(affected, name)
	}
	sort.Strings(affected)
	eventRecords := make([]EventRecord, len(records))
	for i, r := range records {
		eventRecords[i] = EventRecord{Key: r.Key, Value: r.Value}
	}
	if err := c.send(&Event{Tag: EventReplicaPatch, Rows: patch.Rows, Deleted: patch.Deleted,
		Tombs: patch.Tombstones, Clock: patch.Clock, Records: eventRecords, Remint: remint}, cursor, size); err != nil {
		return false, err
	}
	c.capturedCursor = cursor
	c.subMgr.NotifyCommit(cursor, affected)
	return true, nil
}

func encodeTombs(tombs []Tombstone) []byte {
	return encodeOne(func(e *postcard.Encoder) {
		e.Len(len(tombs))
		for i := range tombs {
			encodeTombstone(e, tombs[i])
		}
	})
}

func (c *Core) adoptSchema(incoming Schema) (bool, error) {
	for i := range incoming.Tables {
		table := &incoming.Tables[i]
		if table.ApplicationKey == nil {
			return false, ErrSchemaMismatch
		}
		if incoming.TableReplication == nil {
			incoming.TableReplication = make(map[string]bool)
		}
		incoming.TableReplication[table.ID] = true
	}
	local := FilterSchema(&c.schema)
	remote := FilterSchema(&incoming)
	localCanonical, err := CanonicalSchema(&local)
	if err != nil {
		return false, err
	}
	remoteCanonical, err := CanonicalSchema(&remote)
	if err != nil {
		return false, err
	}
	if schemasEqual(localCanonical, remoteCanonical) {
		return true, nil
	}
	c.status.Mu.Lock()
	committed, durable, storageError := c.status.Status.Committed, c.status.Status.Durable, c.status.Status.StorageError
	c.status.Mu.Unlock()
	if committed != c.capturedCursor || durable != c.capturedCursor || len(c.pendingDelta) != 0 || storageError != nil {
		return false, nil
	}
	// The merge below mutates target tables/columns/flags in place; it
	// must own its backing. A shallow copy aliases the live schema, so a
	// flag-only change would flip c.schema before the equality check,
	// short-circuit to (true, nil), and skip the index build plus the
	// storage event while still recording acceptance (phantom adoption).
	target := CloneSchema(c.schema)
	var ddl strings.Builder
	for _, incomingTable := range incoming.Tables {
		disabled := false
		for _, t := range c.schema.Tables {
			if t.ID == incomingTable.ID && !ReplicationEnabled(&c.schema, incomingTable.ID) {
				disabled = true
				break
			}
		}
		if disabled {
			continue
		}
		if localTable := findSchemaTable(&target, incomingTable.ID); localTable != nil {
			if localTable.Name != incomingTable.Name || !appKeyEqual(localTable.ApplicationKey, incomingTable.ApplicationKey) {
				return false, ErrSchemaMismatch
			}
			for _, column := range incomingTable.Columns {
				if existing := findColumnByID(localTable, column.ID); existing != nil {
					expected := column
					expected.Fts = existing.Fts
					expected.Vector = existing.Vector
					if !columnsEqualValue(*existing, expected) {
						return false, ErrSchemaMismatch
					}
					existing.Fts = column.Fts
					existing.Vector = column.Vector
				} else {
					if column.PrimaryKey > 0 || column.NotNull {
						return false, ErrSchemaMismatch
					}
					for _, l := range localTable.Columns {
						if l.Name == column.Name {
							return false, ErrSchemaMismatch
						}
					}
					definitions, err := ColumnDefinitions(incomingTable.SQL)
					if err != nil {
						return false, err
					}
					definition, ok := definitions[column.Name]
					if !ok {
						return false, ErrSchemaMismatch
					}
					ddl.WriteString("ALTER TABLE " + quote(localTable.Name) + " ADD COLUMN " + definition + ";")
					localTable.Columns = append(localTable.Columns, column)
				}
			}
		} else {
			for _, t := range target.Tables {
				if t.Name == incomingTable.Name {
					return false, ErrSchemaMismatch
				}
			}
			definition, err := canonicalTableDefinition(&incomingTable)
			if err != nil {
				return false, err
			}
			ddl.WriteString(definition)
			ddl.WriteString(";")
			target.Tables = append(target.Tables, incomingTable)
			if target.TableReplication == nil {
				target.TableReplication = make(map[string]bool)
			}
			target.TableReplication[incomingTable.ID] = true
		}
	}
	for _, index := range incoming.Indexes {
		found := false
		for _, existing := range target.Indexes {
			if existing == index {
				found = true
				break
			}
		}
		if !found {
			if table, ok := IndexTable(index); ok {
				var match *Table
				for i := range incoming.Tables {
					if asciiEqualFold(incoming.Tables[i].Name, table) {
						match = &incoming.Tables[i]
						break
					}
				}
				if match == nil {
					return false, ErrSchemaMismatch
				}
				disabled := false
				for _, l := range c.schema.Tables {
					if l.ID == match.ID && !ReplicationEnabled(&c.schema, match.ID) {
						disabled = true
						break
					}
				}
				if disabled {
					continue
				}
			}
			words := Keywords(index)
			banned := map[string]bool{"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true,
				"DROP": true, "TRIGGER": true, "VIEW": true, "PRAGMA": true, "ATTACH": true,
				"DETACH": true, "UNIQUE": true}
			if len(words) < 2 || words[0] != "CREATE" || words[1] != "INDEX" {
				return false, ErrSchemaMismatch
			}
			for _, w := range words {
				if banned[w] {
					return false, ErrSchemaMismatch
				}
			}
			ddl.WriteString(index)
			ddl.WriteString(";")
			target.Indexes = append(target.Indexes, index)
		}
	}
	if ddl.Len() == 0 && schemasEqual(&target, &c.schema) {
		return true, nil
	}
	definitions, err := SchemaSearchDefinitions(nil, &target)
	if err != nil {
		return false, err
	}
	if err := RegisterSearchIndexes(c.connection, definitions); err != nil {
		return false, err
	}
	needsFts := false
	for _, t := range target.Tables {
		for _, col := range t.Columns {
			if col.Fts || col.Vector != nil {
				needsFts = true
			}
		}
	}
	if needsFts {
		row, err := c.connection.QueryRow("PRAGMA temp_store")
		if err != nil {
			return false, sqlErr(err)
		}
		if len(row) == 0 || row[0].Int != 2 {
			if err := c.connection.ExecBatch("PRAGMA temp_store=MEMORY"); err != nil {
				return false, sqlErr(err)
			}
		}
	}
	metadataOnly := ddl.Len() == 0
	var before []RowState
	if !metadataOnly {
		before, err = ReadRows(c.connection, &c.schema)
		if err != nil {
			return false, err
		}
	}
	previousSearch := CloneSearchManager(c.search)
	if c.capture != nil {
		c.capture.Close()
		c.capture = nil
	}
	if err := c.connection.ExecBatch("BEGIN IMMEDIATE"); err != nil {
		return false, sqlErr(err)
	}
	type adoptOut struct {
		actual Schema
		after  []RowState
	}
	out, err := func() (*adoptOut, error) {
		if err := c.connection.ExecBatch(ddl.String()); err != nil {
			return nil, sqlErr(err)
		}
		migration := NewMigration(ddl.String())
		actual, err := Inspect(c.connection, &target, &migration)
		if err != nil {
			return nil, err
		}
		for i := range actual.Tables {
			table := &actual.Tables[i]
			intended := findTableByName(&target, table.Name)
			if intended == nil {
				return nil, ErrSchemaMismatch
			}
			table.ID = intended.ID
			table.ApplicationKey = intended.ApplicationKey
			for j := range table.Columns {
				column := &table.Columns[j]
				definition := findColumnByName(intended, column.Name)
				if definition == nil {
					return nil, ErrSchemaMismatch
				}
				column.ID = definition.ID
				column.Policy = definition.Policy
				column.Fts = definition.Fts
				column.Vector = definition.Vector
			}
		}
		if c.tableFlagsSink != nil {
			if err := c.tableFlagsSink(actual.TableReplication); err != nil {
				return nil, err
			}
		}
		canonical, err := CanonicalSchema(actual)
		if err != nil {
			return nil, err
		}
		actual.History = canonical.History
		if err := c.search.SyncSearchSchema(c.connection, actual, true); err != nil {
			return nil, err
		}
		if _, err := c.search.SearchRefresh(c.connection, actual); err != nil {
			return nil, err
		}
		var after []RowState
		if !metadataOnly {
			after, err = ReadRows(c.connection, actual)
			if err != nil {
				return nil, err
			}
		}
		if err := c.connection.ExecBatch("COMMIT"); err != nil {
			return nil, sqlErr(err)
		}
		return &adoptOut{actual: *actual, after: after}, nil
	}()
	if err != nil {
		_ = c.connection.ExecBatch("ROLLBACK")
		c.search = previousSearch
		c.search.PublishSearchGuard()
		capture, cerr := CaptureSchema(c.connection, &c.schema)
		if cerr != nil {
			return false, cerr
		}
		c.capture = capture
		return false, err
	}
	c.schema = out.actual
	if err := configureAuthorizer(c.connection, &c.schema, c.access, c.search.Guard); err != nil {
		return false, err
	}
	c.status.Mu.Lock()
	c.status.Status.SearchIndexes = c.search.SearchStatus()
	c.status.Mu.Unlock()
	size := len(EncodeRowStates(before)) + len(EncodeRowStates(out.after)) + ddl.Len()
	cursor, err := c.bump()
	if err != nil {
		return false, err
	}
	capture, err := CaptureSchema(c.connection, &c.schema)
	if err != nil {
		return false, err
	}
	c.capture = capture
	actualCopy := CloneSchema(c.schema)
	if err := c.send(&Event{Tag: EventSchemaCaptured, Migration: ddl.String(), Schema: actualCopy, Before: before, After: out.after}, cursor, size); err != nil {
		return false, err
	}
	c.capturedCursor = cursor
	return true, nil
}

func appKeyEqual(a, b *ApplicationKey) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == nil {
		return true
	}
	if len(a.Columns) != len(b.Columns) || len(a.Collations) != len(b.Collations) || len(a.Descending) != len(b.Descending) ||
		a.WithoutRow != b.WithoutRow || a.Strict != b.Strict {
		return false
	}
	for i := range a.Columns {
		if a.Columns[i] != b.Columns[i] || a.Collations[i] != b.Collations[i] || a.Descending[i] != b.Descending[i] {
			return false
		}
	}
	return true
}

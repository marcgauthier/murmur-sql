package murmur

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/compression"
	"github.com/marcgauthier/murmur-sql/murmur/internal/postcard"
	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// Storage format version.
const storageFormat uint32 = 8

func supportedFormat(format uint32) bool {
	return format == 6 || format == 7 || format == storageFormat
}

const (
	storageChunk      = 64 << 10
	storageMaxPayload = 512 << 20
)

// EventTag classifies a storage Event.
type EventTag int

const (
	EventDelta EventTag = iota
	EventSchemaCaptured
	EventSchema
	EventReplica
	EventReplicaPatch
	EventReplicaRecords
	EventTableReplication
	EventMigrationChunk
)

// Event is one durable storage event.
type Event struct {
	Tag       EventTag
	Bytes     []byte
	Version   Version
	Intents   []PolicyIntent
	Migration string
	Schema    Schema
	Before    []RowState
	After     []RowState
	Rows      []RowState
	Deleted   []RowState
	Tombs     []Tombstone
	Clock     Version
	Records   []EventRecord
	Remint    bool
	State     *ReplicaState
	Table     string
	Enabled   bool
}

// EventRecord is one auxiliary record carried by an event.
type EventRecord struct {
	Key   []byte
	Value []byte // nil deletes
}

func encodeEventRecord(e *postcard.Encoder, r EventRecord) {
	e.ByteString(r.Key)
	if r.Value == nil {
		e.None()
	} else {
		e.Some()
		e.ByteString(r.Value)
	}
}

func decodeEventRecord(d *postcard.Decoder) EventRecord {
	var r EventRecord
	r.Key = d.Bytes()
	if d.Present() {
		r.Value = d.Bytes()
	}
	return r
}

func encodeEvent(e *postcard.Encoder, ev *Event) {
	e.Discriminant(uint32(ev.Tag))
	switch ev.Tag {
	case EventDelta:
		e.ByteString(ev.Bytes)
		encodeVersion(e, ev.Version)
		e.Len(len(ev.Intents))
		for _, intent := range ev.Intents {
			encodePolicyIntent(e, intent)
		}
	case EventSchemaCaptured:
		e.String(ev.Migration)
		encodeSchema(e, ev.Schema)
		e.Len(len(ev.Before))
		for _, r := range ev.Before {
			encodeRowState(e, r)
		}
		e.Len(len(ev.After))
		for _, r := range ev.After {
			encodeRowState(e, r)
		}
		encodeVersion(e, ev.Version)
	case EventSchema:
		e.String(ev.Migration)
		encodeSchema(e, ev.Schema)
		encodeVersion(e, ev.Version)
	case EventReplica:
		encodeReplicaState(e, *ev.State)
	case EventReplicaPatch:
		e.Len(len(ev.Rows))
		for _, r := range ev.Rows {
			encodeRowState(e, r)
		}
		e.Len(len(ev.Deleted))
		for _, r := range ev.Deleted {
			encodeRowState(e, r)
		}
		e.Len(len(ev.Tombs))
		for _, t := range ev.Tombs {
			encodeTombstone(e, t)
		}
		encodeVersion(e, ev.Clock)
		e.Len(len(ev.Records))
		for _, r := range ev.Records {
			encodeEventRecord(e, r)
		}
		e.Bool(ev.Remint)
	case EventReplicaRecords:
		encodeReplicaState(e, *ev.State)
		e.Len(len(ev.Records))
		for _, r := range ev.Records {
			encodeEventRecord(e, r)
		}
	case EventTableReplication:
		e.String(ev.Table)
		e.Bool(ev.Enabled)
		encodeSchema(e, ev.Schema)
		e.Len(len(ev.Before))
		for _, r := range ev.Before {
			encodeRowState(e, r)
		}
		e.Len(len(ev.After))
		for _, r := range ev.After {
			encodeRowState(e, r)
		}
		encodeVersion(e, ev.Version)
	case EventMigrationChunk:
		e.Len(len(ev.Before))
		for _, r := range ev.Before {
			encodeRowState(e, r)
		}
		e.Len(len(ev.After))
		for _, r := range ev.After {
			encodeRowState(e, r)
		}
		encodeVersion(e, ev.Version)
	}
}

func decodeEvent(d *postcard.Decoder) Event {
	var ev Event
	ev.Tag = EventTag(d.Discriminant())
	readRows := func() []RowState {
		n := d.Len()
		var out []RowState
		for i := 0; i < n; i++ {
			out = append(out, decodeRowState(d))
		}
		return out
	}
	switch ev.Tag {
	case EventDelta:
		ev.Bytes = d.Bytes()
		ev.Version = decodeVersion(d)
		n := d.Len()
		for i := 0; i < n; i++ {
			ev.Intents = append(ev.Intents, decodePolicyIntent(d))
		}
	case EventSchemaCaptured:
		ev.Migration = d.String()
		ev.Schema = decodeSchema(d)
		ev.Before = readRows()
		ev.After = readRows()
		ev.Version = decodeVersion(d)
	case EventSchema:
		ev.Migration = d.String()
		ev.Schema = decodeSchema(d)
		ev.Version = decodeVersion(d)
	case EventReplica:
		state := decodeReplicaState(d)
		ev.State = &state
	case EventReplicaPatch:
		ev.Rows = readRows()
		ev.Deleted = readRows()
		n := d.Len()
		for i := 0; i < n; i++ {
			ev.Tombs = append(ev.Tombs, decodeTombstone(d))
		}
		ev.Clock = decodeVersion(d)
		n = d.Len()
		for i := 0; i < n; i++ {
			ev.Records = append(ev.Records, decodeEventRecord(d))
		}
		ev.Remint = d.Bool()
	case EventReplicaRecords:
		state := decodeReplicaState(d)
		ev.State = &state
		n := d.Len()
		for i := 0; i < n; i++ {
			ev.Records = append(ev.Records, decodeEventRecord(d))
		}
	case EventTableReplication:
		ev.Table = d.String()
		ev.Enabled = d.Bool()
		ev.Schema = decodeSchema(d)
		ev.Before = readRows()
		ev.After = readRows()
		ev.Version = decodeVersion(d)
	case EventMigrationChunk:
		ev.Before = readRows()
		ev.After = readRows()
		ev.Version = decodeVersion(d)
	}
	return ev
}

// EncodeEvent renders an event.
func EncodeEvent(ev *Event) []byte {
	return encodeOne(func(e *postcard.Encoder) { encodeEvent(e, ev) })
}

// DecodeEvent parses an event.
func DecodeEvent(b []byte) (Event, error) {
	return decodeOne(b, decodeEvent)
}

type logEntry struct {
	format   uint32
	sequence uint64
	cursor   uint64
	event    Event
}

func encodeLogEntry(e *postcard.Encoder, entry *logEntry) {
	e.U32(entry.format)
	e.U64(entry.sequence)
	e.U64(entry.cursor)
	encodeEvent(e, &entry.event)
}

func decodeLogEntry(d *postcard.Decoder) logEntry {
	var entry logEntry
	entry.format = d.U32()
	entry.sequence = d.U64()
	entry.cursor = d.U64()
	entry.event = decodeEvent(d)
	return entry
}

type logSnapshot struct {
	format   uint32
	sequence uint64
	cursor   uint64
	history  []Event
}

func encodeLogSnapshot(e *postcard.Encoder, s *logSnapshot) {
	e.U32(s.format)
	e.U64(s.sequence)
	e.U64(s.cursor)
	e.Len(len(s.history))
	for i := range s.history {
		encodeEvent(e, &s.history[i])
	}
}

func decodeLogSnapshot(d *postcard.Decoder) logSnapshot {
	var s logSnapshot
	s.format = d.U32()
	s.sequence = d.U64()
	s.cursor = d.U64()
	n := d.Len()
	for i := 0; i < n; i++ {
		s.history = append(s.history, decodeEvent(d))
	}
	return s
}

type snapshotHead struct {
	sequence uint64
	cursor   uint64
	chunks   int
	hash     [32]byte
}

func encodeSnapshotHead(e *postcard.Encoder, h *snapshotHead) {
	e.U64(h.sequence)
	e.U64(h.cursor)
	e.U64(uint64(h.chunks))
	e.Raw(h.hash[:])
}

func decodeSnapshotHead(d *postcard.Decoder) snapshotHead {
	var h snapshotHead
	h.sequence = d.U64()
	h.cursor = d.U64()
	h.chunks = int(d.U64())
	copy(h.hash[:], d.Raw(32))
	return h
}

type chunkHeader struct {
	format uint32
	chunks int
	length int
	hash   [32]byte
}

func encodeChunkHeader(e *postcard.Encoder, h *chunkHeader) {
	e.U32(h.format)
	e.U64(uint64(h.chunks))
	e.U64(uint64(h.length))
	e.Raw(h.hash[:])
}

func decodeChunkHeader(d *postcard.Decoder) chunkHeader {
	var h chunkHeader
	h.format = d.U32()
	h.chunks = int(d.U64())
	h.length = int(d.U64())
	copy(h.hash[:], d.Raw(32))
	return h
}

// WorkKind classifies storage worker requests.
type WorkKind int

const (
	WorkCheckpoint WorkKind = iota
	WorkNetwork
	WorkExport
	WorkMerge
	WorkBackup
	WorkBarrier
	WorkRetry
	WorkRotate
	WorkRotateMasterKey
	WorkStop
)

// ExportResult carries a worker export reply.
type ExportResult struct {
	State *ReplicaState
	Err   error
}

// MergeResult carries a worker merge reply.
type MergeResult struct {
	Patch *Patch
	Err   error
}

// Work is one storage worker request.
type Work struct {
	Kind           WorkKind
	Event          *Event
	Cursor         uint64
	Bytes          int
	Network        func(*Opened)
	Rows           []RowState
	ReplicatedOnly bool
	ExportReply    chan<- ExportResult
	Baseline       *ReplicaState
	State          *ReplicaState
	MergeReply     chan<- MergeResult
	Destination    string
	BackupReply    chan<- error
	BarrierReply   chan<- error
	RetryReply     chan<- error
	RotateReply    chan<- error
	// MasterKey and Passphrase carry the new envelope credential for
	// WorkRotateMasterKey: exactly one must be set. The worker wipes
	// MasterKey bytes after use; Passphrase strings are dropped.
	MasterKey  []byte
	Passphrase string
	StopReply  chan<- error
}

// Opened is an open storage handle owned by the worker thread.
type Opened struct {
	SchemaVersion     uint64
	SnapshotHistory   *SnapshotHistoryCache
	LastReclaim       *time.Time
	Store             *LogicalStore
	ReplicationConfig *replication.Config
	History           []Event
	Clock             Version
	Schema            Schema
	Identity          Identity
	Cursor            uint64
	Sequence          uint64
	Keys              [][]byte
	Opts              spool.Options
}

// SnapshotHistoryCache memoizes the compacted snapshot history.
type SnapshotHistoryCache struct {
	Version uint64
	History []Event
}

// Close closes the store and zeroes the master key.
func (o *Opened) Close() {
	if o.Store != nil {
		_ = o.Store.Close()
	}
	for i := range o.Opts.MasterKey {
		o.Opts.MasterKey[i] = 0
	}
}

// StatusState is the shared database status cell.
type StatusState struct {
	Mu     sync.Mutex
	Status Status
}

func spoolOptions(path string, key []byte, o *Options) spool.Options {
	return spoolOptionsWithCredential(path, KeyCredential(key), o)
}

func spoolOptionsWithCredential(path string, cred Credential, o *Options) spool.Options {
	opts := spool.DefaultOptions(path)
	opts.MasterKey = append([]byte(nil), cred.Key...)
	opts.Passphrase = cred.Passphrase
	opts.TargetBlockBytes = o.SpoolBlockBytes
	opts.ReadCacheBytes = o.SpoolCacheBytes
	opts.Codec = compression.MustZstd(o.CompressionLevel)
	opts.ReclaimInterval = 0
	opts.Workers = o.StorageWorkers
	if o.DataKeyMaxAge != nil {
		opts.DataKeyMaxAge = *o.DataKeyMaxAge
	}
	opts.Durability = spool.DurabilitySync
	opts.MaxSegmentSize = int64(o.SegmentBytes)
	opts.MaxAtomicBatchBytes = 768 << 20
	opts.MaxPendingBytes = 768 << 20
	opts.Faults = o.FaultHooks
	return opts
}

func payloadRecords(prefix string, payload []byte) ([]spool.Mutation, error) {
	if len(payload) > storageMaxPayload {
		return nil, limitErr("serialized record exceeds 512 MiB")
	}
	chunks := (len(payload) + storageChunk - 1) / storageChunk
	if chunks < 1 {
		chunks = 1
	}
	header := &chunkHeader{format: storageFormat, chunks: chunks, length: len(payload), hash: hash(payload)}
	records := []spool.Mutation{{Key: []byte(prefix + "/00000000"),
		Value: encodeOne(func(e *postcard.Encoder) { encodeChunkHeader(e, header) })}}
	for i := 0; i < len(payload); i += storageChunk {
		end := i + storageChunk
		if end > len(payload) {
			end = len(payload)
		}
		records = append(records, spool.Mutation{
			Key:   []byte(fmt.Sprintf("%s/%08d", prefix, len(records))),
			Value: append([]byte(nil), payload[i:end]...),
		})
	}
	return records, nil
}

type assembly struct {
	header     chunkHeader
	payload    []byte
	seen       int
	prefix     string
	chunkBytes int
}

func startAssembly(prefix string, value []byte) (*assembly, error) {
	header, err := decodeOne(value, decodeChunkHeader)
	if err != nil {
		return nil, err
	}
	expect64k := (header.length + storageChunk - 1) / storageChunk
	if expect64k < 1 {
		expect64k = 1
	}
	expect4m := (header.length + (4 << 20) - 1) / (4 << 20)
	if expect4m < 1 {
		expect4m = 1
	}
	if !supportedFormat(header.format) || header.length > storageMaxPayload ||
		(header.chunks != expect64k && header.chunks != expect4m) {
		return nil, corruptErr("invalid chunk descriptor")
	}
	chunkBytes := storageChunk
	if header.chunks != expect64k {
		chunkBytes = 4 << 20
	}
	return &assembly{header: header, prefix: prefix, chunkBytes: chunkBytes}, nil
}

func (a *assembly) push(key string, value []byte) (bool, error) {
	a.seen++
	if key != fmt.Sprintf("%s/%08d", a.prefix, a.seen) || len(value) > a.chunkBytes {
		return false, corruptErr("chunk order")
	}
	a.payload = append(a.payload, value...)
	if len(a.payload) > a.header.length {
		return false, corruptErr("chunk length")
	}
	return a.seen == a.header.chunks, nil
}

func (a *assembly) finish() ([]byte, error) {
	if a.seen != a.header.chunks || len(a.payload) != a.header.length || hash(a.payload) != a.header.hash {
		return nil, corruptErr("incomplete or damaged record")
	}
	return a.payload, nil
}

// RecoverCurrent rebuilds the in-memory SQLite database from durable rows.
func RecoverCurrent(opened *Opened, progress *OpenProgressReporter) (*sqlite.Conn, error) {
	progress.Phase(PhaseRebuilding)
	c, err := sqlite.OpenMemory()
	if err != nil {
		return nil, sqlErr(err)
	}
	if err := RegisterKeyFunctions(c); err != nil {
		c.Close()
		return nil, err
	}
	if err := c.ExecBatch("BEGIN"); err != nil {
		c.Close()
		return nil, sqlErr(err)
	}
	fail := func(err error) (*sqlite.Conn, error) {
		c.Close()
		return nil, err
	}
	insertStart := time.Now()
	for _, table := range opened.Schema.Tables {
		if err := c.ExecBatch(table.SQL); err != nil {
			return fail(sqlErr(err))
		}
	}
	for _, table := range opened.Schema.Tables {
		keys := opened.Store.KeysWithPrefix([]byte("04/meta/row/" + table.ID + "/"))
		if len(keys) == 0 {
			continue
		}
		progress.SetTable(table.Name)
		stmt, err := c.Prepare(RowInsertSQL(&table))
		if err != nil {
			return fail(sqlErr(err))
		}
		for _, key := range keys {
			raw, err := opened.Store.Get(key)
			if err != nil {
				stmt.Close()
				return fail(err)
			}
			if raw == nil {
				stmt.Close()
				return fail(corruptErr("missing row record"))
			}
			meta, err := DecodeRowMeta(raw)
			if err != nil {
				stmt.Close()
				return fail(err)
			}
			if err := meta.ExtendNullable(&table); err != nil {
				stmt.Close()
				return fail(err)
			}
			primary, err := DecodeValues(meta.Key)
			if err != nil {
				stmt.Close()
				return fail(err)
			}
			if !bytes.Equal(RowKey(table.ID, EncodeValues(primary)), key) {
				stmt.Close()
				return fail(corruptErr("metadata key collision"))
			}
			values, err := CellValues(&table, meta.Cells)
			if err != nil {
				stmt.Close()
				return fail(err)
			}
			args := make([]any, len(values))
			for i, v := range values {
				args[i] = sqlArg(v)
			}
			if err := stmt.Bind(args...); err != nil {
				stmt.Close()
				return fail(sqlErr(err))
			}
			if _, err := stmt.Step(); err != nil {
				stmt.Close()
				return fail(sqlErr(err))
			}
			if err := stmt.Reset(); err != nil {
				stmt.Close()
				return fail(sqlErr(err))
			}
			progress.AddRows(1, 0)
		}
		stmt.Close()
	}
	progress.RecordStage(StageRowInsert, time.Since(insertStart))
	progress.Phase(PhaseIndexing)
	indexStart := time.Now()
	for _, sql := range opened.Schema.Indexes {
		if err := c.ExecBatch(sql); err != nil {
			return fail(sqlErr(err))
		}
	}
	if err := c.ExecBatch("COMMIT"); err != nil {
		return fail(sqlErr(err))
	}
	progress.RecordStage(StageIndexBuild, time.Since(indexStart))
	checkStart := time.Now()
	row, err := c.QueryRow("PRAGMA integrity_check")
	if err != nil {
		return fail(sqlErr(err))
	}
	progress.RecordStage(StageIntegrityCheck, time.Since(checkStart))
	if len(row) == 0 || row[0].Str != "ok" {
		check := ""
		if len(row) > 0 {
			check = row[0].Str
		}
		return fail(corruptErr("%s", check))
	}
	return c, nil
}

func sqlArg(v Value) any {
	switch v.Tag {
	case ValueInteger:
		return v.I
	case ValueReal:
		return v.F
	case ValueText:
		return v.S
	case ValueBlob:
		return v.B
	default:
		return nil
	}
}

// OpenStorage opens (or creates) the storage handle.
func OpenStorage(path string, key []byte, o *Options, progress *OpenProgressReporter) (*Opened, error) {
	return OpenStorageWithCredential(path, KeyCredential(key), o, progress)
}

func OpenStorageWithCredential(path string, cred Credential, o *Options, progress *OpenProgressReporter) (*Opened, error) {
	progress.Phase(PhaseOpening)
	if err := cred.validate(); err != nil {
		return nil, err
	}
	switch o.Mode {
	case OpenCreateNew:
		if parent := filepath.Dir(path); parent != "" {
			if err := os.MkdirAll(parent, 0o700); err != nil {
				return nil, ioErr(err)
			}
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			return nil, ioErr(err)
		}
	case OpenExisting:
		info, err := os.Stat(filepath.Join(path, "manifest"))
		if err != nil || info.IsDir() {
			return nil, invalidErr("database storage does not exist")
		}
		if err := spool.ValidateCredential(path, cred.Key, cred.Passphrase, nil, ""); err != nil {
			return nil, keyError(err)
		}
	}
	opts := spoolOptionsWithCredential(path, cred, o)
	var history []Event
	schema := Schema{}
	var identity *Identity
	var head *snapshotHead
	var asm *assembly
	var cursor, sequence uint64
	var keys [][]byte
	snapshotLoaded := false
	spoolStart := time.Now()
	store, err := spool.OpenAndLoadOrdered(opts, func(records []spool.Record) error {
		var count uint64
		for _, r := range records {
			_ = r
			count++
		}
		progress.AddProcessedItems(count)
		for _, r := range records {
			if r.Deleted {
				continue
			}
			key := string(r.Key)
			switch {
			case key == "00/identity":
				ident, err := decodeOne(r.Value, decodeIdentity)
				if err != nil {
					return spoolCorrupt(err)
				}
				if !supportedFormat(ident.Format) {
					return spoolCorrupt(corruptErr("unsupported database format"))
				}
				identCopy := ident
				identity = &identCopy
			case key == "00/schema/replication" || key == "00/schema/table-flags-v2" || key == "00/schema/config-owned":
				// Loaded after replay so later schema events cannot overwrite it.
			case key == "01/snapshot/head":
				h, err := decodeOne(r.Value, decodeSnapshotHead)
				if err != nil {
					return spoolCorrupt(err)
				}
				hCopy := h
				head = &hCopy
			case strings.HasPrefix(key, "02/snapshot/"):
				keys = append(keys, append([]byte(nil), r.Key...))
				if head == nil {
					continue
				}
				prefix := fmt.Sprintf("02/snapshot/%020d", head.sequence)
				if !strings.HasPrefix(key, prefix+"/") {
					continue
				}
				if strings.HasSuffix(key, "/00000000") {
					a, err := startAssembly(prefix, r.Value)
					if err != nil {
						return spoolCorrupt(err)
					}
					asm = a
				} else {
					if asm == nil {
						return spoolCorrupt(corruptErr("snapshot descriptor missing"))
					}
					done, err := asm.push(key, r.Value)
					if err != nil {
						return spoolCorrupt(err)
					}
					if done {
						data, err := asm.finish()
						if err != nil {
							return spoolCorrupt(err)
						}
						asm = nil
						if hash(data) != head.hash {
							return spoolCorrupt(corruptErr("snapshot head hash"))
						}
						snap, err := decodeOne(data, decodeLogSnapshot)
						if err != nil {
							return spoolCorrupt(err)
						}
						if !supportedFormat(snap.format) || snap.sequence != head.sequence || snap.cursor != head.cursor {
							return spoolCorrupt(corruptErr("snapshot boundary"))
						}
						history = snap.history
						schema = Schema{}
						for _, e := range history {
							if e.Tag == EventSchema {
								schema = e.Schema
							}
						}
						cursor = head.cursor
						sequence = head.sequence
						snapshotLoaded = true
					}
				}
			case strings.HasPrefix(key, "03/log/"):
				keys = append(keys, append([]byte(nil), r.Key...))
				if head != nil && !snapshotLoaded {
					return spoolCorrupt(corruptErr("snapshot incomplete"))
				}
				parts := strings.Split(key, "/")
				if len(parts) < 3 {
					return spoolCorrupt(corruptErr("log key"))
				}
				logSeq, err := strconv.ParseUint(parts[2], 10, 64)
				if err != nil {
					return spoolCorrupt(corruptErr("log key"))
				}
				if logSeq <= sequence {
					continue
				}
				prefix := fmt.Sprintf("03/log/%020d", logSeq)
				if strings.HasSuffix(key, "/00000000") {
					if asm != nil {
						return spoolCorrupt(corruptErr("incomplete log"))
					}
					a, err := startAssembly(prefix, r.Value)
					if err != nil {
						return spoolCorrupt(err)
					}
					asm = a
				} else {
					if asm == nil {
						return spoolCorrupt(corruptErr("log descriptor missing"))
					}
					done, err := asm.push(key, r.Value)
					if err != nil {
						return spoolCorrupt(err)
					}
					if done {
						data, err := asm.finish()
						if err != nil {
							return spoolCorrupt(err)
						}
						asm = nil
						entry, err := decodeOne(data, decodeLogEntry)
						if err != nil {
							return spoolCorrupt(err)
						}
						if !supportedFormat(entry.format) || entry.sequence != sequence+1 || entry.sequence != logSeq || entry.cursor < cursor {
							return spoolCorrupt(corruptErr("log sequence gap"))
						}
						if entry.event.Tag == EventSchema {
							schema = entry.event.Schema
						}
						history = append(history, entry.event)
						cursor = entry.cursor
						sequence = entry.sequence
					}
				}
			case strings.HasPrefix(key, "04/meta/row/") ||
				strings.HasPrefix(key, "05/meta/tomb/") ||
				key == "06/clock" ||
				strings.HasPrefix(key, "07/replication/") ||
				strings.HasPrefix(key, "08/files/") ||
				strings.HasPrefix(key, "09/bridge/") ||
				strings.HasPrefix(key, "10/policy/") ||
				strings.HasPrefix(key, "11/blob/") ||
				strings.HasPrefix(key, "12/key-transition/") ||
				strings.HasPrefix(key, "05/meta/deleted/"):
				// Authoritative metadata stays in Spool; do not load its values.
			default:
				return spoolCorrupt(corruptErr("unknown storage record"))
			}
		}
		return nil
	})
	progress.RecordStage(StageSpoolOpen, time.Since(spoolStart))
	if err != nil {
		return nil, keyError(err)
	}
	if asm != nil || (head != nil && !snapshotLoaded) {
		store.Close()
		return nil, corruptErr("incomplete record")
	}
	var ident Identity
	switch {
	case identity != nil:
		ident = *identity
	case o.Mode == OpenCreateNew:
		ident = Identity{Format: storageFormat}
		if o.DatabaseID != nil {
			ident.Database = *o.DatabaseID
		} else if _, err := rand.Read(ident.Database[:]); err != nil {
			store.Close()
			return nil, ioErr(err)
		}
		if o.NodeID != nil {
			ident.Node = *o.NodeID
		} else if _, err := rand.Read(ident.Node[:]); err != nil {
			store.Close()
			return nil, ioErr(err)
		}
		if err := store.Commit([]spool.Mutation{{Key: []byte("00/identity"),
			Value: encodeOne(func(e *postcard.Encoder) { encodeIdentity(e, ident) })}}, spool.DurabilitySync); err != nil {
			store.Close()
			return nil, keyError(err)
		}
	default:
		store.Close()
		return nil, corruptErr("database identity missing")
	}
	if (o.DatabaseID != nil && *o.DatabaseID != ident.Database) ||
		(o.NodeID != nil && *o.NodeID != ident.Node) {
		store.Close()
		return nil, invalidErr("database/node identity mismatch")
	}
	if ident.Format != storageFormat {
		ident.Format = storageFormat
		if err := store.Commit([]spool.Mutation{{Key: []byte("00/identity"),
			Value: encodeOne(func(e *postcard.Encoder) { encodeIdentity(e, ident) })}}, spool.DurabilitySync); err != nil {
			store.Close()
			return nil, keyError(err)
		}
	}
	logical := NewLogicalStore(store)
	var clock Version
	if raw, err := logical.Get([]byte("06/clock")); err != nil {
		store.Close()
		return nil, err
	} else if raw != nil {
		v, err := DecodeVersion(raw)
		if err != nil {
			store.Close()
			return nil, err
		}
		clock = v
	}
	flags, err := logical.Get([]byte(SettingsKey))
	if err != nil {
		store.Close()
		return nil, err
	}
	if flags == nil {
		flags, err = logical.Get([]byte(LegacySettingsKey))
		if err != nil {
			store.Close()
			return nil, err
		}
	}
	if flags != nil {
		decoded, err := DecodeFlags(flags)
		if err != nil {
			store.Close()
			return nil, err
		}
		schema.TableReplication = decoded
	}
	return &Opened{
		Store: logical, ReplicationConfig: o.Replication,
		History: history, Clock: clock, Schema: schema,
		Identity: ident, Cursor: cursor, Sequence: sequence,
		Keys: keys, Opts: opts,
	}, nil
}

func keyError(err error) error {
	switch {
	case err == nil:
		return nil
	case isSpoolErr(err, spool.ErrWrongKey):
		return ErrWrongKey
	case isSpoolErr(err, spool.ErrAuthFailed) || isSpoolErr(err, spool.ErrCorrupt):
		return corruptErr("%v", err)
	default:
		return storageErr("%v", err)
	}
}

func isSpoolErr(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func spoolCorrupt(err error) error {
	return &spoolLoadError{err: err}
}

type spoolLoadError struct{ err error }

func (e *spoolLoadError) Error() string { return e.err.Error() }
func (e *spoolLoadError) Unwrap() error { return spool.ErrCorrupt }

type snapshotProgress struct {
	count int
	total int
}

func persistCheckpoint(opened *Opened, o *Options, status *StatusState, event *Event, cursor uint64, size int, progress *snapshotProgress) error {
	var enabledTable *string
	if event.Tag == EventTableReplication && event.Enabled {
		t := event.Table
		enabledTable = &t
	}
	local := event.Tag == EventDelta || event.Tag == EventSchemaCaptured ||
		(event.Tag == EventReplicaPatch && event.Remint) ||
		(event.Tag == EventTableReplication && event.Enabled) ||
		event.Tag == EventMigrationChunk
	materialized, metadataChanges, clock, err := materialize(opened, event)
	if err != nil {
		return err
	}
	if local && o.Replication != nil {
		schema := &opened.Schema
		if materialized.Tag == EventSchema {
			schema = &materialized.Schema
		}
		var delta *ReplicaState
		if enabledTable != nil {
			delta, err = ChangedState(metadataChanges, *schema, opened.Identity.Database, clock)
			if err != nil {
				return err
			}
			kept := delta.Rows[:0]
			for _, r := range delta.Rows {
				if r.Table == *enabledTable {
					kept = append(kept, r)
				}
			}
			delta.Rows = kept
			keptTombs := delta.Tombstones[:0]
			for _, t := range delta.Tombstones {
				if t.Table == *enabledTable {
					keptTombs = append(keptTombs, t)
				}
			}
			delta.Tombstones = keptTombs
		} else {
			delta, err = ChangedState(metadataChanges, *schema, opened.Identity.Database, clock)
			if err != nil {
				return err
			}
		}
		var auxiliary []replication.Record
		for _, r := range metadataChanges {
			if r.Deleted {
				continue
			}
			if bytes.HasPrefix(r.Key, []byte("10/policy/")) ||
				bytes.HasPrefix(r.Key, []byte("09/bridge/owner/")) ||
				bytes.HasPrefix(r.Key, []byte("09/bridge/shadow/")) ||
				bytes.HasPrefix(r.Key, []byte("09/bridge/provenance/")) ||
				bytes.HasPrefix(r.Key, []byte("08/files/meta/")) {
				auxiliary = append(auxiliary, replication.PutRecord(r.Key, r.Value))
			}
		}
		if enabledTable != nil {
			for _, prefix := range []string{"10/policy/state/" + *enabledTable + "/", "09/bridge/owner/" + *enabledTable + "/"} {
				for _, key := range opened.Store.KeysWithPrefix([]byte(prefix)) {
					if value, err := opened.Store.Get(key); err != nil {
						return err
					} else if value != nil {
						auxiliary = append(auxiliary, replication.PutRecord(key, value))
					}
				}
			}
			for _, key := range opened.Store.KeysWithPrefix([]byte("09/bridge/shadow/")) {
				parts := strings.Split(string(key), "/")
				if len(parts) > 3 && parts[3] == *enabledTable {
					if value, err := opened.Store.Get(key); err != nil {
						return err
					} else if value != nil {
						auxiliary = append(auxiliary, replication.PutRecord(key, value))
					}
				}
			}
		}
		signed, err := SignLocal(opened, o.Replication, *delta, auxiliary)
		if err != nil {
			return err
		}
		metadataChanges = append(metadataChanges, signed...)
	}
	next := opened.Sequence + 1
	if next == 0 {
		return corruptErr("sequence exhausted")
	}
	entry := &logEntry{format: storageFormat, sequence: next, cursor: cursor, event: *materialized}
	if materialized.Tag == EventSchema {
		metadataChanges = append(metadataChanges, spool.Mutation{
			Key: []byte(SettingsKey), Value: EncodeFlags(materialized.Schema.TableReplication)})
	}
	metadataChanges, err = SummarizeTombs(opened.Store, metadataChanges)
	if err != nil {
		return err
	}
	payload := encodeOne(func(e *postcard.Encoder) { encodeLogEntry(e, entry) })
	records, err := payloadRecords(fmt.Sprintf("03/log/%020d", next), payload)
	if err != nil {
		return err
	}
	var logKeys [][]byte
	for _, r := range records {
		logKeys = append(logKeys, r.Key)
	}
	records = append(records, metadataChanges...)
	records = append(records, spool.Mutation{Key: []byte("06/clock"), Value: EncodeVersion(clock)})
	if err := opened.Store.Commit(records, spool.DurabilitySync); err != nil {
		return err
	}
	if materialized.Tag == EventSchema {
		opened.Schema = materialized.Schema
		opened.SchemaVersion++
	}
	opened.Clock = clock
	opened.Keys = append(opened.Keys, logKeys...)
	opened.Sequence = next
	opened.Cursor = cursor
	progress.count++
	progress.total += len(payload)
	status.Mu.Lock()
	status.Status.Durable = cursor
	if status.Status.PendingBytes > size {
		status.Status.PendingBytes -= size
	} else {
		status.Status.PendingBytes = 0
	}
	status.Mu.Unlock()
	if progress.count >= o.SnapshotDeltas || progress.total >= o.SnapshotBytes {
		if err := SnapshotStore(opened); err != nil {
			return err
		}
		*progress = snapshotProgress{}
	}
	return nil
}

// StartStorage runs the checkpoint worker until WorkStop.
func StartStorage(opened *Opened, o *Options, status *StatusState) (*spool.Store, chan<- Work, <-chan struct{}) {
	physical := opened.Store.Physical()
	work := make(chan Work)
	done := make(chan struct{})
	go func() {
		defer close(done)
		progress := &snapshotProgress{}
		var failed *string
		type retained struct {
			event  *Event
			cursor uint64
			size   int
		}
		var queue []retained
		for w := range work {
			switch w.Kind {
			case WorkNetwork:
				w.Network(opened)
			case WorkCheckpoint:
				if failed != nil {
					queue = append(queue, retained{w.Event, w.Cursor, w.Bytes})
					continue
				}
				if err := persistCheckpoint(opened, o, status, w.Event, w.Cursor, w.Bytes, progress); err != nil {
					if w.Cursor > opened.Cursor {
						queue = append(queue, retained{w.Event, w.Cursor, w.Bytes})
					}
					msg := err.Error()
					status.Mu.Lock()
					status.Status.StorageError = &msg
					status.Mu.Unlock()
					failed = &msg
				}
			case WorkRetry:
				err := func() error {
					if failed == nil {
						return nil
					}
					if err := opened.Store.Close(); err != nil {
						return err
					}
					reopen := *o
					reopen.Mode = OpenExisting
					reopenCred := Credential{Key: opened.Opts.MasterKey, Passphrase: opened.Opts.Passphrase}
					replacement, err := OpenStorageWithCredential(string(opened.Opts.Path), reopenCred, &reopen, NewOpenProgressReporter(nil))
					if err != nil {
						return err
					}
					replacement.History = nil
					for i := range opened.Opts.MasterKey {
						opened.Opts.MasterKey[i] = 0
					}
					*opened = *replacement
					status.Mu.Lock()
					status.Status.Durable = opened.Cursor
					status.Mu.Unlock()
					for len(queue) > 0 {
						front := queue[0]
						if front.cursor <= opened.Cursor {
							status.Mu.Lock()
							if status.Status.PendingBytes > front.size {
								status.Status.PendingBytes -= front.size
							} else {
								status.Status.PendingBytes = 0
							}
							status.Mu.Unlock()
							queue = queue[1:]
							continue
						}
						err := persistCheckpoint(opened, o, status, front.event, front.cursor, front.size, progress)
						if front.cursor <= opened.Cursor {
							queue = queue[1:]
						}
						if err != nil {
							return err
						}
					}
					if progress.count >= o.SnapshotDeltas || progress.total >= o.SnapshotBytes {
						if err := SnapshotStore(opened); err != nil {
							return err
						}
						*progress = snapshotProgress{}
					}
					status.Mu.Lock()
					status.Status.StorageError = nil
					status.Mu.Unlock()
					failed = nil
					return nil
				}()
				if err != nil {
					msg := err.Error()
					status.Mu.Lock()
					status.Status.StorageError = &msg
					status.Mu.Unlock()
					failed = &msg
				}
				w.RetryReply <- err
			case WorkRotate:
				if failed != nil {
					w.RotateReply <- storageErr("%s", *failed)
				} else {
					w.RotateReply <- opened.Store.RotateKey()
				}
			case WorkRotateMasterKey:
				var err error
				if failed != nil {
					err = storageErr("%s", *failed)
				} else if rerr := opened.Store.RotateCredential(w.MasterKey, w.Passphrase); rerr != nil {
					err = rerr
				} else {
					for i := range opened.Opts.MasterKey {
						opened.Opts.MasterKey[i] = 0
					}
					opened.Opts.MasterKey = append([]byte(nil), w.MasterKey...)
					opened.Opts.Passphrase = w.Passphrase
				}
				for i := range w.MasterKey {
					w.MasterKey[i] = 0
				}
				w.Passphrase = ""
				w.RotateReply <- err
			case WorkBackup:
				if failed != nil {
					w.BackupReply <- storageErr("%s", *failed)
				} else {
					w.BackupReply <- backupStore(opened, w.Destination)
				}
			case WorkBarrier:
				if failed != nil {
					w.BarrierReply <- storageErr("%s", *failed)
				} else {
					w.BarrierReply <- opened.Store.Flush()
				}
			case WorkExport:
				if failed != nil {
					w.ExportReply <- ExportResult{Err: storageErr("%s", *failed)}
					continue
				}
				state, err := ExportState(opened.Store, opened.Schema, opened.Identity.Database, w.Rows, opened.Clock)
				if err != nil {
					w.ExportReply <- ExportResult{Err: err}
					continue
				}
				if w.ReplicatedOnly {
					FilterState(state, &opened.Schema)
				}
				w.ExportReply <- ExportResult{State: state}
			case WorkMerge:
				if failed != nil {
					w.MergeReply <- MergeResult{Err: storageErr("%s", *failed)}
					continue
				}
				if w.Cursor != opened.Cursor {
					w.MergeReply <- MergeResult{Err: ErrBusy}
					continue
				}
				state := w.State
				FilterState(state, &opened.Schema)
				state.Schema.TableReplication = w.Baseline.Schema.TableReplication
				for _, local := range w.Baseline.Schema.Tables {
					if ReplicationEnabled(&w.Baseline.Schema, local.ID) {
						continue
					}
					found := false
					for i := range state.Schema.Tables {
						if state.Schema.Tables[i].ID == local.ID {
							state.Schema.Tables[i] = local
							found = true
							break
						}
					}
					if !found {
						state.Schema.Tables = append(state.Schema.Tables, local)
					}
				}
				sort.Slice(state.Schema.Tables, func(i, j int) bool {
					return state.Schema.Tables[i].Name < state.Schema.Tables[j].Name
				})
				patch, err := Prepare(w.Baseline, state, w.Cursor)
				w.MergeReply <- MergeResult{Patch: patch, Err: err}
			case WorkStop:
				err := opened.Store.Close()
				if failed != nil {
					w.StopReply <- storageErr("%s", *failed)
				} else {
					w.StopReply <- err
				}
				return
			}
		}
		opened.Close()
	}()
	return physical, work, done
}

// Snapshot reclaim piggyback interval: obsolete keys delete every
// snapshot, but the segment rewrite is deferred.
const snapshotReclaimMinInterval = 60 * time.Second

// SnapshotStore compacts history and reclaims obsolete records.
func SnapshotStore(o *Opened) error {
	var history []Event
	if o.SnapshotHistory != nil && o.SnapshotHistory.Version == o.SchemaVersion {
		history = o.SnapshotHistory.History
	} else {
		read, err := readHistory(o)
		if err != nil {
			return err
		}
		history = compactHistory(read)
		o.SnapshotHistory = &SnapshotHistoryCache{Version: o.SchemaVersion, History: history}
	}
	snap := &logSnapshot{format: storageFormat, sequence: o.Sequence, cursor: o.Cursor, history: history}
	payload := encodeOne(func(e *postcard.Encoder) { encodeLogSnapshot(e, snap) })
	prefix := fmt.Sprintf("02/snapshot/%020d", o.Sequence)
	records, err := payloadRecords(prefix, payload)
	if err != nil {
		return err
	}
	if err := o.Store.Commit(records, spool.DurabilitySync); err != nil {
		return err
	}
	head := &snapshotHead{sequence: o.Sequence, cursor: o.Cursor, chunks: len(records) - 1, hash: hash(payload)}
	if err := o.Store.Commit([]spool.Mutation{{Key: []byte("01/snapshot/head"),
		Value: encodeOne(func(e *postcard.Encoder) { encodeSnapshotHead(e, head) })}}, spool.DurabilitySync); err != nil {
		return err
	}
	current := make(map[string]bool)
	for _, r := range records {
		current[string(r.Key)] = true
	}
	var obsolete [][]byte
	for _, k := range o.Keys {
		if !current[string(k)] {
			obsolete = append(obsolete, k)
		}
	}
	for i := 0; i < len(obsolete); i += 4096 {
		end := i + 4096
		if end > len(obsolete) {
			end = len(obsolete)
		}
		var deletes []spool.Mutation
		for _, k := range obsolete[i:end] {
			deletes = append(deletes, spool.Mutation{Key: k, Deleted: true})
		}
		if err := o.Store.Commit(deletes, spool.DurabilitySync); err != nil {
			return err
		}
	}
	o.Keys = o.Keys[:0]
	for _, r := range records {
		o.Keys = append(o.Keys, r.Key)
	}
	if o.LastReclaim == nil || time.Since(*o.LastReclaim) >= snapshotReclaimMinInterval {
		if err := o.Store.Reclaim(); err != nil {
			return err
		}
		now := time.Now()
		o.LastReclaim = &now
	}
	return nil
}

func backupStore(opened *Opened, dest string) error {
	cp, err := opened.Store.Physical().Checkpoint(context.Background(), dest)
	if err == nil {
		_ = cp.Release()
		return nil
	}
	if strings.Contains(err.Error(), "must be a new directory") {
		return invalidErr("backup destination must be a new directory")
	}
	crossFilesystem := strings.Contains(err.Error(), "link checkpoint segment")
	if !crossFilesystem {
		if isSpoolErr(err, spool.ErrCorrupt) || isSpoolErr(err, spool.ErrAuthFailed) {
			return corruptErr("%v", err)
		}
		return storageErr("%v", err)
	}
	if _, statErr := os.Stat(dest); statErr == nil {
		return storageErr("%v", err)
	}
	return backupCopyFallback(opened, dest)
}

func backupCopyFallback(opened *Opened, dest string) error {
	live := string(opened.Opts.Path)
	parent := filepath.Dir(live)
	if parent == "" {
		parent = "."
	}
	for i := 0; i < 8; i++ {
		var rnd [8]byte
		_, _ = rand.Read(rnd[:])
		tmp := filepath.Join(parent, fmt.Sprintf(".backup-tmp-%d-%x", time.Now().UnixNano(), rnd))
		if _, err := os.Stat(tmp); err == nil {
			continue
		}
		cp, err := opened.Store.Physical().Checkpoint(context.Background(), tmp)
		if err != nil {
			if strings.Contains(err.Error(), "must be a new directory") {
				return invalidErr("backup destination must be a new directory")
			}
			if isSpoolErr(err, spool.ErrCorrupt) || isSpoolErr(err, spool.ErrAuthFailed) {
				return corruptErr("%v", err)
			}
			return storageErr("%v", err)
		}
		result := CopyCheckpointFiles(tmp, cp.Files(), dest)
		_ = cp.Release()
		_ = os.RemoveAll(tmp)
		return result
	}
	return invalidErr("could not allocate backup temporary directory")
}

func materialize(o *Opened, event *Event) (*Event, []spool.Mutation, Version, error) {
	switch event.Tag {
	case EventDelta:
		changes, err := Delta(o.Store, &o.Schema, event.Bytes, event.Version)
		if err != nil {
			return nil, nil, Version{}, err
		}
		if err := HighLowPromote(o, &changes, event.Version); err != nil {
			return nil, nil, Version{}, err
		}
		if err := MaterializePolicyState(o, &changes, event.Intents, event.Version); err != nil {
			return nil, nil, Version{}, err
		}
		return event, changes, o.Clock.Max(event.Version), nil
	case EventSchemaCaptured:
		changes, err := MigrationMutations(o.Store, event.Before, event.After, event.Version)
		if err != nil {
			return nil, nil, Version{}, err
		}
		out := &Event{Tag: EventSchema, Migration: event.Migration, Schema: event.Schema, Version: event.Version}
		return out, changes, o.Clock.Max(event.Version), nil
	case EventReplicaPatch:
		records := event.Records
		if err := MonotonicProgress(o, records); err != nil {
			return nil, nil, Version{}, err
		}
		changes, err := PatchMutations(o.Store, event.Rows, event.Deleted, event.Tombs)
		if err != nil {
			return nil, nil, Version{}, err
		}
		for _, r := range records {
			if r.Value == nil {
				changes = append(changes, spool.Mutation{Key: r.Key, Deleted: true})
			} else {
				changes = append(changes, spool.Mutation{Key: r.Key, Value: r.Value})
			}
		}
		return event, changes, o.Clock.Max(event.Clock), nil
	case EventReplicaRecords:
		changes, err := ReplicaMutations(o.Store, event.State)
		if err != nil {
			return nil, nil, Version{}, err
		}
		for _, r := range event.Records {
			if r.Value == nil {
				changes = append(changes, spool.Mutation{Key: r.Key, Deleted: true})
			} else {
				changes = append(changes, spool.Mutation{Key: r.Key, Value: r.Value})
			}
		}
		out := &Event{Tag: EventReplica, State: event.State}
		return out, changes, o.Clock.Max(event.State.Clock), nil
	case EventReplica:
		changes, err := ReplicaMutations(o.Store, event.State)
		if err != nil {
			return nil, nil, Version{}, err
		}
		return event, changes, o.Clock.Max(event.State.Clock), nil
	case EventTableReplication:
		changes, err := TransitionTable(o.Store, event.Table, event.Enabled, event.Before, event.After, event.Version)
		if err != nil {
			return nil, nil, Version{}, err
		}
		out := &Event{Tag: EventSchema, Schema: event.Schema, Version: event.Version}
		return out, changes, o.Clock.Max(event.Version), nil
	case EventMigrationChunk:
		changes, err := MigrationMutations(o.Store, event.Before, event.After, event.Version)
		if err != nil {
			return nil, nil, Version{}, err
		}
		out := &Event{Tag: EventMigrationChunk, Version: event.Version}
		return out, changes, o.Clock.Max(event.Version), nil
	default:
		return nil, nil, Version{}, corruptErr("schema input lacks captured rows")
	}
}

func readPayload(store *LogicalStore, prefix string) ([]byte, error) {
	h, err := store.Get([]byte(prefix + "/00000000"))
	if err != nil {
		return nil, err
	}
	if h == nil {
		return nil, corruptErr("missing record descriptor")
	}
	asm, err := startAssembly(prefix, h)
	if err != nil {
		return nil, err
	}
	for i := 1; i <= asm.header.chunks; i++ {
		key := fmt.Sprintf("%s/%08d", prefix, i)
		value, err := store.Get([]byte(key))
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, corruptErr("missing record chunk")
		}
		if _, err := asm.push(key, value); err != nil {
			return nil, err
		}
	}
	return asm.finish()
}

func readHistory(o *Opened) ([]Event, error) {
	var history []Event
	var covered uint64
	if raw, err := o.Store.Get([]byte("01/snapshot/head")); err != nil {
		return nil, err
	} else if raw != nil {
		head, err := decodeOne(raw, decodeSnapshotHead)
		if err != nil {
			return nil, err
		}
		data, err := readPayload(o.Store, fmt.Sprintf("02/snapshot/%020d", head.sequence))
		if err != nil {
			return nil, err
		}
		snap, err := decodeOne(data, decodeLogSnapshot)
		if err != nil {
			return nil, err
		}
		history = snap.history
		covered = head.sequence
	}
	for _, key := range o.Store.KeysWithPrefix([]byte("03/log/")) {
		name := string(key)
		if !strings.HasSuffix(name, "/00000000") {
			continue
		}
		prefix := strings.TrimSuffix(name, "/00000000")
		seqStr := prefix[strings.LastIndex(prefix, "/")+1:]
		sequence, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			return nil, corruptErr("log sequence")
		}
		if sequence <= covered {
			continue
		}
		data, err := readPayload(o.Store, prefix)
		if err != nil {
			return nil, err
		}
		entry, err := decodeOne(data, decodeLogEntry)
		if err != nil {
			return nil, err
		}
		history = append(history, entry.event)
	}
	return history, nil
}

func compactHistory(history []Event) []Event {
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Tag == EventSchema {
			return []Event{history[i]}
		}
	}
	return nil
}

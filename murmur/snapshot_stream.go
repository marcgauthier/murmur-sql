package murmur

import (
	"bytes"
	"os"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// SnapshotSource streams snapshot records from an immutable Spool
// checkpoint without holding the writer.
type SnapshotSource struct {
	Checkpoint *spool.Checkpoint
	Options    spool.Options
	Schema     Schema
	Clock      Version
	Identity   [2][16]byte
	Progress   []replication.Progress
	Budget     int
}

// Close releases the checkpoint, removes its directory, and zeroes the key.
func (s *SnapshotSource) Close() {
	if s.Checkpoint != nil {
		_ = s.Checkpoint.Release()
		_ = os.RemoveAll(s.Checkpoint.Dir)
		s.Checkpoint = nil
	}
	for i := range s.Options.MasterKey {
		s.Options.MasterKey[i] = 0
	}
}

// SnapshotPages iterates the checkpoint's snapshot records in pages.
type SnapshotPages struct {
	store  *LogicalStore
	source *SnapshotSource
	prefix int
	after  []byte
	keys   [][]byte
}

var snapshotPrefixes = [][]byte{
	[]byte("04/meta/row/"),
	[]byte("05/meta/tomb/"),
	[]byte("08/files/meta/"),
	[]byte("10/policy/"),
	[]byte("09/bridge/owner/"),
	[]byte("09/bridge/shadow/"),
	[]byte("09/bridge/provenance/"),
}

// OpenSnapshotPages opens the checkpoint store for paged reads.
func OpenSnapshotPages(source *SnapshotSource) (*SnapshotPages, error) {
	// Spool copies the key internally; open with a private copy so
	// concurrent opens never share backing. The source keeps its own
	// copy until Close zeroes it.
	opts := source.Options
	opts.MasterKey = append([]byte(nil), source.Options.MasterKey...)
	store, err := spool.Open(opts)
	for i := range opts.MasterKey {
		opts.MasterKey[i] = 0
	}
	if err != nil {
		return nil, err
	}
	return &SnapshotPages{store: NewLogicalStore(store), source: source}, nil
}

// Source returns the underlying snapshot source.
func (p *SnapshotPages) Source() *SnapshotSource { return p.source }

// Close closes the checkpoint store.
func (p *SnapshotPages) Close() {
	if p.store != nil {
		_ = p.store.Close()
		p.store = nil
	}
}

func (p *SnapshotPages) nextKey() []byte {
	for p.prefix < len(snapshotPrefixes) {
		if len(p.keys) > 0 {
			key := p.keys[0]
			p.keys = p.keys[1:]
			p.after = key
			return key
		}
		p.keys = p.store.KeysPage(snapshotPrefixes[p.prefix], p.after, 256)
		if len(p.keys) == 0 {
			p.prefix++
			p.after = nil
		}
	}
	return nil
}

func (p *SnapshotPages) page() ([]byte, bool, error) {
	for {
		filtered := FilterSchema(&p.source.Schema)
		state := &ReplicaState{DatabaseID: p.source.Identity[0], Schema: filtered, Clock: p.source.Clock}
		var records []replication.Record
		size := 0
		for i := 0; i < 256; i++ {
			key := p.nextKey()
			if key == nil {
				break
			}
			value, err := p.store.Get(key)
			if err != nil {
				return nil, false, err
			}
			if value == nil {
				continue
			}
			size += len(value)
			switch {
			case bytes.HasPrefix(key, []byte("04/meta/row/")):
				parts := bytes.Split(key, []byte("/"))
				if len(parts) < 4 {
					return nil, false, ErrSchemaMismatch
				}
				table := string(parts[3])
				if ReplicationEnabled(&p.source.Schema, table) {
					meta, err := DecodeRowMeta(value)
					if err != nil {
						return nil, false, err
					}
					definition := findSchemaTable(&p.source.Schema, table)
					if definition == nil {
						return nil, false, ErrSchemaMismatch
					}
					if err := meta.ExtendNullable(definition); err != nil {
						return nil, false, err
					}
					rowKey, err := DecodeValues(meta.Key)
					if err != nil {
						return nil, false, err
					}
					state.Rows = append(state.Rows, RowState{Table: table, Key: rowKey, Generation: meta.Generation, Cells: meta.Cells})
				}
			case bytes.HasPrefix(key, []byte("05/meta/tomb/")):
				tomb, err := DecodeTombstone(value)
				if err != nil {
					return nil, false, err
				}
				if ReplicationEnabled(&p.source.Schema, tomb.Table) {
					state.Tombstones = append(state.Tombstones, tomb)
				}
			default:
				records = append(records, replication.PutRecord(key, value))
			}
			if size >= 1<<20 {
				break
			}
		}
		records, err := FilterRecords(records, &p.source.Schema)
		if err != nil {
			return nil, false, err
		}
		if len(state.Rows) == 0 && len(state.Tombstones) == 0 && len(records) == 0 {
			if p.prefix < len(snapshotPrefixes) {
				continue
			}
			return nil, false, nil
		}
		content := &replication.Content{SQLite: EncodeReplicaState(*state), Records: records, Origin: p.source.Identity[1]}
		return replication.EncodeContent(content), true, nil
	}
}

// Next returns the next page, or done=false at the end.
func (p *SnapshotPages) Next() (page []byte, done bool, err error) {
	data, ok, err := p.page()
	if err != nil {
		p.prefix = len(snapshotPrefixes)
		return nil, false, &replication.Error{Kind: replication.KindStorage, Msg: err.Error()}
	}
	if !ok {
		return nil, true, nil
	}
	return data, false, nil
}

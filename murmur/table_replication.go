package murmur

import (
	"sort"
	"strings"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/postcard"
	"github.com/marcgauthier/murmur-sql-go/murmur/replication"
)

// Local schema policy, persisted atomically with the schema in Spool.

const (
	// SettingsKey is the spool key holding table replication flags.
	SettingsKey = "00/schema/table-flags-v2"
	// LegacySettingsKey is the previous flags key, still honored on read.
	LegacySettingsKey = "00/schema/replication"
)

// ReplicationEnabled reports whether table (by stable id) replicates.
// Missing means local-only.
func ReplicationEnabled(schema *Schema, table string) bool {
	return schema.TableReplication[table]
}

// FilterState drops rows and tombstones of locally-disabled tables.
func FilterState(state *ReplicaState, schema *Schema) {
	kept := state.Rows[:0]
	for _, row := range state.Rows {
		if ReplicationEnabled(schema, row.Table) {
			kept = append(kept, row)
		}
	}
	state.Rows = kept
	keptTombs := state.Tombstones[:0]
	for _, tomb := range state.Tombstones {
		if ReplicationEnabled(schema, tomb.Table) {
			keptTombs = append(keptTombs, tomb)
		}
	}
	state.Tombstones = keptTombs
}

// FilterSchema returns a copy of schema with locally-disabled tables
// (and their indexes) removed, for replication payloads and schema
// sync. Local settings stay untouched.
func FilterSchema(schema *Schema) Schema {
	out := *schema
	out.Tables = nil
	for _, t := range schema.Tables {
		if ReplicationEnabled(schema, t.ID) {
			out.Tables = append(out.Tables, t)
		}
	}
	present := make([]string, 0, len(out.Tables))
	for _, t := range out.Tables {
		present = append(present, t.Name)
	}
	out.Indexes = nil
	for _, sql := range schema.Indexes {
		table, ok := IndexTable(sql)
		if !ok {
			// Unrecognized shape: keep and let SQLite validate on adopt.
			out.Indexes = append(out.Indexes, sql)
			continue
		}
		for _, name := range present {
			if asciiEqualFold(name, table) {
				out.Indexes = append(out.Indexes, sql)
				break
			}
		}
	}
	return out
}

func asciiEqualFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// FilterRecords drops policy/bridge records of locally-disabled tables,
// keeping unrelated metadata.
func FilterRecords(records []replication.Record, schema *Schema) ([]replication.Record, error) {
	var out []replication.Record
	for _, record := range records {
		key := string(record.Key)
		var table *string
		if strings.HasPrefix(key, "10/policy/state/") || strings.HasPrefix(key, "09/bridge/owner/") {
			if parts := strings.Split(key, "/"); len(parts) > 3 {
				table = &parts[3]
			}
		} else if strings.HasPrefix(key, "09/bridge/shadow/") {
			if parts := strings.Split(key, "/"); len(parts) > 3 {
				table = &parts[3]
			}
		}
		if table == nil || ReplicationEnabled(schema, *table) {
			out = append(out, record)
		}
	}
	return out, nil
}

// PreserveLocal restores locally-disabled baseline rows into a joined
// state whose control projections must not touch them.
func PreserveLocal(state, baseline *ReplicaState) {
	FilterState(state, &baseline.Schema)
	for _, row := range baseline.Rows {
		if !ReplicationEnabled(&baseline.Schema, row.Table) {
			state.Rows = append(state.Rows, row)
		}
	}
	for _, tomb := range baseline.Tombstones {
		if !ReplicationEnabled(&baseline.Schema, tomb.Table) {
			state.Tombstones = append(state.Tombstones, tomb)
		}
	}
}

// EncodeFlags renders the table replication flags map.
func EncodeFlags(flags map[string]bool) []byte {
	keys := make([]string, 0, len(flags))
	for k := range flags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return encodeOne(func(e *postcard.Encoder) {
		e.Len(len(keys))
		for _, k := range keys {
			e.String(k)
			e.Bool(flags[k])
		}
	})
}

// DecodeFlags parses the table replication flags map.
func DecodeFlags(b []byte) (map[string]bool, error) {
	return decodeOne(b, func(d *postcard.Decoder) map[string]bool {
		n := d.Len()
		out := make(map[string]bool, n)
		for i := 0; i < n; i++ {
			out[d.String()] = d.Bool()
		}
		return out
	})
}

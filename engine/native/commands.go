package native

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/netip"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/engine"
)

func (d *database) Command(ctx context.Context, id uint64, c engine.Command) (engine.Result, error) {
	if c.Operation == "ROTATE KEY" {
		return d.rotate(ctx, c.Arguments)
	}
	if c.Operation == "RESTORE DIRECTORY" {
		return d.restore(ctx, c.Arguments)
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if e := d.ready(ctx); e != nil {
		return engine.Result{}, e
	}
	args := c.Arguments
	out := engine.Result{}
	var err error
	switch c.Operation {
	case "FLUSH":
		out.Tag = "FLUSH"
		err = d.db.Flush()
	case "REFRESH INDEXES":
		out.Tag = "REFRESH"
		err = d.db.RefreshIndexes()
	case "SET TABLE":
		out.Tag = "SET"
		err = d.db.SetReplicationSink(args[0].(string), args[1].(bool), d.flagsSink)
	case "SET COLUMN FTS":
		out.Tag = "SET"
		err = d.db.SetColumnFts(args[0].(string), args[1].(string), args[2].(bool))
	case "SET COLUMN VECTOR":
		out.Tag = "SET"
		var cfg *murmur.VectorColumnConfig
		if args[2].(bool) {
			cfg = &murmur.VectorColumnConfig{Dimensions: int(args[3].(int64)), Distance: murmur.DistL2}
			switch strings.ToLower(args[4].(string)) {
			case "cosine":
				cfg.Distance = murmur.DistCosine
			case "l1":
				cfg.Distance = murmur.DistL1
			}
		}
		err = d.db.SetColumnVector(args[0].(string), args[1].(string), cfg)
	case "SHOW STATUS":
		raw, e := json.Marshal(statusJSON(d.db.Status()))
		err = e
		if e == nil {
			out.Rows = [][]engine.Value{{string(raw)}}
		}
	case "SHOW TABLES REPLICATION":
		var schema *murmur.Schema
		schema, err = d.db.Schema()
		if err == nil {
			named := map[string]bool{}
			ids := map[string]bool{}
			for _, t := range schema.Tables {
				named[t.Name] = schema.TableReplication[t.ID]
				ids[t.ID] = true
			}
			for key, value := range schema.TableReplication {
				if !ids[key] {
					named[key] = value
				}
			}
			names := sortedKeys(named)
			for _, name := range names {
				out.Rows = append(out.Rows, []engine.Value{name, strconv.FormatBool(named[name])})
			}
		}
	case "SHOW COLUMNS FTS", "SHOW COLUMNS VECTOR":
		out.Rows, err = d.searchStatus(c.Operation)
	case "SHOW METRICS":
		out.Rows, err = d.metrics(args)
	case "BACKUP DIRECTORY":
		out.Tag = "BACKUP"
		err = d.db.Backup(args[0].(string))
	case "PEER ADD", "PEER REMOVE", "PEER RETIRE", "PEER SYNC", "SHOW PEERS", "SHOW REPLICATION", "SHOW RETENTION", "RETENTION COLLECT", "BRIDGE RELEASE":
		out, err = d.replicationCommand(c)
	case "POLICY COUNTER_ADD", "POLICY SET_ADD", "POLICY SET_REMOVE", "POLICY MIN", "POLICY MAX":
		if d.db.Replication() == nil {
			return out, userError("55000", "replication is not enabled")
		}
		out.Tag = "POLICY"
		_, err = d.db.Transaction(func(sql *murmur.Sql) (any, error) {
			table, column, key := args[0].(string), args[1].(string), []murmur.Value{murmur.TextValue(args[2].(string))}
			var e error
			switch c.Operation {
			case "POLICY COUNTER_ADD":
				e = sql.CounterAdd(table, key, column, args[3].(int64))
			case "POLICY SET_ADD":
				e = sql.SetAdd(table, key, column, murmur.TextValue(args[3].(string)))
			case "POLICY SET_REMOVE":
				e = sql.SetRemove(table, key, column, murmur.TextValue(args[3].(string)))
			case "POLICY MIN":
				e = sql.Min(table, key, column, policyValue(args[3]))
			case "POLICY MAX":
				e = sql.Max(table, key, column, policyValue(args[3]))
			}
			return nil, e
		})
	default:
		if strings.HasPrefix(c.Operation, "FILE ") {
			return d.fileCommand(ctx, id, c)
		}
		err = failed(c.Operation)
	}
	return out, translate(err)
}
func policyValue(v any) murmur.Value {
	switch v := v.(type) {
	case int64:
		return murmur.IntValue(v)
	case float64:
		return murmur.RealValue(v)
	case string:
		return murmur.TextValue(v)
	}
	return murmur.NullValue()
}
func sat(n uint64) int64 {
	if n > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(n)
}
func nullable(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
func (d *database) replicationCommand(c engine.Command) (engine.Result, error) {
	h := d.db.Replication()
	if h == nil {
		return engine.Result{}, userError("55000", "replication is not enabled")
	}
	out := engine.Result{}
	a := c.Arguments
	var err error
	switch c.Operation {
	case "PEER ADD":
		var addr netip.AddrPort
		addr, err = netip.ParseAddrPort(a[0].(string))
		if err == nil {
			err = h.AddPeer(addr)
		}
		out.Tag = "PEER"
	case "PEER REMOVE", "PEER RETIRE":
		var node [16]byte
		node, err = nodeID(a[0].(string))
		if err == nil {
			if c.Operation == "PEER REMOVE" {
				err = h.RemovePeer(node)
			} else {
				err = h.RetirePeer(node)
			}
		}
		out.Tag = "PEER"
	case "PEER SYNC":
		err = h.ForceSync()
		out.Tag = "PEER"
	case "RETENTION COLLECT":
		var n uint64
		n, err = h.CollectRetention()
		if err == nil {
			out.Rows = [][]engine.Value{{sat(n)}}
		}
	case "SHOW PEERS":
		status := h.Status()
		peers := status.PeersDiagnostics
		sort.Slice(peers, func(i, j int) bool {
			return hex.EncodeToString(peers[i].NodeID[:]) < hex.EncodeToString(peers[j].NodeID[:])
		})
		for _, p := range peers {
			var address, last any
			if p.Address != nil {
				address = p.Address.String()
			}
			if p.LastSeenMillisAgo != nil {
				last = sat(*p.LastSeenMillisAgo)
			}
			out.Rows = append(out.Rows, []engine.Value{hex.EncodeToString(p.NodeID[:]), address, strconv.FormatBool(p.Connected), last, sat(p.SentWatermark), strconv.FormatBool(p.SchemaAgreed), strconv.FormatBool(p.PushMode)})
		}
	case "SHOW REPLICATION":
		s := h.Status()
		var address any
		if s.Address != nil {
			address = s.Address.String()
		}
		out.Rows = [][]engine.Value{{strconv.FormatBool(s.Running), address, int64(s.Peers), int64(s.Members), int64(s.StagedTransactions), sat(s.AppliedTransactions), nullable(s.LastError), sat(d.db.Status().Committed)}}
	case "SHOW RETENTION":
		var s *replication.RetentionStatus
		s, err = h.RetentionStatus()
		if err != nil {
			break
		}
		sort.Slice(s.Peers, func(i, j int) bool {
			return hex.EncodeToString(s.Peers[i].Node[:]) < hex.EncodeToString(s.Peers[j].Node[:])
		})
		for _, p := range s.Peers {
			var origins [][16]byte
			for id := range p.Acknowledged {
				origins = append(origins, id)
			}
			sort.Slice(origins, func(i, j int) bool { return hex.EncodeToString(origins[i][:]) < hex.EncodeToString(origins[j][:]) })
			row := func(origin, seq any) {
				out.Rows = append(out.Rows, []engine.Value{hex.EncodeToString(p.Node[:]), sat(p.LastSeenMillis), strconv.FormatBool(p.Retired), origin, seq, sat(s.CollectedTransactions), sat(s.LastCollectionMillis)})
			}
			if len(origins) == 0 {
				row(nil, nil)
			} else {
				for _, id := range origins {
					row(hex.EncodeToString(id[:]), sat(p.Acknowledged[id]))
				}
			}
		}
	case "BRIDGE RELEASE":
		bridge := h.Bridge()
		if bridge == nil {
			return out, userError("55000", "bridge is not enabled")
		}
		schema, e := d.db.Schema()
		if e != nil {
			return out, e
		}
		var table *murmur.Table
		for i := range schema.Tables {
			if strings.EqualFold(schema.Tables[i].Name, a[0].(string)) {
				table = &schema.Tables[i]
				break
			}
		}
		if table == nil {
			return out, userError("22023", "unknown bridge table")
		}
		keys := murmur.ApplicationColumns(table)
		if len(keys) != 1 || !strings.EqualFold(keys[0].DeclaredType, "TEXT") {
			return out, userError("22023", "bridge release needs a single-TEXT-key table")
		}
		var column *string
		if len(a) == 3 {
			for _, c := range table.Columns {
				if strings.EqualFold(c.Name, a[2].(string)) {
					name := c.Name
					column = &name
					break
				}
			}
			if column == nil {
				return out, userError("22023", "unknown bridge column")
			}
		}
		var released bool
		released, err = bridge.Release(table.Name, murmur.EncodeValues([]murmur.Value{murmur.TextValue(a[1].(string))}), column)
		if err == nil {
			out.Rows = [][]engine.Value{{strconv.FormatBool(released)}}
		}
	}
	return out, err
}
func (d *database) searchStatus(op string) ([][]engine.Value, error) {
	schema, statuses, e := d.db.SchemaWithSearchStatus()
	if e != nil {
		return nil, e
	}
	byName := map[string]murmur.SearchIndexStatus{}
	for _, s := range statuses {
		byName[s.Name] = s
	}
	var rows [][]engine.Value
	for _, t := range schema.Tables {
		for _, c := range t.Columns {
			if c.Name == "id_murmur_primary_key" {
				continue
			}
			row := []engine.Value{t.Name, c.Name}
			name := murmur.SchemaIndexName(&t)
			enabled := c.Fts
			if op == "SHOW COLUMNS VECTOR" {
				enabled = c.Vector != nil
				row = append(row, strconv.FormatBool(enabled))
				if enabled {
					row = append(row, int64(c.Vector.Dimensions), c.Vector.Distance.String())
					name = murmur.VectorIndexName(&t, &c)
				} else {
					row = append(row, nil, nil)
				}
			} else {
				row = append(row, strconv.FormatBool(enabled))
			}
			if enabled {
				s, ok := byName[name]
				if !ok {
					return nil, murmur.ErrSchemaMismatch
				}
				row = append(row, name, strconv.FormatBool(s.Ready), strconv.FormatBool(s.Rebuilding), int64(s.PendingKeys), int64(s.QueuedBytes), nullable(s.Error))
			} else {
				row = append(row, nil, nil, nil, nil, nil, nil)
			}
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i][0] != rows[j][0] {
			return rows[i][0].(string) < rows[j][0].(string)
		}
		return rows[i][1].(string) < rows[j][1].(string)
	})
	return rows, nil
}
func (d *database) metrics(args []engine.Value) ([][]engine.Value, error) {
	target := ""
	if len(args) > 0 {
		target = strings.ToLower(args[0].(string))
	}
	var rows [][]engine.Value
	appendMetrics := func(subsystem string, v any) {
		value := reflect.ValueOf(v)
		if value.Kind() == reflect.Pointer {
			value = value.Elem()
		}
		for i := 0; i < value.NumField(); i++ {
			name := snake(value.Type().Field(i).Name)
			field := value.Field(i)
			var text string
			if name == "compression_ratio" {
				text = fmt.Sprintf("%.4f", field.Float())
			} else {
				text = fmt.Sprint(field.Interface())
			}
			rows = append(rows, []engine.Value{subsystem, name, text})
		}
	}
	if target == "" || target == "sqlite" {
		m, e := d.db.SqliteMetrics()
		if e != nil {
			return nil, e
		}
		appendMetrics("sqlite", m)
	}
	if target == "" || target == "spool" {
		appendMetrics("spool", d.db.SpoolMetrics())
	}
	return rows, nil
}
func snake(s string) string {
	r := []rune(s)
	var b strings.Builder
	for i, c := range r {
		if unicode.IsUpper(c) && i > 0 && (unicode.IsLower(r[i-1]) || unicode.IsDigit(r[i-1]) || i+1 < len(r) && unicode.IsLower(r[i+1])) {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToLower(c))
	}
	return b.String()
}
func statusJSON(s murmur.Status) map[string]any {
	return map[string]any{"search_indexes": s.SearchIndexes, "committed": s.Committed, "durable": s.Durable, "pending_bytes": s.PendingBytes, "storage_error": s.StorageError, "checkpoint_pause": durationJSON(s.CheckpointPause), "closed": s.Closed, "open_progress": s.OpenProgress, "sqlite": s.Sqlite, "spool": jsonFields(reflect.ValueOf(s.Spool)), "scheduler": jsonFields(reflect.ValueOf(s.Scheduler)), "replication": s.Replication}
}
func durationJSON(d time.Duration) any {
	return map[string]any{"secs": uint64(d / time.Second), "nanos": uint32(d % time.Second)}
}
func jsonFields(v reflect.Value) any {
	if !v.IsValid() {
		return nil
	}
	if v.CanInterface() {
		if _, ok := v.Interface().(json.Marshaler); ok {
			return v.Interface()
		}
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return jsonFields(v.Elem())
	case reflect.Struct:
		out := map[string]any{}
		for i := 0; i < v.NumField(); i++ {
			f := v.Type().Field(i)
			if f.PkgPath != "" {
				continue
			}
			out[snake(f.Name)] = jsonFields(v.Field(i))
		}
		return out
	case reflect.Slice, reflect.Array:
		out := make([]any, v.Len())
		for i := range out {
			out[i] = jsonFields(v.Index(i))
		}
		return out
	default:
		if v.CanInterface() {
			return v.Interface()
		}
		return nil
	}
}

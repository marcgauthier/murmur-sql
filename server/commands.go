package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"net/netip"
	"strconv"
	"strings"

	"github.com/marcgauthier/murmur-sql/auth"
	"github.com/marcgauthier/murmur-sql/config"
	"github.com/marcgauthier/murmur-sql/engine"
)

type commandSpec struct {
	operation  string
	args       []token
	types      []uint32
	columns    []engine.Column
	parameters []uint32
}

func cols(def string) []engine.Column {
	if def == "" {
		return nil
	}
	var out []engine.Column
	for _, s := range strings.Fields(def) {
		p := strings.Split(s, ":")
		oid := engine.Text
		if len(p) == 2 {
			switch p[1] {
			case "i":
				oid = engine.Int8
			case "b":
				oid = engine.Bytea
			case "bool":
				oid = engine.Bool
			}
		}
		out = append(out, engine.Column{Name: p[0], OID: oid})
	}
	return out
}

var commandColumns = map[string]string{
	"SHOW TABLES REPLICATION": "table replicated", "SHOW STATUS": "status", "SHOW METRICS": "subsystem metric value",
	"SHOW PEERS":          "node_id address connected last_seen_millis_ago:i sent_watermark:i schema_agreed push_mode lag_txns:i origins_behind:i last_progress_millis_ago:i queue_messages:i queue_available_bytes:i member_state",
	"SHOW HEAD":           "origin applied:i",
	"SHOW REPLICATION":    "running address peers:i members:i staged_transactions:i applied_transactions:i last_error committed:i",
	"SHOW RETENTION":      "node_id last_seen_millis:i retired origin acknowledged:i collected_transactions:i last_collection_millis:i",
	"SHOW COLUMNS FTS":    "table column enabled index_name ready rebuilding pending_keys:i queued_bytes:i error",
	"SHOW COLUMNS VECTOR": "table column enabled dimensions:i distance index_name ready rebuilding pending_keys:i queued_bytes:i error",
	"SHOW KEYS":           "key_id:i state segments:i blocks:i checkpoints:i inflight:bool",
	"PRUNE KEYS":          "pruned_key_id:i",
	"RETENTION COLLECT":   "collected:i", "BRIDGE RELEASE": "released",
	"BACKUP SAVE": "name size:i", "BACKUP LIST": "name size:i modified", "BACKUP UPLOAD FINISH": "name size:i", "BACKUP DOWNLOAD": "offset:i total:i chunk:b",
	"FILE LIST": "cursor", "FILE SEARCH": "cursor", "FILE NEXT": "name size:i modified_ms:i sha256 local:bool", "FILE UPLOAD BEGIN": "upload", "FILE UPLOAD FINISH": "name size:i modified_ms:i sha256 local:bool", "FILE DOWNLOAD": "download name size:i", "FILE DOWNLOAD NEXT": "offset:i total:i chunk:b eof:bool",
}

func parseCommand(sql string) (commandSpec, error) {
	ts, e := tokenize(stripTrivia(sql))
	if e != nil {
		return commandSpec{}, e
	}
	if len(ts) < 2 || !strings.EqualFold(ts[0].value, "MURMUR") {
		return commandSpec{}, syntax("expected MURMUR command")
	}
	ts = ts[1:]
	c := commandSpec{}
	upper := func(i int) string {
		if i >= len(ts) || ts[i].quoted || ts[i].param > 0 {
			return ""
		}
		return strings.ToUpper(ts[i].value)
	}
	take := func(op string, start int) {
		c.operation = op
		c.args = append([]token{}, ts[start:]...)
		c.types = make([]uint32, len(c.args))
		for i := range c.types {
			c.types[i] = engine.Text
		}
	}
	invalid := func() (commandSpec, error) { return c, syntax("invalid MURMUR " + upper(0) + " command") }
	switch upper(0) {
	case "UNLOCK":
		if len(ts) != 3 || upper(1) != "KEY" {
			return invalid()
		}
		take("UNLOCK", 2)
	case "FLUSH":
		if len(ts) != 1 {
			return invalid()
		}
		take("FLUSH", 1)
	case "REFRESH":
		if len(ts) != 2 || upper(1) != "INDEXES" {
			return invalid()
		}
		take("REFRESH INDEXES", 2)
	case "SET":
		switch upper(1) {
		case "TABLE":
			if len(ts) != 5 || upper(3) != "REPLICATED" {
				return invalid()
			}
			take("SET TABLE", 2)
			c.args = []token{ts[2], ts[4]}
			c.types = []uint32{engine.Text, engine.Bool}
		case "COLUMN":
			if len(ts) < 6 || (upper(4) != "FTS" && upper(4) != "VECTOR") {
				return invalid()
			}
			kind := upper(4)
			take("SET COLUMN "+kind, 2)
			c.args = []token{ts[2], ts[3], ts[5]}
			c.types = []uint32{engine.Text, engine.Text, engine.Bool}
			if ts[5].param != 0 {
				return invalid()
			}
			enabled, e := boolean(ts[5].value)
			if e != nil {
				return c, e
			}
			if kind == "FTS" || !enabled {
				if len(ts) != 6 {
					return invalid()
				}
			} else {
				if (len(ts) != 8 && len(ts) != 10) || upper(6) != "DIMENSIONS" {
					return invalid()
				}
				n, e := strconv.ParseInt(ts[7].value, 10, 64)
				if e != nil || n < 1 || n > 8192 {
					return c, sqlError("22023", "vector dimensions must be from 1 to 8192")
				}
				c.args = append(c.args, ts[7])
				c.types = append(c.types, engine.Int8)
				distance := token{value: "l2"}
				if len(ts) == 10 {
					if upper(8) != "DISTANCE" {
						return invalid()
					}
					distance = ts[9]
					switch strings.ToLower(distance.value) {
					case "l1", "l2", "cosine":
					default:
						return c, sqlError("22023", "invalid vector distance")
					}
				}
				c.args = append(c.args, distance)
				c.types = append(c.types, engine.Text)
			}
		default:
			return invalid()
		}
	case "SHOW":
		switch upper(1) {
		case "TABLES":
			if len(ts) != 3 || upper(2) != "REPLICATION" {
				return invalid()
			}
			take("SHOW TABLES REPLICATION", 3)
		case "COLUMNS":
			if len(ts) != 3 || (upper(2) != "FTS" && upper(2) != "VECTOR") {
				return invalid()
			}
			take("SHOW COLUMNS "+upper(2), 3)
		case "METRICS":
			if len(ts) > 3 || (len(ts) == 3 && upper(2) != "SQLITE" && upper(2) != "SPOOL") {
				return invalid()
			}
			take("SHOW METRICS", 2)
		case "STATUS", "PEERS", "REPLICATION", "RETENTION", "HEAD", "KEYS":
			if len(ts) != 2 {
				return invalid()
			}
			take("SHOW "+upper(1), 2)
		default:
			return invalid()
		}
	case "ROTATE":
		if upper(1) == "DATA" && upper(2) == "KEY" && len(ts) == 3 {
			take("ROTATE DATA KEY", 3)
			break
		}
		if upper(1) != "KEY" {
			return invalid()
		}
		take("ROTATE KEY", 2)
		if len(c.args) == 3 && c.args[1].value == "," {
			c.args = append(c.args[:1], c.args[2])
			c.types = c.types[:2]
		}
		if len(c.args) != 2 {
			return invalid()
		}
	case "RESTORE":
		if len(ts) != 5 || upper(1) != "FROM" || upper(3) != "KEY" {
			return invalid()
		}
		take("RESTORE", 2)
		c.args = []token{ts[2], ts[4]}
		c.types = []uint32{engine.Text, engine.Text}
	case "PEER":
		switch upper(1) {
		case "SYNC":
			if len(ts) != 2 {
				return invalid()
			}
			take("PEER SYNC", 2)
		case "ADD", "REMOVE", "RETIRE":
			if len(ts) != 3 {
				return invalid()
			}
			take("PEER "+upper(1), 2)
		default:
			return invalid()
		}
	case "RETENTION":
		if len(ts) != 2 || upper(1) != "COLLECT" {
			return invalid()
		}
		take("RETENTION COLLECT", 2)
	case "PRUNE":
		if len(ts) != 2 || upper(1) != "KEYS" {
			return invalid()
		}
		take("PRUNE KEYS", 2)
	case "WAIT":
		if upper(1) != "FOR" {
			return invalid()
		}
		if upper(2) == "LAG" {
			if len(ts) != 4 && (len(ts) != 6 || upper(4) != "TIMEOUT") {
				return invalid()
			}
			take("WAIT FOR LAG", 3)
			if len(ts) == 6 {
				c.args = []token{ts[3], ts[5]}
				c.types = c.types[:2]
			}
			c.types[0] = engine.Int8
			if len(c.types) == 2 {
				c.types[1] = engine.Int8
			}
			break
		}
		if len(ts) != 4 && (len(ts) != 6 || upper(4) != "TIMEOUT") {
			return invalid()
		}
		take("WAIT FOR", 2)
		if len(ts) == 6 {
			c.args = []token{ts[2], ts[3], ts[5]}
			c.types = c.types[:3]
		}
		c.types[1] = engine.Int8
		if len(c.types) == 3 {
			c.types[2] = engine.Int8
		}
	case "POLICY":
		switch upper(1) {
		case "COUNTER_ADD", "SET_ADD", "SET_REMOVE", "MIN", "MAX", "JSON_SET", "JSON_REMOVE", "LIST_APPEND", "LIST_REMOVE":
		default:
			return invalid()
		}
		op := upper(1)
		legacy := op != "JSON_SET" && op != "JSON_REMOVE" && len(ts) == 6
		if legacy {
			take("POLICY "+op, 2)
		} else if len(ts) >= 8 && upper(4) == "KEY" && upper(len(ts)-2) == "VALUE" && op != "JSON_REMOVE" {
			// Composite form: KEY <k1> [<k2>...] [PATH <p>] VALUE
			// <v>. The markers are bare keywords, so quoted
			// 'KEY'/'VALUE'/'PATH' still parse as key material.
			// The engine resolves key arity from the table
			// schema. JSON_SET alone takes PATH, fixed at the
			// fourth position from the end.
			args := append([]token{ts[2], ts[3]}, append([]token{}, ts[5:len(ts)-2]...)...)
			if op == "JSON_SET" {
				if len(ts) < 10 || upper(len(ts)-4) != "PATH" {
					return invalid()
				}
				args = append(args[:len(args)-2], args[len(args)-1])
			}
			c.operation = "POLICY " + op
			c.args = append(args, ts[len(ts)-1])
			c.types = make([]uint32, len(c.args))
			for i := range c.types {
				c.types[i] = engine.Text
			}
		} else if op == "JSON_REMOVE" && len(ts) >= 8 && upper(4) == "KEY" && upper(len(ts)-2) == "PATH" {
			// Removal form: KEY <k1> [<k2>...] PATH <p>.
			c.operation = "POLICY JSON_REMOVE"
			c.args = append(append([]token{ts[2], ts[3]}, ts[5:len(ts)-2]...), ts[len(ts)-1])
			c.types = make([]uint32, len(c.args))
			for i := range c.types {
				c.types[i] = engine.Text
			}
		} else {
			return invalid()
		}
		if op == "COUNTER_ADD" {
			c.types[len(c.types)-1] = engine.Int8
		}
	case "BRIDGE":
		if upper(1) != "RELEASE" || (len(ts) != 4 && len(ts) != 6) || (len(ts) == 6 && upper(4) != "COLUMN") {
			return invalid()
		}
		take("BRIDGE RELEASE", 2)
		if len(ts) == 6 {
			c.args = []token{ts[2], ts[3], ts[5]}
			c.types = c.types[:3]
		}
	case "BACKUP":
		switch upper(1) {
		case "SAVE", "LIST":
			if len(ts) != 2 {
				return invalid()
			}
			take("BACKUP "+upper(1), 2)
		case "UPLOAD":
			if len(ts) < 4 {
				return invalid()
			}
			n := 4
			switch upper(2) {
			case "BEGIN", "FINISH", "ABORT":
			case "APPEND":
				n = 5
			default:
				return invalid()
			}
			if len(ts) != n {
				return invalid()
			}
			take("BACKUP UPLOAD "+upper(2), 3)
		case "DOWNLOAD":
			if len(ts) < 3 {
				return invalid()
			}
			take("BACKUP DOWNLOAD", 3)
			c.args = []token{ts[2], {value: "0"}, {value: strconv.Itoa(1 << 20)}}
			c.types = []uint32{engine.Text, engine.Int8, engine.Int8}
			seen := map[string]bool{}
			for i := 3; i < len(ts); i += 2 {
				if i+1 >= len(ts) {
					return invalid()
				}
				k := upper(i)
				if seen[k] || (k != "OFFSET" && k != "LIMIT") {
					return invalid()
				}
				seen[k] = true
				j := 1
				if k == "LIMIT" {
					j = 2
				}
				c.args[j] = ts[i+1]
			}
		default:
			return invalid()
		}
	case "FILE":
		op := upper(1)
		start := 2
		if op == "UPLOAD" {
			op += " " + upper(2)
			start = 3
		} else if op == "DOWNLOAD" && upper(2) == "NEXT" {
			op += " NEXT"
			start = 3
		}
		n := 1
		switch op {
		case "LIST":
			n = 0
		case "SEARCH", "NEXT", "CLOSE", "UPLOAD BEGIN", "UPLOAD FINISH", "UPLOAD ABORT", "DOWNLOAD", "DOWNLOAD NEXT":
		case "UPLOAD APPEND":
			n = 2
		default:
			return invalid()
		}
		if len(ts) < start+n {
			return invalid()
		}
		take("FILE "+op, start)
		c.args = c.args[:n]
		c.types = c.types[:n]
		if n == 2 {
			c.types[1] = engine.Bytea
		}
		for _, a := range c.args {
			if !a.quoted && a.param == 0 {
				return invalid()
			}
		}
		if op == "NEXT" || op == "DOWNLOAD NEXT" {
			lim := "65536"
			if op == "NEXT" {
				lim = "256"
			}
			if len(ts) > start+n {
				if len(ts) != start+n+2 || upper(start+n) != "LIMIT" {
					return invalid()
				}
				lim = ts[start+n+1].value
			}
			c.args = append(c.args, token{value: lim})
			c.types = append(c.types, engine.Int8)
		} else if len(ts) != start+n {
			return invalid()
		}
	default:
		return invalid()
	}
	if !strings.HasPrefix(c.operation, "FILE ") {
		for _, arg := range c.args {
			if arg.param > 0 {
				return c, syntax("MURMUR commands take no parameters")
			}
		}
	}
	c.columns = cols(commandColumns[c.operation])
	for i, a := range c.args {
		if a.param == 0 {
			continue
		}
		if a.param > 64 {
			return c, sqlError("22023", "command parameter index exceeds 64")
		}
		for len(c.parameters) < a.param {
			c.parameters = append(c.parameters, 0)
		}
		existing := c.parameters[a.param-1]
		if existing != 0 && existing != c.types[i] {
			return c, sqlError("42P08", "inconsistent command parameter types")
		}
		c.parameters[a.param-1] = c.types[i]
	}
	for _, p := range c.parameters {
		if p == 0 {
			return c, syntax("command parameters must be contiguous")
		}
	}
	return c, nil
}
func (c commandSpec) values(params []engine.Value) ([]engine.Value, error) {
	if len(params) != len(c.parameters) {
		return nil, sqlError("08P01", "wrong number of command parameters")
	}
	out := make([]engine.Value, len(c.args))
	for i, a := range c.args {
		var v any = a.value
		if a.param > 0 {
			v = params[a.param-1]
			if v == nil {
				return nil, sqlError("22023", "command argument cannot be NULL")
			}
		}
		switch c.types[i] {
		case engine.Int8:
			if n, ok := v.(int64); ok {
				out[i] = n
			} else {
				s, ok := v.(string)
				if !ok {
					return nil, sqlError("22023", "expected an integer")
				}
				n, e := strconv.ParseInt(s, 10, 64)
				if e != nil {
					return nil, sqlError("22023", "expected an integer")
				}
				out[i] = n
			}
		case engine.Bool:
			if b, ok := v.(bool); ok {
				out[i] = b
			} else {
				s, ok := v.(string)
				if !ok {
					return nil, sqlError("22023", "expected a boolean")
				}
				b, e := boolean(s)
				if e != nil {
					return nil, e
				}
				out[i] = b
			}
		case engine.Bytea:
			if b, ok := v.([]byte); ok {
				out[i] = b
			} else {
				s, ok := v.(string)
				if !ok || !strings.HasPrefix(s, "\\x") {
					return nil, sqlError("22023", "expected hex BYTEA")
				}
				b, e := hex.DecodeString(s[2:])
				if e != nil {
					return nil, sqlError("22023", "invalid hex BYTEA")
				}
				out[i] = b
			}
		default:
			s, ok := v.(string)
			if !ok {
				return nil, sqlError("22023", "expected a text argument")
			}
			out[i] = s
		}
	}
	return out, nil
}

// coercePolicyLiteral renders a typeless policy literal: integers,
// then finite reals, else the text stands.
func coercePolicyLiteral(args []engine.Value, i int) error {
	v := args[i].(string)
	if n, e := strconv.ParseInt(v, 10, 64); e == nil {
		args[i] = n
		return nil
	}
	f, e := strconv.ParseFloat(v, 64)
	if e != nil {
		return nil
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return sqlError("22023", "policy value must be finite")
	}
	args[i] = f
	return nil
}

// packSecret encodes an unlock secret for the engine: raw keys as key
// bytes, passphrases as text. The engine selects the envelope KDF.
func packSecret(secret string) engine.Value {
	if auth.IsRawKey(secret) {
		key := auth.ResolveKey(secret)
		out := append([]byte{}, key[:]...)
		clear(key[:])
		return out
	}
	return secret
}

// clearSecret wipes a packed raw key; passphrases are immutable strings
// and are dropped by the caller instead.
func clearSecret(v engine.Value) {
	if b, ok := v.([]byte); ok {
		clear(b)
	}
}

func (c commandSpec) execute(ctx context.Context, s *session, params []engine.Value) (engine.Result, error) {
	args, e := c.values(params)
	if e != nil {
		return engine.Result{}, e
	}
	tag := strings.Fields(c.operation)[0]
	result := engine.Result{Tag: tag}
	if c.operation == "UNLOCK" {
		secret := args[0].(string)
		if secret == "" {
			return result, sqlError("28000", "MURMUR UNLOCK needs a non-empty key")
		}
		var key [32]byte
		var passphrase string
		if auth.IsRawKey(secret) {
			key = auth.ResolveKey(secret)
		} else {
			passphrase = secret
		}
		defer clear(key[:])
		if e = s.server.unlock(ctx, key, passphrase); e != nil {
			return result, e
		}
		s.unlocked = true
		return result, nil
	}
	if !s.unlocked {
		return result, locked()
	}
	db, e := s.server.database()
	if e != nil {
		return result, e
	}
	switch c.operation {
	case "PEER ADD":
		if _, e = netip.ParseAddrPort(args[0].(string)); e != nil {
			return result, sqlError("22023", "invalid peer address")
		}
	case "PEER REMOVE", "PEER RETIRE":
		if _, e = config.ParseHex(args[0].(string), 16); e != nil {
			return result, sqlError("22023", "invalid node ID")
		}
	case "ROTATE KEY":
		if args[0] == "" {
			return result, sqlError("28000", "previous key must be non-empty")
		}
		if args[1] == "" {
			return result, sqlError("22023", "new key must be non-empty")
		}
		if args[0].(string) == args[1].(string) {
			return result, sqlError("22023", "new key must differ from previous key")
		}
		args = []engine.Value{packSecret(args[0].(string)), packSecret(args[1].(string))}
		defer clearSecret(args[0])
		defer clearSecret(args[1])
	case "RESTORE":
		if args[1] == "" {
			return result, sqlError("28000", "restore key must be non-empty")
		}
		secret := args[1].(string)
		var key [32]byte
		var passphrase string
		if auth.IsRawKey(secret) {
			key = auth.ResolveKey(secret)
		} else {
			passphrase = secret
		}
		if e = db.Unlock(ctx, key, passphrase); e != nil {
			clear(key[:])
			return result, e
		}
		args[1] = packSecret(secret)
		clear(key[:])
		defer clearSecret(args[1])
	case "POLICY MIN", "POLICY MAX":
		if e := coercePolicyLiteral(args, len(args)-1); e != nil {
			return result, e
		}
	case "POLICY JSON_SET", "POLICY LIST_APPEND", "POLICY LIST_REMOVE":
		// JSON and list values keep quoted text textual so
		// '{"a":"123"}' is expressible; only bare literals coerce
		// to numbers, and a bare NULL writes null. Appends and
		// removes coerce identically so removes match.
		last := c.args[len(args)-1]
		if !last.quoted && strings.EqualFold(last.value, "NULL") {
			args[len(args)-1] = nil
		} else if !last.quoted {
			if e := coercePolicyLiteral(args, len(args)-1); e != nil {
				return result, e
			}
		}
	case "BACKUP DOWNLOAD":
		if args[1].(int64) < 0 || args[2].(int64) <= 0 || args[2].(int64) > 16<<20 {
			return result, sqlError("22023", "invalid backup offset or chunk limit")
		}
	case "FILE NEXT", "FILE DOWNLOAD NEXT":
		lim := args[len(args)-1].(int64)
		max := int64(1 << 20)
		if c.operation == "FILE NEXT" {
			max = 1000
		}
		if lim <= 0 || lim > max {
			return result, sqlError("22023", "file page/chunk limit is out of range")
		}
	}
	if strings.HasPrefix(c.operation, "BACKUP ") || c.operation == "RESTORE" {
		return s.server.backupCommand(ctx, s.id, db, c.operation, args)
	}
	result, e = db.Command(ctx, s.id, engine.Command{Operation: c.operation, Arguments: args})
	if e != nil {
		return result, e
	}
	if result.Tag == "" && len(c.columns) == 0 {
		result.Tag = tag
	}
	if len(c.columns) > 0 {
		if len(result.Columns) == 0 {
			result.Columns = c.columns
		}
		if !sameColumns(result.Columns, c.columns) {
			return result, fmt.Errorf("engine command result does not match its description")
		}
	}
	if strings.HasPrefix(c.operation, "SET ") || c.operation == "RESTORE" {
		s.server.generation.Add(1)
	}
	return result, nil
}
func locked() error {
	return sqlError("28000", "connection is locked; send MURMUR UNLOCK KEY '<key>' first")
}
func sameColumns(a, b []engine.Column) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

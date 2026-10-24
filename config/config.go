package config

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/marcgauthier/murmur-sql/keyprovider"
	"gopkg.in/yaml.v3"
)

type Duration time.Duration
type Bytes uint64

func quantity(s string, units map[string]uint64) (uint64, error) {
	s = strings.TrimSpace(s)
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("invalid quantity %q", s)
	}
	n, e := strconv.ParseUint(s[:i], 10, 64)
	if e != nil {
		return 0, e
	}
	m, ok := units[strings.ToLower(strings.TrimSpace(s[i:]))]
	if !ok {
		return 0, fmt.Errorf("unknown unit in %q", s)
	}
	if n > math.MaxUint64/m {
		return 0, fmt.Errorf("quantity overflows: %q", s)
	}
	return n * m, nil
}
func ParseBytes(s string) (Bytes, error) {
	n, e := quantity(s, map[string]uint64{"": 1, "b": 1, "k": 1000, "kb": 1000, "kib": 1024, "m": 1000000, "mb": 1000000, "mib": 1 << 20, "g": 1000000000, "gb": 1000000000, "gib": 1 << 30})
	return Bytes(n), e
}
func ParseDuration(s string) (Duration, error) {
	n, e := quantity(s, map[string]uint64{"": 1e9, "s": 1e9, "sec": 1e9, "secs": 1e9, "second": 1e9, "seconds": 1e9, "ms": 1e6, "milli": 1e6, "millis": 1e6, "m": 60e9, "min": 60e9, "mins": 60e9, "minute": 60e9, "minutes": 60e9, "h": 3600e9, "hour": 3600e9, "hours": 3600e9, "d": 86400e9, "day": 86400e9, "days": 86400e9})
	if e == nil && n > math.MaxInt64 {
		e = errors.New("duration overflows")
	}
	return Duration(n), e
}
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if len(b) > 0 && b[0] == '"' {
		if e := json.Unmarshal(b, &s); e != nil {
			return e
		}
	} else {
		s = string(b)
	}
	v, e := ParseDuration(s)
	if e == nil {
		*d = v
	}
	return e
}
func (b *Bytes) UnmarshalJSON(raw []byte) error {
	var s string
	if len(raw) > 0 && raw[0] == '"' {
		if e := json.Unmarshal(raw, &s); e != nil {
			return e
		}
	} else {
		s = string(raw)
	}
	v, e := ParseBytes(s)
	if e == nil {
		*b = v
	}
	return e
}
func (d Duration) MarshalJSON() ([]byte, error) {
	if d < 0 || time.Duration(d)%time.Millisecond != 0 {
		return nil, errors.New("configuration durations must be nonnegative whole milliseconds")
	}
	for _, u := range []struct {
		duration time.Duration
		suffix   string
	}{{24 * time.Hour, "d"}, {time.Hour, "h"}, {time.Minute, "m"}, {time.Second, "s"}, {time.Millisecond, "ms"}} {
		if time.Duration(d) >= u.duration && time.Duration(d)%u.duration == 0 {
			return json.Marshal(fmt.Sprintf("%d%s", time.Duration(d)/u.duration, u.suffix))
		}
	}
	return json.Marshal("0ms")
}
func (b Bytes) MarshalJSON() ([]byte, error) { return json.Marshal(fmt.Sprintf("%dB", b)) }

func Load(path string) (Config, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return Config{}, e
	}
	return Parse(b)
}
func Parse(b []byte) (Config, error) {
	var m map[string]any
	if e := yaml.Unmarshal(b, &m); e != nil {
		return Config{}, e
	}
	if m == nil {
		m = map[string]any{}
	}
	c := DefaultConfig()
	// Optional nested sections must receive their own defaults before decoding.
	if r, ok := m["replication"].(map[string]any); ok {
		if _, ok = r["files"]; ok {
			v := DefaultReplicationFilesConfig()
			c.Replication.Files = &v
		}
		if _, ok = r["bridge"]; ok {
			v := DefaultReplicationBridgeConfig()
			c.Replication.Bridge = &v
		}
	}
	if e := exactKeys(m, reflect.TypeOf(c), ""); e != nil {
		return c, e
	}
	raw, e := json.Marshal(m)
	if e != nil {
		return c, e
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if e = dec.Decode(&c); e != nil {
		return c, e
	}
	return c, c.Validate()
}
func ParseHex(s string, n int) ([]byte, error) {
	b, e := hex.DecodeString(strings.TrimSpace(s))
	if e != nil || len(b) != n {
		return nil, fmt.Errorf("expected %d bytes of hex", n)
	}
	return b, nil
}
func pointer(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
func nonempty(s string) bool { return strings.TrimSpace(s) != "" }
func socket(s string) error  { _, e := netip.ParseAddrPort(s); return e }

// socketOrName validates host:port where host is an IP literal or DNS
// name. It never resolves: offline validation must not need DNS.
func socketOrName(s string) error {
	if _, e := netip.ParseAddrPort(s); e == nil {
		return nil
	}
	host, port, e := net.SplitHostPort(s)
	if e != nil {
		return e
	}
	if !ValidDNSName(host) {
		return fmt.Errorf("invalid host %q", host)
	}
	p, e := strconv.Atoi(port)
	if e != nil || p < 1 || p > 65535 {
		return fmt.Errorf("invalid port %q", port)
	}
	return nil
}

// ValidDNSName reports whether host is a plausible DNS name: dot-separated
// labels of letters, digits, hyphens, and underscores.
func ValidDNSName(host string) bool {
	if len(host) == 0 || len(host) > 253 {
		return false
	}
	name := strings.TrimSuffix(host, ".")
	if name == "" {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
				continue
			}
			return false
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
	}
	return true
}
func (c Config) Validate() error {
	bad := func(s string) error { return errors.New(s) }
	if e := socket(c.Server.Bind); e != nil {
		return fmt.Errorf("server.bind: %w", e)
	}
	if c.Server.MaxConnections == 0 {
		return bad("server.max_connections must be positive")
	}
	if !nonempty(c.Storage.DataDir) || !nonempty(c.Backups.Dir) {
		return bad("storage.data_dir and backups.dir must not be empty")
	}
	for _, id := range []*string{c.Storage.DatabaseID, c.Storage.NodeID} {
		if id != nil {
			if _, e := ParseHex(*id, 16); e != nil {
				return e
			}
		}
	}
	t := c.Server.TLS
	if !c.Server.Production && nonempty(pointer(t.CaFile)) {
		return bad("server.tls.ca_file requires server.production")
	}
	if c.Server.Production && (!t.Enabled || !nonempty(pointer(t.CaFile))) {
		return bad("production mode requires TLS with client certificates")
	}
	if t.Enabled && (!nonempty(pointer(t.CertFile)) || !nonempty(pointer(t.KeyFile))) {
		return bad("server.tls.enabled requires cert_file and key_file")
	}
	users := map[string]bool{}
	for _, u := range c.Auth.Users {
		if !nonempty(u.Name) || users[u.Name] {
			return bad("empty or duplicate auth user")
		}
		users[u.Name] = true
		if !strings.HasPrefix(u.PasswordHash, "$argon2") {
			return bad("auth users require Argon2 password hashes")
		}
	}
	e := c.Engine
	if e.CaptureInterval <= 0 || e.CaptureBytes == 0 || e.QueueBytes == 0 || e.TransactionBytes == 0 || e.SegmentBytes == 0 || e.SpoolBlockBytes == 0 || e.SnapshotBytes == 0 || e.SnapshotDeltas == 0 || e.StorageWorkers == 0 {
		return bad("engine limits must be positive")
	}
	if e.QueueBytes < e.CaptureBytes || e.TransactionBytes > e.QueueBytes || e.QueueBytes > 128<<20 || e.SpoolBlockBytes > 64<<20 || e.SegmentBytes <= 64<<20 {
		return bad("invalid worker limits (queue maximum 128 MiB)")
	}
	if uint64(e.Scheduler.LocalShare)+uint64(e.Scheduler.RemoteShare) != 100 {
		return bad("writer shares must total 100")
	}
	if e.DataKeyMaxAge != nil && *e.DataKeyMaxAge <= 0 {
		return bad("engine.data_key_max_age must be positive")
	}
	if e.Scheduler.LocalShare == 0 || e.Scheduler.RemoteShare == 0 || e.Scheduler.MaxDebt <= 0 || e.Scheduler.MaintenanceReserveEvery == 0 {
		return bad("invalid engine scheduler")
	}
	if err := c.Replication.Validate(); err != nil {
		return err
	}
	l := c.Logging
	if !nonempty(l.Dir) || !nonempty(l.FilePrefix) || strings.ContainsAny(l.FilePrefix, "/\\") || l.Rotation != "daily" {
		return bad("invalid logging directory, prefix, or rotation")
	}
	switch strings.ToLower(l.Level) {
	case "error", "warn", "info", "debug", "trace":
	default:
		return bad("invalid logging.level")
	}
	v := l.Victorialogs
	if v.Enabled {
		u, e := url.Parse(v.URL)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return bad("invalid VictoriaLogs URL")
		}
		if v.BearerToken != nil && (v.Username != nil || v.Password != nil) {
			return bad("VictoriaLogs requires either bearer or basic authentication")
		}
	}
	return nil
}
func (r ReplicationConfig) Validate() error {
	if e := socket(r.Bind); e != nil {
		return fmt.Errorf("replication.bind: %w", e)
	}
	if r.Advertise != nil {
		if e := socketOrName(*r.Advertise); e != nil {
			return fmt.Errorf("replication.advertise %q: %w", *r.Advertise, e)
		}
	}
	for _, s := range r.Bootstrap {
		if e := socketOrName(s); e != nil {
			return fmt.Errorf("replication.bootstrap %q: %w", s, e)
		}
	}
	for _, s := range r.AllowedNetworks {
		if _, e := netip.ParsePrefix(s); e != nil {
			return e
		}
	}
	for _, s := range append(append([]string{}, r.AllowedPeers...), r.TrustedSnapshotSources...) {
		if _, e := ParseHex(s, 16); e != nil {
			return e
		}
	}
	if r.SchemaMaster != nil {
		if _, e := ParseHex(*r.SchemaMaster, 16); e != nil {
			return e
		}
	}
	if r.Master && r.SchemaMaster == nil {
		return errors.New("replication.master requires schema_master")
	}
	if !strings.EqualFold(r.Dissemination, "gossip") && !strings.EqualFold(r.Dissemination, "plumtree") {
		return errors.New("unknown replication.dissemination")
	}
	if e := validateKeys(r.TrustedOrigins); e != nil {
		return e
	}
	for k, p := range r.MergePolicies {
		if !strings.Contains(k, ".") {
			return errors.New("merge policy key must be table.column")
		}
		switch strings.ToLower(p) {
		case "lww", "counter", "set", "min", "max", "json", "list":
		default:
			return errors.New("unknown merge policy")
		}
	}
	for k := range r.Tables {
		if !nonempty(k) {
			return errors.New("empty replication table ID")
		}
	}
	if r.Enabled {
		if !nonempty(pointer(r.CertChainFile)) || !nonempty(pointer(r.PrivateKeyFile)) || !nonempty(pointer(r.CaFile)) {
			return errors.New("replication requires TLS credential files")
		}
		sources := 0
		for _, s := range []*string{r.SigningKeyHex, r.SigningKeyFile, r.SigningKeyURI} {
			if s != nil {
				sources++
			}
		}
		if sources != 1 {
			return errors.New("set exactly one replication signing key source")
		}
		if r.SigningKeyHex != nil {
			if _, e := ParseHex(*r.SigningKeyHex, 32); e != nil {
				return e
			}
		}
		if r.SigningKeyURI != nil {
			if _, _, e := keyprovider.ParseURI(*r.SigningKeyURI); e != nil {
				return e
			}
		}
	}
	if f := r.Files; f != nil {
		if !nonempty(f.Directory) || f.MaxFileBytes == 0 || f.ConcurrentFetches == 0 || f.FetchInterval <= 0 {
			return errors.New("invalid replication.files limits")
		}
		if nonempty(f.KeyHex) == (f.KeyURI != nil) {
			return errors.New("set exactly one replication.files key source")
		}
		if nonempty(f.KeyHex) {
			if _, e := ParseHex(f.KeyHex, 32); e != nil {
				return e
			}
		}
		if f.KeyURI != nil {
			if _, _, e := keyprovider.ParseURI(*f.KeyURI); e != nil {
				return e
			}
		}
		if f.PreviousKeyHex != nil && f.PreviousKeyURI != nil {
			return errors.New("set at most one replication.files previous key source")
		}
		if f.PreviousKeyHex != nil {
			if _, e := ParseHex(*f.PreviousKeyHex, 32); e != nil {
				return e
			}
		}
		if f.PreviousKeyURI != nil {
			if _, _, e := keyprovider.ParseURI(*f.PreviousKeyURI); e != nil {
				return e
			}
		}
		if e := socket(f.FetchBind); e != nil {
			return e
		}
	}
	if b := r.Bridge; b != nil {
		if b.Role != "low-exporter" && b.Role != "high-receiver" {
			return errors.New("invalid bridge role")
		}
		if _, e := ParseHex(b.Domain, 16); e != nil {
			return e
		}
		if _, e := ParseHex(b.Recipient, 32); e != nil {
			return e
		}
		for _, id := range b.TrustedDomains {
			if _, e := ParseHex(id, 16); e != nil {
				return e
			}
		}
		for _, keys := range []map[string]string{b.TrustedOrigins, b.TrustedSigners} {
			if e := validateKeys(keys); e != nil {
				return e
			}
		}
		for _, pair := range [][2]*string{{b.SignerHex, b.SignerURI}, {b.ReceiverSecretHex, b.ReceiverSecretURI}} {
			if pair[0] != nil && pair[1] != nil {
				return errors.New("set at most one bridge credential source per key")
			}
			if pair[0] != nil {
				if _, e := ParseHex(*pair[0], 32); e != nil {
					return e
				}
			}
			if pair[1] != nil {
				if _, _, e := keyprovider.ParseURI(*pair[1]); e != nil {
					return e
				}
			}
		}
		if !nonempty(b.Directory) || !nonempty(b.Stream) || len(b.Stream) > 64 {
			return errors.New("invalid bridge directory or stream")
		}
		for _, ch := range b.Stream {
			if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_') {
				return errors.New("invalid bridge stream")
			}
		}
		if (b.Role == "low-exporter" && b.SignerHex == nil && b.SignerURI == nil) || (b.Role == "high-receiver" && ((b.ReceiverSecretHex == nil && b.ReceiverSecretURI == nil) || len(b.TrustedDomains) == 0)) {
			return errors.New("missing bridge credentials or trusted domains")
		}
		if b.MaxBundleBytes == 0 || b.MaxBundleBytes > 16<<20 || b.MaxPayloadBytes == 0 || b.MaxPayloadBytes > 64<<20 || b.StagingBytes < b.MaxBundleBytes {
			return errors.New("invalid bridge limits")
		}
	}
	return nil
}
func validateKeys(m map[string]string) error {
	for k, v := range m {
		if _, e := ParseHex(k, 16); e != nil {
			return e
		}
		if _, e := ParseHex(v, 32); e != nil {
			return e
		}
	}
	return nil
}

// Snapshot makes ownership explicit: maps and slices never alias the running config.
func (c Config) Snapshot() Config { return clone(reflect.ValueOf(c)).Interface().(Config) }
func clone(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		n := reflect.New(v.Type().Elem())
		n.Elem().Set(clone(v.Elem()))
		return n
	case reflect.Struct:
		n := reflect.New(v.Type()).Elem()
		for i := 0; i < v.NumField(); i++ {
			n.Field(i).Set(clone(v.Field(i)))
		}
		return n
	case reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		n := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			n.Index(i).Set(clone(v.Index(i)))
		}
		return n
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		n := reflect.MakeMap(v.Type())
		iter := v.MapRange()
		for iter.Next() {
			n.SetMapIndex(iter.Key(), clone(iter.Value()))
		}
		return n
	default:
		return v
	}
}

func exactKeys(value any, typ reflect.Type, prefix string) error {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	switch typ.Kind() {
	case reflect.Struct:
		m, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		fields := map[string]reflect.Type{}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			fields[f.Tag.Get("json")] = f.Type
		}
		for k, v := range m {
			ft, ok := fields[k]
			if !ok {
				return fmt.Errorf("unknown configuration key %s%s", prefix, k)
			}
			if e := exactKeys(v, ft, prefix+k+"."); e != nil {
				return e
			}
		}
	case reflect.Slice:
		if values, ok := value.([]any); ok {
			for _, v := range values {
				if e := exactKeys(v, typ.Elem(), prefix); e != nil {
					return e
				}
			}
		}
	}
	return nil
}

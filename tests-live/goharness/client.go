// Minimal PostgreSQL wire client for the live harness.
//
// The Rust harness drives servers through the murmur-client crate
// (tokio-postgres plus MURMUR UNLOCK). The Go port hand-rolls the same
// surface on the standard library (the repo's lib/pq-based client lives
// in the nested clients/go module and must stay out of this module's
// dependency graph): startup with cleartext-password auth, optional
// TLS, the simple protocol, and the extended protocol with
// text-format parameters and results.
package goharness

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PostgreSQL OIDs the server reports (mirrors engine OIDs).
const (
	oidBool   = 16
	oidBytea  = 17
	oidInt8   = 20
	oidInt2   = 21
	oidInt4   = 23
	oidText   = 25
	oidFloat4 = 700
	oidFloat8 = 701
)

// TLSMode mirrors Postgres sslmode.
type TLSMode int

const (
	// TLSDisable is plaintext (default).
	TLSDisable TLSMode = iota
	// TLSRequire is encrypted with the server certificate not verified.
	TLSRequire
	// TLSVerifyCa verifies the chain against CaFile; host name not checked.
	TLSVerifyCa
	// TLSVerifyFull verifies the chain and host name against CaFile.
	TLSVerifyFull
)

// TLSConfig holds TLS settings. CaFile is required for VerifyCa/VerifyFull;
// CertFile and KeyFile (PEM) enable a client certificate and must be set
// together.
type TLSConfig struct {
	Mode     TLSMode
	CaFile   string
	CertFile string
	KeyFile  string
}

// Config holds connection settings.
type Config struct {
	Host     string
	Port     uint16
	Database string
	User     string
	Password string
	// Key is the encryption key or passphrase for MURMUR UNLOCK (required).
	Key            string
	TLS            TLSConfig
	ConnectTimeout time.Duration
}

// NewConfig returns a Config with defaults (127.0.0.1:5432, database
// murmur, 10 s timeout).
func NewConfig(user, password, key string) Config {
	return Config{
		Host:           "127.0.0.1",
		Port:           5432,
		Database:       "murmur",
		User:           user,
		Password:       password,
		Key:            key,
		ConnectTimeout: 10 * time.Second,
	}
}

func (c Config) validate() error {
	bad := func(m string) error { return fmt.Errorf("murmur: %s", m) }
	if c.User == "" {
		return bad("user is required")
	}
	if c.Password == "" {
		return bad("password is required")
	}
	if c.Key == "" {
		return bad("key is required")
	}
	if c.Port == 0 {
		return bad("invalid port 0")
	}
	if (c.TLS.Mode == TLSVerifyCa || c.TLS.Mode == TLSVerifyFull) && c.TLS.CaFile == "" {
		return bad("verify-ca and verify-full need ca_file")
	}
	if (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
		return bad("cert_file and key_file must be set together")
	}
	return nil
}

// Column describes one result column.
type Column struct {
	Name string
	OID  uint32
}

// Row is one extended-protocol result row. NULL values decode according
// to the accessor: Get maps NULL to "", typed accessors report an error,
// OptString reports presence.
type Row struct {
	columns []Column
	values  []*string
}

// Len returns the column count.
func (r Row) Len() int { return len(r.values) }

// Column returns the column descriptor.
func (r Row) Column(i int) Column { return r.columns[i] }

// IsNull reports whether column i is NULL.
func (r Row) IsNull(i int) bool { return r.values[i] == nil }

// Get returns column i as text (NULL becomes an empty string).
func (r Row) Get(i int) string {
	if r.values[i] == nil {
		return ""
	}
	return *r.values[i]
}

// String returns column i as text, failing on NULL.
func (r Row) String(i int) (string, error) {
	if r.values[i] == nil {
		return "", fmt.Errorf("murmur: column %d is NULL", i)
	}
	return *r.values[i], nil
}

// OptString returns column i as text plus its presence (NULL -> ("", false)).
func (r Row) OptString(i int) (string, bool) {
	if r.values[i] == nil {
		return "", false
	}
	return *r.values[i], true
}

// Int64 parses column i as a decimal integer.
func (r Row) Int64(i int) (int64, error) {
	s, err := r.String(i)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("murmur: column %d: invalid integer %q", i, s)
	}
	return n, nil
}

// Float64 parses column i as a float.
func (r Row) Float64(i int) (float64, error) {
	s, err := r.String(i)
	if err != nil {
		return 0, err
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, fmt.Errorf("murmur: column %d: invalid float %q", i, s)
	}
	return f, nil
}

// Bool parses column i (t/true/1, f/false/0).
func (r Row) Bool(i int) (bool, error) {
	s, err := r.String(i)
	if err != nil {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "t", "true", "1", "yes", "on":
		return true, nil
	case "f", "false", "0", "no", "off":
		return false, nil
	}
	return false, fmt.Errorf("murmur: column %d: invalid bool %q", i, s)
}

// Bytes decodes column i: bytea hex format (\x...) is hex-decoded,
// anything else is returned raw.
func (r Row) Bytes(i int) ([]byte, error) {
	s, err := r.String(i)
	if err != nil {
		return nil, err
	}
	if len(s) >= 2 && (strings.HasPrefix(s, `\x`) || strings.HasPrefix(s, `\X`)) {
		b, err := parseHex(s[2:])
		if err != nil {
			return nil, fmt.Errorf("murmur: column %d: invalid bytea %q", i, s)
		}
		return b, nil
	}
	return []byte(s), nil
}

func parseHex(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd hex length")
	}
	out := make([]byte, len(s)/2)
	for i := range out {
		n, err := strconv.ParseUint(s[2*i:2*i+2], 16, 8)
		if err != nil {
			return nil, err
		}
		out[i] = byte(n)
	}
	return out, nil
}

// Client is one unlocked wire connection to a node. It is safe for
// concurrent use; operations serialize on the connection.
//
// Deviation: tokio-postgres multiplexes concurrent queries over one
// connection; this client serializes them. Identical results, less overlap.
type Client struct {
	mu     sync.Mutex
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	closed bool
}

// Connect dials cfg, verifies the server, and runs MURMUR UNLOCK with
// cfg.Key. A wrong key fails here with SQLSTATE 28000.
func Connect(cfg Config) (*Client, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port)))
	raw, err := net.DialTimeout("tcp", addr, cfg.ConnectTimeout)
	if err != nil {
		return nil, fmt.Errorf("murmur: connect %s: %w", addr, err)
	}
	conn := raw
	if cfg.TLS.Mode != TLSDisable {
		tc, err := tlsUpgrade(raw, cfg)
		if err != nil {
			_ = raw.Close()
			return nil, err
		}
		conn = tc
	}
	c := &Client{conn: conn, reader: bufio.NewReader(conn), writer: bufio.NewWriter(conn)}
	_ = conn.SetDeadline(time.Now().Add(cfg.ConnectTimeout))
	startErr := c.startup(cfg)
	_ = conn.SetDeadline(time.Time{})
	if startErr != nil {
		_ = conn.Close()
		return nil, startErr
	}
	if _, err := c.Simple("MURMUR UNLOCK KEY " + SQLLiteral(cfg.Key)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return c, nil
}

// Close sends Terminate and closes the connection. It is idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	_ = c.send('X', nil)
	_ = c.writer.Flush()
	return c.conn.Close()
}

func tlsUpgrade(raw net.Conn, cfg Config) (net.Conn, error) {
	_ = raw.SetDeadline(time.Now().Add(cfg.ConnectTimeout))
	req := make([]byte, 8)
	binary.BigEndian.PutUint32(req[0:4], 8)
	binary.BigEndian.PutUint32(req[4:8], 80877103) // SSLRequest
	if _, err := raw.Write(req); err != nil {
		return nil, fmt.Errorf("murmur: tls request: %w", err)
	}
	resp := []byte{0}
	if _, err := io.ReadFull(raw, resp); err != nil {
		return nil, fmt.Errorf("murmur: tls request: %w", err)
	}
	if resp[0] != 'S' {
		return nil, fmt.Errorf("murmur: tls: server refused TLS")
	}
	tc, err := tlsConfig(cfg)
	if err != nil {
		return nil, err
	}
	conn := tls.Client(raw, tc)
	if err := conn.Handshake(); err != nil {
		return nil, fmt.Errorf("murmur: tls handshake: %w", err)
	}
	return conn, nil
}

func tlsConfig(cfg Config) (*tls.Config, error) {
	out := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.TLS.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("murmur: tls: load client certificate: %w", err)
		}
		out.Certificates = []tls.Certificate{cert}
	}
	if cfg.TLS.Mode == TLSRequire {
		out.InsecureSkipVerify = true
		return out, nil
	}
	caPEM, err := os.ReadFile(cfg.TLS.CaFile)
	if err != nil {
		return nil, fmt.Errorf("murmur: tls: read ca_file: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("murmur: tls: no CA certificates in %s", cfg.TLS.CaFile)
	}
	host := cfg.Host
	verifyName := cfg.TLS.Mode == TLSVerifyFull
	out.InsecureSkipVerify = true
	out.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("murmur: tls: missing peer certificate")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("murmur: tls: parse peer certificate: %w", err)
		}
		intermediates := x509.NewCertPool()
		for _, raw := range rawCerts[1:] {
			if cert, err := x509.ParseCertificate(raw); err == nil {
				intermediates.AddCert(cert)
			}
		}
		opts := x509.VerifyOptions{
			Roots:         roots,
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		if verifyName {
			// An IP literal here verifies IP SANs; a name verifies DNS SANs.
			opts.DNSName = host
		}
		_, err = leaf.Verify(opts)
		return err
	}
	return out, nil
}

func (c *Client) startup(cfg Config) error {
	body := make([]byte, 4)
	binary.BigEndian.PutUint32(body, 196608)
	for _, kv := range [][2]string{{"user", cfg.User}, {"database", cfg.Database}} {
		body = append(body, kv[0]...)
		body = append(body, 0)
		body = append(body, kv[1]...)
		body = append(body, 0)
	}
	body = append(body, 0)
	frame := make([]byte, 4)
	binary.BigEndian.PutUint32(frame, uint32(len(body)+4))
	frame = append(frame, body...)
	if _, err := c.conn.Write(frame); err != nil {
		return fmt.Errorf("murmur: startup: %w", err)
	}
	for {
		kind, msg, err := c.readMsg()
		if err != nil {
			return err
		}
		switch kind {
		case 'R':
			if len(msg) < 4 {
				return fmt.Errorf("murmur: protocol: short auth message")
			}
			switch binary.BigEndian.Uint32(msg) {
			case 0:
				return c.drainReady()
			case 3: // cleartext password
				pw := append([]byte(cfg.Password), 0)
				if err := c.send('p', pw); err != nil {
					return err
				}
				if err := c.writer.Flush(); err != nil {
					return fmt.Errorf("murmur: auth: %w", err)
				}
			default:
				return fmt.Errorf("murmur: protocol: unsupported auth method %d", binary.BigEndian.Uint32(msg))
			}
		case 'E':
			return c.parseError(msg)
		default:
			return fmt.Errorf("murmur: protocol: unexpected startup message %q", kind)
		}
	}
}

// drainReady consumes messages until ReadyForQuery, returning any error
// reported along the way.
func (c *Client) drainReady() error {
	for {
		kind, msg, err := c.readMsg()
		if err != nil {
			return err
		}
		switch kind {
		case 'Z':
			return nil
		case 'E':
			db := c.parseError(msg)
			if err := c.drainReady(); err != nil {
				return err
			}
			return db
		case 'S', 'K', 'N':
		default:
			return fmt.Errorf("murmur: protocol: unexpected message %q", kind)
		}
	}
}

// Simple runs a parameterless command through the simple protocol; every
// value is returned as text (NULL becomes an empty string).
func (c *Client) Simple(sql string) ([][]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, fmt.Errorf("murmur: connection closed")
	}
	body := append([]byte(sql), 0)
	if err := c.send('Q', body); err != nil {
		return nil, err
	}
	if err := c.writer.Flush(); err != nil {
		return nil, fmt.Errorf("murmur: query: %w", err)
	}
	var out [][]string
	for {
		kind, msg, err := c.readMsg()
		if err != nil {
			return nil, err
		}
		switch kind {
		case 'T', 'C', 'I', 'N':
		case 'D':
			row, err := parseDataRow(msg)
			if err != nil {
				return nil, err
			}
			out = append(out, row)
		case 'E':
			db := c.parseError(msg)
			if err := c.drainReady(); err != nil {
				return nil, err
			}
			return nil, db
		case 'Z':
			return out, nil
		default:
			return nil, fmt.Errorf("murmur: protocol: unexpected message %q", kind)
		}
	}
}

// SimpleTimeout is Simple with an I/O deadline. The deadline applies to
// the shared connection, so concurrent operations inherit it while the
// call runs; forensics uses this on dedicated connections only.
func (c *Client) SimpleTimeout(sql string, d time.Duration) ([][]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, fmt.Errorf("murmur: connection closed")
	}
	_ = c.conn.SetDeadline(time.Now().Add(d))
	defer c.conn.SetDeadline(time.Time{})
	body := append([]byte(sql), 0)
	if err := c.send('Q', body); err != nil {
		return nil, err
	}
	if err := c.writer.Flush(); err != nil {
		return nil, fmt.Errorf("murmur: query: %w", err)
	}
	var out [][]string
	for {
		kind, msg, err := c.readMsg()
		if err != nil {
			return nil, err
		}
		switch kind {
		case 'T', 'C', 'I', 'N':
		case 'D':
			row, err := parseDataRow(msg)
			if err != nil {
				return nil, err
			}
			out = append(out, row)
		case 'E':
			db := c.parseError(msg)
			if err := c.drainReady(); err != nil {
				return nil, err
			}
			return nil, db
		case 'Z':
			return out, nil
		default:
			return nil, fmt.Errorf("murmur: protocol: unexpected message %q", kind)
		}
	}
}

// BatchExecute runs parameterless statements through the simple protocol,
// discarding rows (BEGIN/COMMIT batches).
func (c *Client) BatchExecute(sql string) error {
	_, err := c.Simple(sql)
	return err
}

// Query runs a query with $1, $2, ... parameters. Parameters may be nil
// (NULL), string, []byte, bool, integers, or floats; all travel in text
// format with server-side type inference.
func (c *Client) Query(sql string, params ...any) ([]Row, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.extended(sql, params, true)
}

// Execute runs a statement with parameters and returns the affected row
// count parsed from the command tag (0 when the tag carries none).
func (c *Client) Execute(sql string, params ...any) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rows, tag, err := c.extendedRaw(sql, params, false)
	if err != nil {
		return 0, err
	}
	_ = rows
	return parseTagCount(tag), nil
}

// ExecuteTimeout is Execute with an I/O deadline (per-statement wire
// timeouts, e.g. crash legs bound every insert so a kill can never wedge
// a writer on the lock). Like SimpleTimeout, the deadline applies to the
// shared connection while the call runs.
func (c *Client) ExecuteTimeout(sql string, d time.Duration, params ...any) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, fmt.Errorf("murmur: connection closed")
	}
	_ = c.conn.SetDeadline(time.Now().Add(d))
	defer c.conn.SetDeadline(time.Time{})
	rows, tag, err := c.extendedRaw(sql, params, false)
	if err != nil {
		return 0, err
	}
	_ = rows
	return parseTagCount(tag), nil
}

func (c *Client) extended(sql string, params []any, wantRows bool) ([]Row, error) {
	rows, _, err := c.extendedRaw(sql, params, wantRows)
	return rows, err
}

func (c *Client) extendedRaw(sql string, params []any, describe bool) ([]Row, string, error) {
	if c.closed {
		return nil, "", fmt.Errorf("murmur: connection closed")
	}
	encoded := make([]*string, len(params))
	for i, p := range params {
		s, isNull, err := encodeParam(p)
		if err != nil {
			return nil, "", err
		}
		if !isNull {
			encoded[i] = &s
		}
	}
	parse := []byte{0} // unnamed statement
	parse = append(parse, sql...)
	parse = append(parse, 0)
	parse = append(parse, 0, 0) // no type hints; the server infers
	if err := c.send('P', parse); err != nil {
		return nil, "", err
	}
	bind := []byte{0, 0} // unnamed portal + statement
	bind = append(bind, 0, 0)
	bind = appendUint16(bind, uint16(len(encoded)))
	for _, s := range encoded {
		if s == nil {
			bind = appendUint32(bind, 0xffffffff)
			continue
		}
		bind = appendUint32(bind, uint32(len(*s)))
		bind = append(bind, *s...)
	}
	bind = append(bind, 0, 0) // all-text results
	if err := c.send('B', bind); err != nil {
		return nil, "", err
	}
	if describe {
		if err := c.send('D', []byte{'P', 0}); err != nil {
			return nil, "", err
		}
	}
	if err := c.send('E', []byte{0, 0, 0, 0, 0}); err != nil { // unnamed portal, no limit
		return nil, "", err
	}
	if err := c.send('S', nil); err != nil {
		return nil, "", err
	}
	if err := c.writer.Flush(); err != nil {
		return nil, "", fmt.Errorf("murmur: query: %w", err)
	}
	var (
		columns []Column
		rows    []Row
		tag     string
	)
	for {
		kind, msg, err := c.readMsg()
		if err != nil {
			return nil, "", err
		}
		switch kind {
		case '1', '2', 'n', 's', 'I', 'N':
		case 't':
			// ParameterDescription (statement describe only); ignored.
		case 'T':
			columns, err = parseRowDescription(msg)
			if err != nil {
				return nil, "", err
			}
		case 'D':
			vals, err := parseDataRowNull(msg)
			if err != nil {
				return nil, "", err
			}
			rows = append(rows, Row{columns: columns, values: vals})
		case 'C':
			tag = cstring(msg)
		case 'E':
			db := c.parseError(msg)
			// The server skips extended messages until Sync; ours is
			// already in flight, so drain to ReadyForQuery.
			if err := c.drainReady(); err != nil {
				return nil, "", err
			}
			return nil, "", db
		case 'Z':
			return rows, tag, nil
		default:
			return nil, "", fmt.Errorf("murmur: protocol: unexpected message %q", kind)
		}
	}
}

func encodeParam(p any) (string, bool, error) {
	switch v := p.(type) {
	case nil:
		return "", true, nil
	case string:
		return v, false, nil
	case []byte:
		return `\x` + Hex(v), false, nil
	case bool:
		if v {
			return "t", false, nil
		}
		return "f", false, nil
	case int:
		return strconv.FormatInt(int64(v), 10), false, nil
	case int8:
		return strconv.FormatInt(int64(v), 10), false, nil
	case int16:
		return strconv.FormatInt(int64(v), 10), false, nil
	case int32:
		return strconv.FormatInt(int64(v), 10), false, nil
	case int64:
		return strconv.FormatInt(v, 10), false, nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), false, nil
	case uint8:
		return strconv.FormatUint(uint64(v), 10), false, nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), false, nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), false, nil
	case uint64:
		return strconv.FormatUint(v, 10), false, nil
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32), false, nil
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), false, nil
	default:
		return "", false, fmt.Errorf("murmur: unsupported parameter type %T", p)
	}
}

func parseTagCount(tag string) int64 {
	fields := strings.Fields(tag)
	if len(fields) == 0 {
		return 0
	}
	n, err := strconv.ParseInt(fields[len(fields)-1], 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func (c *Client) send(kind byte, body []byte) error {
	frame := []byte{kind}
	frame = appendUint32(frame, uint32(len(body)+4))
	frame = append(frame, body...)
	_, err := c.writer.Write(frame)
	return err
}

func (c *Client) readMsg() (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(c.reader, header); err != nil {
		return 0, nil, fmt.Errorf("murmur: read: %w", err)
	}
	n := binary.BigEndian.Uint32(header[1:5])
	if n < 4 {
		return 0, nil, fmt.Errorf("murmur: protocol: short message length %d", n)
	}
	body := make([]byte, n-4)
	if _, err := io.ReadFull(c.reader, body); err != nil {
		return 0, nil, fmt.Errorf("murmur: read: %w", err)
	}
	return header[0], body, nil
}

func (c *Client) parseError(msg []byte) *DBError {
	out := &DBError{Code: "XX000"}
	for len(msg) > 0 {
		field := msg[0]
		msg = msg[1:]
		end := 0
		for end < len(msg) && msg[end] != 0 {
			end++
		}
		val := string(msg[:end])
		if end < len(msg) {
			end++
		}
		msg = msg[end:]
		if field == 0 {
			break
		}
		switch field {
		case 'C':
			out.Code = val
		case 'M':
			out.Message = val
		case 'D':
			out.Detail = val
		}
	}
	return out
}

func parseDataRow(msg []byte) ([]string, error) {
	vals, err := parseDataRowNull(msg)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(vals))
	for i, v := range vals {
		if v != nil {
			out[i] = *v
		}
	}
	return out, nil
}

func parseDataRowNull(msg []byte) ([]*string, error) {
	if len(msg) < 2 {
		return nil, fmt.Errorf("murmur: protocol: short data row")
	}
	n := int(binary.BigEndian.Uint16(msg))
	msg = msg[2:]
	vals := make([]*string, 0, n)
	for range n {
		if len(msg) < 4 {
			return nil, fmt.Errorf("murmur: protocol: short data row value")
		}
		size := int32(binary.BigEndian.Uint32(msg))
		msg = msg[4:]
		if size == -1 {
			vals = append(vals, nil)
			continue
		}
		if size < -1 || int(size) > len(msg) {
			return nil, fmt.Errorf("murmur: protocol: bad data row length %d", size)
		}
		s := string(msg[:size])
		vals = append(vals, &s)
		msg = msg[size:]
	}
	return vals, nil
}

func parseRowDescription(msg []byte) ([]Column, error) {
	if len(msg) < 2 {
		return nil, fmt.Errorf("murmur: protocol: short row description")
	}
	n := int(binary.BigEndian.Uint16(msg))
	msg = msg[2:]
	cols := make([]Column, 0, n)
	for range n {
		end := 0
		for end < len(msg) && msg[end] != 0 {
			end++
		}
		if end+18 > len(msg) {
			return nil, fmt.Errorf("murmur: protocol: short row description column")
		}
		name := string(msg[:end])
		rest := msg[end+1:]
		oid := binary.BigEndian.Uint32(rest[6:10])
		msg = rest[18:]
		cols = append(cols, Column{Name: name, OID: oid})
	}
	return cols, nil
}

func cstring(b []byte) string {
	for i, c := range b {
		if c == 0 {
			return string(b[:i])
		}
	}
	return string(b)
}

func appendUint16(b []byte, n uint16) []byte {
	return append(b, byte(n>>8), byte(n))
}

func appendUint32(b []byte, n uint32) []byte {
	return append(b, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
}

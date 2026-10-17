package murmur

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// TLSMode selects Postgres sslmode negotiation.
type TLSMode string

const (
	// TLSDisable sends everything, including the password, in cleartext.
	TLSDisable TLSMode = "disable"
	// TLSRequire negotiates TLS without verifying the server certificate.
	TLSRequire TLSMode = "require"
	// TLSVerifyCA verifies the server chain against CAFile.
	TLSVerifyCA TLSMode = "verify-ca"
	// TLSVerifyFull verifies the chain and the server hostname.
	TLSVerifyFull TLSMode = "verify-full"
)

// TLSConfig mirrors the Postgres sslmode connection parameters.
type TLSConfig struct {
	Mode TLSMode
	// CAFile is the PEM bundle verifying the server (verify-ca/full).
	CAFile string
	// CertFile and KeyFile present a client certificate (mutual TLS).
	CertFile string
	KeyFile  string
}

// Config describes one Murmur server connection.
type Config struct {
	Host           string // default 127.0.0.1
	Port           int    // default 5432
	User           string // required: auth.users entry on the server
	Password       string // required
	Database       string // default murmur; the name is accepted, not used
	Key            string // required: encryption key or passphrase for MURMUR UNLOCK
	TLS            TLSConfig
	ConnectTimeout time.Duration // default 10s
	MaxOpenConns   int           // optional pool tuning, passed to database/sql
	MaxIdleConns   int           // optional pool tuning, passed to database/sql
}

func (c Config) withDefaults() Config {
	if c.Host == "" {
		c.Host = "127.0.0.1"
	}
	if c.Port == 0 {
		c.Port = 5432
	}
	if c.Database == "" {
		c.Database = "murmur"
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 10 * time.Second
	}
	return c
}

func (c Config) validate() error {
	if c.User == "" {
		return errors.New("murmur: User is required")
	}
	if c.Password == "" {
		return errors.New("murmur: Password is required")
	}
	if c.Key == "" {
		return errors.New("murmur: Key is required")
	}
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("murmur: invalid Port %d", c.Port)
	}
	switch c.TLS.Mode {
	case "", TLSDisable, TLSRequire, TLSVerifyCA, TLSVerifyFull:
	default:
		return fmt.Errorf("murmur: invalid TLS mode %q", c.TLS.Mode)
	}
	if (c.TLS.Mode == TLSVerifyCA || c.TLS.Mode == TLSVerifyFull) && c.TLS.CAFile == "" {
		return fmt.Errorf("murmur: TLS mode %q needs CAFile", c.TLS.Mode)
	}
	if (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
		return errors.New("murmur: CertFile and KeyFile must be set together")
	}
	return nil
}

func (c Config) connURL() (string, error) {
	c = c.withDefaults()
	if err := c.validate(); err != nil {
		return "", err
	}
	query := url.Values{}
	if c.TLS.Mode != "" && c.TLS.Mode != TLSDisable {
		query.Set("sslmode", string(c.TLS.Mode))
	} else {
		query.Set("sslmode", "disable")
	}
	if c.TLS.CAFile != "" {
		query.Set("sslrootcert", c.TLS.CAFile)
	}
	if c.TLS.CertFile != "" {
		query.Set("sslcert", c.TLS.CertFile)
		query.Set("sslkey", c.TLS.KeyFile)
	}
	query.Set("connect_timeout", strconv.FormatInt(int64(c.ConnectTimeout/time.Second), 10))
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(c.User, c.Password),
		Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:     "/" + c.Database,
		RawQuery: query.Encode(),
	}
	return u.String(), nil
}

func escapeKey(key string) string {
	return strings.ReplaceAll(key, "'", "''")
}

// DB is a database/sql pool unlocked for Murmur. Use the embedded *sql.DB
// for all application SQL; MURMUR helpers cover server administration.
type DB struct {
	*sql.DB
}

// Open connects, verifies the server, and unlocks every pooled connection
// with cfg.Key. Each physical connection runs MURMUR UNLOCK before first
// use; a wrong key fails Open (and any later reconnect) with code 28000.
func Open(ctx context.Context, cfg Config) (*DB, error) {
	connURL, err := cfg.withDefaults().connURL()
	if err != nil {
		return nil, err
	}
	key := escapeKey(cfg.Key)
	pgxCfg, err := pgx.ParseConfig(connURL)
	if err != nil {
		return nil, fmt.Errorf("murmur: bad connection string: %w", err)
	}
	pgxCfg.AfterConnect = func(ctx context.Context, conn *pgconn.PgConn) error {
		_, err := conn.Exec(ctx, "MURMUR UNLOCK KEY '"+key+"'").ReadAll()
		if err != nil {
			return fmt.Errorf("murmur: unlock failed: %w", err)
		}
		return nil
	}
	db := stdlib.OpenDB(*pgxCfg)
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("murmur: connect: %w", err)
	}
	return &DB{db}, nil
}

// Flush waits for a durable checkpoint of everything written so far.
func (db *DB) Flush(ctx context.Context) error {
	_, err := db.ExecContext(ctx, "MURMUR FLUSH")
	return err
}

// SetTableReplicated stores a per-table replication flag (true = replicated,
// false = local-only) in the server's config.toml. Tables default to local-only.
// Enabling converts the application primary key to a unique index and adds a
// read-only UUIDv5 primary key, including for existing rows. Disabling restores
// the application primary key. These operations can scan and rebuild the table.
func (db *DB) SetTableReplicated(ctx context.Context, table string, replicated bool) error {
	if strings.TrimSpace(table) == "" {
		return errors.New("murmur: table name must not be empty")
	}
	_, err := db.ExecContext(
		ctx,
		"MURMUR SET TABLE "+quoteIdent(table)+" REPLICATED "+strconv.FormatBool(replicated),
	)
	return err
}

// TableFlag pairs a table name with its replication flag.
type TableFlag struct {
	Table      string
	Replicated bool
}

// TableReplication lists every known table and its replication flag.
func (db *DB) TableReplication(ctx context.Context) ([]TableFlag, error) {
	rows, err := db.QueryContext(ctx, "MURMUR SHOW TABLES REPLICATION")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var flags []TableFlag
	for rows.Next() {
		var table, replicated string
		if err := rows.Scan(&table, &replicated); err != nil {
			return nil, err
		}
		flags = append(flags, TableFlag{Table: table, Replicated: replicated == "true"})
	}
	return flags, rows.Err()
}

// Metric is one key-value measurement from the server's SQLite or Spool engine.
type Metric struct {
	Subsystem string
	Metric    string
	Value     string
}

// Metrics retrieves live in-memory SQLite and Spool persistence metrics from the server.
// An optional scope ("SQLITE" or "SPOOL") filters results to that subsystem.
func (db *DB) Metrics(ctx context.Context, scope ...string) ([]Metric, error) {
	cmd := "MURMUR SHOW METRICS"
	if len(scope) > 0 && strings.TrimSpace(scope[0]) != "" {
		cmd += " " + strings.ToUpper(strings.TrimSpace(scope[0]))
	}
	rows, err := db.QueryContext(ctx, cmd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var metrics []Metric
	for rows.Next() {
		var m Metric
		if err := rows.Scan(&m.Subsystem, &m.Metric, &m.Value); err != nil {
			return nil, err
		}
		metrics = append(metrics, m)
	}
	return metrics, rows.Err()
}

// quoteIdent quotes a table name for MURMUR SET TABLE. Names arrive as one
// tokenizer word; embedded quotes are doubled.
func quoteIdent(name string) string {
	return "'" + escapeKey(name) + "'"
}

// CodeOf extracts the Postgres SQLSTATE code from a driver error, or "" when
// the error carries none. Useful codes: 28000 (locked, wrong key, or TLS
// required), 42P01 (unknown table), 23505 (unique violation), 42601 (syntax).
// BackupFile describes one archive in the server's backup folder.
type BackupFile struct {
	Name     string
	Size     int64
	Modified string
}

// BackupSave checkpoints the live database into a datetime-named archive
// and returns its name and size.
func (db *DB) BackupSave(ctx context.Context) (BackupFile, error) {
	var file BackupFile
	err := db.QueryRowContext(ctx, "MURMUR BACKUP SAVE").Scan(&file.Name, &file.Size)
	return file, err
}

// BackupList returns the backup files available to restore, sorted by name.
func (db *DB) BackupList(ctx context.Context) ([]BackupFile, error) {
	rows, err := db.QueryContext(ctx, "MURMUR BACKUP LIST")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var files []BackupFile
	for rows.Next() {
		var file BackupFile
		if err := rows.Scan(&file.Name, &file.Size, &file.Modified); err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	return files, rows.Err()
}

// Upload chunk size; base64 inflates it by a third on the wire.
const uploadChunkBytes = 256 << 10

// BackupUpload streams r into the server's backup folder under name. The
// server only publishes the file on FINISH; a failed upload leaves no
// visible file behind.
func (db *DB) BackupUpload(ctx context.Context, name string, r io.Reader) error {
	if err := checkBackupName(name); err != nil {
		return err
	}
	quoted := quoteLiteral(name)
	if _, err := db.ExecContext(ctx, "MURMUR BACKUP UPLOAD BEGIN "+quoted); err != nil {
		return err
	}
	failed := true
	defer func() {
		if failed {
			_, _ = db.ExecContext(context.WithoutCancel(ctx), "MURMUR BACKUP UPLOAD ABORT "+quoted)
		}
	}()
	buf := make([]byte, uploadChunkBytes)
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			stmt := "MURMUR BACKUP UPLOAD APPEND " + quoted + " '" + base64.StdEncoding.EncodeToString(buf[:n]) + "'"
			if _, aerr := db.ExecContext(ctx, stmt); aerr != nil {
				return aerr
			}
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return err
		}
	}
	var file BackupFile
	if err := db.QueryRowContext(ctx, "MURMUR BACKUP UPLOAD FINISH "+quoted).Scan(&file.Name, &file.Size); err != nil {
		return err
	}
	failed = false
	return nil
}

// Download chunk size per MURMUR BACKUP DOWNLOAD round-trip.
const downloadChunkBytes = 1 << 20

// BackupDownload streams a backup file from the server into w.
func (db *DB) BackupDownload(ctx context.Context, name string, w io.Writer) error {
	if err := checkBackupName(name); err != nil {
		return err
	}
	var offset int64
	for {
		var total int64
		var chunk []byte
		stmt := "MURMUR BACKUP DOWNLOAD " + quoteLiteral(name) +
			" OFFSET " + strconv.FormatInt(offset, 10) +
			" LIMIT " + strconv.Itoa(downloadChunkBytes)
		if err := db.QueryRowContext(ctx, stmt).Scan(&offset, &total, &chunk); err != nil {
			return err
		}
		if len(chunk) == 0 {
			return nil
		}
		n, err := w.Write(chunk)
		if err != nil {
			return err
		}
		if n != len(chunk) {
			return io.ErrShortWrite
		}
		next := offset + int64(len(chunk))
		if next >= total || next <= offset {
			return nil
		}
		offset = next
	}
}

// Restore live-restores a backup archive. The key re-authenticates this
// destructive operation; the server preserves the pre-restore data next to
// the data directory for the operator to inspect or delete.
func (db *DB) Restore(ctx context.Context, name, key string) error {
	if err := checkBackupName(name); err != nil {
		return err
	}
	if key == "" {
		return errors.New("murmur: restore needs a non-empty key")
	}
	_, err := db.ExecContext(ctx,
		"MURMUR RESTORE FROM "+quoteLiteral(name)+" KEY "+quoteLiteral(key))
	return err
}

// checkBackupName mirrors the server's portable filename rule so typos fail
// locally; the server validates again.
func checkBackupName(name string) error {
	if name == "" || len(name) > 255 || strings.HasPrefix(name, ".") {
		return fmt.Errorf("murmur: invalid backup name %q", name)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '.' || c == '_' || c == '-' {
			continue
		}
		return fmt.Errorf("murmur: invalid backup name %q", name)
	}
	return nil
}

// quoteLiteral renders a single-quoted MURMUR word with escapes doubled.
func quoteLiteral(value string) string {
	return "'" + escapeKey(value) + "'"
}

// RotateKey rewraps the key registry under a new master key after
// verifying the previous one. Existing pooled connections keep working,
// but new connections must unlock with the new key: point Config.Key at it
// (typically by opening a fresh DB handle) right after rotating.
func (db *DB) RotateKey(ctx context.Context, previous, next string) error {
	if previous == "" || next == "" {
		return errors.New("murmur: rotate needs non-empty previous and new keys")
	}
	if previous == next {
		return errors.New("murmur: new key must differ from the previous key")
	}
	_, err := db.ExecContext(ctx,
		"MURMUR ROTATE KEY "+quoteLiteral(previous)+" "+quoteLiteral(next))
	return err
}

func CodeOf(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// IsLocked reports whether err is the locked-connection error: a statement
// ran before MURMUR UNLOCK on that connection.
func IsLocked(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "28000" &&
		strings.Contains(pgErr.Message, "locked")
}

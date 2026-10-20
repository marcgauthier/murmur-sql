package murmur

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

// Open connects, verifies the server, and unlocks every pooled connection
// with cfg.Key. Each physical connection runs MURMUR UNLOCK before first
// use; a wrong key fails Open (and any later reconnect) with code 28000.
func Open(ctx context.Context, cfg Config) (*sql.DB, error) {
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
	return db, nil
}

// Package goharness is the Go port of the Rust tests-live/ multi-process
// live harness (tests-live/harness): one murmur-server child process per
// node, driven over the PostgreSQL wire protocol (plain SQL plus MURMUR
// ... commands). It provides cluster orchestration, certificate
// provisioning, connect-with-watchdog, forensics, artifact retention, SQL
// assertions, and a raw-QUIC replication-protocol attacker fixture reusing
// the product wire implementation.
//
// Differences from the Rust source are deliberate and documented per
// symbol. The structural ones: async Rust becomes blocking Go (no
// wait_until_async equivalent is needed); node configs render as YAML,
// which is what the Go server's config loader accepts (it rejects the
// Rust renderer's TOML); panics on test bugs in assertions become
// returned errors.
package goharness

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// DBError is a server wire error carrying its SQLSTATE code. Match on Code
// for SQLSTATEs (28000 locked/wrong key/TLS required, 42P01 unknown
// table, 23505 unique violation, 42601 syntax, 55000 remote-only file).
type DBError struct {
	Code    string
	Message string
	Detail  string
}

// Error renders the full server-side text: CODE: message.
func (e *DBError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("%s: %s (%s)", e.Code, e.Message, e.Detail)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// IsLocked reports the locked-connection error (statement before
// MURMUR UNLOCK).
func (e *DBError) IsLocked() bool {
	return e.Code == "28000" && strings.Contains(e.Message, "locked")
}

// ErrorCode returns the SQLSTATE carried by err, or "" when err is not a
// server error.
func ErrorCode(err error) string {
	var db *DBError
	if errors.As(err, &db) {
		return db.Code
	}
	return ""
}

// DBMessage returns the full server-side text of a wire error for
// assertions: CODE: message for server errors, else the display text.
// (Go port of the Rust db_message helper.)
func DBMessage(err error) string {
	var db *DBError
	if errors.As(err, &db) {
		return db.Error()
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

// NodeStoppedError reports a query against a stopped node.
type NodeStoppedError struct {
	Index int
}

func (e *NodeStoppedError) Error() string {
	return fmt.Sprintf("node index %d is stopped", e.Index)
}

// UnknownNodeError reports an out-of-range node index.
type UnknownNodeError struct {
	Index int
}

func (e *UnknownNodeError) Error() string {
	return fmt.Sprintf("unknown node index %d", e.Index)
}

// UnlockTimeoutError reports a stall/overall watchdog timeout. The "open
// timed out" wording matches the Rust source verbatim.
type UnlockTimeoutError struct {
	Elapsed time.Duration
	Reason  string
}

func (e *UnlockTimeoutError) Error() string {
	return fmt.Sprintf("open timed out after %v (%s)", e.Elapsed, e.Reason)
}

// Hex renders bytes as lowercase hex (node ids, keys, SAN URIs).
func Hex(bytes []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, len(bytes)*2)
	for _, b := range bytes {
		out = append(out, digits[b>>4], digits[b&0x0f])
	}
	return string(out)
}

// DecodeHex32 decodes 64 hex chars into 32 bytes (origin seeds round-trip
// through their hex form). It reports false on any malformed input.
func DecodeHex32(hex string) ([32]byte, bool) {
	var out [32]byte
	if len(hex) != 64 {
		return out, false
	}
	for i := range out {
		n, err := strconv.ParseUint(hex[2*i:2*i+2], 16, 8)
		if err != nil {
			return [32]byte{}, false
		}
		out[i] = byte(n)
	}
	return out, true
}

// EnvSeconds reads a positive-integer seconds env knob with fallback.
func EnvSeconds(name string, fallbackSecs uint64) time.Duration {
	if v, err := strconv.ParseUint(os.Getenv(name), 10, 64); err == nil && v > 0 {
		return time.Duration(v) * time.Second
	}
	return time.Duration(fallbackSecs) * time.Second
}

// WaitUntil polls cond every poll interval until it holds or timeout
// elapses. It returns true when the condition was met (use with a fatal +
// forensics on false).
//
// Deviation: the Rust crate also offers wait_until_async for conditions
// that query nodes; blocking Go calls work in this single function, so no
// second form exists.
func WaitUntil(timeout, poll time.Duration, cond func() bool) bool {
	end := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if !time.Now().Before(end) {
			return false
		}
		time.Sleep(poll)
	}
}

// SQLLiteral escapes a literal command argument; identifiers/keywords
// cannot be parameters.
func SQLLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

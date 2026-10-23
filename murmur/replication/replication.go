// Package replication ports background replication. This package owns no
// SQLite connections and no storage engine; the murmur package implements
// Backend over its checkpoint worker and Spool.
package replication

import (
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/postcard"
)

// NodeId is a 128-bit node or database identity.
type NodeId = [16]byte

// Kind classifies an Error, mirroring the Rust Error variants.
type Kind int

const (
	KindInvalid Kind = iota + 1
	KindProtocol
	KindAuth
	KindStorage
	KindOverloaded
	KindUnavailable
	KindIo
	// KindCollision reports a fail-closed bridge import rejection: an
	// imported row identity collides with data from another domain.
	// Go-port extension (no Rust counterpart); surfaced, never held.
	KindCollision
)

// Error reports a replication failure with its kind, message, and cause.
type Error struct {
	Kind Kind
	Msg  string
	Err  error
}

func (e *Error) Error() string {
	switch e.Kind {
	case KindInvalid:
		return fmt.Sprintf("replication: invalid replication configuration: %s", e.Msg)
	case KindProtocol:
		return fmt.Sprintf("replication: replication protocol: %s", e.Msg)
	case KindAuth:
		return fmt.Sprintf("replication: replication authentication: %s", e.Msg)
	case KindStorage:
		return fmt.Sprintf("replication: replication storage: %s", e.Msg)
	case KindOverloaded:
		return "replication: replication overloaded"
	case KindUnavailable:
		return fmt.Sprintf("replication: replication unavailable: %s", e.Msg)
	case KindIo:
		return fmt.Sprintf("replication: I/O: %v", e.Err)
	case KindCollision:
		return fmt.Sprintf("replication: bridge import: %s", e.Msg)
	}
	return "replication: unknown error"
}

// Unwrap returns the underlying cause, if any.
func (e *Error) Unwrap() error { return e.Err }

// Is matches errors by kind so callers can use errors.Is.
func (e *Error) Is(target error) bool {
	var t *Error
	if errors.As(target, &t) {
		return e.Kind == t.Kind
	}
	return false
}

func invalidErr(format string, a ...any) *Error {
	return &Error{Kind: KindInvalid, Msg: fmt.Sprintf(format, a...)}
}
func protocolErr(format string, a ...any) *Error {
	return &Error{Kind: KindProtocol, Msg: fmt.Sprintf(format, a...)}
}
func authErr(format string, a ...any) *Error {
	return &Error{Kind: KindAuth, Msg: fmt.Sprintf(format, a...)}
}
func storageErr(format string, a ...any) *Error {
	return &Error{Kind: KindStorage, Msg: fmt.Sprintf(format, a...)}
}
func unavailableErr(format string, a ...any) *Error {
	return &Error{Kind: KindUnavailable, Msg: fmt.Sprintf(format, a...)}
}
func ioErr(err error) *Error { return &Error{Kind: KindIo, Err: err} }

// ErrOverloaded signals a saturated queue or oversized frame.
var ErrOverloaded = &Error{Kind: KindOverloaded}

// ErrBridgeIdentityCollision matches fail-closed bridge import
// rejections via errors.Is (matched by kind).
var ErrBridgeIdentityCollision = &Error{Kind: KindCollision}

// encode serializes one value with postcard.
func encode(fn func(*postcard.Encoder)) []byte {
	e := postcard.NewEncoder(64)
	fn(e)
	return append([]byte(nil), e.Bytes()...)
}

// decodeOne deserializes one value, rejecting trailing bytes.
func decodeOne[T any](b []byte, fn func(*postcard.Decoder) T) (T, error) {
	var zero T
	d := postcard.NewDecoder(b)
	v := fn(d)
	if err := d.End(); err != nil {
		return zero, protocolErr("%v", err)
	}
	if err := d.Err(); err != nil {
		return zero, protocolErr("%v", err)
	}
	return v, nil
}

// Hex renders bytes lowercase.
func Hex(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, c := range b {
		out = append(out, digits[c>>4], digits[c&15])
	}
	return string(out)
}

// Record is one key/value mutation; a nil value deletes.
type Record struct {
	Key   []byte
	Value []byte
}

// PutRecord builds a put.
func PutRecord(key, value []byte) Record { return Record{Key: key, Value: value} }

// DeleteRecord builds a delete.
func DeleteRecord(key []byte) Record { return Record{Key: key} }

// Deleted reports a delete record.
func (r Record) Deleted() bool { return r.Value == nil }

// Content is one publishable unit: an optional SQLite payload plus
// auxiliary records from one origin.
type Content struct {
	SQLite  []byte
	Records []Record
	Origin  NodeId
}

// Backend serves the replication engine. All calls run in background
// workers. Implementations must serialize access to Spool.
type Backend interface {
	// CancelPending interrupts pending admission/reply waits; never
	// aborts an active durable commit.
	CancelPending()
	Identity() (database, node NodeId)
	Get(key []byte) ([]byte, error)
	// GetMany reads many keys in one backend round-trip, preserving
	// input order (nil per missing key).
	GetMany(keys [][]byte) ([][]byte, error)
	// GetManyBounded reads an ordered prefix of keys within a byte
	// budget. Missing values consume no bytes; one oversized first
	// value is allowed for progress.
	GetManyBounded(keys [][]byte, maxBytes int) ([][]byte, error)
	Keys(prefix []byte) ([][]byte, error)
	// KeysPage is bounded exclusive-cursor traversal.
	KeysPage(prefix, after []byte, limit int) ([][]byte, error)
	// Progress returns durable per-origin progress.
	Progress() ([]Progress, error)
	Commit(records []Record) error
	ImportBridge(header Header, payload []byte) (bool, error)
	ReleaseOwnership(table string, key []byte, column *string) (bool, error)
	Publish(records []Record) error
	Snapshot() ([]byte, error)
	// SnapshotWithProgress returns logical state and applied progress
	// from the same storage boundary.
	SnapshotWithProgress() ([]byte, []Progress, error)
	// CreateSnapshot captures and publishes an immutable transfer and
	// its retention pin on the serialized storage worker.
	CreateSnapshot(recipient NodeId, config *Config) (Manifest, error)
	// AcknowledgeSnapshot atomically freezes the catch-up target after
	// a durable recipient receipt.
	AcknowledgeSnapshot(recipient, transfer NodeId, digest [32]byte) error
	// RetentionCheckpoint makes current logical state and its schema a
	// durable recovery snapshot.
	RetentionCheckpoint() error
	// PruneHistory atomically rechecks snapshot coverage, deletes a
	// bounded history prefix, and publishes the permanent duplicate
	// floor without losing newer appends.
	PruneHistory(origin NodeId, through uint64) (uint64, error)
	// ReclaimRetention runs a physical reclamation pass, also retrying
	// previously deferred work.
	ReclaimRetention() error
	Schema() ([]byte, error)
	// Apply returns false if local writes are uncollected. Effects and
	// records must be durable and visible before returning true.
	Apply(payloads [][]byte, records []Record) (bool, error)
	// Observe notes authenticated clocks without collecting a session.
	Observe(millis uint64, logical uint32) error
	AcceptSchema(schema []byte) (bool, error)
}

// Credentials holds the TLS chain, private key, and trust roots.
type Credentials struct {
	Chain      [][]byte
	PrivateKey []byte
	Roots      [][]byte
}

// Dissemination selects the broadcast profile.
type Dissemination int

const (
	// DisseminationGossip floods to random peers.
	DisseminationGossip Dissemination = iota
	// DisseminationPlumtree gossips over a spanning tree.
	DisseminationPlumtree
)

// PolicyColumn keys a merge policy by table and column name.
type PolicyColumn struct {
	Table  string
	Column string
}

// Config tunes the replication engine.
type Config struct {
	Bind                     netip.AddrPort
	Advertise                *netip.AddrPort
	Credentials              Credentials
	SigningKey               [32]byte
	TrustedOrigins           map[NodeId][32]byte
	AllowedPeers             map[NodeId]bool
	AllowedNetworks          []netip.Prefix
	TrustedSnapshotSources   map[NodeId]bool
	Bootstrap                []netip.AddrPort
	Membership               bool
	Dissemination            Dissemination
	AcceptRemoteSchema       bool
	Master                   bool
	SchemaMaster             *NodeId
	Fanout                   int
	Repairs                  int
	Sessions                 int
	Connections              int
	QueueBytes               int
	PeerQueueBytes           int
	StagingBytes             int
	StagingTransfers         int
	MaxTransactionBytes      int
	SnapshotBytes            int
	SnapshotStagingBytes     int
	SnapshotTransferLifetime time.Duration
	IncomingInterval         time.Duration
	BatchTransactions        int
	BatchBytes               int
	AdaptiveSync             bool
	AntiEntropyInterval      time.Duration
	Retention                time.Duration
	RetentionInterval        time.Duration
	MinimumRetained          uint64
	MaximumOffline           time.Duration
	MergePolicies            map[PolicyColumn]Policy
	Files                    *FilesConfig
	Bridge                   *BridgeConfig
}

// NewConfig returns the default engine configuration.
func NewConfig(bind netip.AddrPort, credentials Credentials, signingKey [32]byte) *Config {
	return &Config{
		Bind: bind, Credentials: credentials, SigningKey: signingKey,
		Membership: true, Dissemination: DisseminationGossip, AcceptRemoteSchema: true,
		Fanout: 4, Repairs: 2, Sessions: 32, Connections: 64,
		QueueBytes: 64 << 20, PeerQueueBytes: 4 << 20,
		StagingBytes: 256 << 20, StagingTransfers: 4096,
		MaxTransactionBytes: 64 << 20, SnapshotBytes: 512 << 20,
		SnapshotStagingBytes:     512 << 20,
		SnapshotTransferLifetime: 7 * 24 * time.Hour,
		IncomingInterval:         time.Second,
		BatchTransactions:        64,
		BatchBytes:               4 << 20,
		AntiEntropyInterval:      10 * time.Second,
		Retention:                time.Hour,
		RetentionInterval:        time.Minute,
		MinimumRetained:          1000,
		MaximumOffline:           7 * 24 * time.Hour,
	}
}

// Validate rejects inconsistent transport or capacity limits.
func (c *Config) Validate() error {
	if c.BatchTransactions == 0 || c.BatchTransactions > 1024 ||
		c.BatchBytes == 0 || c.BatchBytes > 64<<20 ||
		c.QueueBytes <= 1<<20 || c.QueueBytes > 128<<20 ||
		c.Connections > 4096 || c.Sessions > 4096 ||
		c.Fanout == 0 || c.Repairs == 0 || c.Fanout+c.Repairs > c.Sessions ||
		c.Connections < c.Sessions+8 ||
		c.QueueBytes < c.PeerQueueBytes || c.PeerQueueBytes < 1<<20 ||
		c.StagingBytes < c.MaxTransactionBytes+4096 ||
		c.StagingTransfers == 0 ||
		c.MaxTransactionBytes == 0 || c.MaxTransactionBytes > 64<<20 ||
		c.IncomingInterval <= 0 || c.AntiEntropyInterval <= 0 ||
		c.RetentionInterval <= 0 || c.MaximumOffline <= 0 ||
		c.MinimumRetained == 0 || c.SnapshotTransferLifetime <= 0 ||
		c.SnapshotStagingBytes == 0 || c.SnapshotBytes == 0 ||
		len(c.Credentials.Chain) == 0 || len(c.Credentials.Roots) == 0 {
		return invalidErr("inconsistent transport or capacity limits")
	}
	if c.Master && c.SchemaMaster == nil {
		return invalidErr("schema master requires schema_master")
	}
	if c.Files != nil {
		if err := c.Files.Validate(); err != nil {
			return err
		}
	}
	if c.Bridge != nil {
		if err := c.Bridge.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// BindSigner pins the durable signing-key binding. Never replace a key
// under the same node ID.
func BindSigner(backend Backend, config *Config) error {
	_, node := backend.Identity()
	public := PublicKey(&config.SigningKey)
	if trusted, ok := config.TrustedOrigins[node]; !ok || trusted != public {
		return authErr("local signing key is not registered")
	}
	key := []byte("07/replication/signer/" + Hex(node[:]))
	old, err := backend.Get(key)
	if err != nil {
		return err
	}
	switch {
	case old != nil && string(old) != string(public[:]):
		return authErr("signing identity changed; use a fresh node identity")
	case old == nil:
		if err := backend.Commit([]Record{PutRecord(key, public[:])}); err != nil {
			return err
		}
	}
	if config.SchemaMaster != nil {
		if _, ok := config.TrustedOrigins[*config.SchemaMaster]; !ok {
			return authErr("schema master has no trusted origin key")
		}
		if config.Master && *config.SchemaMaster != node {
			return authErr("schema master must designate its own node id")
		}
	} else if config.Master {
		return authErr("schema master requires schema_master")
	}
	return nil
}

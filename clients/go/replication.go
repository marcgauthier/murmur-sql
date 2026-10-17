package murmur

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"strconv"
	"strings"
)

// PeerInfo is one row of MURMUR SHOW PEERS: a connected replication peer.
type PeerInfo struct {
	NodeID            string
	Address           *string
	Connected         bool
	LastSeenMillisAgo *int64
	SentWatermark     int64
	SchemaAgreed      bool
	PushMode          bool
}

// ReplicationStatus is the single row of MURMUR SHOW REPLICATION: the local
// replication summary. Committed is the local engine committed-batch cursor.
type ReplicationStatus struct {
	Running             bool
	Address             *string
	Peers               int64
	Members             int64
	StagedTransactions  int64
	AppliedTransactions int64
	LastError           *string
	Committed           int64
}

// RetentionEntry is one row of MURMUR SHOW RETENTION: a peer's
// acknowledgement of one origin. Peers with no acknowledgements yield one
// row with Origin and Acknowledged unset; the trailing counters repeat.
type RetentionEntry struct {
	NodeID                string
	LastSeenMillis        int64
	Retired               bool
	Origin                *string
	Acknowledged          *int64
	CollectedTransactions int64
	LastCollectionMillis  int64
}

func checkNodeID(node string) error {
	if len(node) != 32 {
		return fmt.Errorf("murmur: node id must be 32 hex characters, not %q", node)
	}
	for i := 0; i < len(node); i++ {
		c := node[i]
		if c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' {
			continue
		}
		return fmt.Errorf("murmur: node id must be 32 hex characters, not %q", node)
	}
	return nil
}

// PeerAdd dials addr (ip:port) and admits the peer to the mesh, clearing any
// earlier Remove exclusion for its node id. It succeeds once queued; dial
// failures surface asynchronously in ShowReplication (LastError). Without
// enabled replication the server fails with code 55000.
func (db *DB) PeerAdd(ctx context.Context, addr string) error {
	if _, err := netip.ParseAddrPort(addr); err != nil {
		return fmt.Errorf("murmur: invalid peer address %q", addr)
	}
	_, err := db.ExecContext(ctx, "MURMUR PEER ADD "+quoteLiteral(addr))
	return err
}

// PeerRemove disconnects and durably excludes node (32 hex characters);
// reversible with PeerAdd. The peer's retention obligation is kept.
func (db *DB) PeerRemove(ctx context.Context, node string) error {
	if err := checkNodeID(node); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, "MURMUR PEER REMOVE "+quoteLiteral(node))
	return err
}

// PeerRetire permanently excludes node and releases its retention
// obligation. There is no un-retire; retiring the local node fails with
// code 22023.
func (db *DB) PeerRetire(ctx context.Context, node string) error {
	if err := checkNodeID(node); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, "MURMUR PEER RETIRE "+quoteLiteral(node))
	return err
}

// PeerSync asks every connected peer for progress now. It only requests
// state; it does not wait for convergence.
func (db *DB) PeerSync(ctx context.Context) error {
	_, err := db.ExecContext(ctx, "MURMUR PEER SYNC")
	return err
}

// ShowPeers lists connected peers ordered by node id (empty when peerless).
func (db *DB) ShowPeers(ctx context.Context) ([]PeerInfo, error) {
	rows, err := db.QueryContext(ctx, "MURMUR SHOW PEERS")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var peers []PeerInfo
	for rows.Next() {
		var p PeerInfo
		if err := rows.Scan(&p.NodeID, &p.Address, &p.Connected, &p.LastSeenMillisAgo,
			&p.SentWatermark, &p.SchemaAgreed, &p.PushMode); err != nil {
			return nil, err
		}
		peers = append(peers, p)
	}
	return peers, rows.Err()
}

// ShowReplication returns the local replication summary (one row).
func (db *DB) ShowReplication(ctx context.Context) (ReplicationStatus, error) {
	var s ReplicationStatus
	err := db.QueryRowContext(ctx, "MURMUR SHOW REPLICATION").Scan(
		&s.Running, &s.Address, &s.Peers, &s.Members,
		&s.StagedTransactions, &s.AppliedTransactions, &s.LastError, &s.Committed)
	return s, err
}

// ShowRetention lists per-peer retention state, ordered by node id then origin.
func (db *DB) ShowRetention(ctx context.Context) ([]RetentionEntry, error) {
	rows, err := db.QueryContext(ctx, "MURMUR SHOW RETENTION")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []RetentionEntry
	for rows.Next() {
		var e RetentionEntry
		if err := rows.Scan(&e.NodeID, &e.LastSeenMillis, &e.Retired, &e.Origin,
			&e.Acknowledged, &e.CollectedTransactions, &e.LastCollectionMillis); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// RetentionCollect runs one bounded retention pass now and returns the
// number of deleted transaction log records.
func (db *DB) RetentionCollect(ctx context.Context) (int64, error) {
	var collected int64
	err := db.QueryRowContext(ctx, "MURMUR RETENTION COLLECT").Scan(&collected)
	return collected, err
}

func checkPolicyTarget(table, column string) error {
	if strings.TrimSpace(table) == "" || strings.TrimSpace(column) == "" {
		return errors.New("murmur: policy table and column must not be empty")
	}
	return nil
}

// policyLiteral renders a MIN/MAX value the way the server parses it:
// integers bare, finite floats bare (never integer-looking, so 100.0 stays
// REAL), strings quoted. Supported types: ints, uints (within int64),
// float32/64, string.
func policyLiteral(value any) (string, error) {
	switch v := value.(type) {
	case int:
		return strconv.Itoa(v), nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint:
		return policyUint(uint64(v))
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return policyUint(v)
	case float32:
		return policyReal(float64(v))
	case float64:
		return policyReal(v)
	case string:
		return quoteLiteral(v), nil
	default:
		return "", fmt.Errorf("murmur: unsupported policy value type %T", value)
	}
}

func policyUint(v uint64) (string, error) {
	if v > math.MaxInt64 {
		return "", fmt.Errorf("murmur: policy integer %d overflows int64", v)
	}
	return strconv.FormatUint(v, 10), nil
}

func policyReal(f float64) (string, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "", fmt.Errorf("murmur: policy value must be finite, not %v", f)
	}
	s := strconv.FormatFloat(f, 'g', -1, 64)
	if !strings.ContainsAny(s, "./eE") {
		s += ".0"
	}
	return s, nil
}

// PolicyCounterAdd adds delta to a counter column on the single-TEXT-key row.
// The column must carry the matching replication.merge_policies entry (code
// 22023 otherwise); a missing row fails with code 02000.
func (db *DB) PolicyCounterAdd(ctx context.Context, table, column, key string, delta int64) error {
	if err := checkPolicyTarget(table, column); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, "MURMUR POLICY COUNTER_ADD "+quoteLiteral(table)+
		" "+quoteLiteral(column)+" "+quoteLiteral(key)+" "+strconv.FormatInt(delta, 10))
	return err
}

// PolicySetAdd adds a TEXT element to a set column (see PolicyCounterAdd for
// requirements).
func (db *DB) PolicySetAdd(ctx context.Context, table, column, key, value string) error {
	if err := checkPolicyTarget(table, column); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, "MURMUR POLICY SET_ADD "+quoteLiteral(table)+
		" "+quoteLiteral(column)+" "+quoteLiteral(key)+" "+quoteLiteral(value))
	return err
}

// PolicySetRemove removes a TEXT element from a set column.
func (db *DB) PolicySetRemove(ctx context.Context, table, column, key, value string) error {
	if err := checkPolicyTarget(table, column); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, "MURMUR POLICY SET_REMOVE "+quoteLiteral(table)+
		" "+quoteLiteral(column)+" "+quoteLiteral(key)+" "+quoteLiteral(value))
	return err
}

// PolicyMin lowers a min column towards value (int, finite float, or string).
func (db *DB) PolicyMin(ctx context.Context, table, column, key string, value any) error {
	return db.policyMinMax(ctx, "MIN", table, column, key, value)
}

// PolicyMax raises a max column towards value (int, finite float, or string).
func (db *DB) PolicyMax(ctx context.Context, table, column, key string, value any) error {
	return db.policyMinMax(ctx, "MAX", table, column, key, value)
}

func (db *DB) policyMinMax(ctx context.Context, op, table, column, key string, value any) error {
	if err := checkPolicyTarget(table, column); err != nil {
		return err
	}
	literal, err := policyLiteral(value)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "MURMUR POLICY "+op+" "+quoteLiteral(table)+
		" "+quoteLiteral(column)+" "+quoteLiteral(key)+" "+literal)
	return err
}

func checkBridgeTarget(table string, column []string) error {
	if strings.TrimSpace(table) == "" {
		return errors.New("murmur: bridge table must not be empty")
	}
	if len(column) > 1 {
		return errors.New("murmur: bridge release takes at most one column")
	}
	if len(column) == 1 && strings.TrimSpace(column[0]) == "" {
		return errors.New("murmur: bridge column must not be empty")
	}
	return nil
}

// BridgeRelease releases High ownership of one bridge-shielded field on the
// single-TEXT-key row, so later Low updates flow through again. Without a
// column the whole-row ownership is released. It reports true when applied;
// false means the engine was busy, retry. Without enabled replication or a
// configured bridge the server fails with code 55000; a Low exporter, an
// unknown table, or a field that is not High-owned fails with code 22023.
func (db *DB) BridgeRelease(ctx context.Context, table, key string, column ...string) (bool, error) {
	if err := checkBridgeTarget(table, column); err != nil {
		return false, err
	}
	stmt := "MURMUR BRIDGE RELEASE " + quoteLiteral(table) + " " + quoteLiteral(key)
	if len(column) == 1 {
		stmt += " COLUMN " + quoteLiteral(column[0])
	}
	var released bool
	err := db.QueryRowContext(ctx, stmt).Scan(&released)
	return released, err
}

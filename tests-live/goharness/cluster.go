// Multi-process cluster: one murmur-server child per node, driven over the
// PostgreSQL wire protocol.
//
// A "node" is a child process on its own directory
// (runtime/<scenario>/nodeN/) with its own config.yaml, TLS material
// (tls/), and data dir. Stop, restart, kill, and (on unix) suspend legs
// act on the child directly, so no fixture binary is needed. SQL runs
// through NodeClient; replication topology through MURMUR PEER ...
// commands; unlock through MURMUR UNLOCK KEY '<64-hex>'.
//
// Restarts pick a fresh SQL port and re-render config.yaml from the
// stored inputs, preserving the server-owned replication.tables map
// (see MURMUR SET TABLE). The replication socket binds :0; its address
// is discovered via MURMUR SHOW REPLICATION, so re-mesh (AddPeer /
// Mesh) after every restart.
//
// A Cluster is not safe for concurrent use (except concurrent NodeClient
// operations, which serialize on their connections).
package goharness

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
)

// NodeID is a 16-byte node identity.
type NodeID = [16]byte

const (
	pgUser     = "admin"
	pgPassword = "test-password"
)

// startTimeout bounds the SQL-port wait for a spawned server.
const startTimeout = 20 * time.Second

// binaries locates (server, admin): MURMUR_SERVER_BIN or a Go build once
// per process.
var (
	binariesOnce      sync.Once
	binariesServer    string
	binariesAdmin     string
	binariesErr       error
	passwordHashOnce  sync.Once
	passwordHashValue string
	passwordHashErr   error
)

func binaries() (server, admin string, err error) {
	binariesOnce.Do(func() {
		if server := os.Getenv("MURMUR_SERVER_BIN"); server != "" {
			binariesServer = server
			binariesAdmin = filepath.Join(filepath.Dir(server), exeName("murmur-admin"))
			return
		}
		root := filepath.Dir(TestsLiveDir())
		target := filepath.Join(root, "bin")
		if err := os.MkdirAll(target, 0o755); err != nil {
			binariesErr = err
			return
		}
		for _, name := range []string{"murmur-server", "murmur-admin"} {
			cmd := exec.Command("go", "build", "-o", filepath.Join(target, exeName(name)), "./cmd/"+name)
			cmd.Dir = root
			if out, err := cmd.CombinedOutput(); err != nil {
				binariesErr = fmt.Errorf("harness setup: go build %s: %v: %s", name, err, out)
				return
			}
		}
		binariesServer = filepath.Join(target, exeName("murmur-server"))
		binariesAdmin = filepath.Join(target, exeName("murmur-admin"))
	})
	return binariesServer, binariesAdmin, binariesErr
}

func exeName(base string) string {
	if runtime.GOOS == "windows" {
		return base + ".exe"
	}
	return base
}

func passwordHash() (string, error) {
	passwordHashOnce.Do(func() {
		_, admin, err := binaries()
		if err != nil {
			passwordHashErr = err
			return
		}
		out, err := exec.Command(admin, "hash-password", "--password", pgPassword).Output()
		if err != nil {
			passwordHashErr = fmt.Errorf("harness setup: murmur-admin hash-password: %w", err)
			return
		}
		hash := strings.TrimSpace(string(out))
		if !strings.HasPrefix(hash, "$argon2id$") {
			passwordHashErr = fmt.Errorf("harness setup: unexpected password hash %q", hash)
			return
		}
		passwordHashValue = hash
	})
	return passwordHashValue, passwordHashErr
}

// MergePolicy overlays a config replication.merge_policies value.
type MergePolicy int

const (
	// MergePolicyLWW is last-writer-wins.
	MergePolicyLWW MergePolicy = iota
	// MergePolicyCounter is a CRDT counter.
	MergePolicyCounter
	// MergePolicySet is a CRDT set.
	MergePolicySet
	// MergePolicyMin keeps the minimum.
	MergePolicyMin
	// MergePolicyMax keeps the maximum.
	MergePolicyMax
)

func (p MergePolicy) yaml() string {
	switch p {
	case MergePolicyLWW:
		return "lww"
	case MergePolicyCounter:
		return "counter"
	case MergePolicySet:
		return "set"
	case MergePolicyMin:
		return "min"
	case MergePolicyMax:
		return "max"
	}
	return "lww"
}

// MergeColumn identifies one (table, column) merge-policy overlay.
type MergeColumn struct {
	Table  string
	Column string
}

// ReplicationOptions holds overlays rendered into every node's
// replication section.
type ReplicationOptions struct {
	Retention *time.Duration
	// RetentionInterval is the collection cadence (default 60 s).
	// GC-balance legs set this to seconds so background passes run inside
	// the measurement window instead of starving it.
	RetentionInterval   *time.Duration
	MinimumRetained     *uint64
	Fanout              *int
	Dissemination       *replication.Dissemination
	MaxTransactionBytes *int
	// BatchBytes is the send/apply batching unit (default 4 MiB).
	// Sessions larger than this ship as multiple signed transactions;
	// txchunk legs raise it so a multi-MiB trial ships and applies as one
	// unit.
	BatchBytes *int
	// Membership toggles gossip membership (default true). False holds a
	// static topology (no discovery dialing); transaction push still runs
	// over the peer sessions.
	Membership *bool
	// MaximumOffline is the offline-peer memory for retention floors
	// (default 7 days). Peers unseen longer than this stop pinning
	// history, so survivors can GC ranges a stale node still needs
	// (snapshot legs).
	MaximumOffline *time.Duration
	// MergePolicies maps (table, column) -> policy, rendered into every
	// node's config before start. Policies bind at open, so CRDT legs
	// (typed-records, merge-policies) must set this; nil/empty leaves
	// LWW everywhere.
	MergePolicies map[MergeColumn]MergePolicy
}

// FilesOptions is the encrypted file/object store for every node
// (per-node dir under the node).
type FilesOptions struct {
	Key           [32]byte
	FetchInterval *time.Duration
	MaxFileBytes  *uint64
}

// DefaultFilesOptions returns FilesOptions with the standard test object
// key ([61; 32]).
func DefaultFilesOptions() *FilesOptions {
	var key [32]byte
	for i := range key {
		key[i] = 61
	}
	return &FilesOptions{Key: key}
}

// SqliteOptions holds commit-path budget overlays rendered into every
// node's engine section.
type SqliteOptions struct {
	TransactionBytes *int
	QueueBytes       *int
	CaptureBytes     *int
	// CaptureInterval is the background session-capture cadence (default
	// 1 s). Explicit flushes capture regardless; txchunk legs stretch
	// this so a multi-MiB trial ships as one capture on its flush.
	CaptureInterval *time.Duration
}

// SpoolOptions holds per-node spool storage overrides
// (resource-compaction legs size victim segments down).
type SpoolOptions struct {
	// SegmentBytes is the spool segment rotation size (default 256 MiB).
	SegmentBytes *int
}

// BridgeNodeConfig is one node's replication.bridge section: a Low
// exporter or High receiver. Paths are used as given (scenarios share
// drop directories between Low exporters and High receivers); the
// harness does not rewrite them. Unset optionals take server defaults.
type BridgeNodeConfig struct {
	// Role is low-exporter or high-receiver.
	Role string
	// Domain is the database id the bridge serves, 32 hex chars.
	Domain         string
	TrustedDomains []string
	// TrustedOrigins maps node id (32 hex) to Ed25519 public key (64 hex).
	TrustedOrigins map[string]string
	Directory      string
	Stream         string
	// Recipient is the X25519 recipient public key, 64 hex chars.
	Recipient         string
	ReceiverSecretHex *string
	SignerHex         *string
	TrustedSigners    map[string]string
	MaxBundleBytes    *int
	MaxPayloadBytes   *int
	StagingBytes      *int
}

// BridgeOptions holds per-node bridge configs, keyed by node index.
type BridgeOptions struct {
	Nodes map[int]BridgeNodeConfig
}

// ClusterOptions holds cluster construction options. Build from
// DefaultClusterOptions and mutate fields.
type ClusterOptions struct {
	// Name is the scenario name; it owns runtime/<name>/.
	Name string
	// NumNodes is the node count. Default 3.
	NumNodes int
	// DatabaseID is the shared database id. Default: random per cluster.
	DatabaseID *[16]byte
	// NodeIDs holds node ids. Default: random per node (must be NumNodes
	// long if set).
	NodeIDs [][16]byte
	// Key is the unlock key for every node. Default: fixed test key
	// [9; 32] (hex-encoded for MURMUR UNLOCK, so used raw).
	Key [32]byte
	// Replication holds replication overlays for every node.
	Replication *ReplicationOptions
	// Sqlite holds commit-path budget overlays for every node.
	Sqlite *SqliteOptions
	// SpoolByNode holds per-node spool storage overrides.
	SpoolByNode map[int]SpoolOptions
	// ManualPeers skips the automatic full mesh; the test calls AddPeer.
	ManualPeers bool
	// BootstrapSeeds holds seed indices: started first, and every other
	// node dials them via bootstrap addrs (SWIM/seed-discovery legs).
	// Empty disables seeding.
	BootstrapSeeds []int
	// AllowedPeers holds per-node identity allow-lists.
	AllowedPeers map[int]map[[16]byte]bool
	// AllowedNetworks holds per-node address-policy overlays.
	AllowedNetworks map[int][]netip.Prefix
	// TrustedSnapshotSources holds per-node snapshot-donor trust. Empty
	// by default: nodes request snapshots from nobody. Snapshot
	// scenarios list explicit donor ids here.
	TrustedSnapshotSources map[int]map[[16]byte]bool
	// ExtraTrustedOrigins holds per-node extra entries merged into
	// trusted_origins (e.g. an attacker signing key for hostile-source
	// legs).
	ExtraTrustedOrigins map[int]map[[16]byte][32]byte
	// DisseminationByNode holds per-node dissemination-mode overrides
	// (mixed-mode legs; wins over the global dissemination for the
	// listed nodes).
	DisseminationByNode map[int]replication.Dissemination
	// Files is the file/object store for every node.
	Files *FilesOptions
	// Bridge holds bridge configs for individual nodes.
	Bridge BridgeOptions
	// NodeCertTTL holds per-node leaf-cert TTL overrides (default
	// validity is effectively unbounded; cert-lifecycle uses short TTLs
	// for expiry legs).
	NodeCertTTL map[int]time.Duration
	// ExtraYAML is raw YAML merged into every node's config (complete
	// sections; must not redefine top-level keys the renderer owns in a
	// conflicting way).
	//
	// Deviation: the Rust field is extra_toml (TOML); the Go server only
	// accepts YAML, so this port takes YAML. Same for ExtraYAMLByNode.
	ExtraYAML string
	// ExtraYAMLByNode holds per-node raw YAML merged after ExtraYAML.
	ExtraYAMLByNode map[int]string
}

// DefaultClusterOptions returns ClusterOptions with defaults (3 nodes,
// fixed test key).
func DefaultClusterOptions(name string) ClusterOptions {
	var key [32]byte
	for i := range key {
		key[i] = 9
	}
	return ClusterOptions{Name: name, NumNodes: 3, Key: key}
}

// Node is one cluster node: dirs, identity, and addresses.
type Node struct {
	Index int
	// Label is node1 .. nodeN.
	Label string
	// Dir is runtime/<scenario>/nodeN/ (config, tls/, forensics files).
	Dir string
	// DataDir is the database storage dir (<dir>/data/).
	DataDir string
	NodeID  [16]byte
	// ReplAddr is the bound replication address (ephemeral; changes
	// across restarts).
	ReplAddr netip.AddrPort
	// SQLAddr is the SQL listen address (picked free; changes across
	// restarts).
	SQLAddr netip.AddrPort
}

// NodeClient is one unlocked wire connection to a node. Share by
// reference; concurrent queries serialize on the connection.
type NodeClient struct {
	client *Client
}

// Inner returns the underlying driver client for application SQL.
func (n *NodeClient) Inner() *Client { return n.client }

// Execute runs a statement with $1, $2, ... parameters.
func (n *NodeClient) Execute(sql string, params ...any) (int64, error) {
	return n.client.Execute(sql, params...)
}

// ExecuteTimeout runs a statement with $1, $2, ... parameters and an I/O
// deadline, returning the affected row count.
func (n *NodeClient) ExecuteTimeout(sql string, d time.Duration, params ...any) (int64, error) {
	return n.client.ExecuteTimeout(sql, d, params...)
}

// ExecuteRetry runs a statement, retrying replayable 40001 pressure
// errors with backoff until timeout (the documented contract: back off
// and retry; e.g. a write immediately after a budget rejection can trip
// momentary queue pressure). Non-40001 errors return immediately.
func (n *NodeClient) ExecuteRetry(sql string, params []any, timeout time.Duration) (int64, error) {
	end := time.Now().Add(timeout)
	for {
		affected, err := n.client.Execute(sql, params...)
		if err == nil {
			return affected, nil
		}
		if ErrorCode(err) != "40001" || !time.Now().Before(end) {
			return 0, err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Query runs a query with $1, $2, ... parameters.
func (n *NodeClient) Query(sql string, params ...any) ([]Row, error) {
	return n.client.Query(sql, params...)
}

// Simple runs a parameterless command through the simple protocol; every
// value is returned as text (NULL becomes an empty string).
func (n *NodeClient) Simple(sql string) ([][]string, error) {
	return n.client.Simple(sql)
}

// BatchExecute runs parameterless statements through the simple protocol,
// discarding rows (BEGIN/COMMIT batches).
func (n *NodeClient) BatchExecute(sql string) error {
	return n.client.BatchExecute(sql)
}

// Close terminates the connection.
func (n *NodeClient) Close() error {
	return n.client.Close()
}

// nodeInputs holds per-node config inputs, stored for restarts.
// config.yaml is re-rendered from these on every (re)start; the
// server-owned replication.tables map is preserved across renders.
type nodeInputs struct {
	key                    [32]byte
	databaseID             [16]byte
	nodeID                 [16]byte
	signingHex             string
	trustedOrigins         map[[16]byte][32]byte
	chainFile              string
	keyFile                string
	caFile                 string
	bootstrap              []netip.AddrPort
	retention              *time.Duration
	retentionInterval      *time.Duration
	minimumRetained        *uint64
	fanout                 *int
	dissemination          *replication.Dissemination
	maxTransactionBytes    *int
	batchBytes             *int
	membership             *bool
	maximumOffline         *time.Duration
	mergePolicies          map[MergeColumn]MergePolicy
	allowedPeers           map[[16]byte]bool
	allowedNetworks        []netip.Prefix
	trustedSnapshotSources map[[16]byte]bool
	files                  *FilesOptions
	bridge                 *BridgeNodeConfig
	sqlite                 *SqliteOptions
	spool                  *SpoolOptions
	extraYAML              string
}

// childProc is a spawned server with a single-flight waiter.
type childProc struct {
	cmd  *exec.Cmd
	done chan error
}

func spawnChild(server, configPath string) (*childProc, error) {
	cmd := exec.Command(server, "--config", configPath)
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	p := &childProc{cmd: cmd, done: make(chan error, 1)}
	go func() { p.done <- cmd.Wait() }()
	return p, nil
}

// exited reports whether the child has exited (non-blocking when d <= 0).
func (p *childProc) exited(d time.Duration) (bool, error) {
	if d <= 0 {
		select {
		case err := <-p.done:
			return true, err
		default:
			return false, nil
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case err := <-p.done:
		return true, err
	case <-timer.C:
		return false, nil
	}
}

type nodeState struct {
	node       Node
	configPath string
	child      *childProc
	client     *NodeClient
	inputs     nodeInputs
}

func yamlPath(path string) string {
	return filepath.ToSlash(path)
}

func formatDuration(d time.Duration) string {
	ms := d.Milliseconds()
	for _, u := range []struct {
		mult   int64
		suffix string
	}{{86400000, "d"}, {3600000, "h"}, {60000, "m"}, {1000, "s"}} {
		if ms > 0 && ms%u.mult == 0 {
			return fmt.Sprintf("%d%s", ms/u.mult, u.suffix)
		}
	}
	return fmt.Sprintf("%dms", ms)
}

func disseminationName(d replication.Dissemination) (string, error) {
	switch d {
	case replication.DisseminationGossip:
		return "gossip", nil
	case replication.DisseminationPlumtree:
		return "plumtree", nil
	}
	return "", fmt.Errorf("harness setup: unknown dissemination %d", int(d))
}

func sortedHexIDs(set map[[16]byte]bool) []string {
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, Hex(id[:]))
	}
	sort.Strings(out)
	return out
}

// renderConfig renders the node config as YAML. Tables carries the
// preserved server-owned replication.tables map (may be empty).
//
// Deviation: the Rust renderer emits TOML, which the Go server's config
// loader rejects (verified: yaml unmarshal error). This port emits the
// same keys and values as YAML, which config.Parse accepts.
func renderConfig(inputs *nodeInputs, nodeDir string, sqlPort uint16, passwordHash string, tables map[string]bool) ([]byte, error) {
	boot := make([]string, 0, len(inputs.bootstrap))
	for _, a := range inputs.bootstrap {
		boot = append(boot, a.String())
	}
	origins := make(map[string]any, len(inputs.trustedOrigins))
	for id, key := range inputs.trustedOrigins {
		origins[Hex(id[:])] = Hex(key[:])
	}
	repl := map[string]any{
		"enabled":          true,
		"bind":             "127.0.0.1:0",
		"bootstrap":        boot,
		"cert_chain_file":  yamlPath(inputs.chainFile),
		"private_key_file": yamlPath(inputs.keyFile),
		"ca_file":          yamlPath(inputs.caFile),
		"signing_key_hex":  inputs.signingHex,
		"trusted_origins":  origins,
	}
	if len(inputs.allowedPeers) > 0 {
		repl["allowed_peers"] = sortedHexIDs(inputs.allowedPeers)
	}
	if len(inputs.allowedNetworks) > 0 {
		nets := make([]string, 0, len(inputs.allowedNetworks))
		for _, n := range inputs.allowedNetworks {
			nets = append(nets, n.String())
		}
		repl["allowed_networks"] = nets
	}
	if len(inputs.trustedSnapshotSources) > 0 {
		repl["trusted_snapshot_sources"] = sortedHexIDs(inputs.trustedSnapshotSources)
	}
	if inputs.membership != nil {
		repl["membership"] = *inputs.membership
	}
	if inputs.dissemination != nil {
		name, err := disseminationName(*inputs.dissemination)
		if err != nil {
			return nil, err
		}
		repl["dissemination"] = name
	}
	if inputs.fanout != nil {
		repl["fanout"] = *inputs.fanout
	}
	if inputs.maxTransactionBytes != nil {
		repl["max_transaction_bytes"] = *inputs.maxTransactionBytes
	}
	if inputs.batchBytes != nil {
		repl["batch_bytes"] = *inputs.batchBytes
	}
	if inputs.retention != nil {
		repl["retention"] = formatDuration(*inputs.retention)
	}
	if inputs.retentionInterval != nil {
		repl["retention_interval"] = formatDuration(*inputs.retentionInterval)
	}
	if inputs.minimumRetained != nil {
		repl["minimum_retained"] = *inputs.minimumRetained
	}
	if inputs.maximumOffline != nil {
		repl["maximum_offline"] = formatDuration(*inputs.maximumOffline)
	}
	if len(inputs.mergePolicies) > 0 {
		policies := make(map[string]any, len(inputs.mergePolicies))
		for col, policy := range inputs.mergePolicies {
			policies[col.Table+"."+col.Column] = policy.yaml()
		}
		repl["merge_policies"] = policies
	}
	if inputs.files != nil {
		files := map[string]any{
			"directory": yamlPath(filepath.Join(nodeDir, "objects")),
			"key_hex":   Hex(inputs.files.Key[:]),
		}
		if inputs.files.FetchInterval != nil {
			files["fetch_interval"] = formatDuration(*inputs.files.FetchInterval)
		}
		if inputs.files.MaxFileBytes != nil {
			files["max_file_bytes"] = *inputs.files.MaxFileBytes
		}
		repl["files"] = files
	}
	if inputs.bridge != nil {
		b := inputs.bridge
		bridge := map[string]any{
			"role":      b.Role,
			"domain":    b.Domain,
			"directory": b.Directory,
			"stream":    b.Stream,
			"recipient": b.Recipient,
		}
		if len(b.TrustedDomains) > 0 {
			bridge["trusted_domains"] = append([]string(nil), b.TrustedDomains...)
		}
		if len(b.TrustedOrigins) > 0 {
			origins := make(map[string]any, len(b.TrustedOrigins))
			for k, v := range b.TrustedOrigins {
				origins[k] = v
			}
			bridge["trusted_origins"] = origins
		}
		if b.ReceiverSecretHex != nil {
			bridge["receiver_secret_hex"] = *b.ReceiverSecretHex
		}
		if b.SignerHex != nil {
			bridge["signer_hex"] = *b.SignerHex
		}
		if len(b.TrustedSigners) > 0 {
			signers := make(map[string]any, len(b.TrustedSigners))
			for k, v := range b.TrustedSigners {
				signers[k] = v
			}
			bridge["trusted_signers"] = signers
		}
		if b.MaxBundleBytes != nil {
			bridge["max_bundle_bytes"] = *b.MaxBundleBytes
		}
		if b.MaxPayloadBytes != nil {
			bridge["max_payload_bytes"] = *b.MaxPayloadBytes
		}
		if b.StagingBytes != nil {
			bridge["staging_bytes"] = *b.StagingBytes
		}
		repl["bridge"] = bridge
	}
	if len(tables) > 0 {
		flags := make(map[string]any, len(tables))
		for k, v := range tables {
			flags[k] = v
		}
		repl["tables"] = flags
	}
	root := map[string]any{
		"server": map[string]any{
			"bind":       fmt.Sprintf("127.0.0.1:%d", sqlPort),
			"production": false,
		},
		"storage": map[string]any{
			"data_dir":    yamlPath(filepath.Join(nodeDir, "data")),
			"database_id": Hex(inputs.databaseID[:]),
			"node_id":     Hex(inputs.nodeID[:]),
		},
		"auth": map[string]any{
			"users": []any{map[string]any{"name": "admin", "password_hash": passwordHash}},
		},
		"logging": map[string]any{
			"dir": yamlPath(filepath.Join(nodeDir, "logs")),
		},
		"backups": map[string]any{
			"dir": yamlPath(filepath.Join(nodeDir, "backups")),
		},
		"replication": repl,
	}
	engine := map[string]any{}
	if inputs.sqlite != nil {
		if inputs.sqlite.TransactionBytes != nil {
			engine["transaction_bytes"] = *inputs.sqlite.TransactionBytes
		}
		if inputs.sqlite.QueueBytes != nil {
			engine["queue_bytes"] = *inputs.sqlite.QueueBytes
		}
		if inputs.sqlite.CaptureBytes != nil {
			engine["capture_bytes"] = *inputs.sqlite.CaptureBytes
		}
		if inputs.sqlite.CaptureInterval != nil {
			engine["capture_interval"] = formatDuration(*inputs.sqlite.CaptureInterval)
		}
	}
	if inputs.spool != nil && inputs.spool.SegmentBytes != nil {
		engine["segment_bytes"] = *inputs.spool.SegmentBytes
	}
	if len(engine) > 0 {
		root["engine"] = engine
	}
	if strings.TrimSpace(inputs.extraYAML) != "" {
		var frag map[string]any
		if err := yaml.Unmarshal([]byte(inputs.extraYAML), &frag); err != nil {
			return nil, fmt.Errorf("harness setup: invalid extra YAML: %w", err)
		}
		deepMerge(root, frag)
	}
	return yaml.Marshal(root)
}

func deepMerge(dst, src map[string]any) {
	for k, v := range src {
		if sv, ok := v.(map[string]any); ok {
			if dv, ok := dst[k].(map[string]any); ok {
				deepMerge(dv, sv)
				continue
			}
		}
		dst[k] = v
	}
}

// preservedTables extracts the server-owned replication.tables map from an
// existing config file so restarts keep MURMUR SET TABLE flags.
func preservedTables(configPath string) map[string]bool {
	out := map[string]bool{}
	body, err := os.ReadFile(configPath)
	if err != nil {
		return out
	}
	var doc map[string]any
	if err := yaml.Unmarshal(body, &doc); err != nil {
		return out
	}
	repl, ok := doc["replication"].(map[string]any)
	if !ok {
		return out
	}
	tables, ok := repl["tables"].(map[string]any)
	if !ok {
		return out
	}
	for k, v := range tables {
		switch t := v.(type) {
		case bool:
			out[k] = t
		case string:
			if t == "true" {
				out[k] = true
			} else if t == "false" {
				out[k] = false
			}
		}
	}
	return out
}

// serverLogTail returns the tail of the newest server log file for
// early-exit diagnostics (best effort).
func serverLogTail(nodeDir string) string {
	entries, err := os.ReadDir(filepath.Join(nodeDir, "logs"))
	if err != nil {
		return ""
	}
	var newest string
	var newestTime time.Time
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		if newest == "" || info.ModTime().After(newestTime) {
			newestTime = info.ModTime()
			newest = filepath.Join(nodeDir, "logs", e.Name())
		}
	}
	if newest == "" {
		return ""
	}
	body, err := os.ReadFile(newest)
	if err != nil {
		return ""
	}
	if len(body) > 2048 {
		body = body[len(body)-2048:]
	}
	return fmt.Sprintf("\n--- %s tail ---\n%s", newest, body)
}

// startProcess writes the node config, spawns the server, waits for the
// SQL port, unlocks, and discovers the bound replication address.
func startProcess(label, nodeDir, configPath string, inputs *nodeInputs, passwordHash string) (*childProc, *NodeClient, netip.AddrPort, netip.AddrPort, error) {
	sqlPort, err := FreePort()
	if err != nil {
		return nil, nil, netip.AddrPort{}, netip.AddrPort{}, fmt.Errorf("harness setup: %s: no free SQL port: %w", label, err)
	}
	config, err := renderConfig(inputs, nodeDir, sqlPort, passwordHash, preservedTables(configPath))
	if err != nil {
		return nil, nil, netip.AddrPort{}, netip.AddrPort{}, err
	}
	if err := os.WriteFile(configPath, config, 0o644); err != nil {
		return nil, nil, netip.AddrPort{}, netip.AddrPort{}, err
	}
	server, _, err := binaries()
	if err != nil {
		return nil, nil, netip.AddrPort{}, netip.AddrPort{}, err
	}
	child, err := spawnChild(server, configPath)
	if err != nil {
		return nil, nil, netip.AddrPort{}, netip.AddrPort{}, err
	}
	sqlAddr := netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), sqlPort)
	deadline := time.Now().Add(startTimeout)
	for {
		conn, err := net.DialTimeout("tcp", sqlAddr.String(), 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		if exited, status := child.exited(0); exited {
			return nil, nil, netip.AddrPort{}, netip.AddrPort{}, fmt.Errorf("harness setup: %s: server exited early: %v%s", label, status, serverLogTail(nodeDir))
		}
		if time.Now().After(deadline) {
			reapChild(child, label)
			return nil, nil, netip.AddrPort{}, netip.AddrPort{}, fmt.Errorf("harness setup: %s: server on port %d did not start", label, sqlPort)
		}
		time.Sleep(50 * time.Millisecond)
	}
	client, err := ConnectWithWatchdog(sqlAddr, pgUser, pgPassword, Hex(inputs.key[:]))
	if err != nil {
		reapChild(child, label)
		return nil, nil, netip.AddrPort{}, netip.AddrPort{}, err
	}
	replAddr, err := replicationAddr(client, label)
	if err != nil {
		_ = client.Close()
		reapChild(child, label)
		return nil, nil, netip.AddrPort{}, netip.AddrPort{}, err
	}
	return child, client, sqlAddr, replAddr, nil
}

// replicationAddr returns the bound replication address, waiting briefly
// for the endpoint to come up.
func replicationAddr(client *NodeClient, label string) (netip.AddrPort, error) {
	end := time.Now().Add(10 * time.Second)
	for {
		if rows, err := client.Simple("MURMUR SHOW REPLICATION"); err == nil && len(rows) > 0 {
			if row := rows[0]; len(row) >= 2 && row[1] != "" {
				if addr, err := netip.ParseAddrPort(row[1]); err == nil {
					return addr, nil
				}
			}
		}
		if !time.Now().Before(end) {
			return netip.AddrPort{}, fmt.Errorf("harness setup: %s: replication endpoint never bound", label)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// reapChild kills (SIGKILL) and reaps the child with a bounded wait.
func reapChild(child *childProc, label string) {
	_ = child.cmd.Process.Kill()
	if exited, _ := child.exited(10 * time.Second); !exited {
		fmt.Fprintf(os.Stderr, "%s: child did not exit after kill\n", label)
	}
}

// Cluster is a multi-process cluster: one murmur-server child per node.
//
// Deviation: Rust frees children in Drop (panic paths included). Go has
// no equivalent; Finish kills and reaps, and run.sh sweeps stragglers
// scoped to the runtime dir as a backstop.
type Cluster struct {
	name       string
	runtimeDir string
	databaseID [16]byte
	states     []nodeState
	// ca is the cluster CA, kept so scenarios can issue certs for
	// external identities (e.g. a hostile peer speaking the wire
	// protocol).
	ca *Ca
}

// NewCluster builds node dirs + certs, starts every node (seeds first),
// and meshes unless ManualPeers. It wipes any previous
// runtime/<name>/ first.
func NewCluster(opts ClusterOptions) (*Cluster, error) {
	if opts.NumNodes == 0 {
		return nil, fmt.Errorf("harness setup: num_nodes must be >= 1")
	}
	if opts.Name == "" {
		return nil, fmt.Errorf("harness setup: ClusterOptions.Name must be set")
	}
	nodeIDs := opts.NodeIDs
	if len(nodeIDs) == 0 {
		nodeIDs = make([][16]byte, opts.NumNodes)
		for i := range nodeIDs {
			if _, err := rand.Read(nodeIDs[i][:]); err != nil {
				return nil, fmt.Errorf("harness setup: random node id: %w", err)
			}
		}
	} else if len(nodeIDs) != opts.NumNodes {
		return nil, fmt.Errorf("harness setup: node_ids has %d entries for %d nodes", len(nodeIDs), opts.NumNodes)
	}
	distinct := map[[16]byte]bool{}
	for _, id := range nodeIDs {
		distinct[id] = true
	}
	if len(distinct) != len(nodeIDs) {
		return nil, fmt.Errorf("harness setup: node ids must be distinct")
	}
	for _, s := range opts.BootstrapSeeds {
		if s < 0 || s >= opts.NumNodes {
			return nil, fmt.Errorf("harness setup: bootstrap seed %d out of range", s)
		}
	}

	runtimeDir, err := PrepareRuntimeDir(opts.Name)
	if err != nil {
		return nil, err
	}
	var databaseID [16]byte
	if opts.DatabaseID != nil {
		databaseID = *opts.DatabaseID
	} else if _, err := rand.Read(databaseID[:]); err != nil {
		return nil, fmt.Errorf("harness setup: random database id: %w", err)
	}

	// Signing keys + trust: every node trusts every node's origin key.
	seeds := make([][32]byte, opts.NumNodes)
	trustedBase := map[[16]byte][32]byte{}
	for i := range seeds {
		if _, err := rand.Read(seeds[i][:]); err != nil {
			return nil, fmt.Errorf("harness setup: random origin seed: %w", err)
		}
		priv := ed25519.NewKeyFromSeed(seeds[i][:])
		var pub [32]byte
		copy(pub[:], priv.Public().(ed25519.PublicKey))
		trustedBase[nodeIDs[i]] = pub
	}

	ca, err := GenerateCa()
	if err != nil {
		return nil, fmt.Errorf("harness setup: generate CA: %w", err)
	}
	hash, err := passwordHash()
	if err != nil {
		return nil, err
	}

	// Start order: seeds first so their ephemeral addrs can seed the rest.
	seedSet := map[int]bool{}
	for _, s := range opts.BootstrapSeeds {
		seedSet[s] = true
	}
	order := append([]int(nil), opts.BootstrapSeeds...)
	sort.Ints(order)
	deduped := order[:0]
	for _, s := range order {
		if len(deduped) == 0 || deduped[len(deduped)-1] != s {
			deduped = append(deduped, s)
		}
	}
	order = deduped
	for i := 0; i < opts.NumNodes; i++ {
		if !seedSet[i] {
			order = append(order, i)
		}
	}

	states := make([]nodeState, opts.NumNodes)
	started := make([]bool, opts.NumNodes)
	var seedAddrs []netip.AddrPort
	failed := false
	defer func() {
		if failed {
			for i := range states {
				if !started[i] {
					continue
				}
				if states[i].client != nil {
					_ = states[i].client.Close()
				}
				if states[i].child != nil {
					reapChild(states[i].child, states[i].node.Label)
				}
			}
		}
	}()
	for _, i := range order {
		label := fmt.Sprintf("node%d", i+1)
		dir := filepath.Join(runtimeDir, label)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			failed = true
			return nil, err
		}
		var ttl *time.Duration
		if d, ok := opts.NodeCertTTL[i]; ok {
			ttl = &d
		}
		issued, err := ca.Issue(nodeIDs[i], ttl)
		if err != nil {
			failed = true
			return nil, fmt.Errorf("harness setup: issue node cert: %w", err)
		}
		tlsPaths, err := ca.WriteTLSDir(issued, dir)
		if err != nil {
			failed = true
			return nil, err
		}
		trusted := map[[16]byte][32]byte{}
		for k, v := range trustedBase {
			trusted[k] = v
		}
		for k, v := range opts.ExtraTrustedOrigins[i] {
			trusted[k] = v
		}
		nodeExtra := opts.ExtraYAMLByNode[i]
		inputs := nodeInputs{
			key:            opts.Key,
			databaseID:     databaseID,
			nodeID:         nodeIDs[i],
			signingHex:     Hex(seeds[i][:]),
			trustedOrigins: trusted,
			chainFile:      tlsPaths.Chain,
			keyFile:        tlsPaths.Key,
			caFile:         tlsPaths.Ca,
			extraYAML:      strings.Join([]string{opts.ExtraYAML, nodeExtra}, "\n"),
		}
		if len(seedAddrs) > 0 && !seedSet[i] {
			inputs.bootstrap = append(inputs.bootstrap, seedAddrs...)
		}
		if opts.Replication != nil {
			r := opts.Replication
			inputs.retention = r.Retention
			inputs.retentionInterval = r.RetentionInterval
			inputs.minimumRetained = r.MinimumRetained
			inputs.fanout = r.Fanout
			inputs.dissemination = r.Dissemination
			inputs.maxTransactionBytes = r.MaxTransactionBytes
			inputs.batchBytes = r.BatchBytes
			inputs.membership = r.Membership
			inputs.maximumOffline = r.MaximumOffline
			inputs.mergePolicies = r.MergePolicies
		}
		if d, ok := opts.DisseminationByNode[i]; ok {
			inputs.dissemination = &d
		}
		inputs.allowedPeers = opts.AllowedPeers[i]
		inputs.allowedNetworks = opts.AllowedNetworks[i]
		inputs.trustedSnapshotSources = opts.TrustedSnapshotSources[i]
		inputs.files = opts.Files
		if b, ok := opts.Bridge.Nodes[i]; ok {
			inputs.bridge = &b
		}
		inputs.sqlite = opts.Sqlite
		if s, ok := opts.SpoolByNode[i]; ok {
			inputs.spool = &s
		}
		configPath := filepath.Join(dir, "config.yaml")
		child, client, sqlAddr, replAddr, err := startProcess(label, dir, configPath, &inputs, hash)
		if err != nil {
			failed = true
			return nil, err
		}
		if seedSet[i] {
			seedAddrs = append(seedAddrs, replAddr)
		}
		states[i] = nodeState{
			node: Node{
				Index:    i,
				Label:    label,
				Dir:      dir,
				DataDir:  filepath.Join(dir, "data"),
				NodeID:   nodeIDs[i],
				ReplAddr: replAddr,
				SQLAddr:  sqlAddr,
			},
			configPath: configPath,
			child:      child,
			client:     client,
			inputs:     inputs,
		}
		started[i] = true
	}

	cluster := &Cluster{name: opts.Name, runtimeDir: runtimeDir, databaseID: databaseID, states: states, ca: ca}
	if !opts.ManualPeers {
		if err := cluster.Mesh(); err != nil {
			failed = true
			return nil, err
		}
	}
	return cluster, nil
}

// Name returns the scenario name.
func (c *Cluster) Name() string { return c.name }

// RuntimeDir returns the runtime/<scenario>/ root.
func (c *Cluster) RuntimeDir() string { return c.runtimeDir }

// DatabaseID returns the shared database id.
func (c *Cluster) DatabaseID() [16]byte { return c.databaseID }

// IssueCert issues a leaf certificate for an external identity (e.g. a
// hostile fixture peer) from the cluster CA.
func (c *Cluster) IssueCert(nodeID [16]byte) (*Issued, error) {
	return c.ca.Issue(nodeID, nil)
}

// CaDER returns the DER bytes of the cluster CA certificate.
func (c *Cluster) CaDER() []byte { return c.ca.CaDER() }

// CaPEM returns the PEM of the cluster CA certificate, for cross-trust
// legs: hand it to a peer cluster's AddTrustRoot.
func (c *Cluster) CaPEM() string { return c.ca.CaPEM() }

// AddTrustRoot appends an external CA root (PEM bytes) to every node's
// trust file (deduped). It applies to disk state only: stop and StartNode
// every node afterwards (then re-mesh).
func (c *Cluster) AddTrustRoot(rootPEM []byte) {
	for i := range c.states {
		st := &c.states[i]
		current, _ := os.ReadFile(st.inputs.caFile)
		if len(rootPEM) == 0 || bytes.Contains(current, rootPEM) {
			continue
		}
		next := append(append([]byte(nil), current...), '\n')
		if len(current) > 0 && current[len(current)-1] == '\n' {
			next = append([]byte(nil), current...)
		}
		next = append(next, rootPEM...)
		if len(next) == 0 || next[len(next)-1] != '\n' {
			next = append(next, '\n')
		}
		if err := os.WriteFile(st.inputs.caFile, next, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "%s: trust-root append failed: %v\n", st.node.Label, err)
		}
	}
}

// Len returns the node count.
func (c *Cluster) Len() int { return len(c.states) }

// IsEmpty reports whether the cluster has no nodes.
func (c *Cluster) IsEmpty() bool { return len(c.states) == 0 }

func (c *Cluster) mustState(idx int) *nodeState {
	if idx < 0 || idx >= len(c.states) {
		panic(fmt.Sprintf("unknown node index %d", idx))
	}
	return &c.states[idx]
}

func (c *Cluster) getState(idx int) (*nodeState, error) {
	if idx < 0 || idx >= len(c.states) {
		return nil, &UnknownNodeError{Index: idx}
	}
	return &c.states[idx], nil
}

// NodeID returns the node id (available while stopped too). It panics on
// unknown index (a test bug).
func (c *Cluster) NodeID(idx int) [16]byte {
	return c.mustState(idx).node.NodeID
}

// NodeOriginPubkey returns the origin signing public key for node idx
// (ed25519 verifying-key bytes). Available while stopped too.
func (c *Cluster) NodeOriginPubkey(idx int) [32]byte {
	st := c.mustState(idx)
	pub, ok := st.inputs.trustedOrigins[st.node.NodeID]
	if !ok {
		panic(fmt.Sprintf("node%d lost its origin key", idx+1))
	}
	return pub
}

// NodeOriginSeed returns the origin signing seed of one node (decoded
// from its stored config input). Hostile-relay legs use it to craft
// signature attacks that isolate one check: a valid signature under the
// wrong key (fabricated origin) or under the right key over the wrong
// database (cross-DBID).
func (c *Cluster) NodeOriginSeed(idx int) [32]byte {
	st := c.mustState(idx)
	seed, ok := DecodeHex32(st.inputs.signingHex)
	if !ok {
		panic(fmt.Sprintf("node%d lost its origin seed", idx+1))
	}
	return seed
}

// NodeInfo returns node metadata (dirs, addrs; available while stopped
// too). It panics on unknown index (a test bug).
func (c *Cluster) NodeInfo(idx int) *Node {
	return &c.mustState(idx).node
}

// Node returns the open wire connection. It panics if the node is
// stopped or unknown (a test bug).
func (c *Cluster) Node(idx int) *NodeClient {
	st := c.mustState(idx)
	if st.client == nil {
		panic(fmt.Sprintf("node%d is stopped", idx+1))
	}
	return st.client
}

// Connect opens an independent client SQL connection for a concurrent
// worker.
func (c *Cluster) Connect(idx int) (*NodeClient, error) {
	st, err := c.getState(idx)
	if err != nil {
		return nil, err
	}
	return ConnectWithWatchdog(st.node.SQLAddr, pgUser, pgPassword, Hex(st.inputs.key[:]))
}

// NodePID returns the child PID, or false while stopped. It panics on
// unknown index.
func (c *Cluster) NodePID(idx int) (int, bool) {
	st := c.mustState(idx)
	if st.child == nil {
		return 0, false
	}
	return st.child.cmd.Process.Pid, true
}

// EnsureMigrated runs fixture DDL on one node during setup. Fast meshes
// converge the schema from an earlier node before a later node's
// identical statement runs; that "already exists" is the converged
// fixture, not a failure. Node 0 stays strict (nothing has replicated
// yet, so pre-existing tables there mean a dirty runtime dir); later
// nodes skip with a log line.
func (c *Cluster) EnsureMigrated(idx int, sql string) error {
	if _, err := c.Node(idx).Simple(sql); err != nil {
		if idx > 0 && strings.Contains(DBMessage(err), "already exists") {
			fmt.Fprintf(os.Stderr, "node%d: fixture schema already converged, skipping identical migrate\n", idx+1)
			return nil
		}
		return fmt.Errorf("harness setup: node%d fixture migrate failed: %w", idx+1, err)
	}
	_, err := c.Node(idx).Simple("MURMUR FLUSH")
	return err
}

// AddPeer dials target from from (both must be running; target addr is
// its current ephemeral bind). It clears any earlier exclusion for the
// target.
func (c *Cluster) AddPeer(from, target int) error {
	st, err := c.getState(target)
	if err != nil {
		return err
	}
	return c.AddPeerAddr(from, st.node.ReplAddr)
}

// AddPeerAddr dials an explicit address from from (from must be running).
// Proxy legs use this to route a session through a throttle instead of
// the target's bound address.
func (c *Cluster) AddPeerAddr(from int, addr netip.AddrPort) error {
	src, err := c.getState(from)
	if err != nil {
		return err
	}
	if src.client == nil {
		return &NodeStoppedError{Index: from}
	}
	_, err = src.client.Simple("MURMUR PEER ADD '" + addr.String() + "'")
	return err
}

// RemovePeer drops target from from's peer set (from must be running).
// Reversible with AddPeer; retention is kept.
func (c *Cluster) RemovePeer(from, target int) error {
	dst, err := c.getState(target)
	if err != nil {
		return err
	}
	src, err := c.getState(from)
	if err != nil {
		return err
	}
	if src.client == nil {
		return &NodeStoppedError{Index: from}
	}
	_, err = src.client.Simple("MURMUR PEER REMOVE '" + Hex(dst.node.NodeID[:]) + "'")
	return err
}

// RetirePeer permanently excludes target from from and releases its
// retention obligation. There is no un-retire.
func (c *Cluster) RetirePeer(from, target int) error {
	dst, err := c.getState(target)
	if err != nil {
		return err
	}
	src, err := c.getState(from)
	if err != nil {
		return err
	}
	if src.client == nil {
		return &NodeStoppedError{Index: from}
	}
	_, err = src.client.Simple("MURMUR PEER RETIRE '" + Hex(dst.node.NodeID[:]) + "'")
	return err
}

// SyncAll runs MURMUR PEER SYNC on every running node (heal legs converge
// faster than waiting for the anti-entropy tick).
func (c *Cluster) SyncAll() error {
	for i := range c.states {
		if c.states[i].client == nil {
			continue
		}
		if _, err := c.states[i].client.Simple("MURMUR PEER SYNC"); err != nil {
			return err
		}
	}
	return nil
}

// Mesh dials every ordered pair of running nodes (heal legs after
// restart).
func (c *Cluster) Mesh() error {
	for i := range c.states {
		if c.states[i].client == nil {
			continue
		}
		for j := range c.states {
			if i != j && c.states[j].client != nil {
				if err := c.AddPeer(i, j); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// StopNode gracefully stops node idx (SIGTERM on unix, kill elsewhere,
// with a SIGKILL fallback); dirs, key, and identity are kept for
// restart. Unknown index panics (a test bug).
func (c *Cluster) StopNode(idx int) {
	st := c.mustState(idx)
	if st.client != nil {
		_ = st.client.Close()
		st.client = nil
	}
	child := st.child
	if child == nil {
		return
	}
	st.child = nil
	// Signal delivery can race exec; a failed signal falls through to reap.
	_ = terminateProcess(child.cmd.Process)
	if exited, _ := child.exited(10 * time.Second); exited {
		return
	}
	reapChild(child, st.node.Label)
}

// KillNode crashes node idx without a graceful shutdown (crash legs):
// immediate kill + reap, keeping dirs, key, and identity for restart.
// Unknown index panics (a test bug).
func (c *Cluster) KillNode(idx int) {
	st := c.mustState(idx)
	if st.client != nil {
		_ = st.client.Close()
		st.client = nil
	}
	if st.child != nil {
		reapChild(st.child, st.node.Label)
		st.child = nil
	}
}

// SuspendNode suspends node idx with SIGSTOP (unix only; pause legs). The
// node stays "running" from the harness view: sockets are kept, queries
// block until ResumeNode.
func (c *Cluster) SuspendNode(idx int) error {
	st, err := c.getState(idx)
	if err != nil {
		return err
	}
	if st.child == nil {
		return &NodeStoppedError{Index: idx}
	}
	if err := suspendProcess(st.child.cmd.Process); err != nil {
		return fmt.Errorf("harness setup: node%d: SIGSTOP failed: %w", idx+1, err)
	}
	return nil
}

// ResumeNode resumes a node suspended with SuspendNode (unix only).
func (c *Cluster) ResumeNode(idx int) error {
	st, err := c.getState(idx)
	if err != nil {
		return err
	}
	if st.child == nil {
		return &NodeStoppedError{Index: idx}
	}
	if err := resumeProcess(st.child.cmd.Process); err != nil {
		return fmt.Errorf("harness setup: node%d: SIGCONT failed: %w", idx+1, err)
	}
	return nil
}

// StartNode restarts stopped node idx (same dir + key + identity) under
// the unlock watchdog. The node rebinds fresh SQL and replication ports:
// re-mesh (AddPeer/Mesh) afterwards, since peers still dial the old
// replication address. The replication.tables map carries over, so
// MURMUR SET TABLE flags survive.
func (c *Cluster) StartNode(idx int) error {
	st, err := c.getState(idx)
	if err != nil {
		return err
	}
	if st.client != nil {
		return fmt.Errorf("harness setup: node%d is already running", idx+1)
	}
	hash, err := passwordHash()
	if err != nil {
		return err
	}
	child, client, sqlAddr, replAddr, err := startProcess(st.node.Label, st.node.Dir, st.configPath, &st.inputs, hash)
	if err != nil {
		return err
	}
	st.node.SQLAddr = sqlAddr
	st.node.ReplAddr = replAddr
	st.child = child
	st.client = client
	return nil
}

// StartNodesParallel restarts every listed node concurrently, joining all
// restarts (the barrier) before returning. Sequential restarts skew a
// storm by minutes when opens take seconds each; concurrent restarts
// share the wall clock so every node rejoins from the same backlog.
func (c *Cluster) StartNodesParallel(idxs []int) error {
	for _, idx := range idxs {
		st, err := c.getState(idx)
		if err != nil {
			return err
		}
		if st.client != nil {
			return fmt.Errorf("harness setup: node%d is already running", idx+1)
		}
	}
	hash, err := passwordHash()
	if err != nil {
		return err
	}
	type started struct {
		idx      int
		child    *childProc
		client   *NodeClient
		sqlAddr  netip.AddrPort
		replAddr netip.AddrPort
	}
	results := make([]started, len(idxs))
	errs := make([]error, len(idxs))
	var wg sync.WaitGroup
	for k, idx := range idxs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st := &c.states[idx]
			child, client, sqlAddr, replAddr, err := startProcess(st.node.Label, st.node.Dir, st.configPath, &st.inputs, hash)
			if err != nil {
				errs[k] = err
				return
			}
			results[k] = started{idx: idx, child: child, client: client, sqlAddr: sqlAddr, replAddr: replAddr}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	for _, s := range results {
		st := &c.states[s.idx]
		st.node.SQLAddr = s.sqlAddr
		st.node.ReplAddr = s.replAddr
		st.child = s.child
		st.client = s.client
	}
	return nil
}

// SetBridgeConfig replaces node idx's stored bridge config. The node must
// be stopped; the new config takes effect on the next StartNode. A nil
// config removes the bridge.
func (c *Cluster) SetBridgeConfig(idx int, config *BridgeNodeConfig) error {
	st, err := c.getState(idx)
	if err != nil {
		return err
	}
	if st.client != nil {
		return fmt.Errorf("harness setup: node%d is running; stop it first", idx+1)
	}
	st.inputs.bridge = config
	return nil
}

// SetSpoolOptions replaces node idx's stored spool options (nil restores
// production storage behavior). The node must be stopped; the new
// options take effect on the next StartNode.
func (c *Cluster) SetSpoolOptions(idx int, spool *SpoolOptions) error {
	st, err := c.getState(idx)
	if err != nil {
		return err
	}
	if st.client != nil {
		return fmt.Errorf("harness setup: node%d is running; stop it first", idx+1)
	}
	st.inputs.spool = spool
	return nil
}

// SetNodeKey replaces node idx's stored unlock key. It takes effect on
// the next StartNode; online rotation (MURMUR ROTATE) on a running node
// should record the new key here immediately so later restarts unlock
// with it instead of the retired key.
func (c *Cluster) SetNodeKey(idx int, key [32]byte) error {
	st, err := c.getState(idx)
	if err != nil {
		return err
	}
	st.inputs.key = key
	return nil
}

// RotateNodeCert rotates node idx to a freshly issued leaf certificate
// for the SAME NodeID, then restarts it (cert rotation;
// cert-lifecycle). The node must be stopped; ttl overrides the default
// leaf validity (short TTLs for expiry legs, nil for a fresh long-lived
// cert). The node's tls/ dir is rewritten from the new material, so file
// NotAfter always matches the served cert. Like StartNode, the node
// rebinds fresh ports: re-mesh afterwards.
func (c *Cluster) RotateNodeCert(idx int, ttl *time.Duration) error {
	st, err := c.getState(idx)
	if err != nil {
		return err
	}
	if st.client != nil {
		return fmt.Errorf("harness setup: node%d is running; stop it first", idx+1)
	}
	issued, err := c.ca.Issue(st.node.NodeID, ttl)
	if err != nil {
		return fmt.Errorf("harness setup: rotate node cert: %w", err)
	}
	if _, err := c.ca.WriteTLSDir(issued, st.node.Dir); err != nil {
		return err
	}
	return c.StartNode(idx)
}

// RestartNodeWithIdentity restarts stopped node idx under a FRESH node
// identity (clone legs). The node must be stopped and its data dir must
// already carry the fresh identity (restored with a new node id). It
// issues a fresh leaf certificate for the new id (rewriting tls/), mints
// a fresh origin signing key, and registers the new id in the node's own
// trusted_origins (old entries are kept so pre-restore origins still
// verify). Peers are NOT told the fresh origin key, so the restarted
// node must not originate new transactions until every peer restarts
// with it. Like StartNode, the node rebinds fresh ports: re-mesh
// afterwards.
func (c *Cluster) RestartNodeWithIdentity(idx int, newNodeID [16]byte) error {
	st, err := c.getState(idx)
	if err != nil {
		return err
	}
	if st.client != nil {
		return fmt.Errorf("harness setup: node%d is running; stop it first", idx+1)
	}
	for _, s := range c.states {
		if s.node.NodeID == newNodeID {
			return fmt.Errorf("harness setup: fresh node id collides with a cluster member")
		}
	}
	issued, err := c.ca.Issue(newNodeID, nil)
	if err != nil {
		return fmt.Errorf("harness setup: issue node cert: %w", err)
	}
	tlsPaths, err := c.ca.WriteTLSDir(issued, st.node.Dir)
	if err != nil {
		return err
	}
	var seed [32]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return fmt.Errorf("harness setup: random origin seed: %w", err)
	}
	priv := ed25519.NewKeyFromSeed(seed[:])
	var pub [32]byte
	copy(pub[:], priv.Public().(ed25519.PublicKey))
	st.node.NodeID = newNodeID
	st.inputs.nodeID = newNodeID
	st.inputs.chainFile = tlsPaths.Chain
	st.inputs.keyFile = tlsPaths.Key
	st.inputs.caFile = tlsPaths.Ca
	st.inputs.signingHex = Hex(seed[:])
	if st.inputs.trustedOrigins == nil {
		st.inputs.trustedOrigins = map[[16]byte][32]byte{}
	}
	st.inputs.trustedOrigins[newNodeID] = pub
	return c.StartNode(idx)
}

// connectForensics dials a short-lived isolated connection for one
// forensics fetch.
func (c *Cluster) connectForensics(idx int) (*NodeClient, error) {
	st, err := c.getState(idx)
	if err != nil {
		return nil, err
	}
	return connectOnce(st.node.SQLAddr, pgUser, pgPassword, Hex(st.inputs.key[:]), 10*time.Second)
}

// DumpForensics dumps forensics for every node (running or stopped).
// Infallible by contract.
func (c *Cluster) DumpForensics(reason string) {
	for i := range c.states {
		st := &c.states[i]
		if st.client == nil {
			DumpNodeForensics(st.node.Label, st.node.Dir, nil, reason)
			continue
		}
		idx := i
		DumpNodeForensics(st.node.Label, st.node.Dir, func() (*NodeClient, error) {
			return c.connectForensics(idx)
		}, reason)
	}
}

// Finish kills all children (reaping them), then retains-or-cleans the
// runtime dir + RESULT line.
func (c *Cluster) Finish(failed bool) {
	for i := range c.states {
		st := &c.states[i]
		if st.client != nil {
			_ = st.client.Close()
			st.client = nil
		}
		if st.child != nil {
			reapChild(st.child, st.node.Label)
			st.child = nil
		}
	}
	Finish(c.name, c.runtimeDir, failed)
}

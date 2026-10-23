// In-process multi-Database cluster management.
//
// A "node" is an open murmur.Database on its own directory
// (runtime/<scenario>/nodeN/). Stop/restart is Close plus Open on the same
// dir with the same key; the replication mesh is QUIC with ephemeral
// 127.0.0.1:0 binds, so no port registry is needed. True
// SIGKILL/SIGSTOP/SIGTERM legs additionally need fixture subprocesses (see
// fixture.go) because signals cannot target one in-process node without
// freezing the test driver.
package harness

import (
	"crypto/rand"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
)

// NodeID is a 16-byte node identity.
type NodeID = [16]byte

// ReplicationOptions overlays applied to every node's replication Config.
type ReplicationOptions struct {
	Retention *time.Duration
	// Collection cadence (Config.RetentionInterval, default 60 s).
	// GC-balance legs set this to seconds so background passes run inside
	// the measurement window instead of starving it.
	RetentionInterval   *time.Duration
	MinimumRetained     *uint64
	Fanout              *int
	Dissemination       *replication.Dissemination
	MaxTransactionBytes *int
	// Gossip membership on/off (Config.Membership, default true).
	// False holds a static topology (no discovery dialing);
	// transaction push still runs over the peer sessions.
	Membership *bool
	// Offline-peer memory for retention floors
	// (Config.MaximumOffline, default 7 days). Peers unseen longer than
	// this stop pinning history, so survivors can GC ranges a stale node
	// still needs (snapshot legs).
	MaximumOffline *time.Duration
	// Merge-policy overlays (Config.MergePolicies): (table, column) ->
	// policy, applied to every node before open. Policies bind at open,
	// so CRDT legs (typed-records, merge-policies) must set this;
	// nil/empty leaves LWW everywhere.
	MergePolicies map[replication.PolicyColumn]replication.Policy
}

// FilesOptions is the encrypted file/object store for every node
// (per-node dir under the node).
type FilesOptions struct {
	Key           [32]byte
	FetchInterval *time.Duration
	MaxFileBytes  *uint64
}

// DefaultFilesKey is the test file-store key used when FilesOptions.Key
// is all zeros.
var DefaultFilesKey = [32]byte{
	61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61,
	61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61,
}

// DefaultKey is the application key for every node when
// ClusterOptions.Key is all zeros.
var DefaultKey = [32]byte{
	9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
	9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
}

// SqliteOptions holds commit-path budget overlays applied to every node's
// murmur Options.
type SqliteOptions struct {
	TransactionBytes *int
	QueueBytes       *int
	CaptureBytes     *int
}

// SpoolOptions holds per-node spool storage overrides, keyed by node
// index; resource-compaction legs size victim segments down.
type SpoolOptions struct {
	// Spool segment rotation size (Options.SegmentBytes, default 256 MiB).
	SegmentBytes *int
}

// BridgeOptions holds per-node High/Low bridge configs, keyed by node
// index.
//
// The configs are used as given (including their drop directories, which
// scenarios share between Low exporters and High receivers); the harness
// does not rewrite paths.
type BridgeOptions struct {
	Nodes map[int]*replication.BridgeConfig
}

// ClusterOptions holds cluster construction options.
type ClusterOptions struct {
	// Scenario name; owns runtime/<name/>. Required.
	Name string
	// Node count. Default 3 when 0.
	NumNodes int
	// Shared database id. Default: random per cluster.
	DatabaseID *[16]byte
	// Node ids. Default: random per node (must be NumNodes long if set).
	NodeIDs [][16]byte
	// Application key for every node. Default: fixed test key.
	Key [32]byte
	// Replication overlays for every node.
	Replication *ReplicationOptions
	// Commit-path budget overlays for every node.
	Sqlite *SqliteOptions
	// Per-node spool storage overrides.
	SpoolByNode map[int]SpoolOptions
	// Skip the automatic full mesh; the test calls AddPeer.
	ManualPeers bool
	// Seed indices: opened first, and every other node dials them via
	// bootstrap addrs (SWIM/seed-discovery legs). Empty disables seeding.
	BootstrapSeeds []int
	// Per-node identity allow-lists (allowed_peers overlay).
	AllowedPeers map[int]map[[16]byte]bool
	// Per-node address-policy overlays (allowed_networks).
	AllowedNetworks map[int][]netip.Prefix
	// Per-node snapshot-donor trust (trusted_snapshot_sources overlay).
	// Empty by default: nodes request snapshots from nobody. Snapshot
	// scenarios list explicit donor ids here.
	TrustedSnapshotSources map[int]map[[16]byte]bool
	// Per-node extra entries merged into trusted_origins (e.g. an
	// attacker signing key for hostile-source legs).
	ExtraTrustedOrigins map[int]map[[16]byte][32]byte
	// Per-node dissemination-mode overrides (mixed-mode legs; wins over
	// the global Replication.Dissemination for the listed nodes).
	DisseminationByNode map[int]replication.Dissemination
	// File/object store for every node.
	Files *FilesOptions
	// Bridge configs for individual nodes.
	Bridge BridgeOptions
	// Per-node leaf-cert TTL overrides (default validity is effectively
	// unbounded; cert-lifecycle uses short TTLs for expiry legs).
	NodeCertTTL map[int]time.Duration
	// Schema-master node index. When set, that node is the schema
	// master (Master + SchemaMaster=self, so it never adopts) and every
	// other node adopts only the master's manifests. Nil (default) keeps
	// masterless mode, where concurrent schema edits on several nodes can
	// regress each other through stale-manifest adoption.
	MasterNode *int
}

// Node is one cluster node: dirs, identity, and the open handle (nil =
// stopped).
type Node struct {
	Index int
	// node1 .. nodeN.
	Label string
	// runtime/<scenario>/nodeN/ (config, tls/, forensics files).
	Dir string
	// Database storage dir (<dir>/data/).
	DataDir string
	NodeID  [16]byte
	// Open handle; nil while stopped.
	DB *murmur.Database
	// Bound replication address (ephemeral; changes across restarts).
	ReplAddr netip.AddrPort
}

type nodeState struct {
	node       Node
	config     *replication.Config
	key        [32]byte
	databaseID [16]byte
	sqlite     *SqliteOptions
	spool      *SpoolOptions
}

// applySqlite applies commit-path budget overlays to node open options.
func applySqlite(opts *murmur.Options, sqlite *SqliteOptions) {
	if sqlite == nil {
		return
	}
	if sqlite.TransactionBytes != nil {
		opts.TransactionBytes = *sqlite.TransactionBytes
	}
	if sqlite.QueueBytes != nil {
		opts.QueueBytes = *sqlite.QueueBytes
	}
	if sqlite.CaptureBytes != nil {
		opts.CaptureBytes = *sqlite.CaptureBytes
	}
}

// applySpool applies spool storage overlays to node open options.
func applySpool(opts *murmur.Options, spool *SpoolOptions) {
	if spool == nil {
		return
	}
	if spool.SegmentBytes != nil {
		opts.SegmentBytes = *spool.SegmentBytes
	}
}

// Cluster is an in-process multi-node cluster.
type Cluster struct {
	name       string
	runtimeDir string
	databaseID [16]byte
	states     []nodeState
	ca         *Ca
}

// NewCluster builds node dirs + certs, opens every node (seeds first),
// and meshes unless ManualPeers. It wipes any previous
// runtime/<name/> first.
func NewCluster(opts ClusterOptions) (*Cluster, error) {
	numNodes := opts.NumNodes
	if numNodes == 0 {
		numNodes = 3
	}
	if numNodes < 1 {
		return nil, fmt.Errorf("harness setup: num_nodes must be >= 1")
	}
	if opts.Name == "" {
		return nil, fmt.Errorf("harness setup: ClusterOptions.Name must be set")
	}
	var nodeIDs [][16]byte
	if len(opts.NodeIDs) == 0 {
		nodeIDs = make([][16]byte, numNodes)
		for i := range nodeIDs {
			if _, err := rand.Read(nodeIDs[i][:]); err != nil {
				return nil, fmt.Errorf("harness setup: node id entropy: %w", err)
			}
		}
	} else {
		if len(opts.NodeIDs) != numNodes {
			return nil, fmt.Errorf("harness setup: node_ids has %d entries for %d nodes", len(opts.NodeIDs), numNodes)
		}
		nodeIDs = opts.NodeIDs
	}
	distinct := make(map[[16]byte]bool, numNodes)
	for _, id := range nodeIDs {
		distinct[id] = true
	}
	if len(distinct) != len(nodeIDs) {
		return nil, fmt.Errorf("harness setup: node ids must be distinct")
	}
	seedSet := make(map[int]bool)
	for _, s := range opts.BootstrapSeeds {
		if s < 0 || s >= numNodes {
			return nil, fmt.Errorf("harness setup: bootstrap seed %d out of range", s)
		}
		seedSet[s] = true
	}
	key := opts.Key
	if key == ([32]byte{}) {
		key = DefaultKey
	}

	runtimeDir, err := PrepareRuntimeDir(opts.Name)
	if err != nil {
		return nil, err
	}
	var databaseID [16]byte
	if opts.DatabaseID != nil {
		databaseID = *opts.DatabaseID
	} else if _, err := rand.Read(databaseID[:]); err != nil {
		return nil, fmt.Errorf("harness setup: database id entropy: %w", err)
	}

	// Signing keys + trust: every node trusts every node's origin key;
	// allow-lists narrow this.
	seeds := make([][32]byte, numNodes)
	for i := range seeds {
		if _, err := rand.Read(seeds[i][:]); err != nil {
			return nil, fmt.Errorf("harness setup: seed entropy: %w", err)
		}
	}
	trustedOrigins := make(map[[16]byte][32]byte, numNodes)
	for i, id := range nodeIDs {
		trustedOrigins[id] = replication.PublicKey(&seeds[i])
	}

	ca, err := GenerateCa()
	if err != nil {
		return nil, fmt.Errorf("harness setup: CA generation: %w", err)
	}

	// Open order: seeds first so their ephemeral addrs can seed the rest.
	order := append([]int(nil), opts.BootstrapSeeds...)
	sort.Ints(order)
	deduped := order[:0]
	for _, s := range order {
		if len(deduped) == 0 || deduped[len(deduped)-1] != s {
			deduped = append(deduped, s)
		}
	}
	order = deduped
	for i := 0; i < numNodes; i++ {
		if !seedSet[i] {
			order = append(order, i)
		}
	}

	states := make([]nodeState, numNodes)
	var seedAddrs []netip.AddrPort
	for _, i := range order {
		label := fmt.Sprintf("node%d", i+1)
		dir := filepath.Join(runtimeDir, label)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
		dataDir := filepath.Join(dir, "data")

		var ttl *time.Duration
		if d, ok := opts.NodeCertTTL[i]; ok {
			ttl = &d
		}
		issued, err := ca.Issue(nodeIDs[i], ttl)
		if err != nil {
			return nil, fmt.Errorf("harness setup: issue node%d cert: %w", i+1, err)
		}
		if err := ca.WriteTLSDir(issued, dir); err != nil {
			return nil, err
		}

		config := replication.NewConfig(netip.MustParseAddrPort("127.0.0.1:0"), issued.Credentials, seeds[i])
		config.TrustedOrigins = cloneOrigins(trustedOrigins)
		if opts.MasterNode != nil {
			if *opts.MasterNode < 0 || *opts.MasterNode >= numNodes {
				return nil, fmt.Errorf("harness setup: master node %d out of range", *opts.MasterNode)
			}
			master := nodeIDs[*opts.MasterNode]
			config.SchemaMaster = &master
			config.Master = i == *opts.MasterNode
		}
		if peers, ok := opts.AllowedPeers[i]; ok {
			config.AllowedPeers = cloneBools(peers)
		}
		if nets, ok := opts.AllowedNetworks[i]; ok {
			config.AllowedNetworks = append([]netip.Prefix(nil), nets...)
		}
		if sources, ok := opts.TrustedSnapshotSources[i]; ok {
			config.TrustedSnapshotSources = cloneBools(sources)
		}
		if extra, ok := opts.ExtraTrustedOrigins[i]; ok {
			for k, v := range extra {
				config.TrustedOrigins[k] = v
			}
		}
		if r := opts.Replication; r != nil {
			if r.Retention != nil {
				config.Retention = *r.Retention
			}
			if r.RetentionInterval != nil {
				config.RetentionInterval = *r.RetentionInterval
			}
			if r.MinimumRetained != nil {
				config.MinimumRetained = *r.MinimumRetained
			}
			if r.Fanout != nil {
				config.Fanout = *r.Fanout
			}
			if r.Dissemination != nil {
				config.Dissemination = *r.Dissemination
			}
			if r.MaxTransactionBytes != nil {
				config.MaxTransactionBytes = *r.MaxTransactionBytes
			}
			if r.Membership != nil {
				config.Membership = *r.Membership
			}
			if r.MaximumOffline != nil {
				config.MaximumOffline = *r.MaximumOffline
			}
			if len(r.MergePolicies) > 0 {
				config.MergePolicies = clonePolicies(r.MergePolicies)
			}
		}
		if d, ok := opts.DisseminationByNode[i]; ok {
			config.Dissemination = d
		}
		if f := opts.Files; f != nil {
			fkey := f.Key
			if fkey == ([32]byte{}) {
				fkey = DefaultFilesKey
			}
			fc := replication.NewFilesConfig(filepath.Join(dir, "objects"), fkey, netip.MustParseAddrPort("127.0.0.1:0"))
			if f.FetchInterval != nil {
				fc.FetchInterval = *f.FetchInterval
			}
			if f.MaxFileBytes != nil {
				fc.MaxFileBytes = *f.MaxFileBytes
			}
			config.Files = fc
		}
		if bc, ok := opts.Bridge.Nodes[i]; ok {
			config.Bridge = bc
		}
		if len(seedAddrs) > 0 && !seedSet[i] {
			config.Bootstrap = append([]netip.AddrPort(nil), seedAddrs...)
		}

		dbOpts := murmur.DefaultOptions()
		dbOpts.Mode = murmur.OpenCreateNew
		dbOpts.DatabaseID = &databaseID
		dbOpts.Replication = config
		nodeID := nodeIDs[i]
		dbOpts.NodeID = &nodeID
		applySqlite(&dbOpts, opts.Sqlite)
		var spool *SpoolOptions
		if s, ok := opts.SpoolByNode[i]; ok {
			spool = &s
		}
		applySpool(&dbOpts, spool)
		db, err := OpenWithWatchdog(dataDir, append([]byte(nil), key[:]...), dbOpts)
		if err != nil {
			return nil, err
		}
		replAddr, err := replicationAddr(db)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		if seedSet[i] {
			seedAddrs = append(seedAddrs, replAddr)
		}
		states[i] = nodeState{
			node: Node{
				Index: i, Label: label, Dir: dir, DataDir: dataDir,
				NodeID: nodeIDs[i], DB: db, ReplAddr: replAddr,
			},
			config: config, key: key, databaseID: databaseID,
			sqlite: opts.Sqlite, spool: spool,
		}
	}

	cluster := &Cluster{
		name:       opts.Name,
		runtimeDir: runtimeDir,
		databaseID: databaseID,
		states:     states,
		ca:         ca,
	}
	if !opts.ManualPeers {
		if err := cluster.Mesh(); err != nil {
			return nil, err
		}
	}
	return cluster, nil
}

// Name returns the scenario name.
func (c *Cluster) Name() string { return c.name }

// RuntimeDir returns the runtime/<scenario/> root.
func (c *Cluster) RuntimeDir() string { return c.runtimeDir }

// DatabaseID returns the shared database id.
func (c *Cluster) DatabaseID() [16]byte { return c.databaseID }

// IssueCert issues a leaf certificate for an external identity (e.g. a
// hostile fixture peer) from the cluster CA.
func (c *Cluster) IssueCert(nodeID [16]byte) (*Issued, error) {
	return c.ca.Issue(nodeID, nil)
}

// CaDER returns the DER bytes of the cluster CA certificate, for
// cross-trust legs: hand them to a peer cluster's AddTrustRoot.
func (c *Cluster) CaDER() []byte { return c.ca.CaDER() }

// AddTrustRoot appends an external CA root to every node's stored TLS
// trust set (deduped). It applies to the stored config only: stop and
// StartNode every node afterwards (then re-mesh). tls/ keeps the original
// CA file; RotateNodeCert resets trust to the own CA.
func (c *Cluster) AddTrustRoot(rootDER []byte) {
	for i := range c.states {
		found := false
		for _, r := range c.states[i].config.Credentials.Roots {
			if string(r) == string(rootDER) {
				found = true
				break
			}
		}
		if !found {
			c.states[i].config.Credentials.Roots = append(c.states[i].config.Credentials.Roots, append([]byte(nil), rootDER...))
		}
	}
}

// Len returns the node count.
func (c *Cluster) Len() int { return len(c.states) }

// IsEmpty reports whether the cluster has no nodes.
func (c *Cluster) IsEmpty() bool { return len(c.states) == 0 }

// NodeID returns the node id (available while stopped too). It panics on
// an unknown index (a test bug).
func (c *Cluster) NodeID(idx int) [16]byte {
	if idx < 0 || idx >= len(c.states) {
		panic(fmt.Sprintf("unknown node index %d", idx))
	}
	return c.states[idx].node.NodeID
}

// NodeOriginPubkey returns the origin signing public key for node idx
// (ed25519 verifying-key bytes).
//
// Bridge receivers in another domain need this for
// replication.NewBridgeReceiver trusted origins; the seed itself stays
// inside the harness. Available while stopped too.
func (c *Cluster) NodeOriginPubkey(idx int) [32]byte {
	if idx < 0 || idx >= len(c.states) {
		panic(fmt.Sprintf("unknown node index %d", idx))
	}
	return replication.PublicKey(&c.states[idx].config.SigningKey)
}

// NodeInfo returns node metadata (dirs, addr; available while stopped
// too). It panics on an unknown index.
func (c *Cluster) NodeInfo(idx int) *Node {
	if idx < 0 || idx >= len(c.states) {
		panic(fmt.Sprintf("unknown node index %d", idx))
	}
	return &c.states[idx].node
}

// Node returns the open handle. It panics if the node is stopped (a test
// bug).
func (c *Cluster) Node(idx int) *murmur.Database {
	if idx < 0 || idx >= len(c.states) {
		panic(fmt.Sprintf("unknown node index %d", idx))
	}
	db := c.states[idx].node.DB
	if db == nil {
		panic(fmt.Sprintf("node%d is stopped", idx+1))
	}
	return db
}

// EnsureMigrated runs fixture SQL on one node during setup. Fast meshes
// converge the schema from an earlier node before a later node's
// identical migrate runs; that "already exists" is the converged fixture,
// not a failure. Node 0 stays strict (nothing has replicated yet, so
// pre-existing tables there mean a dirty runtime dir); later nodes skip
// on with a log line.
func (c *Cluster) EnsureMigrated(idx int, sql string) error {
	db := c.Node(idx)
	if err := db.Migrate(murmur.NewMigration(sql)); err != nil {
		if idx > 0 && strings.Contains(err.Error(), "already exists") {
			fmt.Fprintf(os.Stderr, "node%d: fixture schema already converged, skipping identical migrate\n", idx+1)
			return nil
		}
		return fmt.Errorf("node%d fixture migrate failed: %w", idx+1, err)
	}
	return db.Flush()
}

// AddPeer dials target from from (both must be running; target addr is
// its current ephemeral bind).
func (c *Cluster) AddPeer(from, target int) error {
	if target < 0 || target >= len(c.states) {
		return fmt.Errorf("unknown node index %d", target)
	}
	if from < 0 || from >= len(c.states) {
		return fmt.Errorf("unknown node index %d", from)
	}
	src := c.states[from].node.DB
	if src == nil {
		return fmt.Errorf("node index %d is stopped", from)
	}
	h := src.Replication()
	if h == nil {
		return fmt.Errorf("harness setup: node%d has no replication", from+1)
	}
	return h.AddPeer(c.states[target].node.ReplAddr)
}

// AddPeerAddr dials an explicit address from from (from must be running).
// Proxy legs use this to route a session through a throttle instead of
// the target's bound address.
func (c *Cluster) AddPeerAddr(from int, addr netip.AddrPort) error {
	if from < 0 || from >= len(c.states) {
		return fmt.Errorf("unknown node index %d", from)
	}
	src := c.states[from].node.DB
	if src == nil {
		return fmt.Errorf("node index %d is stopped", from)
	}
	h := src.Replication()
	if h == nil {
		return fmt.Errorf("harness setup: node%d has no replication", from+1)
	}
	return h.AddPeer(addr)
}

// RemovePeer drops target from from's peer set (from must be running).
func (c *Cluster) RemovePeer(from, target int) error {
	if target < 0 || target >= len(c.states) {
		return fmt.Errorf("unknown node index %d", target)
	}
	if from < 0 || from >= len(c.states) {
		return fmt.Errorf("unknown node index %d", from)
	}
	src := c.states[from].node.DB
	if src == nil {
		return fmt.Errorf("node index %d is stopped", from)
	}
	h := src.Replication()
	if h == nil {
		return fmt.Errorf("harness setup: node%d has no replication", from+1)
	}
	return h.RemovePeer(c.states[target].node.NodeID)
}

// Mesh dials every ordered pair of running nodes (heal legs after
// restart).
func (c *Cluster) Mesh() error {
	for i := range c.states {
		if c.states[i].node.DB == nil {
			continue
		}
		for j := range c.states {
			if i != j && c.states[j].node.DB != nil {
				if err := c.AddPeer(i, j); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// StopNode closes node idx; dirs, key, and identity are kept for restart.
// An unknown index panics (a test bug); close errors are logged.
func (c *Cluster) StopNode(idx int) {
	if idx < 0 || idx >= len(c.states) {
		panic(fmt.Sprintf("unknown node index %d", idx))
	}
	if db := c.states[idx].node.DB; db != nil {
		c.states[idx].node.DB = nil
		if err := db.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "stop node%d: close failed: %v\n", idx+1, err)
		}
	}
}

// StartNode reopens node idx (same dir + key + identity) under the open
// watchdog. The node rebinds a fresh ephemeral port: re-mesh
// (AddPeer/Mesh) afterwards, since peers still dial the old address.
func (c *Cluster) StartNode(idx int) error {
	return c.startNodeInner(idx, nil)
}

// StartNodesParallel reopens every listed node concurrently, joining all
// opens (the barrier) before rebinding addresses. Sequential reopens skew
// a storm by minutes when opens take seconds each; concurrent opens share
// the wall clock so every node rejoins from the same backlog. Opens touch
// disjoint dirs and only reinsert handles afterwards, so no node observes
// a half-restarted peer.
func (c *Cluster) StartNodesParallel(idxs []int) error {
	type params struct {
		idx     int
		dataDir string
		key     [32]byte
		opts    murmur.Options
	}
	plist := make([]params, 0, len(idxs))
	for _, idx := range idxs {
		if idx < 0 || idx >= len(c.states) {
			return fmt.Errorf("unknown node index %d", idx)
		}
		st := &c.states[idx]
		if st.node.DB != nil {
			return fmt.Errorf("harness setup: node%d is already running", idx+1)
		}
		dbOpts := murmur.DefaultOptions()
		dbOpts.Mode = murmur.OpenExisting
		dbOpts.DatabaseID = &st.databaseID
		dbOpts.Replication = st.config
		nodeID := st.node.NodeID
		dbOpts.NodeID = &nodeID
		applySqlite(&dbOpts, st.sqlite)
		applySpool(&dbOpts, st.spool)
		plist = append(plist, params{idx: idx, dataDir: st.node.DataDir, key: st.key, opts: dbOpts})
	}
	type opened struct {
		idx int
		db  *murmur.Database
		err error
	}
	results := make([]opened, len(plist))
	var wg sync.WaitGroup
	for k, p := range plist {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db, err := OpenWithWatchdog(p.dataDir, append([]byte(nil), p.key[:]...), p.opts)
			results[k] = opened{idx: p.idx, db: db, err: err}
		}()
	}
	wg.Wait()
	for _, r := range results {
		if r.err != nil {
			return r.err
		}
		addr, err := replicationAddr(r.db)
		if err != nil {
			_ = r.db.Close()
			return err
		}
		st := &c.states[r.idx]
		st.node.ReplAddr = addr
		st.node.DB = r.db
	}
	return nil
}

// SetBridgeConfig replaces node idx's stored bridge config
// (signer-rotation legs). The node must be stopped; the new config takes
// effect on the next StartNode. Nil removes the bridge from the stored
// config.
func (c *Cluster) SetBridgeConfig(idx int, config *replication.BridgeConfig) error {
	if idx < 0 || idx >= len(c.states) {
		return fmt.Errorf("unknown node index %d", idx)
	}
	st := &c.states[idx]
	if st.node.DB != nil {
		return fmt.Errorf("harness setup: node%d is running; stop it first", idx+1)
	}
	st.config.Bridge = config
	return nil
}

// SetSpoolOptions replaces node idx's stored spool options (nil restores
// production storage behavior). The node must be stopped; the new options
// take effect on the next StartNode.
func (c *Cluster) SetSpoolOptions(idx int, spool *SpoolOptions) error {
	if idx < 0 || idx >= len(c.states) {
		return fmt.Errorf("unknown node index %d", idx)
	}
	st := &c.states[idx]
	if st.node.DB != nil {
		return fmt.Errorf("harness setup: node%d is running; stop it first", idx+1)
	}
	st.spool = spool
	return nil
}

// SetFilesConfig replaces node idx's stored file/object-store key material
// (file key-rotation legs). Generation must advance exactly one step past
// the newest on-disk metadata generation, with previous pointing at the
// retired key while any old-generation object or metadata remains; pass
// nil once KeyRotationStatus reports PreviousKeyUnusedLocally. The node
// must be stopped; the new material takes effect on the next StartNode.
// Limits, intervals, and bind are preserved.
func (c *Cluster) SetFilesConfig(idx int, key [32]byte, generation uint64, previous *[32]byte) error {
	if idx < 0 || idx >= len(c.states) {
		return fmt.Errorf("unknown node index %d", idx)
	}
	st := &c.states[idx]
	if st.node.DB != nil {
		return fmt.Errorf("harness setup: node%d is running; stop it first", idx+1)
	}
	if st.config.Files == nil {
		return fmt.Errorf("harness setup: node%d has no file store configured", idx+1)
	}
	st.config.Files.Key = key
	st.config.Files.Generation = generation
	st.config.Files.PreviousKey = previous
	return nil
}

// SetNodeKey replaces node idx's stored application key (key-rotation
// legs). It takes effect on the next StartNode; online rotation
// (RotateMasterKey on a running node) should record the new key here
// immediately so later restarts unlock with it instead of the retired key.
func (c *Cluster) SetNodeKey(idx int, key [32]byte) error {
	if idx < 0 || idx >= len(c.states) {
		return fmt.Errorf("unknown node index %d", idx)
	}
	c.states[idx].key = key
	return nil
}

// RotateNodeCert rotates node idx to a freshly issued leaf certificate
// for the SAME node id, then reopens it (cert rotation;
// cert-lifecycle). The node must be stopped; ttl overrides the default
// leaf validity (short TTLs for expiry legs, nil for a fresh long-lived
// cert). The node's tls/ dir is rewritten from the new material, so file
// NotAfter always matches the served cert. Like StartNode, the node
// rebinds a fresh ephemeral port: re-mesh afterwards.
func (c *Cluster) RotateNodeCert(idx int, ttl *time.Duration) error {
	if idx < 0 || idx >= len(c.states) {
		return fmt.Errorf("unknown node index %d", idx)
	}
	st := &c.states[idx]
	issued, err := c.ca.Issue(st.node.NodeID, ttl)
	if err != nil {
		return err
	}
	if err := c.ca.WriteTLSDir(issued, st.node.Dir); err != nil {
		return err
	}
	return c.startNodeInner(idx, &issued.Credentials)
}

// RestartNodeWithIdentity restarts stopped node idx under a FRESH node
// identity (backup-restore clone legs: RestoreWithOptions +
// NewNodeID + TLS reissue + fresh origin key). The node must be stopped
// and its data dir must already carry the fresh identity (restored with
// RestoreOptions{NewNodeID}). It issues a fresh leaf certificate for the
// new id (rewriting tls/), mints a fresh origin signing key, registers
// the new id in the node's own trusted_origins (old entries are kept so
// pre-restore origins still verify), and reopens. Peers are NOT told the
// fresh origin key, so the restarted node must not originate new
// transactions until every peer restarts with it (transaction verification
// is by origin; receiving and serving pre-existing origins is
// unaffected). Like StartNode, the node rebinds a fresh ephemeral port:
// re-mesh afterwards.
func (c *Cluster) RestartNodeWithIdentity(idx int, newNodeID [16]byte) error {
	if idx < 0 || idx >= len(c.states) {
		return fmt.Errorf("unknown node index %d", idx)
	}
	if c.states[idx].node.DB != nil {
		return fmt.Errorf("harness setup: node%d is already running", idx+1)
	}
	for _, s := range c.states {
		if s.node.NodeID == newNodeID {
			return fmt.Errorf("harness setup: fresh node id collides with a cluster member")
		}
	}
	dir := c.states[idx].node.Dir
	issued, err := c.ca.Issue(newNodeID, nil)
	if err != nil {
		return err
	}
	if err := c.ca.WriteTLSDir(issued, dir); err != nil {
		return err
	}
	var seed [32]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return fmt.Errorf("harness setup: seed entropy: %w", err)
	}
	public := replication.PublicKey(&seed)
	st := &c.states[idx]
	st.node.NodeID = newNodeID
	st.config.Credentials = issued.Credentials
	st.config.SigningKey = seed
	st.config.TrustedOrigins[newNodeID] = public
	return c.StartNode(idx)
}

func (c *Cluster) startNodeInner(idx int, credentials *replication.Credentials) error {
	if idx < 0 || idx >= len(c.states) {
		return fmt.Errorf("unknown node index %d", idx)
	}
	st := &c.states[idx]
	if st.node.DB != nil {
		return fmt.Errorf("harness setup: node%d is already running", idx+1)
	}
	if credentials != nil {
		st.config.Credentials = *credentials
	}
	dbOpts := murmur.DefaultOptions()
	dbOpts.Mode = murmur.OpenExisting
	dbOpts.DatabaseID = &st.databaseID
	dbOpts.Replication = st.config
	nodeID := st.node.NodeID
	dbOpts.NodeID = &nodeID
	applySqlite(&dbOpts, st.sqlite)
	applySpool(&dbOpts, st.spool)
	db, err := OpenWithWatchdog(st.node.DataDir, append([]byte(nil), st.key[:]...), dbOpts)
	if err != nil {
		return err
	}
	addr, err := replicationAddr(db)
	if err != nil {
		_ = db.Close()
		return err
	}
	st.node.ReplAddr = addr
	st.node.DB = db
	return nil
}

// DumpForensics captures forensics for every node (running or stopped).
// Infallible by contract.
func (c *Cluster) DumpForensics(reason string) {
	for i := range c.states {
		DumpNodeForensics(c.states[i].node.Label, c.states[i].node.Dir, c.states[i].node.DB, reason)
	}
}

// Finish closes all nodes, then retains-or-cleans the runtime dir + the
// RESULT line.
func (c *Cluster) Finish(failed bool) {
	for i := range c.states {
		if db := c.states[i].node.DB; db != nil {
			c.states[i].node.DB = nil
			_ = db.Close()
		}
	}
	Finish(c.name, c.runtimeDir, failed)
}

// replicationAddr returns the bound replication address, waiting briefly
// for the endpoint to come up.
func replicationAddr(db *murmur.Database) (netip.AddrPort, error) {
	end := time.Now().Add(10 * time.Second)
	for {
		if h := db.Replication(); h != nil {
			if addr := h.Status().Address; addr != nil {
				return *addr, nil
			}
		}
		if !time.Now().Before(end) {
			return netip.AddrPort{}, fmt.Errorf("harness setup: replication endpoint never bound")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func cloneOrigins(m map[[16]byte][32]byte) map[[16]byte][32]byte {
	out := make(map[[16]byte][32]byte, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneBools(m map[[16]byte]bool) map[[16]byte]bool {
	out := make(map[[16]byte]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func clonePolicies(m map[replication.PolicyColumn]replication.Policy) map[replication.PolicyColumn]replication.Policy {
	out := make(map[replication.PolicyColumn]replication.Policy, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

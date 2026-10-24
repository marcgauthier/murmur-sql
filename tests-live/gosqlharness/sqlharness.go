// Package gosqlharness is the Go port of the Rust tests-live/
// process-isolated live SQL harness (tests-live/sql-harness): one
// murmur-server child per node, driven through plain wire-client
// connections. This package never links the embedded engine.
//
// Differences from the Rust source are deliberate and documented per
// symbol. The structural ones: node configs render as YAML, which is
// what the Go server's config loader accepts (it rejects the Rust
// renderer's TOML); config edits go through SetConfig (YAML section
// surgery) instead of a toml_edit closure; Rust panics become returned
// errors; the RESULT line needs an explicit Finish call (Go has no Drop).
package gosqlharness

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"io/fs"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

const (
	// Password is the SQL password for every node.
	Password = "live-sql-test-password"
	// Key is the encryption key for every node.
	Key = "live-sql-encryption-key"
)

// binaries locates (server, admin): MURMUR_SERVER_BIN (+MURMUR_ADMIN_BIN)
// or a Go build once per process.
var (
	binariesOnce   sync.Once
	binariesServer string
	binariesAdmin  string
	binariesErr    error
	hashOnce       sync.Once
	hashValue      string
	hashErr        error
)

func binaries() (server, admin string, err error) {
	binariesOnce.Do(func() {
		if server := os.Getenv("MURMUR_SERVER_BIN"); server != "" {
			abs, err := filepath.Abs(server)
			if err != nil {
				binariesErr = fmt.Errorf("harness setup: MURMUR_SERVER_BIN: %w", err)
				return
			}
			binariesServer = abs
			if admin := os.Getenv("MURMUR_ADMIN_BIN"); admin != "" {
				binariesAdmin = admin
			} else {
				binariesAdmin = filepath.Join(filepath.Dir(abs), exeName("murmur-admin"))
			}
			return
		}
		root := filepath.Dir(goharness.TestsLiveDir())
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

func hash() (string, error) {
	hashOnce.Do(func() {
		_, admin, err := binaries()
		if err != nil {
			hashErr = err
			return
		}
		out, err := exec.Command(admin, "hash-password", "--password", Password).Output()
		if err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				hashErr = fmt.Errorf("harness setup: password hash failed: %s", ee.Stderr)
			} else {
				hashErr = fmt.Errorf("harness setup: hash password: %w", err)
			}
			return
		}
		hashValue = strings.TrimSpace(string(out))
	})
	return hashValue, hashErr
}

// ClusterOptions holds cluster construction options.
type ClusterOptions struct {
	Name string
	// NumNodes is the node count. Default 3.
	NumNodes int
	// ExtraYAML is additional top-level configuration, such as engine
	// or replication.files sections, merged into each node's config.
	//
	// Deviation: the Rust field is extra_toml (TOML); the Go server only
	// accepts YAML, so this port takes YAML. Same for ReplicationYAML.
	ExtraYAML string
	// ReplicationYAML holds overrides merged into each node's
	// replication section.
	ReplicationYAML string
	Files           bool
	ManualPeers     bool
	LocalOnly       bool
}

// Node is one cluster node: dirs, addresses, and identity.
type Node struct {
	Dir        string
	DataDir    string
	ConfigPath string
	SQLAddr    netip.AddrPort
	ReplAddr   netip.AddrPort
	NodeID     string
	Label      string
}

// childProc is a spawned server with a single-flight waiter.
type childProc struct {
	cmd  *exec.Cmd
	done chan error
}

func spawnChild(server, configPath string, log *os.File) (*childProc, error) {
	cmd := exec.Command(server, "--config", configPath)
	// Deviation: the Rust harness sets TOKIO_WORKER_THREADS=2, which is
	// meaningless for the Go server; dropped.
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	p := &childProc{cmd: cmd, done: make(chan error, 1)}
	go func() { p.done <- cmd.Wait() }()
	return p, nil
}

func (p *childProc) exited() (bool, error) {
	select {
	case err := <-p.done:
		return true, err
	default:
		return false, nil
	}
}

type sqlNode struct {
	node   Node
	child  *childProc
	client *goharness.Client
	key    string
	tls    goharness.TLSConfig
}

// Cluster is a process-isolated SQL cluster: one murmur-server child per
// node. It is not safe for concurrent use (except concurrent Client
// operations, which serialize on their connections).
type Cluster struct {
	nodes []sqlNode
	dir   string
	name  string
}

// NewCluster builds node dirs + certs, renders configs, and starts every
// node. Each run owns a unique runtime dir.
func NewCluster(opts ClusterOptions) (*Cluster, error) {
	count := opts.NumNodes
	if count == 0 {
		count = 3
	}
	runtimeRoot := os.Getenv("MURMUR_LIVE_RUNTIME")
	if runtimeRoot == "" {
		runtimeRoot = filepath.Join(goharness.TestsLiveDir(), "runtime")
	}
	if err := os.MkdirAll(runtimeRoot, 0o755); err != nil {
		return nil, err
	}
	var base [16]byte
	if _, err := rand.Read(base[:]); err != nil {
		return nil, fmt.Errorf("harness setup: random node id: %w", err)
	}
	dir := filepath.Join(runtimeRoot, opts.Name+"-"+strconv.Itoa(os.Getpid())+"-"+goharness.Hex(base[:4]))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	cluster := &Cluster{dir: dir, name: opts.Name}
	ca, err := goharness.GenerateCa()
	if err != nil {
		return nil, fmt.Errorf("harness setup: generate CA: %w", err)
	}
	signing := make([][32]byte, count)
	nodes := make([]sqlNode, count)
	for i := range nodes {
		var seed [32]byte
		if _, err := rand.Read(seed[:]); err != nil {
			return nil, fmt.Errorf("harness setup: random signing key: %w", err)
		}
		signing[i] = seed
		id := base
		id[15] = byte(i)
		sqlAddr, err := goharness.FreeAddr()
		if err != nil {
			return nil, fmt.Errorf("harness setup: free SQL port: %w", err)
		}
		replAddr, err := goharness.FreeUDPAddr()
		if err != nil {
			return nil, fmt.Errorf("harness setup: free replication port: %w", err)
		}
		nodeDir := filepath.Join(dir, "node"+strconv.Itoa(i+1))
		if err := os.MkdirAll(nodeDir, 0o755); err != nil {
			return nil, err
		}
		nodes[i] = sqlNode{
			node: Node{
				Dir:        nodeDir,
				DataDir:    filepath.Join(nodeDir, "data"),
				ConfigPath: filepath.Join(nodeDir, "config.yaml"),
				SQLAddr:    sqlAddr,
				ReplAddr:   replAddr,
				NodeID:     goharness.Hex(id[:]),
				Label:      "node" + strconv.Itoa(i+1),
			},
			key: Key,
		}
	}
	trusted := map[string]any{}
	for i := range nodes {
		priv := ed25519.NewKeyFromSeed(signing[i][:])
		trusted[nodes[i].node.NodeID] = goharness.Hex(priv.Public().(ed25519.PublicKey))
	}
	for i := range nodes {
		n := &nodes[i]
		var idBytes [16]byte
		for k := 0; k < 16; k++ {
			v, err := strconv.ParseUint(n.node.NodeID[2*k:2*k+2], 16, 8)
			if err != nil {
				return nil, fmt.Errorf("harness setup: node id: %w", err)
			}
			idBytes[k] = byte(v)
		}
		issued, err := ca.IssueWithHosts(idBytes, []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
		if err != nil {
			return nil, fmt.Errorf("harness setup: issue node cert: %w", err)
		}
		if err := os.WriteFile(filepath.Join(n.node.Dir, "ca.pem"), []byte(ca.CaPEM()), 0o644); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(n.node.Dir, "cert.pem"), []byte(issued.CertPEM), 0o644); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(n.node.Dir, "key.pem"), []byte(issued.KeyPEM), 0o600); err != nil {
			return nil, err
		}
		if runtime.GOOS != "windows" {
			if err := os.Chmod(filepath.Join(n.node.Dir, "key.pem"), 0o600); err != nil {
				return nil, err
			}
		}
		var peers []string
		if !opts.ManualPeers {
			for j := range nodes {
				if j != i {
					peers = append(peers, nodes[j].node.ReplAddr.String())
				}
			}
			if peers == nil {
				peers = []string{}
			}
		} else {
			peers = []string{}
		}
		config, err := renderConfig(n, base, signing[i], trusted, peers, opts)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(n.node.ConfigPath, config, 0o644); err != nil {
			return nil, err
		}
	}
	cluster.nodes = nodes
	for i := range cluster.nodes {
		if err := cluster.StartNode(i); err != nil {
			return nil, err
		}
	}
	return cluster, nil
}

func renderConfig(n *sqlNode, databaseID [16]byte, seed [32]byte, trusted map[string]any, peers []string, opts ClusterOptions) ([]byte, error) {
	hash, err := hash()
	if err != nil {
		return nil, err
	}
	repl := map[string]any{
		"enabled":          !opts.LocalOnly,
		"bind":             n.node.ReplAddr.String(),
		"bootstrap":        peers,
		"cert_chain_file":  filepath.ToSlash(filepath.Join(n.node.Dir, "cert.pem")),
		"private_key_file": filepath.ToSlash(filepath.Join(n.node.Dir, "key.pem")),
		"ca_file":          filepath.ToSlash(filepath.Join(n.node.Dir, "ca.pem")),
		"signing_key_hex":  goharness.Hex(seed[:]),
		"trusted_origins":  trusted,
	}
	if strings.TrimSpace(opts.ReplicationYAML) != "" {
		var frag map[string]any
		if err := yaml.Unmarshal([]byte(opts.ReplicationYAML), &frag); err != nil {
			return nil, fmt.Errorf("harness setup: invalid replication YAML: %w", err)
		}
		for k, v := range frag {
			repl[k] = v
		}
	}
	if opts.Files {
		repl["files"] = map[string]any{
			"directory":      filepath.ToSlash(filepath.Join(n.node.Dir, "objects")),
			"key_hex":        strings.Repeat("3d", 32),
			"fetch_interval": "100ms",
		}
	}
	root := map[string]any{
		"server": map[string]any{
			"bind":       n.node.SQLAddr.String(),
			"production": false,
		},
		"storage": map[string]any{
			"data_dir":    filepath.ToSlash(n.node.DataDir),
			"database_id": goharness.Hex(databaseID[:]),
			"node_id":     n.node.NodeID,
		},
		"auth": map[string]any{
			"users": []any{map[string]any{"name": "admin", "password_hash": hash}},
		},
		"logging": map[string]any{
			"dir":     filepath.ToSlash(filepath.Join(n.node.Dir, "logs")),
			"console": false,
		},
		"backups": map[string]any{
			"dir": filepath.ToSlash(filepath.Join(n.node.Dir, "backups")),
		},
		"replication": repl,
	}
	if strings.TrimSpace(opts.ExtraYAML) != "" {
		var frag map[string]any
		if err := yaml.Unmarshal([]byte(opts.ExtraYAML), &frag); err != nil {
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

// Len returns the node count.
func (c *Cluster) Len() int { return len(c.nodes) }

// IsEmpty reports whether the cluster has no nodes.
func (c *Cluster) IsEmpty() bool { return len(c.nodes) == 0 }

// Node returns the open wire connection. It panics if the node is
// stopped (a test bug).
func (c *Cluster) Node(i int) *goharness.Client {
	if c.nodes[i].client == nil {
		panic("SQL node stopped")
	}
	return c.nodes[i].client
}

// NodeInfo returns node metadata. It panics on unknown index (a test bug).
func (c *Cluster) NodeInfo(i int) *Node { return &c.nodes[i].node }

// PID returns the child PID, or false while stopped, for scoped
// signal/network-fault scenarios; the harness retains ownership and
// still reaps the process through StopNode. It panics on unknown index.
func (c *Cluster) PID(i int) (int, bool) {
	if c.nodes[i].child == nil {
		return 0, false
	}
	return c.nodes[i].child.cmd.Process.Pid, true
}

// RuntimeDir returns the unique runtime dir of this run.
func (c *Cluster) RuntimeDir() string { return c.dir }

// Config returns the wire-client config for node i.
func (c *Cluster) Config(i int) goharness.Config {
	cfg := goharness.NewConfig("admin", Password, c.nodes[i].key)
	cfg.Port = c.nodes[i].node.SQLAddr.Port()
	cfg.TLS = c.nodes[i].tls
	return cfg
}

// Connect opens an independent wire-client connection to node i.
func (c *Cluster) Connect(i int) (*goharness.Client, error) {
	return goharness.Connect(c.Config(i))
}

// StopNode stops node i: the client is dropped and the child killed and
// reaped. A signal scenario may already have terminated the process; that
// is tolerated, but a live process that resists kill is an error.
func (c *Cluster) StopNode(i int) error {
	n := &c.nodes[i]
	if n.client != nil {
		_ = n.client.Close()
		n.client = nil
	}
	child := n.child
	if child == nil {
		return nil
	}
	n.child = nil
	if exited, _ := child.exited(); exited {
		return nil
	}
	if err := child.cmd.Process.Kill(); err != nil {
		if exited, _ := child.exited(); !exited {
			return fmt.Errorf("kill SQL server: %w", err)
		}
		return nil
	}
	// A killed child reports a signal status; reap it without failing.
	<-child.done
	return nil
}

// StartNode starts stopped node i and connects.
func (c *Cluster) StartNode(i int) error {
	n := &c.nodes[i]
	if n.child != nil {
		return fmt.Errorf("harness setup: node already running")
	}
	log, err := os.OpenFile(filepath.Join(n.node.Dir, "server.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer log.Close()
	server, _, err := binaries()
	if err != nil {
		return err
	}
	child, err := spawnChild(server, n.node.ConfigPath, log)
	if err != nil {
		return err
	}
	n.child = child
	deadline := time.Now().Add(30 * time.Second)
	for {
		if exited, status := child.exited(); exited {
			n.child = nil
			body, _ := os.ReadFile(filepath.Join(n.node.Dir, "server.log"))
			return fmt.Errorf("%s exited %v: %s", n.node.Label, status, body)
		}
		conn, err := net.DialTimeout("tcp", n.node.SQLAddr.String(), 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("SQL listener timeout: %s", n.node.Label)
		}
		time.Sleep(30 * time.Millisecond)
	}
	client, err := goharness.Connect(c.Config(i))
	if err != nil {
		return err
	}
	n.client = client
	return nil
}

// SetKey replaces node i's stored unlock key.
func (c *Cluster) SetKey(i int, key string) { c.nodes[i].key = key }

// SetClientTLS matches client-side TLS to a stopped node's configured
// SQL listener.
func (c *Cluster) SetClientTLS(i int, tls goharness.TLSConfig) { c.nodes[i].tls = tls }

// SetConfig sets keys in a (dotted) config section of node i while
// stopped; missing sections are created. Values may be strings, bools,
// integers, or string lists. Server-written table flags are preserved.
//
// Deviation: the Rust edit_config hands scenarios a toml_edit document
// closure. This port covers every real use (replication/server.tls key
// sets) with section surgery on the YAML config.
func (c *Cluster) SetConfig(i int, section string, kv map[string]any) error {
	path := c.nodes[i].node.ConfigPath
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var doc map[string]any
	if err := yaml.Unmarshal(body, &doc); err != nil {
		return fmt.Errorf("harness setup: parse %s: %w", path, err)
	}
	if doc == nil {
		doc = map[string]any{}
	}
	target := doc
	if section != "" {
		for _, part := range strings.Split(section, ".") {
			next, ok := target[part].(map[string]any)
			if !ok {
				if _, exists := target[part]; exists {
					return fmt.Errorf("harness setup: config section %q is not a map", section)
				}
				next = map[string]any{}
				target[part] = next
			}
			target = next
		}
	}
	for k, v := range kv {
		target[k] = v
	}
	out, err := yaml.Marshal(doc)
	if err != nil {
		return err
	}
	return os.WriteFile(path, out, 0o644)
}

// SetPeers replaces bootstrap peers while stopped; restart both sides to
// close old sessions.
func (c *Cluster) SetPeers(i int, peers []int) error {
	addrs := make([]string, 0, len(peers))
	for _, p := range peers {
		addrs = append(addrs, c.nodes[p].node.ReplAddr.String())
	}
	return c.SetConfig(i, "replication", map[string]any{"bootstrap": addrs, "membership": false})
}

// Finish stops every node (best effort), then retains-or-cleans the
// runtime dir and prints the RESULT line to stderr.
//
// Deviation: the Rust harness prints its RESULT line in Drop. Go has no
// equivalent; scenarios must call Finish (usually deferred right after
// NewCluster, with a failed flag).
func (c *Cluster) Finish(failed bool) {
	for i := range c.nodes {
		n := &c.nodes[i]
		if n.client != nil {
			_ = n.client.Close()
			n.client = nil
		}
		if n.child != nil {
			_ = n.child.cmd.Process.Kill()
			select {
			case <-n.child.done:
			case <-time.After(10 * time.Second):
			}
			n.child = nil
		}
	}
	if failed {
		fmt.Fprintf(os.Stderr, "RESULT: FAIL scenario=%s artifacts=%s\n", c.name, c.dir)
	} else {
		_ = os.RemoveAll(c.dir)
		fmt.Fprintf(os.Stderr, "RESULT: PASS scenario=%s\n", c.name)
	}
}

// AssertRows asserts exact SQL rows with a bounded convergence deadline.
func AssertRows(cluster *Cluster, nodes []int, query string, expected [][]string) error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		all := true
		type mismatch struct {
			node int
			rows [][]string
		}
		var last []mismatch
		for _, i := range nodes {
			rows, err := cluster.Node(i).Simple(query)
			if err != nil {
				if code := goharness.ErrorCode(err); code == "42703" || code == "42P01" {
					all = false
					continue
				}
				return fmt.Errorf("poll SQL state: %w", err)
			}
			if !rowsEqual(rows, expected) {
				all = false
				last = append(last, mismatch{node: i, rows: rows})
			}
		}
		if all {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("SQL convergence timed out: %s; expected=%v, got=%v", query, expected, last)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// AssertNoPlaintext recursively inspects durable artifacts without
// opening an embedded database, failing when any needle appears.
func AssertNoPlaintext(root string, needles [][]byte) error {
	for _, needle := range needles {
		if len(needle) == 0 {
			return fmt.Errorf("plaintext audit: empty needle")
		}
	}
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, needle := range needles {
			if bytes.Index(contents, needle) >= 0 {
				return fmt.Errorf("plaintext found in %s", path)
			}
		}
		return nil
	})
}

// SQLLiteral escapes a literal command argument; identifiers/keywords
// cannot be parameters.
func SQLLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// rowsEqual compares row sets by value, treating a nil set as equal to an
// empty one (both mean "no rows").
func rowsEqual(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

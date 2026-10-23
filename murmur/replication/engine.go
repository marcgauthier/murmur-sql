package replication

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/postcard"
)

// ReplicationMetrics counts engine activity. Field names mirror the Rust
// struct for status consumers.
type ReplicationMetrics struct {
	PushedTransactions   uint64 `json:"pushed_transactions"`
	ReceivedTransactions uint64 `json:"received_transactions"`
	AppliedTransactions  uint64 `json:"applied_transactions"`
	StagedTransactions   int    `json:"staged_transactions"`
	SnapshotsApplied     uint64 `json:"snapshots_applied"`
	GossipRounds         uint64 `json:"gossip_rounds"`
	ProbesSent           uint64 `json:"probes_sent"`
	ProbesFailed         uint64 `json:"probes_failed"`
	AntiEntropyRuns      uint64 `json:"anti_entropy_runs"`
	SchemaPushes         uint64 `json:"schema_pushes"`
	SchemaAdoptions      uint64 `json:"schema_adoptions"`
	RetentionRuns        uint64 `json:"retention_runs"`
	RetentionPruned      uint64 `json:"retention_pruned"`
	QueueAvailableBytes  int    `json:"queue_available_bytes"`
	ChunkNeedSent        uint64 `json:"chunk_need_sent"`
	ChunkNeedServed      uint64 `json:"chunk_need_served"`
	NeedServed           uint64 `json:"need_served"`
	ApplyBatches         uint64 `json:"apply_batches"`
	ApplyBytes           uint64 `json:"apply_bytes"`
	ApplyNanos           uint64 `json:"apply_nanos"`
}

// PeerDiagnostics describes one connected peer.
type PeerDiagnostics struct {
	NodeID            NodeId          `json:"node_id"`
	Address           *netip.AddrPort `json:"address"`
	Connected         bool            `json:"connected"`
	LastSeenMillisAgo *uint64         `json:"last_seen_millis_ago"`
	SentWatermark     uint64          `json:"sent_watermark"`
	SchemaAgreed      bool            `json:"schema_agreed"`
	PushMode          bool            `json:"push_mode"`
}

// Status is the engine status snapshot.
type Status struct {
	Running             bool               `json:"running"`
	Address             *netip.AddrPort    `json:"address"`
	Peers               int                `json:"peers"`
	Members             int                `json:"members"`
	StagedTransactions  int                `json:"staged_transactions"`
	AppliedTransactions uint64             `json:"applied_transactions"`
	LastError           *string            `json:"last_error"`
	Deferred            bool               `json:"deferred"`
	FileEndpoint        *netip.AddrPort    `json:"file_endpoint"`
	Metrics             ReplicationMetrics `json:"metrics"`
	PeersDiagnostics    []PeerDiagnostics  `json:"peers_diagnostics"`
}

type engineCommandKind int

const (
	cmdAdd engineCommandKind = iota
	cmdRemove
	cmdRetire
	cmdRetentionStatus
	cmdCollect
	cmdSync
	cmdStop
)

type engineCommand struct {
	kind         engineCommandKind
	addr         netip.AddrPort
	node         NodeId
	retireReply  chan error
	statusReply  chan retentionStatusResult
	collectReply chan collectResult
}

type retentionStatusResult struct {
	status *RetentionStatus
	err    error
}

type collectResult struct {
	pruned uint64
	err    error
}

type engineControl struct {
	backend       Backend
	stop          atomic.Bool
	statusMu      sync.Mutex
	status        Status
	commands      chan engineCommand
	objects       *FileObjects
	bridge        *Bridge
	startedMillis uint64
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
}

// Handle owns the replication engine's control plane.
type Handle struct{ control *engineControl }

// Start validates the configuration, binds the endpoint, and runs the
// engine loop in the background.
func Start(config *Config, backend Backend) (*Handle, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if err := BindSigner(backend, config); err != nil {
		return nil, err
	}
	if err := MigratePeers(backend); err != nil {
		return nil, err
	}
	var objects *FileObjects
	if config.Files != nil {
		o, err := OpenFileObjects(*config.Files, backend)
		if err != nil {
			return nil, err
		}
		objects = o
	}
	var bridge *Bridge
	if config.Bridge != nil {
		b, err := OpenBridge(*config.Bridge, backend)
		if err != nil {
			return nil, err
		}
		bridge = b
	}
	if bridge != nil {
		bridge.AttachFiles(objects)
	}
	ctx, cancel := context.WithCancel(context.Background())
	control := &engineControl{
		backend: backend, commands: make(chan engineCommand, 128),
		objects: objects, bridge: bridge, startedMillis: NowMillis(),
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
	}
	_, node := backend.Identity()
	endpoint, err := newEndpoint(config, config.Bind, node, ALPN)
	if err != nil {
		cancel()
		return nil, err
	}
	address := endpoint.LocalAddr()
	var files *fetchService
	if objects != nil {
		f, err := startFetchService(config, backend, objects, ctx)
		if err != nil {
			endpoint.Close()
			cancel()
			return nil, err
		}
		files = f
	}
	core := newEngineCore(config, backend, endpoint, files, control)
	core.viewMu.Lock()
	core.view.Local.Node = node
	if config.Advertise != nil {
		core.view.Local.Address = *config.Advertise
	} else {
		core.view.Local.Address = address
	}
	incarnationKey := []byte("07/replication/incarnation/" + Hex(node[:]))
	var incarnation uint64
	if raw, err := backend.Get(incarnationKey); err == nil && raw != nil {
		if v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() }); err == nil {
			incarnation = v
		}
	}
	incarnation++
	core.view.Local.Incarnation = incarnation
	_ = backend.Commit([]Record{PutRecord(incarnationKey, encode(func(e *postcard.Encoder) { e.U64(incarnation) }))})
	excluded, _ := backend.Keys([]byte("07/replication/excluded/"))
	for _, key := range excluded {
		if raw, err := backend.Get(key); err == nil && raw != nil {
			if id, err := decodeOne(raw, func(d *postcard.Decoder) NodeId {
				var id NodeId
				copy(id[:], d.Raw(16))
				return id
			}); err == nil {
				core.view.Excluded[id] = true
			}
		}
	}
	core.viewMu.Unlock()
	control.statusMu.Lock()
	control.status.Running = true
	control.status.Address = &address
	if files != nil {
		fe := files.endpoint.LocalAddr()
		control.status.FileEndpoint = &fe
	}
	control.statusMu.Unlock()
	go core.run()
	return &Handle{control: control}, nil
}

// Files returns the file objects store, if configured.
func (h *Handle) Files() *FileObjects { return h.control.objects }

// Bridge returns the bridge, if configured.
func (h *Handle) Bridge() *Bridge { return h.control.bridge }

// Status snapshots engine status.
func (h *Handle) Status() Status {
	h.control.statusMu.Lock()
	defer h.control.statusMu.Unlock()
	return h.control.status
}

func (h *Handle) tryCommand(cmd engineCommand) error {
	select {
	case h.control.commands <- cmd:
		return nil
	default:
		return ErrOverloaded
	}
}

// AddPeer dials an address, re-admitting whoever answers there.
func (h *Handle) AddPeer(address netip.AddrPort) error {
	return h.tryCommand(engineCommand{kind: cmdAdd, addr: address})
}

// RemovePeer excludes a node and drops its session.
func (h *Handle) RemovePeer(node NodeId) error {
	return h.tryCommand(engineCommand{kind: cmdRemove, node: node})
}

// RetirePeer permanently excludes a peer and releases its history
// retention obligation.
func (h *Handle) RetirePeer(node NodeId) error {
	reply := make(chan error, 1)
	if err := h.tryCommand(engineCommand{kind: cmdRetire, node: node, retireReply: reply}); err != nil {
		return err
	}
	select {
	case err := <-reply:
		return err
	case <-h.control.ctx.Done():
		return unavailableErr("replication stopped")
	}
}

// RetentionStatus reports retention state.
func (h *Handle) RetentionStatus() (*RetentionStatus, error) {
	reply := make(chan retentionStatusResult, 1)
	if err := h.tryCommand(engineCommand{kind: cmdRetentionStatus, statusReply: reply}); err != nil {
		return nil, err
	}
	select {
	case r := <-reply:
		return r.status, r.err
	case <-h.control.ctx.Done():
		return nil, unavailableErr("replication stopped")
	}
}

// CollectRetention runs a bounded collection pass now.
func (h *Handle) CollectRetention() (uint64, error) {
	reply := make(chan collectResult, 1)
	if err := h.tryCommand(engineCommand{kind: cmdCollect, collectReply: reply}); err != nil {
		return 0, err
	}
	select {
	case r := <-reply:
		return r.pruned, r.err
	case <-h.control.ctx.Done():
		return 0, unavailableErr("replication stopped")
	}
}

// ForceSync requests progress from all peers.
func (h *Handle) ForceSync() error {
	return h.tryCommand(engineCommand{kind: cmdSync})
}

// Close stops the engine and waits for its loop to exit.
func (h *Handle) Close() error {
	h.control.stop.Store(true)
	h.control.cancel()
	h.control.backend.CancelPending()
	_ = h.tryCommand(engineCommand{kind: cmdStop})
	<-h.control.done
	return nil
}

type schemaStamp struct {
	digest   [32]byte
	sequence uint64
}

type enginePeer struct {
	conn          quicConn
	fetch         *netip.AddrPort
	send          *queueSender
	progress      map[NodeId]uint64
	lastSeen      time.Time
	push          bool
	lazy          bool
	outgoing      bool
	sent          map[NodeId]uint64
	sentAt        map[NodeId]time.Time
	resentAt      map[NodeId]time.Time
	resent        map[NodeId]uint64
	ackAnyAt      *time.Time
	bulkLagSince  *time.Time
	pushOrigin    *NodeId
	schemaAcked   *schemaStamp
	schemaPushAt  time.Time
	schemaSent    *schemaStamp
	lastTouch     *time.Time
	snapshotReqAt *time.Time
}

type relayKey struct {
	target NodeId
	nonce  uint64
}

type relayEntry struct {
	requester NodeId
	at        time.Time
}

type chunkKey struct {
	origin NodeId
	seq    uint64
}

// ChunkProgress paces repair requests for one chunked transfer.
type chunkProgress struct {
	received uint32
	updated  time.Time
	repaired time.Time
}

type chunkHolders struct {
	holders map[NodeId]map[uint32]bool
	at      time.Time
}

type transferKey struct {
	sender int64
	origin NodeId
	seq    uint64
}

type chunkCounters struct {
	count int
	total int
}

type missingEntry struct {
	transfer NodeId
	missing  []uint32
}

type engineCore struct {
	config   *Config
	backend  Backend
	endpoint quicListener
	files    *fetchService
	control  *engineControl
	ctx      context.Context

	peersMu  sync.Mutex
	peers    map[NodeId]*enginePeer
	viewMu   sync.Mutex
	view     *View
	sessions chan struct{}

	relaysMu sync.Mutex
	relays   map[relayKey]relayEntry

	stageGate     sync.Mutex
	stageCommit   sync.Mutex
	retainedGate  sync.Mutex
	retentionGate sync.Mutex

	queueLease   *byteLease
	controlLease *byteLease

	tasks       sync.WaitGroup
	applyNotify chan struct{}

	stagedMu sync.Mutex
	staged   *stagedSizes

	lastOriginMu sync.Mutex
	lastOrigin   NodeId

	incomingHintMu   sync.Mutex
	incomingHint     *NodeId
	incomingNoneAtMu sync.Mutex
	incomingNoneAt   *time.Time
	missingMu        sync.Mutex
	missingCache     *missingEntry

	adaptiveBatch atomic.Int64

	verifyMu     sync.Mutex
	verification Cache

	transfers chan struct{}

	schemaReqAtMu sync.Mutex
	schemaReqAt   *time.Time

	chunkCountersMu sync.Mutex
	chunkCounters   *chunkCounters

	transfersActiveMu sync.Mutex
	transfersActive   map[transferKey]bool

	chunkSeenMu    sync.Mutex
	chunkSeen      map[chunkKey]*chunkProgress
	chunkHoldersMu sync.Mutex
	chunkHolders   map[chunkKey]*chunkHolders

	pendingAdmitMu sync.Mutex
	pendingAdmit   map[netip.AddrPort]uint64
	admitRemovedMu sync.Mutex
	admitRemoved   map[NodeId]uint64
	admitSeq       atomic.Uint64

	needAtMu sync.Mutex
	needAt   map[chunkKey]time.Time

	outgoingMu sync.Mutex
	outgoing   outgoingShared
}

func newEngineCore(config *Config, backend Backend, endpoint quicListener, files *fetchService, control *engineControl) *engineCore {
	_, node := backend.Identity()
	_ = node
	c := &engineCore{
		config: config, backend: backend, endpoint: endpoint, files: files,
		control: control, ctx: control.ctx,
		peers:           make(map[NodeId]*enginePeer),
		sessions:        make(chan struct{}, config.Sessions),
		relays:          make(map[relayKey]relayEntry),
		queueLease:      newByteLease(int64(config.QueueBytes - (1 << 20))),
		controlLease:    newByteLease(1 << 20),
		applyNotify:     make(chan struct{}, 1),
		transfers:       make(chan struct{}, 2),
		transfersActive: make(map[transferKey]bool),
		chunkSeen:       make(map[chunkKey]*chunkProgress),
		chunkHolders:    make(map[chunkKey]*chunkHolders),
		pendingAdmit:    make(map[netip.AddrPort]uint64),
		admitRemoved:    make(map[NodeId]uint64),
		needAt:          make(map[chunkKey]time.Time),
	}
	c.view = NewView(Member{Address: config.Bind, Incarnation: 0, State: MemberAlive})
	c.admitSeq.Store(1)
	c.adaptiveBatch.Store(1<<62 - 1)
	return c
}

func (c *engineCore) mutateStatus(fn func(*Status)) {
	c.control.statusMu.Lock()
	defer c.control.statusMu.Unlock()
	fn(&c.control.status)
}

func (c *engineCore) recordError(err error) {
	text := err.Error()
	c.mutateStatus(func(s *Status) { s.LastError = &text })
}

// spawn runs fn in a tracked goroutine that exits early on shutdown.
func (c *engineCore) spawn(fn func(ctx context.Context)) {
	c.tasks.Add(1)
	go func() {
		defer c.tasks.Done()
		if c.ctx.Err() != nil {
			return
		}
		fn(c.ctx)
	}()
}

func (c *engineCore) notifyApply() {
	select {
	case c.applyNotify <- struct{}{}:
	default:
	}
}

func (c *engineCore) applyLoop(ctx context.Context) {
	interval := c.config.IncomingInterval
	if interval > time.Second {
		interval = time.Second
	}
	retry := time.NewTicker(interval)
	defer retry.Stop()
	for {
		select {
		case <-c.applyNotify:
		case <-retry.C:
		case <-ctx.Done():
			return
		}
		for {
			more, err := c.applyPending()
			if err != nil {
				c.recordError(err)
				break
			}
			if !more {
				break
			}
		}
	}
}

func (c *engineCore) sessionsAvailable() int {
	return cap(c.sessions) - len(c.sessions)
}

// acceptAllowed reports whether an incoming connection may proceed to
// the session handshake: parseable remote address, allowed network,
// room under the connection cap, and session headroom past the
// fanout-plus-repairs reserve (gossip and repair must never starve
// behind a flood of half-open sessions).
func acceptAllowed(config *Config, remote netip.AddrPort, addrOK bool, peers, sessionsAvail int) bool {
	return addrOK && NetworkAllowed(config, remote) &&
		peers < config.Connections && sessionsAvail > config.Fanout+config.Repairs
}

func (c *engineCore) trySession() bool {
	select {
	case c.sessions <- struct{}{}:
		return true
	default:
		return false
	}
}

func (c *engineCore) releaseSession() {
	select {
	case <-c.sessions:
	default:
	}
}

func (c *engineCore) run() {
	defer close(c.control.done)
	c.spawn(func(ctx context.Context) { c.applyLoop(ctx) })
	apply := time.NewTicker(time.Second)
	defer apply.Stop()
	repair := time.NewTicker(c.config.AntiEntropyInterval)
	defer repair.Stop()
	push := time.NewTicker(100 * time.Millisecond)
	defer push.Stop()
	probes := time.NewTicker(time.Second)
	defer probes.Stop()
	gossip := time.NewTicker(200 * time.Millisecond)
	defer gossip.Stop()
	rotation := time.NewTicker(5 * time.Second)
	defer rotation.Stop()
	bootstrap := time.NewTicker(5 * time.Second)
	defer bootstrap.Stop()
	rotationCount := uint64(0)
	retention := time.NewTicker(c.config.RetentionInterval)
	defer retention.Stop()
	fileInterval := 30 * time.Second
	if c.config.Files != nil {
		fileInterval = c.config.Files.FetchInterval
	}
	fileTick := time.NewTicker(fileInterval)
	defer fileTick.Stop()
	seeds := append([]netip.AddrPort(nil), c.config.Bootstrap...)
	snapshotReceiptCursor := 0

	incoming := make(chan quicConn, 16)
	c.spawn(func(ctx context.Context) {
		for {
			conn, err := c.endpoint.Accept(ctx)
			if err != nil {
				return
			}
			select {
			case incoming <- conn:
			case <-ctx.Done():
				conn.CloseWithError(1, "shutdown")
				return
			}
		}
	})

loop:
	for {
		if c.control.stop.Load() {
			break
		}
		select {
		case conn := <-incoming:
			remote, err := netip.ParseAddrPort(conn.RemoteAddr().String())
			c.peersMu.Lock()
			peerCount := len(c.peers)
			c.peersMu.Unlock()
			if !acceptAllowed(c.config, remote, err == nil, peerCount, c.sessionsAvailable()) {
				refuse(conn)
				continue
			}
			c.spawn(func(ctx context.Context) {
				if err := c.accept(conn); err != nil {
					c.recordError(err)
					conn.CloseWithError(1, "protocol rejected")
				}
			})
		case cmd := <-c.control.commands:
			switch cmd.kind {
			case cmdStop:
				break loop
			case cmdAdd:
				c.commandAdd(cmd.addr, &seeds)
			case cmdRemove:
				c.commandRemove(cmd.node)
			case cmdRetire:
				c.retentionGate.Lock()
				_, node := c.backend.Identity()
				var result error
				if cmd.node == node {
					result = invalidErr("cannot retire this node")
				} else {
					result = Retire(c.backend, cmd.node)
				}
				if result == nil {
					c.viewMu.Lock()
					c.view.Excluded[cmd.node] = true
					c.viewMu.Unlock()
					seq := c.admitSeq.Add(1)
					c.admitRemovedMu.Lock()
					if len(c.admitRemoved) >= 4096 {
						c.evictMinAdmitRemoved()
					}
					c.admitRemoved[cmd.node] = seq
					c.admitRemovedMu.Unlock()
					c.peersMu.Lock()
					if peer, ok := c.peers[cmd.node]; ok {
						delete(c.peers, cmd.node)
						peer.conn.CloseWithError(0, "peer retired")
					}
					c.peersMu.Unlock()
				}
				c.retentionGate.Unlock()
				select {
				case cmd.retireReply <- result:
				case <-c.ctx.Done():
				}
			case cmdRetentionStatus:
				status, err := RetentionReport(c.backend)
				select {
				case cmd.statusReply <- retentionStatusResult{status, err}:
				case <-c.ctx.Done():
				}
			case cmdCollect:
				pruned, err := c.collectHistory()
				select {
				case cmd.collectReply <- collectResult{pruned, err}:
				case <-c.ctx.Done():
				}
			case cmdSync:
				c.requestProgress()
			}
		case <-bootstrap.C:
			c.bootstrapTick(&seeds)
		case <-apply.C:
			if err := Maintain(c.backend, NowMillis()); err != nil {
				c.recordError(err)
			}
			c.peersMu.Lock()
			online := make(map[NodeId]bool, len(c.peers))
			for id := range c.peers {
				online[id] = true
			}
			c.peersMu.Unlock()
			if err := reapForgottenPins(c.backend, NowMillis(), durationMillis(c.config.MaximumOffline), online); err != nil {
				c.recordError(err)
			}
			if err := c.resendSnapshotReceipts(&snapshotReceiptCursor); err != nil {
				c.recordError(err)
			}
			if err := c.repairChunks(); err != nil {
				c.recordError(err)
			}
			if c.control.bridge != nil {
				if _, err := c.control.bridge.Capture(); err != nil {
					c.recordError(err)
				} else if _, err := c.control.bridge.Process(); err != nil {
					c.recordError(err)
				}
			}
		case <-fileTick.C:
			if c.files != nil {
				c.peersMu.Lock()
				var sources []fileSource
				for id, p := range c.peers {
					if p.fetch != nil {
						addr := *p.fetch
						sources = append(sources, fileSource{node: id, addr: addr})
					}
				}
				c.peersMu.Unlock()
				if err := c.files.fetchMissing(sources); err != nil {
					c.recordError(err)
				}
				if _, err := c.files.objects.RotateKeys(4); err != nil {
					c.recordError(err)
				}
				if _, err := c.files.objects.Collect(7 * 24 * time.Hour); err != nil {
					c.recordError(err)
				}
			}
		case <-repair.C:
			c.mutateStatus(func(s *Status) { s.Metrics.AntiEntropyRuns++ })
			c.requestProgress()
			c.pushSchema()
		case <-push.C:
			if err := c.pushLogs(); err != nil {
				c.recordError(err)
			}
		case <-gossip.C:
			if c.config.Membership {
				c.mutateStatus(func(s *Status) { s.Metrics.GossipRounds++ })
				c.gossip()
			}
		case <-probes.C:
			if c.config.Membership {
				c.mutateStatus(func(s *Status) { s.Metrics.ProbesSent++ })
				c.probe()
			}
		case <-rotation.C:
			rotationCount++
			c.rotate(rotationCount%6 == 0)
		case <-retention.C:
			if _, err := c.collectHistory(); err != nil {
				c.recordError(err)
			}
		case <-c.ctx.Done():
			break loop
		}
		// Publish diagnostics without holding the status lock across waits.
		now := time.Now()
		c.peersMu.Lock()
		diagnostics := make([]PeerDiagnostics, 0, len(c.peers))
		for id, p := range c.peers {
			var addr *netip.AddrPort
			if parsed, err := netip.ParseAddrPort(p.conn.RemoteAddr().String()); err == nil {
				a := parsed
				addr = &a
			}
			ago := uint64(now.Sub(p.lastSeen).Milliseconds())
			if now.Before(p.lastSeen) {
				ago = 0
			}
			diagnostics = append(diagnostics, PeerDiagnostics{
				NodeID: id, Address: addr, Connected: !connClosed(p.conn),
				LastSeenMillisAgo: &ago, SentWatermark: p.sent[id],
				SchemaAgreed: p.schemaAcked != nil, PushMode: p.push,
			})
		}
		peerCount := len(c.peers)
		c.peersMu.Unlock()
		c.viewMu.Lock()
		memberCount := len(c.view.Members)
		c.viewMu.Unlock()
		available := int(c.queueLease.available())
		c.mutateStatus(func(s *Status) {
			s.Peers = peerCount
			s.Members = memberCount
			s.Metrics.AppliedTransactions = s.AppliedTransactions
			s.Metrics.StagedTransactions = s.StagedTransactions
			s.Metrics.QueueAvailableBytes = available
			s.PeersDiagnostics = diagnostics
		})
	}
	// Say goodbye: mark ourselves Left in a final datagram burst.
	c.viewMu.Lock()
	members := c.view.Gossip()
	c.viewMu.Unlock()
	db, node := c.backend.Identity()
	c.peersMu.Lock()
	conns := make([]quicConn, 0, len(c.peers))
	for _, p := range c.peers {
		conns = append(conns, p.conn)
	}
	c.peersMu.Unlock()
	for _, conn := range conns {
		bye := make([]Member, len(members))
		copy(bye, members)
		for i := range bye {
			if bye[i].Node == node {
				bye[i].State = MemberLeft
			}
		}
		if data := EncodePacket(&Packet{Kind: PacketPing, Database: db, Sender: node, Members: bye}); len(data) <= 1000 {
			_ = conn.SendDatagram(data)
		}
		conn.CloseWithError(0, "shutdown")
	}
	c.tasks.Wait()
	if c.files != nil {
		c.files.endpoint.Close()
	}
	c.endpoint.Close()
	c.mutateStatus(func(s *Status) { s.Running = false })
}

func (c *engineCore) commandAdd(address netip.AddrPort, seeds *[]netip.AddrPort) {
	c.viewMu.Lock()
	var known *NodeId
	for _, m := range c.view.Members {
		if m.Address == address {
			id := m.Node
			known = &id
			break
		}
	}
	if known != nil {
		delete(c.view.Excluded, *known)
	}
	c.viewMu.Unlock()
	if known != nil {
		if err := c.backend.Commit([]Record{DeleteRecord([]byte("07/replication/excluded/" + Hex(known[:])))}); err != nil {
			c.recordError(err)
		}
	} else {
		seq := c.admitSeq.Add(1)
		c.pendingAdmitMu.Lock()
		if len(c.pendingAdmit) >= 4096 {
			var oldest netip.AddrPort
			var oldestSeq uint64
			first := true
			for addr, s := range c.pendingAdmit {
				if first || s < oldestSeq {
					oldest, oldestSeq, first = addr, s, false
				}
			}
			if !first {
				delete(c.pendingAdmit, oldest)
			}
		}
		c.pendingAdmit[address] = seq
		c.pendingAdmitMu.Unlock()
	}
	found := false
	for _, s := range *seeds {
		if s == address {
			found = true
			break
		}
	}
	if len(*seeds) < 4096 && !found {
		*seeds = append(*seeds, address)
	}
	c.dial(address)
}

func (c *engineCore) commandRemove(id NodeId) {
	c.viewMu.Lock()
	c.view.Excluded[id] = true
	c.viewMu.Unlock()
	seq := c.admitSeq.Add(1)
	c.admitRemovedMu.Lock()
	if len(c.admitRemoved) >= 4096 {
		c.evictMinAdmitRemoved()
	}
	c.admitRemoved[id] = seq
	c.admitRemovedMu.Unlock()
	if err := c.backend.Commit([]Record{PutRecord(
		[]byte("07/replication/excluded/"+Hex(id[:])),
		encode(func(e *postcard.Encoder) { e.Raw(id[:]) }))}); err != nil {
		c.recordError(err)
	}
	c.peersMu.Lock()
	if peer, ok := c.peers[id]; ok {
		delete(c.peers, id)
		peer.conn.CloseWithError(0, "administratively removed")
	}
	c.peersMu.Unlock()
}

func (c *engineCore) evictMinAdmitRemoved() {
	var oldest NodeId
	var oldestSeq uint64
	first := true
	for id, s := range c.admitRemoved {
		if first || s < oldestSeq {
			oldest, oldestSeq, first = id, s, false
		}
	}
	if !first {
		delete(c.admitRemoved, oldest)
	}
}

func (c *engineCore) bootstrapTick(seeds *[]netip.AddrPort) {
	staleAfter := 3 * c.config.AntiEntropyInterval
	if staleAfter < 15*time.Second {
		staleAfter = 15 * time.Second
	}
	c.peersMu.Lock()
	for id, peer := range c.peers {
		if connClosed(peer.conn) || time.Since(peer.lastSeen) >= staleAfter {
			peer.conn.CloseWithError(0, "stale session")
			delete(c.peers, id)
		}
	}
	c.peersMu.Unlock()
	shuffled := append([]netip.AddrPort(nil), *seeds...)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	for _, address := range shuffled {
		if c.outgoingCount() >= c.config.Sessions {
			break
		}
		c.dial(address)
	}
	c.viewMu.Lock()
	var discovered []netip.AddrPort
	for _, m := range c.view.Members {
		if m.State == MemberAlive {
			discovered = append(discovered, m.Address)
			if len(discovered) >= 4096 {
				break
			}
		}
	}
	c.viewMu.Unlock()
	rand.Shuffle(len(discovered), func(i, j int) { discovered[i], discovered[j] = discovered[j], discovered[i] })
	for _, address := range discovered {
		if c.outgoingCount() >= c.config.Sessions {
			break
		}
		c.dial(address)
	}
}

func (c *engineCore) outgoingCount() int {
	c.peersMu.Lock()
	defer c.peersMu.Unlock()
	n := 0
	for _, p := range c.peers {
		if p.outgoing {
			n++
		}
	}
	return n
}

func (c *engineCore) collectHistory() (uint64, error) {
	c.retentionGate.Lock()
	defer c.retentionGate.Unlock()
	// Serialize against applies (shared retained heads), but never
	// against staging or snapshots: retention must not stall the
	// connection read loop.
	c.retainedGate.Lock()
	defer c.retainedGate.Unlock()
	// Peers with live sessions stay protected even when their acks go
	// quiet (backpressure stalls): pruning what a connected peer still
	// needs would wedge it into an unrecoverable snapshot requirement.
	c.peersMu.Lock()
	online := make(map[NodeId]bool, len(c.peers))
	for id := range c.peers {
		online[id] = true
	}
	c.peersMu.Unlock()
	pruned, err := Collect(c.backend, c.config, online, c.control.startedMillis)
	if err == nil {
		c.mutateStatus(func(s *Status) {
			s.Metrics.RetentionRuns++
			s.Metrics.RetentionPruned += pruned
		})
	}
	return pruned, err
}

func (c *engineCore) dial(address netip.AddrPort) {
	if !NetworkAllowed(c.config, address) || address == c.endpoint.LocalAddr() {
		return
	}
	c.peersMu.Lock()
	for _, p := range c.peers {
		if parsed, err := netip.ParseAddrPort(p.conn.RemoteAddr().String()); err == nil && parsed == address {
			c.peersMu.Unlock()
			return
		}
	}
	c.peersMu.Unlock()
	if !c.trySession() {
		return
	}
	c.spawn(func(ctx context.Context) {
		defer c.releaseSession()
		if err := c.dialInner(ctx, address); err != nil {
			c.recordError(err)
		}
	})
}

func (c *engineCore) dialInner(ctx context.Context, address netip.AddrPort) error {
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	conn, err := c.endpoint.Dial(dialCtx, address)
	cancel()
	if err != nil {
		return protocolErr("%v", err)
	}
	id, err := peerID(conn, c.config)
	if err != nil {
		conn.CloseWithError(1, "protocol rejected")
		return err
	}
	// An explicit AddPeer re-admits whoever answers there: the id is
	// authenticated by the TLS handshake, but known only now, and a
	// restarted peer answers from a fresh address the member table
	// cannot resolve. Clear before the Hello/Welcome exchange: the
	// far side may still reject this connection (its own exclusion
	// clears when its dial reaches us), and its close would otherwise
	// win the race against our Welcome read. The sequence check keeps
	// a newer Remove/Retire authoritative (last command wins).
	c.pendingAdmitMu.Lock()
	added, ok := c.pendingAdmit[address]
	if ok {
		delete(c.pendingAdmit, address)
	}
	c.pendingAdmitMu.Unlock()
	if ok {
		c.admitRemovedMu.Lock()
		removed := c.admitRemoved[id]
		c.admitRemovedMu.Unlock()
		if removed < added {
			c.viewMu.Lock()
			delete(c.view.Excluded, id)
			c.viewMu.Unlock()
			if err := c.backend.Commit([]Record{DeleteRecord([]byte("07/replication/excluded/" + Hex(id[:])))}); err != nil {
				c.recordError(err)
			}
		}
	}
	db, node := c.backend.Identity()
	schema, err := c.backend.Schema()
	if err != nil {
		conn.CloseWithError(1, "protocol rejected")
		return err
	}
	stream, err := c.openHandshakeStream(ctx, conn)
	if err != nil {
		if isDuplicateClose(conn, err) {
			return nil
		}
		return err
	}
	hello := Message{Kind: MsgHello, Capabilities: Capabilities, Required: Required,
		Version: Version, Database: db, Node: node, Schema: schema,
		Mode: c.config.Dissemination, MaxTransaction: uint64(c.config.MaxTransactionBytes)}
	if c.files != nil {
		addr := c.files.endpoint.LocalAddr()
		hello.Fetch = &addr
	}
	if err := WriteMessage(stream, &hello); err != nil {
		stream.Close()
		if isDuplicateClose(conn, err) {
			return nil
		}
		return protocolErr("%v", err)
	}
	welcome, err := readMessageTimeout(stream, 5*time.Second, "handshake timeout")
	if err != nil {
		stream.Close()
		if isDuplicateClose(conn, err) {
			return nil
		}
		return err
	}
	if welcome.Kind != MsgWelcome || welcome.Database != db || welcome.Node != id ||
		welcome.Mode != c.config.Dissemination || !compatibleCaps(welcome.Capabilities, welcome.Required) {
		stream.Close()
		conn.CloseWithError(1, "protocol rejected")
		return authErr("incompatible welcome")
	}
	if err := c.checkHandshakeSchema(welcome.Schema); err != nil {
		stream.Close()
		conn.CloseWithError(1, "protocol rejected")
		return err
	}
	return c.session(conn, stream, id, true, welcome.Fetch, welcome.Schema)
}

func (c *engineCore) openHandshakeStream(ctx context.Context, conn quicConn) (quicStream, error) {
	openCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := conn.OpenStreamSync(openCtx)
	if err != nil {
		return nil, protocolErr("%v", err)
	}
	return stream, nil
}

// readMessageTimeout reads one frame, abandoning the read after d. The
// stream is left for the session to close, which unblocks the reader.
func readMessageTimeout(stream quicStream, d time.Duration, reason string) (Message, error) {
	type result struct {
		msg Message
		err error
	}
	ch := make(chan result, 1)
	go func() {
		msg, err := ReadMessage(stream)
		ch <- result{msg, err}
	}()
	select {
	case r := <-ch:
		return r.msg, r.err
	case <-time.After(d):
		return Message{}, unavailableErr("%s", reason)
	}
}

func (c *engineCore) accept(conn quicConn) error {
	if !c.trySession() {
		return ErrOverloaded
	}
	defer c.releaseSession()
	id, err := peerID(conn, c.config)
	if err != nil {
		return err
	}
	acceptCtx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	stream, err := conn.AcceptStream(acceptCtx)
	cancel()
	if err != nil {
		return unavailableErr("stream timeout")
	}
	hello, err := readMessageTimeout(stream, 5*time.Second, "hello timeout")
	if err != nil {
		stream.Close()
		return err
	}
	db, node := c.backend.Identity()
	_ = node
	if hello.Kind != MsgHello || hello.Version != Version || hello.Database != db ||
		hello.Node != id || hello.Mode != c.config.Dissemination ||
		!compatibleCaps(hello.Capabilities, hello.Required) || hello.MaxTransaction == 0 {
		stream.Close()
		return authErr("incompatible hello")
	}
	if err := c.checkHandshakeSchema(hello.Schema); err != nil {
		stream.Close()
		return err
	}
	schema, err := c.backend.Schema()
	if err != nil {
		stream.Close()
		return err
	}
	welcome := Message{Kind: MsgWelcome, Capabilities: Capabilities, Required: Required,
		Database: db, Node: node, Schema: schema, Mode: c.config.Dissemination}
	if c.files != nil {
		addr := c.files.endpoint.LocalAddr()
		welcome.Fetch = &addr
	}
	if err := WriteMessage(stream, &welcome); err != nil {
		stream.Close()
		return protocolErr("%v", err)
	}
	return c.session(conn, stream, id, false, hello.Fetch, hello.Schema)
}

// checkHandshakeSchema treats handshake schema bytes as an unsigned
// mismatch hint only. Adoption happens exclusively through signed
// manifests inside the session.
func (c *engineCore) checkHandshakeSchema(peer []byte) error {
	ours, err := c.backend.Schema()
	if err != nil {
		return err
	}
	if string(peer) == string(ours) {
		return nil
	}
	if !c.config.AcceptRemoteSchema {
		return protocolErr("schema mismatch")
	}
	return nil
}

func (c *engineCore) isMaster() bool {
	_, node := c.backend.Identity()
	return c.config.Master && c.config.SchemaMaster != nil && *c.config.SchemaMaster == node
}

// manifest renders the current outbound manifest. The sequence bumps
// only when our canonical schema bytes change, so retransmits stay
// idempotent for receivers.
func (c *engineCore) manifest() (*SignedSchema, error) {
	bytes, err := c.backend.Schema()
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(bytes)
	key := SchemaManifestKey()
	var stored *schemaStamp
	if raw, err := c.backend.Get(key); err != nil {
		return nil, err
	} else if raw != nil {
		stamp, err := decodeOne(raw, func(d *postcard.Decoder) schemaStamp {
			var s schemaStamp
			copy(s.digest[:], d.Raw(32))
			s.sequence = d.U64()
			return s
		})
		if err != nil {
			return nil, err
		}
		stored = &stamp
	}
	var sequence uint64
	switch {
	case stored != nil && stored.digest == sum:
		sequence = stored.sequence
	case stored != nil:
		sequence = stored.sequence + 1
		if sequence == 0 {
			return nil, protocolErr("schema sequence exhausted")
		}
	default:
		sequence = 1
	}
	if stored == nil || stored.digest != sum || stored.sequence != sequence {
		if err := c.backend.Commit([]Record{PutRecord(key, encode(func(e *postcard.Encoder) {
			e.Raw(sum[:])
			e.U64(sequence)
		}))}); err != nil {
			return nil, err
		}
	}
	db, node := c.backend.Identity()
	signed := &SignedSchema{Database: db, Signer: node, Sequence: sequence, Schema: bytes}
	seed := c.config.SigningKey
	signed.Sign(&seed)
	return signed, nil
}

func (c *engineCore) adoptManifest(from NodeId, signed *SignedSchema, send *queueSender) error {
	_ = from
	decision, err := checkSchemaManifest(c.config, c.backend, signed)
	if err != nil {
		return err
	}
	switch decision {
	case schemaStale, schemaCurrent, schemaRecord:
		_ = send.TrySend(Message{Kind: MsgSchemaAck, Digest: signed.Digest(), Sequence: signed.Sequence})
	case schemaAdopt:
		accepted, err := c.backend.AcceptSchema(append([]byte(nil), signed.Schema...))
		if err != nil {
			return err
		}
		if !accepted {
			// Adopter busy (uncollected writes). Only a schema master
			// re-pushes unacked manifests, so an idle non-master
			// adopter would stall here forever once the mismatched
			// batches that triggered discovery stop arriving: ask for
			// the manifest again after a short drain delay instead of
			// relying on the sender.
			retry := send
			go func() {
				time.Sleep(time.Second)
				_ = retry.TrySend(Message{Kind: MsgSchemaRequest})
			}()
			return nil
		}
		if err := recordSchemaAccepted(c.backend, signed); err != nil {
			return err
		}
		c.mutateStatus(func(s *Status) { s.Metrics.SchemaAdoptions++ })
		_ = send.TrySend(Message{Kind: MsgSchemaAck, Digest: signed.Digest(), Sequence: signed.Sequence})
	}
	return nil
}

func (c *engineCore) wantSchema(send *queueSender) {
	now := time.Now()
	c.schemaReqAtMu.Lock()
	if c.schemaReqAt != nil && now.Sub(*c.schemaReqAt) < 5*time.Second {
		c.schemaReqAtMu.Unlock()
		return
	}
	c.schemaReqAt = &now
	c.schemaReqAtMu.Unlock()
	_ = send.TrySend(Message{Kind: MsgSchemaRequest})
}

// pushSchema has the master push its signed schema to peers that have
// not acked it.
func (c *engineCore) pushSchema() {
	if !c.isMaster() {
		return
	}
	m, err := c.manifest()
	if err != nil {
		c.recordError(err)
		return
	}
	current := schemaStamp{digest: m.Digest(), sequence: m.Sequence}
	now := time.Now()
	var pushed uint64
	c.peersMu.Lock()
	for _, peer := range c.peers {
		if (peer.schemaAcked != nil && *peer.schemaAcked == current) ||
			(peer.schemaSent != nil && *peer.schemaSent == current && now.Before(peer.schemaPushAt)) {
			continue
		}
		peer.schemaPushAt = now.Add(30 * time.Second)
		if peer.send.TrySend(Message{Kind: MsgSchemaManifest, Signed: *m}) == nil {
			stamp := current
			peer.schemaSent = &stamp
			pushed++
		}
	}
	c.peersMu.Unlock()
	if pushed > 0 {
		c.mutateStatus(func(s *Status) { s.Metrics.SchemaPushes += pushed })
	}
}

type inboundMsg struct {
	msg Message
	err error
}

func (c *engineCore) session(conn quicConn, stream quicStream, id NodeId, outgoing bool, fetch *netip.AddrPort, peerSchema []byte) error {
	_, node := c.backend.Identity()
	c.viewMu.Lock()
	excluded := c.view.Excluded[id]
	c.viewMu.Unlock()
	if id == node || excluded {
		stream.Close()
		return authErr("excluded peer")
	}
	func() {
		c.retentionGate.Lock()
		defer c.retentionGate.Unlock()
		_ = Touch(c.backend, id, nil)
	}()
	send, rx := withControlQueue(c.queueLease, c.controlLease, c.config.PeerQueueBytes)
	// A connection that just completed TLS + Hello/Welcome is proven live.
	// When the existing entry is already closed, or has carried no inbound
	// traffic for a while (a zombie to a restarted peer's dead address:
	// live sessions exchange Ping/Pong every second), replace it instead
	// of tie-breaking: the deterministic direction rule would otherwise
	// keep the zombie and close every fresh handshake until the 30s stale
	// prune whenever the id order prefers it, stalling re-mesh. A genuine
	// simultaneous-dial race always finds a fresh entry, so the rule below
	// still resolves it deterministically.
	c.peersMu.Lock()
	if old, ok := c.peers[id]; ok {
		preferOutgoing := nodeLess(node, id)
		zombie := connClosed(old.conn) || time.Since(old.lastSeen) > 5*time.Second
		if !zombie && (old.outgoing == preferOutgoing || outgoing != preferOutgoing) {
			c.peersMu.Unlock()
			stream.Close()
			conn.CloseWithError(0, duplicateCloseReason)
			return nil
		}
		old.conn.CloseWithError(0, duplicateCloseReason)
	}
	old, hadOld := c.peers[id]
	delete(c.peers, id)
	pushCount := 0
	for _, p := range c.peers {
		if p.push {
			pushCount++
		}
	}
	peer := &enginePeer{
		conn: conn, fetch: fetch, send: send,
		progress: make(map[NodeId]uint64), lastSeen: time.Now(),
		push: pushCount < c.config.Fanout, outgoing: outgoing,
		sent: make(map[NodeId]uint64), sentAt: make(map[NodeId]time.Time),
		resentAt: make(map[NodeId]time.Time), resent: make(map[NodeId]uint64),
		schemaPushAt: time.Now().Add(30 * time.Second),
	}
	if hadOld {
		peer.progress = old.progress
		peer.sent = old.sent
		peer.sentAt = old.sentAt
		peer.resentAt = old.resentAt
		peer.resent = old.resent
		peer.ackAnyAt = old.ackAnyAt
		peer.bulkLagSince = old.bulkLagSince
		peer.pushOrigin = old.pushOrigin
		peer.schemaAcked = old.schemaAcked
	}
	c.peers[id] = peer
	c.peersMu.Unlock()
	remote, _ := netip.ParseAddrPort(conn.RemoteAddr().String())
	c.viewMu.Lock()
	c.view.Merge(Member{Node: id, Address: remote, State: MemberAlive})
	c.viewMu.Unlock()
	c.spawn(func(ctx context.Context) {
		for {
			data, err := conn.ReceiveDatagram(ctx)
			if err != nil {
				return
			}
			if len(data) > 1000 {
				continue
			}
			if err := c.packet(id, conn, data); err != nil {
				c.recordError(err)
			}
		}
	})
	_ = send.TrySend(Message{Kind: MsgProgressRequest})
	// Schema sync starts inside the session: everyone asks (answers declare
	// each sender's replicated subset for payload filtering), and the
	// master additionally pushes so divergent peers converge without asking.
	ours, err := c.backend.Schema()
	if err != nil {
		stream.Close()
		return err
	}
	if string(peerSchema) != string(ours) {
		_ = send.TrySend(Message{Kind: MsgSchemaRequest})
		if c.isMaster() {
			if m, err := c.manifest(); err != nil {
				c.recordError(err)
			} else {
				_ = send.TrySend(Message{Kind: MsgSchemaManifest, Signed: *m})
			}
		}
	}
	done := make(chan struct{})
	defer close(done)
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- c.sessionWriter(conn, stream, send, rx)
	}()
	inbound := make(chan inboundMsg, 1)
	go func() {
		for {
			msg, err := ReadMessage(stream)
			select {
			case inbound <- inboundMsg{msg, err}:
				if err != nil {
					return
				}
			case <-done:
				return
			}
		}
	}()
	ping := time.NewTicker(time.Second)
	defer ping.Stop()
	var result error
sessionLoop:
	for {
		select {
		case err := <-writerDone:
			if err != nil && !isDuplicateClose(conn, err) {
				result = err
			}
			break sessionLoop
		case in := <-inbound:
			if in.err != nil {
				if !isDuplicateClose(conn, in.err) {
					result = in.err
				}
				break sessionLoop
			}
			c.peersMu.Lock()
			if p, ok := c.peers[id]; ok {
				p.lastSeen = time.Now()
			}
			c.peersMu.Unlock()
			if _, err := peerID(conn, c.config); err != nil {
				result = authErr("migrated peer denied")
				break sessionLoop
			}
			if err := c.message(id, in.msg, send); err != nil {
				_ = send.TrySend(Message{Kind: MsgError, Code: 1, ErrMessage: err.Error()})
				c.recordError(err)
				var rerr *Error
				if errors.As(err, &rerr) && rerr.Kind == KindAuth {
					result = err
					break sessionLoop
				}
			}
		case <-ping.C:
			if c.control.stop.Load() {
				break sessionLoop
			}
			_ = send.TrySend(Message{Kind: MsgPing, Nonce: rand.Uint64()})
		case <-c.ctx.Done():
			break sessionLoop
		}
	}
	// A superseded task (peers entry already replaced by the
	// duplicate tie-break) exits quietly: its read failure is
	// expected churn from our own deliberate close, not an engine
	// error, and must not pollute last_error.
	rx.Close()
	stream.Close()
	superseded := false
	c.peersMu.Lock()
	if p, ok := c.peers[id]; ok {
		if p.conn == conn {
			delete(c.peers, id)
		} else {
			superseded = true
		}
	}
	c.peersMu.Unlock()
	conn.CloseWithError(0, "session closed")
	if superseded {
		return nil
	}
	return result
}

func (c *engineCore) sessionWriter(conn quicConn, stream quicStream, send *queueSender, rx *queueReceiver) error {
	_ = send
	for {
		q, ok := rx.Recv()
		if !ok {
			return nil
		}
		remote, err := netip.ParseAddrPort(conn.RemoteAddr().String())
		if err != nil || !NetworkAllowed(c.config, remote) {
			q.Release()
			return authErr("migrated network denied")
		}
		err = writeQueuedTimeout(stream, q, 5*time.Second)
		q.Release()
		if err != nil {
			return err
		}
	}
}

func writeQueuedTimeout(stream quicStream, q *queued, d time.Duration) error {
	ch := make(chan error, 1)
	go func() {
		if q.Prepared != nil {
			ch <- q.Prepared.Write(stream)
		} else {
			ch <- WriteMessage(stream, &q.Message)
		}
	}()
	select {
	case err := <-ch:
		if err != nil {
			return protocolErr("%v", err)
		}
		return nil
	case <-time.After(d):
		return unavailableErr("write deadline")
	}
}

func nodeLess(a, b NodeId) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

func (c *engineCore) progress() ([]Progress, error) {
	return c.backend.Progress()
}

func (c *engineCore) requestProgress() {
	c.peersMu.Lock()
	defer c.peersMu.Unlock()
	for _, peer := range c.peers {
		_ = peer.send.TrySend(Message{Kind: MsgProgressRequest})
		if c.config.Membership {
			_ = peer.send.TrySend(Message{Kind: MsgMembersRequest})
		}
	}
}

func (c *engineCore) message(id NodeId, message Message, send *queueSender) error {
	switch message.Kind {
	case MsgBatches:
		if len(message.Transactions) > 64 {
			return ErrOverloaded
		}
		ours, err := c.backend.Schema()
		if err != nil {
			return err
		}
		schema := SchemaDigest(ours)
		for _, tx := range message.Transactions {
			if tx.Schema != schema {
				c.wantSchema(send)
				break
			}
		}
		return c.stageBatch(message.Transactions)
	case MsgPlumtreeData:
		duplicate := false
		if raw, err := c.backend.Get(ReceiptKey(message.Tx.Origin, message.Tx.Sequence)); err != nil {
			return err
		} else if raw != nil {
			duplicate = true
		}
		if !duplicate {
			if raw, err := c.backend.Get(StageKey(message.Tx.Origin, message.Tx.Sequence)); err != nil {
				return err
			} else if raw != nil {
				duplicate = true
			}
		}
		if err := c.stage(message.Tx); err != nil {
			return err
		}
		if duplicate {
			_ = send.TrySend(Message{Kind: MsgPrune})
		}
		return nil
	case MsgPrune:
		c.peersMu.Lock()
		if p, ok := c.peers[id]; ok {
			p.lazy = true
		}
		c.peersMu.Unlock()
		return nil
	case MsgIHave:
		var applied uint64
		if raw, err := c.backend.Get(ProgressKey(message.Origin)); err != nil {
			return err
		} else if raw != nil {
			v, err := decodeOne(raw, func(d *postcard.Decoder) uint64 { return d.U64() })
			if err != nil {
				return err
			}
			applied = v
		}
		if message.Sequence > applied {
			_ = send.TrySend(Message{Kind: MsgGraft, Origin: message.Origin, Sequence: message.Sequence})
		}
		return nil
	case MsgGraft:
		c.peersMu.Lock()
		if p, ok := c.peers[id]; ok {
			p.lazy = false
		}
		c.peersMu.Unlock()
		if err := c.sendTransaction(message.Origin, message.Sequence, send); err != nil {
			var rerr *Error
			if errors.As(err, &rerr) && rerr.Kind == KindUnavailable {
				missing, merr := historyDefinitivelyMissing(c.backend, message.Origin, message.Sequence)
				if merr != nil {
					return merr
				}
				if missing {
					_ = send.TrySend(Message{Kind: MsgError, Code: 3, ErrMessage: "history unavailable"})
				}
				return nil
			}
			return err
		}
		return nil
	case MsgNeed:
		if message.From == 0 || message.Through < message.From {
			return protocolErr("invalid missing range")
		}
		if os.Getenv("MURMUR_NEED_TIMING") != "" {
			top := message.Through
			if end := message.From + 15; end < top {
				top = end
			}
			needServedN.Add(top - message.From + 1)
			if needServeCtr.Add(1)%100 == 0 {
				fmt.Fprintf(os.Stderr, "[need-timing] served_per_100needs=%d\n", needServedN.Swap(0))
			}
		}
		served := message.From - 1
		end := message.Through
		if cap := satAdd(message.From, 15); cap < end {
			end = cap
		}
		for seq := message.From; ; seq++ {
			if err := c.sendTransaction(message.Origin, seq, send); err != nil {
				var rerr *Error
				switch {
				case errors.As(err, &rerr) && rerr.Kind == KindUnavailable:
					missing, merr := historyDefinitivelyMissing(c.backend, message.Origin, seq)
					if merr != nil {
						return merr
					}
					if missing {
						_ = send.TrySend(Message{Kind: MsgError, Code: 3, ErrMessage: "history unavailable"})
					}
					seq = end // break
				case errors.As(err, &rerr) && rerr.Kind == KindOverloaded:
					continue
				default:
					return err
				}
				break
			}
			if seq == served+1 {
				served = seq
			}
			if seq == end {
				break
			}
		}
		if served >= message.From {
			c.mutateStatus(func(s *Status) { s.Metrics.NeedServed += served - message.From + 1 })
			c.peersMu.Lock()
			if p, ok := c.peers[id]; ok {
				if served > p.sent[message.Origin] {
					p.sent[message.Origin] = served
				}
			}
			c.peersMu.Unlock()
		}
		return nil
	case MsgAck:
		return c.handleProgress(id, message.Ack, send)
	case MsgProgressPage:
		if message.More {
			if err := c.handleProgress(id, message.Entries, send); err != nil {
				return err
			}
			_ = send.TrySend(Message{Kind: MsgProgressRequest, Offset: message.Offset + 128})
			return nil
		}
		return c.handleProgress(id, message.Entries, send)
	case MsgProgressRequest:
		entries, err := c.progress()
		if err != nil {
			return err
		}
		start := int(message.Offset)
		if start > len(entries) {
			return protocolErr("progress offset")
		}
		end := start + 128
		if end > len(entries) {
			end = len(entries)
		}
		page := append([]Progress(nil), entries[start:end]...)
		_ = send.TrySend(Message{Kind: MsgProgressPage, Offset: message.Offset, More: end < len(entries), Entries: page})
		return nil
	case MsgTransactionChunk:
		return c.stageChunk(message.Chunk)
	case MsgChunkNeed:
		return c.serveChunkNeed(message.Origin, message.Sequence, message.Indexes, send)
	case MsgChunkAvailabilityRequest:
		return c.serveChunkAvailability(message.Offset, send)
	case MsgChunkAvailabilityPage:
		return c.mergeChunkAvailability(id, message.Availability)
	case MsgSnapshotRequest:
		c.sendSnapshot(id, send)
		return nil
	case MsgSnapshotResume:
		c.stageGate.Lock()
		defer c.stageGate.Unlock()
		m, err := Outgoing(c.backend, id, message.Transfer, NowMillis())
		if err != nil {
			return err
		}
		if m != nil {
			return send.TrySend(Message{Kind: MsgSnapshotManifest, Manifest: *m})
		}
		return send.TrySend(Message{Kind: MsgSnapshotUnavailable, Transfer: message.Transfer})
	case MsgSnapshotManifest:
		c.stageGate.Lock()
		defer c.stageGate.Unlock()
		fmt.Fprintf(os.Stderr, "[snap-rx] manifest from %02x%02x chunks=%d transfer=%02x%02x\n",
			id[0], id[1], len(message.Manifest.Chunks), message.Manifest.Transfer[0], message.Manifest.Transfer[1])
		// Committed to a different transfer: release this donor now.
		// Its pin wedges its retention until expiry otherwise, and
		// this transfer will never be installed here. The donor
		// authenticates the cancellation against the pin recipient.
		if r, err := ReadIncoming(c.backend); err != nil {
			return err
		} else if r != nil && r.Manifest.Transfer != message.Transfer {
			_ = send.TrySend(Message{Kind: MsgSnapshotCancel, Transfer: message.Transfer})
			return nil
		}
		if err := Accept(c.backend, c.config, id, &message.Manifest, NowMillis()); err != nil {
			return err
		}
		transfer := message.Manifest.Transfer
		c.incomingHintMu.Lock()
		c.incomingHint = &transfer
		c.incomingHintMu.Unlock()
		c.incomingNoneAtMu.Lock()
		c.incomingNoneAt = nil
		c.incomingNoneAtMu.Unlock()
		return c.requestSnapshotChunks(id, message.Manifest.Transfer, send)
	case MsgSnapshotChunk:
		if !c.config.TrustedSnapshotSources[id] {
			return authErr("untrusted snapshot chunk source")
		}
		c.stageGate.Lock()
		defer c.stageGate.Unlock()
		if err := Receive(c.backend, id, message.Transfer, message.Index, message.Data, NowMillis()); err != nil {
			return err
		}
		c.missingMu.Lock()
		c.missingCache = nil
		c.missingMu.Unlock()
		return nil
	case MsgSnapshotNeed:
		return c.sendSnapshotChunks(id, message.Transfer, message.Indexes, send)
	case MsgSnapshotDone:
		c.stageGate.Lock()
		defer c.stageGate.Unlock()
		return c.requestSnapshotChunks(id, message.Transfer, send)
	case MsgSnapshotApplied:
		c.stageGate.Lock()
		defer c.stageGate.Unlock()
		return c.backend.AcknowledgeSnapshot(id, message.Transfer, message.Digest)
	case MsgSnapshotCancel:
		c.stageGate.Lock()
		defer c.stageGate.Unlock()
		if err := Cancel(c.backend, id, message.Transfer); err != nil {
			return err
		}
		return Maintain(c.backend, NowMillis())
	case MsgSnapshotUnavailable:
		c.stageGate.Lock()
		defer c.stageGate.Unlock()
		r, err := c.cachedIncoming()
		if err != nil {
			return err
		}
		if r != nil {
			if r.Manifest.Donor != id || r.Manifest.Transfer != message.Transfer {
				return authErr("snapshot unavailable identity")
			}
			if err := send.TrySend(Message{Kind: MsgSnapshotCancel, Transfer: message.Transfer}); err != nil {
				return err
			}
			if err := c.backend.Commit([]Record{DeleteRecord(SnapshotHeaderKey(SnapshotIn, message.Transfer))}); err != nil {
				return err
			}
			if err := Maintain(c.backend, NowMillis()); err != nil {
				return err
			}
			c.requestSnapshotDamped(id, send)
		}
		return nil
	case MsgMembersRequest:
		if !c.config.Membership {
			return nil
		}
		c.viewMu.Lock()
		members := make([]Member, 0, len(c.view.Members)+1)
		members = append(members, c.view.Local)
		for _, m := range c.view.Members {
			members = append(members, m)
		}
		c.viewMu.Unlock()
		sort.Slice(members, func(i, j int) bool { return nodeLess(members[i].Node, members[j].Node) })
		start := int(message.Offset)
		if start > len(members) {
			return protocolErr("membership offset")
		}
		end := start + 128
		if end > len(members) {
			end = len(members)
		}
		page := append([]Member(nil), members[start:end]...)
		_ = send.TrySend(Message{Kind: MsgMembersPage, Offset: message.Offset, More: end < len(members), Members: page})
		return nil
	case MsgMembersPage:
		if len(message.Members) > 128 {
			return ErrOverloaded
		}
		c.viewMu.Lock()
		for _, member := range message.Members {
			c.view.Merge(member)
		}
		c.viewMu.Unlock()
		if message.More {
			_ = send.TrySend(Message{Kind: MsgMembersRequest, Offset: message.Offset + 128})
		}
		return nil
	case MsgSchemaRequest:
		m, err := c.manifest()
		if err != nil {
			return err
		}
		_ = send.TrySend(Message{Kind: MsgSchemaManifest, Signed: *m})
		return nil
	case MsgSchemaManifest:
		return c.adoptManifest(id, &message.Signed, send)
	case MsgSchemaAck:
		c.peersMu.Lock()
		if p, ok := c.peers[id]; ok {
			stamp := schemaStamp{digest: message.Digest, sequence: message.Sequence}
			p.schemaAcked = &stamp
		}
		c.peersMu.Unlock()
		return nil
	case MsgPing:
		_ = send.TrySend(Message{Kind: MsgPong, Nonce: message.Nonce})
		return nil
	case MsgPong:
		return nil
	case MsgError:
		if message.Code == 3 {
			if c.config.TrustedSnapshotSources[id] {
				c.requestSnapshotDamped(id, send)
			}
			return nil
		}
		// Record peer errors locally without replying: answering an Error
		// with another Error ping-pongs between nodes, prefixing
		// "peer {code}: " every round trip until messages are megabytes.
		text := fmt.Sprintf("peer %d: %s", message.Code, message.ErrMessage)
		if len(text) > 512 {
			text = text[:512]
		}
		c.recordError(unavailableErr("%s", text))
		return nil
	case MsgHello, MsgWelcome:
		return protocolErr("repeated handshake")
	default:
		return protocolErr("unknown message")
	}
}

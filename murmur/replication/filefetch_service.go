package replication

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var fileFetchALPN = []byte("murmur-sqlite/files/1")

type fileSource struct {
	node NodeId
	addr netip.AddrPort
}

// fetchService serves file objects over an independent QUIC endpoint;
// metadata is replicated by the ordinary mesh.
type fetchService struct {
	endpoint       quicListener
	objects        *FileObjects
	config         *Config
	database       NodeId
	ctx            context.Context
	activeMu       sync.Mutex
	active         map[[32]byte]bool
	permits        chan struct{}
	stagingPermits chan struct{}
}

func startFetchService(config *Config, backend Backend, objects *FileObjects, ctx context.Context) (*fetchService, error) {
	db, node := backend.Identity()
	fileConfig := config.Files
	endpoint, err := newEndpoint(config, fileConfig.FetchBind, node, fileFetchALPN)
	if err != nil {
		return nil, err
	}
	stagingUnits := int(min(divCeil(fileConfig.MaxStagingBytes, 1<<20), uint64(^uint32(0))))
	if stagingUnits < 1 {
		stagingUnits = 1
	}
	service := &fetchService{
		endpoint: endpoint, objects: objects, config: config, database: db, ctx: ctx,
		active:         make(map[[32]byte]bool),
		permits:        make(chan struct{}, fileConfig.ConcurrentFetches),
		stagingPermits: make(chan struct{}, stagingUnits),
	}
	go service.acceptLoop()
	return service, nil
}

func (s *fetchService) acceptLoop() {
	for {
		conn, err := s.endpoint.Accept(s.ctx)
		if err != nil {
			return
		}
		remote, rerr := netip.ParseAddrPort(conn.RemoteAddr().String())
		if rerr != nil || !NetworkAllowed(s.config, remote) {
			refuse(conn)
			continue
		}
		select {
		case s.permits <- struct{}{}:
		default:
			refuse(conn)
			continue
		}
		go func() {
			defer func() { <-s.permits }()
			ctx, cancel := context.WithTimeout(s.ctx, 300*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- s.serve(conn) }()
			select {
			case err := <-done:
				if err != nil {
					s.objects.fetchError(err)
				}
			case <-ctx.Done():
			}
		}()
	}
}

// lingerPeerClose waits for the peer to close a served file connection.
// The client closes once it holds the terminal frame; closing here would
// race (and truncate) its final reads, so shutdown is peer-driven with a
// bound for wedged peers.
func (s *fetchService) lingerPeerClose(conn quicConn) {
	select {
	case <-conn.Context().Done():
	case <-s.ctx.Done():
	case <-time.After(30 * time.Second):
	}
}

func (s *fetchService) serve(conn quicConn) error {
	defer conn.CloseWithError(0, "file serve complete")
	if _, err := peerID(conn, s.config); err != nil {
		return err
	}
	stream, err := conn.AcceptStream(s.ctx)
	if err != nil {
		return protocolErr("%v", err)
	}
	defer stream.Close()
	frame, err := ReadFileFrame(stream)
	if err != nil {
		return err
	}
	if frame.Kind != FileRequest {
		return protocolErr("file request")
	}
	if frame.Database != s.database {
		return authErr("file database identity")
	}
	path := s.objects.path(frame.Digest, frame.Generation)
	file, err := os.Open(path)
	if err != nil {
		if err := WriteFileFrame(stream, &FileFrame{Kind: FileMissing}); err != nil {
			return err
		}
		if err := stream.Close(); err != nil {
			return err
		}
		s.lingerPeerClose(conn)
		return nil
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	bytes := uint64(info.Size())
	if frame.Offset > bytes {
		return protocolErr("file resume offset")
	}
	var header [40]byte
	if _, err := file.ReadAt(header[:], 0); err != nil {
		return protocolErr("%v", err)
	}
	identity := sha256.Sum256(header[:])
	if err := WriteFileFrame(stream, &FileFrame{Kind: FileHeader, Bytes: bytes, Identity: identity}); err != nil {
		return err
	}
	if _, err := file.Seek(int64(frame.Offset), 0); err != nil {
		return err
	}
	chunk := make([]byte, 64<<10)
	for {
		remote, rerr := netip.ParseAddrPort(conn.RemoteAddr().String())
		if rerr != nil || !NetworkAllowed(s.config, remote) {
			return authErr("file peer migrated to denied network")
		}
		n, err := file.Read(chunk)
		if n > 0 {
			if err := WriteFileFrame(stream, &FileFrame{Kind: FileChunkFrame, Data: append([]byte(nil), chunk[:n]...)}); err != nil {
				return err
			}
		}
		if err != nil {
			break
		}
	}
	if err := WriteFileFrame(stream, &FileFrame{Kind: FileDone}); err != nil {
		return err
	}
	if err := stream.Close(); err != nil {
		return err
	}
	s.lingerPeerClose(conn)
	return nil
}

func (s *fetchService) fetchMissing(sources []fileSource) error {
	metadata, err := s.objects.List()
	if err != nil {
		return err
	}
	for _, meta := range metadata {
		meta := meta
		available, err := s.objects.Available(meta.Name)
		if err != nil {
			return err
		}
		s.activeMu.Lock()
		dup := available || s.active[meta.Digest]
		s.activeMu.Unlock()
		if dup {
			continue
		}
		select {
		case s.permits <- struct{}{}:
		default:
			// Permits exhausted: stop this round and retry on the next
			// tick. Every spawned fetch unconditionally releases one
			// permit on exit, so falling through here would spawn
			// unthrottled and corrupt the accounting (a bare break
			// would only exit the select).
			return nil
		}
		container := satAdd(meta.Bytes, meta.Bytes/ChunkBytes*20+256)
		if container < meta.Bytes {
			<-s.permits
			s.objects.fetchError(ErrOverloaded)
			continue
		}
		maximum := s.config.Files.MaxStagingBytes
		if container > maximum {
			<-s.permits
			s.objects.fetchError(ErrOverloaded)
			continue
		}
		units := int(min(divCeil(max(container, 1), 1<<20), uint64(^uint32(0))))
		acquired := true
		for i := 0; i < units; i++ {
			select {
			case s.stagingPermits <- struct{}{}:
			default:
				for j := 0; j < i; j++ {
					<-s.stagingPermits
				}
				acquired = false
			}
			if !acquired {
				break
			}
		}
		if !acquired {
			<-s.permits
			continue
		}
		s.activeMu.Lock()
		s.active[meta.Digest] = true
		s.activeMu.Unlock()
		go func() {
			defer func() { <-s.permits }()
			defer func() {
				for i := 0; i < units; i++ {
					<-s.stagingPermits
				}
			}()
			defer func() {
				s.activeMu.Lock()
				delete(s.active, meta.Digest)
				s.activeMu.Unlock()
			}()
			reservation, err := s.objects.ReserveStaging(container)
			if err != nil {
				s.objects.fetchError(err)
				return
			}
			defer reservation.Release()
			for _, source := range sources {
				ctx, cancel := context.WithTimeout(s.ctx, 300*time.Second)
				err := s.fetch(ctx, source, &meta)
				cancel()
				if err == nil {
					break
				}
				if ctx.Err() == context.DeadlineExceeded {
					s.objects.fetchError(unavailableErr("file transfer timeout"))
				} else {
					s.objects.fetchError(err)
				}
			}
		}()
	}
	return nil
}

func (s *fetchService) fetch(ctx context.Context, source fileSource, meta *FileMetadata) error {
	if !NetworkAllowed(s.config, source.addr) {
		return authErr("file source network")
	}
	maximum := s.config.Files.MaxStagingBytes
	containerLimit := satAdd(meta.Bytes, meta.Bytes/ChunkBytes*20+256)
	if containerLimit < meta.Bytes {
		return ErrOverloaded
	}
	if containerLimit > maximum {
		return ErrOverloaded
	}
	staging := filepath.Join(s.config.Files.Directory, "staging")
	if err := os.MkdirAll(staging, 0o700); err != nil {
		return err
	}
	path := filepath.Join(staging, fmt.Sprintf("%s-%s.g%d.part", Hex(source.node[:]), Hex(meta.Digest[:]), meta.Generation))
	guard := s.objects.ProtectStaging(path)
	defer guard.Release()
	offset, err := fileResumeOffset(path)
	if err != nil {
		return err
	}
	conn, err := s.endpoint.Dial(ctx, source.addr)
	if err != nil {
		return protocolErr("%v", err)
	}
	defer conn.CloseWithError(0, "file transfer complete")
	id, err := peerID(conn, s.config)
	if err != nil {
		return err
	}
	if id != source.node {
		return authErr("file source identity mismatch")
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return protocolErr("%v", err)
	}
	defer stream.Close()
	if err := WriteFileFrame(stream, &FileFrame{Kind: FileRequest, Database: s.database, Digest: meta.Digest, Generation: meta.Generation, Offset: offset}); err != nil {
		return err
	}
	frame, err := ReadFileFrame(stream)
	if err != nil {
		return err
	}
	var total uint64
	var identity [32]byte
	switch frame.Kind {
	case FileHeader:
		identity = frame.Identity
	case FileMissing:
		if err := s.objects.recordPeerAvailability(meta.Digest, meta.Generation, source.node, false); err != nil {
			return err
		}
		return unavailableErr("file source has no object")
	default:
		return protocolErr("file response header")
	}
	bytes := frame.Bytes
	if bytes > containerLimit || bytes > maximum || offset > bytes {
		return ErrOverloaded
	}
	identityPath := stagingIdentityPath(path)
	if offset > 0 {
		old, err := os.ReadFile(identityPath)
		if err != nil {
			return err
		}
		if string(old) != string(identity[:]) {
			os.Remove(path)
			return unavailableErr("source container changed; restart transfer")
		}
	} else if err := os.WriteFile(identityPath, identity[:], 0o600); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	total = offset
	complete := false
	for {
		frame, err := ReadFileFrame(stream)
		if err != nil {
			file.Close()
			return err
		}
		switch frame.Kind {
		case FileChunkFrame:
			if len(frame.Data) > 64<<10 {
				file.Close()
				return protocolErr("file chunk bounds")
			}
			total += uint64(len(frame.Data))
			if total > bytes {
				file.Close()
				return protocolErr("file container length")
			}
			if _, err := file.Write(frame.Data); err != nil {
				file.Close()
				return err
			}
		case FileDone:
			complete = true
		default:
			file.Close()
			return protocolErr("file transfer frame")
		}
		if complete {
			break
		}
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	file.Close()
	if total != bytes {
		return unavailableErr("incomplete file container")
	}
	if err := publishStagedFile(s.objects, path, identityPath, meta); err != nil {
		return err
	}
	if err := s.objects.recordPeerAvailability(meta.Digest, meta.Generation, source.node, true); err != nil {
		return err
	}
	os.Remove(identityPath)
	return nil
}

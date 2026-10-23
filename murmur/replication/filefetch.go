package replication

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/postcard"
)

// Independent file-fetch endpoint; metadata is replicated by the
// ordinary mesh. The QUIC service is wired with the engine transport.

// FileFrameKind classifies a file-transfer frame.
type FileFrameKind int

const (
	FileRequest FileFrameKind = iota
	FileHeader
	FileChunkFrame
	FileDone
	FileMissing
)

// FileFrame is one file-transfer frame.
type FileFrame struct {
	Kind       FileFrameKind
	Database   NodeId
	Digest     [32]byte
	Generation uint64
	Offset     uint64
	Bytes      uint64
	Identity   [32]byte
	Data       []byte
}

func encodeFileFrame(e *postcard.Encoder, f *FileFrame) {
	e.Discriminant(uint32(f.Kind))
	switch f.Kind {
	case FileRequest:
		e.Raw(f.Database[:])
		e.Raw(f.Digest[:])
		e.U64(f.Generation)
		e.U64(f.Offset)
	case FileHeader:
		e.U64(f.Bytes)
		e.Raw(f.Identity[:])
	case FileChunkFrame:
		e.ByteString(f.Data)
	case FileDone, FileMissing:
	}
}

func decodeFileFrame(d *postcard.Decoder) FileFrame {
	var f FileFrame
	switch d.Discriminant() {
	case 0:
		f.Kind = FileRequest
	case 1:
		f.Kind = FileHeader
	case 2:
		f.Kind = FileChunkFrame
	case 3:
		f.Kind = FileDone
	case 4:
		f.Kind = FileMissing
	default:
		d.Fail(protocolErr("bad file frame"))
		return f
	}
	switch f.Kind {
	case FileRequest:
		copy(f.Database[:], d.Raw(16))
		copy(f.Digest[:], d.Raw(32))
		f.Generation = d.U64()
		f.Offset = d.U64()
	case FileHeader:
		f.Bytes = d.U64()
		copy(f.Identity[:], d.Raw(32))
	case FileChunkFrame:
		f.Data = append([]byte(nil), d.Bytes()...)
	case FileDone, FileMissing:
	}
	return f
}

// EncodeFileFrame serializes a frame.
func EncodeFileFrame(f *FileFrame) []byte {
	return encode(func(e *postcard.Encoder) { encodeFileFrame(e, f) })
}

// DecodeFileFrame parses a frame.
func DecodeFileFrame(b []byte) (FileFrame, error) { return decodeOne(b, decodeFileFrame) }

// WriteFileFrame sends one frame.
func WriteFileFrame(w io.Writer, f *FileFrame) error {
	data := EncodeFileFrame(f)
	if len(data) > 128<<10 {
		return ErrOverloaded
	}
	var header [12]byte
	copy(header[:2], "FF")
	binary.BigEndian.PutUint16(header[2:4], 1)
	binary.BigEndian.PutUint32(header[8:], uint32(len(data)))
	if _, err := w.Write(header[:]); err != nil {
		return protocolErr("%v", err)
	}
	if _, err := w.Write(data); err != nil {
		return protocolErr("%v", err)
	}
	return nil
}

// ReadFileFrame reads one frame.
func ReadFileFrame(r io.Reader) (FileFrame, error) {
	var f FileFrame
	var header [12]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return f, protocolErr("%v", err)
	}
	n := int(binary.BigEndian.Uint32(header[8:]))
	if string(header[:2]) != "FF" || binary.BigEndian.Uint16(header[2:4]) != 1 || n > 128<<10 {
		return f, protocolErr("file frame")
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return f, protocolErr("%v", err)
	}
	return DecodeFileFrame(b)
}

// publishStagedFile publishes only an authenticated staged container,
// discarding poisoned bytes and their resume identity.
func publishStagedFile(objects *FileObjects, path, identity string, meta *FileMetadata) error {
	if err := objects.PublishFetched(path, meta); err != nil {
		// Authentication failure must not leave a complete poisoned container
		// that every subsequent request attempts to resume at its end.
		_ = os.Remove(path)
		_ = os.Remove(identity)
		return err
	}
	return nil
}

// stagingIdentityPath locates the resume-identity sidecar for a staged
// partial. The sidecar replaces the staging extension (with_extension
// semantics): "X.g0.part" -> "X.g0.identity". Fetch, resume, publish,
// and staging protection must all agree here; a mismatch silently
// disables resume and strands sidecars.
func stagingIdentityPath(path string) string {
	if ext := filepath.Ext(path); ext != "" {
		return path[:len(path)-len(ext)] + ".identity"
	}
	return path + ".identity"
}

// A crash can publish a partial container without its identity sidecar. Such
// bytes cannot be safely resumed; discard them and request a fresh container.
func fileResumeOffset(path string) (uint64, error) {
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, ioErr(err)
	}
	offset := uint64(st.Size())
	if offset > 0 {
		identity := stagingIdentityPath(path)
		ist, err := os.Stat(identity)
		size := int64(-1)
		if err == nil {
			size = ist.Size()
		} else if !os.IsNotExist(err) {
			return 0, ioErr(err)
		}
		if size != 32 {
			if err := os.Remove(path); err != nil {
				return 0, ioErr(err)
			}
			return 0, nil
		}
	}
	return offset, nil
}

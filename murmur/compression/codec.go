// Package compression defines the compression hook shared by Spool blocks,
// replication snapshot frames and bridge bundles, plus the dependency-free
// built-in codecs (none and stdlib deflate).
//
// Wire ids:
//
//	0        none (passthrough)
//	1, 2     retired; formerly zstd-fast / zstd-default. Never reused.
//	3        deflate (compress/flate, default level 1)
//	4..127   reserved for future built-ins
//	128..255 user codecs supplied through the hook
package compression

import (
	"bytes"
	"compress/flate"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Built-in and range constants.
const (
	IDNone    uint8 = 0
	IDDeflate uint8 = 3

	// MinUserID is the lowest id a user codec may use.
	MinUserID uint8 = 128
)

// ErrRetired is returned for retired ids (1 and 2, formerly zstd).
var ErrRetired = errors.New("compression: retired codec id (zstd removed)")

// ErrUnknown is returned when an id is not registered.
var ErrUnknown = errors.New("compression: unknown codec id")

// ErrTooLarge is returned when decompressed output exceeds the bound.
var ErrTooLarge = errors.New("compression: decompressed size exceeds limit")

// Codec is a block compressor. Implementations must be safe for
// concurrent use.
type Codec interface {
	// ID is the on-wire identifier (see the package doc for ranges).
	ID() uint8
	// Name is a human-readable name used in status and errors.
	Name() string
	// Compress appends the compressed form of src to dst and returns it.
	// It must not retain or modify src.
	Compress(dst, src []byte) ([]byte, error)
	// Decompress appends the plain form of src to dst and returns it. It
	// must fail (ideally with ErrTooLarge) rather than produce more than
	// maxPlain bytes, so hostile input cannot trigger huge allocations.
	Decompress(dst, src []byte, maxPlain int) ([]byte, error)
}

// None is the passthrough codec (id 0).
var None Codec = noneCodec{}

type noneCodec struct{}

func (noneCodec) ID() uint8    { return IDNone }
func (noneCodec) Name() string { return "none" }
func (noneCodec) Compress(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}
func (noneCodec) Decompress(dst, src []byte, maxPlain int) ([]byte, error) {
	if len(src) > maxPlain {
		return nil, ErrTooLarge
	}
	return append(dst, src...), nil
}

// Deflate is the default compression: stdlib compress/flate at level 1
// (fastest). Use NewDeflate for a different level.
var Deflate Codec = mustDeflate(flate.BestSpeed)

type deflateCodec struct {
	level int
	wpool sync.Pool // *flate.Writer
	rpool sync.Pool // io.ReadCloser implementing flate.Resetter
}

// NewDeflate returns a deflate codec at the given level (flate.HuffmanOnly
// through flate.BestCompression).
func NewDeflate(level int) (Codec, error) {
	if level < flate.HuffmanOnly || level > flate.BestCompression {
		return nil, fmt.Errorf("compression: invalid deflate level %d", level)
	}
	return &deflateCodec{level: level}, nil
}

func mustDeflate(level int) Codec {
	c, err := NewDeflate(level)
	if err != nil {
		panic(err)
	}
	return c
}

func (*deflateCodec) ID() uint8    { return IDDeflate }
func (*deflateCodec) Name() string { return "deflate" }

func (c *deflateCodec) Compress(dst, src []byte) ([]byte, error) {
	buf := bytes.NewBuffer(dst)
	w, _ := c.wpool.Get().(*flate.Writer)
	if w == nil {
		var err error
		if w, err = flate.NewWriter(buf, c.level); err != nil {
			return nil, err
		}
	} else {
		w.Reset(buf)
	}
	if _, err := w.Write(src); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	c.wpool.Put(w)
	return buf.Bytes(), nil
}

func (c *deflateCodec) Decompress(dst, src []byte, maxPlain int) ([]byte, error) {
	r, _ := c.rpool.Get().(io.ReadCloser)
	if r == nil {
		r = flate.NewReader(bytes.NewReader(src))
	} else if err := r.(flate.Resetter).Reset(bytes.NewReader(src), nil); err != nil {
		return nil, err
	}
	defer func() { c.rpool.Put(r) }()
	buf := bytes.NewBuffer(dst)
	n, err := io.Copy(buf, io.LimitReader(r, int64(maxPlain)+1))
	if err != nil {
		return nil, fmt.Errorf("compression: deflate: %w", err)
	}
	if n > int64(maxPlain) {
		return nil, ErrTooLarge
	}
	return buf.Bytes(), nil
}

// Registry resolves wire ids to codecs. The zero value is not usable;
// build one with NewRegistry. A Registry is immutable and safe for
// concurrent use.
type Registry struct {
	byID map[uint8]Codec
}

// NewRegistry returns a registry holding None, Deflate and the given user
// codecs. User codecs must use ids in 128..255 and be unique.
func NewRegistry(user ...Codec) (*Registry, error) {
	r := &Registry{byID: map[uint8]Codec{IDNone: None, IDDeflate: Deflate}}
	for _, c := range user {
		if c == nil {
			return nil, errors.New("compression: nil codec")
		}
		id := c.ID()
		if id < MinUserID {
			return nil, fmt.Errorf("compression: %q uses id %d; user codecs must use %d-255", c.Name(), id, MinUserID)
		}
		if prev, dup := r.byID[id]; dup {
			return nil, fmt.Errorf("compression: id %d registered twice (%q, %q)", id, prev.Name(), c.Name())
		}
		r.byID[id] = c
	}
	return r, nil
}

// Get returns the codec for id, ErrRetired for ids 1 and 2, or ErrUnknown.
func (r *Registry) Get(id uint8) (Codec, error) {
	if r == nil || r.byID == nil {
		switch id {
		case IDNone:
			return None, nil
		case IDDeflate:
			return Deflate, nil
		case 1, 2:
			return nil, ErrRetired
		default:
			return nil, fmt.Errorf("%w: %d", ErrUnknown, id)
		}
	}
	if c, ok := r.byID[id]; ok {
		return c, nil
	}
	if id == 1 || id == 2 {
		return nil, ErrRetired
	}
	return nil, fmt.Errorf("%w: %d", ErrUnknown, id)
}

// IDs returns the registered ids (unordered).
func (r *Registry) IDs() []uint8 {
	out := make([]uint8, 0, len(r.byID))
	for id := range r.byID {
		out = append(out, id)
	}
	return out
}

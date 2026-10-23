package spool

import (
	"errors"
	"fmt"
	"sync"

	"github.com/marcgauthier/murmur-sql/murmur/compression"
)

// compressor dispatches block compression through the codec registry.
// None passes bytes through. The Compression value equals the wire
// codec id, so a user codec is selected as Compression(c.ID()).
type compressor struct {
	mode Compression
	reg  *compression.Registry
	// codecs are safe for concurrent use; the pool only recycles
	// output buffers.
	pool sync.Pool
}

// newCompressor builds the registry (built-ins plus user codecs).
// Decoding is always available for every registered codec; mode only
// selects the codec used for new blocks.
func newCompressor(mode Compression, user []compression.Codec) (*compressor, error) {
	reg, err := compression.NewRegistry(user...)
	if err != nil {
		return nil, fmt.Errorf("spool: %w", err)
	}
	c := &compressor{mode: mode, reg: reg}
	c.pool.New = func() any { return make([]byte, 0, 64<<10) }
	if mode != CompressionNone {
		if _, err := reg.Get(uint8(mode)); err != nil {
			return nil, fmt.Errorf("spool: compression %d: %w", int(mode), err)
		}
	}
	return c, nil
}

// compress returns the encoded body. The input must not be modified
// afterwards when mode is None (bytes pass through by reference).
func (c *compressor) compress(body []byte) ([]byte, error) {
	return c.compressWith(c.mode, body)
}

// compressWith encodes under an explicit mode so maintenance
// re-sealing preserves each block's original codec (and therefore
// its exact length, which index offsets depend on).
func (c *compressor) compressWith(mode Compression, body []byte) ([]byte, error) {
	if mode == CompressionNone {
		return body, nil
	}
	cd, err := c.reg.Get(uint8(mode))
	if err != nil {
		return nil, fmt.Errorf("spool: compression %d: %w", int(mode), err)
	}
	out, err := cd.Compress(nil, body)
	if err != nil {
		return nil, fmt.Errorf("spool: %s compress: %w", cd.Name(), err)
	}
	return out, nil
}

// modeForCompressionID maps a wire codec id to its mode. Retired ids
// (1, 2: formerly zstd) are rejected as corruption.
func modeForCompressionID(id uint8) (Compression, error) {
	if id == 1 || id == 2 {
		return CompressionNone, fmt.Errorf("spool: %w: %w", compression.ErrRetired, ErrCorrupt)
	}
	return Compression(id), nil
}

// decompress decodes one block body, enforcing the exact expected
// plain size so corrupt length fields cannot trigger huge
// allocations.
func (c *compressor) decompress(mode uint8, src []byte, plainLen uint32) ([]byte, error) {
	if mode == 0 {
		if uint64(len(src)) != uint64(plainLen) {
			return nil, fmt.Errorf("spool: plain size %d != %d: %w", len(src), plainLen, ErrCorrupt)
		}
		return src, nil
	}
	cd, err := c.reg.Get(mode)
	if err != nil {
		if errors.Is(err, compression.ErrRetired) || errors.Is(err, compression.ErrUnknown) {
			return nil, fmt.Errorf("spool: block codec id %d: %w: %w", mode, err, ErrCorrupt)
		}
		return nil, err
	}
	out, err := cd.Decompress(make([]byte, 0, plainLen), src, int(plainLen))
	if err != nil {
		return nil, fmt.Errorf("spool: %s decode: %w", cd.Name(), err)
	}
	if uint64(len(out)) != uint64(plainLen) {
		return nil, fmt.Errorf("spool: decoded size %d != %d: %w", len(out), plainLen, ErrCorrupt)
	}
	return out, nil
}

// compressionID maps the configured mode to its wire id.
func compressionID(m Compression) uint8 { return uint8(m) }

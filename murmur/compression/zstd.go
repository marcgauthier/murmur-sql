package compression

import (
	"fmt"

	ddzstd "github.com/DataDog/zstd"
)

// IDZstd is the user-codec id for murmur zstd blocks, matching Rust.
const IDZstd uint8 = 128

// ZstdName is the codec name, matching Rust.
const ZstdName = "sqlite-session-zstd-v1"

// Zstd is a block codec using zstd frames, interoperable with the Rust
// backend's sqlite-session-zstd-v1 codec (id 128). It drives the Meta
// zstd C library vendored in third_party/zstd through the DataDog Go
// driver. Use NewZstd to build one.
type Zstd struct {
	level int
}

// NewZstd returns a zstd codec at the given zstd level (as in Rust
// Options.compression_level; 3 is the default).
func NewZstd(level int) (*Zstd, error) {
	return &Zstd{level: level}, nil
}

// MustZstd returns a zstd codec, panicking on setup failure.
func MustZstd(level int) *Zstd {
	c, err := NewZstd(level)
	if err != nil {
		panic(err)
	}
	return c
}

func (*Zstd) ID() uint8    { return IDZstd }
func (*Zstd) Name() string { return ZstdName }

// Compress appends the zstd frame of src to dst.
func (c *Zstd) Compress(dst, src []byte) ([]byte, error) {
	frame, err := ddzstd.CompressLevel(nil, src, c.level)
	if err != nil {
		return nil, fmt.Errorf("compression: zstd: %w", err)
	}
	return append(dst, frame...), nil
}

// Decompress appends the plain form of src to dst, failing rather than
// producing more than maxPlain bytes. One-shot C decoding rejects
// truncated frames and decodes consecutive frames (as written by
// streaming encoders) in sequence.
func (c *Zstd) Decompress(dst, src []byte, maxPlain int) ([]byte, error) {
	if len(src) == 0 {
		return nil, fmt.Errorf("compression: zstd: empty frame")
	}
	if maxPlain < 0 {
		return nil, ErrTooLarge
	}
	// One slot past the bound distinguishes overflow from exact fit.
	plain := make([]byte, maxPlain+1)
	n, err := ddzstd.DecompressInto(plain, src)
	if err != nil {
		return nil, fmt.Errorf("compression: zstd: %w", err)
	}
	if n > maxPlain {
		return nil, ErrTooLarge
	}
	return append(dst, plain[:n]...), nil
}

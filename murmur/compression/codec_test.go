package compression

import (
	"bytes"
	"errors"
	"testing"
)

// flipCodec is a trivial custom codec (bitwise NOT) used to exercise the hook.
type flipCodec struct{ id uint8 }

func (f flipCodec) ID() uint8    { return f.id }
func (f flipCodec) Name() string { return "flip" }
func (f flipCodec) Compress(dst, src []byte) ([]byte, error) {
	for _, b := range src {
		dst = append(dst, ^b)
	}
	return dst, nil
}
func (f flipCodec) Decompress(dst, src []byte, maxPlain int) ([]byte, error) {
	if len(src) > maxPlain {
		return nil, ErrTooLarge
	}
	for _, b := range src {
		dst = append(dst, ^b)
	}
	return dst, nil
}

func TestDeflateRoundTripAndBound(t *testing.T) {
	plain := bytes.Repeat([]byte("murmur "), 10_000)
	c, err := Deflate.Compress(nil, plain)
	if err != nil {
		t.Fatal(err)
	}
	if len(c) >= len(plain)/4 {
		t.Fatalf("deflate did not compress: %d -> %d", len(plain), len(c))
	}
	got, err := Deflate.Decompress(nil, c, len(plain))
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("round trip failed: %v", err)
	}
	// Decompression bomb: tight bound must fail, not allocate.
	if _, err := Deflate.Decompress(nil, c, len(plain)-1); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("want ErrTooLarge, got %v", err)
	}
	// Garbage input fails.
	if _, err := Deflate.Decompress(nil, []byte("not deflate data \xff\xff"), 1<<20); err == nil {
		t.Fatal("garbage accepted")
	}
	// Pooled readers/writers stay correct across reuse.
	for i := 0; i < 5; i++ {
		c2, _ := Deflate.Compress(nil, plain[:1000+i])
		g2, err := Deflate.Decompress(nil, c2, 1<<20)
		if err != nil || !bytes.Equal(g2, plain[:1000+i]) {
			t.Fatalf("reuse %d failed: %v", i, err)
		}
	}
}

func TestDeflateLevels(t *testing.T) {
	if _, err := NewDeflate(99); err == nil {
		t.Fatal("invalid level accepted")
	}
	c, err := NewDeflate(9)
	if err != nil {
		t.Fatal(err)
	}
	plain := bytes.Repeat([]byte("abcd"), 5000)
	out, _ := c.Compress(nil, plain)
	back, err := Deflate.Decompress(nil, out, len(plain)) // same wire format
	if err != nil || !bytes.Equal(back, plain) {
		t.Fatalf("cross-level decode failed: %v", err)
	}
}

func TestRegistry(t *testing.T) {
	r, err := NewRegistry(flipCodec{200})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint8{IDNone, IDDeflate, 200} {
		if _, err := r.Get(id); err != nil {
			t.Fatalf("id %d: %v", id, err)
		}
	}
	for _, id := range []uint8{1, 2} {
		if _, err := r.Get(id); !errors.Is(err, ErrRetired) {
			t.Fatalf("id %d: want ErrRetired, got %v", id, err)
		}
	}
	if _, err := r.Get(77); !errors.Is(err, ErrUnknown) {
		t.Fatalf("want ErrUnknown, got %v", err)
	}
	// Id range and duplicate enforcement.
	if _, err := NewRegistry(flipCodec{3}); err == nil {
		t.Fatal("user codec in built-in range accepted")
	}
	if _, err := NewRegistry(flipCodec{1}); err == nil {
		t.Fatal("user codec on retired id accepted")
	}
	if _, err := NewRegistry(flipCodec{130}, flipCodec{130}); err == nil {
		t.Fatal("duplicate id accepted")
	}
	if _, err := NewRegistry(nil); err == nil {
		t.Fatal("nil codec accepted")
	}
}

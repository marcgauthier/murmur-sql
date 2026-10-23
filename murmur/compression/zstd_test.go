package compression

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestZstdBounded(t *testing.T) {
	codec := MustZstd(3)
	if codec.ID() != 128 || codec.Name() != "sqlite-session-zstd-v1" {
		t.Fatalf("id/name = %d/%q", codec.ID(), codec.Name())
	}
	for _, ln := range []int{0, 1, 64 << 10, 2 << 20} {
		plain := make([]byte, ln)
		for n := range plain {
			plain[n] = byte(n % 251)
		}
		frame, err := codec.Compress(nil, plain)
		if err != nil {
			t.Fatal(err)
		}
		back, err := codec.Decompress(nil, frame, ln)
		if err != nil || !bytes.Equal(back, plain) {
			t.Fatalf("len %d roundtrip: %v", ln, err)
		}
		if ln > 0 {
			if _, err := codec.Decompress(nil, frame, ln-1); err == nil {
				t.Fatalf("len %d short bound accepted", ln)
			}
		}
		if ln == 0 && len(frame) == 0 {
			t.Fatal("empty input produced no frame")
		}
		if _, err := codec.Decompress(nil, frame[:len(frame)-1], ln); err == nil {
			t.Fatalf("len %d truncated frame accepted", ln)
		}
	}
	// Streaming encoders may write consecutive frames; old stores remain readable.
	first, err := codec.Compress(nil, []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := codec.Compress(nil, []byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	frames := append(first, second...)
	back, err := codec.Decompress(nil, frames, 11)
	if err != nil || string(back) != "firstsecond" {
		t.Fatalf("concat = %q, %v", back, err)
	}
	if _, err := codec.Decompress(nil, frames, 10); err == nil {
		t.Fatal("concat short bound accepted")
	}
	junk := bytes.Repeat([]byte{0xff}, 16)
	if _, err := codec.Decompress(nil, junk, 64<<10); err == nil {
		t.Fatal("junk accepted")
	}
}

func TestZstdCrossReadsRustFrame(t *testing.T) {
	// Frame produced by Rust zstd 0.13 stream encoder, level 3, for
	// "murmur-zstd-interop" (scratch cargo probe, same crate MURMUR uses).
	frame, err := hex.DecodeString("28b52ffd00589900006d75726d75722d7a7374642d696e7465726f70")
	if err != nil {
		t.Fatal(err)
	}
	back, err := MustZstd(3).Decompress(nil, frame, 64)
	if err != nil || string(back) != "murmur-zstd-interop" {
		t.Fatalf("rust frame = %q, %v", back, err)
	}
}

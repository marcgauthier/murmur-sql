package spool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/compression"
)

// TestCoverageStringFormatters tests string representations of enums across
// Durability, Compression, Encryption, and SegmentState including unknown/boundary values.
func TestCoverageStringFormatters(t *testing.T) {
	// Durability
	durs := []struct {
		d Durability
		s string
	}{
		{DurabilityAsync, "async"},
		{DurabilityFlush, "flush"},
		{DurabilitySync, "sync"},
		{Durability(999), "unknown(999)"},
	}
	for _, tc := range durs {
		if got := tc.d.String(); got != tc.s {
			t.Errorf("Durability(%d).String() = %q, want %q", tc.d, got, tc.s)
		}
	}

	// Compression
	comps := []struct {
		c Compression
		s string
	}{
		{CompressionNone, "none"},
		{CompressionDeflate, "deflate"},
		{Compression(150), "custom(150)"},
		{Compression(999), "unknown(999)"},
	}
	for _, tc := range comps {
		if got := tc.c.String(); got != tc.s {
			t.Errorf("Compression(%d).String() = %q, want %q", tc.c, got, tc.s)
		}
	}

	// Encryption
	encs := []struct {
		e Encryption
		s string
	}{
		{EncryptionDefault, "default(aes-256-gcm)"},
		{EncryptionNone, "none"},
		{EncryptionAES256GCM, "aes-256-gcm"},
		{Encryption(999), "unknown(999)"},
	}
	for _, tc := range encs {
		if got := tc.e.String(); got != tc.s {
			t.Errorf("Encryption(%d).String() = %q, want %q", tc.e, got, tc.s)
		}
	}

	// SegmentState
	states := []struct {
		st SegmentState
		s  string
	}{
		{SegmentActive, "active"},
		{SegmentSealed, "sealed"},
		{SegmentCompacting, "compacting"},
		{SegmentObsolete, "obsolete"},
		{SegmentState(99), "unknown"},
	}
	for _, tc := range states {
		if got := tc.st.String(); got != tc.s {
			t.Errorf("SegmentState(%d).String() = %q, want %q", tc.st, got, tc.s)
		}
	}
}

// TestCoverageFlushAndReclaimErrorAccessors tests LastFlushErr and LastReclaimErr
// recording and retrieval paths.
func TestCoverageFlushAndReclaimErrorAccessors(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Path:      dir,
		MasterKey: make([]byte, 32),
	}
	st, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	if err := st.LastFlushErr(); err != nil {
		t.Errorf("initial LastFlushErr = %v, want nil", err)
	}
	if err := st.LastReclaimErr(); err != nil {
		t.Errorf("initial LastReclaimErr = %v, want nil", err)
	}

	testErr1 := fmt.Errorf("simulated flush error")
	st.recordFlushErr(testErr1)
	if !errors.Is(st.LastFlushErr(), testErr1) {
		t.Errorf("LastFlushErr = %v, want %v", st.LastFlushErr(), testErr1)
	}

	testErr2 := fmt.Errorf("simulated reclaim error")
	st.recordReclaimErr(testErr2)
	if !errors.Is(st.LastReclaimErr(), testErr2) {
		t.Errorf("LastReclaimErr = %v, want %v", st.LastReclaimErr(), testErr2)
	}
}

// TestCoverageListSegments tests segment file enumeration and name parsing.
func TestCoverageListSegments(t *testing.T) {
	dir := t.TempDir()

	// Empty dir
	ids, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments empty: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected 0 ids, got %v", ids)
	}

	// Write segments out of order plus extra non-segment files and a subdirectory
	files := []string{
		segmentFileName(5),
		segmentFileName(1),
		segmentFileName(12),
		"not-a-segment.txt",
		"000000000000.spool", // id 0 is invalid
		"badname.spool",
	}
	for _, f := range files {
		_ = os.WriteFile(filepath.Join(dir, f), []byte("test"), 0o600)
	}
	_ = os.Mkdir(filepath.Join(dir, "subfolder"), 0o700)

	ids, err = listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 5 || ids[2] != 12 {
		t.Fatalf("listSegments = %v, want [1 5 12]", ids)
	}

	// Non-existent directory
	_, err = listSegments(filepath.Join(dir, "does-not-exist"))
	if err == nil {
		t.Fatal("expected error on non-existent directory, got nil")
	}
}

// TestCoverageCompressionEdgeCases tests compressor methods, retired codecs,
// and custom user codecs.
func TestCoverageCompressionEdgeCases(t *testing.T) {
	// Mode none
	cNone, err := newCompressor(CompressionNone, nil)
	if err != nil {
		t.Fatalf("newCompressor None: %v", err)
	}
	input := []byte("hello uncompressed world")
	compressed, err := cNone.compress(input)
	if err != nil {
		t.Fatalf("compress None: %v", err)
	}
	if string(compressed) != string(input) {
		t.Errorf("compress None modified input")
	}

	// Decompress with wrong plainLen under mode 0
	_, err = cNone.decompress(0, compressed, uint32(len(compressed)+5))
	if err == nil {
		t.Fatal("expected decompress error on plainLen mismatch, got nil")
	}

	// Mode deflate
	cDeflate, err := newCompressor(CompressionDeflate, nil)
	if err != nil {
		t.Fatalf("newCompressor Deflate: %v", err)
	}
	compDeflate, err := cDeflate.compress(input)
	if err != nil {
		t.Fatalf("compress Deflate: %v", err)
	}
	decomp, err := cDeflate.decompress(uint8(CompressionDeflate), compDeflate, uint32(len(input)))
	if err != nil {
		t.Fatalf("decompress Deflate: %v", err)
	}
	if string(decomp) != string(input) {
		t.Errorf("decompressed != input: got %q", string(decomp))
	}

	// Retired codec IDs (1, 2)
	for _, id := range []uint8{1, 2} {
		_, err := modeForCompressionID(id)
		if err == nil || !errors.Is(err, ErrCorrupt) || !errors.Is(err, compression.ErrRetired) {
			t.Errorf("modeForCompressionID(%d) = %v, want ErrRetired and ErrCorrupt", id, err)
		}
	}

	// Invalid compression mode
	_, err = newCompressor(Compression(250), nil)
	if err == nil {
		t.Fatal("expected error for unregistered compression mode, got nil")
	}
}

// TestCoverageOptionsValidationEdgeCases covers invalid option boundary checks.
func TestCoverageOptionsValidationEdgeCases(t *testing.T) {
	valid := DefaultOptions(t.TempDir())
	valid.MasterKey = make([]byte, 32)

	tests := []struct {
		name string
		mod  func(o *Options)
	}{
		{"empty path", func(o *Options) { o.Path = "" }},
		{"invalid key length", func(o *Options) { o.MasterKey = []byte("short") }},
		{"both master key and passphrase", func(o *Options) { o.Passphrase = "pass" }},
		{"bad compaction threshold low", func(o *Options) { o.CompactionThreshold = -0.1 }},
		{"bad compaction threshold high", func(o *Options) { o.CompactionThreshold = 1.1 }},
		{"bad shards non-power-of-2", func(o *Options) { o.IndexShards = 3 }},
		{"oversized key+val for block", func(o *Options) {
			o.MaxKeySize = 3000
			o.MaxValueSize = 3000
			o.MaxBlockBytes = 4000
		}},
		{"negative target block bytes", func(o *Options) { o.TargetBlockBytes = -1 }},
		{"negative data key max age", func(o *Options) { o.DataKeyMaxAge = -1 * time.Second }},
		{"negative compaction min free bytes", func(o *Options) { o.CompactionMinFreeBytes = -1 }},
		{"negative tomb proof threshold", func(o *Options) { o.TombProofThreshold = -1 }},
		{"negative workers", func(o *Options) { o.Workers = -1 }},
		{"invalid context id length", func(o *Options) { o.ContextID = []byte("short") }},
		{"write shards non-pow2", func(o *Options) { o.WriteShards = 5 }},
		{"unknown durability", func(o *Options) { o.Durability = Durability(99) }},
		{"encryption none with master key", func(o *Options) {
			o.Encryption = EncryptionNone
			o.MasterKey = make([]byte, 32)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := valid
			tc.mod(&opts)
			_, err := Open(opts)
			if err == nil {
				t.Fatalf("expected error for invalid option %q, got nil", tc.name)
			}
		})
	}
}

// TestCoverageSegmentWriterHelpers exercises auxiliary helpers in segment.go.
func TestCoverageSegmentWriterHelpers(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Path:             dir,
		MasterKey:        make([]byte, 32),
		MaxKeySize:       128,
		MaxValueSize:     512,
		MaxSegmentSize:   8192,
		TargetBlockBytes: 1024,
		MaxBlockBytes:    4096,
	}

	st, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Verify activeID and endOffset
	activeID := st.seg.activeID()
	if activeID == 0 {
		t.Fatalf("expected non-zero activeID, got 0")
	}

	initOffset := st.seg.endOffset()
	_ = st.Put([]byte("helper-key"), []byte("val"))
	_ = st.Flush()

	newOffset := st.seg.endOffset()
	if newOffset <= initOffset {
		t.Errorf("endOffset did not increase after flush: before=%d, after=%d", initOffset, newOffset)
	}

	// File stats register and string checks
	stats := st.Stats()
	if stats.Segments == 0 {
		t.Errorf("Stats Segments = 0, want >= 1")
	}

	_ = st.Close()
}

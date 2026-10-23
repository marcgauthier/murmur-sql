// Live scenario: spool compression size gates across traffic profiles.
//
// Go port of the Rust `compression` scenario
// (/home/marc/MURMUR/tests-live/compression/tests/compression.rs),
// TestSpoolCompressionSizes. Each (compression mode, traffic profile) cell
// opens a fresh encrypted store, writes a seeded deterministic dataset in
// batches of 500, applies an update pass over every 10th row where
// applicable, closes cleanly, and measures the on-disk store directory.
//
// Deviation (inherited from Rust): the Database hardcodes its Spool block
// codec, so a none-vs-deflate comparison is impossible through
// murmur.Open. The cells therefore write equivalent seeded KV traffic
// directly to spool.Store with CompressionNone vs CompressionDeflate (same
// 64 KiB target blocks, same AES-256-GCM encryption). Row counts, ID
// checksums, and the size gates mirror the original.
package compression

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// seed is the seeded RNG stream. The same seed is reused for every mode so
// all modes get byte-identical keys and datasets.
const seed = uint64(42)

// batch is the insert batch size.
const batch = 500

// keyID is the key-envelope id.
const keyID = "compression-bench"

// targetBlockBytes is the target plaintext bytes per Spool block (64 KiB).
const targetBlockBytes = 64 << 10

// storeKey is the fresh-application key per store.
var storeKey = [32]byte{
	0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a,
	0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a, 0x3a,
}

// record is one traffic record. All profiles share this single stable
// shape; unused fields stay empty.
type record struct {
	id      [16]byte
	code    string
	qty     int64
	price   float64
	title   string
	body    string
	tags    string
	kind    string
	payload string
	data    []byte
}

// encode is the deterministic field encoding: fixed field order, each
// length-prefixed.
func (r *record) encode() []byte {
	var out []byte
	pushStr := func(s string) {
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(s)))
		out = append(out, lenBuf[:]...)
		out = append(out, s...)
	}
	pushBytes := func(b []byte) {
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(b)))
		out = append(out, lenBuf[:]...)
		out = append(out, b...)
	}
	pushStr(r.code)
	var numBuf [8]byte
	binary.LittleEndian.PutUint64(numBuf[:], uint64(r.qty))
	out = append(out, numBuf[:]...)
	binary.LittleEndian.PutUint64(numBuf[:], float64Bits(r.price))
	out = append(out, numBuf[:]...)
	pushStr(r.title)
	pushStr(r.body)
	pushStr(r.tags)
	pushStr(r.kind)
	pushStr(r.payload)
	pushBytes(r.data)
	return out
}

func float64Bits(f float64) uint64 {
	return math.Float64bits(f)
}

func (r *record) key() []byte {
	key := []byte("rec/")
	const hexDigits = "0123456789abcdef"
	for _, b := range r.id {
		key = append(key, hexDigits[b>>4], hexDigits[b&0xf])
	}
	return key
}

type mode struct {
	name string
	algo spool.Compression
}

var modes = []mode{
	{"none", spool.CompressionNone},
	{"deflate", spool.CompressionDeflate},
}

// profileKind selects which fields a profile fills, and its update pass.
type profileKind int

const (
	oltpSmall profileKind = iota
	textHeavy
	jsonDocs
	blobsRandom
)

type profile struct {
	name string
	rows int
	kind profileKind
}

var profiles = []profile{
	{"oltp-small", 20_000, oltpSmall},
	{"text-heavy", 8_000, textHeavy},
	{"json-docs", 8_000, jsonDocs},
	{"blobs-random", 2_000, blobsRandom},
}

const loremText = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod\ntempor incididunt ut labore et dolore magna aliqua enim ad minim veniam quis nostrud exercitation\nullamco laboris nisi aliquip ex ea commodo consequat duis aute irure in reprehenderit voluptate\nvelit esse cillum fugiat nulla pariatur excepteur sint occaecat cupidatat non proident sunt culpa\nqui officia deserunt mollit anim id est laborum perspiciatis unde omnis natus error voluptatem"

func lorem(rng *rand.Rand, words []string, n int) string {
	parts := make([]string, 0, n)
	for i := 0; i < n; i++ {
		parts = append(parts, words[rng.IntN(len(words))])
	}
	return strings.Join(parts, " ")
}

func fillBytes(rng *rand.Rand, b []byte) {
	for len(b) >= 8 {
		binary.LittleEndian.PutUint64(b[:8], rng.Uint64())
		b = b[8:]
	}
	if len(b) > 0 {
		v := rng.Uint64()
		for i := range b {
			b[i] = byte(v >> (8 * i))
		}
	}
}

func createRow(rng *rand.Rand, words []string, kind profileKind, i int, id [16]byte) record {
	row := record{id: id}
	switch kind {
	case oltpSmall:
		row.code = fmt.Sprintf("SKU-%05d", rng.IntN(2000))
		row.qty = int64(rng.IntN(100))
		row.price = rng.Float64() * 999.99
	case textHeavy:
		row.title = lorem(rng, words, 4+rng.IntN(6))
		row.body = lorem(rng, words, 100+rng.IntN(200))
		row.tags = "news,tech,local"
	case jsonDocs:
		row.kind = "pageview"
		row.payload = fmt.Sprintf(
			`{"event_id":%d,"user":{"id":%d,"name":"user-%d","roles":["reader","editor"]},`+
				`"context":{"app":"storefront","region":"eu-west","build":%d},`+
				`"metrics":{"latency_ms":%.3f,"retries":%d,"flags":[true,false,true]},`+
				`"trace":["recv","auth","load","render","send"]}`,
			i, rng.IntN(50_000), rng.IntN(50_000), 1000+rng.IntN(50), rng.Float64()*250.0, rng.IntN(4),
		)
	case blobsRandom:
		row.data = make([]byte, 4096)
		fillBytes(rng, row.data)
	}
	return row
}

// updateRow runs the update pass over one row. It returns false for
// profiles without updates (immutable blobs).
func updateRow(rng *rand.Rand, kind profileKind, row *record) bool {
	switch kind {
	case oltpSmall:
		row.qty = int64(rng.IntN(100))
		return true
	case textHeavy:
		row.tags = "news,tech,local,updated"
		return true
	case jsonDocs:
		row.kind = "pageview-v2"
		return true
	case blobsRandom:
		return false
	}
	return false
}

type cellResult struct {
	rows     int
	total    uint64
	files    int
	writeDur time.Duration
	checksum string
}

// envScale reads MURMUR_LIVE_COMPRESSION_SCALE (multiplies every profile's
// row count).
func envScale(t *testing.T) int {
	t.Helper()
	raw := os.Getenv("MURMUR_LIVE_COMPRESSION_SCALE")
	if raw == "" {
		return 1
	}
	var n int
	if _, err := fmt.Sscanf(raw, "%d", &n); err != nil || n < 1 {
		t.Fatalf("MURMUR_LIVE_COMPRESSION_SCALE must be a positive integer")
	}
	return n
}

// checksumIDs is FNV-1a/64 over row IDs in write order (only cross-mode
// equality within this process is asserted).
func checksumIDs(rows []record) string {
	h := uint64(0xcbf29ce484222325)
	for i := range rows {
		for _, b := range rows[i].id {
			h ^= uint64(b)
			h *= 0x100000001b3
		}
	}
	return fmt.Sprintf("%016x", h)
}

func dirUsage(t *testing.T, path string) (uint64, int) {
	t.Helper()
	var total uint64
	var files int
	stack := []string{path}
	for len(stack) > 0 {
		dir := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read_dir %s: %v", dir, err)
		}
		for _, entry := range entries {
			if entry.IsDir() {
				stack = append(stack, filepath.Join(dir, entry.Name()))
				continue
			}
			info, err := entry.Info()
			if err != nil {
				t.Fatalf("file metadata: %v", err)
			}
			files++
			total += uint64(info.Size())
		}
	}
	return total, files
}

func runCell(t *testing.T, runtime string, m mode, prof profile, scale int) cellResult {
	t.Helper()
	dir := filepath.Join(runtime, m.name+"-"+prof.name)
	opts := spool.DefaultOptions(dir)
	opts.MasterKey = append([]byte(nil), storeKey[:]...)
	opts.WrappingKeyID = keyID
	opts.TargetBlockBytes = targetBlockBytes
	opts.Compression = m.algo
	opts.CompressionSet = true
	// No codec override: the store's built-in none/deflate codecs apply.
	opts.ReclaimInterval = 0 // deterministic sizes

	store, err := spool.Open(opts)
	if err != nil {
		t.Fatalf("spool open: %v", err)
	}
	rowsTotal := prof.rows * scale
	rng := rand.New(rand.NewPCG(seed, seed))
	words := strings.Fields(loremText)
	rows := make([]record, 0, rowsTotal)

	start := time.Now()
	for base := 0; base < rowsTotal; base += batch {
		end := base + batch
		if end > rowsTotal {
			end = rowsTotal
		}
		mutations := make([]spool.Mutation, 0, end-base)
		for i := base; i < end; i++ {
			var id [16]byte
			fillBytes(rng, id[:])
			row := createRow(rng, words, prof.kind, i, id)
			mutations = append(mutations, spool.Mutation{Key: row.key(), Value: row.encode()})
			rows = append(rows, row)
		}
		if err := store.Commit(mutations, spool.DurabilityAsync); err != nil {
			t.Fatalf("insert batch: %v", err)
		}
	}
	// Churn pass: update every 10th row like live traffic would.
	if prof.kind != blobsRandom {
		for base := 0; base < rowsTotal; base += batch * 10 {
			end := base + batch*10
			if end > rowsTotal {
				end = rowsTotal
			}
			var mutations []spool.Mutation
			for i := base; i < end; i += 10 {
				if !updateRow(rng, prof.kind, &rows[i]) {
					t.Fatalf("update pass missing for profile %s", prof.name)
				}
				mutations = append(mutations, spool.Mutation{Key: rows[i].key(), Value: rows[i].encode()})
			}
			if err := store.Commit(mutations, spool.DurabilityAsync); err != nil {
				t.Fatalf("update batch: %v", err)
			}
		}
	}
	writeDur := time.Since(start)

	// Async commits are admitted without blocking; the location index only
	// reflects them once the background writer processes the batch, so a
	// barrier is required before counting (and before measuring sizes).
	if err := store.Flush(); err != nil {
		t.Fatalf("flush before count: %v", err)
	}
	count := len(store.KeysWithPrefix([]byte("rec/")))
	if count != rowsTotal {
		t.Fatalf("mode %s profile %s: count = %d, want %d", m.name, prof.name, count, rowsTotal)
	}
	checksum := checksumIDs(rows)

	if err := store.Flush(); err != nil {
		t.Fatalf("flush before close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	total, files := dirUsage(t, dir)
	return cellResult{rows: count, total: total, files: files, writeDur: writeDur, checksum: checksum}
}

func formatBytes(n uint64) string {
	const unit = uint64(1024)
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div := unit
	exp := 0
	for n/div >= unit && exp < 3 {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGT"[exp])
}

func TestSpoolCompressionSizes(t *testing.T) {
	runtime, err := harness.PrepareRuntimeDir("compression")
	if err != nil {
		t.Fatalf("prepare runtime dir: %v", err)
	}
	failed := true
	defer func() {
		// No cluster/forensics handle in this in-process single-store
		// scenario; on failure the runtime dir (all cell stores) is
		// retained as-is.
		harness.Finish("compression", runtime, failed)
	}()

	scale := envScale(t)

	// profile -> mode -> result
	results := make(map[string]map[string]cellResult)
	for _, prof := range profiles {
		perMode := make(map[string]cellResult)
		for _, m := range modes {
			res := runCell(t, runtime, m, prof, scale)
			t.Logf("%-12s %-8s rows=%d total=%s files=%d write=%v",
				prof.name, m.name, res.rows, formatBytes(res.total), res.files, res.writeDur)
			perMode[m.name] = res
		}
		results[prof.name] = perMode
	}

	t.Log("mode \\ profile | oltp-small | text-heavy | json-docs | blobs-random")
	for _, m := range modes {
		var cells []string
		for _, prof := range profiles {
			res := results[prof.name][m.name]
			base := results[prof.name]["none"].total
			ratio := 0.0
			if base > 0 {
				ratio = float64(res.total) / float64(base)
			}
			cells = append(cells, fmt.Sprintf("%s (%.0f%%)", formatBytes(res.total), ratio*100.0))
		}
		t.Logf("%-8s | %s", m.name, strings.Join(cells, " | "))
	}

	// Sanity gates: identical datasets must hold identical rows with
	// identical checksums; compressible traffic must shrink under deflate;
	// incompressible traffic must not explode; and segment bytes must exist.
	for _, prof := range profiles {
		var wantSum string
		for i, m := range modes {
			res := results[prof.name][m.name]
			if i == 0 {
				wantSum = res.checksum
			} else if res.checksum != wantSum {
				t.Fatalf("profile %s: mode %s dataset diverged", prof.name, m.name)
			}
			if res.rows != prof.rows*scale {
				t.Fatalf("profile %s mode %s: rows = %d", prof.name, m.name, res.rows)
			}
			if res.files == 0 || res.total == 0 {
				t.Fatalf("profile %s mode %s: no segment bytes measured", prof.name, m.name)
			}
		}
	}
	textNone := results["text-heavy"]["none"].total
	textDeflate := results["text-heavy"]["deflate"].total
	if textDeflate >= textNone {
		t.Fatalf("text-heavy deflate (%d) not smaller than none (%d): compression disengaged?", textDeflate, textNone)
	}
	blobNone := results["blobs-random"]["none"].total
	blobDeflate := results["blobs-random"]["deflate"].total
	if blobDeflate > blobNone*11/10 {
		t.Fatalf("blobs-random deflate grew >10%% over none (%d vs %d)", blobDeflate, blobNone)
	}
	failed = false
}

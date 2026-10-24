// Live benchmark: compare the on-disk store size across Spool
// compression levels for several realistic traffic shapes.
//
// Go port of tests/compression.rs using tests-live/goharness. Identical
// behavior: each (profile, level) cell runs a fresh single-node
// murmur-server with engine.compression_level 1 or 12, writes a seeded
// deterministic dataset through plain SQL in 500-row multi-row INSERT
// batches, runs a churn pass over every 10th row, shuts down cleanly, and
// measures the data directory. Same gates: identical checksums across
// levels, text-heavy shrinks at the high level, blobs-random grows by at
// most 10%.
package compression

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// Compression levels under test: lowest vs high zstd.
var levels = [2]int32{1, 12}

// Batch is the insert batch size (Rust: BATCH = 500).
const batch = 500

// ChurnFlushStride is the flush/update stride inside the churn pass.
const churnFlushStride = 500

// Seed is the fixed dataset seed: every level of one profile gets
// byte-identical keys and datasets.
const seed = uint64(42)

const loremText = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua enim ad minim veniam quis nostrud exercitation ullamco laboris nisi aliquip ex ea commodo consequat duis aute irure in reprehenderit voluptate velit esse cillum fugiat nulla pariatur excepteur sint occaecat cupidatat non proident sunt culpa qui officia deserunt mollit anim id est laborum perspiciatis unde omnis natus error voluptatem"

// rng is a minimal deterministic PRNG (xorshift64*): only same-seed
// equality across levels within one process is needed.
type rng struct {
	state uint64
}

func (r *rng) next() uint64 {
	x := r.state
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	r.state = x
	return x * 0x2545f4914f6cdd1d
}

func (r *rng) below(n uint64) uint64 {
	return r.next() % n
}

func (r *rng) bytes(out []byte) {
	i := 0
	for i < len(out) {
		var word [8]byte
		v := r.next()
		for b := 0; b < 8; b++ {
			word[b] = byte(v >> (8 * b))
		}
		take := len(out) - i
		if take > 8 {
			take = 8
		}
		copy(out[i:i+take], word[:take])
		i += take
	}
}

func (r *rng) float() float64 {
	return float64(r.next()>>11) / float64(uint64(1)<<53)
}

func lorem(r *rng, words int) string {
	vocab := strings.Split(loremText, " ")
	out := make([]string, 0, words)
	for i := 0; i < words; i++ {
		out = append(out, vocab[r.below(uint64(len(vocab)))])
	}
	return strings.Join(out, " ")
}

// profile is one traffic profile. Kind: 0 = oltp-small, 1 = text-heavy,
// 2 = json-docs, 3 = blobs-random.
type profile struct {
	name string
	rows int
	kind uint8
}

var profiles = [4]profile{
	{name: "oltp-small", rows: 20_000, kind: 0},
	{name: "text-heavy", rows: 8_000, kind: 1},
	{name: "json-docs", rows: 8_000, kind: 2},
	{name: "blobs-random", rows: 2_000, kind: 3},
}

// record is one generated row: all columns, with zero values for
// untouched fields.
type record struct {
	id      string
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

func create(r *rng, p *profile, i int) record {
	var idBytes [16]byte
	r.bytes(idBytes[:])
	rec := record{id: fmt.Sprintf("%x", idBytes)}
	switch p.kind {
	case 0:
		rec.code = fmt.Sprintf("SKU-%05d", r.below(2000))
		rec.qty = int64(r.below(100))
		rec.price = r.float() * 999.99
	case 1:
		titleWords := 4 + int(r.below(6))
		bodyWords := 100 + int(r.below(200))
		rec.title = lorem(r, titleWords)
		rec.body = lorem(r, bodyWords)
		rec.tags = "news,tech,local"
	case 2:
		rec.kind = "pageview"
		rec.payload = fmt.Sprintf(
			`{"event_id":%d,"user":{"id":%d,"name":"user-%d","roles":["reader","editor"]},"context":{"app":"storefront","region":"eu-west","build":%d},"metrics":{"latency_ms":%.3f,"retries":%d,"flags":[true,false,true]},"trace":["recv","auth","load","render","send"]}`,
			i,
			r.below(50_000),
			r.below(50_000),
			1000+r.below(50),
			r.float()*250.0,
			r.below(4),
		)
	default:
		buf := make([]byte, 4096)
		r.bytes(buf)
		rec.data = buf
	}
	return rec
}

// churn runs the churn pass for one row id: update every 10th row like
// live traffic would (blobs are immutable). It reports whether a write ran.
func churn(cluster *goharness.Cluster, p *profile, id string) (bool, error) {
	switch p.kind {
	case 0:
		qty := int64(len(id) % 100)
		if _, err := cluster.Node(0).Execute("UPDATE cells SET qty = $1 WHERE id = $2", qty, id); err != nil {
			return false, fmt.Errorf("churn update %q: %w", id, err)
		}
		return true, nil
	case 1:
		if _, err := cluster.Node(0).Execute("UPDATE cells SET tags = $1 WHERE id = $2", "news,tech,local,updated", id); err != nil {
			return false, fmt.Errorf("churn update %q: %w", id, err)
		}
		return true, nil
	case 2:
		if _, err := cluster.Node(0).Execute("UPDATE cells SET kind = $1 WHERE id = $2", "pageview-v2", id); err != nil {
			return false, fmt.Errorf("churn update %q: %w", id, err)
		}
		return true, nil
	default:
		return false, nil
	}
}

type cellResult struct {
	rows     int
	total    uint64
	files    int
	writeDur time.Duration
	checksum uint64
}

func fnv1a(ids []string) uint64 {
	hash := uint64(0xcbf29ce484222325)
	for _, id := range ids {
		for i := 0; i < len(id); i++ {
			hash ^= uint64(id[i])
			hash *= 0x100000001b3
		}
	}
	return hash
}

func dirSize(dir string) (uint64, int) {
	var total uint64
	var files int
	var stack []string
	stack = append(stack, dir)
	for len(stack) > 0 {
		path := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		entries, err := os.ReadDir(path)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			p := filepath.Join(path, entry.Name())
			if entry.IsDir() {
				stack = append(stack, p)
			} else if meta, err := os.Stat(p); err == nil && !meta.IsDir() {
				total += uint64(meta.Size())
				files++
			}
		}
	}
	return total, files
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
	return fmt.Sprintf("%.1f%sB", float64(n)/float64(div), []string{"K", "M", "G", "T"}[exp])
}

// runCell runs one (profile, level) cell: fresh single-node server, seeded
// dataset, churn pass, clean shutdown, data-dir measurement.
func runCell(p *profile, level int32, scale int) (cellResult, error) {
	var zero cellResult
	name := fmt.Sprintf("compression-%s-l%d", p.name, level)
	opts := goharness.DefaultClusterOptions(name)
	opts.NumNodes = 1
	opts.ExtraYAML = fmt.Sprintf("engine:\n  compression_level: %d\n", level)
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		return zero, fmt.Errorf("cell %s setup: %w", p.name, err)
	}

	body := func() (cellResult, error) {
		if _, err := cluster.Node(0).Simple("CREATE TABLE cells(id TEXT NOT NULL PRIMARY KEY, code TEXT NOT NULL, qty INTEGER NOT NULL, price REAL NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL, tags TEXT NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL, data BLOB NOT NULL)"); err != nil {
			return zero, fmt.Errorf("create table error: %v", err)
		}
		if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
			return zero, fmt.Errorf("flush error: %v", err)
		}

		rows := p.rows * scale
		r := &rng{state: seed}
		ids := make([]string, 0, rows)
		start := time.Now()
		for base := 0; base < rows; base += batch {
			end := base + batch
			if end > rows {
				end = rows
			}
			batchRows := make([]record, 0, end-base)
			for i := base; i < end; i++ {
				rec := create(r, p, i)
				ids = append(ids, rec.id)
				batchRows = append(batchRows, rec)
			}
			// One multi-row INSERT per batch.
			var sql strings.Builder
			sql.WriteString("INSERT INTO cells(id, code, qty, price, title, body, tags, kind, payload, data) VALUES")
			refs := make([]any, 0, len(batchRows)*10)
			for n, rec := range batchRows {
				baseParam := n*10 + 1
				sep := ""
				if n != 0 {
					sep = ","
				}
				fmt.Fprintf(&sql, "%s ($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
					sep,
					baseParam, baseParam+1, baseParam+2, baseParam+3, baseParam+4,
					baseParam+5, baseParam+6, baseParam+7, baseParam+8, baseParam+9)
				refs = append(refs, rec.id, rec.code, rec.qty, rec.price, rec.title,
					rec.body, rec.tags, rec.kind, rec.payload, rec.data)
			}
			if _, err := cluster.Node(0).Execute(sql.String(), refs...); err != nil {
				return zero, fmt.Errorf("batch insert at %d: %w", base, err)
			}
			if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
				return zero, fmt.Errorf("flush batch at %d: %w", base, err)
			}
		}
		// Churn pass: update every 10th row like live traffic would.
		churned := 0
		for i, id := range ids {
			if i%10 != 0 {
				continue
			}
			ran, err := churn(cluster, p, id)
			if err != nil {
				return zero, err
			}
			if ran {
				churned++
				if churned%churnFlushStride == 0 {
					if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
						return zero, fmt.Errorf("flush churn: %w", err)
					}
				}
			}
		}
		if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
			return zero, fmt.Errorf("final flush: %w", err)
		}
		writeDur := time.Since(start)

		countRows, err := cluster.Node(0).Query("SELECT COUNT(*) FROM cells")
		if err != nil {
			return zero, fmt.Errorf("count: %w", err)
		}
		if len(countRows) == 0 {
			return zero, fmt.Errorf("count returned no rows")
		}
		count, err := countRows[0].Int64(0)
		if err != nil {
			return zero, fmt.Errorf("decode count: %w", err)
		}
		if int(count) != rows {
			return zero, fmt.Errorf("level %d profile %s: count = %d, want %d", level, p.name, count, rows)
		}
		checksum := fnv1a(ids)
		dataDir := cluster.NodeInfo(0).DataDir
		cluster.StopNode(0)
		total, files := dirSize(dataDir)
		return cellResult{rows: int(count), total: total, files: files, writeDur: writeDur, checksum: checksum}, nil
	}
	result, err := body()

	failed := err != nil
	if err != nil {
		fmt.Fprintf(os.Stderr, "cell %s l%d failed: %v\n", p.name, level, err)
	}
	if failed {
		cluster.DumpForensics("compression-cell")
	}
	// The node is already stopped on success; finish reaps children and
	// retains-or-cleans the runtime dir + RESULT line.
	cluster.Finish(failed)
	if err != nil {
		return zero, err
	}
	fmt.Fprintf(os.Stderr, "%-12s l%-2d rows=%d total=%s (%d files) write=%s\n",
		p.name, level, result.rows, formatBytes(result.total), result.files, result.writeDur)
	return result, nil
}

func TestSpoolCompressionSizes(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	scale := 1
	if v, err := strconv.Atoi(os.Getenv("MURMUR_LIVE_COMPRESSION_SCALE")); err == nil && v >= 1 {
		scale = v
	}

	failed := false
	// profile -> level -> result.
	type levelCell struct {
		level  int32
		result cellResult
	}
	var results []struct {
		name  string
		cells []levelCell
	}
	for i := range profiles {
		p := &profiles[i]
		var cells []levelCell
		for _, level := range levels {
			result, err := runCell(p, level, scale)
			if err != nil {
				fmt.Fprintf(os.Stderr, "scenario failed: %v\n", err)
				failed = true
				break
			}
			cells = append(cells, levelCell{level: level, result: result})
		}
		if failed {
			break
		}
		results = append(results, struct {
			name  string
			cells []levelCell
		}{name: p.name, cells: cells})
	}
	if failed {
		t.Fatal("scenario failed; artifacts retained under tests-live/failures/")
	}

	names := make([]string, 0, len(profiles))
	for _, p := range profiles {
		names = append(names, p.name)
	}
	fmt.Fprintf(os.Stderr, "level \\ profile | %s\n", strings.Join(names, " | "))
	for _, level := range levels {
		var cells []string
		for _, r := range results {
			var base, total uint64
			for _, c := range r.cells {
				if c.level == levels[0] {
					base = c.result.total
				}
				if c.level == level {
					total = c.result.total
				}
			}
			ratio := 0.0
			if base > 0 {
				ratio = float64(total) / float64(base) * 100.0
			}
			cells = append(cells, fmt.Sprintf("%s (%.0f%%)", formatBytes(total), ratio))
		}
		fmt.Fprintf(os.Stderr, "l%-7d | %s\n", level, strings.Join(cells, " | "))
	}

	// Sanity gates: identical datasets must hold identical rows with
	// identical checksums; compressible traffic must shrink at the high
	// level; incompressible traffic must not explode; and segments must
	// exist.
	for _, r := range results {
		var want *profile
		for i := range profiles {
			if profiles[i].name == r.name {
				want = &profiles[i]
			}
		}
		var wantSum *uint64
		for _, c := range r.cells {
			if wantSum == nil {
				sum := c.result.checksum
				wantSum = &sum
			} else if *wantSum != c.result.checksum {
				t.Fatalf("profile %s: level %d dataset diverged", r.name, c.level)
			}
			if c.result.rows != want.rows*scale {
				t.Fatalf("profile %s level %d: row count = %d, want %d", r.name, c.level, c.result.rows, want.rows*scale)
			}
			if !(c.result.files > 0 && c.result.total > 0) {
				t.Fatalf("profile %s level %d: no store bytes measured", r.name, c.level)
			}
		}
	}
	get := func(profileName string, level int32) uint64 {
		for _, r := range results {
			if r.name != profileName {
				continue
			}
			for _, c := range r.cells {
				if c.level == level {
					return c.result.total
				}
			}
		}
		t.Fatalf("profile %s level %d: no result", profileName, level)
		return 0
	}
	textLow := get("text-heavy", levels[0])
	textHigh := get("text-heavy", levels[1])
	if !(textHigh < textLow) {
		t.Fatalf("text-heavy l%d (%d) not smaller than l%d (%d): compression disengaged?",
			levels[1], textHigh, levels[0], textLow)
	}
	blobLow := get("blobs-random", levels[0])
	blobHigh := get("blobs-random", levels[1])
	if !(blobHigh <= blobLow*11/10) {
		t.Fatalf("blobs-random l%d grew >10%% over l%d (%d vs %d)",
			levels[1], levels[0], blobHigh, blobLow)
	}
}

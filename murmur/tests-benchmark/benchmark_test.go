// Package benchmark ports tests-benchmark from the Rust workspace to Go.
//
// It evaluates sustained throughput and latency under a realistic 80% read /
// 20% write workload against Murmur's in-memory SQLite core with Spool
// persistence, using micro-batching (up to 50 requests or 5 ms).
package benchmark

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur"
)

var (
	benchDuration = flag.Duration("bench-duration", 30*time.Second,
		"Duration of the micro-batched workload run")
	benchSeed = flag.Uint64("bench-seed", 20000,
		"Number of initial user rows to pre-seed")
	benchBatchSize = flag.Int("bench-batch-size", 50,
		"Max requests per micro-batch")
	benchBatchTimeout = flag.Duration("bench-batch-timeout", 5*time.Millisecond,
		"Max wait time per micro-batch")
)

const (
	// usersDDL mirrors the Rust benchmark: 10-column users table with a
	// primary key and two secondary indexes.
	usersDDL = `CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT NOT NULL,
    full_name TEXT NOT NULL,
    role TEXT NOT NULL,
    status INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    login_count INTEGER NOT NULL,
    bio TEXT NOT NULL
);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_status_created ON users(status, created_at);`

	selectUsers = "SELECT id, username, email, full_name, role, status, created_at, updated_at, login_count, bio FROM users WHERE "

	maxLatencySamples = 100_000
	seedBatchSize     = 1000
)

// rng is a fast xorshift64 pseudo-random generator, matching the Rust bench.
type rng uint64

func (r *rng) next() uint64 {
	x := uint64(*r)
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	*r = rng(x)
	return x
}

type writeOp struct {
	insert bool
	id     uint64
	status int64
	ts     int64
	t0     time.Time
}

type readOp struct {
	pk    bool
	id    int64
	email string
	t0    time.Time
}

type latencyStats struct {
	count         int
	p50, p95, p99 uint32
	max           uint32
}

func calcLatency(samples []uint32) latencyStats {
	if len(samples) == 0 {
		return latencyStats{}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	n := len(samples)
	return latencyStats{
		count: n,
		p50:   samples[n*50/100],
		p95:   samples[n*95/100],
		p99:   samples[n*99/100],
		max:   samples[n-1],
	}
}

func dirSize(root string) uint64 {
	var total uint64
	_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			if info, err := d.Info(); err == nil {
				total += uint64(info.Size())
			}
		}
		return nil
	})
	return total
}

func isBusy(err error) bool {
	return err != nil && errors.Is(err, murmur.ErrBusy)
}

func benchKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = 42
	}
	return key
}

func benchOptions() murmur.Options {
	opts := murmur.DefaultOptions()
	opts.Mode = murmur.OpenCreateNew
	opts.CaptureInterval = time.Second
	opts.CaptureBytes = 8 << 20
	opts.QueueBytes = 128 << 20
	opts.TransactionBytes = 128 << 20
	return opts
}

// transact retries write transactions on Busy, mirroring the Rust bench.
func transact(db *murmur.Database, f func(sql *murmur.Sql) (any, error)) error {
	for {
		_, err := db.Transaction(f)
		if isBusy(err) {
			runtime.Gosched()
			continue
		}
		return err
	}
}

// flushWrites executes buffered writes in one transaction.
func flushWrites(db *murmur.Database, writes *[]writeOp, latencies *[]uint32) (uint64, error) {
	if len(*writes) == 0 {
		return 0, nil
	}
	batch := *writes
	*writes = (*writes)[:0]
	if err := transact(db, func(sql *murmur.Sql) (any, error) {
		insertStmt, err := sql.Prepare("INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		if err != nil {
			return nil, err
		}
		defer insertStmt.Close()
		updateStmt, err := sql.Prepare("UPDATE users SET updated_at = ?, login_count = login_count + 1, bio = ?, status = ? WHERE id = ?")
		if err != nil {
			return nil, err
		}
		defer updateStmt.Close()
		for _, op := range batch {
			if op.insert {
				id := int64(op.id)
				if err := insertStmt.Bind(
					id,
					fmt.Sprintf("user_%d", op.id),
					fmt.Sprintf("user_%d@example.com", op.id),
					fmt.Sprintf("New User %d", op.id),
					"member",
					int64(1),
					1_700_000_000+id,
					1_700_000_000+id,
					int64(0),
					"Newly registered user account",
				); err != nil {
					return nil, err
				}
				if _, err := insertStmt.Step(); err != nil {
					return nil, err
				}
				if err := insertStmt.Reset(); err != nil {
					return nil, err
				}
			} else {
				if err := updateStmt.Bind(
					op.ts,
					fmt.Sprintf("Updated bio note at iteration %d", op.ts),
					op.status,
					int64(op.id),
				); err != nil {
					return nil, err
				}
				if _, err := updateStmt.Step(); err != nil {
					return nil, err
				}
				if err := updateStmt.Reset(); err != nil {
					return nil, err
				}
			}
		}
		return nil, nil
	}); err != nil {
		return 0, err
	}
	now := time.Now()
	for _, op := range batch {
		if len(*latencies) < maxLatencySamples {
			*latencies = append(*latencies, uint32(now.Sub(op.t0).Microseconds()))
		}
	}
	return uint64(len(batch)), nil
}

// flushReads executes buffered reads in one read call.
func flushReads(db *murmur.Database, reads *[]readOp, latencies *[]uint32) (uint64, error) {
	if len(*reads) == 0 {
		return 0, nil
	}
	batch := *reads
	*reads = (*reads)[:0]
	if _, err := db.Read(func(sql *murmur.Sql) (any, error) {
		pkStmt, err := sql.Prepare(selectUsers + "id = ?")
		if err != nil {
			return nil, err
		}
		defer pkStmt.Close()
		emailStmt, err := sql.Prepare(selectUsers + "email = ?")
		if err != nil {
			return nil, err
		}
		defer emailStmt.Close()
		for _, op := range batch {
			if op.pk {
				if err := pkStmt.Bind(op.id); err != nil {
					return nil, err
				}
				if ok, err := pkStmt.Step(); err != nil {
					return nil, err
				} else if ok {
					_ = pkStmt.ColumnText(4)
				}
				if err := pkStmt.Reset(); err != nil {
					return nil, err
				}
			} else {
				if err := emailStmt.Bind(op.email); err != nil {
					return nil, err
				}
				if ok, err := emailStmt.Step(); err != nil {
					return nil, err
				} else if ok {
					_ = emailStmt.ColumnInt64(5)
				}
				if err := emailStmt.Reset(); err != nil {
					return nil, err
				}
			}
		}
		return nil, nil
	}); err != nil {
		return 0, err
	}
	now := time.Now()
	for _, op := range batch {
		if len(*latencies) < maxLatencySamples {
			*latencies = append(*latencies, uint32(now.Sub(op.t0).Microseconds()))
		}
	}
	return uint64(len(batch)), nil
}

// seedUsers pre-seeds count user rows in batches of 1000.
func seedUsers(db *murmur.Database, count uint64) error {
	for batchStart := uint64(1); batchStart <= count; batchStart += seedBatchSize {
		batchEnd := batchStart + seedBatchSize
		if batchEnd > count+1 {
			batchEnd = count + 1
		}
		start, end := batchStart, batchEnd
		if err := transact(db, func(sql *murmur.Sql) (any, error) {
			stmt, err := sql.Prepare("INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			if err != nil {
				return nil, err
			}
			defer stmt.Close()
			for i := start; i < end; i++ {
				role := "viewer"
				if i%10 == 0 {
					role = "admin"
				} else if i%3 == 0 {
					role = "editor"
				}
				id := int64(i)
				if err := stmt.Bind(
					id,
					fmt.Sprintf("user_%d", i),
					fmt.Sprintf("user_%d@example.com", i),
					fmt.Sprintf("User Name %d", i),
					role,
					int64(i%5),
					1_700_000_000+id,
					1_700_000_000+id,
					int64(i%100),
					fmt.Sprintf("Bio description text for user %d", i),
				); err != nil {
					return nil, err
				}
				if _, err := stmt.Step(); err != nil {
					return nil, err
				}
				if err := stmt.Reset(); err != nil {
					return nil, err
				}
			}
			return nil, nil
		}); err != nil {
			return err
		}
	}
	return db.Flush()
}

type workloadResult struct {
	reads, writes uint64
	readStats     latencyStats
	writeStats    latencyStats
	elapsed       time.Duration
	flush         time.Duration
	diskBytes     uint64
	recovery      time.Duration
}

// runWorkload executes the timed 80/20 micro-batched loop.
func runWorkload(db *murmur.Database, seedCount uint64, duration time.Duration, maxBatch int, batchTimeout time.Duration) (workloadResult, error) {
	var res workloadResult
	r := rng(12345678901234567)
	var readLatencies, writeLatencies []uint32
	pendingWrites := make([]writeOp, 0, maxBatch)
	pendingReads := make([]readOp, 0, maxBatch)
	lastWriteFlush := time.Now()
	lastReadFlush := time.Now()
	nextID := seedCount + 1
	var opCounter uint64

	deadline := time.Now().Add(duration)
	start := time.Now()
	for time.Now().Before(deadline) {
		isWrite := r.next()%5 == 0 // 20% write, 80% read
		randID := r.next()%seedCount + 1
		now := time.Now()

		if isWrite {
			if r.next()%2 == 0 {
				id := nextID
				nextID++
				pendingWrites = append(pendingWrites, writeOp{insert: true, id: id, t0: now})
			} else {
				pendingWrites = append(pendingWrites, writeOp{
					id:     randID,
					status: int64(r.next() % 5),
					ts:     1_700_000_000 + int64(opCounter),
					t0:     now,
				})
			}
			if len(pendingWrites) >= maxBatch || time.Since(lastWriteFlush) >= batchTimeout {
				n, err := flushWrites(db, &pendingWrites, &writeLatencies)
				if err != nil {
					return res, err
				}
				res.writes += n
				lastWriteFlush = time.Now()
			}
		} else {
			if r.next()%2 == 0 {
				pendingReads = append(pendingReads, readOp{pk: true, id: int64(randID), t0: now})
			} else {
				pendingReads = append(pendingReads, readOp{
					email: fmt.Sprintf("user_%d@example.com", randID),
					t0:    now,
				})
			}
			if len(pendingReads) >= maxBatch || time.Since(lastReadFlush) >= batchTimeout {
				n, err := flushReads(db, &pendingReads, &readLatencies)
				if err != nil {
					return res, err
				}
				res.reads += n
				lastReadFlush = time.Now()
			}
		}

		if len(pendingWrites) > 0 && time.Since(lastWriteFlush) >= batchTimeout {
			n, err := flushWrites(db, &pendingWrites, &writeLatencies)
			if err != nil {
				return res, err
			}
			res.writes += n
			lastWriteFlush = time.Now()
		}
		if len(pendingReads) > 0 && time.Since(lastReadFlush) >= batchTimeout {
			n, err := flushReads(db, &pendingReads, &readLatencies)
			if err != nil {
				return res, err
			}
			res.reads += n
			lastReadFlush = time.Now()
		}

		opCounter++
	}
	if len(pendingWrites) > 0 {
		n, err := flushWrites(db, &pendingWrites, &writeLatencies)
		if err != nil {
			return res, err
		}
		res.writes += n
	}
	if len(pendingReads) > 0 {
		n, err := flushReads(db, &pendingReads, &readLatencies)
		if err != nil {
			return res, err
		}
		res.reads += n
	}
	res.elapsed = time.Since(start)
	res.readStats = calcLatency(readLatencies)
	res.writeStats = calcLatency(writeLatencies)
	return res, nil
}

// openSeeded opens a fresh database, migrates the schema, and pre-seeds it.
func openSeeded(tb testing.TB, path string, seedCount uint64) *murmur.Database {
	tb.Helper()
	db, err := murmur.Open(path, benchKey(), benchOptions())
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	if err := db.Migrate(murmur.NewMigration(usersDDL)); err != nil {
		tb.Fatalf("migrate: %v", err)
	}
	if err := seedUsers(db, seedCount); err != nil {
		tb.Fatalf("seed: %v", err)
	}
	return db
}

func reportWorkload(b *testing.B, res workloadResult) {
	b.Helper()
	total := res.reads + res.writes
	secs := res.elapsed.Seconds()
	b.ReportMetric(float64(total)/secs, "ops/s")
	b.ReportMetric(float64(res.reads)/secs, "reads/s")
	b.ReportMetric(float64(res.writes)/secs, "writes/s")
	b.ReportMetric(float64(res.readStats.p50), "read-p50-us")
	b.ReportMetric(float64(res.readStats.p95), "read-p95-us")
	b.ReportMetric(float64(res.readStats.p99), "read-p99-us")
	b.ReportMetric(float64(res.writeStats.p50), "write-p50-us")
	b.ReportMetric(float64(res.writeStats.p95), "write-p95-us")
	b.ReportMetric(float64(res.writeStats.p99), "write-p99-us")
	b.Logf("elapsed=%.3fs total=%d reads=%d writes=%d", secs, total, res.reads, res.writes)
	b.Logf("read latency us: p50=%d p95=%d p99=%d max=%d (sampled %d)",
		res.readStats.p50, res.readStats.p95, res.readStats.p99, res.readStats.max, res.readStats.count)
	b.Logf("write latency us: p50=%d p95=%d p99=%d max=%d (sampled %d)",
		res.writeStats.p50, res.writeStats.p95, res.writeStats.p99, res.writeStats.max, res.writeStats.count)
	b.Logf("spool flush=%.3fms disk=%.2f MiB (%d bytes) recovery=%.3fms",
		res.flush.Seconds()*1000, float64(res.diskBytes)/(1024*1024), res.diskBytes, res.recovery.Seconds()*1000)
}

// BenchmarkMicroBatchWorkload runs the full 80/20 micro-batched workload once.
// b.N is intentionally ignored: the run length is set by -bench-duration.
func BenchmarkMicroBatchWorkload(b *testing.B) {
	duration, seedCount := *benchDuration, *benchSeed
	if testing.Short() {
		if !flagChanged("bench-duration") {
			duration = 2 * time.Second
		}
		if !flagChanged("bench-seed") {
			seedCount = 2000
		}
	}
	if duration <= 0 {
		b.Fatalf("duration must be greater than zero")
	}
	dbPath := filepath.Join(b.TempDir(), "murmur_bench")
	b.Logf("pre-seeding %d users...", seedCount)
	seedStart := time.Now()
	db := openSeeded(b, dbPath, seedCount)
	b.Logf("pre-seeded in %.2fs", time.Since(seedStart).Seconds())

	b.ResetTimer()
	res, err := runWorkload(db, seedCount, duration, *benchBatchSize, *benchBatchTimeout)
	if err != nil {
		b.Fatalf("workload: %v", err)
	}
	b.StopTimer()

	flushStart := time.Now()
	if err := db.Flush(); err != nil {
		b.Fatalf("flush: %v", err)
	}
	res.flush = time.Since(flushStart)
	res.diskBytes = dirSize(dbPath)
	if err := db.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}

	reopenStart := time.Now()
	reopened, err := murmur.Open(dbPath, benchKey(), murmur.DefaultOptions())
	if err != nil {
		b.Fatalf("reopen: %v", err)
	}
	res.recovery = time.Since(reopenStart)
	if err := reopened.Close(); err != nil {
		b.Fatalf("reopen close: %v", err)
	}

	reportWorkload(b, res)
}

func flagChanged(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

// TestWorkloadSmoke runs a tiny end-to-end workload as a correctness check.
func TestWorkloadSmoke(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "murmur_bench")
	db := openSeeded(t, dbPath, 500)
	res, err := runWorkload(db, 500, time.Second, 50, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("workload: %v", err)
	}
	if res.reads == 0 || res.writes == 0 {
		t.Fatalf("expected reads and writes, got reads=%d writes=%d", res.reads, res.writes)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := murmur.Open(dbPath, benchKey(), murmur.DefaultOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("reopen close: %v", err)
	}
	t.Logf("smoke: reads=%d writes=%d elapsed=%s", res.reads, res.writes, res.elapsed.Round(time.Millisecond))
}

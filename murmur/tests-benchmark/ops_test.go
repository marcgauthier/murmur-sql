package benchmark

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur"
)

// Focused N-driven benchmarks over a pre-seeded database. Each complements
// BenchmarkMicroBatchWorkload by isolating one operation of the 80/20 mix:
// PK lookup, secondary-index lookup, profile update, and insert.

func openSeededBench(b *testing.B, seedCount uint64) *murmur.Database {
	b.Helper()
	db := openSeeded(b, filepath.Join(b.TempDir(), "murmur_bench"), seedCount)
	b.Cleanup(func() {
		if err := db.Flush(); err != nil {
			b.Errorf("flush: %v", err)
		}
		if err := db.Close(); err != nil {
			b.Errorf("close: %v", err)
		}
	})
	return db
}

func benchSeedCount() uint64 {
	if flagChanged("bench-seed") {
		return *benchSeed
	}
	if testing.Short() {
		return 2000
	}
	return *benchSeed
}

func BenchmarkReadPK(b *testing.B) {
	seed := benchSeedCount()
	db := openSeededBench(b, seed)
	r := rng(987654321)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := int64(r.next()%seed + 1)
		if _, err := db.Read(func(sql *murmur.Sql) (any, error) {
			return sql.QueryRow(selectUsers+"id = ?", id)
		}); err != nil {
			b.Fatalf("read pk: %v", err)
		}
	}
}

func BenchmarkReadEmail(b *testing.B) {
	seed := benchSeedCount()
	db := openSeededBench(b, seed)
	r := rng(987654321)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		email := fmt.Sprintf("user_%d@example.com", r.next()%seed+1)
		if _, err := db.Read(func(sql *murmur.Sql) (any, error) {
			return sql.QueryRow(selectUsers+"email = ?", email)
		}); err != nil {
			b.Fatalf("read email: %v", err)
		}
	}
}

func BenchmarkWriteUpdate(b *testing.B) {
	seed := benchSeedCount()
	db := openSeededBench(b, seed)
	r := rng(13579)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := int64(r.next()%seed + 1)
		status := int64(r.next() % 5)
		ts := int64(1_700_000_000 + i)
		if err := transact(db, func(sql *murmur.Sql) (any, error) {
			return nil, execUpdate(sql, ts, status, id)
		}); err != nil {
			b.Fatalf("update: %v", err)
		}
	}
}

func execUpdate(sql *murmur.Sql, ts, status, id int64) error {
	stmt, err := sql.Prepare("UPDATE users SET updated_at = ?, login_count = login_count + 1, bio = ?, status = ? WHERE id = ?")
	if err != nil {
		return err
	}
	defer stmt.Close()
	if err := stmt.Bind(ts, fmt.Sprintf("Updated bio note at iteration %d", ts), status, id); err != nil {
		return err
	}
	_, err = stmt.Step()
	return err
}

func BenchmarkWriteInsert(b *testing.B) {
	seed := benchSeedCount()
	db := openSeededBench(b, seed)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := uint64(i) + seed + 1
		if err := transact(db, func(sql *murmur.Sql) (any, error) {
			return nil, execInsert(sql, id)
		}); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}

func execInsert(sql *murmur.Sql, id uint64) error {
	stmt, err := sql.Prepare("INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()
	n := int64(id)
	if err := stmt.Bind(
		n,
		fmt.Sprintf("user_%d", id),
		fmt.Sprintf("user_%d@example.com", id),
		fmt.Sprintf("New User %d", id),
		"member",
		int64(1),
		1_700_000_000+n,
		1_700_000_000+n,
		int64(0),
		"Newly registered user account",
	); err != nil {
		return err
	}
	_, err = stmt.Step()
	return err
}

// BenchmarkBatchWrite50 flushes one 50-request write micro-batch per iteration.
func BenchmarkBatchWrite50(b *testing.B) {
	seed := benchSeedCount()
	db := openSeededBench(b, seed)
	r := rng(24680)
	var latencies []uint32
	nextID := seed + 1
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := make([]writeOp, 0, 50)
		now := time.Now()
		for j := 0; j < 50; j++ {
			if r.next()%2 == 0 {
				id := nextID
				nextID++
				batch = append(batch, writeOp{insert: true, id: id, t0: now})
			} else {
				batch = append(batch, writeOp{
					id:     r.next()%seed + 1,
					status: int64(r.next() % 5),
					ts:     1_700_000_000 + int64(i*50+j),
					t0:     now,
				})
			}
		}
		if _, err := flushWrites(db, &batch, &latencies); err != nil {
			b.Fatalf("batch write: %v", err)
		}
	}
	b.ReportMetric(float64(b.N*50)/b.Elapsed().Seconds(), "writes/s")
}

// BenchmarkBatchRead50 flushes one 50-request read micro-batch per iteration.
func BenchmarkBatchRead50(b *testing.B) {
	seed := benchSeedCount()
	db := openSeededBench(b, seed)
	r := rng(1357911)
	var latencies []uint32
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := make([]readOp, 0, 50)
		now := time.Now()
		for j := 0; j < 50; j++ {
			id := r.next()%seed + 1
			if r.next()%2 == 0 {
				batch = append(batch, readOp{pk: true, id: int64(id), t0: now})
			} else {
				batch = append(batch, readOp{
					email: fmt.Sprintf("user_%d@example.com", id),
					t0:    now,
				})
			}
		}
		if _, err := flushReads(db, &batch, &latencies); err != nil {
			b.Fatalf("batch read: %v", err)
		}
	}
	b.ReportMetric(float64(b.N*50)/b.Elapsed().Seconds(), "reads/s")
}

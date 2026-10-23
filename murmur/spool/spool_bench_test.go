package spool_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// BenchmarkPutWriters measures concurrent Put throughput at 1, 8, 32
// and 128 writers (§39 phase 2 validation). The background flusher
// drains during the run; the benchmark resets its timer after setup.
func BenchmarkPutWriters(b *testing.B) {
	for _, writers := range []int{1, 8, 32, 128} {
		b.Run(fmt.Sprint(writers), func(b *testing.B) {
			dir := b.TempDir()
			o := spool.DefaultOptions(dir)
			o.MasterKey = testMasterKey
			st, err := spool.Open(o)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer st.Close()
			val := make([]byte, 128)
			b.ResetTimer()
			b.SetBytes(int64(len(val)))
			perWriter := (b.N + writers - 1) / writers
			var wg sync.WaitGroup
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < perWriter; i++ {
						if err := st.Put([]byte(fmt.Sprintf("w%d/%d", w, i)), val); err != nil {
							b.Errorf("Put: %v", err)
							return
						}
					}
				}(w)
			}
			wg.Wait()
			b.StopTimer()
		})
	}
}

// BenchmarkRotateKey measures data-key rotation latency: each op
// persists the keyring before publishing the new current key, so
// this bounds rotation cost under the §44 evidence matrix.
func BenchmarkRotateKey(b *testing.B) {
	dir := b.TempDir()
	o := spool.DefaultOptions(dir)
	o.MasterKey = testMasterKey
	st, err := spool.Open(o)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer st.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := st.RotateKey(); err != nil {
			b.Fatalf("RotateKey: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "rot/s")
}

// BenchmarkGroupSyncWrites measures synchronous commit-group
// throughput for isolated (1-mutation) and grouped (20-mutation)
// Sync commits.
func BenchmarkGroupSyncWrites(b *testing.B) {
	for _, size := range []int{1, 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			dir := b.TempDir()
			o := spool.DefaultOptions(dir)
			o.MasterKey = testMasterKey
			st, err := spool.Open(o)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer st.Close()
			val := make([]byte, 128)
			b.ResetTimer()
			b.SetBytes(int64(size * len(val)))
			for i := 0; i < b.N; i++ {
				muts := make([]spool.Mutation, size)
				for s := range muts {
					muts[s] = spool.Mutation{
						Key:   []byte(fmt.Sprintf("g%08d/s%02d", i, s)),
						Value: val,
					}
				}
				if err := st.Commit(muts, spool.DurabilitySync); err != nil {
					b.Fatalf("Commit: %v", err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "grp/s")
		})
	}
}

// BenchmarkHotKey measures same-key update throughput through the
// async admit path with periodic flushes (sustained mixed
// admit+flush rate).
func BenchmarkHotKey(b *testing.B) {
	dir := b.TempDir()
	o := spool.DefaultOptions(dir)
	o.MasterKey = testMasterKey
	st, err := spool.Open(o)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer st.Close()
	key := []byte("hot")
	val := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val[0] = byte(i)
		if err := st.Put(key, val); err != nil {
			b.Fatalf("Put: %v", err)
		}
		if i%1000 == 999 {
			if err := st.Flush(); err != nil {
				b.Fatalf("Flush: %v", err)
			}
		}
	}
	b.StopTimer()
}

// BenchmarkFlushThroughput measures sustained admit+flush
// throughput for highly compressible versus incompressible 1KB
// values, flushed every 2000 records.
func BenchmarkFlushThroughput(b *testing.B) {
	mkval := map[string]func(i int) []byte{
		"compressible":   func(i int) []byte { return make([]byte, 1024) },
		"incompressible": func(i int) []byte { return incompressible(1024, uint64(i)) },
	}
	for name, mk := range mkval {
		b.Run(name, func(b *testing.B) {
			dir := b.TempDir()
			o := spool.DefaultOptions(dir)
			o.MasterKey = testMasterKey
			st, err := spool.Open(o)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer st.Close()
			b.ResetTimer()
			b.SetBytes(1024)
			for i := 0; i < b.N; i++ {
				if err := st.Put([]byte(fmt.Sprintf("k%08d", i)), mk(i)); err != nil {
					b.Fatalf("Put: %v", err)
				}
				if i%2000 == 1999 {
					if err := st.Flush(); err != nil {
						b.Fatalf("Flush: %v", err)
					}
				}
			}
			b.StopTimer()
		})
	}
}

// BenchmarkReload measures full-dataset Load throughput over a
// 50k-key store. The callback consumes every record so the
// compiler cannot discard the scan.
func BenchmarkReload(b *testing.B) {
	dir := b.TempDir()
	o := spool.DefaultOptions(dir)
	o.MasterKey = testMasterKey
	st, err := spool.Open(o)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	const keys = 50000
	for i := 0; i < keys; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%08d", i)), incompressible(128, uint64(i))); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		b.Fatalf("Flush: %v", err)
	}
	if err := st.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var n int64
		if err := spool.Load(dir, testMasterKey, func(recs []spool.Record) error {
			for _, r := range recs {
				n += int64(len(r.Key) + len(r.Value))
			}
			return nil
		}); err != nil {
			b.Fatalf("Load: %v", err)
		}
		if n == 0 {
			b.Fatal("empty load")
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*keys)/b.Elapsed().Seconds(), "keys/s")
}

// BenchmarkCompaction measures a steady compaction cycle: each
// iteration writes one generation of 100 padded keys, supersedes
// 90 of them (leaving the old files ~10% live, under the 20%
// threshold), and reclaims until a rewrite pass completes.
func BenchmarkCompaction(b *testing.B) {
	dir := b.TempDir()
	o := testOptions(b, dir)
	o.MasterKey = testMasterKey
	o.MaxSegmentSize = 8192
	o.TargetBlockBytes = 2048
	o.MaxBlockBytes = 4096
	o.MaxRecordsPerBlock = 1000
	o.MaxKeySize = 64
	o.MaxValueSize = 2048
	st, err := spool.Open(o)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer st.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for k := 0; k < 100; k++ {
			key := []byte(fmt.Sprintf("i%06d/k%03d", i, k))
			if err := st.Put(key, incompressible(300, uint64(i*100+k))); err != nil {
				b.Fatalf("Put: %v", err)
			}
		}
		if err := st.Flush(); err != nil {
			b.Fatalf("Flush: %v", err)
		}
		for k := 0; k < 90; k++ {
			key := []byte(fmt.Sprintf("i%06d/k%03d", i, k))
			if err := st.Put(key, []byte("new")); err != nil {
				b.Fatalf("Put: %v", err)
			}
		}
		if err := st.Flush(); err != nil {
			b.Fatalf("Flush: %v", err)
		}
		before := st.Stats().Compactions
		for j := 0; j < 200 && st.Stats().Compactions == before; j++ {
			if err := st.Reclaim(); err != nil {
				b.Fatalf("Reclaim: %v", err)
			}
		}
		if st.Stats().Compactions == before {
			b.Fatalf("iteration %d compacted nothing in 200 passes", i)
		}
	}
	b.StopTimer()
}

// BenchmarkCheckpointCapture measures capture latency over a
// multi-segment store. Each capture seals one member, so later
// iterations link slightly more; the drift is noted, not hidden.
func BenchmarkCheckpointCapture(b *testing.B) {
	dir := b.TempDir()
	o := spool.DefaultOptions(dir)
	o.MasterKey = testMasterKey
	st, err := spool.Open(o)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer st.Close()
	for i := 0; i < 20000; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%08d", i)), incompressible(256, uint64(i))); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		b.Fatalf("Flush: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst := filepath.Join(dir, fmt.Sprintf("cp-%d", i))
		cp, err := st.Checkpoint(context.Background(), dst)
		if err != nil {
			b.Fatalf("Checkpoint: %v", err)
		}
		if err := cp.Release(); err != nil {
			b.Fatalf("Release: %v", err)
		}
		os.RemoveAll(dst)
	}
	b.StopTimer()
}

// BenchmarkCheckpointUnderWrite measures capture latency while
// writers hammer the live store, and reports the writers'
// sustained rate alongside as the interference signal.
func BenchmarkCheckpointUnderWrite(b *testing.B) {
	dir := b.TempDir()
	o := spool.DefaultOptions(dir)
	o.MasterKey = testMasterKey
	st, err := spool.Open(o)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer st.Close()
	// Writers overwrite a fixed key set so the run stays
	// memory-bounded: unique keys per put would grow the index
	// and pending buffers without bound and OOM the run.
	var stop atomic.Bool
	var puts atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var round int64
			val := make([]byte, 128)
			for !stop.Load() {
				round++
				for s := 0; s < 50; s++ {
					k := fmt.Sprintf("w%02d/s%05d", w, s)
					val[0] = byte(round)
					if err := st.Put([]byte(k), val); err != nil {
						return
					}
					puts.Add(1)
				}
			}
		}(w)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst := filepath.Join(dir, fmt.Sprintf("cp-%d", i))
		cp, err := st.Checkpoint(context.Background(), dst)
		if err != nil {
			stop.Store(true)
			b.Fatalf("Checkpoint: %v", err)
		}
		if err := cp.Release(); err != nil {
			stop.Store(true)
			b.Fatalf("Release: %v", err)
		}
		os.RemoveAll(dst)
	}
	b.StopTimer()
	stop.Store(true)
	wg.Wait()
	b.ReportMetric(float64(puts.Load())/b.Elapsed().Seconds(), "puts/s")
}

// BenchmarkRewrite measures full-store rewrite throughput over a
// ~20MB dataset. Rewrites are repeatable, so every iteration pays
// the full re-seal cost honestly.
func BenchmarkRewrite(b *testing.B) {
	dir := b.TempDir()
	o := spool.DefaultOptions(dir)
	o.MasterKey = testMasterKey
	st, err := spool.Open(o)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer st.Close()
	const keys = 20000
	for i := 0; i < keys; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%08d", i)), incompressible(1024, uint64(i))); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		b.Fatalf("Flush: %v", err)
	}
	var total int64
	for _, f := range st.FileStats() {
		total += int64(f.TotalBytes)
	}
	b.ResetTimer()
	b.SetBytes(total)
	for i := 0; i < b.N; i++ {
		if err := st.RewriteDataKeys(); err != nil {
			b.Fatalf("RewriteDataKeys: %v", err)
		}
	}
	b.StopTimer()
}

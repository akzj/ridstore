package ridstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var benchmarkValueSizes = []int{128, 4 << 10, 64 << 10}

func BenchmarkDurableCreate(b *testing.B) {
	for _, size := range benchmarkValueSizes {
		b.Run(fmt.Sprintf("value-%d", size), func(b *testing.B) {
			store := openBenchmarkStore(b, size)
			value := make([]byte, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					batch, err := store.Begin(context.Background())
					if err == nil {
						_, err = batch.Create(context.Background(), value)
					}
					if err == nil {
						_, err = batch.Commit(context.Background())
					}
					if err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

func BenchmarkDurableHotOverwrite(b *testing.B) {
	for _, size := range benchmarkValueSizes {
		b.Run(fmt.Sprintf("value-%d", size), func(b *testing.B) {
			store := openBenchmarkStore(b, size)
			value := make([]byte, size)
			seed, err := store.Begin(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			id, err := seed.Create(context.Background(), value)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := seed.Commit(context.Background()); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					batch, err := store.Begin(context.Background())
					if err == nil {
						err = batch.Put(context.Background(), id, value)
					}
					if err == nil {
						_, err = batch.Commit(context.Background())
					}
					if err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

// BenchmarkDurableMaintenanceInterference measures foreground tail latency
// while maintenance is deliberately run back-to-back. It is a same-machine
// diagnostic, not a production latency gate: filesystem and fsync latency are
// intentionally part of every sample.
func BenchmarkDurableMaintenanceInterference(b *testing.B) {
	for _, maintenance := range []string{"none", "checkpoint", "mapping-compact"} {
		b.Run(maintenance, func(b *testing.B) {
			runDurableMaintenanceInterference(b, maintenance, false)
		})
	}
}

// BenchmarkDurableDataCompactionInterference gives idle and aggressive Data GC
// the same sealed/dead/open-Batch fixture. Compare the two sub-benchmarks from
// one invocation; absolute filesystem latency is not portable across hosts.
func BenchmarkDurableDataCompactionInterference(b *testing.B) {
	for _, maintenance := range []string{"none", "data-compact"} {
		b.Run(maintenance, func(b *testing.B) {
			runDurableMaintenanceInterference(b, maintenance, true)
		})
	}
}

func runDurableMaintenanceInterference(b *testing.B, maintenance string, prepareDataCompaction bool) {
	b.Helper()
	store, dir := openMaintenanceBenchmarkStore(b)
	ids := seedMaintenanceBenchmark(b, store, 2800, 256)
	value := make([]byte, 256)
	var pending *Batch
	if prepareDataCompaction {
		pending = prepareDataCompactionBenchmark(b, store, ids, value)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var maintenanceCount atomic.Uint64
	startMaintenance := make(chan struct{})
	maintenanceDone := startBenchmarkMaintenance(ctx, startMaintenance, store, maintenance, &maintenanceCount)

	getLatency := make([]int64, b.N)
	putLatency := make([]int64, b.N)
	commitLatency := make([]int64, b.N)
	var sampleIndex atomic.Uint64
	var nextID atomic.Uint64
	var firstErr benchmarkFirstError

	b.SetBytes(int64(len(value)))
	b.ReportAllocs()
	b.ResetTimer()
	close(startMaintenance)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := ids[(nextID.Add(1)-1)%uint64(len(ids))]
			getStarted := time.Now()
			_, err := store.Get(ctx, id)
			getElapsed := time.Since(getStarted)
			if err != nil {
				firstErr.set(err)
				cancel()
				return
			}
			batch, err := store.Begin(ctx)
			if err != nil {
				firstErr.set(err)
				cancel()
				return
			}
			putStarted := time.Now()
			err = batch.Put(ctx, id, value)
			putElapsed := time.Since(putStarted)
			if err != nil {
				firstErr.set(err)
				cancel()
				return
			}
			commitStarted := time.Now()
			_, err = batch.Commit(ctx)
			commitElapsed := time.Since(commitStarted)
			if err != nil {
				firstErr.set(err)
				cancel()
				return
			}
			index := sampleIndex.Add(1) - 1
			getLatency[index] = getElapsed.Nanoseconds()
			putLatency[index] = putElapsed.Nanoseconds()
			commitLatency[index] = commitElapsed.Nanoseconds()
		}
	})
	b.StopTimer()
	cancel()
	if err := <-maintenanceDone; err != nil {
		b.Fatal(err)
	}
	if pending != nil {
		if err := pending.Abort(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	if err := firstErr.get(); err != nil {
		b.Fatal(err)
	}
	completed := int(sampleIndex.Load())
	reportLatencyDistribution(b, "get", getLatency[:completed])
	reportLatencyDistribution(b, "put", putLatency[:completed])
	reportLatencyDistribution(b, "commit", commitLatency[:completed])
	b.ReportMetric(float64(maintenanceCount.Load()), "maintenance-ops")
	metrics := store.Metrics()
	b.ReportMetric(float64(metrics.GCCompleted), "gc-completed")
	b.ReportMetric(float64(metrics.GCCommitRedirects), "gc-redirects")
	b.ReportMetric(float64(metrics.GCOpenRefsRedirected), "gc-redirected-refs")
	if metrics.GCCommitRedirects != 0 {
		b.ReportMetric(float64(metrics.GCCommitRedirectWaitNanos)/float64(metrics.GCCommitRedirects), "gc-redirect-wait-ns/op")
		b.ReportMetric(float64(metrics.GCCommitRedirectAdmissionNanos)/float64(metrics.GCCommitRedirects), "gc-redirect-admission-ns/op")
	}
	sealed, err := filepath.Glob(filepath.Join(dir, "records", "record-*.sealed"))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(sealed)), "data-rotations")
}

type benchmarkFirstError struct {
	mu  sync.Mutex
	err error
}

func (e *benchmarkFirstError) set(err error) {
	e.mu.Lock()
	if e.err == nil {
		e.err = err
	}
	e.mu.Unlock()
}

func (e *benchmarkFirstError) get() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

func startBenchmarkMaintenance(ctx context.Context, start <-chan struct{}, store *Store, kind string, count *atomic.Uint64) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer close(done)
		select {
		case <-start:
		case <-ctx.Done():
			done <- nil
			return
		}
		if kind == "none" {
			done <- nil
			return
		}
		for ctx.Err() == nil {
			var err error
			switch kind {
			case "checkpoint":
				err = store.Checkpoint(ctx)
			case "mapping-compact":
				err = store.CompactMapping(ctx)
			case "data-compact":
				// Cancellation stops admission of the next maintenance operation.
				// Do not interrupt a compaction after it has crossed a durable
				// publication boundary merely because the benchmark timer ended.
				_, _, err = store.CompactNextSegment(context.Background(), CompactionPolicy{BypassCooldown: true})
			default:
				err = fmt.Errorf("unknown benchmark maintenance %q", kind)
			}
			if err != nil {
				if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
					done <- nil
					return
				}
				done <- err
				return
			}
			count.Add(1)
		}
		done <- nil
	}()
	return done
}

func reportLatencyDistribution(b *testing.B, operation string, samples []int64) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	for _, percentile := range []struct {
		name        string
		numerator   int
		denominator int
	}{
		{name: "p50", numerator: 50, denominator: 100},
		{name: "p99", numerator: 99, denominator: 100},
		{name: "p999", numerator: 999, denominator: 1000},
	} {
		index := (len(samples)*percentile.numerator + percentile.denominator - 1) / percentile.denominator
		if index > 0 {
			index--
		}
		b.ReportMetric(float64(samples[index]), operation+"-"+percentile.name+"-ns")
	}
	b.ReportMetric(float64(samples[len(samples)-1]), operation+"-max-ns")
}

func openMaintenanceBenchmarkStore(b *testing.B) (*Store, string) {
	b.Helper()
	dir := filepath.Join(b.TempDir(), "store")
	config := CreateConfig{
		Dir: dir,
		HardLimits: HardLimits{
			SegmentSize: 1 << 20, MaxValueSize: 256, MaxBatchBytes: 1 << 20,
			MaxBatchMutations: 4096, MaxBatchConditions: 4096, MaxOpenBatches: 1024,
			MaxRecordLogPayload: 256 << 10, IDReserveSize: 1 << 16, BatchIDReserveSize: 1 << 16,
		},
		Runtime: RuntimeConfig{
			MaxQueuedBytes: 64 << 20, AppendQueueCapacity: 4096, AppendBufferBytes: 8 << 20,
			AppendBufferRecords: 4096, CommitQueueCapacity: 4096, MaxGroupBatches: 64,
			MaxGroupPayload: 256 << 10, MappingCacheBytes: 64 << 20,
			CheckpointSortBytes: 64 << 20, MaxSegmentStats: 1 << 16,
			DeltaSoftLimitBytes: 64 << 20, DeltaHardLimitBytes: 128 << 20,
			StatusRetention: 1 << 16, WriteStopFreeBytes: 1, SpaceCheckInterval: time.Second,
			GCBytesPerSecond: ^uint64(0),
		},
	}
	store, err := Create(context.Background(), config)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Error(err)
		}
	})
	return store, dir
}

func seedMaintenanceBenchmark(b *testing.B, store *Store, records, valueSize int) []ID {
	b.Helper()
	batch, err := store.Begin(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	value := make([]byte, valueSize)
	ids := make([]ID, 0, records)
	for range records {
		id, err := batch.Create(context.Background(), value)
		if err != nil {
			b.Fatal(err)
		}
		ids = append(ids, id)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		b.Fatal(err)
	}
	if err := store.Checkpoint(context.Background()); err != nil {
		b.Fatal(err)
	}
	return ids
}

func prepareDataCompactionBenchmark(b *testing.B, store *Store, ids []ID, value []byte) *Batch {
	b.Helper()
	pending, err := store.Begin(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	// These uncommitted records are appended before the overwrite run forces a
	// rotation, making Open Batch redirect deterministic for the first GC.
	for range 128 {
		if _, err := pending.Create(context.Background(), value); err != nil {
			b.Fatal(err)
		}
	}
	overwrite, err := store.Begin(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	for _, id := range ids {
		if err := overwrite.Put(context.Background(), id, value); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := overwrite.Commit(context.Background()); err != nil {
		b.Fatal(err)
	}
	if err := store.Checkpoint(context.Background()); err != nil {
		b.Fatal(err)
	}
	return pending
}

// BenchmarkDurableAppendBaseline is a lower-bound reference, not a competing
// record store: every operation appends one value to a regular file and calls
// fsync, but it provides no framing, recovery, indexing, or atomic batches.
func BenchmarkDurableAppendBaseline(b *testing.B) {
	for _, size := range benchmarkValueSizes {
		b.Run(fmt.Sprintf("value-%d", size), func(b *testing.B) {
			file, err := os.OpenFile(filepath.Join(b.TempDir(), "append.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := file.Close(); err != nil {
					b.Error(err)
				}
			})
			value := make([]byte, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := file.Write(value); err != nil {
						b.Error(err)
						return
					}
					if err := file.Sync(); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

func openBenchmarkStore(b *testing.B, valueSize int) *Store {
	b.Helper()
	config := CreateConfig{
		Dir: filepath.Join(b.TempDir(), "store"),
		HardLimits: HardLimits{
			SegmentSize: 256 << 20, MaxValueSize: uint64(valueSize), MaxBatchBytes: 1 << 20,
			MaxBatchMutations: 1024, MaxBatchConditions: 1024, MaxOpenBatches: 1024,
			MaxRecordLogPayload: 4 << 20, IDReserveSize: 1 << 16, BatchIDReserveSize: 1 << 16,
		},
		Runtime: RuntimeConfig{
			MaxQueuedBytes: 64 << 20, AppendQueueCapacity: 4096, AppendBufferBytes: 8 << 20,
			AppendBufferRecords: 4096, CommitQueueCapacity: 4096, MaxGroupBatches: 64,
			MaxGroupPayload: 4 << 20, MappingCacheBytes: 64 << 20,
			CheckpointSortBytes: 64 << 20, MaxSegmentStats: 1 << 16,
			DeltaSoftLimitBytes: 64 << 20, DeltaHardLimitBytes: 128 << 20,
			StatusRetention: 1 << 16, WriteStopFreeBytes: 1, SpaceCheckInterval: time.Second,
			GCBytesPerSecond: ^uint64(0),
		},
	}
	store, err := Create(context.Background(), config)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Error(err)
		}
	})
	return store
}

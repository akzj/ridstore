package ridstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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

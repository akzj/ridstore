package v2

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkPageCommit(b *testing.B) {
	for _, payloadSize := range []int{128, 4096, 128 << 10} {
		b.Run(fmt.Sprintf("payload-%d", payloadSize), func(b *testing.B) {
			cfg := DefaultConfig(b.TempDir())
			cfg.SegmentSize = 256 << 20
			cfg.MaxPayloadSize = 1 << 20
			cfg.MaxBufferBytes = 4 << 20
			cfg.MaxQueuedBytes = 16 << 20
			log, err := Open(cfg)
			if err != nil {
				b.Fatal(err)
			}
			payload := make([]byte, payloadSize)
			commit := []byte("commit")
			const pagesPerCommit = 32
			b.SetBytes(int64(payloadSize * pagesPerCommit))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for page := 0; page < pagesPerCommit; page++ {
					if _, err := log.Append(context.Background(), payload, false); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := log.Append(context.Background(), commit, true); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			status := log.Status()
			if b.N != 0 {
				b.ReportMetric(float64(status.WriteCalls)/float64(b.N), "writes/txn")
				b.ReportMetric(float64(status.SyncCalls)/float64(b.N), "syncs/txn")
			}
			if err := log.Close(); err != nil {
				b.Fatal(err)
			}
		})
	}
}

func BenchmarkConcurrentSync(b *testing.B) {
	cfg := DefaultConfig(b.TempDir())
	cfg.SegmentSize = 256 << 20
	log, err := Open(cfg)
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 128)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := log.Append(context.Background(), payload, true); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	status := log.Status()
	if b.N != 0 {
		b.ReportMetric(float64(status.WriteCalls)/float64(b.N), "writes/op")
		b.ReportMetric(float64(status.SyncCalls)/float64(b.N), "syncs/op")
	}
	if err := log.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkEncodeRecord(b *testing.B) {
	for _, payloadSize := range []int{128, 4096, 128 << 10} {
		b.Run(fmt.Sprintf("payload-%d", payloadSize), func(b *testing.B) {
			payload := make([]byte, payloadSize)
			physicalSize, _ := encodedRecordSize(uint64(len(payload)))
			addr, _ := makeVAddr(1, segmentHeaderSize, physicalSize)
			b.SetBytes(int64(payloadSize))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := encodeRecord(addr, payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRead(b *testing.B) {
	for _, payloadSize := range []int{32, 1024, 5000, 128 << 10} {
		b.Run(fmt.Sprintf("payload-%d", payloadSize), func(b *testing.B) {
			cfg := DefaultConfig(b.TempDir())
			cfg.SegmentSize = 256 << 20
			payload := make([]byte, payloadSize)
			addr, err := func() (VAddr, error) {
				log, err := Open(cfg)
				if err != nil {
					return 0, err
				}
				defer log.Close()
				return log.Append(context.Background(), payload, true)
			}()
			if err != nil {
				b.Fatal(err)
			}
			log, err := Open(cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer log.Close()
			b.SetBytes(int64(payloadSize))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := log.Read(context.Background(), addr)
				if err != nil || len(got) != payloadSize {
					b.Fatalf("read = %d bytes, %v", len(got), err)
				}
			}
		})
	}
}

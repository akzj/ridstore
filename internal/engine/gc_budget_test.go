package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

func TestGCPacerUsesCumulativeCopyRate(t *testing.T) {
	now := time.Unix(100, 0)
	var delays []time.Duration
	pacer := gcPacer{
		started: now, rate: 100,
		now: func() time.Time { return now },
		wait: func(_ context.Context, delay time.Duration) error {
			delays = append(delays, delay)
			now = now.Add(delay)
			return nil
		},
	}
	if waited, err := pacer.pace(context.Background(), 25); err != nil || waited != 250*time.Millisecond {
		t.Fatalf("first wait=%v err=%v", waited, err)
	}
	now = now.Add(100 * time.Millisecond)
	if waited, err := pacer.pace(context.Background(), 25); err != nil || waited != 150*time.Millisecond {
		t.Fatalf("second wait=%v err=%v", waited, err)
	}
	if len(delays) != 2 || delays[0] != 250*time.Millisecond || delays[1] != 150*time.Millisecond {
		t.Fatalf("delays=%v", delays)
	}
}

func TestGCPacerPropagatesCancellation(t *testing.T) {
	now := time.Unix(100, 0)
	pacer := gcPacer{
		started: now, rate: 1,
		now:  func() time.Time { return now },
		wait: func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pacer.pace(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("pace err=%v", err)
	}
}

func TestGCCopySpaceAdmissionUsesConservativeUpperBound(t *testing.T) {
	const minimum = uint64(10)
	source := recordlog.SegmentSummary{SegmentID: 1, ValidEnd: recordlog.SegmentHeaderSize + 50, RecordCount: 2}
	manifest := storecatalog.Manifest{HardLimits: storecatalog.HardLimits{SegmentSize: 100}}
	commitPhysical, err := recordlog.PhysicalRecordSize(uint64(recordcodec.CommitGroupHeadSize + recordcodec.DescriptorHeadSize + recordcodec.MutationSize))
	if err != nil {
		t.Fatal(err)
	}
	reservePhysical, err := recordlog.PhysicalRecordSize(uint64(recordcodec.FixedRecordSize))
	if err != nil {
		t.Fatal(err)
	}
	want := uint64(50) + uint64(source.RecordCount)*(uint64(commitPhysical)+uint64(reservePhysical)) + 200
	free := want + minimum
	gate := newSpaceGate("test", 100, time.Second, func(string) (uint64, error) { return free, nil })
	store := &Store{space: gate, gcMinFreeBytes: minimum, maxRelocationBytes: 1, maxRelocationMutations: 1}
	reservation, err := store.reserveGCCopy(context.Background(), manifest, source)
	if err != nil {
		t.Fatal(err)
	}
	if reservation.bytes != want {
		t.Fatalf("reserved=%d want=%d", reservation.bytes, want)
	}
	reservation.complete(false)
	free--
	if _, err := store.reserveGCCopy(context.Background(), manifest, source); !errors.Is(err, base.ErrInsufficientSpace) {
		t.Fatalf("reserve err=%v", err)
	}
	if got := store.metrics.gcSpaceRejections.Load(); got != 1 {
		t.Fatalf("rejections=%d", got)
	}
}

func TestGCCheckpointSpaceAdmissionUsesDensePathUpperBound(t *testing.T) {
	const minimum = uint64(10)
	manifest := storecatalog.Manifest{HardLimits: storecatalog.HardLimits{SegmentSize: 100}}
	want := uint64(2)*(uint64(mapstore.MaxLevel)+1)*uint64(mapstore.DenseNodeSize) + 100
	gate := newSpaceGate("test", 100, time.Second, func(string) (uint64, error) { return want + minimum, nil })
	store := &Store{space: gate, gcMinFreeBytes: minimum}
	reservation, err := store.reserveGCCheckpoint(context.Background(), manifest, 2)
	if err != nil {
		t.Fatal(err)
	}
	if reservation.bytes != want {
		t.Fatalf("reserved=%d want=%d", reservation.bytes, want)
	}
	reservation.complete(false)
}

func TestGCSpaceAdmissionRejectsOverflow(t *testing.T) {
	gate := newSpaceGate("test", 1, time.Second, func(string) (uint64, error) { return ^uint64(0), nil })
	store := &Store{space: gate, gcMinFreeBytes: 1, maxRelocationBytes: 1, maxRelocationMutations: 1}
	manifest := storecatalog.Manifest{HardLimits: storecatalog.HardLimits{SegmentSize: 100}}
	source := recordlog.SegmentSummary{SegmentID: 1, ValidEnd: recordlog.SegmentHeaderSize, RecordCount: ^uint64(0)}
	if _, err := store.reserveGCCopy(context.Background(), manifest, source); !errors.Is(err, base.ErrOverflow) {
		t.Fatalf("copy reserve err=%v", err)
	}
	if _, err := store.reserveGCCheckpoint(context.Background(), manifest, ^uint64(0)); !errors.Is(err, base.ErrOverflow) {
		t.Fatalf("checkpoint reserve err=%v", err)
	}
}

func TestNormalizeGCRuntimeDefaultsAndRejectsIncompatibleLimits(t *testing.T) {
	created := testCreateConfig()
	hard := created.HardLimits
	created.Runtime.WriteStopFreeBytes = hard.SegmentSize
	config := normalizeGCRuntime(created.Runtime, hard)
	if config.GCBatchBytes != hard.MaxBatchBytes || config.GCBatchMutations != hard.MaxBatchMutations ||
		config.GCMinFreeBytes != hard.SegmentSize || config.GCBytesPerSecond == 0 {
		t.Fatalf("config=%+v", config)
	}
	config.WriteStopFreeBytes = 100
	config.GCMinFreeBytes = 101
	if err := validateRuntimeAgainstHard(config, hard); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("validate err=%v", err)
	}
}

package verifier_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/verifier"
)

func TestVerifyPhysicalUsesReadOnlyExclusiveLease(t *testing.T) {
	ctx := context.Background()
	config := verifyCreateConfig(filepath.Join(t.TempDir(), "store"))
	store, err := ridstore.Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.VerifyPhysical(ctx, config.Dir); !errors.Is(err, base.ErrLocked) {
		t.Fatalf("verify open store err=%v", err)
	}
	batch, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Create(ctx, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	report, err := verifier.VerifyPhysical(ctx, config.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if report.Stage != verifier.StagePhysical || report.ManifestGeneration == 0 || report.Data.Records < 2 || report.Mapping.Segments != 1 {
		t.Fatalf("report=%+v", report)
	}
}

func TestVerifyPhysicalReportsRecoveryWithoutChangingTail(t *testing.T) {
	ctx := context.Background()
	config := verifyCreateConfig(filepath.Join(t.TempDir(), "store"))
	store, err := ridstore.Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	active := filepath.Join(config.Dir, "records", "record-0000000001.active")
	file, err := os.OpenFile(active, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(active)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.VerifyPhysical(ctx, config.Dir); !errors.Is(err, base.ErrRecoveryRequired) {
		t.Fatalf("verify err=%v", err)
	}
	after, err := os.Stat(active)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() != after.Size() {
		t.Fatalf("verify changed active size from %d to %d", before.Size(), after.Size())
	}
}

func verifyCreateConfig(dir string) ridstore.CreateConfig {
	return ridstore.CreateConfig{
		Dir: dir,
		HardLimits: ridstore.HardLimits{
			SegmentSize: 1 << 20, MaxValueSize: 1024, MaxBatchBytes: 4096,
			MaxBatchMutations: 16, MaxBatchConditions: 16, MaxOpenBatches: 4,
			MaxRecordLogPayload: 64 << 10, IDReserveSize: 16, BatchIDReserveSize: 16,
		},
		Runtime: ridstore.RuntimeConfig{
			MaxQueuedBytes: 1 << 20, AppendQueueCapacity: 32, AppendBufferBytes: 64 << 10,
			AppendBufferRecords: 32, CommitQueueCapacity: 16, MaxGroupBatches: 8,
			MaxGroupPayload: 64 << 10, MappingCacheBytes: 1 << 20,
			CheckpointSortBytes: 16 << 10, MaxSegmentStats: 1024,
			DeltaSoftLimitBytes: 32 << 10, DeltaHardLimitBytes: 64 << 10,
			WriteStopFreeBytes: 1,
		},
	}
}

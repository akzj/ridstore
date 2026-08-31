package verifier_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapgcstate"
	"github.com/akzj/ridstore/internal/storecatalog"
	"github.com/akzj/ridstore/internal/verifier"
)

func TestVerifyPhysicalUsesReadOnlyExclusiveLease(t *testing.T) {
	ctx := context.Background()
	config := verifyCreateConfig(filepath.Join(t.TempDir(), "store"))
	store, err := ridstore.Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	verifyConfig := verifier.Config{MappingCacheBytes: 1 << 20, MaxLiveIDs: 1024, MaxReplayStatuses: 1024}
	if _, err := verifier.Verify(ctx, config.Dir, verifyConfig); !errors.Is(err, base.ErrLocked) {
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
	if err := store.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	report, err := verifier.Verify(ctx, config.Dir, verifyConfig)
	if err != nil {
		t.Fatal(err)
	}
	if report.Stage != verifier.StageExact || report.ManifestGeneration == 0 || report.Data.Records < 2 || report.Mapping.Segments != 1 || report.CheckpointLiveIDs != 1 || report.LiveIDs != 1 || report.NextCommitSeq != 2 || report.VerifiedPuts != 1 {
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
	if _, err := verifier.Verify(ctx, config.Dir, verifier.Config{MappingCacheBytes: 1 << 20, MaxLiveIDs: 1024, MaxReplayStatuses: 1024}); !errors.Is(err, base.ErrRecoveryRequired) {
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

func TestVerifyRejectsMappingGCMarker(t *testing.T) {
	ctx := context.Background()
	config := verifyCreateConfig(filepath.Join(t.TempDir(), "store"))
	store, err := ridstore.Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(mapgcstate.Path(config.Dir)), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(mapgcstate.Path(config.Dir), []byte("incomplete"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.Verify(ctx, config.Dir, verifier.Config{
		MappingCacheBytes: 1 << 20, MaxLiveIDs: 1024, MaxReplayStatuses: 1024,
	}); !errors.Is(err, base.ErrRecoveryRequired) {
		t.Fatalf("verify err=%v", err)
	}
}

func TestVerifyBoundsReachableMappingState(t *testing.T) {
	ctx := context.Background()
	config := verifyCreateConfig(filepath.Join(t.TempDir(), "store"))
	store, err := ridstore.Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Create(ctx, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Create(ctx, []byte("two")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.Verify(ctx, config.Dir, verifier.Config{MappingCacheBytes: 1 << 20, MaxLiveIDs: 1, MaxReplayStatuses: 1024}); !errors.Is(err, verifier.ErrLimit) {
		t.Fatalf("verify err=%v", err)
	}
}

func TestVerifyReplaysDurableTailFromCheckpoint(t *testing.T) {
	ctx := context.Background()
	config := verifyCreateConfig(filepath.Join(t.TempDir(), "store"))
	store, err := ridstore.Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Create(ctx, []byte("checkpoint")); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Create(ctx, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	third, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := third.Create(ctx, []byte("tail-two")); err != nil {
		t.Fatal(err)
	}
	if _, err := third.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	report, err := verifier.Verify(ctx, config.Dir, verifier.Config{
		MappingCacheBytes: 1 << 20, MaxLiveIDs: 1024, MaxReplayStatuses: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Stage != verifier.StageExact || report.CheckpointLiveIDs != 1 || report.LiveIDs != 3 || report.ReplayedCommits != 2 || report.BatchStatuses != 2 || report.NextCommitSeq != 4 || report.VerifiedPuts != 3 {
		t.Fatalf("report=%+v", report)
	}
	if _, err := verifier.Verify(ctx, config.Dir, verifier.Config{
		MappingCacheBytes: 1 << 20, MaxLiveIDs: 1, MaxReplayStatuses: 1024,
	}); !errors.Is(err, verifier.ErrLimit) || errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("bounded replay err=%v", err)
	}
	if _, err := verifier.Verify(ctx, config.Dir, verifier.Config{
		MappingCacheBytes: 1 << 20, MaxLiveIDs: 1024, MaxReplayStatuses: 1,
	}); !errors.Is(err, base.ErrStatusCapacity) {
		t.Fatalf("bounded status err=%v", err)
	}
}

func TestVerifyRejectsCheckpointSegmentStatsMismatch(t *testing.T) {
	ctx := context.Background()
	config := verifyCreateConfig(filepath.Join(t.TempDir(), "store"))
	config.HardLimits.SegmentSize = 8192
	config.HardLimits.MaxRecordLogPayload = 4096
	config.Runtime.MaxGroupPayload = 4096
	store, err := ridstore.Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		batch, err := store.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Create(ctx, make([]byte, 1024)); err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	manager, err := storecatalog.OpenManager(config.Dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	manifest := manager.Snapshot()
	if len(manifest.SegmentStats) == 0 || manifest.SegmentStats[0].LiveBytes == 0 {
		t.Fatalf("stats=%+v", manifest.SegmentStats)
	}
	checkpointBase := manifest.Clone()
	manifest.SegmentStats[0].LiveBytes--
	if _, err := manager.InstallCheckpoint(checkpointBase, storecatalog.Checkpoint{
		MappingRoot: manifest.MappingRoot, MappingEntryCount: manifest.MappingEntryCount, CoveredCommitSeq: manifest.CoveredCommitSeq, ReplayStart: manifest.ReplayStart,
		ReservedIDHigh: manifest.ReservedIDHigh, ReservedBatchIDHigh: manifest.ReservedBatchIDHigh,
		IssuedBatchIDHighAtCut: manifest.IssuedBatchIDHighAtCut, OpenBatchIDsAtCut: manifest.OpenBatchIDsAtCut,
		StatsCoveredCommitSeq: manifest.StatsCoveredCommitSeq, SegmentStats: manifest.SegmentStats,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.Verify(ctx, config.Dir, verifier.Config{
		MappingCacheBytes: 1 << 20, MaxLiveIDs: 1024, MaxReplayStatuses: 1024,
	}); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("verify err=%v", err)
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
			CheckpointSortBytes: 24 << 10, MaxSegmentStats: 1024,
			DeltaSoftLimitBytes: 32 << 10, DeltaHardLimitBytes: 64 << 10,
			WriteStopFreeBytes: 1,
		},
	}
}

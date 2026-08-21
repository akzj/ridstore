package verify_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/manifest"
	"github.com/akzj/ridstore/internal/segment"
	"github.com/akzj/ridstore/internal/verify"
)

func TestVerifyReplaysPostCheckpointCommitsAndReportsDeadRecords(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := ridstore.Create(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	batch, _ := store.Begin(ctx)
	id, _ := batch.Allocate(ctx)
	if err := batch.Put(ctx, id, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	batch, _ = store.Begin(ctx)
	if err := batch.Put(ctx, id, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := verify.Run(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Clean || report.MappingEntries != 1 || report.LiveRecords != 1 || report.DeadRecords == 0 || report.CurrentCommitSeq != 2 {
		t.Fatalf("report=%+v", report)
	}
}

func TestVerifyRefusesOpenStore(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := ridstore.Create(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := verify.Run(context.Background(), dir); !errors.Is(err, base.ErrLocked) {
		t.Fatalf("error=%v", err)
	}
}

func TestVerifyStatusReplayLimitIsExplicit(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := ridstore.Create(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		batch, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Abort(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := verify.RunWithOptions(context.Background(), dir, verify.Options{StatusLimit: 1})
	if !errors.Is(err, base.ErrStatusCapacity) || report.Clean || len(report.Issues) == 0 {
		t.Fatalf("report=%+v error=%v", report, err)
	}
	if report, err = verify.RunWithOptions(context.Background(), dir, verify.Options{StatusLimit: 2}); err != nil || !report.Clean {
		t.Fatalf("report=%+v error=%v", report, err)
	}
}

func TestVerifyMissingLockDoesNotCreateIt(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := ridstore.Create(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "LOCK")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := verify.Run(context.Background(), dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("error=%v", err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("verify created missing LOCK: %v", err)
	}
}

func TestVerifyRefusesRecoveryArtifactWithoutMutation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := ridstore.Create(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "journal", "MAINTENANCE")
	if err := os.WriteFile(path, []byte("incomplete"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := verify.Run(context.Background(), dir); !errors.Is(err, base.ErrRecoveryRequired) {
		t.Fatalf("error=%v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "incomplete" {
		t.Fatalf("artifact=%q error=%v", data, err)
	}
}

func TestVerifyRejectsAndPreservesPartialActiveDataTail(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := ridstore.Create(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := manifest.LoadCurrent(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "data", segment.ActiveDataFileName(m.ActiveDataSegmentID))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(path)
	if _, err := verify.Run(context.Background(), dir); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("error=%v", err)
	}
	after, _ := os.Stat(path)
	if before.Size() != after.Size() {
		t.Fatalf("verify changed active size from %d to %d", before.Size(), after.Size())
	}
}

func testConfig(dir string) ridstore.Config {
	return ridstore.Config{
		Dir: dir, SegmentSize: 16 << 10, MaxValueSize: 1024, MaxBatchBytes: 4096,
		MaxBatchMutations: 16, MaxBatchConditions: 16, MaxOpenBatches: 16,
		IDReserveSize: 16, BatchIDReserveSize: 16,
		MappingCacheBytes: 64 << 10, DeltaSoftLimitBytes: 64 << 10, DeltaHardLimitBytes: 128 << 10,
		CheckpointMemoryBytes: 64 << 10, MaxGroupBytes: 4096, MaxGroupBatches: 4,
		GCBatchBytes: 4096, GCBatchMutations: 16,
	}
}

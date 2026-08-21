package integration_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore"
	"github.com/akzj/ridstore/internal/backup"
	"github.com/akzj/ridstore/internal/verify"
)

func TestCheckpointVerifyBackupRestoreLifecycle(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	artifact := filepath.Join(root, "backup")
	restored := filepath.Join(root, "restored")
	cfg := testConfig(source)

	store, err := ridstore.Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := first.Allocate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Put(ctx, id, []byte("before-checkpoint")); err != nil {
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
	deletedID, err := second.Allocate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Put(ctx, id, []byte("after-checkpoint")); err != nil {
		t.Fatal(err)
	}
	if err := second.Put(ctx, deletedID, []byte("temporary")); err != nil {
		t.Fatal(err)
	}
	secondCommit, err := second.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	third, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := third.Delete(ctx, deletedID); err != nil {
		t.Fatal(err)
	}
	if _, err := third.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	assertVerified(t, ctx, source)
	created, err := backup.Create(ctx, source, artifact)
	if err != nil {
		t.Fatal(err)
	}
	if created.Files == 0 || created.Bytes == 0 || created.StoreUUID == "" {
		t.Fatalf("backup report=%+v", created)
	}
	restoredReport, err := backup.Restore(ctx, artifact, restored, backup.RestoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if restoredReport.SourceStoreUUID == restoredReport.RestoredStoreUUID || restoredReport.PreservedUUID {
		t.Fatalf("restore UUID policy=%+v", restoredReport)
	}
	assertVerified(t, ctx, restored)

	restoredStore, err := ridstore.Open(testConfig(restored))
	if err != nil {
		t.Fatal(err)
	}
	record, err := restoredStore.GetRecord(ctx, id)
	if err != nil || string(record.Value) != "after-checkpoint" || record.Revision != ridstore.Revision(secondCommit.BatchID) {
		t.Fatalf("restored record=%+v commit=%+v error=%v", record, secondCommit, err)
	}
	if _, err := restoredStore.Get(ctx, deletedID); !errors.Is(err, ridstore.ErrNotFound) {
		t.Fatalf("restored deleted record error=%v", err)
	}
	if err := restoredStore.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertVerified(t *testing.T, ctx context.Context, root string) {
	t.Helper()
	report, err := verify.Run(ctx, root)
	if err != nil || !report.Clean || len(report.Issues) != 0 {
		t.Fatalf("verify report=%+v error=%v", report, err)
	}
}

func testConfig(dir string) ridstore.Config {
	return ridstore.Config{
		Dir: dir, SegmentSize: 1 << 20, MaxValueSize: 64 << 10, MaxBatchBytes: 256 << 10,
		MaxBatchMutations: 128, MaxBatchConditions: 128, MaxOpenBatches: 16,
		IDReserveSize: 16, BatchIDReserveSize: 16,
		MappingCacheBytes: 1 << 20, DeltaSoftLimitBytes: 1 << 20, DeltaHardLimitBytes: 2 << 20,
		CheckpointMemoryBytes: 1 << 20, MaxGroupBytes: 256 << 10, MaxGroupBatches: 16,
		GCBatchBytes: 256 << 10, GCBatchMutations: 128, GCMinFreeBytes: 0, GCBytesPerSecond: 1 << 20,
	}
}

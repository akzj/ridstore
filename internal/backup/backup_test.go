package backup_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore"
	"github.com/akzj/ridstore/internal/backup"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/initialize"
	"github.com/akzj/ridstore/internal/verify"
)

func TestCreateAndInspectConsistentBackup(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "source")
	store, err := ridstore.Create(testConfig(source))
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.Allocate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Put(ctx, id, []byte("backup-value")); err != nil {
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
	destination := filepath.Join(t.TempDir(), "backup")
	report, err := backup.Create(ctx, source, destination)
	if err != nil {
		t.Fatal(err)
	}
	if report.Destination != destination || report.StoreUUID == "" || report.ManifestGeneration < 2 || report.Files < 4 || report.Bytes == 0 {
		t.Fatalf("report=%+v", report)
	}
	metadata, err := backup.Inspect(ctx, destination)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.StoreUUID != report.StoreUUID || metadata.ManifestGeneration != report.ManifestGeneration || uint64(len(metadata.Files)) != report.Files {
		t.Fatalf("metadata=%+v report=%+v", metadata, report)
	}
	if _, err := os.Lstat(filepath.Join(destination, "INCOMPLETE")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("published artifact retains INCOMPLETE: %v", err)
	}
}

func TestCreateRefusesLiveStoreBeforeCreatingDestination(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source")
	store, err := ridstore.Create(testConfig(source))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	destination := filepath.Join(t.TempDir(), "backup")
	if _, err := backup.Create(context.Background(), source, destination); !errors.Is(err, base.ErrLocked) {
		t.Fatalf("error=%v", err)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination created despite lock failure: %v", err)
	}
}

func TestInspectRejectsIncompleteAndTamperedArtifact(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "source")
	store, err := ridstore.Create(testConfig(source))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "backup")
	if _, err := backup.Create(ctx, source, destination); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(destination, "INCOMPLETE"), []byte("incomplete"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := backup.Inspect(ctx, destination); !errors.Is(err, base.ErrRecoveryRequired) {
		t.Fatalf("incomplete error=%v", err)
	}
	if err := os.Remove(filepath.Join(destination, "INCOMPLETE")); err != nil {
		t.Fatal(err)
	}
	metadata, err := backup.Inspect(ctx, destination)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(destination, "files", metadata.Files[len(metadata.Files)-1].Path)
	file, err := os.OpenFile(target, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("tamper")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := backup.Inspect(ctx, destination); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("tamper error=%v", err)
	}
}

func TestCreateCanceledDoesNotCreateDestination(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source")
	store, err := ridstore.Create(testConfig(source))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "backup")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := backup.Create(ctx, source, destination); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination created after pre-cancel: %v", err)
	}
}

func TestCreateNeverOverwritesExistingDestination(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source")
	store, err := ridstore.Create(testConfig(source))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "backup")
	if err := os.Mkdir(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(destination, "sentinel")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := backup.Create(context.Background(), source, destination); !errors.Is(err, os.ErrExist) {
		t.Fatalf("error=%v", err)
	}
	data, err := os.ReadFile(sentinel)
	if err != nil || string(data) != "keep" {
		t.Fatalf("sentinel=%q error=%v", data, err)
	}
}

func TestRestoreCloneRewritesUUIDAndPreservesRecords(t *testing.T) {
	ctx := context.Background()
	source, artifact, id, record := createBackupWithRecord(t, ctx)
	_ = source
	destination := filepath.Join(t.TempDir(), "restored")
	report, err := backup.Restore(ctx, artifact, destination, backup.RestoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if report.SourceStoreUUID == "" || report.RestoredStoreUUID == "" || report.SourceStoreUUID == report.RestoredStoreUUID || report.PreservedUUID {
		t.Fatalf("report=%+v", report)
	}
	verified, err := verify.Run(ctx, destination)
	if err != nil || !verified.Clean || verified.StoreUUID != report.RestoredStoreUUID {
		t.Fatalf("verify=%+v error=%v", verified, err)
	}
	restored, err := ridstore.Open(testConfig(destination))
	if err != nil {
		t.Fatal(err)
	}
	got, err := restored.GetRecord(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Value) != string(record.Value) || got.Revision != record.Revision {
		t.Fatalf("record=%+v want=%+v", got, record)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreCanExplicitlyPreserveUUID(t *testing.T) {
	ctx := context.Background()
	_, artifact, _, _ := createBackupWithRecord(t, ctx)
	destination := filepath.Join(t.TempDir(), "replacement")
	report, err := backup.Restore(ctx, artifact, destination, backup.RestoreOptions{PreserveUUID: true})
	if err != nil {
		t.Fatal(err)
	}
	if !report.PreservedUUID || report.SourceStoreUUID != report.RestoredStoreUUID {
		t.Fatalf("report=%+v", report)
	}
}

func TestRestoringMarkerBlocksOpenAndPublicVerify(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := ridstore.Create(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, initialize.RestoringMarkerFileName)
	if err := os.WriteFile(marker, []byte("incomplete"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ridstore.Create(testConfig(dir)); !errors.Is(err, base.ErrRecoveryRequired) {
		t.Fatalf("create error=%v", err)
	}
	if _, err := ridstore.Open(ridstore.Config{Dir: dir}); !errors.Is(err, base.ErrRecoveryRequired) {
		t.Fatalf("open error=%v", err)
	}
	if _, err := verify.Run(context.Background(), dir); !errors.Is(err, base.ErrRecoveryRequired) {
		t.Fatalf("verify error=%v", err)
	}
}

func TestRestoreRejectsTamperedArtifactBeforeCreatingDestination(t *testing.T) {
	ctx := context.Background()
	_, artifact, _, _ := createBackupWithRecord(t, ctx)
	metadata, err := backup.Inspect(ctx, artifact)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(artifact, "files", metadata.Files[0].Path)
	file, err := os.OpenFile(target, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("tamper")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "restored")
	if _, err := backup.Restore(ctx, artifact, destination, backup.RestoreOptions{}); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("error=%v", err)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination created for invalid artifact: %v", err)
	}
}

func TestRestoringMarkerWithoutLockIsRejectedWithoutMutation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "partial-restore")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, initialize.RestoringMarkerFileName)
	if err := os.WriteFile(marker, []byte("incomplete"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ridstore.Create(testConfig(dir)); !errors.Is(err, base.ErrRecoveryRequired) {
		t.Fatalf("create error=%v", err)
	}
	if _, err := ridstore.Open(ridstore.Config{Dir: dir}); !errors.Is(err, base.ErrRecoveryRequired) {
		t.Fatalf("open error=%v", err)
	}
	if _, err := verify.Run(context.Background(), dir); !errors.Is(err, base.ErrRecoveryRequired) {
		t.Fatalf("verify error=%v", err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "LOCK")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("offline operation created LOCK: %v", err)
	}
}

func TestRestoreNeverOverwritesExistingDestination(t *testing.T) {
	ctx := context.Background()
	_, artifact, _, _ := createBackupWithRecord(t, ctx)
	destination := filepath.Join(t.TempDir(), "restored")
	if err := os.Mkdir(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(destination, "sentinel")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := backup.Restore(ctx, artifact, destination, backup.RestoreOptions{}); !errors.Is(err, os.ErrExist) {
		t.Fatalf("error=%v", err)
	}
	data, err := os.ReadFile(sentinel)
	if err != nil || string(data) != "keep" {
		t.Fatalf("sentinel=%q error=%v", data, err)
	}
}

func createBackupWithRecord(t *testing.T, ctx context.Context) (source, artifact string, id ridstore.ID, record ridstore.Record) {
	t.Helper()
	source = filepath.Join(t.TempDir(), "source")
	store, err := ridstore.Create(testConfig(source))
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err = batch.Allocate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Put(ctx, id, []byte("restored-value")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	record, err = store.GetRecord(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	artifact = filepath.Join(t.TempDir(), "backup")
	if _, err := backup.Create(ctx, source, artifact); err != nil {
		t.Fatal(err)
	}
	return source, artifact, id, record
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

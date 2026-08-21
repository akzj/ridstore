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

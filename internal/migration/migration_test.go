package migration_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/ridstore"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/migration"
	"github.com/akzj/ridstore/internal/storecatalog"
)

func TestRegistryBuildsExactForwardPath(t *testing.T) {
	v20, v21, v30 := migration.Version{Major: 2}, migration.Version{Major: 2, Minor: 1}, migration.Version{Major: 3}
	registry, err := migration.NewRegistry([]migration.Step{{Name: "minor", From: v20, To: v21}, {Name: "major", From: v21, To: v30}})
	if err != nil {
		t.Fatal(err)
	}
	path, ok := registry.Path(v20, v30)
	if !ok || len(path) != 2 || path[0].Name != "minor" || path[1].Name != "major" {
		t.Fatalf("path=%+v ok=%v", path, ok)
	}
	if _, ok := registry.Path(v30, v20); ok {
		t.Fatal("reverse path accepted")
	}
	if _, err := migration.NewRegistry([]migration.Step{{Name: "one", From: v20, To: v21}, {Name: "two", From: v20, To: v30}}); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("duplicate source error=%v", err)
	}
	if _, err := migration.NewRegistry([]migration.Step{{Name: "same", From: v20, To: v21}, {Name: "same", From: v21, To: v30}}); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("duplicate name error=%v", err)
	}
}

func TestInspectCurrentStoreIsVerifiedAndReadOnly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := ridstore.Create(context.Background(), testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	before := directoryDigest(t, dir)
	plan, err := migration.Inspect(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if plan.MigrationRequired || !plan.Supported || !plan.VerifiedCurrent || plan.From != migration.CurrentVersion() || plan.To != migration.CurrentVersion() || plan.StoreUUID == "" || len(plan.Steps) != 0 {
		t.Fatalf("plan=%+v", plan)
	}
	if after := directoryDigest(t, dir); before != after {
		t.Fatalf("inspection changed directory\nbefore=%x\nafter=%x", before, after)
	}
}

func TestInspectReportsChecksummedUnknownVersionWithoutMutation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := ridstore.Create(context.Background(), testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "MANIFEST-v2-1")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint16(data[8:10], storecatalog.FormatMajor+1)
	binary.LittleEndian.PutUint32(data[52:56], crc32.Checksum(data[:52], crc32.MakeTable(crc32.Castagnoli)))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	before := directoryDigest(t, dir)
	plan, err := migration.Inspect(context.Background(), dir)
	if !errors.Is(err, ridstore.ErrUnsupported) || plan.Supported || !plan.MigrationRequired || plan.From.Major != storecatalog.FormatMajor+1 {
		t.Fatalf("plan=%+v error=%v", plan, err)
	}
	if after := directoryDigest(t, dir); before != after {
		t.Fatalf("inspection changed directory\nbefore=%x\nafter=%x", before, after)
	}
}

func TestInspectRequiresOfflineStore(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := ridstore.Create(context.Background(), testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := migration.Inspect(context.Background(), dir); !errors.Is(err, ridstore.ErrLocked) {
		t.Fatalf("error=%v", err)
	}
}

func directoryDigest(t *testing.T, root string) [32]byte {
	t.Helper()
	hash := sha256.New()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		hash.Write([]byte(relative))
		hash.Write([]byte{0})
		hash.Write([]byte(entry.Type().String()))
		hash.Write([]byte{0})
		if entry.Type().IsRegular() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			hash.Write(data)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func testConfig(dir string) ridstore.CreateConfig {
	return ridstore.CreateConfig{Dir: dir,
		HardLimits: ridstore.HardLimits{SegmentSize: 1 << 20, MaxValueSize: 1024, MaxBatchBytes: 4096, MaxBatchMutations: 16, MaxBatchConditions: 16, MaxOpenBatches: 4, MaxRecordLogPayload: 64 << 10, IDReserveSize: 16, BatchIDReserveSize: 16},
		Runtime:    ridstore.RuntimeConfig{MaxQueuedBytes: 1 << 20, AppendQueueCapacity: 32, AppendBufferBytes: 64 << 10, AppendBufferRecords: 32, CommitQueueCapacity: 16, MaxGroupBatches: 8, MaxGroupPayload: 64 << 10, MappingCacheBytes: 1 << 20, CheckpointSortBytes: 16 << 10, MaxSegmentStats: 1024, DeltaSoftLimitBytes: 32 << 10, DeltaHardLimitBytes: 64 << 10, StatusRetention: 16, WriteStopFreeBytes: 1, SpaceCheckInterval: time.Second}}
}

package migration_test

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore"
	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/manifest"
	"github.com/akzj/ridstore/internal/migration"
)

func TestRegistryBuildsExactPathAndRejectsGaps(t *testing.T) {
	v1, v2, v3 := migration.Version{Major: 1}, migration.Version{Major: 1, Minor: 1}, migration.Version{Major: 2}
	registry, err := migration.NewRegistry([]migration.Step{{Name: "one", From: v1, To: v2}, {Name: "two", From: v2, To: v3}})
	if err != nil {
		t.Fatal(err)
	}
	path, ok := registry.Path(v1, v3)
	if !ok || len(path) != 2 || path[0].Name != "one" || path[1].Name != "two" {
		t.Fatalf("path=%+v ok=%v", path, ok)
	}
	if _, ok := registry.Path(v3, v1); ok {
		t.Fatal("unexpected reverse path")
	}
	if _, err := migration.NewRegistry([]migration.Step{{Name: "one", From: v1, To: v2}, {Name: "duplicate", From: v1, To: v3}}); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("error=%v", err)
	}
}

func TestInspectCurrentStoreIsVerifiedAndReadOnly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := ridstore.Create(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := directoryState(dir)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := migration.Inspect(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if plan.MigrationRequired || !plan.Supported || !plan.VerifiedCurrent || plan.From != migration.CurrentVersion() || plan.To != migration.CurrentVersion() || plan.StoreUUID == "" {
		t.Fatalf("plan=%+v", plan)
	}
	after, err := directoryState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("migration inspection changed directory before=%q after=%q", before, after)
	}
}

func TestInspectReportsUnknownVersionWithoutMutation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := ridstore.Create(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	name, err := manifest.ReadCurrentName(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, manifest.ManifestDirName, name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint16(data[8:10], storeformat.FormatMajorVersion+1)
	binary.LittleEndian.PutUint32(data[52:56], 0)
	binary.LittleEndian.PutUint32(data[52:56], crc32.Checksum(data[:storeformat.ContainerHeaderSize], crc32.MakeTable(crc32.Castagnoli)))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := directoryState(dir)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := migration.Inspect(context.Background(), dir)
	if !errors.Is(err, base.ErrUnsupported) || plan.Supported || !plan.MigrationRequired || plan.From.Major != storeformat.FormatMajorVersion+1 {
		t.Fatalf("plan=%+v error=%v", plan, err)
	}
	after, stateErr := directoryState(dir)
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	if before != after {
		t.Fatalf("migration inspection changed unknown format before=%q after=%q", before, after)
	}
}

func directoryState(root string) (string, error) {
	var state string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, _ := filepath.Rel(root, path)
		state += relative + ":" + info.Mode().String() + ":" + info.ModTime().UTC().String() + "\n"
		return nil
	})
	return state, err
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

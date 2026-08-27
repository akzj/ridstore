package mapstore_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/radix"
	"github.com/akzj/ridstore/internal/recordlog"
)

func TestGenerationWriterBuildsIndependentVerifiedFileSet(t *testing.T) {
	root := t.TempDir()
	storeID := mapstore.StoreID{1}
	writer, err := mapstore.CreateGenerationWriter(root, storeID, 8192, 17, nil)
	if err != nil {
		t.Fatal(err)
	}
	builder, err := radix.NewRebuildBuilder(writer, 9, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	const entries = 600
	for index := 0; index < entries; index++ {
		id := model.ID(uint64(index)*512 + 1)
		addr, err := recordlog.NewVAddr(1, uint32(index+1)*64, 64)
		if err != nil {
			t.Fatal(err)
		}
		if err := builder.Add(id, addr); err != nil {
			t.Fatal(err)
		}
	}
	tree, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}
	generation, err := writer.Finish(tree.Root(), tree.Covered())
	if err != nil {
		t.Fatal(err)
	}
	if len(generation.SealedSegments) == 0 || generation.ActiveSegment <= 17 || generation.NextSegment != generation.ActiveSegment+1 || generation.Covered != 9 {
		t.Fatalf("generation=%+v", generation)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, report, err := mapstore.OpenVerifiedReader(context.Background(), root, mapstore.CatalogSnapshot{
		Generation: 1, StoreID: storeID, SegmentSize: 8192,
		ActiveSegment: generation.ActiveSegment, NextSegment: generation.NextSegment,
		SealedSegments: generation.SealedSegments, Root: generation.Root, Covered: generation.Covered,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if report.SealedSegments == 0 || report.Segments != report.SealedSegments+1 {
		t.Fatalf("report=%+v", report)
	}
	rebuilt, err := radix.OpenReadOnly(reader, generation.Root, generation.Covered, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	if err := rebuilt.Walk(context.Background(), func(model.ID, recordlog.VAddr) error {
		seen++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != entries {
		t.Fatalf("seen=%d want=%d", seen, entries)
	}
}

func TestGenerationWriterFinishesEmptyMapping(t *testing.T) {
	root := t.TempDir()
	writer, err := mapstore.CreateGenerationWriter(root, mapstore.StoreID{1}, 8192, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	builder, err := radix.NewRebuildBuilder(writer, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}
	generation, err := writer.Finish(tree.Root(), tree.Covered())
	if err != nil {
		t.Fatal(err)
	}
	if generation.Root != 0 || generation.ActiveSegment != 2 || generation.NextSegment != 3 || len(generation.SealedSegments) != 0 {
		t.Fatalf("generation=%+v", generation)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenVerifiedGenerationAllowsRetiringFilesToCoexist(t *testing.T) {
	root := t.TempDir()
	storeID := mapstore.StoreID{1}
	writer, err := mapstore.CreateGenerationWriter(root, storeID, 8192, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	builder, err := radix.NewRebuildBuilder(writer, 3, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	addr, _ := recordlog.NewVAddr(1, 64, 64)
	if err := builder.Add(1, addr); err != nil {
		t.Fatal(err)
	}
	tree, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}
	generation, err := writer.Finish(tree.Root(), tree.Covered())
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "mapping-v2", "map-00000001.active"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot := mapstore.CatalogSnapshot{
		Generation: 4, StoreID: storeID, SegmentSize: 8192,
		ActiveSegment: generation.ActiveSegment, NextSegment: generation.NextSegment,
		SealedSegments: generation.SealedSegments, Root: generation.Root, Covered: generation.Covered,
	}
	if _, _, err := mapstore.OpenVerifiedReader(context.Background(), root, snapshot); !errors.Is(err, mapstore.ErrCorrupt) {
		t.Fatalf("exact reader err=%v", err)
	}
	reader, _, err := mapstore.OpenVerifiedGeneration(context.Background(), root, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPromoteGenerationFaultsConvergeOnRetry(t *testing.T) {
	for _, point := range []mapstore.FaultPoint{mapstore.FaultBeforeGCPromoteRename, mapstore.FaultBeforeGCPromoteSync} {
		t.Run(string(point), func(t *testing.T) {
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, "mapping-v2"), 0o700); err != nil {
				t.Fatal(err)
			}
			staging := filepath.Join(root, "stage")
			if err := os.Mkdir(staging, 0o700); err != nil {
				t.Fatal(err)
			}
			storeID := mapstore.StoreID{1}
			writer, err := mapstore.CreateGenerationWriter(staging, storeID, 8192, 2, nil)
			if err != nil {
				t.Fatal(err)
			}
			builder, err := radix.NewRebuildBuilder(writer, 3, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			for index := 0; index < 600; index++ {
				addr, err := recordlog.NewVAddr(1, uint32(index+1)*64, 64)
				if err != nil {
					t.Fatal(err)
				}
				if err := builder.Add(model.ID(uint64(index)*512+1), addr); err != nil {
					t.Fatal(err)
				}
			}
			tree, err := builder.Finish()
			if err != nil {
				t.Fatal(err)
			}
			generation, err := writer.Finish(tree.Root(), tree.Covered())
			if err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected")
			calls := 0
			if err := mapstore.PromoteGeneration(root, staging, generation, func(got mapstore.FaultPoint) error {
				if got == point {
					calls++
				}
				if got == point && (point != mapstore.FaultBeforeGCPromoteRename || calls == 2) {
					return injected
				}
				return nil
			}); !errors.Is(err, injected) {
				t.Fatalf("promote err=%v", err)
			}
			if err := mapstore.PromoteGeneration(root, staging, generation, nil); err != nil {
				t.Fatal(err)
			}
			snapshot := mapstore.CatalogSnapshot{
				Generation: 2, StoreID: storeID, SegmentSize: 8192,
				ActiveSegment: generation.ActiveSegment, NextSegment: generation.NextSegment,
				SealedSegments: generation.SealedSegments, Root: generation.Root, Covered: generation.Covered,
			}
			reader, _, err := mapstore.OpenVerifiedGeneration(context.Background(), root, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			if err := mapstore.RemoveGenerationStaging(root, staging); err != nil {
				t.Fatal(err)
			}
		})
	}
}

package radix

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/catalog"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/initialize"
	"github.com/akzj/ridstore/internal/maintenance"
)

func TestNestedDataGCRotationUsesParentJournal(t *testing.T) {
	dir, manifest := nestedRotationFixture(t)
	catalogManager, err := catalog.New(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	store, err := openNodeStore(dir, manifest, catalogManager)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	fillMappingSegmentForRotation(t, store)
	journal := installDataGCParent(t, dir, manifest)
	if _, err := store.append(denseLeafBuild()); err != nil {
		t.Fatal(err)
	}
	got, found, err := maintenance.Load(dir)
	if err != nil || !found {
		t.Fatalf("journal found=%v error=%v", found, err)
	}
	if got.OperationID != journal.OperationID || got.OperationType != storeformat.MaintenanceDataGC || got.Phase != 3 {
		t.Fatalf("journal=%+v", got)
	}
	if _, ok := journalMappingRef(got.SourceFiles, 1); !ok {
		t.Fatalf("missing old mapping ref: %+v", got.SourceFiles)
	}
	if ref, ok := journalMappingRef(got.DestinationFiles, 2); !ok || ref.State != storeformat.FileStateActive {
		t.Fatalf("new mapping ref=%+v found=%v", ref, ok)
	}
	current := catalogManager.Snapshot()
	if current.ActiveMapSegmentID != 2 || !hasMapSummary(current.SealedMappingSegments, 1) || current.MaintenanceGeneration != journal.Generation {
		t.Fatalf("manifest=%+v", current)
	}
}

func TestRecoverNestedDataGCRotationAtDurableBoundaries(t *testing.T) {
	points := []failpoint.Point{PointRotationPrepared, PointRotationOldSealed, PointRotationNewCreated, PointRotationManifestInstalled}
	for _, point := range points {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir, manifest := nestedRotationFixture(t)
			catalogManager, err := catalog.New(dir, manifest)
			if err != nil {
				t.Fatal(err)
			}
			store, err := openNodeStore(dir, manifest, catalogManager)
			if err != nil {
				t.Fatal(err)
			}
			fillMappingSegmentForRotation(t, store)
			journal := installDataGCParent(t, dir, manifest)
			injected := errors.New("injected nested rotation crash")
			store.setHook(failpoint.Func(func(got failpoint.Point) error {
				if got == point {
					return injected
				}
				return nil
			}))
			if _, err := store.append(denseLeafBuild()); !errors.Is(err, injected) {
				t.Fatalf("append error=%v", err)
			}
			_ = store.Close()
			current, err := initialize.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := RecoverMappingRotation(dir, current)
			if err != nil {
				t.Fatal(err)
			}
			if recovered.ActiveMapSegmentID != 2 || !hasMapSummary(recovered.SealedMappingSegments, 1) || recovered.MaintenanceGeneration != journal.Generation {
				t.Fatalf("recovered=%+v", recovered)
			}
			if _, err := os.Stat(filepath.Join(dir, "mapping", sealedMapFileName(1))); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(dir, "mapping", activeMapFileName(2))); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRecoverNestedDataGCRotationRecreatesPartialDestination(t *testing.T) {
	dir, manifest := nestedRotationFixture(t)
	catalogManager, err := catalog.New(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	store, err := openNodeStore(dir, manifest, catalogManager)
	if err != nil {
		t.Fatal(err)
	}
	fillMappingSegmentForRotation(t, store)
	installDataGCParent(t, dir, manifest)
	injected := errors.New("stop after prepared")
	store.setHook(failpoint.Func(func(point failpoint.Point) error {
		if point == PointRotationPrepared {
			return injected
		}
		return nil
	}))
	if _, err := store.append(denseLeafBuild()); !errors.Is(err, injected) {
		t.Fatalf("append error=%v", err)
	}
	_ = store.Close()
	partial := filepath.Join(dir, "mapping", activeMapFileName(2))
	if err := os.WriteFile(partial, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	current, err := initialize.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RecoverMappingRotation(dir, current); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(partial)
	if err != nil || info.Size() != storeformat.SegmentHeaderSize {
		t.Fatalf("destination info=%v error=%v", info, err)
	}
}

func nestedRotationFixture(t *testing.T) (string, storeformat.Manifest) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	hard := storeformat.HardLimits{
		SegmentSize: 16 << 10, MaxValueSize: 1024, MaxBatchBytes: 4096,
		MaxBatchMutations: 64, MaxBatchConditions: 64, MaxOpenBatches: 64,
		IDReserveSize: 64, BatchIDReserveSize: 64,
	}
	manifest, err := initialize.Create(dir, hard)
	if err != nil {
		t.Fatal(err)
	}
	return dir, manifest
}

func fillMappingSegmentForRotation(t *testing.T, store *nodeStore) {
	t.Helper()
	if _, err := store.append(denseLeafBuild()); err != nil {
		t.Fatal(err)
	}
}

func denseLeafBuild() storeformat.MappingNodeBuild {
	addr, _ := base.NewVAddr(1, storeformat.SegmentHeaderSize)
	build := storeformat.MappingNodeBuild{Level: 0, Encoding: storeformat.NodeEncodingDense512, CoveredCommitSeq: 1}
	for i := range build.Slots {
		build.Slots[i] = uint64(addr)
	}
	return build
}

func installDataGCParent(t *testing.T, dir string, manifest storeformat.Manifest) storeformat.MaintenanceJournal {
	t.Helper()
	journal := storeformat.MaintenanceJournal{
		Generation: manifest.MaintenanceGeneration + 1, StoreUUID: manifest.StoreUUID, OperationID: [16]byte{1},
		OperationType: storeformat.MaintenanceDataGC, Phase: 3, OldManifestGeneration: manifest.Generation,
		SourceFiles: []storeformat.JournalFileRef{{Kind: storeformat.FileKindData, State: storeformat.FileStateSealed, FileID: 1, ValidEnd: storeformat.SegmentHeaderSize, FirstSeq: 1, LastSeq: 1}},
	}
	if err := maintenance.Install(dir, journal); err != nil {
		t.Fatal(err)
	}
	return journal
}

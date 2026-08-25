package storecatalog

import (
	"errors"
	"testing"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

func TestManagerDrivesMapStoreRotation(t *testing.T) {
	root := t.TempDir()
	manifest := testManifest()
	manifest.ActiveMapSegmentID = 1
	manifest.NextMapSegmentID = 2
	manifest.SealedMapSegments = nil
	manifest.MappingRoot = 0
	manifest.HardLimits.SegmentSize = 8192
	manifest.HardLimits.MaxValueSize = 64
	manifest.HardLimits.MaxBatchBytes = 4096
	manifest.HardLimits.MaxBatchMutations = 4
	manifest.HardLimits.MaxBatchConditions = 4
	manifest.HardLimits.MaxRecordLogPayload = 4096
	if err := Install(root, manifest, nil); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(root, manifest, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := mapstore.CreateInitialSegment(root, mapstore.StoreID(manifest.StoreUUID), uint32(manifest.HardLimits.SegmentSize)); err != nil {
		t.Fatal(err)
	}
	store, err := mapstore.Open(root, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var slots [mapstore.NodeSlots]uint64
	for index := range slots {
		addr, err := model.NewMapAddr(1, mapstore.SegmentHeaderSize+uint32(index)*8)
		if err != nil {
			t.Fatal(err)
		}
		slots[index] = uint64(addr)
	}
	first, err := store.Append(1, 0, 1, slots)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Append(1, 0, 1, slots)
	if err != nil {
		t.Fatal(err)
	}
	current := manager.Snapshot()
	if first.SegmentID() != 1 || second.SegmentID() != 2 || current.ActiveMapSegmentID != 2 || current.NextMapSegmentID != 3 || len(current.SealedMapSegments) != 1 {
		t.Fatalf("first=%v second=%v manifest=%+v", first, second, current)
	}
}

func TestManagerInstallsTypedUpdates(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	initial := testManifest()
	if err := Install(root, initial, nil); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(root, initial, nil)
	if err != nil {
		t.Fatal(err)
	}
	addr, _ := recordlog.NewVAddr(2, 64, 64)
	rotated, err := manager.InstallDataRotation(1, DataRotation{
		SealedOld: DataSegmentSummary{SegmentID: 2, ValidEnd: 128, RecordCount: 1, FirstAddr: addr, LastAddr: addr},
		NewActive: 3, NextID: 4,
	})
	if err != nil || rotated.Generation != 2 || rotated.ActiveDataSegmentID != 3 || len(rotated.SealedDataSegments) != 2 {
		t.Fatalf("rotated=%+v err=%v", rotated, err)
	}
	if _, err := manager.InstallDataRotation(1, DataRotation{}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale generation err=%v", err)
	}

	rootAddr, _ := model.NewMapAddr(2, 128)
	checkpoint, err := manager.InstallCheckpoint(2, Checkpoint{
		MappingRoot: rootAddr, CoveredCommitSeq: 4, ReplayStart: rotated.ReplayStart,
		ReservedIDHigh: 200, ReservedBatchIDHigh: 200, IssuedBatchIDHighAtCut: 100,
		OpenBatchIDsAtCut: []model.BatchID{9}, StatsCoveredCommitSeq: 4,
		SegmentStats: []SegmentStats{{SegmentID: 1}, {SegmentID: 2, LiveBytes: 64, LiveRecords: 1}},
	})
	if err != nil || checkpoint.Generation != 3 || checkpoint.CoveredCommitSeq != 4 {
		t.Fatalf("checkpoint=%+v err=%v", checkpoint, err)
	}

	retired, err := manager.InstallDataRetire(3, DataRetire{Source: checkpoint.SealedDataSegments[0], CoveredCommitSeq: 4, ReplayStart: checkpoint.ReplayStart})
	if err != nil || retired.Generation != 4 || len(retired.SealedDataSegments) != 1 || retired.SealedDataSegments[0].SegmentID != 2 {
		t.Fatalf("retired=%+v err=%v", retired, err)
	}
	loaded, err := Load(root)
	if err != nil || loaded.Generation != 4 {
		t.Fatalf("loaded generation=%d err=%v", loaded.Generation, err)
	}
}

func TestDataRetireRequiresExactZeroLiveStats(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	initial := testManifest()
	if err := Install(root, initial, nil); err != nil {
		t.Fatal(err)
	}
	manager, _ := NewManager(root, initial, nil)
	_, err := manager.InstallDataRetire(1, DataRetire{Source: initial.SealedDataSegments[0], CoveredCommitSeq: 0, ReplayStart: initial.ReplayStart})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err=%v", err)
	}
}

func TestManagerImplementsRecordLogCatalogPort(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	initial := testManifest()
	if err := Install(root, initial, nil); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(root, initial, nil)
	if err != nil {
		t.Fatal(err)
	}
	addr, _ := recordlog.NewVAddr(2, recordlog.SegmentHeaderSize, 64)
	sealed := recordlog.SegmentSummary{SegmentID: 2, ValidEnd: 128, RecordCount: 1, FirstAddr: addr, LastAddr: addr}
	rotated, err := manager.InstallRecordLogRotation(1, sealed, 3, 4)
	if err != nil || rotated.Generation != 2 || rotated.ActiveSegmentID != 3 || len(rotated.SealedSegments) != 2 {
		t.Fatalf("rotated=%+v err=%v", rotated, err)
	}
	rootAddr, _ := model.NewMapAddr(2, 128)
	checkpoint, err := manager.InstallCheckpoint(2, Checkpoint{
		MappingRoot: rootAddr, CoveredCommitSeq: 1, ReplayStart: initial.ReplayStart,
		ReservedIDHigh: 100, ReservedBatchIDHigh: 100, IssuedBatchIDHighAtCut: 50,
		OpenBatchIDsAtCut: []model.BatchID{2, 7}, StatsCoveredCommitSeq: 1,
		SegmentStats: []SegmentStats{{SegmentID: 1}, {SegmentID: 2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	retired, err := manager.RemoveRecordLogSegment(checkpoint.Generation, checkpoint.SealedDataSegments[0])
	if err != nil || retired.Generation != 4 || len(retired.SealedSegments) != 1 || retired.SealedSegments[0].SegmentID != 2 {
		t.Fatalf("retired=%+v err=%v", retired, err)
	}
}

package storecatalog

import (
	"errors"
	"testing"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

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

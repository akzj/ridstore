package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/coordinator"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

func TestCheckpointCatalogFailureRecoversFromAuthoritativeManifest(t *testing.T) {
	tests := []struct {
		point          storecatalog.FaultPoint
		wantGeneration uint64
	}{
		{storecatalog.FaultBeforeManifestWrite, 1},
		{storecatalog.FaultBeforeManifestSync, 1},
		{storecatalog.FaultBeforeManifestRename, 1},
		{storecatalog.FaultBeforeManifestDirSync, 2},
	}
	for _, test := range tests {
		t.Run(string(test.point), func(t *testing.T) {
			root, config := prepareCheckpointStore(t)
			injected := errors.New("injected catalog failure")
			hook := func(point storecatalog.FaultPoint) error {
				if point == test.point {
					return injected
				}
				return nil
			}
			store, err := open(context.Background(), root, config, hook)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := store.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			id, err := batch.Create(context.Background(), []byte("survives checkpoint failure"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := batch.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := store.Checkpoint(context.Background()); !errors.Is(err, injected) {
				t.Fatalf("checkpoint err=%v", err)
			}
			if _, err := store.Get(context.Background(), id); !errors.Is(err, base.ErrReadOnly) {
				t.Fatalf("get after ambiguous catalog install err=%v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			manifest, err := storecatalog.Load(root)
			if err != nil || manifest.Generation != test.wantGeneration {
				t.Fatalf("manifest generation=%d want=%d err=%v", manifest.Generation, test.wantGeneration, err)
			}
			reopened, err := Open(context.Background(), root, config)
			if err != nil {
				t.Fatal(err)
			}
			record, err := reopened.Get(context.Background(), id)
			if err != nil || string(record.Value) != "survives checkpoint failure" {
				t.Fatalf("record=%+v err=%v", record, err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func prepareCheckpointStore(t *testing.T) (string, OpenConfig) {
	t.Helper()
	root := t.TempDir()
	logID := recordlog.LogID{9, 8, 7, 6}
	storeID := storecatalog.StoreUUID{4, 3, 2, 1}
	const segmentSize = uint32(1 << 20)
	if err := recordlog.CreateInitialSegment(root, logID, segmentSize); err != nil {
		t.Fatal(err)
	}
	if err := mapstore.CreateInitialSegment(root, mapstore.StoreID(storeID), segmentSize); err != nil {
		t.Fatal(err)
	}
	replayStart, err := recordlog.NewLogPos(1, recordlog.SegmentHeaderSize)
	if err != nil {
		t.Fatal(err)
	}
	manifest := storecatalog.Manifest{
		Generation: 1, StoreUUID: storeID, RecordLogID: logID,
		HardLimits: storecatalog.HardLimits{
			SegmentSize: uint64(segmentSize), MaxValueSize: 1024, MaxBatchBytes: 4096,
			MaxBatchMutations: 16, MaxBatchConditions: 16, MaxOpenBatches: 4,
			MaxRecordLogPayload: 64 << 10, IDReserveSize: 16, BatchIDReserveSize: 16,
		},
		ActiveDataSegmentID: 1, NextDataSegmentID: 2,
		ActiveMapSegmentID: 1, NextMapSegmentID: 2,
		ReplayStart: replayStart, ReservedIDHigh: 1, ReservedBatchIDHigh: 1, IssuedBatchIDHighAtCut: 1,
	}
	if err := storecatalog.Install(root, manifest, nil); err != nil {
		t.Fatal(err)
	}
	return root, OpenConfig{
		RecordLog:         recordlog.Config{MaxQueuedBytes: 1 << 20, QueueCapacity: 32, BufferBytes: 64 << 10, BufferRecords: 32},
		Commit:            coordinator.Config{QueueCapacity: 16, MaxGroupBatches: 8, MaxGroupPayload: 64 << 10},
		MappingCacheBytes: 1 << 20, MaxCheckpointEntries: 1024,
	}
}

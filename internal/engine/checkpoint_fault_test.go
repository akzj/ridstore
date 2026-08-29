package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/coordinator"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
	"github.com/akzj/ridstore/internal/transaction"
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
			store, err := open(context.Background(), root, config, openFaultHooks{catalog: hook})
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

func TestCheckpointMapStoreFailureKeepsOldManifestRecoverable(t *testing.T) {
	for _, point := range []mapstore.FaultPoint{mapstore.FaultBeforeAppendWrite, mapstore.FaultBeforeSync} {
		t.Run(string(point), func(t *testing.T) {
			root, config := prepareCheckpointStore(t)
			injected := errors.New("injected mapstore failure")
			store, err := open(context.Background(), root, config, openFaultHooks{mapStore: func(got mapstore.FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			batch, err := store.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			id, err := batch.Create(context.Background(), []byte("survives mapstore failure"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := batch.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := store.Checkpoint(context.Background()); !errors.Is(err, injected) || !errors.Is(err, mapstore.ErrPoisoned) {
				t.Fatalf("checkpoint err=%v", err)
			}
			if _, err := store.Get(context.Background(), id); !errors.Is(err, base.ErrReadOnly) {
				t.Fatalf("get after mapstore failure err=%v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			manifest, err := storecatalog.Load(root)
			if err != nil || manifest.Generation != 1 || manifest.MappingRoot != 0 || manifest.CoveredCommitSeq != 0 {
				t.Fatalf("manifest=%+v err=%v", manifest, err)
			}
			reopened, err := Open(context.Background(), root, config)
			if err != nil {
				t.Fatal(err)
			}
			record, err := reopened.Get(context.Background(), id)
			if err != nil || string(record.Value) != "survives mapstore failure" {
				t.Fatalf("record=%+v err=%v", record, err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBackgroundCheckpointFailureFailsStoreClosed(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	config.Runtime.CheckpointSortBytes = 64
	config.Runtime.DeltaSoftLimitBytes = 64
	config.Runtime.DeltaHardLimitBytes = 256
	store, err := Create(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	injected := errors.New("injected background checkpoint failure")
	var armed atomic.Bool
	store, err = open(context.Background(), root, config.Runtime, openFaultHooks{mapStore: func(point mapstore.FaultPoint) error {
		if armed.Load() && point == mapstore.FaultBeforeAppendWrite {
			return injected
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Create(context.Background(), []byte("value")); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for store.Metrics().BackgroundCheckpointFailed == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("background checkpoint did not fail: %+v", store.Metrics())
		}
		time.Sleep(time.Millisecond)
	}
	metrics := store.Metrics()
	if metrics.BackgroundCheckpointRequested != 1 || metrics.BackgroundCheckpointCompleted != 0 || metrics.BackgroundCheckpointFailed != 1 {
		t.Fatalf("metrics=%+v", metrics)
	}
	if _, err := store.Begin(context.Background()); !errors.Is(err, base.ErrReadOnly) || !errors.Is(err, injected) {
		t.Fatalf("begin after background checkpoint failure err=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitSyncFailureIsResolvedByFreshOpen(t *testing.T) {
	root, config := prepareCheckpointStore(t)
	injected := errors.New("injected recordlog sync failure")
	var armed atomic.Bool
	store, err := open(context.Background(), root, config, openFaultHooks{recordLog: func(point recordlog.FaultPoint) error {
		if armed.Load() && point == recordlog.FaultBeforeDataSync {
			return injected
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	openBatch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	batchID := batch.ID()
	id, err := batch.Create(context.Background(), []byte("commit outcome recovered from log"))
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	if _, err := batch.Commit(context.Background()); !errors.Is(err, base.ErrCommitUnknown) || !errors.Is(err, injected) {
		t.Fatalf("commit err=%v", err)
	}
	status, err := store.Status(context.Background(), batchID)
	if err != nil || status.State != BatchStateCommitUnknown || status.CommitSeq != 0 {
		t.Fatalf("unknown status=%+v err=%v", status, err)
	}
	if _, err := store.Begin(context.Background()); !errors.Is(err, base.ErrReadOnly) {
		t.Fatalf("begin after commit uncertainty err=%v", err)
	}
	if _, err := store.Get(context.Background(), model.ID(999)); !errors.Is(err, base.ErrReadOnly) {
		t.Fatalf("get after commit uncertainty err=%v", err)
	}
	if _, err := openBatch.Create(context.Background(), []byte("must fail closed")); !errors.Is(err, base.ErrReadOnly) {
		t.Fatalf("batch mutation after commit uncertainty err=%v", err)
	}
	if err := store.Close(); !errors.Is(err, recordlog.ErrPoisoned) {
		t.Fatalf("close err=%v", err)
	}

	manifest, err := storecatalog.Load(root)
	if err != nil || manifest.Generation != 1 || manifest.MappingRoot != 0 || manifest.CoveredCommitSeq != 0 {
		t.Fatalf("manifest=%+v err=%v", manifest, err)
	}
	reopened, err := Open(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	record, err := reopened.Get(context.Background(), id)
	if err != nil || string(record.Value) != "commit outcome recovered from log" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	status, err = reopened.Status(context.Background(), batchID)
	if err != nil || status.State != BatchStateCommitted || status.CommitSeq == 0 {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitSyncFailureWithPartialRecordRecoversAsAborted(t *testing.T) {
	root, config := prepareCheckpointStore(t)
	injected := errors.New("injected recordlog sync failure")
	var armed atomic.Bool
	store, err := open(context.Background(), root, config, openFaultHooks{recordLog: func(point recordlog.FaultPoint) error {
		if armed.Load() && point == recordlog.FaultBeforeDataSync {
			return injected
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	batchID := batch.ID()
	id, err := batch.Create(context.Background(), []byte("orphan after torn commit"))
	if err != nil {
		t.Fatal(err)
	}
	beforeCommit := store.log.Status().Watermarks.Written
	armed.Store(true)
	if _, err := batch.Commit(context.Background()); !errors.Is(err, base.ErrCommitUnknown) {
		t.Fatalf("commit err=%v", err)
	}
	afterCommit := store.log.Status().Watermarks.Written
	if afterCommit.SegmentID != beforeCommit.SegmentID || afterCommit.Offset <= beforeCommit.Offset+8 {
		t.Fatalf("before=%+v after=%+v", beforeCommit, afterCommit)
	}
	if err := store.Close(); !errors.Is(err, recordlog.ErrPoisoned) {
		t.Fatalf("close err=%v", err)
	}
	active := filepath.Join(root, "records", fmt.Sprintf("record-%010d.active", beforeCommit.SegmentID))
	if err := os.Truncate(active, int64(beforeCommit.Offset+8)); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Get(context.Background(), id); !errors.Is(err, base.ErrNotFound) {
		t.Fatalf("torn commit became visible err=%v", err)
	}
	status, err := reopened.Status(context.Background(), batchID)
	if err != nil || status.State != BatchStateAborted || status.CommitSeq != 0 {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointExcludesCommittedBatchWhoseCallerHasNotFinished(t *testing.T) {
	root, config := prepareCheckpointStore(t)
	store, err := Open(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := committed.Create(context.Background(), []byte("committed before checkpoint barrier")); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.submitCommit(context.Background(), committed.inner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receipt.Wait(); err != nil {
		t.Fatal(err)
	}
	if state, _ := committed.inner.State(); state != transaction.StateCommitted {
		t.Fatalf("committed batch state=%v", state)
	}
	store.mu.Lock()
	_, stillTracked := store.open[committed.ID()]
	store.mu.Unlock()
	if !stillTracked {
		t.Fatal("test did not preserve the client-finish race window")
	}

	open, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	manifest, err := storecatalog.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.OpenBatchIDsAtCut) != 1 || manifest.OpenBatchIDsAtCut[0] != open.ID() {
		t.Fatalf("open batches=%v committed=%d open=%d", manifest.OpenBatchIDsAtCut, committed.ID(), open.ID())
	}

	committed.finish()
	if err := open.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointSupportsDataRotationAfterCut(t *testing.T) {
	root, config := prepareCheckpointStore(t)
	config.CheckpointSortBytes = 1 << 20
	config.DeltaSoftLimitBytes = 512 << 10
	config.DeltaHardLimitBytes = 1 << 20
	config.StatusRetention = 2048
	store, err := Open(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	store.stopBackgroundCheckpoint()
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Create(context.Background(), []byte("covered before checkpoint cut")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}

	store.ops.Lock()
	work, err := store.prepareCheckpointLocked(context.Background())
	store.ops.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	before := store.catalog.Snapshot()
	for index := 0; index < 2048 && store.catalog.Snapshot().ActiveDataSegmentID == work.cut.ReplayStart.SegmentID; index++ {
		batch, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Create(context.Background(), make([]byte, 1024)); err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	rotated := store.catalog.Snapshot()
	if rotated.ActiveDataSegmentID == work.cut.ReplayStart.SegmentID {
		t.Fatal("record log did not rotate after checkpoint cut")
	}
	if err := store.finishCheckpoint(context.Background(), work); err != nil {
		t.Fatal(err)
	}
	after := store.catalog.Snapshot()
	if after.Generation != rotated.Generation+1 || after.MappingRoot == before.MappingRoot || after.CoveredCommitSeq != work.cut.CoveredCommitSeq ||
		after.ReplayStart != work.cut.ReplayStart || after.ActiveDataSegmentID != rotated.ActiveDataSegmentID {
		t.Fatalf("checkpoint did not publish across data rotation: before=%+v rotated=%+v after=%+v", before, rotated, after)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
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
		MappingCacheBytes: 1 << 20, CheckpointSortBytes: 16 << 10, MaxSegmentStats: 1024,
		DeltaSoftLimitBytes: 32 << 10, DeltaHardLimitBytes: 64 << 10,
		StatusRetention: 64,
	}
}

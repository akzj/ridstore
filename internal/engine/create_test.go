package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/bootstrap"
	"github.com/akzj/ridstore/internal/coordinator"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

func TestCreateBuildsUsableExclusiveStore(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	store, err := Create(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), root, config.Runtime); !errors.Is(err, base.ErrLocked) {
		t.Fatalf("concurrent open err=%v", err)
	}
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.Create(context.Background(), []byte("created by v2"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(context.Background(), root, config.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	record, err := reopened.Get(context.Background(), id)
	if err != nil || string(record.Value) != "created by v2" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRejectsIncompleteInitializationAndCreateResumes(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	stop := errors.New("stop before mapping segment")
	_, err := create(context.Background(), root, config, func(point bootstrap.FaultPoint) error {
		if point == bootstrap.FaultBeforeMapSegment {
			return stop
		}
		return nil
	}, nil)
	if !errors.Is(err, stop) {
		t.Fatalf("create err=%v", err)
	}
	if _, err := Open(context.Background(), root, config.Runtime); !errors.Is(err, base.ErrRecoveryRequired) {
		t.Fatalf("open incomplete err=%v", err)
	}
	store, err := Create(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenCleansUnpublishedManifestTemp(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	store, err := Create(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	temp := filepath.Join(root, "MANIFEST-v2-0.tmp")
	if err := os.WriteFile(temp, []byte("unpublished"), 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(context.Background(), root, config.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(temp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp err=%v", err)
	}
	if _, err := storecatalog.LoadStrict(root); err != nil {
		t.Fatalf("strict load after open: %v", err)
	}
}

func TestCreateRejectsUncheckpointableDeltaBudgetBeforeBootstrap(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	config.Runtime.CheckpointSortBytes = 24
	config.Runtime.DeltaHardLimitBytes = 128
	if _, err := Create(context.Background(), root, config); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("create err=%v", err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid create changed root err=%v", err)
	}
}

func TestCreateRejectsStatusRetentionBelowOpenLimitBeforeBootstrap(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	config.Runtime.StatusRetention = config.HardLimits.MaxOpenBatches - 1
	if _, err := Create(context.Background(), root, config); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("create err=%v", err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid create changed root err=%v", err)
	}
}

func TestCreateRejectsCommitPayloadOutsidePersistentBounds(t *testing.T) {
	for _, mutate := range []func(*CreateConfig){
		func(config *CreateConfig) {
			config.Runtime.Commit.MaxGroupPayload = config.HardLimits.MaxRecordLogPayload + 1
		},
		func(config *CreateConfig) { config.Runtime.Commit.MaxGroupPayload = 128 },
	} {
		root := filepath.Join(t.TempDir(), "store")
		config := testCreateConfig()
		mutate(&config)
		if _, err := Create(context.Background(), root, config); !errors.Is(err, base.ErrInvalidConfig) {
			t.Fatalf("create err=%v", err)
		}
		if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalid create changed root err=%v", err)
		}
	}
}

func TestCreateRejectsGCConfigOutsidePersistentBounds(t *testing.T) {
	for _, mutate := range []func(*CreateConfig){
		func(config *CreateConfig) { config.Runtime.GCBatchBytes = config.HardLimits.MaxBatchBytes + 1 },
		func(config *CreateConfig) { config.Runtime.GCBatchMutations = config.HardLimits.MaxBatchMutations + 1 },
		func(config *CreateConfig) {
			config.Runtime.WriteStopFreeBytes = 100
			config.Runtime.GCMinFreeBytes = 101
		},
	} {
		root := filepath.Join(t.TempDir(), "store")
		config := testCreateConfig()
		mutate(&config)
		if _, err := Create(context.Background(), root, config); !errors.Is(err, base.ErrInvalidConfig) {
			t.Fatalf("create err=%v", err)
		}
		if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalid create changed root err=%v", err)
		}
	}
}

func TestCommitAdvancesCheckpointUnderDeltaHardPressure(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	config.Runtime.CheckpointSortBytes = 24
	config.Runtime.DeltaSoftLimitBytes = 32
	config.Runtime.DeltaHardLimitBytes = 64
	store, err := Create(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil && !errors.Is(err, base.ErrClosed) {
			t.Errorf("close: %v", err)
		}
	}()

	first, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	firstID, err := first.Create(context.Background(), []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := second.Create(context.Background(), []byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := second.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if store.mapping.CoveredCommitSeq() != 2 {
		t.Fatalf("covered=%d", store.mapping.CoveredCommitSeq())
	}
	for id, want := range map[model.ID]string{firstID: "first", secondID: "second"} {
		record, err := store.Get(context.Background(), id)
		if err != nil || string(record.Value) != want {
			t.Fatalf("id=%d record=%+v err=%v", id, record, err)
		}
	}
}

func TestDeltaSoftPressureSchedulesBackgroundCheckpoint(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	config.Runtime.CheckpointSortBytes = 96
	config.Runtime.DeltaSoftLimitBytes = 64
	config.Runtime.DeltaHardLimitBytes = 256
	store, err := Create(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil && !errors.Is(err, base.ErrClosed) {
			t.Errorf("close: %v", err)
		}
	}()
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.Create(context.Background(), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		metrics := store.Metrics()
		if metrics.BackgroundCheckpointCompleted != 0 {
			if metrics.BackgroundCheckpointRequested != 1 || metrics.BackgroundCheckpointFailed != 0 || metrics.DeltaChargedBytes != 0 {
				t.Fatalf("metrics=%+v", metrics)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("background checkpoint did not complete: %+v", metrics)
		}
		time.Sleep(time.Millisecond)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil || string(record.Value) != "value" || store.mapping.CoveredCommitSeq() != 1 {
		t.Fatalf("record=%+v covered=%d err=%v", record, store.mapping.CoveredCommitSeq(), err)
	}
}

func TestCheckpointPressureGenerationDeduplicatesLateRequest(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	config.Runtime.CheckpointSortBytes = 96
	config.Runtime.DeltaSoftLimitBytes = 64
	config.Runtime.DeltaHardLimitBytes = 256
	store, err := Create(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	store.stopBackgroundCheckpoint()
	defer func() {
		if err := store.Close(); err != nil && !errors.Is(err, base.ErrClosed) {
			t.Errorf("close: %v", err)
		}
	}()

	commit := func(value string) uint64 {
		batch, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Create(context.Background(), []byte(value)); err != nil {
			t.Fatal(err)
		}
		receipt, err := store.submitCommit(context.Background(), batch.inner)
		if err != nil {
			t.Fatal(err)
		}
		generation := receipt.DeltaPressureGeneration()
		if generation == 0 {
			t.Fatal("soft pressure did not carry a generation")
		}
		if _, err := receipt.Wait(); err != nil {
			t.Fatal(err)
		}
		batch.finish()
		return generation
	}

	first := commit("first")
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if completed := store.checkpointPressureCompleted.Load(); completed < first {
		t.Fatalf("completed generation=%d first=%d", completed, first)
	}
	store.requestBackgroundCheckpoint(first)
	if pending, completed := store.checkpointPressurePending.Load(), store.checkpointPressureCompleted.Load(); pending > completed {
		t.Fatalf("late request was not deduplicated: pending=%d completed=%d", pending, completed)
	}
	if requested := store.Metrics().BackgroundCheckpointRequested; requested != 0 {
		t.Fatalf("covered late request changed requested counter: %d", requested)
	}

	second := commit("second")
	if second <= first {
		t.Fatalf("new active Delta generation=%d first=%d", second, first)
	}
	store.requestBackgroundCheckpoint(second)
	if pending, completed := store.checkpointPressurePending.Load(), store.checkpointPressureCompleted.Load(); pending != second || pending <= completed {
		t.Fatalf("new pressure was lost: pending=%d completed=%d second=%d", pending, completed, second)
	}
}

func TestBackgroundCheckpointCoalescesSameDeltaGeneration(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	config.Runtime.CheckpointSortBytes = 96
	config.Runtime.DeltaSoftLimitBytes = 64
	config.Runtime.DeltaHardLimitBytes = 256
	store, err := Create(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil && !errors.Is(err, base.ErrClosed) {
			t.Errorf("close: %v", err)
		}
	}()

	store.checkpointMu.Lock()
	locked := true
	defer func() {
		if locked {
			store.checkpointMu.Unlock()
		}
	}()
	commit := func(value string) {
		batch, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Create(context.Background(), []byte(value)); err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	commit("first")

	deadline := time.Now().Add(2 * time.Second)
	for len(store.checkpointRequests) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("background worker did not consume the first wake")
		}
		time.Sleep(time.Millisecond)
	}
	commit("second")
	if requested := store.Metrics().BackgroundCheckpointRequested; requested != 1 {
		t.Fatalf("same Delta generation queued %d checkpoint requests", requested)
	}

	store.checkpointMu.Unlock()
	locked = false
	deadline = time.Now().Add(2 * time.Second)
	for store.Metrics().BackgroundCheckpointCompleted == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("background checkpoint did not complete: %+v", store.Metrics())
		}
		time.Sleep(time.Millisecond)
	}
	metrics := store.Metrics()
	if metrics.BackgroundCheckpointRequested != 1 || metrics.BackgroundCheckpointCompleted != 1 || metrics.DeltaChargedBytes != 0 {
		t.Fatalf("metrics=%+v", metrics)
	}
}

func TestIncrementalCheckpointUsesRecordRefs(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	store, err := Create(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.Create(context.Background(), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(context.Background(), root, config.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	batch, err = store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Put(context.Background(), id, []byte("new value")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCloseStopsBackgroundCheckpointBeforeClosingStorage(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	config.Runtime.CheckpointSortBytes = 96
	config.Runtime.DeltaSoftLimitBytes = 64
	config.Runtime.DeltaHardLimitBytes = 256
	store, err := Create(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.Create(context.Background(), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.checkpointDone:
	default:
		t.Fatal("background checkpoint worker still running after Close")
	}
	reopened, err := Open(context.Background(), root, config.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	record, err := reopened.Get(context.Background(), id)
	if err != nil || string(record.Value) != "value" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

func TestConcurrentCommitCheckpointAndClose(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	store, err := Create(context.Background(), root, testCreateConfig())
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Create(context.Background(), []byte("racing value")); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errorsByOperation := make(chan error, 3)
	var workers sync.WaitGroup
	workers.Add(3)
	go func() {
		defer workers.Done()
		<-start
		_, err := batch.Commit(context.Background())
		errorsByOperation <- err
	}()
	go func() {
		defer workers.Done()
		<-start
		errorsByOperation <- store.Checkpoint(context.Background())
	}()
	go func() {
		defer workers.Done()
		<-start
		errorsByOperation <- store.Close()
	}()
	close(start)
	workers.Wait()
	close(errorsByOperation)
	for err := range errorsByOperation {
		if err != nil && !errors.Is(err, base.ErrClosed) && !errors.Is(err, base.ErrBatchClosed) {
			t.Fatalf("concurrent operation err=%v", err)
		}
	}
}

func TestCloseDoesNotWaitForCheckpointMutex(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	store, err := Create(context.Background(), root, testCreateConfig())
	if err != nil {
		t.Fatal(err)
	}

	// A running operation may be about to enter checkpoint after Close marks
	// the Store closing. Close must drain active operations without owning the
	// same mutex they may still need, otherwise the lifecycle protocol can
	// deadlock permanently.
	store.checkpointMu.Lock()
	done := make(chan error, 1)
	go func() { done <- store.Close() }()
	select {
	case err := <-done:
		store.checkpointMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		store.checkpointMu.Unlock()
		t.Fatal("Close waited for checkpoint mutex")
	}
}

func TestStatusCapacityAdvancesCheckpointBeforeNextBatch(t *testing.T) {
	ctx := context.Background()
	config := testCreateConfig()
	config.HardLimits.MaxOpenBatches = 1
	config.Runtime.StatusRetention = 1
	root := filepath.Join(t.TempDir(), "store")
	store, err := Create(ctx, root, config)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Abort(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Abort(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Status(ctx, first.ID()); !errors.Is(err, base.ErrStatusExpired) {
		t.Fatalf("first status err=%v", err)
	}
	if status, err := store.Status(ctx, second.ID()); err != nil || status.State != BatchStateAborted {
		t.Fatalf("second status=%+v err=%v", status, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, root, config.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Status(ctx, first.ID()); !errors.Is(err, base.ErrStatusExpired) {
		t.Fatalf("reopened first status err=%v", err)
	}
	if status, err := reopened.Status(ctx, second.ID()); err != nil || status.State != BatchStateAborted {
		t.Fatalf("reopened second status=%+v err=%v", status, err)
	}
	third, err := reopened.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := third.Abort(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Status(ctx, second.ID()); !errors.Is(err, base.ErrStatusExpired) {
		t.Fatalf("evicted recovered status err=%v", err)
	}
}

func TestRecoveredStatusesEvictInDurableOrder(t *testing.T) {
	ctx := context.Background()
	config := testCreateConfig()
	config.HardLimits.MaxOpenBatches = 3
	config.Runtime.StatusRetention = 3
	root := filepath.Join(t.TempDir(), "store")
	store, err := Create(ctx, root, config)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]model.BatchID, 0, 4)
	for range 3 {
		batch, err := store.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, batch.ID())
		if err := batch.Abort(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(ctx, root, config.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	fourth, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ids = append(ids, fourth.ID())
	if err := fourth.Abort(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Status(ctx, ids[0]); !errors.Is(err, base.ErrStatusExpired) {
		t.Fatalf("oldest status err=%v", err)
	}
	for _, id := range ids[1:] {
		if status, err := store.Status(ctx, id); err != nil || status.State != BatchStateAborted {
			t.Fatalf("batch=%d status=%+v err=%v", id, status, err)
		}
	}
}

func testCreateConfig() CreateConfig {
	return CreateConfig{
		HardLimits: storecatalog.HardLimits{
			SegmentSize: 1 << 20, MaxValueSize: 1024, MaxBatchBytes: 4096,
			MaxBatchMutations: 16, MaxBatchConditions: 16, MaxOpenBatches: 4,
			MaxRecordLogPayload: 64 << 10, IDReserveSize: 16, BatchIDReserveSize: 16,
		},
		Runtime: OpenConfig{
			RecordLog:         recordlog.Config{MaxQueuedBytes: 1 << 20, QueueCapacity: 32, BufferBytes: 64 << 10, BufferRecords: 32},
			Commit:            coordinator.Config{QueueCapacity: 16, MaxGroupBatches: 8, MaxGroupPayload: 64 << 10},
			MappingCacheBytes: 1 << 20, CheckpointSortBytes: 24 << 10, MaxSegmentStats: 1024,
			DeltaSoftLimitBytes: 32 << 10, DeltaHardLimitBytes: 64 << 10,
			StatusRetention: 64, GCBytesPerSecond: ^uint64(0),
		},
	}
}

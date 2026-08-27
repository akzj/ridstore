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
	config.Runtime.CheckpointSortBytes = 16
	config.Runtime.DeltaHardLimitBytes = 128
	if _, err := Create(context.Background(), root, config); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("create err=%v", err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid create changed root err=%v", err)
	}
}

func TestCreateRejectsUnrepresentableRecordMetaCacheBeforeBootstrap(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	config.Runtime.RecordMetaCacheEntries = ^uint64(0)
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
	config.Runtime.CheckpointSortBytes = 16
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
	config.Runtime.CheckpointSortBytes = 64
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

func TestIncrementalCheckpointSkipsUnchangedAndActiveRecordMetadata(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	config.Runtime.RecordMetaCacheEntries = 64
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
	if metrics := store.Metrics(); metrics.RecordMetaCacheEntries != 1 || metrics.RecordMetaCacheHits != 0 {
		t.Fatalf("warm put metrics=%+v", metrics)
	}
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if metrics := store.Metrics(); metrics.RecordMetaCacheHits != 0 || metrics.RecordMetaCacheMisses != 0 {
		t.Fatalf("warm checkpoint metrics=%+v", metrics)
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
	if metrics := store.Metrics(); metrics.RecordMetaCacheEntries != 0 || metrics.RecordMetaCacheMisses != 0 {
		t.Fatalf("cold checkpoint metrics=%+v", metrics)
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
	if metrics := store.Metrics(); metrics.RecordMetaCacheEntries != 2 || metrics.RecordMetaCacheHits != 0 || metrics.RecordMetaCacheMisses != 0 {
		t.Fatalf("read-warmed checkpoint metrics=%+v", metrics)
	}
}

func TestCloseStopsBackgroundCheckpointBeforeClosingStorage(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	config.Runtime.CheckpointSortBytes = 64
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
			MappingCacheBytes: 1 << 20, CheckpointSortBytes: 16 << 10, MaxSegmentStats: 1024,
			DeltaSoftLimitBytes: 32 << 10, DeltaHardLimitBytes: 64 << 10,
			StatusRetention: 64, GCBytesPerSecond: ^uint64(0),
		},
	}
}

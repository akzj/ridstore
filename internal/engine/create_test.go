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

func TestCreateNormalizesAutomaticMaintenanceAgainstSegmentSize(t *testing.T) {
	config := testCreateConfig()
	config.Runtime.Maintenance.Enabled = true
	config.Runtime.Maintenance.Interval = time.Hour
	store, err := Create(context.Background(), filepath.Join(t.TempDir(), "store"), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	maintenance := store.maintenance.config
	if maintenance.SegmentPolicy.MinReclaimableBytes != config.HardLimits.SegmentSize/4 ||
		maintenance.MappingMinReclaimableBytes != config.HardLimits.SegmentSize ||
		maintenance.SegmentPolicy.MinReclaimableRatioBasis != 2_500 || maintenance.MappingMinReclaimableRatioBasis != 5_000 {
		t.Fatalf("maintenance=%+v", maintenance)
	}
	deadline := time.Now().Add(2 * time.Second)
	for store.Metrics().MappingSurveyGeneration == 0 {
		if time.Now().After(deadline) {
			t.Fatal("initial asynchronous Mapping survey did not finish")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAutomaticMaintenanceSubmitsSegmentWorker(t *testing.T) {
	config := testCreateConfig()
	config.Runtime.Maintenance = MaintenanceConfig{
		Enabled: true, Interval: time.Hour, DisableMappingGC: true,
		SegmentPolicy: CompactionPolicy{MinReclaimableBytes: ^uint64(0)},
	}
	store, err := Create(context.Background(), filepath.Join(t.TempDir(), "store"), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	before := store.Metrics().MaintenanceRequested
	store.scheduleAutomaticMaintenance()
	deadline := time.Now().Add(2 * time.Second)
	for {
		metrics := store.Metrics()
		if metrics.MaintenanceRequested > before && metrics.MaintenanceQueued == 0 && metrics.MaintenanceRunning == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("automatic Segment worker did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	after := store.Metrics()
	if after.MaintenanceRequested <= before || after.MaintenanceFailed != 0 || after.MaintenanceAutomaticFailed != 0 {
		t.Fatalf("metrics before=%d after=%+v", before, after)
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

func TestCheckpointMetricsRecordCompletedOperation(t *testing.T) {
	store, err := Create(context.Background(), filepath.Join(t.TempDir(), "store"), testCreateConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	metrics := store.Metrics()
	if metrics.CheckpointsStarted != 1 || metrics.CheckpointsCompleted != 1 || metrics.CheckpointsFailed != 0 ||
		metrics.CheckpointDurationNanos == 0 || metrics.CheckpointMaxDurationNanos == 0 || metrics.CheckpointFences != 1 ||
		metrics.CheckpointCaptureNanos == 0 || metrics.CheckpointMaxCaptureNanos == 0 ||
		metrics.CheckpointBuildNanos == 0 || metrics.CheckpointMaxBuildNanos == 0 ||
		metrics.CheckpointPublishNanos == 0 || metrics.CheckpointMaxPublishNanos == 0 {
		t.Fatalf("checkpoint metrics=%+v", metrics)
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
	if store.core.mapping.CoveredCommitSeq() != 2 {
		t.Fatalf("covered=%d", store.core.mapping.CoveredCommitSeq())
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
	if err != nil || string(record.Value) != "value" || store.core.mapping.CoveredCommitSeq() != 1 {
		t.Fatalf("record=%+v covered=%d err=%v", record, store.core.mapping.CoveredCommitSeq(), err)
	}
}

func TestPeriodicCheckpointFlushesOnlyNonEmptyDelta(t *testing.T) {
	config := testCreateConfig()
	config.Runtime.CheckpointInterval = 10 * time.Millisecond
	config.Runtime.DeltaSoftLimitBytes = 128
	config.Runtime.DeltaHardLimitBytes = 256
	config.Runtime.CheckpointSortBytes = 96
	store, err := Create(context.Background(), filepath.Join(t.TempDir(), "store"), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	time.Sleep(30 * time.Millisecond)
	if started := store.Metrics().CheckpointsStarted; started != 0 {
		t.Fatalf("empty periodic checkpoints=%d", started)
	}
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Create(context.Background(), []byte("periodic")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for store.Metrics().CheckpointsCompleted == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("periodic checkpoint did not flush Delta: %+v", store.Metrics())
		}
		time.Sleep(time.Millisecond)
	}
	if metrics := store.Metrics(); metrics.DeltaChargedBytes != 0 || metrics.BackgroundCheckpointRequested != 0 {
		t.Fatalf("metrics=%+v", metrics)
	}
}

func TestCheckpointPressureWaitHonorsCallerCancellation(t *testing.T) {
	config := testCreateConfig()
	config.Runtime.CheckpointSortBytes = 24
	config.Runtime.DeltaSoftLimitBytes = 32
	config.Runtime.DeltaHardLimitBytes = 64
	store, err := Create(context.Background(), filepath.Join(t.TempDir(), "store"), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.checkpoints.captureMu.Lock()
	locked := true
	defer func() {
		if locked {
			store.checkpoints.captureMu.Unlock()
		}
	}()
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Create(context.Background(), []byte("pressure")); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.submitCommit(context.Background(), batch.inner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receipt.Wait(); err != nil {
		t.Fatal(err)
	}
	batch.finish()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := store.awaitCheckpointPressure(ctx, receipt.DeltaPressureGeneration(), false); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait err=%v", err)
	}
	store.checkpoints.captureMu.Unlock()
	locked = false
	deadline := time.Now().Add(2 * time.Second)
	for store.core.mapping.CoveredCommitSeq() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("shared checkpoint stopped after waiter cancellation")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCheckpointPressureWaitersShareOneGeneration(t *testing.T) {
	config := testCreateConfig()
	config.Runtime.CheckpointSortBytes = 24
	config.Runtime.DeltaSoftLimitBytes = 32
	config.Runtime.DeltaHardLimitBytes = 64
	store, err := Create(context.Background(), filepath.Join(t.TempDir(), "store"), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.checkpoints.captureMu.Lock()
	locked := true
	defer func() {
		if locked {
			store.checkpoints.captureMu.Unlock()
		}
	}()
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Create(context.Background(), []byte("pressure")); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.submitCommit(context.Background(), batch.inner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receipt.Wait(); err != nil {
		t.Fatal(err)
	}
	batch.finish()
	generation := receipt.DeltaPressureGeneration()
	if generation == 0 {
		t.Fatal("missing pressure generation")
	}
	store.requestBackgroundCheckpoint(generation)
	deadline := time.Now().Add(2 * time.Second)
	for store.Metrics().CheckpointsStarted == 0 {
		if time.Now().After(deadline) {
			t.Fatal("checkpoint worker did not start")
		}
		time.Sleep(time.Millisecond)
	}

	const waiterCount = 8
	coalescedBefore := store.Metrics().MaintenanceCoalesced
	done := make(chan error, waiterCount)
	for index := 0; index < waiterCount; index++ {
		go func() { done <- store.awaitCheckpointPressure(context.Background(), generation, false) }()
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		queued := store.Metrics().MaintenanceCoalesced - coalescedBefore
		if queued >= waiterCount {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("coalesced waiters=%d", queued)
		}
		time.Sleep(time.Millisecond)
	}
	store.checkpoints.captureMu.Unlock()
	locked = false
	for index := 0; index < waiterCount; index++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("checkpoint waiter did not complete")
		}
	}
	if metrics := store.Metrics(); metrics.CheckpointsCompleted != 1 || metrics.DeltaChargedBytes != 0 {
		t.Fatalf("metrics=%+v", metrics)
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
	if completed := store.checkpoints.pressureCompleted.Load(); completed < first {
		t.Fatalf("completed generation=%d first=%d", completed, first)
	}
	store.requestBackgroundCheckpoint(first)
	if pending, completed := store.checkpoints.pressurePending.Load(), store.checkpoints.pressureCompleted.Load(); pending > completed {
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
	if pending, completed := store.checkpoints.pressurePending.Load(), store.checkpoints.pressureCompleted.Load(); pending != second || pending <= completed {
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

	store.checkpoints.captureMu.Lock()
	locked := true
	defer func() {
		if locked {
			store.checkpoints.captureMu.Unlock()
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
	for store.Metrics().CheckpointsStarted == 0 {
		if time.Now().After(deadline) {
			t.Fatal("scheduler did not start the first checkpoint")
		}
		time.Sleep(time.Millisecond)
	}
	commit("second")
	if requested := store.Metrics().BackgroundCheckpointRequested; requested != 1 {
		t.Fatalf("same Delta generation queued %d checkpoint requests", requested)
	}

	store.checkpoints.captureMu.Unlock()
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
	manifest, err := storecatalog.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.SegmentStats) != 1 || manifest.SegmentStats[0].SegmentID != manifest.ActiveDataSegmentID ||
		manifest.SegmentStats[0].LiveRecords != manifest.MappingEntryCount {
		t.Fatalf("checkpoint does not carry complete active stats: %+v", manifest)
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
	case <-store.Done():
	default:
		t.Fatal("Store goroutines still running after Close")
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
	store.checkpoints.captureMu.Lock()
	done := make(chan error, 1)
	go func() { done <- store.Close() }()
	select {
	case err := <-done:
		store.checkpoints.captureMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		store.checkpoints.captureMu.Unlock()
		t.Fatal("Close waited for checkpoint mutex")
	}
}

func TestCloseCancelsBlockedOperationAndClosesDone(t *testing.T) {
	config := testCreateConfig()
	config.HardLimits.MaxOpenBatches = 1
	config.Runtime.StatusRetention = 1
	store, err := Create(context.Background(), filepath.Join(t.TempDir(), "store"), config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Begin(context.Background()); err != nil {
		t.Fatal(err)
	}

	blocked := make(chan error, 1)
	go func() {
		_, beginErr := store.Begin(context.Background())
		blocked <- beginErr
	}()
	select {
	case err := <-blocked:
		t.Fatalf("Begin was not blocked: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := store.CloseContext(closeCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-blocked:
		if !errors.Is(err, context.Canceled) && !errors.Is(err, base.ErrClosed) {
			t.Fatalf("blocked Begin err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Begin did not observe Store cancellation")
	}
	select {
	case <-store.Done():
	default:
		t.Fatal("Done remained open after CloseContext returned")
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
			StatusRetention: 64, GCBytesPerSecond: ^uint64(0), CheckpointInterval: time.Hour,
		},
	}
}

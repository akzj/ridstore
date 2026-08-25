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

func TestCreateRejectsUncheckpointableDeltaBudgetBeforeBootstrap(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	config.Runtime.MaxCheckpointEntries = 1
	config.Runtime.DeltaHardLimitBytes = 128
	if _, err := Create(context.Background(), root, config); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("create err=%v", err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid create changed root err=%v", err)
	}
}

func TestCommitAdvancesCheckpointUnderDeltaHardPressure(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	config.Runtime.MaxCheckpointEntries = 1
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
			MappingCacheBytes: 1 << 20, MaxCheckpointEntries: 1024,
			DeltaSoftLimitBytes: 32 << 10, DeltaHardLimitBytes: 64 << 10,
		},
	}
}

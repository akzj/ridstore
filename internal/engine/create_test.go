package engine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/bootstrap"
	"github.com/akzj/ridstore/internal/coordinator"
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
		},
	}
}

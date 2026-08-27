package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/ridstore"
	"github.com/akzj/ridstore/internal/migration"
)

func TestMigratePlanCommand(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := ridstore.Create(context.Background(), toolTestConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if status := run(context.Background(), []string{"migrate", "plan", "--dir", dir}, &stdout, &stderr); status != 0 {
		t.Fatalf("status=%d stderr=%q", status, stderr.String())
	}
	var plan migration.Plan
	if err := json.Unmarshal(stdout.Bytes(), &plan); err != nil || !plan.VerifiedCurrent || plan.MigrationRequired {
		t.Fatalf("plan=%+v error=%v", plan, err)
	}
}

func TestUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if status := run(context.Background(), nil, &stdout, &stderr); status != 2 || stderr.Len() == 0 {
		t.Fatalf("status=%d stderr=%q", status, stderr.String())
	}
}

func toolTestConfig(dir string) ridstore.CreateConfig {
	return ridstore.CreateConfig{Dir: dir,
		HardLimits: ridstore.HardLimits{SegmentSize: 1 << 20, MaxValueSize: 1024, MaxBatchBytes: 4096, MaxBatchMutations: 16, MaxBatchConditions: 16, MaxOpenBatches: 4, MaxRecordLogPayload: 64 << 10, IDReserveSize: 16, BatchIDReserveSize: 16},
		Runtime:    ridstore.RuntimeConfig{MaxQueuedBytes: 1 << 20, AppendQueueCapacity: 32, AppendBufferBytes: 64 << 10, AppendBufferRecords: 32, CommitQueueCapacity: 16, MaxGroupBatches: 8, MaxGroupPayload: 64 << 10, MappingCacheBytes: 1 << 20, CheckpointSortBytes: 16 << 10, MaxSegmentStats: 1024, DeltaSoftLimitBytes: 32 << 10, DeltaHardLimitBytes: 64 << 10, StatusRetention: 16, WriteStopFreeBytes: 1, SpaceCheckInterval: time.Second}}
}

package ridstore

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"testing"
	"time"
)

func TestPublicV2LifecycleAndTokenAcrossReopen(t *testing.T) {
	ctx := context.Background()
	config := testCreateConfig(filepath.Join(t.TempDir(), "store"))
	store, err := Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}

	created, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := created.Create(ctx, []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	commit, err := created.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := store.Status(ctx, commit.BatchID)
	if err != nil || status.State != BatchStateCommitted || status.CommitSeq != commit.CommitSeq {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	record, err := store.Get(ctx, id)
	if err != nil || string(record.Value) != "one" || record.Token == (VersionToken{}) {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if err := store.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.CompactMapping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(ctx, OpenConfig{Dir: config.Dir, Runtime: config.Runtime})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	updated, err := reopened.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := updated.CompareAndPut(ctx, id, record.Token, []byte("two")); err != nil {
		t.Fatal(err)
	}
	if _, err := updated.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Get(ctx, id)
	if err != nil || string(got.Value) != "two" {
		t.Fatalf("record=%+v err=%v", got, err)
	}

	stale, _ := reopened.Begin(ctx)
	if err := stale.CompareAndDelete(id, record.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := stale.Commit(ctx); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale commit err=%v", err)
	}
}

func TestPublicOfflineVerify(t *testing.T) {
	ctx := context.Background()
	config := testCreateConfig(filepath.Join(t.TempDir(), "store"))
	store, err := Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, VerifyConfig{Dir: config.Dir}); !errors.Is(err, ErrLocked) {
		t.Fatalf("verify open store err=%v", err)
	}
	batch, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Create(ctx, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Create(ctx, []byte("tail-two")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := Verify(ctx, VerifyConfig{Dir: config.Dir})
	if err != nil {
		t.Fatal(err)
	}
	if report.Stage != VerifyStageExact || report.StoreID == ([16]byte{}) || report.CheckpointLiveIDs != 0 || report.LiveIDs != 2 || report.ReplayedCommits != 1 || report.VerifiedPuts != 2 {
		t.Fatalf("report=%+v", report)
	}
	if _, err := Verify(ctx, VerifyConfig{Dir: config.Dir, MaxLiveIDs: 1, MaxReplayStatuses: 1, MappingCacheBytes: 1}); !errors.Is(err, ErrVerifyLimit) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("verify limit err=%v", err)
	}
}

func TestPublicCompactMappingPassesOfflineVerify(t *testing.T) {
	ctx := context.Background()
	config := testCreateConfig(filepath.Join(t.TempDir(), "store"))
	store, err := Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.Create(ctx, []byte("mapping-gc"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.CompactMapping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := Verify(ctx, VerifyConfig{Dir: config.Dir})
	if err != nil || report.Stage != VerifyStageExact || report.LiveIDs != 1 || report.CheckpointLiveIDs != 1 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	reopened, err := Open(ctx, OpenConfig{Dir: config.Dir, Runtime: config.Runtime})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	record, err := reopened.Get(ctx, id)
	if err != nil || string(record.Value) != "mapping-gc" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

func TestVersionTokenRejectsZeroAndOtherStore(t *testing.T) {
	ctx := context.Background()
	firstConfig := testCreateConfig(filepath.Join(t.TempDir(), "first"))
	first, err := Create(ctx, firstConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	batch, _ := first.Begin(ctx)
	id, err := batch.Create(ctx, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	record, err := first.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	secondConfig := testCreateConfig(filepath.Join(t.TempDir(), "second"))
	second, err := Create(ctx, secondConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	for _, token := range []VersionToken{{}, record.Token} {
		candidate, beginErr := second.Begin(ctx)
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		if err := candidate.CompareAndPut(ctx, id, token, []byte("bad")); !errors.Is(err, ErrInvalidToken) {
			t.Fatalf("token=%+v err=%v", token, err)
		}
		if err := candidate.Abort(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPublicBatchCreateDeleteAndExpectAbsent(t *testing.T) {
	ctx := context.Background()
	store, err := Create(ctx, testCreateConfig(filepath.Join(t.TempDir(), "store")))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	batch, _ := store.Begin(ctx)
	id, err := batch.Allocate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.ExpectAbsent(id); err != nil {
		t.Fatal(err)
	}
	if err := batch.Put(ctx, id, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	record, err := store.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	deleted, _ := store.Begin(ctx)
	if err := deleted.CompareAndDelete(id, record.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := deleted.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get deleted err=%v", err)
	}
}

func TestUserPutStopsBeforeReservedFilesystemHeadroom(t *testing.T) {
	ctx := context.Background()
	config := testCreateConfig(filepath.Join(t.TempDir(), "store"))
	config.Runtime.WriteStopFreeBytes = math.MaxUint64
	store, err := Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	batch, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Create(ctx, []byte("blocked")); !errors.Is(err, ErrInsufficientSpace) {
		t.Fatalf("create record err=%v", err)
	}
	if err := batch.Abort(ctx); err != nil {
		t.Fatalf("control append could not use reserved headroom: %v", err)
	}
}

func TestPublicMetricsReportV2Runtime(t *testing.T) {
	ctx := context.Background()
	config := testCreateConfig(filepath.Join(t.TempDir(), "store"))
	store, err := Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	committed, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := committed.Create(ctx, []byte("metrics")); err != nil {
		t.Fatal(err)
	}
	if _, err := committed.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	aborted, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := aborted.Abort(ctx); err != nil {
		t.Fatal(err)
	}

	metrics := store.Metrics()
	if metrics.CommitQueued != 1 || metrics.CommitGroups != 1 || metrics.GroupBatches != 1 || metrics.Committed != 1 || metrics.Aborted != 1 {
		t.Fatalf("pipeline metrics=%+v", metrics)
	}
	if metrics.DeltaChargedBytes == 0 || metrics.DeltaReservedBytes != 0 ||
		metrics.DeltaSoftLimitBytes != config.Runtime.DeltaSoftLimitBytes || metrics.DeltaHardLimitBytes != config.Runtime.DeltaHardLimitBytes {
		t.Fatalf("delta metrics=%+v", metrics)
	}
	if metrics.WriteStopFreeBytes != config.Runtime.WriteStopFreeBytes || metrics.DiskAvailableEstimateBytes == 0 {
		t.Fatalf("space metrics=%+v", metrics)
	}
}

func TestPublicGCThrottleUpdateAppliesToNextCompaction(t *testing.T) {
	ctx := context.Background()
	store, err := Create(ctx, testCreateConfig(filepath.Join(t.TempDir(), "store")))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetGCBytesPerSecond(12345); err != nil {
		t.Fatal(err)
	}
	if got := store.Metrics().GCBytesPerSecond; got != 12345 {
		t.Fatalf("gc bytes per second=%d", got)
	}
	if err := store.SetGCBytesPerSecond(0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("zero rate err=%v", err)
	}
	if got := store.Metrics().GCBytesPerSecond; got != 12345 {
		t.Fatalf("invalid update changed rate=%d", got)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.SetGCBytesPerSecond(1); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed update err=%v", err)
	}
}

func testCreateConfig(dir string) CreateConfig {
	return CreateConfig{
		Dir: dir,
		HardLimits: HardLimits{
			SegmentSize: 1 << 20, MaxValueSize: 1024, MaxBatchBytes: 4096,
			MaxBatchMutations: 16, MaxBatchConditions: 16, MaxOpenBatches: 4,
			MaxRecordLogPayload: 64 << 10, IDReserveSize: 16, BatchIDReserveSize: 16,
		},
		Runtime: RuntimeConfig{
			MaxQueuedBytes: 1 << 20, AppendQueueCapacity: 32, AppendBufferBytes: 64 << 10,
			AppendBufferRecords: 32, CommitQueueCapacity: 16, MaxGroupBatches: 8,
			MaxGroupPayload: 64 << 10, MappingCacheBytes: 1 << 20,
			CheckpointSortBytes: 24 << 10, MaxSegmentStats: 1024,
			DeltaSoftLimitBytes: 32 << 10, DeltaHardLimitBytes: 64 << 10,
			WriteStopFreeBytes: 1, SpaceCheckInterval: time.Second, GCBytesPerSecond: ^uint64(0),
		},
	}
}

func TestGroupCommitDelayDefaultsAndCanBeDisabled(t *testing.T) {
	defaults := (RuntimeConfig{}).engineConfig()
	if defaults.Commit.GroupCommitDelay != 50*time.Microsecond {
		t.Fatalf("default group commit delay=%v", defaults.Commit.GroupCommitDelay)
	}
	disabled := (RuntimeConfig{DisableGroupCommitDelay: true}).engineConfig()
	if disabled.Commit.GroupCommitDelay != 0 {
		t.Fatalf("disabled group commit delay=%v", disabled.Commit.GroupCommitDelay)
	}
	explicit := (RuntimeConfig{GroupCommitDelay: 125 * time.Microsecond}).engineConfig()
	if explicit.Commit.GroupCommitDelay != 125*time.Microsecond {
		t.Fatalf("explicit group commit delay=%v", explicit.Commit.GroupCommitDelay)
	}
}

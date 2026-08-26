package ridstore

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
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
			CheckpointSortBytes: 16 << 10, MaxSegmentStats: 1024,
			DeltaSoftLimitBytes: 32 << 10, DeltaHardLimitBytes: 64 << 10,
		},
	}
}

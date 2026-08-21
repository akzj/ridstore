package ridstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/mapping/radix"
)

func TestCreateOpenLockAndConfig(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := Create(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{Dir: dir}); !errors.Is(err, ErrLocked) {
		t.Fatalf("concurrent Open error=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("second Close error=%v", err)
	}
	if _, err := Create(Config{Dir: dir}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("second Create error=%v", err)
	}
	reopened, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.config.SegmentSize != 256*mib || reopened.manifest.Generation != 1 {
		t.Fatalf("config=%+v manifest=%+v", reopened.config, reopened.manifest)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{Dir: dir, SegmentSize: 128 * mib}); !errors.Is(err, ErrConfigMismatch) {
		t.Fatalf("mismatched Open error=%v", err)
	}
}

func smallTestConfig(dir string) Config {
	return Config{
		Dir: dir, SegmentSize: 1 << 20, MaxValueSize: 1024, MaxBatchBytes: 4096,
		MaxBatchMutations: 16, MaxBatchConditions: 16, MaxOpenBatches: 4,
		IDReserveSize: 4, BatchIDReserveSize: 4,
		GCBatchBytes: 4096, GCBatchMutations: 16,
	}
}

func TestPublicCommitGetStatusAndRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b.ID() != 1 {
		t.Fatalf("batch ID=%d", b.ID())
	}
	id, err := b.Allocate(context.Background())
	if err != nil || id != 1 {
		t.Fatalf("id=%d error=%v", id, err)
	}
	value := []byte("value")
	if err := b.Put(context.Background(), id, value); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	commitResult, err := b.Commit(context.Background())
	if err != nil || commitResult.BatchID != 1 || commitResult.CommitSeq != 1 {
		t.Fatalf("commit=%+v error=%v", commitResult, err)
	}
	metrics := store.Metrics()
	if metrics.CommitQueued != 1 || metrics.CommitGroups != 1 || metrics.GroupBatches != 1 || metrics.Committed != 1 || metrics.WriteSyncNanos == 0 {
		t.Fatalf("metrics=%+v", metrics)
	}
	record, err := store.GetRecord(context.Background(), id)
	if err != nil || string(record.Value) != "value" || record.Revision != 1 {
		t.Fatalf("record=%+v error=%v", record, err)
	}
	status, err := store.Status(context.Background(), b.ID())
	if err != nil || status.State != BatchStateCommitted || status.CommitSeq != 1 {
		t.Fatalf("status=%+v error=%v", status, err)
	}
	if _, err := store.Status(context.Background(), 2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unissued status error=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	record, err = store.GetRecord(context.Background(), id)
	if err != nil || string(record.Value) != "value" || record.Revision != 1 {
		t.Fatalf("recovered record=%+v error=%v", record, err)
	}
	status, err = store.Status(context.Background(), 1)
	if err != nil || status.State != BatchStateCommitted {
		t.Fatalf("recovered status=%+v error=%v", status, err)
	}
	status, err = store.Status(context.Background(), 2)
	if err != nil || status.State != BatchStateAborted {
		t.Fatalf("skipped reserve status=%+v error=%v", status, err)
	}
	b, err = store.Begin(context.Background())
	if err != nil || b.ID() != 5 {
		t.Fatalf("recovered batch ID=%d error=%v", b.ID(), err)
	}
	nextID, err := b.Allocate(context.Background())
	if err != nil || nextID != 5 {
		t.Fatalf("recovered ID=%d error=%v", nextID, err)
	}
	if err := b.Delete(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted get error=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Get(context.Background(), id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("recovered delete error=%v", err)
	}
}

func TestSegmentRotationPreservesReadsAndRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.SegmentSize = 16 << 10
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	values := make(map[ID]string)
	for i := 0; i < 24; i++ {
		b, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		id, err := b.Allocate(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		value := fmt.Sprintf("value-%02d-%s", i, strings.Repeat("x", 512))
		if err := b.Put(context.Background(), id, []byte(value)); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
		values[id] = value
	}
	if current := store.rotation.Current(); current.Generation <= 1 || len(current.SealedDataSegments) == 0 {
		t.Fatalf("rotation did not advance manifest: %+v", current)
	}
	for id, want := range values {
		got, err := store.Get(context.Background(), id)
		if err != nil || string(got) != want {
			t.Fatalf("before reopen id=%d error=%v", id, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for id, want := range values {
		got, err := store.Get(context.Background(), id)
		if err != nil || string(got) != want {
			t.Fatalf("after reopen id=%d error=%v", id, err)
		}
	}
}

func TestConcurrentCommitsShareGroupsAndRemainRecoverable(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.MaxOpenBatches = 64
	cfg.IDReserveSize = 128
	cfg.BatchIDReserveSize = 128
	cfg.MaxGroupBatches = 64
	cfg.MaxGroupDelay = 2 * time.Millisecond
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	const count = 64
	batches := make([]*Batch, count)
	ids := make([]ID, count)
	for i := range batches {
		b, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		id, err := b.Allocate(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put(context.Background(), id, []byte(fmt.Sprintf("value-%d", i))); err != nil {
			t.Fatal(err)
		}
		batches[i], ids[i] = b, id
	}
	start := make(chan struct{})
	errCh := make(chan error, count)
	var workers sync.WaitGroup
	for _, b := range batches {
		workers.Add(1)
		go func(b *Batch) {
			defer workers.Done()
			<-start
			_, err := b.Commit(context.Background())
			errCh <- err
		}(b)
	}
	close(start)
	workers.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	metrics := store.Metrics()
	if metrics.Committed != count || metrics.GroupBatches != count || metrics.CommitGroups >= count {
		t.Fatalf("group metrics=%+v", metrics)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i, id := range ids {
		value, err := store.Get(context.Background(), id)
		if err != nil || string(value) != fmt.Sprintf("value-%d", i) {
			t.Fatalf("id=%d value=%q error=%v", id, value, err)
		}
	}
}

func TestCheckpointInstallsPersistentRootStatsAndReplayCut(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]ID, 0, 8)
	for i := 0; i < 8; i++ {
		batch, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		id, err := batch.Allocate(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Put(context.Background(), id, []byte(fmt.Sprintf("checkpoint-%d", i))); err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	manifest := store.catalog.Snapshot()
	if manifest.MappingRoot == 0 || manifest.CoveredCommitSeq != 8 || manifest.StatsCoveredCommitSeq != 8 || len(manifest.SegmentStats) == 0 || store.mapping.DeltaEntries() != 0 {
		t.Fatalf("manifest=%+v delta=%d", manifest, store.mapping.DeltaEntries())
	}
	post, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	postID, err := post.Allocate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := post.Put(context.Background(), postID, []byte("after-cut")); err != nil {
		t.Fatal(err)
	}
	if _, err := post.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i, id := range ids {
		value, err := store.Get(context.Background(), id)
		if err != nil || string(value) != fmt.Sprintf("checkpoint-%d", i) {
			t.Fatalf("id=%d value=%q error=%v", id, value, err)
		}
	}
	if value, err := store.Get(context.Background(), postID); err != nil || string(value) != "after-cut" {
		t.Fatalf("post value=%q error=%v", value, err)
	}
}

func TestDeltaSoftLimitSchedulesCheckpointAfterCommit(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.DeltaSoftLimitBytes = 64
	cfg.DeltaHardLimitBytes = 128
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.Allocate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Put(context.Background(), id, []byte("soft-limit")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		metrics := store.Metrics()
		status := store.MaintenanceStatus()
		if store.catalog.Snapshot().CoveredCommitSeq >= 1 && metrics.DeltaChargedBytes == 0 && metrics.DeltaReservedBytes == 0 && !status.CheckpointPending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("automatic checkpoint did not complete: manifest=%+v metrics=%+v status=%+v", store.catalog.Snapshot(), metrics, status)
		}
		time.Sleep(time.Millisecond)
	}
	if status := store.MaintenanceStatus(); status.CheckpointPending || status.LastCheckpointError != nil {
		t.Fatalf("maintenance status=%+v", status)
	}
	if value, err := store.Get(context.Background(), id); err != nil || string(value) != "soft-limit" {
		t.Fatalf("value=%q error=%v", value, err)
	}
}

func TestCheckpointRotatesMappingSegments(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.SegmentSize = 16 << 10
	cfg.MaxOpenBatches = 128
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]ID, 0, 80)
	for i := 1; i <= 80; i++ {
		id := ID(uint64(i) << 20)
		batch, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Put(context.Background(), id, []byte(fmt.Sprintf("sparse-%d", i))); err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	manifest := store.catalog.Snapshot()
	if len(manifest.SealedMappingSegments) == 0 || manifest.ActiveMapSegmentID == 1 {
		t.Fatalf("mapping rotation missing: %+v", manifest)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i, id := range ids {
		value, err := store.Get(context.Background(), id)
		if err != nil || string(value) != fmt.Sprintf("sparse-%d", i+1) {
			t.Fatalf("id=%d value=%q error=%v", id, value, err)
		}
	}
}

func TestCompactMappingReclaimsOldGenerationAndReopens(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.SegmentSize = 16 << 10
	cfg.MaxOpenBatches = 128
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]ID, 0, 80)
	for i := 0; i < 80; i++ {
		b, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		id, err := b.Allocate(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put(context.Background(), id, []byte("generation-0")); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	for generation := 1; generation <= 3; generation++ {
		for _, id := range ids {
			b, err := store.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := b.Put(context.Background(), id, []byte(fmt.Sprintf("generation-%d", generation))); err != nil {
				t.Fatal(err)
			}
			if _, err := b.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if err := store.Checkpoint(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	before := store.catalog.Snapshot()
	spaceBefore, err := store.MappingSpaceUsage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if spaceBefore.UnreachableBytes == 0 {
		t.Fatalf("expected COW garbage before compaction: %+v", spaceBefore)
	}
	oldIDs := make(map[uint32]struct{}, len(before.SealedMappingSegments)+1)
	oldIDs[uint32(before.ActiveMapSegmentID)] = struct{}{}
	for _, summary := range before.SealedMappingSegments {
		oldIDs[summary.FileID] = struct{}{}
	}
	if err := store.CompactMapping(context.Background()); err != nil {
		t.Fatal(err)
	}
	after := store.catalog.Snapshot()
	spaceAfter, err := store.MappingSpaceUsage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if spaceAfter.UnreachableBytes != 0 || spaceAfter.TotalBytes != spaceAfter.ReachableBytes {
		t.Fatalf("mapping space did not converge: before=%+v after=%+v", spaceBefore, spaceAfter)
	}
	if after.MaintenanceGeneration <= before.MaintenanceGeneration || after.MappingRoot == 0 || after.CoveredCommitSeq != before.CoveredCommitSeq {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
	for id := range oldIDs {
		if id == uint32(after.ActiveMapSegmentID) {
			t.Fatalf("old active mapping ID %d was reused", id)
		}
		for _, summary := range after.SealedMappingSegments {
			if summary.FileID == id {
				t.Fatalf("old sealed mapping ID %d remained", id)
			}
		}
		for _, name := range []string{fmt.Sprintf("MAP-%08d.active", id), fmt.Sprintf("MAP-%08d.seg", id)} {
			if _, err := os.Stat(filepath.Join(dir, "mapping", name)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("old mapping file %s still exists: %v", name, err)
			}
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, id := range ids {
		value, err := store.Get(context.Background(), id)
		if err != nil || string(value) != "generation-3" {
			t.Fatalf("id=%d value=%q error=%v", id, value, err)
		}
	}
}

func TestCompactMappingPreservesConcurrentCommitDelta(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := b.Allocate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put(context.Background(), id, []byte("before-gc")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	store.mapping.SetHook(failpoint.Func(func(point failpoint.Point) error {
		if point == radix.PointMappingGCFilesDurable {
			close(entered)
			<-release
		}
		return nil
	}))
	compactResult := make(chan error, 1)
	go func() { compactResult <- store.CompactMapping(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("mapping GC did not reach files-durable boundary")
	}
	b, err = store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put(context.Background(), id, []byte("during-gc")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-compactResult; err != nil {
		t.Fatal(err)
	}
	if value, err := store.Get(context.Background(), id); err != nil || string(value) != "during-gc" {
		t.Fatalf("runtime value=%q error=%v", value, err)
	}
	store.mapping.SetHook(nil)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if value, err := store.Get(context.Background(), id); err != nil || string(value) != "during-gc" {
		t.Fatalf("recovered value=%q error=%v", value, err)
	}
}

func BenchmarkConcurrentDurableCommit(b *testing.B) {
	cfg := smallTestConfig(filepath.Join(b.TempDir(), "store"))
	cfg.MaxOpenBatches = 1024
	cfg.IDReserveSize = 1 << 16
	cfg.BatchIDReserveSize = 1 << 16
	cfg.MaxGroupDelay = 200 * time.Microsecond
	store, err := Create(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			batch, err := store.Begin(context.Background())
			if err != nil {
				b.Error(err)
				return
			}
			id, err := batch.Allocate(context.Background())
			if err == nil {
				err = batch.Put(context.Background(), id, []byte("benchmark"))
			}
			if err == nil {
				_, err = batch.Commit(context.Background())
			}
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func TestPublicConditionalConflictAndOpenBatchBackpressure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.MaxOpenBatches = 1
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	b1, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := store.Begin(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("backpressure error=%v", err)
	}
	if err := b1.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	b2, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := b2.ExpectAbsent(1); err != nil {
		t.Fatal(err)
	}
	if err := b2.Put(context.Background(), 1, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := b2.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	b3, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := b3.ExpectAbsent(1); err != nil {
		t.Fatal(err)
	}
	if err := b3.Put(context.Background(), 1, []byte("second")); err != nil {
		t.Fatal(err)
	}
	if _, err := b3.Commit(context.Background()); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict error=%v", err)
	}
	value, err := store.Get(context.Background(), 1)
	if err != nil || string(value) != "first" {
		t.Fatalf("value=%q error=%v", value, err)
	}
}

func TestOpenBatchPinsAndReleasesReferencedSegments(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := Create(smallTestConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	b, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := b.Allocate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put(context.Background(), id, []byte("pinned")); err != nil {
		t.Fatal(err)
	}
	segmentID := store.segments.Active().SegmentID()
	if refs := store.segments.OpenBatchRefs(segmentID); refs != 1 {
		t.Fatalf("open refs=%d", refs)
	}
	if err := b.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if refs := store.segments.OpenBatchRefs(segmentID); refs != 0 {
		t.Fatalf("released refs=%d", refs)
	}
}

func TestCloseWakesBlockedBeginAndAbortsOpenBatch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.MaxOpenBatches = 1
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Begin(context.Background()); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := store.Begin(context.Background())
		result <- err
	}()
	time.Sleep(10 * time.Millisecond)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("blocked Begin error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Begin was not released by Close")
	}
}

func TestOpenDoesNotCreate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing")
	if _, err := Open(Config{Dir: dir}); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("error=%v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Open created directory: %v", err)
	}
}

func TestCreateRejectsNonEmptyAndSymlinkDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "foreign"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(Config{Dir: dir}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("non-empty Create error=%v", err)
	}
	link := filepath.Join(t.TempDir(), "store-link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(Config{Dir: link}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("symlink Create error=%v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []Config{
		{Dir: t.TempDir(), SegmentSize: -1},
		{Dir: t.TempDir(), SegmentSize: 8 << 20, MaxValueSize: 16 << 20},
		{Dir: t.TempDir(), DeltaSoftLimitBytes: 2, DeltaHardLimitBytes: 1},
		{Dir: t.TempDir(), CheckpointMemoryBytes: 32 << 10},
		{Dir: t.TempDir(), MaxBatchBytes: 1 << 20, GCBatchBytes: 2 << 20},
		{Dir: t.TempDir(), MaxGroupDelay: -1},
		{Dir: t.TempDir(), GCMinFreeBytes: -1},
		{Dir: t.TempDir(), GCBytesPerSecond: -1},
	}
	for i, cfg := range cases {
		if _, _, err := normalizeCreateConfig(cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("case %d error=%v", i, err)
		}
	}
}

func TestConfigAllowsExactFourGiBSegment(t *testing.T) {
	cfg, hard, err := normalizeCreateConfig(Config{Dir: t.TempDir(), SegmentSize: int64(1) << 32})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SegmentSize != int64(1)<<32 || hard.SegmentSize != uint64(1)<<32 {
		t.Fatalf("config=%d hard=%d", cfg.SegmentSize, hard.SegmentSize)
	}
}

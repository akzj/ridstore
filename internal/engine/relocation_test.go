package engine

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

func TestRelocateSegmentCopiesLivePutAndPreservesOrigin(t *testing.T) {
	store, source, id, oldAddr, origin := relocationFixture(t)

	result, err := store.RelocateSegment(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if result.LiveCandidates == 0 || result.Applied == 0 || result.Applied != result.CopiedRecords || result.Skipped != 0 ||
		result.FirstCommitSeq == 0 || result.LastCommitSeq < result.FirstCommitSeq {
		t.Fatalf("result=%+v", result)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil || record.Addr == oldAddr || string(record.Value) != "source-value" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	payload, err := store.log.Read(context.Background(), record.Addr)
	if err != nil {
		t.Fatal(err)
	}
	put, err := recordcodec.DecodePut(payload, store.limits.MaxValueSize)
	if err != nil || put.OriginBatchID != origin || put.RecordID != id {
		t.Fatalf("put=%+v err=%v", put, err)
	}
}

func TestRelocateSegmentReportsCommitSequenceRangeAcrossBatches(t *testing.T) {
	store, source, _, _, _ := relocationFixture(t)
	store.maxRelocationMutations = 1

	result, err := store.RelocateSegment(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if result.Applied < 2 || result.FirstCommitSeq == 0 ||
		uint64(result.LastCommitSeq-result.FirstCommitSeq)+1 != result.Applied {
		t.Fatalf("result=%+v", result)
	}
}

func TestPrepareSegmentRetirementCheckpointsAndProvesNoLiveMapping(t *testing.T) {
	store, source, _, _, _ := relocationFixture(t)
	proof, relocated, err := store.PrepareSegmentRetirement(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if proof.Source.SegmentID != source || proof.CatalogGeneration == 0 ||
		proof.CoveredCommitSeq < relocated.LastCommitSeq || !proof.ReplayStart.Valid() {
		t.Fatalf("proof=%+v relocated=%+v", proof, relocated)
	}
	manifest := store.catalog.Snapshot()
	if manifest.Generation != proof.CatalogGeneration || manifest.CoveredCommitSeq != proof.CoveredCommitSeq {
		t.Fatalf("manifest=%+v proof=%+v", manifest, proof)
	}
	for _, stat := range manifest.SegmentStats {
		if stat.SegmentID == source && (stat.LiveBytes != 0 || stat.LiveRecords != 0) {
			t.Fatalf("source remains live in checkpoint: %+v", stat)
		}
	}
}

func TestPrepareSegmentRetirementRejectsOpenBatchReference(t *testing.T) {
	store := newRelocationStore(t)
	pending, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.Create(context.Background(), []byte("open-before-rotation")); err != nil {
		t.Fatal(err)
	}
	source := recordlog.SegmentID(1)
	for store.catalog.Snapshot().ActiveDataSegmentID == source {
		filler, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := filler.Create(context.Background(), bytes.Repeat([]byte{'x'}, 512)); err != nil {
			t.Fatal(err)
		}
		if _, err := filler.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := store.PrepareSegmentRetirement(context.Background(), source); !errors.Is(err, base.ErrConflict) {
		t.Fatalf("prepare err=%v", err)
	}
	if err := pending.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	proof, _, err := store.PrepareSegmentRetirement(context.Background(), source)
	if err != nil || proof.Source.SegmentID != source {
		t.Fatalf("proof=%+v err=%v", proof, err)
	}
}

func TestCompactSegmentRetiresSourceAndKeepsRecordsReadable(t *testing.T) {
	store, source, id, oldAddr, _ := relocationFixture(t)
	result, err := store.CompactSegment(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if result.Proof.Source.SegmentID != source || result.Proof.CatalogGeneration == 0 {
		t.Fatalf("result=%+v", result)
	}
	if containsSealedSegment(store.catalog.Snapshot(), result.Proof.Source) {
		t.Fatal("retired source remains in Catalog")
	}
	if _, err := store.log.Read(context.Background(), oldAddr); !errors.Is(err, recordlog.ErrSegmentMissing) {
		t.Fatalf("old address read err=%v", err)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil || string(record.Value) != "source-value" || record.Addr == oldAddr {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

func TestCompactNextSegmentSelectsAndRetiresOneCandidate(t *testing.T) {
	store, source, id, _, _ := relocationFixture(t)
	result, found, err := store.CompactNextSegment(context.Background(), CompactionPolicy{})
	if err != nil || !found {
		t.Fatalf("result=%+v found=%v err=%v", result, found, err)
	}
	if result.Candidate.Source.SegmentID != source || result.Compaction.Proof.Source.SegmentID != source {
		t.Fatalf("result=%+v", result)
	}
	if containsSealedSegment(store.catalog.Snapshot(), result.Candidate.Source) {
		t.Fatal("selected source remains in Catalog")
	}
	if record, err := store.Get(context.Background(), id); err != nil || string(record.Value) != "source-value" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

func TestCompactNextSegmentHonorsPolicy(t *testing.T) {
	store, source, _, _, _ := relocationFixture(t)
	result, found, err := store.CompactNextSegment(context.Background(), CompactionPolicy{MinReclaimableBytes: ^uint64(0)})
	if err != nil || found || result != (NextSegmentCompactionResult{}) {
		t.Fatalf("result=%+v found=%v err=%v", result, found, err)
	}
	if !containsSegmentID(store.catalog.Snapshot().SealedDataSegments, source) {
		t.Fatal("policy-rejected source was retired")
	}
}

func TestCompactNextSegmentSkipsOpenBatchReferences(t *testing.T) {
	store := newRelocationStore(t)
	pending, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.Create(context.Background(), []byte("open-before-rotation")); err != nil {
		t.Fatal(err)
	}
	source := recordlog.SegmentID(1)
	for store.catalog.Snapshot().ActiveDataSegmentID == source {
		filler, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := filler.Create(context.Background(), bytes.Repeat([]byte{'x'}, 512)); err != nil {
			t.Fatal(err)
		}
		if _, err := filler.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if _, found, err := store.CompactNextSegment(context.Background(), CompactionPolicy{}); err != nil || found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if err := pending.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if result, found, err := store.CompactNextSegment(context.Background(), CompactionPolicy{}); err != nil || !found || result.Candidate.Source.SegmentID != source {
		t.Fatalf("result=%+v found=%v err=%v", result, found, err)
	}
}

func TestConcurrentUserUpdateWinsOverSegmentRelocation(t *testing.T) {
	store, source, id, oldAddr, _ := relocationFixture(t)
	underlying := store.log
	underlyingMaintenance := store.maintenance
	blocked := &blockingCopyLog{
		Log: underlying, maintenanceLog: underlyingMaintenance, target: id, value: []byte("source-value"),
		reached: make(chan struct{}), release: make(chan struct{}),
	}
	store.log = blocked
	store.maintenance = blocked

	type relocationAnswer struct {
		result SegmentRelocationResult
		err    error
	}
	done := make(chan relocationAnswer, 1)
	go func() {
		result, err := store.RelocateSegment(context.Background(), source)
		done <- relocationAnswer{result: result, err: err}
	}()
	<-blocked.reached

	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.CompareAndPut(context.Background(), id, oldAddr, []byte("user-wins")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(blocked.release)
	got := <-done
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.result.Skipped == 0 {
		t.Fatalf("result=%+v", got.result)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil || string(record.Value) != "user-wins" || record.Addr == oldAddr {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

type blockingCopyLog struct {
	Log
	maintenanceLog
	target  model.ID
	value   []byte
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (l *blockingCopyLog) Append(ctx context.Context, payload []byte, syncWrite bool) (recordlog.AppendResult, error) {
	result, err := l.Log.Append(ctx, payload, syncWrite)
	if err != nil || syncWrite {
		return result, err
	}
	put, decodeErr := recordcodec.DecodePut(payload, 1<<20)
	if decodeErr == nil && put.RecordID == l.target && bytes.Equal(put.Value, l.value) {
		l.once.Do(func() {
			close(l.reached)
			<-l.release
		})
	}
	return result, err
}

func relocationFixture(t *testing.T) (*Store, recordlog.SegmentID, model.ID, recordlog.VAddr, model.BatchID) {
	t.Helper()
	store := newRelocationStore(t)

	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	origin := batch.ID()
	id, err := batch.Create(context.Background(), []byte("source-value"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	source := record.Addr.SegmentID()
	for store.catalog.Snapshot().ActiveDataSegmentID == source {
		filler, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := filler.Create(context.Background(), bytes.Repeat([]byte{'x'}, 512)); err != nil {
			t.Fatal(err)
		}
		if _, err := filler.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return store, source, id, record.Addr, origin
}

func newRelocationStore(t *testing.T) *Store {
	t.Helper()
	config := relocationConfig()
	store, err := Create(context.Background(), t.TempDir(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil && !errors.Is(err, recordlog.ErrClosed) && !errors.Is(err, base.ErrClosed) {
			t.Errorf("close: %v", err)
		}
	})
	return store
}

func relocationConfig() CreateConfig {
	config := testCreateConfig()
	config.HardLimits.SegmentSize = 8192
	config.HardLimits.MaxValueSize = 512
	config.HardLimits.MaxBatchBytes = 2048
	config.HardLimits.MaxBatchMutations = 4
	config.HardLimits.MaxRecordLogPayload = 1024
	config.Runtime.RecordLog.BufferBytes = 2048
	config.Runtime.Commit.MaxGroupPayload = 1024
	return config
}

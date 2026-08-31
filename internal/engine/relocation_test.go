package engine

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/compactionstate"
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

func TestRelocateSegmentAccountsForRateLimitDelay(t *testing.T) {
	store, source, _, _, _ := relocationFixture(t)
	now := time.Unix(100, 0)
	var waited time.Duration
	store.gcBytesPerSecond.Store(1024)
	store.gcNow = func() time.Time { return now }
	store.gcWait = func(_ context.Context, delay time.Duration) error {
		waited += delay
		now = now.Add(delay)
		return nil
	}
	if _, err := store.RelocateSegment(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	if waited == 0 || store.Metrics().GCThrottledNanos != uint64(waited) {
		t.Fatalf("waited=%v metrics=%+v", waited, store.Metrics())
	}
}

func TestRelocateSegmentCancellationDuringPacingReleasesSourcePin(t *testing.T) {
	store, source, _, _, _ := relocationFixture(t)
	now := time.Unix(100, 0)
	store.gcBytesPerSecond.Store(1)
	store.gcNow = func() time.Time { return now }
	store.gcWait = func(context.Context, time.Duration) error { return context.Canceled }
	partial, err := store.RelocateSegment(context.Background(), source)
	if !errors.Is(err, context.Canceled) || partial.Applied == 0 {
		t.Fatalf("partial=%+v err=%v", partial, err)
	}
	store.gcBytesPerSecond.Store(^uint64(0))
	store.gcWait = func(_ context.Context, delay time.Duration) error {
		now = now.Add(delay)
		return nil
	}
	if _, err := store.RelocateSegment(context.Background(), source); err != nil {
		t.Fatalf("retry after paced cancellation: %v", err)
	}
}

func TestRelocateSegmentOrdersChangesByRecordID(t *testing.T) {
	store := newRelocationStore(t)
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	low, err := batch.Create(context.Background(), []byte("low-initial"))
	if err != nil {
		t.Fatal(err)
	}
	high, err := batch.Create(context.Background(), []byte("high-initial"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, update := range []struct {
		id    model.ID
		value string
	}{{high, "high-current"}, {low, "low-current"}} {
		batch, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Put(context.Background(), update.id, []byte(update.value)); err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	source := store.catalog.Snapshot().ActiveDataSegmentID
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

	store.maxRelocationBytes = 1
	result, err := store.RelocateSegment(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if result.Applied < 2 {
		t.Fatalf("result=%+v", result)
	}
	if uint64(result.LastCommitSeq-result.FirstCommitSeq)+1 != result.Applied {
		t.Fatalf("byte budget did not isolate oversized records: %+v", result)
	}
	for id, want := range map[model.ID]string{low: "low-current", high: "high-current"} {
		record, err := store.Get(context.Background(), id)
		if err != nil || string(record.Value) != want || record.Addr.SegmentID() == source {
			t.Fatalf("id=%d record=%+v err=%v", id, record, err)
		}
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
	if !recordlog.IsCompactionSegment(record.Addr.SegmentID()) || record.Addr.SegmentID() == store.catalog.Snapshot().ActiveDataSegmentID {
		t.Fatalf("GC copy was not isolated from user active segment: addr=%v active=%d", record.Addr, store.catalog.Snapshot().ActiveDataSegmentID)
	}
}

func TestCompactSegmentRejectsOpenBatchReferenceBeforeJournal(t *testing.T) {
	store := newRelocationStore(t)
	pending, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.Create(context.Background(), []byte("open-before-rotation")); err != nil {
		t.Fatal(err)
	}
	source := store.catalog.Snapshot().ActiveDataSegmentID
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
	if _, err := store.CompactSegment(context.Background(), source); !errors.Is(err, base.ErrConflict) {
		t.Fatalf("compact err=%v", err)
	}
	if found, err := compactionstate.RecoveryArtifacts(store.root); err != nil || found {
		t.Fatalf("compaction marker found=%v err=%v", found, err)
	}
	if err := pending.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCompactNextSegmentSelectsAndRetiresOneCandidate(t *testing.T) {
	store, source, id, _, _ := relocationFixture(t)
	result, found, err := store.CompactNextSegment(context.Background(), CompactionPolicy{BypassCooldown: true})
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
	metrics := store.Metrics()
	if metrics.GCStarted != 1 || metrics.GCCompleted != 1 || metrics.GCFailed != 0 || metrics.GCCopiedBytes == 0 || metrics.GCRelocated == 0 || metrics.GCDurationNanos == 0 {
		t.Fatalf("metrics=%+v", metrics)
	}
}

func TestCompactNextSegmentRewritesAdjacentInputsIntoDedicatedOutput(t *testing.T) {
	store := newRelocationStore(t)
	store.maxRelocationMutations = 1
	bySegment := make(map[recordlog.SegmentID][]model.ID)
	for store.catalog.Snapshot().ActiveDataSegmentID < 4 {
		batch, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		id, err := batch.Create(context.Background(), bytes.Repeat([]byte{'a'}, 512))
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
		bySegment[record.Addr.SegmentID()] = append(bySegment[record.Addr.SegmentID()], id)
	}
	for _, segment := range []recordlog.SegmentID{1, 2} {
		ids := bySegment[segment]
		if len(ids) < 2 {
			t.Fatalf("segment %d has only %d puts", segment, len(ids))
		}
		for _, id := range ids[1:] {
			batch, err := store.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := batch.Put(context.Background(), id, []byte("new")); err != nil {
				t.Fatal(err)
			}
			if _, err := batch.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
	result, found, err := store.CompactNextSegment(context.Background(), CompactionPolicy{BypassCooldown: true, MaxInputSegments: 2, MinReclaimableRatioBasis: 5_000})
	if err != nil || !found {
		t.Fatalf("result=%+v found=%v err=%v", result, found, err)
	}
	if len(result.Candidate.Sources) != 2 || result.Candidate.Sources[0].SegmentID != 1 || result.Candidate.Sources[1].SegmentID != 2 {
		t.Fatalf("candidate=%+v", result.Candidate)
	}
	if result.Compaction.Relocation.Applied < 2 ||
		uint64(result.Compaction.Relocation.LastCommitSeq-result.Compaction.Relocation.FirstCommitSeq)+1 != result.Compaction.Relocation.Applied {
		t.Fatalf("compaction output was not published in bounded batches: %+v", result.Compaction.Relocation)
	}
	manifest := store.catalog.Snapshot()
	if containsSegmentID(manifest.SealedDataSegments, 1) || containsSegmentID(manifest.SealedDataSegments, 2) {
		t.Fatal("adjacent inputs were not retired atomically")
	}
	for _, segment := range []recordlog.SegmentID{1, 2} {
		record, err := store.Get(context.Background(), bySegment[segment][0])
		if err != nil || !recordlog.IsCompactionSegment(record.Addr.SegmentID()) {
			t.Fatalf("segment=%d record=%+v err=%v", segment, record, err)
		}
	}
}

func TestCompactSegmentPacesBeforePublishingOutputs(t *testing.T) {
	store, source, _, _, _ := relocationFixture(t)
	store.maxRelocationBytes = 1
	store.gcBytesPerSecond.Store(1)
	now := time.Unix(100, 0)
	store.gcNow = func() time.Time { return now }
	var waits int
	store.gcWait = func(_ context.Context, delay time.Duration) error {
		waits++
		for _, segment := range store.catalog.Snapshot().SealedDataSegments {
			if recordlog.IsCompactionSegment(segment.SegmentID) {
				t.Fatal("compaction output was published before copy pacing")
			}
		}
		now = now.Add(delay)
		return nil
	}
	if _, err := store.CompactSegment(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	if waits == 0 {
		t.Fatal("compaction copy was not paced")
	}
}

func TestCompactSegmentCancellationBeforeOutputPublicationRollsBack(t *testing.T) {
	store, source, id, oldAddr, _ := relocationFixture(t)
	store.maxRelocationBytes = 1
	store.gcBytesPerSecond.Store(1)
	store.gcNow = func() time.Time { return time.Unix(100, 0) }
	store.gcWait = func(context.Context, time.Duration) error { return context.Canceled }
	if _, err := store.CompactSegment(context.Background(), source); !errors.Is(err, context.Canceled) {
		t.Fatalf("compact err=%v", err)
	}
	if store.fault != nil {
		t.Fatalf("recoverable cancellation faulted store: %v", store.fault)
	}
	if found, err := compactionstate.RecoveryArtifacts(store.root); err != nil || found {
		t.Fatalf("compaction marker found=%v err=%v", found, err)
	}
	for _, segment := range store.catalog.Snapshot().SealedDataSegments {
		if recordlog.IsCompactionSegment(segment.SegmentID) {
			t.Fatalf("unpublished output remains in Catalog: %d", segment.SegmentID)
		}
	}
	record, err := store.Get(context.Background(), id)
	if err != nil || record.Addr != oldAddr {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

func TestCompactNextSegmentHonorsPolicy(t *testing.T) {
	store, source, _, _, _ := relocationFixture(t)
	result, found, err := store.CompactNextSegment(context.Background(), CompactionPolicy{MinReclaimableBytes: ^uint64(0)})
	if err != nil || found || result.Candidate.Source.SegmentID != 0 || result.Compaction.Relocation.ScannedRecords != 0 {
		t.Fatalf("result=%+v found=%v err=%v", result, found, err)
	}
	if !containsSegmentID(store.catalog.Snapshot().SealedDataSegments, source) {
		t.Fatal("policy-rejected source was retired")
	}
	if metrics := store.Metrics(); metrics.GCNoCandidate != 1 || metrics.GCStarted != 0 {
		t.Fatalf("metrics=%+v", metrics)
	}
}

func TestRelocateSegmentRejectsInsufficientCopySpaceWithoutMutation(t *testing.T) {
	store, source, id, oldAddr, _ := relocationFixture(t)
	store.space = newSpaceGate("test", 1, time.Hour, func(string) (uint64, error) { return 1, nil })
	store.gcMinFreeBytes = 1
	if _, err := store.RelocateSegment(context.Background(), source); !errors.Is(err, base.ErrInsufficientSpace) {
		t.Fatalf("relocate err=%v", err)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil || record.Addr != oldAddr || string(record.Value) != "source-value" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if metrics := store.Metrics(); metrics.GCSpaceRejections != 1 {
		t.Fatalf("metrics=%+v", metrics)
	}
}

func TestCompactSegmentKeepsSourceWhenCheckpointSpaceIsInsufficient(t *testing.T) {
	store, source, id, oldAddr, _ := relocationFixture(t)
	manifest := store.catalog.Snapshot()
	var summary recordlog.SegmentSummary
	for _, candidate := range manifest.SealedDataSegments {
		if candidate.SegmentID == source {
			summary = candidate
			break
		}
	}
	commitPhysical, err := recordlog.PhysicalRecordSize(uint64(recordcodec.CommitGroupHeadSize + recordcodec.DescriptorHeadSize + recordcodec.MutationSize))
	if err != nil {
		t.Fatal(err)
	}
	reservePhysical, err := recordlog.PhysicalRecordSize(uint64(recordcodec.FixedRecordSize))
	if err != nil {
		t.Fatal(err)
	}
	copyEstimate := uint64(summary.ValidEnd-recordlog.SegmentHeaderSize) +
		uint64(summary.RecordCount)*(uint64(commitPhysical)+uint64(reservePhysical)) + 2*manifest.HardLimits.SegmentSize
	store.space = newSpaceGate("test", 1, time.Hour, func(string) (uint64, error) { return copyEstimate + 1, nil })
	store.gcMinFreeBytes = 1
	result, err := store.compactSegmentLocked(context.Background(), source, store.gcBytesPerSecond.Load())
	if !errors.Is(err, base.ErrInsufficientSpace) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if !containsSegmentID(store.catalog.Snapshot().SealedDataSegments, source) {
		t.Fatal("source retired after checkpoint admission failure")
	}
	record, getErr := store.Get(context.Background(), id)
	if getErr != nil || record.Addr == oldAddr || string(record.Value) != "source-value" {
		t.Fatalf("record=%+v err=%v", record, getErr)
	}
	if metrics := store.Metrics(); metrics.GCSpaceRejections != 1 {
		t.Fatalf("metrics=%+v", metrics)
	}
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatalf("checkpoint retry after GC admission failure: %v", err)
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
	if result, found, err := store.CompactNextSegment(context.Background(), CompactionPolicy{BypassCooldown: true}); err != nil || !found || result.Candidate.Source.SegmentID != source {
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

func TestCheckpointDoesNotWaitForRelocationCopy(t *testing.T) {
	store, source, id, _, _ := relocationFixture(t)
	underlying := store.log
	blocked := &blockingCopyLog{
		Log: underlying, maintenanceLog: store.maintenance, target: id, value: []byte("source-value"),
		reached: make(chan struct{}), release: make(chan struct{}),
	}
	store.log = blocked
	store.maintenance = blocked
	released := false
	defer func() {
		if !released {
			close(blocked.release)
		}
	}()

	relocationDone := make(chan error, 1)
	go func() {
		_, err := store.RelocateSegment(context.Background(), source)
		relocationDone <- err
	}()
	<-blocked.reached

	checkpointDone := make(chan error, 1)
	go func() { checkpointDone <- store.Checkpoint(context.Background()) }()
	select {
	case err := <-checkpointDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Checkpoint blocked behind Relocation copy")
	}
	close(blocked.release)
	released = true
	if err := <-relocationDone; err != nil {
		t.Fatal(err)
	}
}

func TestRetirementProofDoesNotBlockUserCommit(t *testing.T) {
	store, source, id, _, _ := relocationFixture(t)
	relocated, err := store.RelocateSegment(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.checkpoint(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	blocked := &blockingProofLog{
		maintenanceLog: store.maintenance, source: source,
		reached: make(chan struct{}), release: make(chan struct{}),
	}
	store.maintenance = blocked
	released := false
	defer func() {
		if !released {
			close(blocked.release)
		}
	}()
	proofDone := make(chan error, 1)
	go func() {
		_, err := store.proveSegmentRetirement(context.Background(), source, relocated.LastCommitSeq)
		proofDone <- err
	}()
	<-blocked.reached

	commitDone := make(chan error, 1)
	go func() {
		batch, err := store.Begin(context.Background())
		if err == nil {
			err = batch.Put(context.Background(), id, []byte("during-proof"))
		}
		if err == nil {
			_, err = batch.Commit(context.Background())
		}
		commitDone <- err
	}()
	select {
	case err := <-commitDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("user Commit blocked behind retirement proof scan")
	}
	close(blocked.release)
	released = true
	if err := <-proofDone; err != nil {
		t.Fatal(err)
	}
}

func TestRetirementProofDrainsInFlightBatchMutationBeforeScanning(t *testing.T) {
	store, source, _, _, _ := relocationFixture(t)
	relocated, err := store.RelocateSegment(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.checkpoint(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	blocked := &blockingProofLog{
		maintenanceLog: store.maintenance, source: source,
		reached: make(chan struct{}), release: make(chan struct{}),
	}
	store.maintenance = blocked
	released := false
	defer func() {
		if !released {
			close(blocked.release)
		}
	}()

	// This models the interval after a Put Record append and before its final
	// Batch mutation becomes visible. Retirement must drain that interval before
	// it may inspect open-Batch references or scan the source.
	store.mutationFence.RLock()
	proofDone := make(chan error, 1)
	go func() {
		_, err := store.proveSegmentRetirement(context.Background(), source, relocated.LastCommitSeq)
		proofDone <- err
	}()
	select {
	case <-blocked.reached:
		store.mutationFence.RUnlock()
		t.Fatal("retirement scan crossed an in-flight Batch mutation")
	case <-time.After(20 * time.Millisecond):
	}
	store.mutationFence.RUnlock()
	select {
	case <-blocked.reached:
	case <-time.After(time.Second):
		t.Fatal("retirement did not resume after Batch mutation drained")
	}
	close(blocked.release)
	released = true
	if err := <-proofDone; err != nil {
		t.Fatal(err)
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

type blockingProofLog struct {
	maintenanceLog
	source  recordlog.SegmentID
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (l *blockingProofLog) ScanSegment(ctx context.Context, source recordlog.SegmentID, visit func(recordlog.AppendResult, []byte) error) error {
	if source == l.source {
		l.once.Do(func() {
			close(l.reached)
			<-l.release
		})
	}
	return l.maintenanceLog.ScanSegment(ctx, source, visit)
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

package engine

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/compactionstate"
	"github.com/akzj/ridstore/internal/coordinator"
	"github.com/akzj/ridstore/internal/mapping"
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
	payload, err := store.core.log.Read(context.Background(), record.Addr)
	if err != nil {
		t.Fatal(err)
	}
	put, err := recordcodec.DecodePut(payload, store.state.limits.MaxValueSize)
	if err != nil || put.OriginBatchID != origin || put.RecordID != id {
		t.Fatalf("put=%+v err=%v", put, err)
	}
}

func TestRelocationWaitsForCheckpointUnderDeltaHardPressure(t *testing.T) {
	config := testCreateConfig()
	config.Runtime.CheckpointSortBytes = 24
	config.Runtime.DeltaSoftLimitBytes = 32
	config.Runtime.DeltaHardLimitBytes = 64
	store, err := Create(context.Background(), filepath.Join(t.TempDir(), "store"), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	rootBatch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := rootBatch.Create(context.Background(), []byte("root"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rootBatch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	old, exists, err := store.core.mapping.LookupRef(id)
	if err != nil || !exists {
		t.Fatalf("old=%+v exists=%v err=%v", old, exists, err)
	}
	payload, err := store.core.log.Read(context.Background(), old.Addr)
	if err != nil {
		t.Fatal(err)
	}
	copied, err := store.core.log.Append(context.Background(), payload, false)
	if err != nil {
		t.Fatal(err)
	}
	newRef, err := copied.Ref()
	if err != nil {
		t.Fatal(err)
	}
	rawBatchID, err := store.core.batches.Allocate(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	store.checkpoints.captureMu.Lock()
	locked := true
	defer func() {
		if locked {
			store.checkpoints.captureMu.Unlock()
		}
	}()
	filler, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := filler.Create(context.Background(), []byte("fill Delta")); err != nil {
		t.Fatal(err)
	}
	if _, err := filler.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	coalescedBefore := store.Metrics().MaintenanceCoalesced
	go func() {
		_, relocateErr := store.relocateWithBudgetRetry(context.Background(), coordinator.Relocation{
			BatchID: model.BatchID(rawBatchID), LogicalPayloadBytes: 4,
			Changes: []mapping.Change{{RecordID: id, ExpectedOldAddr: old.Addr, NewRef: newRef, Operation: mapping.OperationRelocate}},
		})
		done <- relocateErr
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		waiting := store.Metrics().MaintenanceCoalesced > coalescedBefore
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("relocation did not wait for checkpoint pressure")
		}
		time.Sleep(time.Millisecond)
	}
	store.checkpoints.captureMu.Unlock()
	locked = false
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("relocation did not resume after checkpoint")
	}
	if current, exists, err := store.core.mapping.LookupRef(id); err != nil || !exists || current != newRef {
		t.Fatalf("current=%+v exists=%v err=%v", current, exists, err)
	}
}

func TestRelocateSegmentReportsCommitSequenceRangeAcrossBatches(t *testing.T) {
	store, source, _, _, _ := relocationFixture(t)
	store.maintenance.maxRelocationMutations = 1

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
	store.maintenance.gcBytesPerSecond.Store(1024)
	store.maintenance.gcNow = func() time.Time { return now }
	store.maintenance.gcWait = func(_ context.Context, delay time.Duration) error {
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
	store.maintenance.gcBytesPerSecond.Store(1)
	store.maintenance.gcNow = func() time.Time { return now }
	store.maintenance.gcWait = func(context.Context, time.Duration) error { return context.Canceled }
	partial, err := store.RelocateSegment(context.Background(), source)
	if !errors.Is(err, context.Canceled) || partial.Applied == 0 {
		t.Fatalf("partial=%+v err=%v", partial, err)
	}
	store.maintenance.gcBytesPerSecond.Store(^uint64(0))
	store.maintenance.gcWait = func(_ context.Context, delay time.Duration) error {
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
	source := store.core.catalog.Snapshot().ActiveDataSegmentID
	for store.core.catalog.Snapshot().ActiveDataSegmentID == source {
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

	store.maintenance.maxRelocationBytes = 1
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
	manifest := store.core.catalog.Snapshot()
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
	for store.core.catalog.Snapshot().ActiveDataSegmentID == source {
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

func TestCompactionRetirementRejectsStalePublishedGeneration(t *testing.T) {
	store, source, _, _, _ := relocationFixture(t)
	proof, _, err := store.PrepareSegmentRetirement(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	manifest := store.catalogSnapshot()
	if _, _, err := store.core.publisher.ReserveCompactionSegments(manifest.Generation, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.installCompactionRetirement([]recordlog.SegmentSummary{proof.Source}, []SegmentRetirementProof{proof}); !errors.Is(err, base.ErrConflict) {
		t.Fatalf("retire with stale proof err=%v", err)
	}
	if !containsSealedSegment(store.catalogSnapshot(), proof.Source) {
		t.Fatal("stale proof retired source")
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
	if containsSealedSegment(store.core.catalog.Snapshot(), result.Proof.Source) {
		t.Fatal("retired source remains in Catalog")
	}
	if _, err := store.core.log.Read(context.Background(), oldAddr); !errors.Is(err, recordlog.ErrSegmentMissing) {
		t.Fatalf("old address read err=%v", err)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil || string(record.Value) != "source-value" || record.Addr == oldAddr {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if !recordlog.IsCompactionSegment(record.Addr.SegmentID()) || record.Addr.SegmentID() == store.core.catalog.Snapshot().ActiveDataSegmentID {
		t.Fatalf("GC copy was not isolated from user active segment: addr=%v active=%d", record.Addr, store.core.catalog.Snapshot().ActiveDataSegmentID)
	}
}

func TestCompactSegmentScansEachInputOnlyOnce(t *testing.T) {
	store, source, _, _, _ := relocationFixture(t)
	counted := &countingScanLog{maintenanceLog: store.core.compactionLog, source: source}
	store.core.compactionLog = counted
	if _, err := store.CompactSegment(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	counted.mu.Lock()
	scans := counted.scans
	counted.mu.Unlock()
	if scans != 1 {
		t.Fatalf("source scans=%d, want 1", scans)
	}
}

func TestCompactSegmentRedirectsOpenBatchReference(t *testing.T) {
	store := newRelocationStore(t)
	pending, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := pending.Create(context.Background(), []byte("open-before-rotation"))
	if err != nil {
		t.Fatal(err)
	}
	source := store.core.catalog.Snapshot().ActiveDataSegmentID
	for store.core.catalog.Snapshot().ActiveDataSegmentID == source {
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
	result, err := store.CompactSegment(context.Background(), source)
	if err != nil {
		t.Fatalf("compact err=%v", err)
	}
	if containsSegmentID(store.core.catalog.Snapshot().SealedDataSegments, source) {
		t.Fatal("source referenced by open Batch was not retired")
	}
	if result.Relocation.CopiedRecords == 0 || result.Relocation.Skipped == 0 {
		t.Fatalf("result=%+v", result)
	}
	metrics := store.Metrics()
	if metrics.GCCommitRedirects != 1 || metrics.GCOpenRefsRedirected != 1 {
		t.Fatalf("ordering-fence metrics=%+v", metrics)
	}
	if _, err := pending.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil || string(record.Value) != "open-before-rotation" || record.Addr.SegmentID() == source || !recordlog.IsCompactionSegment(record.Addr.SegmentID()) {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if found, err := compactionstate.RecoveryArtifacts(store.core.root); err != nil || found {
		t.Fatalf("compaction marker found=%v err=%v", found, err)
	}
	root := store.core.root
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(context.Background(), root, relocationConfig().Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	record, err = reopened.Get(context.Background(), id)
	if err != nil || string(record.Value) != "open-before-rotation" || !recordlog.IsCompactionSegment(record.Addr.SegmentID()) {
		t.Fatalf("reopened record=%+v err=%v", record, err)
	}
}

func TestCompactSegmentRelocatesOpenBatchThatCommitsDuringCopy(t *testing.T) {
	store := newRelocationStore(t)
	pending, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := pending.Create(context.Background(), []byte("commit-during-copy"))
	if err != nil {
		t.Fatal(err)
	}
	source := store.core.catalog.Snapshot().ActiveDataSegmentID
	for store.core.catalog.Snapshot().ActiveDataSegmentID == source {
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
	blocked := &blockingProofLog{
		maintenanceLog: store.core.compactionLog, source: source,
		reached: make(chan struct{}), release: make(chan struct{}),
	}
	store.core.compactionLog = blocked
	type answer struct {
		result SegmentCompactionResult
		err    error
	}
	done := make(chan answer, 1)
	go func() {
		result, err := store.CompactSegment(context.Background(), source)
		done <- answer{result: result, err: err}
	}()
	<-blocked.reached
	if _, err := pending.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(blocked.release)
	got := <-done
	if got.err != nil || got.result.Relocation.Applied == 0 {
		t.Fatalf("result=%+v err=%v", got.result, got.err)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil || string(record.Value) != "commit-during-copy" || record.Addr.SegmentID() == source {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

func TestCompactSegmentRedirectsDistinctPendingVersionsOfSameRecord(t *testing.T) {
	store := newRelocationStore(t)
	seed, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := seed.Create(context.Background(), []byte("committed"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Put(context.Background(), id, []byte("pending-first")); err != nil {
		t.Fatal(err)
	}
	second, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Put(context.Background(), id, []byte("pending-second")); err != nil {
		t.Fatal(err)
	}
	source := store.core.catalog.Snapshot().ActiveDataSegmentID
	for store.core.catalog.Snapshot().ActiveDataSegmentID == source {
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
	if _, err := store.CompactSegment(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil || string(record.Value) != "pending-second" || record.Addr.SegmentID() == source || !recordlog.IsCompactionSegment(record.Addr.SegmentID()) {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

func TestCompactSegmentRelocatesExactPendingVersionAfterConcurrentCommits(t *testing.T) {
	store := newRelocationStore(t)
	seed, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := seed.Create(context.Background(), []byte("committed"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Put(context.Background(), id, []byte("pending-first")); err != nil {
		t.Fatal(err)
	}
	second, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Put(context.Background(), id, []byte("pending-second")); err != nil {
		t.Fatal(err)
	}
	source := store.core.catalog.Snapshot().ActiveDataSegmentID
	for store.core.catalog.Snapshot().ActiveDataSegmentID == source {
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
	blocked := &blockingSegmentVisitLog{
		maintenanceLog: store.core.compactionLog, source: source, target: id, value: []byte("committed"),
		reached: make(chan struct{}), release: make(chan struct{}),
	}
	store.core.compactionLog = blocked
	done := make(chan error, 1)
	go func() {
		_, err := store.CompactSegment(context.Background(), source)
		done <- err
	}()
	<-blocked.reached
	if _, err := first.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(blocked.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil || string(record.Value) != "pending-second" || record.Addr.SegmentID() == source {
		t.Fatalf("record=%+v err=%v", record, err)
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
	if containsSealedSegment(store.core.catalog.Snapshot(), result.Candidate.Source) {
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

func TestCompactNextSegmentReusesExistingCheckpointBeforeCopy(t *testing.T) {
	store, source, _, _, _ := relocationFixture(t)
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	blocked := &blockingProofLog{
		maintenanceLog: store.core.compactionLog, source: source,
		reached: make(chan struct{}), release: make(chan struct{}),
	}
	store.core.compactionLog = blocked
	store.checkpoints.captureMu.Lock()
	checkpointHeld := true
	released := false
	defer func() {
		if !released {
			close(blocked.release)
		}
		if checkpointHeld {
			store.checkpoints.captureMu.Unlock()
		}
	}()
	type answer struct {
		found bool
		err   error
	}
	done := make(chan answer, 1)
	go func() {
		_, found, err := store.CompactNextSegment(context.Background(), CompactionPolicy{BypassCooldown: true})
		done <- answer{found: found, err: err}
	}()
	select {
	case <-blocked.reached:
		// Copy started while the checkpoint capture lock was unavailable, proving selection
		// reused the already durable checkpoint.
	case <-time.After(time.Second):
		t.Fatal("compaction attempted a redundant selection checkpoint")
	}
	close(blocked.release)
	released = true
	store.checkpoints.captureMu.Unlock()
	checkpointHeld = false
	got := <-done
	if got.err != nil || !got.found {
		t.Fatalf("found=%v err=%v", got.found, got.err)
	}
}

func TestCompactNextSegmentRewritesAdjacentInputsIntoDedicatedOutput(t *testing.T) {
	store := newRelocationStore(t)
	store.maintenance.maxRelocationMutations = 1
	bySegment := make(map[recordlog.SegmentID][]model.ID)
	for store.core.catalog.Snapshot().ActiveDataSegmentID < 4 {
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
	manifest := store.core.catalog.Snapshot()
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
	store.maintenance.maxRelocationBytes = 1
	store.maintenance.gcBytesPerSecond.Store(1)
	now := time.Unix(100, 0)
	store.maintenance.gcNow = func() time.Time { return now }
	var waits int
	store.maintenance.gcWait = func(_ context.Context, delay time.Duration) error {
		waits++
		for _, segment := range store.core.catalog.Snapshot().SealedDataSegments {
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
	store.maintenance.maxRelocationBytes = 1
	store.maintenance.gcBytesPerSecond.Store(1)
	store.maintenance.gcNow = func() time.Time { return time.Unix(100, 0) }
	store.maintenance.gcWait = func(context.Context, time.Duration) error { return context.Canceled }
	if _, err := store.CompactSegment(context.Background(), source); !errors.Is(err, context.Canceled) {
		t.Fatalf("compact err=%v", err)
	}
	if store.state.fault != nil {
		t.Fatalf("recoverable cancellation faulted store: %v", store.state.fault)
	}
	if found, err := compactionstate.RecoveryArtifacts(store.core.root); err != nil || found {
		t.Fatalf("compaction marker found=%v err=%v", found, err)
	}
	for _, segment := range store.core.catalog.Snapshot().SealedDataSegments {
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
	if !containsSegmentID(store.core.catalog.Snapshot().SealedDataSegments, source) {
		t.Fatal("policy-rejected source was retired")
	}
	if metrics := store.Metrics(); metrics.GCNoCandidate != 1 || metrics.GCStarted != 0 {
		t.Fatalf("metrics=%+v", metrics)
	}
}

func TestRelocateSegmentRejectsInsufficientCopySpaceWithoutMutation(t *testing.T) {
	store, source, id, oldAddr, _ := relocationFixture(t)
	store.core.space = newSpaceGate("test", 1, time.Hour, func(string) (uint64, error) { return 1, nil })
	store.maintenance.gcMinFreeBytes = 1
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
	manifest := store.core.catalog.Snapshot()
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
	store.core.space = newSpaceGate("test", 1, time.Hour, func(string) (uint64, error) { return copyEstimate + 1, nil })
	store.maintenance.gcMinFreeBytes = 1
	result, err := store.compactSegmentLocked(context.Background(), source, store.maintenance.gcBytesPerSecond.Load())
	if !errors.Is(err, base.ErrInsufficientSpace) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if !containsSegmentID(store.core.catalog.Snapshot().SealedDataSegments, source) {
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

func TestCompactNextSegmentRedirectsOpenBatchReferences(t *testing.T) {
	store := newRelocationStore(t)
	pending, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := pending.Create(context.Background(), []byte("open-before-rotation"))
	if err != nil {
		t.Fatal(err)
	}
	source := recordlog.SegmentID(1)
	for store.core.catalog.Snapshot().ActiveDataSegmentID == source {
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
	result, found, err := store.CompactNextSegment(context.Background(), CompactionPolicy{BypassCooldown: true})
	if err != nil || !found || result.Candidate.Source.SegmentID != source {
		t.Fatalf("result=%+v found=%v err=%v", result, found, err)
	}
	if second, found, err := store.CompactNextSegment(context.Background(), CompactionPolicy{BypassCooldown: true}); err != nil || found {
		t.Fatalf("pending output was selected again: result=%+v found=%v err=%v", second, found, err)
	}
	if metrics := store.Metrics(); metrics.GCCommitRedirects != 1 || metrics.GCOpenRefsRedirected != 1 {
		t.Fatalf("repeated redirect metrics=%+v", metrics)
	}
	if _, err := pending.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if record, err := store.Get(context.Background(), id); err != nil || string(record.Value) != "open-before-rotation" || record.Addr.SegmentID() == source {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

func TestConcurrentUserUpdateWinsOverSegmentRelocation(t *testing.T) {
	store, source, id, oldAddr, _ := relocationFixture(t)
	underlying := store.core.log
	underlyingMaintenance := store.core.compactionLog
	blocked := &blockingCopyLog{
		Log: underlying, maintenanceLog: underlyingMaintenance, target: id, value: []byte("source-value"),
		reached: make(chan struct{}), release: make(chan struct{}),
	}
	store.core.log = blocked
	store.core.compactionLog = blocked

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
	underlying := store.core.log
	blocked := &blockingCopyLog{
		Log: underlying, maintenanceLog: store.core.compactionLog, target: id, value: []byte("source-value"),
		reached: make(chan struct{}), release: make(chan struct{}),
	}
	store.core.log = blocked
	store.core.compactionLog = blocked
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
		maintenanceLog: store.core.compactionLog, source: source,
		reached: make(chan struct{}), release: make(chan struct{}),
	}
	store.core.compactionLog = blocked
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
		maintenanceLog: store.core.compactionLog, source: source,
		reached: make(chan struct{}), release: make(chan struct{}),
	}
	store.core.compactionLog = blocked
	released := false
	defer func() {
		if !released {
			close(blocked.release)
		}
	}()

	// This models the interval after a Put Record append and before its final
	// Batch mutation becomes visible. Retirement must drain that interval before
	// it may inspect open-Batch references or scan the source.
	store.mutationAdmission.readLock()
	proofDone := make(chan error, 1)
	go func() {
		_, err := store.proveSegmentRetirement(context.Background(), source, relocated.LastCommitSeq)
		proofDone <- err
	}()
	select {
	case <-blocked.reached:
		store.mutationAdmission.readUnlock()
		t.Fatal("retirement scan crossed an in-flight Batch mutation")
	case <-time.After(20 * time.Millisecond):
	}
	store.mutationAdmission.readUnlock()
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

type countingScanLog struct {
	maintenanceLog
	mu     sync.Mutex
	source recordlog.SegmentID
	scans  int
}

func (l *countingScanLog) ScanSegment(ctx context.Context, source recordlog.SegmentID, visit func(recordlog.AppendResult, []byte) error) error {
	if source == l.source {
		l.mu.Lock()
		l.scans++
		l.mu.Unlock()
	}
	return l.maintenanceLog.ScanSegment(ctx, source, visit)
}

type blockingSegmentVisitLog struct {
	maintenanceLog
	source  recordlog.SegmentID
	target  model.ID
	value   []byte
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (l *blockingSegmentVisitLog) ScanSegment(ctx context.Context, source recordlog.SegmentID, visit func(recordlog.AppendResult, []byte) error) error {
	return l.maintenanceLog.ScanSegment(ctx, source, func(scanned recordlog.AppendResult, payload []byte) error {
		if err := visit(scanned, payload); err != nil {
			return err
		}
		if source != l.source {
			return nil
		}
		put, err := recordcodec.DecodePut(payload, 1<<20)
		if err == nil && put.RecordID == l.target && bytes.Equal(put.Value, l.value) {
			l.once.Do(func() {
				close(l.reached)
				<-l.release
			})
		}
		return nil
	})
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
	for store.core.catalog.Snapshot().ActiveDataSegmentID == source {
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

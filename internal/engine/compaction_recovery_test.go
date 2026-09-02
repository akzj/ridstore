package engine

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/akzj/ridstore/internal/compactionstate"
	"github.com/akzj/ridstore/internal/filelock"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

func TestOpenRollsBackUnpublishedCompaction(t *testing.T) {
	store, source, id, _, _ := relocationFixture(t)
	manifest := store.core.catalog.Snapshot()
	reserved, ids, err := store.core.publisher.ReserveCompactionSegments(manifest.Generation, 1)
	if err != nil {
		t.Fatal(err)
	}
	state := compactionstate.State{Phase: compactionstate.PhaseReserved, StoreUUID: reserved.StoreUUID, LogID: reserved.RecordLogID, BaseGeneration: reserved.Generation, Inputs: []recordlog.SegmentSummary{findSummary(t, reserved.SealedDataSegments, source)}, OutputIDs: ids}
	if err := compactionstate.Install(store.core.root, state); err != nil {
		t.Fatal(err)
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
	if _, err := reopened.Get(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if !containsSegmentID(reopened.core.catalog.Snapshot().SealedDataSegments, source) {
		t.Fatal("unpublished input was retired")
	}
	if found, err := compactionstate.RecoveryArtifacts(root); err != nil || found {
		t.Fatalf("marker found=%v err=%v", found, err)
	}
}

func TestOpenResumesPublishedCompaction(t *testing.T) {
	store, source, id, oldAddr, _ := relocationFixture(t)
	manifest := store.core.catalog.Snapshot()
	input := findSummary(t, manifest.SealedDataSegments, source)
	reserved, ids, err := store.core.publisher.ReserveCompactionSegments(manifest.Generation, 1)
	if err != nil {
		t.Fatal(err)
	}
	state := compactionstate.State{Phase: compactionstate.PhaseReserved, StoreUUID: reserved.StoreUUID, LogID: reserved.RecordLogID, BaseGeneration: reserved.Generation, Inputs: []recordlog.SegmentSummary{input}, OutputIDs: ids}
	if err := compactionstate.Install(store.core.root, state); err != nil {
		t.Fatal(err)
	}
	writer, err := store.core.compactionLog.NewCompactionWriter(ids)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.core.compactionLog.ScanSegment(context.Background(), source, func(scanned recordlog.AppendResult, payload []byte) error {
		typ, err := recordcodec.TypeOf(payload)
		if err != nil || typ != recordcodec.RecordTypePut {
			return err
		}
		put, err := recordcodec.DecodePut(payload, store.state.limits.MaxValueSize)
		if err != nil {
			return err
		}
		current, exists, err := store.core.mapping.Lookup(put.RecordID)
		if err != nil {
			return err
		}
		if exists && current == scanned.Addr {
			_, err = writer.Append(context.Background(), payload)
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	outputs, err := writer.Finish()
	if err != nil {
		t.Fatal(err)
	}
	latest := store.core.catalog.Snapshot()
	if _, err := store.core.publisher.InstallCompactionOutputs(latest.Generation, outputs); err != nil {
		t.Fatal(err)
	}
	state.Phase, state.Outputs = compactionstate.PhaseOutputsPublished, outputs
	if err := compactionstate.Update(store.core.root, state); err != nil {
		t.Fatal(err)
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
	record, err := reopened.Get(context.Background(), id)
	if err != nil || record.Addr == oldAddr || !recordlog.IsCompactionSegment(record.Addr.SegmentID()) {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if containsSegmentID(reopened.core.catalog.Snapshot().SealedDataSegments, source) {
		t.Fatal("source remains after recovery")
	}
	assertPublishedStateMatchesCatalog(t, reopened)
	if _, err := os.Stat(compactionstate.Path(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("marker remains: %v", err)
	}
}

func TestOpenResumesPublishedCompactionWithDuplicatePendingRecordIDs(t *testing.T) {
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
		if _, err := filler.Create(context.Background(), make([]byte, 512)); err != nil {
			t.Fatal(err)
		}
		if _, err := filler.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	manifest := store.core.catalog.Snapshot()
	input := findSummary(t, manifest.SealedDataSegments, source)
	reserved, ids, err := store.core.publisher.ReserveCompactionSegments(manifest.Generation, 1)
	if err != nil {
		t.Fatal(err)
	}
	state := compactionstate.State{Phase: compactionstate.PhaseReserved, StoreUUID: reserved.StoreUUID, LogID: reserved.RecordLogID, BaseGeneration: reserved.Generation, Inputs: []recordlog.SegmentSummary{input}, OutputIDs: ids}
	if err := compactionstate.Install(store.core.root, state); err != nil {
		t.Fatal(err)
	}
	writer, err := store.core.compactionLog.NewCompactionWriter(ids)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.core.compactionLog.ScanSegment(context.Background(), source, func(_ recordlog.AppendResult, payload []byte) error {
		typ, err := recordcodec.TypeOf(payload)
		if err != nil {
			return err
		}
		if typ != recordcodec.RecordTypePut {
			return nil
		}
		_, err = writer.Append(context.Background(), payload)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	outputs, err := writer.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.installCompactionOutputs(outputs); err != nil {
		t.Fatal(err)
	}
	if err := store.core.compactionLog.RegisterCompactionOutputs(outputs); err != nil {
		t.Fatal(err)
	}
	state.Phase, state.Outputs = compactionstate.PhaseOutputsPublished, outputs
	if err := compactionstate.Update(store.core.root, state); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Commit(context.Background()); err != nil {
		t.Fatal(err)
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
	record, err := reopened.Get(context.Background(), id)
	if err != nil || string(record.Value) != "pending-second" || record.Addr.SegmentID() == source || !recordlog.IsCompactionSegment(record.Addr.SegmentID()) {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if containsSegmentID(reopened.core.catalog.Snapshot().SealedDataSegments, source) {
		t.Fatal("source remains after duplicate-ID recovery")
	}
}

func TestOpenReplaysDurableCompactionRelocationBeforeCheckpoint(t *testing.T) {
	store, source, id, oldAddr, _ := relocationFixture(t)
	manifest := store.core.catalog.Snapshot()
	input := findSummary(t, manifest.SealedDataSegments, source)
	reserved, ids, err := store.core.publisher.ReserveCompactionSegments(manifest.Generation, 1)
	if err != nil {
		t.Fatal(err)
	}
	state := compactionstate.State{Phase: compactionstate.PhaseReserved, StoreUUID: reserved.StoreUUID, LogID: reserved.RecordLogID, BaseGeneration: reserved.Generation, Inputs: []recordlog.SegmentSummary{input}, OutputIDs: ids}
	if err := compactionstate.Install(store.core.root, state); err != nil {
		t.Fatal(err)
	}
	writer, err := store.core.compactionLog.NewCompactionWriter(ids)
	if err != nil {
		t.Fatal(err)
	}
	var pending []copiedRecord
	if err := store.core.compactionLog.ScanSegment(context.Background(), source, func(scanned recordlog.AppendResult, payload []byte) error {
		typ, err := recordcodec.TypeOf(payload)
		if err != nil || typ != recordcodec.RecordTypePut {
			return err
		}
		put, err := recordcodec.DecodePut(payload, store.state.limits.MaxValueSize)
		if err != nil {
			return err
		}
		current, exists, err := store.core.mapping.Lookup(put.RecordID)
		if err != nil || !exists || current != scanned.Addr {
			return err
		}
		copied, err := writer.Append(context.Background(), payload)
		if err != nil {
			return err
		}
		ref, err := copied.Ref()
		if err != nil {
			return err
		}
		pending = append(pending, copiedRecord{id: put.RecordID, oldAddr: scanned.Addr, newRef: ref, valueBytes: uint64(len(put.Value))})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	outputs, err := writer.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.installCompactionOutputs(outputs); err != nil {
		t.Fatal(err)
	}
	if err := store.core.compactionLog.RegisterCompactionOutputs(outputs); err != nil {
		t.Fatal(err)
	}
	state.Phase, state.Outputs = compactionstate.PhaseOutputsPublished, outputs
	if err := compactionstate.Update(store.core.root, state); err != nil {
		t.Fatal(err)
	}
	if len(pending) < 2 {
		t.Fatalf("need multiple relocation records, got %d", len(pending))
	}
	var relocated SegmentRelocationResult
	if err := store.publishCopiedRecords(context.Background(), pending[:1], &relocated); err != nil {
		t.Fatal(err)
	}
	if relocated.Applied != 1 {
		t.Fatalf("partially applied relocation=%+v", relocated)
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
	record, err := reopened.Get(context.Background(), id)
	if err != nil || record.Addr == oldAddr || !recordlog.IsCompactionSegment(record.Addr.SegmentID()) {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if containsSegmentID(reopened.core.catalog.Snapshot().SealedDataSegments, source) {
		t.Fatal("source remains after resumed relocation")
	}
}

func TestOpenReleasesResourcesWhenCompactionResumeFails(t *testing.T) {
	store, source, _, _, _ := relocationFixture(t)
	manifest := store.core.catalog.Snapshot()
	input := findSummary(t, manifest.SealedDataSegments, source)
	reserved, ids, err := store.core.publisher.ReserveCompactionSegments(manifest.Generation, 1)
	if err != nil {
		t.Fatal(err)
	}
	state := compactionstate.State{Phase: compactionstate.PhaseReserved, StoreUUID: reserved.StoreUUID, LogID: reserved.RecordLogID, BaseGeneration: reserved.Generation, Inputs: []recordlog.SegmentSummary{input}, OutputIDs: ids}
	if err := compactionstate.Install(store.core.root, state); err != nil {
		t.Fatal(err)
	}
	writer, err := store.core.compactionLog.NewCompactionWriter(ids)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := recordcodec.EncodeAbort(recordcodec.AbortRecord{BatchID: model.BatchID(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append(context.Background(), payload); err != nil {
		t.Fatal(err)
	}
	outputs, err := writer.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.installCompactionOutputs(outputs); err != nil {
		t.Fatal(err)
	}
	state.Phase, state.Outputs = compactionstate.PhaseOutputsPublished, outputs
	if err := compactionstate.Update(store.core.root, state); err != nil {
		t.Fatal(err)
	}
	root := store.core.root
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), root, relocationConfig().Runtime); err == nil {
		t.Fatal("open accepted non-Put compaction output")
	}
	lock, err := filelock.Acquire(root)
	if err != nil {
		t.Fatalf("failed Open retained directory lock: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
}

func findSummary(t *testing.T, segments []recordlog.SegmentSummary, id recordlog.SegmentID) recordlog.SegmentSummary {
	t.Helper()
	for _, segment := range segments {
		if segment.SegmentID == id {
			return segment
		}
	}
	t.Fatalf("segment %d not found", id)
	return recordlog.SegmentSummary{}
}

package engine

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/akzj/ridstore/internal/compactionstate"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

func TestOpenRollsBackUnpublishedCompaction(t *testing.T) {
	store, source, id, _, _ := relocationFixture(t)
	manifest := store.catalog.Snapshot()
	reserved, ids, err := store.catalog.ReserveCompactionSegments(manifest.Generation, 1)
	if err != nil {
		t.Fatal(err)
	}
	state := compactionstate.State{Phase: compactionstate.PhaseReserved, StoreUUID: reserved.StoreUUID, LogID: reserved.RecordLogID, BaseGeneration: reserved.Generation, Inputs: []recordlog.SegmentSummary{findSummary(t, reserved.SealedDataSegments, source)}, OutputIDs: ids}
	if err := compactionstate.Install(store.root, state); err != nil {
		t.Fatal(err)
	}
	root := store.root
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
	if !containsSegmentID(reopened.catalog.Snapshot().SealedDataSegments, source) {
		t.Fatal("unpublished input was retired")
	}
	if found, err := compactionstate.RecoveryArtifacts(root); err != nil || found {
		t.Fatalf("marker found=%v err=%v", found, err)
	}
}

func TestOpenResumesPublishedCompaction(t *testing.T) {
	store, source, id, oldAddr, _ := relocationFixture(t)
	manifest := store.catalog.Snapshot()
	input := findSummary(t, manifest.SealedDataSegments, source)
	reserved, ids, err := store.catalog.ReserveCompactionSegments(manifest.Generation, 1)
	if err != nil {
		t.Fatal(err)
	}
	state := compactionstate.State{Phase: compactionstate.PhaseReserved, StoreUUID: reserved.StoreUUID, LogID: reserved.RecordLogID, BaseGeneration: reserved.Generation, Inputs: []recordlog.SegmentSummary{input}, OutputIDs: ids}
	if err := compactionstate.Install(store.root, state); err != nil {
		t.Fatal(err)
	}
	writer, err := store.maintenance.NewCompactionWriter(ids)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.maintenance.ScanSegment(context.Background(), source, func(scanned recordlog.AppendResult, payload []byte) error {
		typ, err := recordcodec.TypeOf(payload)
		if err != nil || typ != recordcodec.RecordTypePut {
			return err
		}
		put, err := recordcodec.DecodePut(payload, store.limits.MaxValueSize)
		if err != nil {
			return err
		}
		current, exists, err := store.mapping.Lookup(put.RecordID)
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
	latest := store.catalog.Snapshot()
	if _, err := store.catalog.InstallCompactionOutputs(latest.Generation, outputs); err != nil {
		t.Fatal(err)
	}
	state.Phase, state.Outputs = compactionstate.PhaseOutputsPublished, outputs
	if err := compactionstate.Update(store.root, state); err != nil {
		t.Fatal(err)
	}
	root := store.root
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
	if containsSegmentID(reopened.catalog.Snapshot().SealedDataSegments, source) {
		t.Fatal("source remains after recovery")
	}
	if _, err := os.Stat(compactionstate.Path(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("marker remains: %v", err)
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

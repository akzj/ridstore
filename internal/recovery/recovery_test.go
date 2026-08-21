package recovery

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/appendlog"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/batch"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/segment"
)

func recoveryFixture(t *testing.T) (*segment.ActiveData, *appendlog.Log, storeformat.Manifest) {
	t.Helper()
	root := t.TempDir()
	uuid := base.StoreUUID{1, 2, 3}
	if err := os.Mkdir(filepath.Join(root, "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	header, _ := storeformat.EncodeSegmentHeader(storeformat.SegmentHeader{Kind: storeformat.SegmentKindData, StoreUUID: uuid, FileID: 1, FirstSeq: 1})
	if err := os.WriteFile(filepath.Join(root, "data", segment.ActiveDataFileName(1)), header[:], 0o600); err != nil {
		t.Fatal(err)
	}
	active, err := segment.OpenActiveData(root, uuid, 1, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	log, err := appendlog.New(active, 1, 1024, 64)
	if err != nil {
		t.Fatal(err)
	}
	replay, _ := base.NewLogPos(1, storeformat.SegmentHeaderSize)
	manifest := storeformat.Manifest{
		Generation: 1, StoreUUID: uuid,
		HardLimits:        storeformat.HardLimits{SegmentSize: 1 << 20, MaxValueSize: 1024, MaxBatchBytes: 4096, MaxBatchMutations: 16, MaxBatchConditions: 16, MaxOpenBatches: 16, IDReserveSize: 4, BatchIDReserveSize: 4},
		NextDataSegmentID: 2, NextMapSegmentID: 2, ActiveDataSegmentID: 1, ActiveMapSegmentID: 1,
		ReplayStart: replay, ReservedIDHighExclusive: 1, ReservedBatchIDHighExclusive: 1, IssuedBatchIDHighExclusiveAtCut: 1,
		NextFrameSeq: 1, NextCommitSeq: 1,
	}
	return active, log, manifest
}

func TestRecoverCommitReserveAbortAndOrphanPut(t *testing.T) {
	active, log, manifest := recoveryFixture(t)
	defer active.Close()
	addr, _, physical, err := log.AppendPut(context.Background(), 7, 1, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	if err := log.AppendReserve(context.Background(), storeformat.FrameTypeIDReserve, storeformat.ReservePayload{PreviousHighExclusive: 1, NewHighExclusive: 5, Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := log.AppendCommit(batch.Prepared{BatchID: 7, LogicalPayloadBytes: 5, Mutations: []batch.Mutation{{RecordID: 1, Operation: batch.Put, Addr: addr, ValueBytes: 5, PhysicalSize: physical}}}, 1); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := log.AppendPut(context.Background(), 8, 2, []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	if err := log.AppendAbort(context.Background(), 8, storeformat.BatchAbortPayload{Reason: storeformat.AbortReasonCaller, FinalMutationCount: 1, AppendedPayloadBytes: 6, LastBatchFrameSeq: 5}); err != nil {
		t.Fatal(err)
	}
	if err := log.AppendReserve(context.Background(), storeformat.FrameTypeBatchIDReserve, storeformat.ReservePayload{PreviousHighExclusive: 1, NewHighExclusive: 5, Generation: 1}); err != nil {
		t.Fatal(err)
	}

	result, err := RecoverPhase1(manifest, active)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := result.Mapping.Lookup(1)
	if err != nil || !ok || got != addr {
		t.Fatalf("mapping addr=%x ok=%v error=%v", got, ok, err)
	}
	if _, ok, _ := result.Mapping.Lookup(2); ok {
		t.Fatal("orphan Put became visible")
	}
	if result.NextFrameSeq != 8 || result.NextCommitSeq != 2 || result.ReservedIDHighExclusive != 5 || result.ReservedBatchIDHighExclusive != 5 {
		t.Fatalf("result=%+v", result)
	}
	if result.Statuses[7] != (BatchStatus{State: BatchCommitted, CommitSeq: 1}) || result.Statuses[8].State != BatchAborted {
		t.Fatalf("statuses=%+v", result.Statuses)
	}
}

func TestRecoverRejectsDescriptorLogicalMismatch(t *testing.T) {
	active, log, manifest := recoveryFixture(t)
	defer active.Close()
	addr, _, physical, err := log.AppendPut(context.Background(), 7, 1, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	prepared := batch.Prepared{BatchID: 7, LogicalPayloadBytes: 999, Mutations: []batch.Mutation{{RecordID: 1, Operation: batch.Put, Addr: addr, ValueBytes: 5, PhysicalSize: physical}}}
	if _, err := log.AppendCommit(prepared, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := RecoverPhase1(manifest, active); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("error=%v", err)
	}
}

func TestRecoverRejectsUnsupportedFileTopology(t *testing.T) {
	active, _, manifest := recoveryFixture(t)
	defer active.Close()
	manifest.SealedDataSegments = []storeformat.FileSummary{{FileID: 2, ValidEnd: 8192, FirstSeq: 1, LastSeq: 1}}
	if _, err := RecoverPhase1(manifest, active); !errors.Is(err, base.ErrUnsupported) {
		t.Fatalf("error=%v", err)
	}
}

func TestRecoverRelocationApplyAndCASSkip(t *testing.T) {
	active, log, manifest := recoveryFixture(t)
	defer active.Close()
	oldAddr, _, oldPhysical, err := log.AppendPut(context.Background(), 7, 1, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.AppendCommit(batch.Prepared{BatchID: 7, LogicalPayloadBytes: 3, Mutations: []batch.Mutation{{
		RecordID: 1, Operation: batch.Put, Addr: oldAddr, ValueBytes: 3, PhysicalSize: oldPhysical,
	}}}, 1); err != nil {
		t.Fatal(err)
	}
	copy1, _, _, err := log.AppendPut(context.Background(), 7, 1, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.AppendRelocation(appendlog.RelocationPrepared{BatchID: 8, LogicalPayloadBytes: 3, Entries: []appendlog.RelocationEntry{{
		RecordID: 1, ExpectedOldAddr: oldAddr, NewAddr: copy1,
	}}}, 2); err != nil {
		t.Fatal(err)
	}
	newerAddr, _, newerPhysical, err := log.AppendPut(context.Background(), 9, 1, []byte("newer"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.AppendCommit(batch.Prepared{BatchID: 9, LogicalPayloadBytes: 5, Mutations: []batch.Mutation{{
		RecordID: 1, Operation: batch.Put, Addr: newerAddr, ValueBytes: 5, PhysicalSize: newerPhysical,
	}}}, 3); err != nil {
		t.Fatal(err)
	}
	copy2, _, _, err := log.AppendPut(context.Background(), 7, 1, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.AppendRelocation(appendlog.RelocationPrepared{BatchID: 10, LogicalPayloadBytes: 3, Entries: []appendlog.RelocationEntry{{
		RecordID: 1, ExpectedOldAddr: copy1, NewAddr: copy2,
	}}}, 4); err != nil {
		t.Fatal(err)
	}
	result, err := RecoverPhase1(manifest, active)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, err := result.Mapping.Lookup(1); err != nil || !ok || got != newerAddr {
		t.Fatalf("mapping=(%x,%v,%v) want=%x", got, ok, err, newerAddr)
	}
	if result.NextCommitSeq != 5 || result.Statuses[8].CommitSeq != 2 || result.Statuses[10].CommitSeq != 4 {
		t.Fatalf("result=%+v", result)
	}
}

func TestRecoverRejectsRelocationValueMismatch(t *testing.T) {
	active, log, manifest := recoveryFixture(t)
	defer active.Close()
	oldAddr, _, physical, err := log.AppendPut(context.Background(), 7, 1, []byte("same-size-a"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.AppendCommit(batch.Prepared{BatchID: 7, LogicalPayloadBytes: 11, Mutations: []batch.Mutation{{
		RecordID: 1, Operation: batch.Put, Addr: oldAddr, ValueBytes: 11, PhysicalSize: physical,
	}}}, 1); err != nil {
		t.Fatal(err)
	}
	badCopy, _, _, err := log.AppendPut(context.Background(), 7, 1, []byte("same-size-b"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.AppendRelocation(appendlog.RelocationPrepared{BatchID: 8, LogicalPayloadBytes: 11, Entries: []appendlog.RelocationEntry{{
		RecordID: 1, ExpectedOldAddr: oldAddr, NewAddr: badCopy,
	}}}, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := RecoverPhase1(manifest, active); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("error=%v", err)
	}
}

package transaction

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

type fakeAllocator struct{ next uint64 }

func (a *fakeAllocator) Allocate(context.Context) (uint64, error) {
	a.next++
	return a.next, nil
}

func (a *fakeAllocator) CanUse(uint64) bool { return true }

type appended struct {
	payload []byte
	sync    bool
}

type fakeLog struct {
	records []appended
	err     error
}

func (l *fakeLog) Append(_ context.Context, payload []byte, syncWrite bool) (recordlog.AppendResult, error) {
	if l.err != nil {
		return recordlog.AppendResult{}, l.err
	}
	l.records = append(l.records, appended{payload: append([]byte(nil), payload...), sync: syncWrite})
	addr, err := recordlog.NewVAddr(1, uint32(64+(len(l.records)-1)*128), 128)
	if err != nil {
		return recordlog.AppendResult{}, err
	}
	return recordlog.NewAppendResult(addr, 128)
}

func testBatch(t *testing.T, log *fakeLog, limits Limits) *Batch {
	t.Helper()
	if limits == (Limits{}) {
		limits = Limits{MaxValueSize: 1024, MaxBatchBytes: 4096, MaxBatchMutations: 16, MaxBatchConditions: 16}
	}
	b, err := New(7, limits, log, &fakeAllocator{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestPutFoldsFinalMutationsAndUsesProtocolRecords(t *testing.T) {
	log := &fakeLog{}
	b := testBatch(t, log, Limits{})
	value := []byte("first")
	if err := b.Put(context.Background(), 2, value); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	if err := b.Put(context.Background(), 2, []byte("final")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(context.Background(), 1, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(2); err != nil {
		t.Fatal(err)
	}
	if len(log.records) != 3 {
		t.Fatalf("records=%d", len(log.records))
	}
	first, err := recordcodec.DecodePut(log.records[0].payload, 1024)
	if err != nil || first.OriginBatchID != 7 || first.RecordID != 2 || string(first.Value) != "first" || log.records[0].sync {
		t.Fatalf("first=%+v sync=%v err=%v", first, log.records[0].sync, err)
	}
	prepared, err := b.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	if prepared.BatchID != 7 || prepared.LogicalPayloadBytes != 3 || len(prepared.Mutations) != 2 || prepared.Mutations[0].RecordID != 1 || prepared.Mutations[1].Operation != mapping.OperationDelete {
		t.Fatalf("prepared=%+v", prepared)
	}
	proposal := prepared.Proposal()
	if len(proposal.Changes) != 2 || proposal.Changes[1].Operation != mapping.OperationDelete {
		t.Fatalf("proposal=%+v", proposal)
	}
}

func TestReferencedSegmentTracksOnlyFinalPublishablePut(t *testing.T) {
	log := &fakeLog{}
	b := testBatch(t, log, Limits{})
	if b.ReferencesSegment(1) || b.ReferencesSegment(0) {
		t.Fatal("empty batch references a segment")
	}
	if err := b.Put(context.Background(), 1, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if !b.ReferencesSegment(1) {
		t.Fatal("final Put reference was not retained")
	}
	if err := b.Delete(1); err != nil {
		t.Fatal(err)
	}
	if b.ReferencesSegment(1) {
		t.Fatal("superseded Put remained a publishable reference")
	}
	if err := b.Put(context.Background(), 1, []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Prepare(); err != nil {
		t.Fatal(err)
	}
	if !b.ReferencesSegment(1) {
		t.Fatal("committing Put lost its reference before publication")
	}
	if err := b.MarkCommitted(1); err != nil {
		t.Fatal(err)
	}
	if b.ReferencesSegment(1) {
		t.Fatal("terminal batch retained a segment reference")
	}
}

func TestCreateIsBlindAndCompareAndPutDeclaresAddress(t *testing.T) {
	log := &fakeLog{}
	created := testBatch(t, log, Limits{})
	id, err := created.Create(context.Background(), []byte("new"))
	if err != nil || id != 1 {
		t.Fatalf("id=%d err=%v", id, err)
	}
	prepared, err := created.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.Conditions) != 0 {
		t.Fatalf("conditions=%+v", prepared.Conditions)
	}

	updated := testBatch(t, log, Limits{})
	expected, _ := recordlog.NewVAddr(1, 64, 64)
	if err := updated.CompareAndPut(context.Background(), 9, expected, []byte("updated")); err != nil {
		t.Fatal(err)
	}
	prepared, err = updated.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(prepared.Conditions, []mapping.Condition{{RecordID: 9, ExpectedAddr: expected}}) {
		t.Fatalf("conditions=%+v", prepared.Conditions)
	}
}

func TestConditionValidatedBeforeAppend(t *testing.T) {
	log := &fakeLog{}
	b := testBatch(t, log, Limits{MaxValueSize: 1024, MaxBatchBytes: 4096, MaxBatchMutations: 2, MaxBatchConditions: 1})
	if err := b.ExpectAbsent(1); err != nil {
		t.Fatal(err)
	}
	expected, _ := recordlog.NewVAddr(1, 64, 64)
	if err := b.CompareAndPut(context.Background(), 2, expected, []byte("must-not-append")); !errors.Is(err, base.ErrBatchTooLarge) {
		t.Fatalf("update err=%v", err)
	}
	if len(log.records) != 0 {
		t.Fatalf("records=%d", len(log.records))
	}
}

func TestLimitsCountAllAppendedPayload(t *testing.T) {
	b := testBatch(t, &fakeLog{}, Limits{MaxValueSize: 3, MaxBatchBytes: 4, MaxBatchMutations: 1, MaxBatchConditions: 1})
	if err := b.Put(context.Background(), 1, []byte("long")); !errors.Is(err, base.ErrValueTooLarge) {
		t.Fatalf("value err=%v", err)
	}
	if err := b.Put(context.Background(), 1, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(context.Background(), 1, []byte("de")); !errors.Is(err, base.ErrBatchTooLarge) {
		t.Fatalf("bytes err=%v", err)
	}
	if err := b.Delete(2); !errors.Is(err, base.ErrBatchTooLarge) {
		t.Fatalf("mutation err=%v", err)
	}
}

func TestAbortUsesUnsyncedDiagnosticRecord(t *testing.T) {
	log := &fakeLog{}
	b := testBatch(t, log, Limits{})
	if err := b.Put(context.Background(), 1, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Abort(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if len(log.records) != 2 || log.records[1].sync {
		t.Fatalf("records=%+v", log.records)
	}
	abort, err := recordcodec.DecodeAbort(log.records[1].payload)
	if err != nil || abort.BatchID != 7 || abort.Reason != 3 {
		t.Fatalf("abort=%+v err=%v", abort, err)
	}
	state, _ := b.State()
	if state != StateAborted {
		t.Fatalf("state=%d", state)
	}
}

func TestAppendFailureFailsBatchAndTerminalTransitions(t *testing.T) {
	appendErr := errors.New("append failed")
	b := testBatch(t, &fakeLog{err: appendErr}, Limits{})
	if err := b.Put(context.Background(), 1, nil); !errors.Is(err, appendErr) {
		t.Fatalf("put err=%v", err)
	}
	if err := b.Delete(1); !errors.Is(err, base.ErrBatchFailed) {
		t.Fatalf("delete err=%v", err)
	}

	committed := testBatch(t, &fakeLog{}, Limits{})
	prepared, err := committed.Prepare()
	if err != nil || prepared.BatchID != 7 {
		t.Fatalf("prepared=%+v err=%v", prepared, err)
	}
	if err := committed.MarkCommitted(model.CommitSeq(4)); err != nil {
		t.Fatal(err)
	}
	state, seq := committed.State()
	if state != StateCommitted || seq != 4 {
		t.Fatalf("state=%d seq=%d", state, seq)
	}
}

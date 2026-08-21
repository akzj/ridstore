package batch

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
)

type fakeAllocator struct{ next uint64 }

func (a *fakeAllocator) Allocate(context.Context) (uint64, error) {
	a.next++
	return a.next, nil
}

type appendedPut struct {
	batch base.BatchID
	id    base.ID
	value []byte
}

type fakeAppender struct {
	puts       []appendedPut
	aborts     []storeformat.BatchAbortPayload
	appendErr  error
	abortErr   error
	nextOffset uint32
}

func (a *fakeAppender) AppendPut(_ context.Context, batchID base.BatchID, id base.ID, value []byte) (base.VAddr, base.FrameSeq, uint64, error) {
	if a.appendErr != nil {
		return 0, 0, 0, a.appendErr
	}
	a.puts = append(a.puts, appendedPut{batch: batchID, id: id, value: append([]byte(nil), value...)})
	if a.nextOffset == 0 {
		a.nextOffset = 4096
	}
	addr, _ := base.NewVAddr(1, a.nextOffset)
	a.nextOffset += 64
	return addr, base.FrameSeq(len(a.puts)), 64 + uint64(len(value)), nil
}

func (a *fakeAppender) AppendAbort(_ context.Context, _ base.BatchID, payload storeformat.BatchAbortPayload) error {
	a.aborts = append(a.aborts, payload)
	return a.abortErr
}

func newTestBatch(t *testing.T, appender *fakeAppender, limits Limits) *Batch {
	t.Helper()
	if limits == (Limits{}) {
		limits = Limits{MaxValueSize: 1024, MaxBatchBytes: 4096, MaxBatchMutations: 16, MaxBatchConditions: 16}
	}
	b, err := New(7, limits, appender, &fakeAllocator{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestPutOverwriteDeleteAndPrepare(t *testing.T) {
	appender := &fakeAppender{}
	b := newTestBatch(t, appender, Limits{})
	value := []byte("first")
	if err := b.Put(context.Background(), 2, value); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	if string(appender.puts[0].value) != "first" {
		t.Fatalf("appender did not consume value: %q", appender.puts[0].value)
	}
	if err := b.Put(context.Background(), 2, []byte("final")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(context.Background(), 1, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(2); err != nil {
		t.Fatal(err)
	}
	if err := b.ExpectAbsent(3); err != nil {
		t.Fatal(err)
	}
	if err := b.ExpectRevision(1, 9); err != nil {
		t.Fatal(err)
	}
	prepared, err := b.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	if prepared.BatchID != 7 || prepared.AppendedPayloadBytes != 13 || prepared.LogicalPayloadBytes != 3 || prepared.LastBatchFrameSeq != 3 {
		t.Fatalf("prepared=%+v", prepared)
	}
	if len(prepared.Mutations) != 2 || prepared.Mutations[0].RecordID != 1 || prepared.Mutations[1].Operation != Delete {
		t.Fatalf("mutations=%+v", prepared.Mutations)
	}
	if !reflect.DeepEqual([]base.ID{prepared.Conditions[0].RecordID, prepared.Conditions[1].RecordID}, []base.ID{1, 3}) {
		t.Fatalf("conditions=%+v", prepared.Conditions)
	}
	if err := b.MarkCommitted(4); err != nil {
		t.Fatal(err)
	}
	state, seq := b.State()
	if state != StateCommitted || seq != 4 {
		t.Fatalf("state=%d seq=%d", state, seq)
	}
}

func TestAllocateAndAbort(t *testing.T) {
	appender := &fakeAppender{}
	b := newTestBatch(t, appender, Limits{})
	id, err := b.Allocate(context.Background())
	if err != nil || id != 1 {
		t.Fatalf("id=%d error=%v", id, err)
	}
	if err := b.Put(context.Background(), id, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Abort(context.Background(), storeformat.AbortReasonCaller); err != nil {
		t.Fatal(err)
	}
	state, _ := b.State()
	if state != StateAborted || len(appender.aborts) != 1 || appender.aborts[0].FinalMutationCount != 1 || appender.aborts[0].LastBatchFrameSeq != 1 {
		t.Fatalf("state=%d aborts=%+v", state, appender.aborts)
	}
	if err := b.Delete(1); !errors.Is(err, base.ErrBatchClosed) {
		t.Fatalf("delete after abort error=%v", err)
	}
}

func TestLimitsAndConflictingConditions(t *testing.T) {
	b := newTestBatch(t, &fakeAppender{}, Limits{MaxValueSize: 3, MaxBatchBytes: 4, MaxBatchMutations: 1, MaxBatchConditions: 1})
	if err := b.Put(context.Background(), 1, []byte("long")); !errors.Is(err, base.ErrValueTooLarge) {
		t.Fatalf("value error=%v", err)
	}
	if err := b.Put(context.Background(), 1, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(context.Background(), 1, []byte("de")); !errors.Is(err, base.ErrBatchTooLarge) {
		t.Fatalf("bytes error=%v", err)
	}
	if err := b.Delete(2); !errors.Is(err, base.ErrBatchTooLarge) {
		t.Fatalf("mutation error=%v", err)
	}
	if err := b.ExpectAbsent(1); err != nil {
		t.Fatal(err)
	}
	if err := b.ExpectRevision(1, 1); !errors.Is(err, base.ErrBatchFailed) {
		t.Fatalf("condition conflict error=%v", err)
	}
	if _, err := b.Prepare(); !errors.Is(err, base.ErrBatchFailed) {
		t.Fatalf("prepare failed error=%v", err)
	}
}

func TestAppendFailureRequiresAbortAndMarkerErrorIsReturned(t *testing.T) {
	appendErr := errors.New("short write")
	abortErr := errors.New("abort marker")
	appender := &fakeAppender{appendErr: appendErr, abortErr: abortErr}
	b := newTestBatch(t, appender, Limits{})
	if err := b.Put(context.Background(), 1, nil); !errors.Is(err, appendErr) {
		t.Fatalf("put error=%v", err)
	}
	if err := b.Delete(1); !errors.Is(err, base.ErrBatchFailed) {
		t.Fatalf("failed batch error=%v", err)
	}
	if err := b.Abort(context.Background(), storeformat.AbortReasonBatchFailed); !errors.Is(err, abortErr) {
		t.Fatalf("abort error=%v", err)
	}
	state, _ := b.State()
	if state != StateAborted {
		t.Fatalf("state=%d", state)
	}
}

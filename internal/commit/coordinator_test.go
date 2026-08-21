package commit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/appendlog"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/batch"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/mapping/memory"
	"github.com/akzj/ridstore/internal/segment"
)

type incrementAllocator struct{ next uint64 }

func (a *incrementAllocator) Allocate(context.Context) (uint64, error) { a.next++; return a.next, nil }

type activeReader struct{ active *segment.ActiveData }

func (r activeReader) ReadPutHeader(addr base.VAddr) (RecordHeader, error) {
	frame, err := r.active.ReadFrame(addr)
	if err != nil {
		return RecordHeader{}, err
	}
	if frame.Type != storeformat.FrameTypePutRecord {
		return RecordHeader{}, base.ErrCorrupt
	}
	physicalSize, err := base.Align8(storeformat.FrameHeaderSize + uint64(len(frame.Payload)))
	if err != nil {
		return RecordHeader{}, err
	}
	return RecordHeader{RecordID: frame.RecordID, OriginBatch: frame.BatchID, ValueBytes: uint64(len(frame.Payload)), PhysicalSize: physicalSize}, nil
}

func coordinatorFixture(t *testing.T) (*Coordinator, *appendlog.Log, *segment.ActiveData, *memory.Mapping) {
	t.Helper()
	root := t.TempDir()
	uuid := base.StoreUUID{1}
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
	mapping := memory.NewEmpty()
	coordinator, err := New(1, log, mapping, activeReader{active})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator, log, active, mapping
}

func makeBatch(t *testing.T, id base.BatchID, log *appendlog.Log) *batch.Batch {
	t.Helper()
	b, err := batch.New(id, batch.Limits{MaxValueSize: 1024, MaxBatchBytes: 4096, MaxBatchMutations: 16, MaxBatchConditions: 16}, log, &incrementAllocator{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDurableCommitPublishesMappingAndConditions(t *testing.T) {
	coordinator, log, active, mapping := coordinatorFixture(t)
	defer active.Close()
	b1 := makeBatch(t, 7, log)
	if err := b1.Put(context.Background(), 1, []byte("one")); err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.Commit(context.Background(), b1)
	if err != nil || result.CommitSeq != 1 || result.BatchID != 7 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	addr1, ok, err := mapping.Lookup(1)
	if err != nil || !ok {
		t.Fatalf("lookup addr=%x ok=%v error=%v", addr1, ok, err)
	}
	b2 := makeBatch(t, 8, log)
	if err := b2.ExpectRevision(1, 7); err != nil {
		t.Fatal(err)
	}
	if err := b2.Put(context.Background(), 1, []byte("two")); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Commit(context.Background(), b2); err != nil {
		t.Fatal(err)
	}
	addr2, ok, _ := mapping.Lookup(1)
	if !ok || addr2 == addr1 {
		t.Fatalf("new addr=%x old=%x", addr2, addr1)
	}
	b3 := makeBatch(t, 9, log)
	if err := b3.ExpectRevision(1, 7); err != nil {
		t.Fatal(err)
	}
	if err := b3.Put(context.Background(), 1, []byte("stale")); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Commit(context.Background(), b3); !errors.Is(err, base.ErrConflict) {
		t.Fatalf("conflict error=%v", err)
	}
	if got, _, _ := mapping.Lookup(1); got != addr2 {
		t.Fatalf("conflict changed mapping: %x", got)
	}
	state, _ := b3.State()
	if state != batch.StateAborted || coordinator.NextCommitSeq() != 3 {
		t.Fatalf("state=%d next=%d", state, coordinator.NextCommitSeq())
	}
}

func TestDeleteAndEmptyCommit(t *testing.T) {
	coordinator, log, active, mapping := coordinatorFixture(t)
	defer active.Close()
	b1 := makeBatch(t, 1, log)
	if err := b1.Put(context.Background(), 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Commit(context.Background(), b1); err != nil {
		t.Fatal(err)
	}
	b2 := makeBatch(t, 2, log)
	if err := b2.Delete(1); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Commit(context.Background(), b2); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := mapping.Lookup(1); ok {
		t.Fatal("delete remained visible")
	}
	empty := makeBatch(t, 3, log)
	if result, err := coordinator.Commit(context.Background(), empty); err != nil || result.CommitSeq != 3 {
		t.Fatalf("empty result=%+v error=%v", result, err)
	}
}

type recordingGroupLog struct {
	mu    sync.Mutex
	calls int
	seqs  [][]base.CommitSeq
}

func (l *recordingGroupLog) AppendCommit(prepared batch.Prepared, seq base.CommitSeq) (appendlog.CommitAppendResult, error) {
	results, err := l.AppendCommitGroup([]batch.Prepared{prepared}, []base.CommitSeq{seq})
	return results[0], err
}

func (l *recordingGroupLog) AppendCommitGroup(_ []batch.Prepared, seqs []base.CommitSeq) ([]appendlog.CommitAppendResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	l.seqs = append(l.seqs, append([]base.CommitSeq(nil), seqs...))
	results := make([]appendlog.CommitAppendResult, len(seqs))
	for i := range results {
		results[i].SealStarted = true
	}
	return results, nil
}

func TestGroupCommitUsesVirtualMappingAndSharedAppend(t *testing.T) {
	addr, _ := base.NewVAddr(1, 4096)
	log := &recordingGroupLog{}
	mapping := memory.NewEmpty()
	reader := fakeReader{records: map[base.VAddr]RecordHeader{
		addr: {RecordID: 1, OriginBatch: 7, PhysicalSize: 64},
	}}
	coordinator, err := NewGrouped(1, log, mapping, reader, Config{
		QueueDepth: 4, MaxBatches: 4, MaxBytes: 4096, MaxDelay: 50 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()
	b1, err := batch.New(7, batch.Limits{MaxValueSize: 16, MaxBatchBytes: 16, MaxBatchMutations: 2, MaxBatchConditions: 2}, fakeBatchAppender{addr}, &incrementAllocator{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.Put(context.Background(), 1, nil); err != nil {
		t.Fatal(err)
	}
	b2, err := batch.New(8, batch.Limits{MaxValueSize: 16, MaxBatchBytes: 16, MaxBatchMutations: 2, MaxBatchConditions: 2}, fakeBatchAppender{addr}, &incrementAllocator{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b2.ExpectRevision(1, 7); err != nil {
		t.Fatal(err)
	}
	prepared1, err := b1.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	prepared2, err := b2.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan response, 2)
	coordinator.process([]request{
		{ctx: context.Background(), batch: b1, prepared: prepared1, result: results},
		{ctx: context.Background(), batch: b2, prepared: prepared2, result: results},
	})
	seen := make(map[base.BatchID]base.CommitSeq)
	for i := 0; i < 2; i++ {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		seen[got.result.BatchID] = got.result.CommitSeq
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.calls != 1 || len(log.seqs) != 1 || len(log.seqs[0]) != 2 {
		t.Fatalf("group calls=%d seqs=%v", log.calls, log.seqs)
	}
	if seen[7] == 0 || seen[8] == 0 || seen[7] >= seen[8] {
		t.Fatalf("results=%v", seen)
	}
}

func TestBarrierOrdersPublishedCommits(t *testing.T) {
	addr1, _ := base.NewVAddr(1, 4096)
	addr2, _ := base.NewVAddr(1, 8192)
	log := &recordingGroupLog{}
	mapping := memory.NewEmpty()
	reader := fakeReader{records: map[base.VAddr]RecordHeader{
		addr1: {RecordID: 1, OriginBatch: 7, PhysicalSize: 64},
		addr2: {RecordID: 2, OriginBatch: 8, PhysicalSize: 64},
	}}
	coordinator, err := NewGrouped(1, log, mapping, reader, Config{
		QueueDepth: 8, MaxBatches: 8, MaxBytes: 4096, MaxDelay: time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()
	newBatch := func(id base.BatchID, recordID base.ID, addr base.VAddr) *batch.Batch {
		b, err := batch.New(id, batch.Limits{MaxValueSize: 16, MaxBatchBytes: 16, MaxBatchMutations: 2, MaxBatchConditions: 2}, fakeBatchAppender{addr}, &incrementAllocator{})
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put(context.Background(), recordID, nil); err != nil {
			t.Fatal(err)
		}
		return b
	}
	firstResult := make(chan error, 1)
	go func() {
		_, err := coordinator.Commit(context.Background(), newBatch(7, 1, addr1))
		firstResult <- err
	}()
	select {
	case err := <-firstResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first commit did not publish")
	}
	barrierEntered := make(chan struct{})
	releaseBarrier := make(chan struct{})
	barrierResult := make(chan error, 1)
	go func() {
		barrierResult <- coordinator.Barrier(context.Background(), func() error {
			if _, exists, err := mapping.Lookup(1); err != nil || !exists {
				return fmt.Errorf("first commit not published at barrier: exists=%v: %w", exists, err)
			}
			if _, exists, err := mapping.Lookup(2); err != nil || exists {
				return fmt.Errorf("later commit crossed barrier: exists=%v: %w", exists, err)
			}
			close(barrierEntered)
			<-releaseBarrier
			return nil
		})
	}()
	select {
	case <-barrierEntered:
	case <-time.After(time.Second):
		t.Fatal("barrier did not execute")
	}
	secondResult := make(chan error, 1)
	go func() {
		_, err := coordinator.Commit(context.Background(), newBatch(8, 2, addr2))
		secondResult <- err
	}()
	select {
	case err := <-secondResult:
		t.Fatalf("later commit completed during barrier: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseBarrier)
	if err := <-barrierResult; err != nil {
		t.Fatal(err)
	}
	if err := <-secondResult; err != nil {
		t.Fatal(err)
	}
}

func TestBarrierCancellationAndClose(t *testing.T) {
	coordinator, _, active, _ := coordinatorFixture(t)
	defer active.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	if err := coordinator.Barrier(ctx, func() error { called = true; return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("barrier error=%v", err)
	}
	if called {
		t.Fatal("cancelled barrier callback executed")
	}
	if err := coordinator.Close(); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Barrier(context.Background(), func() error { return nil }); !errors.Is(err, base.ErrClosed) {
		t.Fatalf("closed barrier error=%v", err)
	}
}

type fakeCommitLog struct {
	result appendlog.CommitAppendResult
	err    error
}

func (l fakeCommitLog) AppendCommit(batch.Prepared, base.CommitSeq) (appendlog.CommitAppendResult, error) {
	return l.result, l.err
}

type fakeReader struct{ records map[base.VAddr]RecordHeader }

func (r fakeReader) ReadPutHeader(addr base.VAddr) (RecordHeader, error) {
	header, ok := r.records[addr]
	if !ok {
		return RecordHeader{}, base.ErrCorrupt
	}
	return header, nil
}

type fakeBatchAppender struct{ addr base.VAddr }

func (a fakeBatchAppender) AppendPut(context.Context, base.BatchID, base.ID, []byte) (base.VAddr, base.FrameSeq, uint64, error) {
	return a.addr, 1, 64, nil
}
func (fakeBatchAppender) AppendAbort(context.Context, base.BatchID, storeformat.BatchAbortPayload) error {
	return nil
}

func TestCommitErrorClassificationAndCancellation(t *testing.T) {
	addr, _ := base.NewVAddr(1, 4096)
	newBatch := func(t *testing.T) *batch.Batch {
		b, err := batch.New(1, batch.Limits{MaxValueSize: 16, MaxBatchBytes: 16, MaxBatchMutations: 1, MaxBatchConditions: 1}, fakeBatchAppender{addr}, &incrementAllocator{})
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put(context.Background(), 1, nil); err != nil {
			t.Fatal(err)
		}
		return b
	}
	reader := fakeReader{records: map[base.VAddr]RecordHeader{addr: {RecordID: 1, OriginBatch: 1, PhysicalSize: 64}}}
	for _, tc := range []struct {
		name  string
		log   fakeCommitLog
		want  error
		state batch.State
		fault bool
	}{
		{"no-space", fakeCommitLog{err: segment.ErrFull}, segment.ErrFull, batch.StateAborted, false},
		{"part-write", fakeCommitLog{err: errors.New("write")}, nil, batch.StateAborted, true},
		{"seal-write", fakeCommitLog{result: appendlog.CommitAppendResult{SealStarted: true}, err: errors.New("sync")}, base.ErrCommitUnknown, batch.StateCommitUnknown, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mapping := memory.NewEmpty()
			coordinator, err := New(1, tc.log, mapping, reader)
			if err != nil {
				t.Fatal(err)
			}
			b := newBatch(t)
			_, gotErr := coordinator.Commit(context.Background(), b)
			if tc.want != nil && !errors.Is(gotErr, tc.want) {
				t.Fatalf("error=%v want=%v", gotErr, tc.want)
			}
			if tc.want == nil && gotErr == nil {
				t.Fatal("expected write error")
			}
			state, _ := b.State()
			if state != tc.state || (coordinator.Fault() != nil) != tc.fault {
				t.Fatalf("state=%d fault=%v error=%v", state, coordinator.Fault(), gotErr)
			}
		})
	}
	mapping := memory.NewEmpty()
	coordinator, err := New(1, fakeCommitLog{}, mapping, reader)
	if err != nil {
		t.Fatal(err)
	}
	b := newBatch(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := coordinator.Commit(ctx, b); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
	state, _ := b.State()
	if state != batch.StateAborted {
		t.Fatalf("cancel state=%d", state)
	}
}

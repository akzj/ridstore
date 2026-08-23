package appendlog

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/batch"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/segment"
)

func TestSequencerBatchesQueuedPutsAndSkipsCanceled(t *testing.T) {
	active, _ := newActive(t, 1<<20)
	defer active.Close()
	started := make(chan struct{})
	release := make(chan struct{})
	var appendWrites atomic.Int32
	active.SetHook(failpoint.Func(func(point failpoint.Point) error {
		if point == segment.PointBeforeAppendWrite {
			appendWrites.Add(1)
		} else if point == segment.PointBeforeSync {
			close(started)
			<-release
		}
		return nil
	}))
	log, err := New(active, 1, 1024, 64)
	if err != nil {
		t.Fatal(err)
	}
	sequencer, err := NewSequencer(log, SequencerConfig{QueueDepth: 8, MaxFrames: 8, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer sequencer.Close()

	blockDone := make(chan error, 1)
	go func() {
		_, err := sequencer.Barrier(context.Background())
		blockDone <- err
	}()
	<-started

	type outcome struct {
		seq base.FrameSeq
		err error
	}
	results := make(chan outcome, 3)
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < 3; i++ {
		requestCtx := context.Background()
		if i == 1 {
			requestCtx = ctx
		}
		go func(id int, requestCtx context.Context) {
			_, seq, _, err := sequencer.AppendPut(requestCtx, 1, base.ID(id+1), []byte("value"))
			results <- outcome{seq: seq, err: err}
		}(i, requestCtx)
	}
	deadline := time.Now().Add(time.Second)
	for len(sequencer.reqs) != 3 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if len(sequencer.reqs) != 3 {
		t.Fatalf("queued requests=%d", len(sequencer.reqs))
	}
	cancel()
	close(release)
	if err := <-blockDone; err != nil {
		t.Fatal(err)
	}
	var succeeded, canceled int
	for i := 0; i < 3; i++ {
		result := <-results
		if result.err == nil {
			succeeded++
		} else if errors.Is(result.err, context.Canceled) && result.seq == 0 {
			canceled++
		} else {
			t.Fatalf("result=%+v", result)
		}
	}
	if succeeded != 2 || canceled != 1 || log.NextFrameSeq() != 3 || appendWrites.Load() != 1 {
		t.Fatalf("succeeded=%d canceled=%d next=%d writes=%d", succeeded, canceled, log.NextFrameSeq(), appendWrites.Load())
	}
}

func TestSequencerExecutesEveryCommandInStreamOrder(t *testing.T) {
	active, _ := newActive(t, 1<<20)
	defer active.Close()
	log, err := New(active, 1, 1024, 64)
	if err != nil {
		t.Fatal(err)
	}
	sequencer, err := NewSequencer(log, SequencerConfig{QueueDepth: 8, MaxFrames: 8, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer sequencer.Close()

	addr, putSeq, _, err := sequencer.AppendPut(context.Background(), 1, 1, []byte("v"))
	if err != nil || putSeq != 1 {
		t.Fatalf("put seq=%d error=%v", putSeq, err)
	}
	if err := sequencer.AppendAbort(context.Background(), 2, storeformat.BatchAbortPayload{Reason: storeformat.AbortReasonCaller}); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.AppendReserve(context.Background(), storeformat.FrameTypeIDReserve, storeformat.ReservePayload{PreviousHighExclusive: 1, NewHighExclusive: 2, Generation: 1}); err != nil {
		t.Fatal(err)
	}
	commits, err := sequencer.AppendCommitGroup([]batch.Prepared{{BatchID: 1, Mutations: []batch.Mutation{{RecordID: 1, Operation: batch.Put, Addr: addr, ValueBytes: 1}}}}, []base.CommitSeq{1})
	if err != nil || len(commits) != 1 || commits[0].SealFrameSeq != 5 {
		t.Fatalf("commits=%+v error=%v", commits, err)
	}
	old, _ := base.NewVAddr(2, storeformat.SegmentHeaderSize)
	relocation, err := sequencer.AppendRelocation(RelocationPrepared{BatchID: 3, Entries: []RelocationEntry{{RecordID: 1, ExpectedOldAddr: old, NewAddr: addr}}}, 2)
	if err != nil || relocation.SealFrameSeq != 7 {
		t.Fatalf("relocation=%+v error=%v", relocation, err)
	}
	barrier, err := sequencer.Barrier(context.Background())
	if err != nil || barrier.LastFrameSeq != 7 || barrier.NextFrameSeq != 8 {
		t.Fatalf("barrier=%+v error=%v", barrier, err)
	}

	var types []storeformat.FrameType
	if err := active.Scan(func(_ base.VAddr, frame storeformat.Frame) error {
		types = append(types, frame.Type)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []storeformat.FrameType{
		storeformat.FrameTypePutRecord, storeformat.FrameTypeBatchAbort, storeformat.FrameTypeIDReserve,
		storeformat.FrameTypeCommitPart, storeformat.FrameTypeCommitSeal,
		storeformat.FrameTypeRelocationPart, storeformat.FrameTypeRelocationSeal,
	}
	if len(types) != len(want) {
		t.Fatalf("types=%v want=%v", types, want)
	}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("types=%v want=%v", types, want)
		}
	}
}

func TestSequencerOwnsConcurrentFrameOrderAndClose(t *testing.T) {
	active, _ := newActive(t, 1<<20)
	defer active.Close()
	log, err := New(active, 1, 1024, 64)
	if err != nil {
		t.Fatal(err)
	}
	sequencer, err := NewSequencer(log, SequencerConfig{QueueDepth: 4, MaxFrames: 4, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	const count = 32
	seqs := make(chan base.FrameSeq, count)
	var group sync.WaitGroup
	for i := 0; i < count; i++ {
		group.Add(1)
		go func(id int) {
			defer group.Done()
			_, seq, _, err := sequencer.AppendPut(context.Background(), 1, base.ID(id+1), nil)
			if err != nil {
				t.Errorf("append %d: %v", id, err)
				return
			}
			seqs <- seq
		}(i)
	}
	group.Wait()
	close(seqs)
	seen := make(map[base.FrameSeq]bool, count)
	for seq := range seqs {
		seen[seq] = true
	}
	for seq := base.FrameSeq(1); seq <= count; seq++ {
		if !seen[seq] {
			t.Fatalf("missing frame sequence %d", seq)
		}
	}
	if err := sequencer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := sequencer.AppendPut(context.Background(), 1, count+1, nil); !errors.Is(err, base.ErrClosed) {
		t.Fatalf("append after close: %v", err)
	}
}

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
	var blockFirstSync sync.Once
	var appendWrites atomic.Int32
	active.SetHook(failpoint.Func(func(point failpoint.Point) error {
		if point == segment.PointBeforeAppendWrite {
			appendWrites.Add(1)
		} else if point == segment.PointBeforeSync {
			blockFirstSync.Do(func() {
				close(started)
				<-release
			})
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
	if _, err := sequencer.Barrier(context.Background()); err != nil {
		t.Fatal(err)
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

func TestSequencerCoalescesReservedPutsAndCommitIntoOneWriteAndSync(t *testing.T) {
	active, _ := newActive(t, 1<<20)
	defer active.Close()
	var appendWrites atomic.Int32
	var syncs atomic.Int32
	active.SetHook(failpoint.Func(func(point failpoint.Point) error {
		switch point {
		case segment.PointBeforeAppendWrite:
			appendWrites.Add(1)
		case segment.PointBeforeSync:
			syncs.Add(1)
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

	value1 := []byte("one")
	addr1, _, size1, err := sequencer.AppendPut(context.Background(), 1, 1, value1)
	if err != nil {
		t.Fatal(err)
	}
	addr2, _, size2, err := sequencer.AppendPut(context.Background(), 2, 2, []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	if appendWrites.Load() != 0 || syncs.Load() != 0 {
		t.Fatalf("put performed I/O: writes=%d syncs=%d", appendWrites.Load(), syncs.Load())
	}
	value1[0] = 'X'
	watermarks, err := sequencer.Watermarks()
	if err != nil {
		t.Fatal(err)
	}
	if !(watermarks.DurablePos == watermarks.WrittenPos && watermarks.WrittenPos < watermarks.ReservedPos) {
		t.Fatalf("reserved watermarks=%+v", watermarks)
	}
	frame, ok := sequencer.ReadPendingFrame(addr1)
	if !ok || string(frame.Payload) != "one" {
		t.Fatalf("pending frame=%+v found=%v", frame, ok)
	}
	frame.Payload[0] = 'X'
	frame, ok = sequencer.ReadPendingFrame(addr1)
	if !ok || string(frame.Payload) != "one" {
		t.Fatalf("pending frame exposed mutable payload: %+v found=%v", frame, ok)
	}

	results, err := sequencer.AppendCommitGroup([]batch.Prepared{
		{BatchID: 1, LogicalPayloadBytes: 3, Mutations: []batch.Mutation{{RecordID: 1, Operation: batch.Put, Addr: addr1, ValueBytes: 3, PhysicalSize: size1}}},
		{BatchID: 2, LogicalPayloadBytes: 3, Mutations: []batch.Mutation{{RecordID: 2, Operation: batch.Put, Addr: addr2, ValueBytes: 3, PhysicalSize: size2}}},
	}, []base.CommitSeq{1, 2})
	if err != nil || len(results) != 2 {
		t.Fatalf("commit results=%+v error=%v", results, err)
	}
	if appendWrites.Load() != 1 || syncs.Load() != 1 {
		t.Fatalf("physical I/O writes=%d syncs=%d", appendWrites.Load(), syncs.Load())
	}
	watermarks, err = sequencer.Watermarks()
	if err != nil {
		t.Fatal(err)
	}
	if watermarks.ReservedPos != watermarks.WrittenPos || watermarks.WrittenPos != watermarks.DurablePos {
		t.Fatalf("durable watermarks=%+v", watermarks)
	}
	if _, ok := sequencer.ReadPendingFrame(addr1); ok {
		t.Fatal("durable frame remained in pending index")
	}
}

func TestSequencerBufferLimitWritesWithoutAdvancingDurability(t *testing.T) {
	active, _ := newActive(t, 1<<20)
	defer active.Close()
	var appendWrites atomic.Int32
	var syncs atomic.Int32
	active.SetHook(failpoint.Func(func(point failpoint.Point) error {
		switch point {
		case segment.PointBeforeAppendWrite:
			appendWrites.Add(1)
		case segment.PointBeforeSync:
			syncs.Add(1)
		}
		return nil
	}))
	log, err := New(active, 1, 1024, 64)
	if err != nil {
		t.Fatal(err)
	}
	sequencer, err := NewSequencer(log, SequencerConfig{QueueDepth: 4, MaxFrames: 2, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer sequencer.Close()
	for id := base.ID(1); id <= 3; id++ {
		if _, _, _, err := sequencer.AppendPut(context.Background(), 1, id, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if appendWrites.Load() != 1 || syncs.Load() != 0 {
		t.Fatalf("limit I/O writes=%d syncs=%d", appendWrites.Load(), syncs.Load())
	}
	watermarks, err := sequencer.Watermarks()
	if err != nil {
		t.Fatal(err)
	}
	if !(watermarks.DurablePos < watermarks.WrittenPos && watermarks.WrittenPos < watermarks.ReservedPos) {
		t.Fatalf("buffer-limit watermarks=%+v", watermarks)
	}
	if _, err := sequencer.Barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if appendWrites.Load() != 2 || syncs.Load() != 1 {
		t.Fatalf("barrier I/O writes=%d syncs=%d", appendWrites.Load(), syncs.Load())
	}
}

func TestSequencerClosePersistsReservedFrames(t *testing.T) {
	active, _ := newActive(t, 1<<20)
	defer active.Close()
	log, err := New(active, 1, 1024, 64)
	if err != nil {
		t.Fatal(err)
	}
	sequencer, err := NewSequencer(log, SequencerConfig{QueueDepth: 2, MaxFrames: 2, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	addr, _, _, err := sequencer.AppendPut(context.Background(), 1, 1, []byte("persist-on-close"))
	if err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Close(); err != nil {
		t.Fatal(err)
	}
	frame, err := active.ReadFrame(addr)
	if err != nil || string(frame.Payload) != "persist-on-close" {
		t.Fatalf("frame=%+v error=%v", frame, err)
	}
}

func TestBufferedCommitFailurePoisonsLogAndPreservesWatermarkTruth(t *testing.T) {
	injected := errors.New("injected append failure")
	for _, point := range []failpoint.Point{segment.PointBeforeAppendWrite, segment.PointBeforeSync} {
		t.Run(string(point), func(t *testing.T) {
			active, _ := newActive(t, 1<<20)
			defer active.Close()
			active.SetHook(failpoint.Func(func(hit failpoint.Point) error {
				if hit == point {
					return injected
				}
				return nil
			}))
			log, err := New(active, 1, 1024, 64)
			if err != nil {
				t.Fatal(err)
			}
			sequencer, err := NewSequencer(log, SequencerConfig{QueueDepth: 4, MaxFrames: 4, MaxBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			addr, _, physical, err := sequencer.AppendPut(context.Background(), 1, 1, []byte("value"))
			if err != nil {
				t.Fatal(err)
			}
			results, err := sequencer.AppendCommitGroup([]batch.Prepared{{BatchID: 1, Mutations: []batch.Mutation{{RecordID: 1, Operation: batch.Put, Addr: addr, ValueBytes: 5, PhysicalSize: physical}}}}, []base.CommitSeq{1})
			if !errors.Is(err, injected) || len(results) != 1 || !sequencer.Faulted() {
				t.Fatalf("results=%+v error=%v faulted=%v", results, err, sequencer.Faulted())
			}
			watermarks, watermarkErr := sequencer.Watermarks()
			if watermarkErr != nil {
				t.Fatal(watermarkErr)
			}
			if point == segment.PointBeforeAppendWrite {
				if watermarks.WrittenPos != watermarks.DurablePos || watermarks.ReservedPos <= watermarks.WrittenPos {
					t.Fatalf("write failure watermarks=%+v", watermarks)
				}
			} else if watermarks.ReservedPos != watermarks.WrittenPos || watermarks.DurablePos >= watermarks.WrittenPos {
				t.Fatalf("sync failure watermarks=%+v", watermarks)
			}
			if _, _, _, nextErr := sequencer.AppendPut(context.Background(), 2, 2, nil); !errors.Is(nextErr, segment.ErrPoisoned) {
				t.Fatalf("append after failure=%v", nextErr)
			}
			if closeErr := sequencer.Close(); point == segment.PointBeforeAppendWrite && !errors.Is(closeErr, segment.ErrPoisoned) {
				t.Fatalf("close after write failure=%v", closeErr)
			}
		})
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

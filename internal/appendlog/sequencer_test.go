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
	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/segment"
)

func TestSequencerBatchesQueuedPutsAndSkipsCanceled(t *testing.T) {
	active, _ := newActive(t, 1<<20)
	defer active.Close()
	var appendWrites atomic.Int32
	active.SetHook(failpoint.Func(func(point failpoint.Point) error {
		if point == segment.PointBeforeAppendWrite {
			appendWrites.Add(1)
		}
		return nil
	}))
	log, err := New(active, 1, 1024, 64)
	if err != nil {
		t.Fatal(err)
	}
	sequencer, err := NewBatchedSequencer(log, SequencerConfig{QueueDepth: 8, MaxFrames: 8, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer sequencer.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	blockDone := make(chan error, 1)
	go func() {
		_, err := sequencer.submit(context.Background(), func(*Log) any {
			close(started)
			<-release
			return nil
		})
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

func TestSequencerOwnsConcurrentFrameOrderAndClose(t *testing.T) {
	active, _ := newActive(t, 1<<20)
	defer active.Close()
	log, err := New(active, 1, 1024, 64)
	if err != nil {
		t.Fatal(err)
	}
	sequencer, err := NewSequencer(log, 4)
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

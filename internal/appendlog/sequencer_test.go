package appendlog

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/akzj/ridstore/internal/base"
)

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

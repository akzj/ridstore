package appendlog

import (
	"context"
	"sync"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/batch"
	storeformat "github.com/akzj/ridstore/internal/format"
)

var (
	_ batch.Appender = (*Sequencer)(nil)
)

// Sequencer gives one goroutine exclusive ownership of append request order.
// Once accepted, a request is joined before returning even if its caller's
// context is cancelled. This preserves ownership of caller buffers and lets
// the concrete Log make the final before-write cancellation decision.
type Sequencer struct {
	log *Log

	mu     sync.Mutex
	closed bool
	reqs   chan sequenceRequest
	done   chan struct{}
}

type sequenceRequest struct {
	run    func(*Log) any
	result chan any
}

func NewSequencer(log *Log, queueDepth int) (*Sequencer, error) {
	if log == nil || queueDepth <= 0 {
		return nil, base.ErrInvalidConfig
	}
	s := &Sequencer{
		log: log, reqs: make(chan sequenceRequest, queueDepth), done: make(chan struct{}),
	}
	go s.run()
	return s, nil
}

func (s *Sequencer) run() {
	defer close(s.done)
	for request := range s.reqs {
		request.result <- request.run(s.log)
	}
}

func (s *Sequencer) submit(ctx context.Context, run func(*Log) any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	request := sequenceRequest{run: run, result: make(chan any, 1)}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, base.ErrClosed
	}
	select {
	case s.reqs <- request:
		s.mu.Unlock()
	case <-ctx.Done():
		s.mu.Unlock()
		return nil, ctx.Err()
	}
	return <-request.result, nil
}

type putResult struct {
	addr    base.VAddr
	seq     base.FrameSeq
	written uint64
	err     error
}

func (s *Sequencer) AppendPut(ctx context.Context, batchID base.BatchID, id base.ID, value []byte) (base.VAddr, base.FrameSeq, uint64, error) {
	result, err := s.submit(ctx, func(log *Log) any {
		addr, seq, written, err := log.AppendPut(ctx, batchID, id, value)
		return putResult{addr: addr, seq: seq, written: written, err: err}
	})
	if err != nil {
		return 0, 0, 0, err
	}
	got := result.(putResult)
	return got.addr, got.seq, got.written, got.err
}

func (s *Sequencer) AppendAbort(ctx context.Context, batchID base.BatchID, payload storeformat.BatchAbortPayload) error {
	result, err := s.submit(ctx, func(log *Log) any { return log.AppendAbort(ctx, batchID, payload) })
	if err != nil {
		return err
	}
	if result == nil {
		return nil
	}
	return result.(error)
}

func (s *Sequencer) AppendReserve(ctx context.Context, typ storeformat.FrameType, payload storeformat.ReservePayload) error {
	result, err := s.submit(ctx, func(log *Log) any { return log.AppendReserve(ctx, typ, payload) })
	if err != nil {
		return err
	}
	if result == nil {
		return nil
	}
	return result.(error)
}

type commitResult struct {
	result CommitAppendResult
	err    error
}

func (s *Sequencer) AppendCommit(prepared batch.Prepared, seq base.CommitSeq) (CommitAppendResult, error) {
	result, err := s.submit(context.Background(), func(log *Log) any {
		got, err := log.AppendCommit(prepared, seq)
		return commitResult{result: got, err: err}
	})
	if err != nil {
		return CommitAppendResult{}, err
	}
	got := result.(commitResult)
	return got.result, got.err
}

func (s *Sequencer) NextFrameSeq() base.FrameSeq { return s.log.NextFrameSeq() }
func (s *Sequencer) Faulted() bool               { return s.log.Faulted() }

// Close rejects new requests and waits until every accepted request has left
// the sequencer. The underlying Data Segment remains owned by Store.
func (s *Sequencer) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return base.ErrClosed
	}
	s.closed = true
	close(s.reqs)
	s.mu.Unlock()
	<-s.done
	return nil
}

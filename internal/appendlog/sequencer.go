package appendlog

import (
	"context"
	"sync"
	"time"

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
	cfg SequencerConfig

	mu     sync.Mutex
	closed bool
	reqs   chan sequenceRequest
	done   chan struct{}
}

type sequenceRequest struct {
	put    *putSequenceRequest
	run    func(*Log) any
	result chan any
}

type putSequenceRequest struct {
	ctx     context.Context
	batchID base.BatchID
	id      base.ID
	value   []byte
}

type SequencerConfig struct {
	QueueDepth int
	MaxFrames  int
	MaxBytes   uint64
	MaxDelay   time.Duration
}

func NewSequencer(log *Log, queueDepth int) (*Sequencer, error) {
	return NewBatchedSequencer(log, SequencerConfig{QueueDepth: queueDepth, MaxFrames: 1, MaxBytes: ^uint64(0)})
}

func NewBatchedSequencer(log *Log, cfg SequencerConfig) (*Sequencer, error) {
	if log == nil || cfg.QueueDepth <= 0 || cfg.MaxFrames <= 0 || cfg.MaxBytes == 0 || cfg.MaxDelay < 0 {
		return nil, base.ErrInvalidConfig
	}
	s := &Sequencer{
		log: log, cfg: cfg, reqs: make(chan sequenceRequest, cfg.QueueDepth), done: make(chan struct{}),
	}
	go s.run()
	return s, nil
}

func (s *Sequencer) run() {
	defer close(s.done)
	var pending *sequenceRequest
	for {
		var request sequenceRequest
		if pending != nil {
			request, pending = *pending, nil
		} else {
			var ok bool
			request, ok = <-s.reqs
			if !ok {
				return
			}
		}
		if request.put == nil {
			request.result <- request.run(s.log)
			continue
		}

		group := []sequenceRequest{request}
		bytes := putRequestBytes(request.put)
		var timer *time.Timer
		var timeout <-chan time.Time
		if s.cfg.MaxDelay > 0 && len(group) < s.cfg.MaxFrames && bytes < s.cfg.MaxBytes {
			timer = time.NewTimer(s.cfg.MaxDelay)
			timeout = timer.C
		}
	collect:
		for len(group) < s.cfg.MaxFrames && bytes < s.cfg.MaxBytes {
			var next sequenceRequest
			var ok bool
			select {
			case next, ok = <-s.reqs:
				if !ok {
					break collect
				}
			case <-timeout:
				break collect
			default:
				if timeout == nil {
					break collect
				}
				select {
				case next, ok = <-s.reqs:
					if !ok {
						break collect
					}
				case <-timeout:
					break collect
				}
			}
			if next.put == nil {
				pending = &next
				break
			}
			nextBytes := putRequestBytes(next.put)
			if nextBytes > s.cfg.MaxBytes-bytes {
				pending = &next
				break
			}
			group = append(group, next)
			bytes += nextBytes
		}
		if timer != nil && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		puts := make([]PutRequest, len(group))
		for i := range group {
			puts[i] = PutRequest{Context: group[i].put.ctx, BatchID: group[i].put.batchID, RecordID: group[i].put.id, Value: group[i].put.value}
		}
		results := s.log.AppendPutGroup(puts)
		for i := range group {
			result := results[i]
			group[i].result <- putResult{addr: result.Addr, seq: result.Seq, written: result.Written, err: result.Err}
		}
	}
}

func putRequestBytes(request *putSequenceRequest) uint64 {
	// Frame encoding is 8-byte aligned. Config validation bounds Value well
	// below uint64 overflow, but saturating keeps this queue admission helper
	// independent from persisted hard limits.
	if uint64(len(request.value)) > ^uint64(0)-storeformat.FrameHeaderSize-7 {
		return ^uint64(0)
	}
	return (uint64(len(request.value)) + storeformat.FrameHeaderSize + 7) &^ 7
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
	if err := ctx.Err(); err != nil {
		return 0, 0, 0, err
	}
	request := sequenceRequest{
		put:    &putSequenceRequest{ctx: ctx, batchID: batchID, id: id, value: value},
		result: make(chan any, 1),
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, 0, 0, base.ErrClosed
	}
	select {
	case s.reqs <- request:
		s.mu.Unlock()
	case <-ctx.Done():
		s.mu.Unlock()
		return 0, 0, 0, ctx.Err()
	}
	result := <-request.result
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

type commitGroupResult struct {
	results []CommitAppendResult
	err     error
}

func (s *Sequencer) AppendRelocation(prepared RelocationPrepared, seq base.CommitSeq) (CommitAppendResult, error) {
	result, err := s.submit(context.Background(), func(log *Log) any {
		got, err := log.AppendRelocation(prepared, seq)
		return commitResult{result: got, err: err}
	})
	if err != nil {
		return CommitAppendResult{}, err
	}
	got := result.(commitResult)
	return got.result, got.err
}

func (s *Sequencer) AppendCommitGroup(prepared []batch.Prepared, seqs []base.CommitSeq) ([]CommitAppendResult, error) {
	result, err := s.submit(context.Background(), func(log *Log) any {
		got, err := log.AppendCommitGroup(prepared, seqs)
		return commitGroupResult{results: got, err: err}
	})
	if err != nil {
		return nil, err
	}
	got := result.(commitGroupResult)
	return got.results, got.err
}

type barrierResult struct {
	barrier Barrier
	err     error
}

func (s *Sequencer) Barrier(ctx context.Context) (Barrier, error) {
	result, err := s.submit(ctx, func(log *Log) any {
		barrier, err := log.Barrier()
		return barrierResult{barrier: barrier, err: err}
	})
	if err != nil {
		return Barrier{}, err
	}
	got := result.(barrierResult)
	return got.barrier, got.err
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

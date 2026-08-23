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
// It stages an available bounded cycle, performs at most one final sync for
// all durable requests in that cycle, and completes each request at its
// declared durability level. Once accepted, a request is joined before
// returning even if its caller's context is cancelled.
type Sequencer struct {
	log      *Log
	cfg      SequencerConfig
	buffered bool

	mu       sync.Mutex
	closed   bool
	closeErr error
	reqs     chan sequenceRequest
	done     chan struct{}
}

type sequenceRequest struct {
	kind       sequenceRequestKind
	durability requestDurability
	marker     bool
	ctx        context.Context

	batchID  base.BatchID
	recordID base.ID
	value    []byte

	frameType     storeformat.FrameType
	abort         storeformat.BatchAbortPayload
	reserve       storeformat.ReservePayload
	prepared      []batch.Prepared
	commitSeqs    []base.CommitSeq
	relocation    RelocationPrepared
	relocationSeq base.CommitSeq

	result chan sequenceResult
}

type sequenceRequestKind uint8

type requestDurability uint8

const (
	completeReserved requestDurability = iota + 1
	completeDurable
)

const (
	requestPut sequenceRequestKind = iota + 1
	requestAbort
	requestReserve
	requestCommitGroup
	requestRelocation
	requestBarrier
)

type sequenceResult struct {
	put        putAppendResult
	commits    []CommitAppendResult
	descriptor CommitAppendResult
	barrier    Barrier
	err        error
}

type cycleCompletion struct {
	request sequenceRequest
	result  sequenceResult
}

type SequencerConfig struct {
	QueueDepth int
	MaxFrames  int
	MaxBytes   uint64
}

func NewSequencer(log *Log, cfg SequencerConfig) (*Sequencer, error) {
	if log == nil || cfg.QueueDepth <= 0 || cfg.MaxFrames <= 0 || cfg.MaxBytes == 0 {
		return nil, base.ErrInvalidConfig
	}
	// Per-frame failpoint tests require exact physical write boundaries. The
	// production path has no Log hook and uses the buffered engine.
	buffered := log.hook == nil
	if buffered {
		if err := log.enableBuffer(cfg.MaxFrames, cfg.MaxBytes); err != nil {
			return nil, err
		}
	}
	s := &Sequencer{
		log: log, cfg: cfg, buffered: buffered, reqs: make(chan sequenceRequest, cfg.QueueDepth), done: make(chan struct{}),
	}
	go s.run()
	return s, nil
}

func (s *Sequencer) run() {
	defer close(s.done)
	if !s.buffered {
		for request := range s.reqs {
			request.result <- s.applyRequest(request)
		}
		return
	}
	closed := false
	for !closed {
		request, ok := <-s.reqs
		if !ok {
			break
		}

		completions := make([]cycleCompletion, 0, s.cfg.MaxFrames)
		processed := 0
		for {
			processed++
			result := s.applyRequest(request)
			if request.durability == completeDurable && result.err == nil {
				completions = append(completions, cycleCompletion{request: request, result: result})
			} else {
				request.result <- result
			}
			if request.marker || processed >= s.cfg.QueueDepth {
				break
			}
			select {
			case next, ok := <-s.reqs:
				if !ok {
					closed = true
					break
				}
				request = next
				continue
			default:
			}
			break
		}
		if len(completions) != 0 {
			outcome, syncErr := s.log.syncCycle()
			for i := range completions {
				finalizeCycleResult(&completions[i].result, outcome)
				if syncErr != nil && completions[i].result.err == nil {
					completions[i].result.err = syncErr
				}
				completions[i].request.result <- completions[i].result
			}
		}
	}
	err := s.log.closePending()
	s.mu.Lock()
	s.closeErr = err
	s.mu.Unlock()
}

func finalizeCycleResult(result *sequenceResult, outcome cycleOutcome) {
	for i := range result.commits {
		if cycleWrotePosition(outcome, result.commits[i].sealPos) {
			result.commits[i].SealStarted = true
		}
	}
	if cycleWrotePosition(outcome, result.descriptor.sealPos) {
		result.descriptor.SealStarted = true
	}
}

func cycleWrotePosition(outcome cycleOutcome, pos base.LogPos) bool {
	return pos != 0 && (outcome.writtenSegment > pos.SegmentID() ||
		(outcome.writtenSegment == pos.SegmentID() && outcome.writtenEnd > uint64(pos.Offset())))
}

// applyRequest validates and stages one logical request when buffering is
// enabled. Physical write and sync belong to the surrounding append cycle;
// Barrier is a zero-data marker that closes that cycle. The non-buffered
// failpoint path executes synchronously for crash testing.
func (s *Sequencer) applyRequest(request sequenceRequest) sequenceResult {
	switch request.kind {
	case requestPut:
		addr, seq, written, err := s.log.AppendPut(request.ctx, request.batchID, request.recordID, request.value)
		return sequenceResult{put: putAppendResult{Addr: addr, Seq: seq, Written: written, Err: err}}
	case requestAbort:
		return sequenceResult{err: s.log.AppendAbort(request.ctx, request.batchID, request.abort)}
	case requestReserve:
		return sequenceResult{err: s.log.AppendReserve(request.ctx, request.frameType, request.reserve)}
	case requestCommitGroup:
		results, err := s.log.AppendCommitGroup(request.prepared, request.commitSeqs)
		return sequenceResult{commits: results, err: err}
	case requestRelocation:
		result, err := s.log.AppendRelocation(request.relocation, request.relocationSeq)
		return sequenceResult{descriptor: result, err: err}
	case requestBarrier:
		if s.buffered {
			barrier, err := s.log.barrierMarker()
			return sequenceResult{barrier: barrier, err: err}
		}
		barrier, err := s.log.Barrier()
		return sequenceResult{barrier: barrier, err: err}
	default:
		return sequenceResult{err: base.ErrCorrupt}
	}
}

func (s *Sequencer) submit(ctx context.Context, request sequenceRequest) (sequenceResult, error) {
	if err := ctx.Err(); err != nil {
		return sequenceResult{}, err
	}
	if request.durability != completeReserved && request.durability != completeDurable {
		return sequenceResult{}, base.ErrInvalidConfig
	}
	request.ctx = ctx
	request.result = make(chan sequenceResult, 1)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return sequenceResult{}, base.ErrClosed
	}
	select {
	case s.reqs <- request:
		s.mu.Unlock()
	case <-ctx.Done():
		s.mu.Unlock()
		return sequenceResult{}, ctx.Err()
	}
	return <-request.result, nil
}

func (s *Sequencer) AppendPut(ctx context.Context, batchID base.BatchID, id base.ID, value []byte) (base.VAddr, base.FrameSeq, uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, 0, err
	}
	result, err := s.submit(ctx, sequenceRequest{kind: requestPut, durability: completeReserved, batchID: batchID, recordID: id, value: value})
	if err != nil {
		return 0, 0, 0, err
	}
	return result.put.Addr, result.put.Seq, result.put.Written, result.put.Err
}

func (s *Sequencer) AppendAbort(ctx context.Context, batchID base.BatchID, payload storeformat.BatchAbortPayload) error {
	result, err := s.submit(ctx, sequenceRequest{kind: requestAbort, durability: completeReserved, batchID: batchID, abort: payload})
	if err != nil {
		return err
	}
	return result.err
}

func (s *Sequencer) AppendReserve(ctx context.Context, typ storeformat.FrameType, payload storeformat.ReservePayload) error {
	result, err := s.submit(ctx, sequenceRequest{kind: requestReserve, durability: completeDurable, frameType: typ, reserve: payload})
	if err != nil {
		return err
	}
	return result.err
}

func (s *Sequencer) AppendRelocation(prepared RelocationPrepared, seq base.CommitSeq) (CommitAppendResult, error) {
	result, err := s.submit(context.Background(), sequenceRequest{kind: requestRelocation, durability: completeDurable, relocation: prepared, relocationSeq: seq})
	if err != nil {
		return CommitAppendResult{}, err
	}
	return result.descriptor, result.err
}

func (s *Sequencer) AppendCommitGroup(prepared []batch.Prepared, seqs []base.CommitSeq) ([]CommitAppendResult, error) {
	result, err := s.submit(context.Background(), sequenceRequest{kind: requestCommitGroup, durability: completeDurable, prepared: prepared, commitSeqs: seqs})
	if err != nil {
		return nil, err
	}
	return result.commits, result.err
}

func (s *Sequencer) Barrier(ctx context.Context) (Barrier, error) {
	result, err := s.submit(ctx, sequenceRequest{kind: requestBarrier, durability: completeDurable, marker: true})
	if err != nil {
		return Barrier{}, err
	}
	return result.barrier, result.err
}

func (s *Sequencer) NextFrameSeq() base.FrameSeq { return s.log.NextFrameSeq() }
func (s *Sequencer) Faulted() bool               { return s.log.Faulted() }
func (s *Sequencer) Watermarks() (Watermarks, error) {
	return s.log.Watermarks()
}

// ReadPendingFrame returns a safe copy of a reserved frame that has not yet
// reached the active segment. Disk readers should try this before Registry.
func (s *Sequencer) ReadPendingFrame(addr base.VAddr) (storeformat.Frame, bool) {
	return s.log.readPendingFrame(addr)
}

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
	s.mu.Lock()
	err := s.closeErr
	s.mu.Unlock()
	return err
}

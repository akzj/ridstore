package coordinator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/transaction"
)

type Appender interface {
	Append(context.Context, []byte, bool) (recordlog.AppendResult, error)
	Status() recordlog.Status
}

type Config struct {
	QueueCapacity   int
	MaxGroupBatches int
	MaxGroupPayload uint64
}

type Result struct {
	BatchID   model.BatchID
	CommitSeq model.CommitSeq
}

type response struct {
	result Result
	err    error
}

type CheckpointCut struct {
	CoveredCommitSeq model.CommitSeq
	ReplayStart      recordlog.LogPos
}

type checkpointResponse struct {
	cut CheckpointCut
	err error
}

type request struct {
	batch    *transaction.Batch
	prepared transaction.Prepared
	result   chan response
	barrier  chan checkpointResponse
}

type Coordinator struct {
	log     Appender
	mapping mapping.Index
	config  Config

	stateMu  sync.Mutex
	next     model.CommitSeq
	fault    error
	submitMu sync.Mutex
	closed   bool
	requests chan request
	done     chan struct{}
}

func New(next model.CommitSeq, log Appender, current mapping.Index, config Config) (*Coordinator, error) {
	if next == 0 || log == nil || current == nil || config.QueueCapacity <= 0 || config.MaxGroupBatches <= 0 || config.MaxGroupPayload == 0 ||
		config.MaxGroupPayload < uint64(recordcodec.CommitGroupHeadSize+recordcodec.DescriptorHeadSize) ||
		current.CoveredCommitSeq() == model.CommitSeq(math.MaxUint64) || next != current.CoveredCommitSeq()+1 {
		return nil, fmt.Errorf("coordinator configuration: %w", base.ErrInvalidConfig)
	}
	c := &Coordinator{
		log: log, mapping: current, config: config, next: next,
		requests: make(chan request, config.QueueCapacity), done: make(chan struct{}),
	}
	go c.run()
	return c, nil
}

func (c *Coordinator) Commit(ctx context.Context, batch *transaction.Batch) (Result, error) {
	if batch == nil {
		return Result{}, base.ErrInvalidConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.Fault(); err != nil {
		return Result{}, errors.Join(base.ErrReadOnly, err)
	}
	prepared, err := batch.Prepare()
	if err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		_ = batch.MarkAborted()
		return Result{}, err
	}
	req := request{batch: batch, prepared: prepared, result: make(chan response, 1)}
	c.submitMu.Lock()
	if c.closed {
		c.submitMu.Unlock()
		_ = batch.MarkAborted()
		return Result{}, base.ErrClosed
	}
	select {
	case c.requests <- req:
		c.submitMu.Unlock()
	case <-ctx.Done():
		c.submitMu.Unlock()
		_ = batch.MarkAborted()
		return Result{}, ctx.Err()
	}
	// Admission transfers completion ownership to the coordinator. The caller
	// joins the result even if ctx is cancelled while durability is in flight.
	answer := <-req.result
	return answer.result, answer.err
}

// CheckpointCut appends a durable marker after every commit admitted before
// this call and returns the exact replay position after that marker. Callers
// must separately quiesce non-commit RecordLog producers while capturing the
// rest of the checkpoint state.
func (c *Coordinator) CheckpointCut(ctx context.Context) (CheckpointCut, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return CheckpointCut{}, err
	}
	if err := c.Fault(); err != nil {
		return CheckpointCut{}, errors.Join(base.ErrReadOnly, err)
	}
	req := request{barrier: make(chan checkpointResponse, 1)}
	c.submitMu.Lock()
	if c.closed {
		c.submitMu.Unlock()
		return CheckpointCut{}, base.ErrClosed
	}
	select {
	case c.requests <- req:
		c.submitMu.Unlock()
	case <-ctx.Done():
		c.submitMu.Unlock()
		return CheckpointCut{}, ctx.Err()
	}
	answer := <-req.barrier
	return answer.cut, answer.err
}

func (c *Coordinator) Fault() error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.fault
}

func (c *Coordinator) NextCommitSeq() model.CommitSeq {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.next
}

func (c *Coordinator) Close() error {
	c.submitMu.Lock()
	if c.closed {
		c.submitMu.Unlock()
		return base.ErrClosed
	}
	c.closed = true
	close(c.requests)
	c.submitMu.Unlock()
	<-c.done
	return nil
}

func (c *Coordinator) run() {
	defer close(c.done)
	for first := range c.requests {
		if first.barrier != nil {
			c.processCheckpoint(first)
			continue
		}
		group := []request{first}
		descriptorBytes, err := descriptorPayloadSize(first.prepared)
		bytes := uint64(recordcodec.CommitGroupHeadSize) + descriptorBytes
		if err != nil || bytes > c.config.MaxGroupPayload {
			c.rejectInvalid(first, errors.Join(base.ErrBatchTooLarge, err))
			continue
		}
		for len(group) < c.config.MaxGroupBatches {
			select {
			case next, ok := <-c.requests:
				if !ok {
					c.process(group)
					return
				}
				if next.barrier != nil {
					c.process(group)
					c.processCheckpoint(next)
					group = nil
					break
				}
				nextBytes, sizeErr := descriptorPayloadSize(next.prepared)
				if sizeErr != nil || uint64(recordcodec.CommitGroupHeadSize)+nextBytes > c.config.MaxGroupPayload {
					c.rejectInvalid(next, errors.Join(base.ErrBatchTooLarge, sizeErr))
					continue
				}
				if bytes > c.config.MaxGroupPayload-nextBytes {
					c.process(group)
					group = []request{next}
					bytes = uint64(recordcodec.CommitGroupHeadSize) + nextBytes
					continue
				}
				group = append(group, next)
				bytes += nextBytes
			default:
				c.process(group)
				group = nil
			}
			if group == nil {
				break
			}
		}
		if len(group) != 0 {
			c.process(group)
		}
	}
}

func (c *Coordinator) processCheckpoint(req request) {
	if fault := c.Fault(); fault != nil {
		req.barrier <- checkpointResponse{err: errors.Join(base.ErrReadOnly, fault)}
		return
	}
	c.stateMu.Lock()
	next := c.next
	c.stateMu.Unlock()
	covered := c.mapping.CoveredCommitSeq()
	if next == 0 || covered == model.CommitSeq(math.MaxUint64) || next != covered+1 {
		err := fmt.Errorf("checkpoint commit sequence: %w", base.ErrCorrupt)
		c.fail(err)
		req.barrier <- checkpointResponse{err: err}
		return
	}
	payload := recordcodec.EncodeCheckpoint(recordcodec.CheckpointMarker{CoveredCommitSeq: covered})
	physical, err := c.log.Append(context.Background(), payload, true)
	if err != nil {
		c.fail(err)
		req.barrier <- checkpointResponse{err: err}
		return
	}
	req.barrier <- checkpointResponse{cut: CheckpointCut{CoveredCommitSeq: covered, ReplayStart: physical.End}}
}

func (c *Coordinator) process(group []request) {
	if fault := c.Fault(); fault != nil {
		for _, req := range group {
			_ = req.batch.MarkAborted()
			req.result <- response{err: errors.Join(base.ErrReadOnly, fault)}
		}
		return
	}
	active := make([]request, 0, len(group))
	proposals := make([]mapping.Proposal, 0, len(group))
	for _, req := range group {
		active = append(active, req)
		proposals = append(proposals, req.prepared.Proposal())
	}
	if len(active) == 0 {
		return
	}
	plan, err := c.mapping.ResolveGroup(proposals)
	if err != nil {
		c.fail(err)
		c.rejectGroup(active, err)
		return
	}

	c.stateMu.Lock()
	next := c.next
	c.stateMu.Unlock()
	descriptors := make([]recordcodec.Descriptor, 0, len(active))
	sequences := make([]model.CommitSeq, len(active))
	for i, resolved := range plan.Proposals {
		if !resolved.Accepted {
			continue
		}
		if next == model.CommitSeq(math.MaxUint64) {
			c.rejectGroup(active, base.ErrGenerationExhausted)
			return
		}
		sequences[i] = next
		descriptors = append(descriptors, descriptor(active[i].prepared, next))
		next++
	}
	if len(descriptors) == 0 {
		for _, req := range active {
			_ = req.batch.MarkAborted()
			req.result <- response{err: base.ErrConflict}
		}
		return
	}
	payload, err := recordcodec.EncodeCommitGroup(recordcodec.CommitGroup{Descriptors: descriptors}, c.config.MaxGroupPayload)
	if err != nil {
		c.fail(err)
		c.rejectGroup(active, err)
		return
	}
	if _, err := c.log.Append(context.Background(), payload, true); err != nil {
		c.fail(err)
		unknown := c.log.Status().Poisoned
		for i, req := range active {
			if !plan.Proposals[i].Accepted {
				_ = req.batch.MarkAborted()
				req.result <- response{err: base.ErrConflict}
				continue
			}
			if unknown {
				_ = req.batch.MarkCommitUnknown()
				req.result <- response{err: errors.Join(base.ErrCommitUnknown, err)}
			} else {
				_ = req.batch.MarkAborted()
				req.result <- response{err: err}
			}
		}
		return
	}
	published, err := c.mapping.PublishGroup(descriptors[0].CommitSeq, plan)
	if err == nil {
		var mutations uint64
		for _, item := range descriptors {
			mutations += uint64(len(item.Mutations))
		}
		if published.Committed != uint32(len(descriptors)) || uint64(published.Applied) != mutations || published.Skipped != 0 {
			err = fmt.Errorf("mapping publish result: %w", base.ErrCorrupt)
		}
	}
	if err != nil {
		c.fail(err)
		for i, req := range active {
			if plan.Proposals[i].Accepted {
				_ = req.batch.MarkCommitUnknown()
				req.result <- response{err: errors.Join(base.ErrCommitUnknown, err)}
			} else {
				_ = req.batch.MarkAborted()
				req.result <- response{err: base.ErrConflict}
			}
		}
		return
	}
	c.stateMu.Lock()
	c.next = next
	c.stateMu.Unlock()
	for i, req := range active {
		if !plan.Proposals[i].Accepted {
			_ = req.batch.MarkAborted()
			req.result <- response{err: base.ErrConflict}
			continue
		}
		seq := sequences[i]
		if err := req.batch.MarkCommitted(seq); err != nil {
			c.fail(err)
			_ = req.batch.MarkCommitUnknown()
			req.result <- response{err: errors.Join(base.ErrCommitUnknown, err)}
			continue
		}
		req.result <- response{result: Result{BatchID: req.prepared.BatchID, CommitSeq: seq}}
	}
}

func descriptorPayloadSize(prepared transaction.Prepared) (uint64, error) {
	descriptorSize, err := recordcodec.DescriptorSize(uint64(len(prepared.Mutations)))
	if err != nil {
		return 0, err
	}
	return uint64(descriptorSize), nil
}

func descriptor(prepared transaction.Prepared, seq model.CommitSeq) recordcodec.Descriptor {
	mutations := make([]recordcodec.Mutation, len(prepared.Mutations))
	for i, mutation := range prepared.Mutations {
		operation := recordcodec.OperationDelete
		if mutation.Operation == mapping.OperationPut {
			operation = recordcodec.OperationPut
		}
		mutations[i] = recordcodec.Mutation{RecordID: mutation.RecordID, NewAddr: mutation.Addr, Operation: operation}
	}
	return recordcodec.Descriptor{
		Kind: recordcodec.DescriptorUserCommit, BatchID: prepared.BatchID, CommitSeq: seq,
		LogicalPayloadBytes: prepared.LogicalPayloadBytes, Mutations: mutations,
	}
}

func (c *Coordinator) rejectInvalid(req request, err error) {
	_ = req.batch.MarkAborted()
	req.result <- response{err: err}
}

func (c *Coordinator) rejectGroup(group []request, err error) {
	for _, req := range group {
		_ = req.batch.MarkAborted()
		req.result <- response{err: err}
	}
}

func (c *Coordinator) fail(err error) {
	c.stateMu.Lock()
	if c.fault == nil {
		c.fault = err
	}
	c.stateMu.Unlock()
}

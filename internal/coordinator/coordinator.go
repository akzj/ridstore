package coordinator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

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

type Relocation struct {
	BatchID             model.BatchID
	Changes             []mapping.Change
	LogicalPayloadBytes uint64
}

type RelocationResult struct {
	BatchID   model.BatchID
	CommitSeq model.CommitSeq
	Applied   uint32
	Skipped   uint32
}

type response struct {
	result Result
	err    error
}

type Receipt struct {
	result                  <-chan response
	deltaPressureGeneration uint64
}

func (r Receipt) Wait() (Result, error) {
	if r.result == nil {
		return Result{}, base.ErrInvalidConfig
	}
	answer := <-r.result
	return answer.result, answer.err
}

// DeltaPressure reports that admission reached the Mapping soft limit. It is
// advisory: the admitted request still owns its normal durable completion.
func (r Receipt) DeltaPressure() bool { return r.deltaPressureGeneration != 0 }

// DeltaPressureGeneration identifies the active Mapping Delta admitted by
// this request. Equal generations may be coalesced into one Checkpoint.
func (r Receipt) DeltaPressureGeneration() uint64 { return r.deltaPressureGeneration }

type CheckpointCut struct {
	CoveredCommitSeq model.CommitSeq
	ReplayStart      recordlog.LogPos
}

// CheckpointFence is a durable commit-order cut that prevents new Commit and
// Relocation admission until Release. It lets checkpoint and Mapping-GC
// callers perform short cross-component state transitions without stopping
// reads, record appends, or GC copying.
type CheckpointFence struct {
	Cut  CheckpointCut
	once sync.Once
	c    *Coordinator
}

func (f *CheckpointFence) Release() {
	if f == nil || f.c == nil {
		return
	}
	f.once.Do(func() { f.c.admissionMu.Unlock() })
}

type checkpointResponse struct {
	cut CheckpointCut
	err error
}

type relocationResponse struct {
	result RelocationResult
	err    error
}

type request struct {
	batch      *transaction.Batch
	prepared   transaction.Prepared
	relocation *Relocation
	reserve    mapping.DeltaReservation
	result     chan response
	relocated  chan relocationResponse
	barrier    chan checkpointResponse
	queuedAt   time.Time
}

type Coordinator struct {
	log     Appender
	mapping mapping.Index
	config  Config

	stateMu sync.Mutex
	next    model.CommitSeq
	fault   error
	// admissionMu makes ReserveDelta -> queue admission atomic with a
	// checkpoint fence. It never covers durable append or result waiting.
	admissionMu sync.RWMutex
	submitMu    sync.Mutex
	closed      bool
	requests    chan request
	done        chan struct{}
	metrics     runtimeMetrics
}

func New(next model.CommitSeq, log Appender, current mapping.Index, config Config) (*Coordinator, error) {
	if next == 0 || log == nil || current == nil || ValidateConfig(config) != nil ||
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

func ValidateConfig(config Config) error {
	if config.QueueCapacity <= 0 || config.MaxGroupBatches <= 0 ||
		config.MaxGroupPayload < uint64(recordcodec.CommitGroupHeadSize+recordcodec.DescriptorHeadSize) {
		return base.ErrInvalidConfig
	}
	return nil
}

func (c *Coordinator) Commit(ctx context.Context, batch *transaction.Batch) (Result, error) {
	receipt, err := c.Submit(ctx, batch)
	if err != nil {
		return Result{}, err
	}
	return receipt.Wait()
}

// Relocate durably publishes physical-address CAS changes through the same
// ordering and fsync path as user commits. The caller must append and validate
// every NewAddr Put before admission and must allocate BatchID from the shared
// durable BatchID allocator.
func (c *Coordinator) Relocate(ctx context.Context, relocation Relocation) (RelocationResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return RelocationResult{}, err
	}
	if err := c.Fault(); err != nil {
		return RelocationResult{}, errors.Join(base.ErrReadOnly, err)
	}
	c.admissionMu.RLock()
	admissionHeld := true
	defer func() {
		if admissionHeld {
			c.admissionMu.RUnlock()
		}
	}()
	proposal := relocationProposal(relocation)
	if relocation.BatchID == 0 || mapping.ValidateProposal(proposal) != nil {
		return RelocationResult{}, base.ErrInvalidConfig
	}
	ids := make([]model.ID, len(relocation.Changes))
	for i := range relocation.Changes {
		ids[i] = relocation.Changes[i].RecordID
	}
	reservation, _, err := c.mapping.ReserveDelta(ids)
	if err != nil {
		return RelocationResult{}, err
	}
	owned := relocation
	owned.Changes = append([]mapping.Change(nil), relocation.Changes...)
	req := request{relocation: &owned, reserve: reservation, relocated: make(chan relocationResponse, 1)}
	c.submitMu.Lock()
	if c.closed {
		c.submitMu.Unlock()
		reservation.Release()
		return RelocationResult{}, base.ErrClosed
	}
	select {
	case c.requests <- req:
		c.submitMu.Unlock()
	case <-ctx.Done():
		c.submitMu.Unlock()
		reservation.Release()
		return RelocationResult{}, ctx.Err()
	}
	c.admissionMu.RUnlock()
	admissionHeld = false
	answer := <-req.relocated
	return answer.result, answer.err
}

// Submit reserves Delta capacity, transitions the Batch to Committing and
// transfers completion ownership to the Coordinator. It returns after the
// request is queued, before durability; Wait obtains the final result.
func (c *Coordinator) Submit(ctx context.Context, batch *transaction.Batch) (Receipt, error) {
	if batch == nil {
		return Receipt{}, base.ErrInvalidConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	if err := c.Fault(); err != nil {
		return Receipt{}, errors.Join(base.ErrReadOnly, err)
	}
	c.admissionMu.RLock()
	defer c.admissionMu.RUnlock()
	mutationIDs, err := batch.MutationIDs()
	if err != nil {
		return Receipt{}, err
	}
	reservation, pressureGeneration, err := c.mapping.ReserveDelta(mutationIDs)
	if err != nil {
		return Receipt{}, err
	}
	prepared, err := batch.Prepare()
	if err != nil {
		reservation.Release()
		return Receipt{}, err
	}
	if err := ctx.Err(); err != nil {
		reservation.Release()
		_ = batch.MarkAborted()
		return Receipt{}, err
	}
	req := request{batch: batch, prepared: prepared, reserve: reservation, result: make(chan response, 1), queuedAt: time.Now()}
	c.submitMu.Lock()
	if c.closed {
		c.submitMu.Unlock()
		reservation.Release()
		_ = batch.MarkAborted()
		return Receipt{}, base.ErrClosed
	}
	select {
	case c.requests <- req:
		c.metrics.commitQueued.Add(1)
		c.submitMu.Unlock()
	case <-ctx.Done():
		c.submitMu.Unlock()
		reservation.Release()
		_ = batch.MarkAborted()
		return Receipt{}, ctx.Err()
	}
	// Admission transfers completion ownership to the coordinator. Wait joins
	// the result even if ctx is cancelled while durability is in flight.
	return Receipt{result: req.result, deltaPressureGeneration: pressureGeneration}, nil
}

// CheckpointCut appends a durable marker after every commit admitted before
// this call and returns the exact replay position after that marker. Callers
// must separately quiesce non-commit RecordLog producers while capturing the
// rest of the checkpoint state.
func (c *Coordinator) CheckpointCut(ctx context.Context) (CheckpointCut, error) {
	fence, err := c.AcquireCheckpointFence(ctx)
	if err != nil {
		return CheckpointCut{}, err
	}
	defer fence.Release()
	return fence.Cut, nil
}

// AcquireCheckpointFence drains and durably marks every commit admitted before
// the call. New Commit and Relocation admission remains stopped until Release;
// callers must keep the fenced section short and must always release it.
func (c *Coordinator) AcquireCheckpointFence(ctx context.Context) (*CheckpointFence, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.Fault(); err != nil {
		return nil, errors.Join(base.ErrReadOnly, err)
	}
	c.admissionMu.Lock()
	release := true
	defer func() {
		if release {
			c.admissionMu.Unlock()
		}
	}()
	req := request{barrier: make(chan checkpointResponse, 1)}
	c.submitMu.Lock()
	if c.closed {
		c.submitMu.Unlock()
		return nil, base.ErrClosed
	}
	select {
	case c.requests <- req:
		c.submitMu.Unlock()
	case <-ctx.Done():
		c.submitMu.Unlock()
		return nil, ctx.Err()
	}
	answer := <-req.barrier
	if answer.err != nil {
		return nil, answer.err
	}
	release = false
	return &CheckpointFence{Cut: answer.cut, c: c}, nil
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
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()
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
		descriptorBytes, err := requestDescriptorSize(first)
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
				nextBytes, sizeErr := requestDescriptorSize(next)
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
	started := time.Now()
	userRequests := uint64(0)
	for _, req := range group {
		if req.relocation == nil {
			userRequests++
			c.metrics.queueWaitNanos.Add(uint64(started.Sub(req.queuedAt)))
		}
	}
	if fault := c.Fault(); fault != nil {
		for _, req := range group {
			c.completeFailure(req, errors.Join(base.ErrReadOnly, fault), false, false)
		}
		return
	}
	active := orderUserBeforeRelocation(group)
	proposals := make([]mapping.Proposal, 0, len(group))
	for _, req := range active {
		proposals = append(proposals, requestProposal(req))
	}
	if len(active) == 0 {
		return
	}
	plan, err := c.mapping.ResolveGroup(proposals)
	if err != nil {
		if userRequests != 0 {
			c.metrics.validationNanos.Add(uint64(time.Since(started)))
		}
		c.fail(err)
		c.rejectGroup(active, err)
		return
	}

	c.stateMu.Lock()
	next := c.next
	c.stateMu.Unlock()
	descriptors := make([]recordcodec.Descriptor, 0, len(active))
	sequences := make([]model.CommitSeq, len(active))
	acceptedUsers := uint64(0)
	for i, resolved := range plan.Proposals {
		if !resolved.Accepted {
			continue
		}
		if next == model.CommitSeq(math.MaxUint64) {
			c.rejectGroup(active, base.ErrGenerationExhausted)
			return
		}
		sequences[i] = next
		descriptors = append(descriptors, requestDescriptor(active[i], next))
		if active[i].relocation == nil {
			acceptedUsers++
		}
		next++
	}
	if len(descriptors) == 0 {
		if userRequests != 0 {
			c.metrics.validationNanos.Add(uint64(time.Since(started)))
		}
		for _, req := range active {
			if req.relocation == nil {
				c.metrics.conflicts.Add(1)
			}
			c.completeFailure(req, base.ErrConflict, false, false)
		}
		return
	}
	payload, err := recordcodec.EncodeCommitGroup(recordcodec.CommitGroup{Descriptors: descriptors}, c.config.MaxGroupPayload)
	if err != nil {
		if userRequests != 0 {
			c.metrics.validationNanos.Add(uint64(time.Since(started)))
		}
		c.fail(err)
		c.rejectGroup(active, err)
		return
	}
	if userRequests != 0 {
		c.metrics.validationNanos.Add(uint64(time.Since(started)))
	}
	if acceptedUsers != 0 {
		c.metrics.commitGroups.Add(1)
		c.metrics.groupBatches.Add(acceptedUsers)
	}
	writeStarted := time.Now()
	_, err = c.log.Append(context.Background(), payload, true)
	if acceptedUsers != 0 {
		c.metrics.writeSyncNanos.Add(uint64(time.Since(writeStarted)))
	}
	if err != nil {
		c.fail(err)
		unknown := c.log.Status().Poisoned
		for i, req := range active {
			if !plan.Proposals[i].Accepted {
				if req.relocation == nil {
					c.metrics.conflicts.Add(1)
				}
				c.completeFailure(req, base.ErrConflict, false, false)
				continue
			}
			c.completeFailure(req, err, unknown, true)
		}
		return
	}
	reservations := make([]mapping.DeltaReservation, len(active))
	for index := range active {
		reservations[index] = active[index].reserve
	}
	publishStarted := time.Now()
	published, err := c.mapping.PublishGroup(descriptors[0].CommitSeq, plan, reservations)
	if acceptedUsers != 0 {
		c.metrics.publishNanos.Add(uint64(time.Since(publishStarted)))
	}
	if err == nil {
		var applied, skipped uint64
		for _, proposal := range plan.Proposals {
			if !proposal.Accepted {
				continue
			}
			for _, change := range proposal.Changes {
				if change.Apply {
					applied++
				} else {
					skipped++
				}
			}
		}
		if published.Committed != uint32(len(descriptors)) || uint64(published.Applied) != applied || uint64(published.Skipped) != skipped {
			err = fmt.Errorf("mapping publish result: %w", base.ErrCorrupt)
		}
	}
	if err != nil {
		c.fail(err)
		for i, req := range active {
			if plan.Proposals[i].Accepted {
				c.completeFailure(req, err, true, true)
			} else {
				if req.relocation == nil {
					c.metrics.conflicts.Add(1)
				}
				c.completeFailure(req, base.ErrConflict, false, false)
			}
		}
		return
	}
	c.stateMu.Lock()
	c.next = next
	c.stateMu.Unlock()
	for i, req := range active {
		if !plan.Proposals[i].Accepted {
			if req.relocation == nil {
				c.metrics.conflicts.Add(1)
			}
			c.completeFailure(req, base.ErrConflict, false, false)
			continue
		}
		seq := sequences[i]
		if err := c.completeSuccess(req, seq, plan.Proposals[i]); err != nil {
			c.fail(err)
		}
	}
}

// orderUserBeforeRelocation gives foreground commits semantic priority within
// an already formed durability group. Both classes remain FIFO, and run()
// keeps checkpoint barriers outside the group.
func orderUserBeforeRelocation(group []request) []request {
	ordered := make([]request, 0, len(group))
	for _, req := range group {
		if req.relocation == nil {
			ordered = append(ordered, req)
		}
	}
	for _, req := range group {
		if req.relocation != nil {
			ordered = append(ordered, req)
		}
	}
	return ordered
}

func requestDescriptorSize(req request) (uint64, error) {
	mutations := len(req.prepared.Mutations)
	if req.relocation != nil {
		mutations = len(req.relocation.Changes)
	}
	descriptorSize, err := recordcodec.DescriptorSize(uint64(mutations))
	if err != nil {
		return 0, err
	}
	return uint64(descriptorSize), nil
}

func requestProposal(req request) mapping.Proposal {
	if req.relocation != nil {
		return relocationProposal(*req.relocation)
	}
	return req.prepared.Proposal()
}

func relocationProposal(relocation Relocation) mapping.Proposal {
	return mapping.Proposal{Kind: mapping.ProposalRelocation, Changes: append([]mapping.Change(nil), relocation.Changes...)}
}

func requestDescriptor(req request, seq model.CommitSeq) recordcodec.Descriptor {
	if req.relocation == nil {
		return descriptor(req.prepared, seq)
	}
	mutations := make([]recordcodec.Mutation, len(req.relocation.Changes))
	for i, change := range req.relocation.Changes {
		mutations[i] = recordcodec.Mutation{
			RecordID: change.RecordID, NewAddr: change.NewAddr,
			ExpectedOldAddr: change.ExpectedOldAddr, Operation: recordcodec.OperationRelocate,
		}
	}
	return recordcodec.Descriptor{
		Kind: recordcodec.DescriptorRelocation, BatchID: req.relocation.BatchID, CommitSeq: seq,
		LogicalPayloadBytes: req.relocation.LogicalPayloadBytes, Mutations: mutations,
	}
}

func (c *Coordinator) completeSuccess(req request, seq model.CommitSeq, resolved mapping.ResolvedProposal) error {
	if req.relocation != nil {
		result := RelocationResult{BatchID: req.relocation.BatchID, CommitSeq: seq}
		for _, change := range resolved.Changes {
			if change.Apply {
				result.Applied++
			} else {
				result.Skipped++
			}
		}
		req.relocated <- relocationResponse{result: result}
		return nil
	}
	if err := req.batch.MarkCommitted(seq); err != nil {
		_ = req.batch.MarkCommitUnknown()
		req.result <- response{err: errors.Join(base.ErrCommitUnknown, err)}
		return err
	}
	req.result <- response{result: Result{BatchID: req.prepared.BatchID, CommitSeq: seq}}
	return nil
}

func (c *Coordinator) completeFailure(req request, cause error, unknown, accepted bool) {
	req.reserve.Release()
	if req.relocation != nil {
		if unknown && accepted {
			cause = errors.Join(base.ErrCommitUnknown, cause)
		}
		req.relocated <- relocationResponse{err: cause}
		return
	}
	if unknown && accepted {
		_ = req.batch.MarkCommitUnknown()
		cause = errors.Join(base.ErrCommitUnknown, cause)
	} else {
		_ = req.batch.MarkAborted()
	}
	req.result <- response{err: cause}
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
	c.completeFailure(req, err, false, false)
}

func (c *Coordinator) rejectGroup(group []request, err error) {
	for _, req := range group {
		c.completeFailure(req, err, false, false)
	}
}

func (c *Coordinator) fail(err error) {
	c.stateMu.Lock()
	if c.fault == nil {
		c.fault = err
	}
	c.stateMu.Unlock()
}

package commit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/akzj/ridstore/internal/appendlog"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/batch"
	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/mapping/api"
	"github.com/akzj/ridstore/internal/metrics"
	"github.com/akzj/ridstore/internal/segment"
)

type RecordHeader struct {
	RecordID     base.ID
	OriginBatch  base.BatchID
	ValueBytes   uint64
	PhysicalSize uint64
}

type PutRecord struct {
	Header RecordHeader
	Value  []byte
}

type RecordReader interface {
	ReadPutHeader(base.VAddr) (RecordHeader, error)
}

type RelocationRecordReader interface {
	ReadPutRecord(base.VAddr) (PutRecord, error)
}

type CommitLog interface {
	AppendCommit(batch.Prepared, base.CommitSeq) (appendlog.CommitAppendResult, error)
}

type GroupCommitLog interface {
	AppendCommitGroup([]batch.Prepared, []base.CommitSeq) ([]appendlog.CommitAppendResult, error)
}

type RelocationLog interface {
	AppendRelocation(appendlog.RelocationPrepared, base.CommitSeq) (appendlog.CommitAppendResult, error)
}

type Result struct {
	BatchID   base.BatchID
	CommitSeq base.CommitSeq
}

type RelocationResult struct {
	BatchID   base.BatchID
	CommitSeq base.CommitSeq
	Applied   uint32
	Skipped   uint32
}

type Config struct {
	QueueDepth       int
	MaxBatches       int
	MaxBytes         uint64
	MaxDelay         time.Duration
	Metrics          *metrics.Runtime
	OnDeltaSoftLimit func()
}

type Coordinator struct {
	mu       sync.Mutex
	next     base.CommitSeq
	log      CommitLog
	mapping  api.Mapping
	reader   RecordReader
	faulted  bool
	faultErr error
	hook     failpoint.Hook
	config   Config

	submitMu    sync.Mutex
	closed      bool
	requests    chan request
	relocations chan request
	done        chan struct{}
}

type request struct {
	ctx         context.Context
	batch       *batch.Batch
	prepared    batch.Prepared
	result      chan response
	queuedAt    time.Time
	barrier     func() error
	reservation api.DeltaReservation
	softLimit   bool
	relocation  *relocationRequest
}

type relocationRequest struct {
	batchID base.BatchID
	changes []api.Change
}

type response struct {
	result     Result
	relocation RelocationResult
	err        error
}

type admittedRequest struct {
	request request
	seq     base.CommitSeq
}

type virtualEntry struct {
	addr   base.VAddr
	exists bool
}

const (
	PointMappingPublished failpoint.Point = "commit.mapping-published"
	PointResultReady      failpoint.Point = "commit.result-ready"
)

func New(next base.CommitSeq, log CommitLog, mapping api.Mapping, reader RecordReader) (*Coordinator, error) {
	return NewWithHook(next, log, mapping, reader, nil)
}

func NewWithHook(next base.CommitSeq, log CommitLog, mapping api.Mapping, reader RecordReader, hook failpoint.Hook) (*Coordinator, error) {
	return NewGrouped(next, log, mapping, reader, Config{QueueDepth: 1, MaxBatches: 1, MaxBytes: math.MaxUint64}, hook)
}

func NewGrouped(next base.CommitSeq, log CommitLog, mapping api.Mapping, reader RecordReader, config Config, hook failpoint.Hook) (*Coordinator, error) {
	if next == 0 || log == nil || mapping == nil || reader == nil || next <= mapping.CoveredCommitSeq() ||
		config.QueueDepth <= 0 || config.MaxBatches <= 0 || config.MaxBytes == 0 || config.MaxDelay < 0 {
		return nil, fmt.Errorf("commit coordinator configuration: %w", base.ErrInvalidConfig)
	}
	c := &Coordinator{
		next: next, log: log, mapping: mapping, reader: reader, hook: hook, config: config,
		requests: make(chan request, config.QueueDepth), relocations: make(chan request, config.QueueDepth), done: make(chan struct{}),
	}
	go c.run()
	return c, nil
}

func (c *Coordinator) Commit(ctx context.Context, b *batch.Batch) (Result, error) {
	if b == nil {
		return Result{}, fmt.Errorf("nil batch: %w", base.ErrInvalidConfig)
	}
	if fault := c.Fault(); fault != nil {
		return Result{}, errors.Join(base.ErrReadOnly, fault)
	}
	prepared, err := b.Prepare()
	if err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		_ = b.MarkAborted()
		return Result{}, err
	}
	var reservation api.DeltaReservation
	var soft bool
	if budget, ok := c.mapping.(api.DeltaBudget); ok {
		reservation, soft, err = budget.ReserveDelta(ctx, uint64(len(prepared.Mutations)))
		if err != nil {
			_ = b.MarkAborted()
			return Result{}, err
		}
	}
	request := request{ctx: ctx, batch: b, prepared: prepared, result: make(chan response, 1), queuedAt: time.Now(), reservation: reservation, softLimit: soft}
	c.submitMu.Lock()
	if c.closed {
		c.submitMu.Unlock()
		_ = b.MarkAborted()
		releaseReservation(request)
		return Result{}, base.ErrClosed
	}
	select {
	case c.requests <- request:
		if c.config.Metrics != nil {
			c.config.Metrics.CommitQueued()
		}
		c.submitMu.Unlock()
		if request.softLimit && c.config.OnDeltaSoftLimit != nil {
			c.config.OnDeltaSoftLimit()
		}
	case <-ctx.Done():
		c.submitMu.Unlock()
		_ = b.MarkAborted()
		releaseReservation(request)
		return Result{}, ctx.Err()
	}
	// Once admitted to the queue the caller joins the result. Returning early
	// would release Batch/Store ownership while a durable Seal may still appear.
	response := <-request.result
	return response.result, response.err
}

// Relocate submits one internal GC descriptor to the same serialization point
// as user commits. Once queued, the caller joins the result because a durable
// RelocationSeal may appear even if its context is cancelled concurrently.
func (c *Coordinator) Relocate(ctx context.Context, batchID base.BatchID, changes []api.Change) (RelocationResult, error) {
	if batchID == 0 || len(changes) == 0 {
		return RelocationResult{}, base.ErrInvalidConfig
	}
	if _, ok := c.log.(RelocationLog); !ok {
		return RelocationResult{}, base.ErrUnsupported
	}
	if _, ok := c.reader.(RelocationRecordReader); !ok {
		return RelocationResult{}, base.ErrUnsupported
	}
	if err := validateRelocationChanges(changes); err != nil {
		return RelocationResult{}, err
	}
	if fault := c.Fault(); fault != nil {
		return RelocationResult{}, errors.Join(base.ErrReadOnly, fault)
	}
	if err := ctx.Err(); err != nil {
		return RelocationResult{}, err
	}
	var reservation api.DeltaReservation
	var soft bool
	var err error
	if budget, ok := c.mapping.(api.DeltaBudget); ok {
		reservation, soft, err = budget.ReserveDelta(ctx, uint64(len(changes)))
		if err != nil {
			return RelocationResult{}, err
		}
	}
	request := request{
		ctx: ctx, result: make(chan response, 1), queuedAt: time.Now(), reservation: reservation, softLimit: soft,
		relocation: &relocationRequest{batchID: batchID, changes: append([]api.Change(nil), changes...)},
	}
	c.submitMu.Lock()
	if c.closed {
		c.submitMu.Unlock()
		releaseReservation(request)
		return RelocationResult{}, base.ErrClosed
	}
	select {
	case c.relocations <- request:
		c.submitMu.Unlock()
		if request.softLimit && c.config.OnDeltaSoftLimit != nil {
			c.config.OnDeltaSoftLimit()
		}
	case <-ctx.Done():
		c.submitMu.Unlock()
		releaseReservation(request)
		return RelocationResult{}, ctx.Err()
	}
	response := <-request.result
	return response.relocation, response.err
}

func (c *Coordinator) run() {
	defer close(c.done)
	var pending *request
	foreground, background := c.requests, c.relocations
	for {
		var first request
		if pending != nil {
			first, pending = *pending, nil
		} else {
			// Foreground Commit/Barrier requests always win when already queued.
			// A selected Relocation remains a bounded atomic descriptor, so newly
			// arriving foreground work waits for at most one GC batch.
			if foreground != nil {
				select {
				case request, ok := <-foreground:
					if !ok {
						foreground = nil
					} else {
						first = request
						goto selected
					}
				default:
				}
			}
			if foreground == nil && background == nil {
				return
			}
			select {
			case request, ok := <-foreground:
				if !ok {
					foreground = nil
					continue
				}
				first = request
			case request, ok := <-background:
				if !ok {
					background = nil
					continue
				}
				first = request
			}
		}
	selected:
		if first.barrier != nil {
			err := first.ctx.Err()
			if err == nil {
				err = first.barrier()
			}
			first.result <- response{err: err}
			continue
		}
		if first.relocation != nil {
			c.processRelocation(first)
			continue
		}
		group := []request{first}
		groupBytes := requestBytes(first.prepared)
		if c.config.MaxBatches > 1 {
			group, pending = c.collect(group, &groupBytes)
		}
		c.process(group)
	}
}

func (c *Coordinator) collect(group []request, groupBytes *uint64) ([]request, *request) {
	started := time.Now()
	for len(group) < c.config.MaxBatches {
		select {
		case request, ok := <-c.requests:
			if !ok {
				return group, nil
			}
			if request.barrier != nil || request.relocation != nil {
				return group, &request
			}
			bytes := requestBytes(request.prepared)
			if wouldExceed(*groupBytes, bytes, c.config.MaxBytes) {
				return group, &request
			}
			group = append(group, request)
			*groupBytes += bytes
			if *groupBytes >= c.config.MaxBytes {
				return group, nil
			}
		default:
			wait := c.groupWait(group, started)
			if wait <= 0 {
				return group, nil
			}
			timer := time.NewTimer(wait)
			select {
			case request, ok := <-c.requests:
				if !timer.Stop() {
					<-timer.C
				}
				if !ok {
					return group, nil
				}
				if request.barrier != nil || request.relocation != nil {
					return group, &request
				}
				bytes := requestBytes(request.prepared)
				if wouldExceed(*groupBytes, bytes, c.config.MaxBytes) {
					return group, &request
				}
				group = append(group, request)
				*groupBytes += bytes
			case <-timer.C:
				return group, nil
			}
		}
	}
	return group, nil
}

func (c *Coordinator) Barrier(ctx context.Context, barrier func() error) error {
	if barrier == nil {
		return base.ErrInvalidConfig
	}
	request := request{ctx: ctx, barrier: barrier, result: make(chan response, 1), queuedAt: time.Now()}
	c.submitMu.Lock()
	if c.closed {
		c.submitMu.Unlock()
		return base.ErrClosed
	}
	select {
	case c.requests <- request:
		c.submitMu.Unlock()
	case <-ctx.Done():
		c.submitMu.Unlock()
		return ctx.Err()
	}
	return (<-request.result).err
}

func (c *Coordinator) groupWait(group []request, started time.Time) time.Duration {
	if c.config.MaxDelay == 0 {
		return 0
	}
	wait := c.config.MaxDelay - time.Since(started)
	now := time.Now()
	for _, request := range group {
		if deadline, ok := request.ctx.Deadline(); ok {
			remaining := deadline.Sub(now)
			if remaining < wait {
				wait = remaining
			}
		}
	}
	return wait
}

func (c *Coordinator) process(group []request) {
	if len(group) == 0 {
		return
	}
	validationStarted := time.Now()
	if c.config.Metrics != nil {
		now := time.Now()
		for _, request := range group {
			c.config.Metrics.AddQueueWait(uint64(now.Sub(request.queuedAt)))
		}
	}
	if fault := c.Fault(); fault != nil {
		for _, request := range group {
			_ = request.batch.MarkAborted()
			c.recordAborted()
			releaseReservation(request)
			request.result <- response{err: errors.Join(base.ErrReadOnly, fault)}
		}
		return
	}
	virtual := make(map[base.ID]virtualEntry)
	admitted := make([]admittedRequest, 0, len(group))
	c.mu.Lock()
	next := c.next
	c.mu.Unlock()
	for i, request := range group {
		if err := request.ctx.Err(); err != nil {
			_ = request.batch.MarkAborted()
			c.recordAborted()
			releaseReservation(request)
			request.result <- response{err: err}
			continue
		}
		if next == base.CommitSeq(math.MaxUint64) {
			_ = request.batch.MarkAborted()
			c.recordAborted()
			releaseReservation(request)
			request.result <- response{err: base.ErrGenerationExhausted}
			continue
		}
		if err := c.validatePutRecords(request.prepared); err != nil {
			_ = request.batch.MarkAborted()
			c.recordAborted()
			releaseReservation(request)
			c.fail(err)
			request.result <- response{err: err}
			c.rejectTail(group[i+1:], err)
			return
		}
		conflict, err := c.hasConflictVirtual(request.prepared.Conditions, virtual)
		if err != nil {
			_ = request.batch.MarkAborted()
			c.recordAborted()
			releaseReservation(request)
			c.fail(err)
			request.result <- response{err: err}
			c.rejectTail(group[i+1:], err)
			return
		}
		if conflict {
			_ = request.batch.MarkAborted()
			releaseReservation(request)
			if c.config.Metrics != nil {
				c.config.Metrics.Conflict()
			}
			request.result <- response{err: base.ErrConflict}
			continue
		}
		admitted = append(admitted, admittedRequest{request: request, seq: next})
		for _, mutation := range request.prepared.Mutations {
			virtual[mutation.RecordID] = virtualEntry{addr: mutation.Addr, exists: mutation.Operation == batch.Put}
		}
		next++
	}
	if len(admitted) == 0 {
		c.recordValidation(validationStarted)
		return
	}
	c.recordValidation(validationStarted)
	if c.config.Metrics != nil {
		c.config.Metrics.CommitGroup(len(admitted))
	}
	prepared := make([]batch.Prepared, len(admitted))
	seqs := make([]base.CommitSeq, len(admitted))
	for i := range admitted {
		prepared[i], seqs[i] = admitted[i].request.prepared, admitted[i].seq
	}
	writeStarted := time.Now()
	appendResults, err := c.appendGroup(prepared, seqs)
	if c.config.Metrics != nil {
		c.config.Metrics.AddWriteSync(uint64(time.Since(writeStarted)))
	}
	if err != nil {
		c.handleAppendError(admitted, appendResults, err)
		return
	}
	c.mu.Lock()
	c.next = next
	c.mu.Unlock()
	for i := range admitted {
		publishStarted := time.Now()
		if err := c.publish(admitted[i]); err != nil {
			if c.config.Metrics != nil {
				c.config.Metrics.AddPublish(uint64(time.Since(publishStarted)))
			}
			c.rejectDurableTail(admitted[i+1:], err)
			return
		}
		if c.config.Metrics != nil {
			c.config.Metrics.AddPublish(uint64(time.Since(publishStarted)))
		}
	}
}

func (c *Coordinator) processRelocation(request request) {
	failBeforeSeal := func(err error, fault bool) {
		releaseReservation(request)
		if fault {
			c.fail(err)
		}
		request.result <- response{err: err}
	}
	if fault := c.Fault(); fault != nil {
		failBeforeSeal(errors.Join(base.ErrReadOnly, fault), false)
		return
	}
	if err := request.ctx.Err(); err != nil {
		failBeforeSeal(err, false)
		return
	}
	c.mu.Lock()
	seq := c.next
	c.mu.Unlock()
	if seq == base.CommitSeq(math.MaxUint64) {
		failBeforeSeal(base.ErrGenerationExhausted, false)
		return
	}
	prepared, changes, err := c.prepareRelocation(*request.relocation)
	if err != nil {
		failBeforeSeal(err, true)
		return
	}
	plan, err := c.mapping.Resolve(api.ApplyRelocation, changes)
	if err != nil {
		failBeforeSeal(err, true)
		return
	}
	appendResult, err := c.log.(RelocationLog).AppendRelocation(prepared, seq)
	if err != nil {
		releaseReservation(request)
		if appendResult.SealStarted {
			c.fail(err)
			request.result <- response{err: errors.Join(base.ErrCommitUnknown, err)}
			return
		}
		if !errors.Is(err, segment.ErrFull) {
			c.fail(err)
		}
		request.result <- response{err: err}
		return
	}
	c.mu.Lock()
	c.next = seq + 1
	c.mu.Unlock()
	var applied api.ApplyResult
	if budget, ok := c.mapping.(api.DeltaBudget); ok {
		applied, err = budget.ApplyResolvedReserved(request.reservation, seq, plan)
		request.reservation = nil
	} else {
		applied, err = c.mapping.ApplyResolved(seq, plan)
	}
	var wantApplied uint32
	for _, change := range plan.Changes {
		if change.Apply {
			wantApplied++
		}
	}
	wantSkipped := uint32(len(plan.Changes)) - wantApplied
	if err != nil || applied.Applied != wantApplied || applied.Skipped != wantSkipped {
		if err == nil {
			err = fmt.Errorf("relocation mapping result applied=%d/%d skipped=%d/%d: %w", applied.Applied, wantApplied, applied.Skipped, wantSkipped, base.ErrCorrupt)
		}
		c.fail(err)
		request.result <- response{err: errors.Join(base.ErrCommitUnknown, err)}
		return
	}
	request.result <- response{relocation: RelocationResult{
		BatchID: request.relocation.batchID, CommitSeq: seq, Applied: applied.Applied, Skipped: applied.Skipped,
	}}
}

func (c *Coordinator) prepareRelocation(request relocationRequest) (appendlog.RelocationPrepared, []api.Change, error) {
	reader := c.reader.(RelocationRecordReader)
	prepared := appendlog.RelocationPrepared{BatchID: request.batchID, Entries: make([]appendlog.RelocationEntry, len(request.changes))}
	changes := append([]api.Change(nil), request.changes...)
	for i, change := range changes {
		oldRecord, err := reader.ReadPutRecord(change.ExpectedOldAddr)
		if err != nil {
			return appendlog.RelocationPrepared{}, nil, err
		}
		newRecord, err := reader.ReadPutRecord(change.NewAddr)
		if err != nil {
			return appendlog.RelocationPrepared{}, nil, err
		}
		if oldRecord.Header.RecordID != change.RecordID || newRecord.Header.RecordID != change.RecordID ||
			oldRecord.Header.OriginBatch == 0 || oldRecord.Header.OriginBatch != newRecord.Header.OriginBatch ||
			request.batchID == oldRecord.Header.OriginBatch || oldRecord.Header.ValueBytes != newRecord.Header.ValueBytes ||
			oldRecord.Header.ValueBytes != uint64(len(oldRecord.Value)) || newRecord.Header.ValueBytes != uint64(len(newRecord.Value)) ||
			oldRecord.Header.PhysicalSize != newRecord.Header.PhysicalSize || !bytes.Equal(oldRecord.Value, newRecord.Value) {
			return appendlog.RelocationPrepared{}, nil, fmt.Errorf("relocation record identity mismatch: %w", base.ErrCorrupt)
		}
		prepared.LogicalPayloadBytes, err = base.AddUint64(prepared.LogicalPayloadBytes, newRecord.Header.ValueBytes)
		if err != nil {
			return appendlog.RelocationPrepared{}, nil, fmt.Errorf("relocation logical bytes: %w", base.ErrCorrupt)
		}
		prepared.Entries[i] = appendlog.RelocationEntry{RecordID: change.RecordID, ExpectedOldAddr: change.ExpectedOldAddr, NewAddr: change.NewAddr}
	}
	return prepared, changes, nil
}

func validateRelocationChanges(changes []api.Change) error {
	var previous base.ID
	for _, change := range changes {
		if change.RecordID == 0 || (previous != 0 && change.RecordID <= previous) || change.ExpectedOldAddr == 0 || change.NewAddr == 0 {
			return base.ErrInvalidConfig
		}
		previous = change.RecordID
	}
	return nil
}

func (c *Coordinator) appendGroup(prepared []batch.Prepared, seqs []base.CommitSeq) ([]appendlog.CommitAppendResult, error) {
	if groupLog, ok := c.log.(GroupCommitLog); ok {
		return groupLog.AppendCommitGroup(prepared, seqs)
	}
	results := make([]appendlog.CommitAppendResult, len(prepared))
	for i := range prepared {
		result, err := c.log.AppendCommit(prepared[i], seqs[i])
		results[i] = result
		if err != nil {
			return results, err
		}
	}
	return results, nil
}

func (c *Coordinator) handleAppendError(admitted []admittedRequest, results []appendlog.CommitAppendResult, err error) {
	if !errors.Is(err, segment.ErrFull) {
		c.fail(err)
	}
	for i, item := range admitted {
		releaseReservation(item.request)
		sealStarted := i < len(results) && results[i].SealStarted
		if sealStarted {
			_ = item.request.batch.MarkCommitUnknown()
			c.recordUnknown()
			item.request.result <- response{err: errors.Join(base.ErrCommitUnknown, err)}
		} else {
			_ = item.request.batch.MarkAborted()
			c.recordAborted()
			item.request.result <- response{err: err}
		}
	}
}

func (c *Coordinator) publish(item admittedRequest) error {
	changes := changesFor(item.request.prepared)
	var applyResult api.ApplyResult
	var err error
	if budget, ok := c.mapping.(api.DeltaBudget); ok {
		applyResult, err = budget.ApplyReserved(item.request.reservation, item.seq, api.ApplyUserCommit, changes)
		item.request.reservation = nil
	} else {
		applyResult, err = c.mapping.Apply(item.seq, api.ApplyUserCommit, changes)
	}
	if err != nil || applyResult.Applied != uint32(len(changes)) || applyResult.Skipped != 0 {
		if err == nil {
			err = fmt.Errorf("mapping applied %d/%d changes: %w", applyResult.Applied, len(changes), base.ErrCorrupt)
		}
		_ = item.request.batch.MarkCommitUnknown()
		c.recordUnknown()
		c.fail(err)
		item.request.result <- response{err: errors.Join(base.ErrCommitUnknown, err)}
		return err
	}
	if err := failpoint.Hit(c.hook, PointMappingPublished); err != nil {
		_ = item.request.batch.MarkCommitUnknown()
		c.recordUnknown()
		c.fail(err)
		item.request.result <- response{err: errors.Join(base.ErrCommitUnknown, err)}
		return err
	}
	if err := item.request.batch.MarkCommitted(item.seq); err != nil {
		_ = item.request.batch.MarkCommitUnknown()
		c.recordUnknown()
		c.fail(err)
		item.request.result <- response{err: errors.Join(base.ErrCommitUnknown, err)}
		return err
	}
	if c.config.Metrics != nil {
		c.config.Metrics.Committed()
	}
	if err := failpoint.Hit(c.hook, PointResultReady); err != nil {
		c.recordUnknown()
		c.fail(err)
		item.request.result <- response{err: errors.Join(base.ErrCommitUnknown, err)}
		return err
	}
	item.request.result <- response{result: Result{BatchID: item.request.prepared.BatchID, CommitSeq: item.seq}}
	return nil
}

func (c *Coordinator) validatePutRecords(prepared batch.Prepared) error {
	for _, mutation := range prepared.Mutations {
		if mutation.Operation != batch.Put {
			continue
		}
		header, err := c.reader.ReadPutHeader(mutation.Addr)
		if err != nil {
			return err
		}
		if header.RecordID != mutation.RecordID || header.OriginBatch != prepared.BatchID ||
			header.ValueBytes != mutation.ValueBytes || header.PhysicalSize != mutation.PhysicalSize {
			return fmt.Errorf("put record identity mismatch: %w", base.ErrCorrupt)
		}
	}
	return nil
}

func (c *Coordinator) hasConflictVirtual(conditions []batch.Condition, virtual map[base.ID]virtualEntry) (bool, error) {
	for _, condition := range conditions {
		entry, ok := virtual[condition.RecordID]
		var addr base.VAddr
		var exists bool
		var err error
		if ok {
			addr, exists = entry.addr, entry.exists
		} else {
			addr, exists, err = c.mapping.Lookup(condition.RecordID)
			if err != nil {
				return false, err
			}
		}
		switch condition.Kind {
		case batch.ConditionAbsent:
			if exists {
				return true, nil
			}
		case batch.ConditionRevision:
			if !exists {
				return true, nil
			}
			header, err := c.reader.ReadPutHeader(addr)
			if err != nil {
				return false, err
			}
			if header.RecordID != condition.RecordID {
				return false, fmt.Errorf("mapping record identity mismatch: %w", base.ErrCorrupt)
			}
			if base.Revision(header.OriginBatch) != condition.Revision {
				return true, nil
			}
		default:
			return false, fmt.Errorf("condition kind: %w", base.ErrCorrupt)
		}
	}
	return false, nil
}

func changesFor(prepared batch.Prepared) []api.Change {
	changes := make([]api.Change, len(prepared.Mutations))
	for i, mutation := range prepared.Mutations {
		changes[i] = api.Change{RecordID: mutation.RecordID}
		if mutation.Operation == batch.Put {
			changes[i].NewAddr = mutation.Addr
		}
	}
	return changes
}

func requestBytes(prepared batch.Prepared) uint64 {
	bytes := uint64(len(prepared.Mutations))*24 + 64
	if bytes < 64 {
		return math.MaxUint64
	}
	return bytes
}

func wouldExceed(current, added, limit uint64) bool {
	return current >= limit || added > limit-current
}

func (c *Coordinator) rejectTail(requests []request, cause error) {
	for _, request := range requests {
		releaseReservation(request)
		_ = request.batch.MarkAborted()
		c.recordAborted()
		request.result <- response{err: errors.Join(base.ErrReadOnly, cause)}
	}
}

func (c *Coordinator) rejectDurableTail(items []admittedRequest, cause error) {
	for _, item := range items {
		releaseReservation(item.request)
		_ = item.request.batch.MarkCommitUnknown()
		c.recordUnknown()
		item.request.result <- response{err: errors.Join(base.ErrCommitUnknown, cause)}
	}
}

func releaseReservation(request request) {
	if request.reservation != nil {
		request.reservation.Release()
	}
}

func (c *Coordinator) recordAborted() {
	if c.config.Metrics != nil {
		c.config.Metrics.Aborted()
	}
}

func (c *Coordinator) recordUnknown() {
	if c.config.Metrics != nil {
		c.config.Metrics.Unknown()
	}
}

func (c *Coordinator) recordValidation(started time.Time) {
	if c.config.Metrics != nil {
		c.config.Metrics.AddValidation(uint64(time.Since(started)))
	}
}

func (c *Coordinator) NextCommitSeq() base.CommitSeq {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.next
}

func (c *Coordinator) Fault() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.faultErr
}

func (c *Coordinator) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.faulted {
		c.faulted, c.faultErr = true, err
	}
}

func (c *Coordinator) Close() error {
	c.submitMu.Lock()
	if c.closed {
		c.submitMu.Unlock()
		return base.ErrClosed
	}
	c.closed = true
	close(c.requests)
	close(c.relocations)
	c.submitMu.Unlock()
	<-c.done
	return nil
}

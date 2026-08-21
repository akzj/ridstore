package commit

import (
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

type RecordReader interface {
	ReadPutHeader(base.VAddr) (RecordHeader, error)
}

type CommitLog interface {
	AppendCommit(batch.Prepared, base.CommitSeq) (appendlog.CommitAppendResult, error)
}

type GroupCommitLog interface {
	AppendCommitGroup([]batch.Prepared, []base.CommitSeq) ([]appendlog.CommitAppendResult, error)
}

type Result struct {
	BatchID   base.BatchID
	CommitSeq base.CommitSeq
}

type Config struct {
	QueueDepth int
	MaxBatches int
	MaxBytes   uint64
	MaxDelay   time.Duration
	Metrics    *metrics.Runtime
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

	submitMu sync.Mutex
	closed   bool
	requests chan request
	done     chan struct{}
}

type request struct {
	ctx      context.Context
	batch    *batch.Batch
	prepared batch.Prepared
	result   chan response
	queuedAt time.Time
}

type response struct {
	result Result
	err    error
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
		requests: make(chan request, config.QueueDepth), done: make(chan struct{}),
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
	request := request{ctx: ctx, batch: b, prepared: prepared, result: make(chan response, 1), queuedAt: time.Now()}
	c.submitMu.Lock()
	if c.closed {
		c.submitMu.Unlock()
		_ = b.MarkAborted()
		return Result{}, base.ErrClosed
	}
	select {
	case c.requests <- request:
		if c.config.Metrics != nil {
			c.config.Metrics.CommitQueued()
		}
		c.submitMu.Unlock()
	case <-ctx.Done():
		c.submitMu.Unlock()
		_ = b.MarkAborted()
		return Result{}, ctx.Err()
	}
	// Once admitted to the queue the caller joins the result. Returning early
	// would release Batch/Store ownership while a durable Seal may still appear.
	response := <-request.result
	return response.result, response.err
}

func (c *Coordinator) run() {
	defer close(c.done)
	for first := range c.requests {
		group := []request{first}
		groupBytes := requestBytes(first.prepared)
		if c.config.MaxBatches > 1 {
			group = c.collect(group, &groupBytes)
		}
		c.process(group)
	}
}

func (c *Coordinator) collect(group []request, groupBytes *uint64) []request {
	started := time.Now()
	for len(group) < c.config.MaxBatches {
		select {
		case request, ok := <-c.requests:
			if !ok {
				return group
			}
			bytes := requestBytes(request.prepared)
			if wouldExceed(*groupBytes, bytes, c.config.MaxBytes) {
				// Preserve FIFO without a secondary queue by processing the full
				// prefix now and this oversized request as a one-item group.
				c.process(group)
				group = group[:0]
				*groupBytes = 0
			}
			group = append(group, request)
			*groupBytes += bytes
			if *groupBytes >= c.config.MaxBytes {
				return group
			}
		default:
			wait := c.groupWait(group, started)
			if wait <= 0 {
				return group
			}
			timer := time.NewTimer(wait)
			select {
			case request, ok := <-c.requests:
				if !timer.Stop() {
					<-timer.C
				}
				if !ok {
					return group
				}
				bytes := requestBytes(request.prepared)
				if wouldExceed(*groupBytes, bytes, c.config.MaxBytes) {
					c.process(group)
					group = group[:0]
					*groupBytes = 0
				}
				group = append(group, request)
				*groupBytes += bytes
			case <-timer.C:
				return group
			}
		}
	}
	return group
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
			request.result <- response{err: err}
			continue
		}
		if next == base.CommitSeq(math.MaxUint64) {
			_ = request.batch.MarkAborted()
			c.recordAborted()
			request.result <- response{err: base.ErrGenerationExhausted}
			continue
		}
		if err := c.validatePutRecords(request.prepared); err != nil {
			_ = request.batch.MarkAborted()
			c.recordAborted()
			c.fail(err)
			request.result <- response{err: err}
			c.rejectTail(group[i+1:], err)
			return
		}
		conflict, err := c.hasConflictVirtual(request.prepared.Conditions, virtual)
		if err != nil {
			_ = request.batch.MarkAborted()
			c.recordAborted()
			c.fail(err)
			request.result <- response{err: err}
			c.rejectTail(group[i+1:], err)
			return
		}
		if conflict {
			_ = request.batch.MarkAborted()
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
	applyResult, err := c.mapping.Apply(item.seq, api.ApplyUserCommit, changes)
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
		_ = request.batch.MarkAborted()
		c.recordAborted()
		request.result <- response{err: errors.Join(base.ErrReadOnly, cause)}
	}
}

func (c *Coordinator) rejectDurableTail(items []admittedRequest, cause error) {
	for _, item := range items {
		_ = item.request.batch.MarkCommitUnknown()
		c.recordUnknown()
		item.request.result <- response{err: errors.Join(base.ErrCommitUnknown, cause)}
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
	c.submitMu.Unlock()
	<-c.done
	return nil
}

package ridstore

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/akzj/ridstore/internal/base"
	batchimpl "github.com/akzj/ridstore/internal/batch"
	storeformat "github.com/akzj/ridstore/internal/format"
)

type Record struct {
	Value    []byte
	Revision Revision
}

type BatchState uint8

const (
	BatchStateOpen BatchState = iota + 1
	BatchStateCommitting
	BatchStateCommitted
	BatchStateAborted
	BatchStateCommitUnknown
)

type BatchStatus struct {
	BatchID   BatchID
	State     BatchState
	CommitSeq CommitSeq
}

type CommitResult struct {
	BatchID   BatchID
	CommitSeq CommitSeq
}

type Metrics struct {
	CommitQueued, CommitGroups, GroupBatches     uint64
	Committed, Aborted, Conflicts, CommitUnknown uint64
	QueueWaitNanos, ValidationNanos              uint64
	WriteSyncNanos, PublishNanos                 uint64
}

func (s *Store) Metrics() Metrics {
	if s == nil || s.metrics == nil {
		return Metrics{}
	}
	snapshot := s.metrics.Snapshot()
	return Metrics{
		CommitQueued: snapshot.CommitQueued, CommitGroups: snapshot.CommitGroups, GroupBatches: snapshot.GroupBatches,
		Committed: snapshot.Committed, Aborted: snapshot.Aborted, Conflicts: snapshot.Conflicts, CommitUnknown: snapshot.CommitUnknown,
		QueueWaitNanos: snapshot.QueueWaitNanos, ValidationNanos: snapshot.ValidationNanos,
		WriteSyncNanos: snapshot.WriteSyncNanos, PublishNanos: snapshot.PublishNanos,
	}
}

type Batch struct {
	store *Store
	inner *batchimpl.Batch
	done  sync.Once
	mu    sync.Mutex
	pins  map[base.DataSegmentID]struct{}
}

type batchAppender struct {
	batch *Batch
}

func (a batchAppender) AppendPut(ctx context.Context, batchID base.BatchID, id base.ID, value []byte) (base.VAddr, base.FrameSeq, uint64, error) {
	addr, seq, written, err := a.batch.store.log.AppendPut(ctx, batchID, id, value)
	if err != nil {
		return addr, seq, written, err
	}
	if err := a.batch.pin(addr.SegmentID()); err != nil {
		a.batch.store.setFault(err)
		return 0, 0, written, err
	}
	return addr, seq, written, nil
}

func (a batchAppender) AppendAbort(ctx context.Context, batchID base.BatchID, payload storeformat.BatchAbortPayload) error {
	return a.batch.store.log.AppendAbort(ctx, batchID, payload)
}

func (s *Store) Begin(ctx context.Context) (*Batch, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.ops.RLock()
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			s.ops.RUnlock()
			return nil, base.ErrClosed
		}
		if s.fault != nil {
			err := errors.Join(base.ErrReadOnly, s.fault)
			s.mu.Unlock()
			s.ops.RUnlock()
			return nil, err
		}
		if s.openCount < s.config.MaxOpenBatches {
			s.openCount++
			s.mu.Unlock()
			break
		}
		s.mu.Unlock()
		s.ops.RUnlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.slotNotify:
		}
	}
	id, err := s.batchAllocator.Allocate(ctx)
	if err != nil {
		s.releaseOpenSlot()
		s.ops.RUnlock()
		if s.log.Faulted() {
			s.setFault(err)
		}
		return nil, err
	}
	b := &Batch{store: s, pins: make(map[base.DataSegmentID]struct{})}
	inner, err := batchimpl.New(base.BatchID(id), batchimpl.Limits{
		MaxValueSize: uint64(s.config.MaxValueSize), MaxBatchBytes: uint64(s.config.MaxBatchBytes),
		MaxBatchMutations: uint64(s.config.MaxBatchMutations), MaxBatchConditions: uint64(s.config.MaxBatchConditions),
	}, batchAppender{batch: b}, s.idAllocator)
	if err != nil {
		s.releaseOpenSlot()
		s.ops.RUnlock()
		return nil, err
	}
	b.inner = inner
	s.mu.Lock()
	s.batches[b.ID()] = b
	if uint64(b.ID()) >= s.issuedBatchHigh {
		s.issuedBatchHigh = uint64(b.ID()) + 1
	}
	s.mu.Unlock()
	s.ops.RUnlock()
	return b, nil
}

func (s *Store) Get(ctx context.Context, id ID) ([]byte, error) {
	record, err := s.GetRecord(ctx, id)
	if err != nil {
		return nil, err
	}
	return record.Value, nil
}

func (s *Store) GetRecord(ctx context.Context, id ID) (Record, error) {
	if id == 0 {
		return Record{}, base.ErrInvalidID
	}
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	s.ops.RLock()
	defer s.ops.RUnlock()
	if err := s.checkAvailable(); err != nil {
		return Record{}, err
	}
	addr, ok, err := s.mapping.Lookup(id)
	if err != nil {
		return Record{}, err
	}
	if !ok {
		return Record{}, base.ErrNotFound
	}
	frame, err := s.segments.ReadFrame(addr)
	if err != nil {
		s.setFault(err)
		return Record{}, err
	}
	if frame.Type != storeformat.FrameTypePutRecord || frame.RecordID != id || frame.BatchID == 0 {
		err := fmt.Errorf("mapping points to wrong record: %w", base.ErrCorrupt)
		s.setFault(err)
		return Record{}, err
	}
	return Record{Value: append([]byte(nil), frame.Payload...), Revision: Revision(frame.BatchID)}, nil
}

func (s *Store) Status(ctx context.Context, id BatchID) (BatchStatus, error) {
	if id == 0 {
		return BatchStatus{}, base.ErrInvalidID
	}
	if err := ctx.Err(); err != nil {
		return BatchStatus{}, err
	}
	s.ops.RLock()
	defer s.ops.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return BatchStatus{}, base.ErrClosed
	}
	if b, ok := s.batches[id]; ok {
		state, seq := b.inner.State()
		return BatchStatus{BatchID: id, State: publicBatchState(state), CommitSeq: seq}, nil
	}
	if status, ok := s.statuses[id]; ok {
		return status, nil
	}
	raw := uint64(id)
	if raw >= s.recoveryAbortedStart && raw < s.recoveryAbortedEnd {
		return BatchStatus{BatchID: id, State: BatchStateAborted}, nil
	}
	if raw < s.manifest.IssuedBatchIDHighExclusiveAtCut || raw < s.issuedBatchHigh {
		return BatchStatus{}, base.ErrStatusExpired
	}
	return BatchStatus{}, base.ErrNotFound
}

func (s *Store) Checkpoint(context.Context) error { return base.ErrUnsupported }

func (b *Batch) ID() BatchID {
	if b == nil || b.inner == nil {
		return 0
	}
	return b.inner.ID()
}

func (b *Batch) Allocate(ctx context.Context) (ID, error) {
	if b == nil || b.store == nil {
		return 0, base.ErrBatchClosed
	}
	b.store.ops.RLock()
	defer b.store.ops.RUnlock()
	if err := b.store.checkAvailable(); err != nil {
		return 0, err
	}
	id, err := b.inner.Allocate(ctx)
	if err != nil && b.store.log.Faulted() {
		b.store.setFault(err)
	}
	return id, err
}

func (b *Batch) Put(ctx context.Context, id ID, value []byte) error {
	if b == nil || b.store == nil {
		return base.ErrBatchClosed
	}
	b.store.ops.RLock()
	defer b.store.ops.RUnlock()
	if err := b.store.checkAvailable(); err != nil {
		return err
	}
	err := b.inner.Put(ctx, id, value)
	if err != nil && b.store.log.Faulted() {
		b.store.setFault(err)
	}
	return err
}

func (b *Batch) Delete(ctx context.Context, id ID) error {
	if b == nil || b.store == nil {
		return base.ErrBatchClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.store.ops.RLock()
	defer b.store.ops.RUnlock()
	if err := b.store.checkAvailable(); err != nil {
		return err
	}
	return b.inner.Delete(id)
}

func (b *Batch) ExpectRevision(id ID, revision Revision) error {
	if b == nil || b.store == nil {
		return base.ErrBatchClosed
	}
	b.store.ops.RLock()
	defer b.store.ops.RUnlock()
	if err := b.store.checkAvailable(); err != nil {
		return err
	}
	return b.inner.ExpectRevision(id, revision)
}

func (b *Batch) ExpectAbsent(id ID) error {
	if b == nil || b.store == nil {
		return base.ErrBatchClosed
	}
	b.store.ops.RLock()
	defer b.store.ops.RUnlock()
	if err := b.store.checkAvailable(); err != nil {
		return err
	}
	return b.inner.ExpectAbsent(id)
}

func (b *Batch) Commit(ctx context.Context) (CommitResult, error) {
	if b == nil || b.store == nil {
		return CommitResult{}, base.ErrBatchClosed
	}
	b.store.ops.RLock()
	defer b.store.ops.RUnlock()
	if err := b.store.checkAvailable(); err != nil {
		return CommitResult{}, err
	}
	result, err := b.store.coordinator.Commit(ctx, b.inner)
	state, seq := b.inner.State()
	if state == batchimpl.StateCommitted || state == batchimpl.StateAborted || state == batchimpl.StateCommitUnknown {
		status := BatchStatus{BatchID: b.ID(), State: publicBatchState(state), CommitSeq: seq}
		b.finish(status)
	}
	if fault := b.store.coordinator.Fault(); fault != nil {
		b.store.setFault(fault)
	}
	return CommitResult{BatchID: result.BatchID, CommitSeq: result.CommitSeq}, err
}

func (b *Batch) Abort(ctx context.Context) error {
	if b == nil || b.store == nil {
		return base.ErrBatchClosed
	}
	b.store.ops.RLock()
	defer b.store.ops.RUnlock()
	err := b.inner.Abort(ctx, storeformat.AbortReasonCaller)
	state, _ := b.inner.State()
	if state == batchimpl.StateAborted {
		b.finish(BatchStatus{BatchID: b.ID(), State: BatchStateAborted})
	}
	if err != nil && b.store.log.Faulted() {
		b.store.setFault(err)
	}
	return err
}

func (b *Batch) finish(status BatchStatus) {
	b.done.Do(func() {
		s := b.store
		for _, id := range b.takePins() {
			if err := s.segments.UnpinOpenBatch(id); err != nil {
				s.setFault(err)
			}
		}
		s.mu.Lock()
		delete(s.batches, b.ID())
		s.statuses[b.ID()] = status
		if s.openCount > 0 {
			s.openCount--
		}
		s.signalSlotLocked()
		s.mu.Unlock()
	})
}

func (b *Batch) pin(id base.DataSegmentID) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.pins[id]; exists {
		return nil
	}
	if err := b.store.segments.PinOpenBatch(id); err != nil {
		return err
	}
	b.pins[id] = struct{}{}
	return nil
}

func (b *Batch) takePins() []base.DataSegmentID {
	b.mu.Lock()
	defer b.mu.Unlock()
	ids := make([]base.DataSegmentID, 0, len(b.pins))
	for id := range b.pins {
		ids = append(ids, id)
	}
	b.pins = nil
	return ids
}

func (s *Store) releaseOpenSlot() {
	s.mu.Lock()
	if s.openCount > 0 {
		s.openCount--
	}
	s.signalSlotLocked()
	s.mu.Unlock()
}

func publicBatchState(state batchimpl.State) BatchState {
	switch state {
	case batchimpl.StateOpen, batchimpl.StateFailed:
		return BatchStateOpen
	case batchimpl.StateCommitting:
		return BatchStateCommitting
	case batchimpl.StateCommitted:
		return BatchStateCommitted
	case batchimpl.StateAborted:
		return BatchStateAborted
	case batchimpl.StateCommitUnknown:
		return BatchStateCommitUnknown
	default:
		return 0
	}
}

package ridstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/akzj/ridstore/internal/appendlog"
	"github.com/akzj/ridstore/internal/base"
	batchimpl "github.com/akzj/ridstore/internal/batch"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/mapping/radix"
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
	DeltaChargedBytes, DeltaReservedBytes        uint64
	DeltaSoftLimitBytes, DeltaHardLimitBytes     uint64
}

const (
	pointCheckpointMappingSynced     failpoint.Point = "checkpoint.mapping-synced"
	pointCheckpointManifestInstalled failpoint.Point = "checkpoint.manifest-installed"
	pointCheckpointRuntimePublished  failpoint.Point = "checkpoint.runtime-published"
)

func (s *Store) Metrics() Metrics {
	if s == nil || s.metrics == nil {
		return Metrics{}
	}
	snapshot := s.metrics.Snapshot()
	result := Metrics{
		CommitQueued: snapshot.CommitQueued, CommitGroups: snapshot.CommitGroups, GroupBatches: snapshot.GroupBatches,
		Committed: snapshot.Committed, Aborted: snapshot.Aborted, Conflicts: snapshot.Conflicts, CommitUnknown: snapshot.CommitUnknown,
		QueueWaitNanos: snapshot.QueueWaitNanos, ValidationNanos: snapshot.ValidationNanos,
		WriteSyncNanos: snapshot.WriteSyncNanos, PublishNanos: snapshot.PublishNanos,
		DeltaSoftLimitBytes: uint64(s.config.DeltaSoftLimitBytes), DeltaHardLimitBytes: uint64(s.config.DeltaHardLimitBytes),
	}
	if s.mapping != nil {
		result.DeltaChargedBytes, result.DeltaReservedBytes = s.mapping.DeltaBytes()
	}
	return result
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
	issuedAtCut := s.manifest.IssuedBatchIDHighExclusiveAtCut
	if s.catalog != nil {
		issuedAtCut = s.catalog.Snapshot().IssuedBatchIDHighExclusiveAtCut
	}
	if raw < issuedAtCut || raw < s.issuedBatchHigh {
		return BatchStatus{}, base.ErrStatusExpired
	}
	return BatchStatus{}, base.ErrNotFound
}

func (s *Store) Checkpoint(ctx context.Context) error {
	if s == nil {
		return base.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.checkpointMu.Lock()
	defer s.checkpointMu.Unlock()
	s.ops.RLock()
	defer s.ops.RUnlock()
	if err := s.checkAvailable(); err != nil {
		return err
	}
	var (
		barrier           appendlog.Barrier
		checkpoint        *radix.Checkpoint
		cutCommitSeq      base.CommitSeq
		nextCommitSeq     base.CommitSeq
		openBatchIDs      []base.BatchID
		issuedBatchHigh   uint64
		reservedIDHigh    uint64
		reservedBatchHigh uint64
	)
	err := s.coordinator.Barrier(ctx, func() error {
		var err error
		barrier, err = s.log.Barrier(ctx)
		if err != nil {
			return err
		}
		checkpoint, err = s.mapping.BeginCheckpoint()
		if err != nil {
			return err
		}
		cutCommitSeq = checkpoint.CoveredCommitSeq()
		nextCommitSeq = s.coordinator.NextCommitSeq()
		if cutCommitSeq == base.CommitSeq(math.MaxUint64) || cutCommitSeq+1 != nextCommitSeq {
			s.mapping.AbortCheckpoint()
			checkpoint = nil
			return fmt.Errorf("checkpoint commit boundary mismatch: %w", base.ErrCorrupt)
		}
		s.mu.Lock()
		openBatchIDs = make([]base.BatchID, 0, len(s.batches))
		for id := range s.batches {
			openBatchIDs = append(openBatchIDs, id)
		}
		issuedBatchHigh = s.issuedBatchHigh
		s.mu.Unlock()
		sort.Slice(openBatchIDs, func(i, j int) bool { return openBatchIDs[i] < openBatchIDs[j] })
		reservedIDHigh = s.idAllocator.DurableHigh()
		reservedBatchHigh = s.batchAllocator.DurableHigh()
		return nil
	})
	if err != nil {
		if s.log.Faulted() {
			s.setFault(err)
		}
		if errors.Is(err, base.ErrCorrupt) {
			s.setFault(err)
		}
		return err
	}

	root, err := s.mapping.BuildCheckpoint(checkpoint)
	if err != nil {
		s.mapping.AbortCheckpoint()
		s.setFault(err)
		return err
	}
	if err := failpoint.Hit(s.hook, pointCheckpointMappingSynced); err != nil {
		s.mapping.AbortCheckpoint()
		return err
	}
	if err := ctx.Err(); err != nil {
		s.mapping.AbortCheckpoint()
		return err
	}
	stats, err := s.buildSegmentStats(root, cutCommitSeq)
	if err != nil {
		s.mapping.AbortCheckpoint()
		s.setFault(err)
		return err
	}
	if barrier.End > math.MaxUint32 {
		s.mapping.AbortCheckpoint()
		return base.ErrInvalidAddress
	}
	replayStart, err := base.NewLogPos(barrier.SegmentID, uint32(barrier.End))
	if err != nil {
		s.mapping.AbortCheckpoint()
		return err
	}
	installed, err := s.catalog.Install(0, func(next *storeformat.Manifest) error {
		next.MappingRoot = root
		next.CoveredCommitSeq = cutCommitSeq
		next.StatsCoveredCommitSeq = cutCommitSeq
		next.CutFrameSeq = barrier.LastFrameSeq
		next.ReplayStart = replayStart
		next.ReservedIDHighExclusive = reservedIDHigh
		next.ReservedBatchIDHighExclusive = reservedBatchHigh
		next.IssuedBatchIDHighExclusiveAtCut = issuedBatchHigh
		next.OpenBatchIDsAtCut = append([]base.BatchID(nil), openBatchIDs...)
		next.NextFrameSeq = barrier.NextFrameSeq
		next.NextCommitSeq = nextCommitSeq
		next.SegmentStats = append([]storeformat.SegmentStatsEntry(nil), stats...)
		return nil
	})
	if err != nil {
		s.mapping.AbortCheckpoint()
		return err
	}
	if err := failpoint.Hit(s.hook, pointCheckpointManifestInstalled); err != nil {
		s.mapping.AbortCheckpoint()
		s.setFault(err)
		return err
	}
	if err := s.mapping.CompleteCheckpoint(checkpoint, root); err != nil {
		s.setFault(err)
		return err
	}
	if err := failpoint.Hit(s.hook, pointCheckpointRuntimePublished); err != nil {
		s.setFault(err)
		return err
	}
	s.mu.Lock()
	s.manifest = installed
	s.recoveryAbortedStart = installed.IssuedBatchIDHighExclusiveAtCut
	s.mu.Unlock()
	return nil
}

func (s *Store) requestCheckpoint() {
	s.mu.Lock()
	if s.closed || s.checkpointPending {
		s.mu.Unlock()
		return
	}
	s.checkpointPending = true
	s.mu.Unlock()
	go func() {
		err := s.Checkpoint(context.Background())
		s.mu.Lock()
		s.checkpointPending = false
		s.checkpointErr = err
		s.mu.Unlock()
	}()
}

func (s *Store) buildSegmentStats(root base.MapAddr, covered base.CommitSeq) ([]storeformat.SegmentStatsEntry, error) {
	bySegment := make(map[base.DataSegmentID]storeformat.SegmentStatsEntry)
	maxSegments := int(s.config.CheckpointMemoryBytes / 128)
	err := s.mapping.WalkRoot(root, covered, func(id base.ID, addr base.VAddr) error {
		header, err := s.segments.ReadFrameHeader(addr)
		if err != nil {
			return err
		}
		if header.Type != storeformat.FrameTypePutRecord || header.RecordID != id || header.BatchID == 0 {
			return fmt.Errorf("checkpoint mapping target: %w", base.ErrCorrupt)
		}
		physical := header.TotalSize
		entry := bySegment[addr.SegmentID()]
		if entry.SegmentID == 0 && len(bySegment) >= maxSegments {
			return fmt.Errorf("segment stats exceed checkpoint memory budget: %w", base.ErrInvalidConfig)
		}
		entry.SegmentID = addr.SegmentID()
		entry.ExactLiveBytes, err = base.AddUint64(entry.ExactLiveBytes, physical)
		if err != nil {
			return err
		}
		entry.ExactLiveRecords, err = base.AddUint64(entry.ExactLiveRecords, 1)
		if err != nil {
			return err
		}
		bySegment[addr.SegmentID()] = entry
		return nil
	})
	if err != nil {
		return nil, err
	}
	stats := make([]storeformat.SegmentStatsEntry, 0, len(bySegment))
	for _, entry := range bySegment {
		stats = append(stats, entry)
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].SegmentID < stats[j].SegmentID })
	return stats, nil
}

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

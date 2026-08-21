package ridstore

import (
	"errors"
	"fmt"
	"math"

	"github.com/akzj/ridstore/internal/allocator"
	"github.com/akzj/ridstore/internal/appendlog"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/commit"
	"github.com/akzj/ridstore/internal/filelock"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/recovery"
	"github.com/akzj/ridstore/internal/segment"
)

func buildStore(cfg Config, manifest storeformat.Manifest, lock *filelock.Lock) (*Store, error) {
	maxFramePayload, maxPartPayload, err := framePayloadLimits(manifest.HardLimits)
	if err != nil {
		return nil, err
	}
	active, err := segment.OpenActiveData(cfg.Dir, manifest.StoreUUID, manifest.ActiveDataSegmentID, manifest.HardLimits.SegmentSize, maxFramePayload)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Store, error) {
		return nil, errors.Join(err, active.Close())
	}
	recovered, err := recovery.RecoverPhase1(manifest, active)
	if err != nil {
		return fail(err)
	}
	log, err := appendlog.New(active, recovered.NextFrameSeq, maxFramePayload, maxPartPayload)
	if err != nil {
		return fail(err)
	}
	idAllocator, err := allocator.New(allocator.RecordID, manifest.HardLimits.IDReserveSize, recovered.ReservedIDHighExclusive, log)
	if err != nil {
		return fail(err)
	}
	batchAllocator, err := allocator.New(allocator.BatchID, manifest.HardLimits.BatchIDReserveSize, recovered.ReservedBatchIDHighExclusive, log)
	if err != nil {
		return fail(err)
	}
	coordinator, err := commit.New(recovered.NextCommitSeq, log, recovered.Mapping, activeRecordReader{active: active})
	if err != nil {
		return fail(err)
	}
	store := &Store{
		config: cfg, manifest: manifest, lock: lock, active: active, log: log,
		mapping: recovered.Mapping, coordinator: coordinator, idAllocator: idAllocator, batchAllocator: batchAllocator,
		batches: make(map[BatchID]*Batch), statuses: make(map[BatchID]BatchStatus), slotNotify: make(chan struct{}, 1),
		issuedBatchHigh:      recovered.ReservedBatchIDHighExclusive,
		recoveryAbortedStart: manifest.IssuedBatchIDHighExclusiveAtCut,
		recoveryAbortedEnd:   recovered.ReservedBatchIDHighExclusive,
	}
	for id, status := range recovered.Statuses {
		converted := BatchStatus{BatchID: id}
		if status.State == recovery.BatchCommitted {
			converted.State, converted.CommitSeq = BatchStateCommitted, status.CommitSeq
		} else {
			converted.State = BatchStateAborted
		}
		store.statuses[id] = converted
	}
	for _, id := range manifest.OpenBatchIDsAtCut {
		if _, exists := store.statuses[id]; !exists {
			store.statuses[id] = BatchStatus{BatchID: id, State: BatchStateAborted}
		}
	}
	return store, nil
}

func framePayloadLimits(h storeformat.HardLimits) (uint64, uint64, error) {
	descriptorBytes, err := base.MulUint64(h.MaxBatchMutations, storeformat.MutationEntrySize)
	if err != nil {
		return 0, 0, err
	}
	contentBytes := h.SegmentSize - storeformat.SegmentHeaderSize - storeformat.SegmentFooterSize
	if contentBytes <= storeformat.FrameHeaderSize {
		return 0, 0, fmt.Errorf("segment frame capacity: %w", base.ErrInvalidConfig)
	}
	frameCapacity := contentBytes - storeformat.FrameHeaderSize
	if frameCapacity > uint64(math.MaxUint32)-storeformat.FrameHeaderSize {
		frameCapacity = uint64(math.MaxUint32) - storeformat.FrameHeaderSize
	}
	maxPart := descriptorBytes
	if maxPart > frameCapacity {
		maxPart = frameCapacity - frameCapacity%storeformat.MutationEntrySize
	}
	maxFrame := h.MaxValueSize
	if maxPart > maxFrame {
		maxFrame = maxPart
	}
	if maxFrame < storeformat.DescriptorSealSize {
		maxFrame = storeformat.DescriptorSealSize
	}
	if maxFrame > frameCapacity || maxPart < storeformat.MutationEntrySize {
		return 0, 0, fmt.Errorf("frame payload limits: %w", base.ErrInvalidConfig)
	}
	return maxFrame, maxPart, nil
}

type activeRecordReader struct{ active *segment.ActiveData }

func (r activeRecordReader) ReadPutHeader(addr base.VAddr) (commit.RecordHeader, error) {
	frame, err := r.active.ReadFrame(addr)
	if err != nil {
		return commit.RecordHeader{}, err
	}
	if frame.Type != storeformat.FrameTypePutRecord {
		return commit.RecordHeader{}, fmt.Errorf("mapping target is not PutRecord: %w", base.ErrCorrupt)
	}
	physicalSize, err := base.Align8(storeformat.FrameHeaderSize + uint64(len(frame.Payload)))
	if err != nil {
		return commit.RecordHeader{}, err
	}
	return commit.RecordHeader{
		RecordID: frame.RecordID, OriginBatch: frame.BatchID,
		ValueBytes: uint64(len(frame.Payload)), PhysicalSize: physicalSize,
	}, nil
}

func (s *Store) setFault(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	if s.fault == nil {
		s.fault = err
	}
	s.mu.Unlock()
}

func (s *Store) checkAvailable() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return base.ErrClosed
	}
	if s.fault != nil {
		return errors.Join(base.ErrReadOnly, s.fault)
	}
	return nil
}

func (s *Store) signalSlotLocked() {
	select {
	case s.slotNotify <- struct{}{}:
	default:
	}
}

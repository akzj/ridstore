package ridstore

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/akzj/ridstore/internal/allocator"
	"github.com/akzj/ridstore/internal/appendlog"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/catalog"
	"github.com/akzj/ridstore/internal/commit"
	"github.com/akzj/ridstore/internal/diskspace"
	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/filelock"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/mapping/radix"
	"github.com/akzj/ridstore/internal/metrics"
	"github.com/akzj/ridstore/internal/recovery"
	"github.com/akzj/ridstore/internal/rotation"
	"github.com/akzj/ridstore/internal/segment"
)

func buildStore(cfg Config, manifest storeformat.Manifest, lock *filelock.Lock, hook failpoint.Hook) (*Store, error) {
	maxFramePayload, maxPartPayload, err := framePayloadLimits(manifest.HardLimits)
	if err != nil {
		return nil, err
	}
	manifest, err = rotation.RecoverWithHook(cfg.Dir, manifest, maxFramePayload, hook)
	if err != nil {
		return nil, err
	}
	manifest, err = radix.RecoverMappingRotationWithHook(cfg.Dir, manifest, hook)
	if err != nil {
		return nil, err
	}
	catalogManager, err := catalog.NewWithHook(cfg.Dir, manifest, hook)
	if err != nil {
		return nil, err
	}
	sealed := make([]*segment.SealedData, 0, len(manifest.SealedDataSegments))
	closeSealed := func() error {
		var result error
		for _, item := range sealed {
			result = errors.Join(result, item.Close())
		}
		return result
	}
	for _, summary := range manifest.SealedDataSegments {
		item, openErr := segment.OpenSealedData(cfg.Dir, manifest.StoreUUID, summary, manifest.HardLimits.SegmentSize, maxFramePayload)
		if openErr != nil {
			return nil, errors.Join(openErr, closeSealed())
		}
		sealed = append(sealed, item)
	}
	active, err := segment.OpenActiveDataWithHook(cfg.Dir, manifest.StoreUUID, manifest.ActiveDataSegmentID, manifest.HardLimits.SegmentSize, maxFramePayload, hook)
	if err != nil {
		return nil, errors.Join(err, closeSealed())
	}
	segments, err := segment.NewRegistry(active, sealed)
	if err != nil {
		return nil, errors.Join(err, active.Close(), closeSealed())
	}
	persistentMapping, err := radix.OpenWithHook(cfg.Dir, manifest, cfg.MappingCacheBytes, hook, catalogManager)
	if err != nil {
		return nil, errors.Join(err, segments.Close())
	}
	if err := persistentMapping.SetDeltaLimits(cfg.DeltaSoftLimitBytes, cfg.DeltaHardLimitBytes); err != nil {
		return nil, errors.Join(err, persistentMapping.Close(), segments.Close())
	}
	if err := persistentMapping.SetCheckpointMemory(cfg.CheckpointMemoryBytes); err != nil {
		return nil, errors.Join(err, persistentMapping.Close(), segments.Close())
	}
	fail := func(err error) (*Store, error) {
		return nil, errors.Join(err, persistentMapping.Close(), segments.Close())
	}
	recovered, err := recovery.RecoverInto(manifest, sealed, active, persistentMapping, uint64(cfg.StatusRetention))
	if err != nil {
		return fail(err)
	}
	rotator, err := rotation.NewManager(cfg.Dir, catalogManager, segments, maxFramePayload, hook)
	if err != nil {
		return fail(err)
	}
	physicalLog, err := appendlog.NewWithRotator(active, recovered.NextFrameSeq, maxFramePayload, maxPartPayload, hook, rotator)
	if err != nil {
		return fail(err)
	}
	log, err := appendlog.NewSequencer(physicalLog, cfg.MaxOpenBatches+cfg.MaxGroupBatches)
	if err != nil {
		return fail(err)
	}
	failWithLog := func(err error) (*Store, error) {
		return nil, errors.Join(err, log.Close(), persistentMapping.Close(), segments.Close())
	}
	var store *Store
	spaceGuard, err := diskspace.NewGuard(cfg.Dir, uint64(cfg.WriteStopFreeBytes), cfg.DiskSpaceCheckInterval, func(path string) (uint64, error) {
		if store == nil {
			return defaultAvailableBytes(path)
		}
		return store.availableBytes(path)
	})
	if err != nil {
		return failWithLog(err)
	}
	reserveWriter := spaceAwareReserveWriter{log: log, guard: spaceGuard}
	idAllocator, err := allocator.New(allocator.RecordID, manifest.HardLimits.IDReserveSize, recovered.ReservedIDHighExclusive, reserveWriter)
	if err != nil {
		return failWithLog(err)
	}
	batchAllocator, err := allocator.New(allocator.BatchID, manifest.HardLimits.BatchIDReserveSize, recovered.ReservedBatchIDHighExclusive, reserveWriter)
	if err != nil {
		return failWithLog(err)
	}
	runtimeMetrics := &metrics.Runtime{}
	var requestSoftCheckpoint func()
	coordinator, err := commit.NewGrouped(recovered.NextCommitSeq, log, persistentMapping, segmentRecordReader{segments: segments}, commit.Config{
		QueueDepth: cfg.MaxOpenBatches, MaxBatches: cfg.MaxGroupBatches, MaxBytes: uint64(cfg.MaxGroupBytes), MaxDelay: cfg.MaxGroupDelay,
		Metrics: runtimeMetrics, OnDeltaSoftLimit: func() {
			if requestSoftCheckpoint != nil {
				requestSoftCheckpoint()
			}
		},
	}, hook)
	if err != nil {
		return failWithLog(err)
	}
	store = &Store{
		config: cfg, manifest: manifest, lock: lock, segments: segments, rotation: rotator, catalog: catalogManager, metrics: runtimeMetrics, hook: hook, log: log,
		mapping: persistentMapping, coordinator: coordinator, idAllocator: idAllocator, batchAllocator: batchAllocator,
		batches: make(map[BatchID]*Batch), statuses: make(map[BatchID]statusEntry), slotNotify: make(chan struct{}),
		issuedBatchHigh:      recovered.ReservedBatchIDHighExclusive,
		recoveryAbortedStart: manifest.IssuedBatchIDHighExclusiveAtCut,
		recoveryAbortedEnd:   recovered.ReservedBatchIDHighExclusive,
		terminalStatusTotal:  recovered.TerminalStatusCount,
		availableBytes:       defaultAvailableBytes,
		spaceGuard:           spaceGuard,
	}
	requestSoftCheckpoint = store.requestCheckpoint
	for _, id := range manifest.OpenBatchIDsAtCut {
		store.addStatusLocked(BatchStatus{BatchID: id, State: BatchStateAborted})
	}
	for _, id := range recovered.StatusOrder {
		status := recovered.Statuses[id]
		converted := BatchStatus{BatchID: id}
		if status.State == recovery.BatchCommitted {
			converted.State, converted.CommitSeq = BatchStateCommitted, status.CommitSeq
		} else {
			converted.State = BatchStateAborted
		}
		store.addStatusLocked(converted)
	}
	if err := store.resumeDataGC(); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	if charged, _ := persistentMapping.DeltaBytes(); charged >= uint64(cfg.DeltaSoftLimitBytes) {
		store.requestCheckpoint()
	}
	if store.statusCheckpointNeededLocked() {
		store.requestCheckpoint()
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

type spaceAwareReserveWriter struct {
	log   *appendlog.Sequencer
	guard *diskspace.Guard
}

func (w spaceAwareReserveWriter) AppendReserve(ctx context.Context, typ storeformat.FrameType, payload storeformat.ReservePayload) error {
	if typ != storeformat.FrameTypeIDReserve && typ != storeformat.FrameTypeBatchIDReserve {
		return base.ErrInvalidConfig
	}
	encoded, err := storeformat.EncodeReservePayload(payload)
	if err != nil {
		return err
	}
	physical, err := base.Align8(storeformat.FrameHeaderSize + uint64(len(encoded)))
	if err != nil {
		return err
	}
	if err := w.guard.Reserve(ctx, physical); err != nil {
		return err
	}
	defer w.guard.Release()
	return w.log.AppendReserve(ctx, typ, payload)
}

type segmentRecordReader struct{ segments *segment.Registry }

func (r segmentRecordReader) ReadPutHeader(addr base.VAddr) (commit.RecordHeader, error) {
	header, err := r.segments.ReadFrameHeader(addr)
	if err != nil {
		return commit.RecordHeader{}, err
	}
	if header.Type != storeformat.FrameTypePutRecord {
		return commit.RecordHeader{}, fmt.Errorf("mapping target is not PutRecord: %w", base.ErrCorrupt)
	}
	return commit.RecordHeader{
		RecordID: header.RecordID, OriginBatch: header.BatchID,
		ValueBytes: header.PayloadSize, PhysicalSize: header.TotalSize,
	}, nil
}

func (r segmentRecordReader) ReadPutRecord(addr base.VAddr) (commit.PutRecord, error) {
	frame, err := r.segments.ReadFrame(addr)
	if err != nil {
		return commit.PutRecord{}, err
	}
	if frame.Type != storeformat.FrameTypePutRecord {
		return commit.PutRecord{}, fmt.Errorf("mapping target is not PutRecord: %w", base.ErrCorrupt)
	}
	physicalSize, err := base.Align8(storeformat.FrameHeaderSize + uint64(len(frame.Payload)))
	if err != nil {
		return commit.PutRecord{}, err
	}
	return commit.PutRecord{Header: commit.RecordHeader{
		RecordID: frame.RecordID, OriginBatch: frame.BatchID,
		ValueBytes: uint64(len(frame.Payload)), PhysicalSize: physicalSize,
	}, Value: frame.Payload}, nil
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

func (s *Store) admitNewWrite(ctx context.Context, bytes uint64) error {
	if s == nil || s.spaceGuard == nil {
		return base.ErrClosed
	}
	return s.spaceGuard.Admit(ctx, bytes)
}

func (s *Store) signalSlotLocked() {
	if s.slotWaiters == 0 {
		return
	}
	close(s.slotNotify)
	s.slotNotify = make(chan struct{})
}

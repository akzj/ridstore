package engine

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/coordinator"
	"github.com/akzj/ridstore/internal/filelock"
	"github.com/akzj/ridstore/internal/idalloc"
	"github.com/akzj/ridstore/internal/maintstate"
	"github.com/akzj/ridstore/internal/mapgcstate"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
	"github.com/akzj/ridstore/internal/transaction"
)

type Log interface {
	coordinator.Appender
	Read(context.Context, recordlog.VAddr) ([]byte, error)
	Close() error
}

type maintenanceLog interface {
	ScanSegment(context.Context, recordlog.SegmentID, func(recordlog.AppendResult, []byte) error) error
	RetireSegment(context.Context, recordlog.SegmentID, uint64) error
	NewCompactionWriter([]recordlog.SegmentID) (*recordlog.CompactionWriter, error)
	RegisterCompactionOutputs([]recordlog.SegmentSummary) error
	RemoveUnpublishedCompactionFiles([]recordlog.SegmentID) error
	FinalizeCompactionRetirement(context.Context, []recordlog.SegmentSummary, uint64) error
}

type Config struct {
	Batch           transaction.Limits
	Commit          coordinator.Config
	MaxOpenBatches  int
	StatusRetention uint64
}

type Record struct {
	Value []byte
	Addr  recordlog.VAddr
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
	BatchID   model.BatchID
	State     BatchState
	CommitSeq model.CommitSeq
}

type statusEntry struct {
	status BatchStatus
	serial uint64
}

type statusOrderEntry struct {
	id     model.BatchID
	serial uint64
}

type Store struct {
	mu              sync.Mutex
	batchSnapshotMu sync.Mutex
	mutationFence   sync.RWMutex
	checkpointMu    sync.Mutex
	maintenanceMu   sync.Mutex
	activeOps       uint64
	closing         bool

	log                         Log
	maintenance                 maintenanceLog
	maintenanceHook             maintstate.FaultHook
	mapStoreHook                mapstore.FaultHook
	mappingGCHook               mapgcstate.FaultHook
	root                        string
	mapping                     *mapping.Persistent
	mapStore                    *mapstore.Store
	catalog                     *storecatalog.Manager
	ids                         *idalloc.Allocator
	batches                     *idalloc.Allocator
	commits                     *coordinator.Coordinator
	limits                      transaction.Limits
	userAppender                transaction.Appender
	maxOpen                     int
	open                        map[model.BatchID]*Batch
	statuses                    map[model.BatchID]statusEntry
	statusOrder                 []statusOrderEntry
	statusOrderHead             int
	statusSerial                uint64
	statusRetention             uint64
	terminalTotal               uint64
	terminalBase                uint64
	openCount                   int
	recoveryAbortedStart        uint64
	recoveryAbortedEnd          uint64
	recoveryAbortedValid        bool
	notify                      chan struct{}
	closed                      bool
	fault                       error
	maxStats                    uint64
	mappingCacheBytes           uint64
	maxRelocationBytes          uint64
	maxRelocationMutations      uint64
	gcMinFreeBytes              uint64
	gcBytesPerSecond            atomic.Uint64
	gcNow                       func() time.Time
	gcWait                      func(context.Context, time.Duration) error
	dirLock                     *filelock.Lock
	identity                    [16]byte
	space                       *spaceGate
	metrics                     runtimeMetrics
	gcStability                 gcStability
	checkpointRequests          chan struct{}
	checkpointStop              chan struct{}
	checkpointDone              chan struct{}
	checkpointStopOnce          sync.Once
	checkpointRequestMu         sync.Mutex
	checkpointWaiters           []checkpointWaiter
	checkpointWorkerStopped     bool
	checkpointInterval          time.Duration
	checkpointPressurePending   atomic.Uint64
	checkpointPressureCompleted atomic.Uint64
}

// Identity returns the persistent identity of this store. It is stable across
// reopen and is used by the public package to bind opaque observation tokens
// to the store that issued them.
func (s *Store) Identity() [16]byte {
	if s == nil {
		return [16]byte{}
	}
	return s.identity
}

// SetGCBytesPerSecond changes the copy-rate budget sampled by the next Data
// compaction. An already running compaction retains the rate it sampled when
// its pacer was created. Zero is rejected rather than overloaded as pause or
// unlimited; callers pause maintenance by not starting another compaction.
func (s *Store) SetGCBytesPerSecond(rate uint64) error {
	if rate == 0 {
		return base.ErrInvalidConfig
	}
	if err := s.beginOperation(); err != nil {
		return err
	}
	defer s.endOperation()
	s.gcBytesPerSecond.Store(rate)
	return nil
}

func New(log Log, current *mapping.Persistent, ids, batches *idalloc.Allocator, config Config) (*Store, error) {
	if log == nil || current == nil || ids == nil || batches == nil || config.MaxOpenBatches <= 0 ||
		config.StatusRetention < uint64(config.MaxOpenBatches) || transaction.ValidateLimits(config.Batch) != nil {
		return nil, base.ErrInvalidConfig
	}
	commits, err := coordinator.New(current.CoveredCommitSeq()+1, log, current, config.Commit)
	if err != nil {
		return nil, err
	}
	return &Store{
		log: log, mapping: current, ids: ids, batches: batches, commits: commits,
		limits: config.Batch, userAppender: log, maxOpen: config.MaxOpenBatches, open: make(map[model.BatchID]*Batch),
		statuses: make(map[model.BatchID]statusEntry), statusRetention: config.StatusRetention, notify: make(chan struct{}),
		maxRelocationBytes:     config.Batch.MaxBatchBytes,
		maxRelocationMutations: (config.Commit.MaxGroupPayload - uint64(recordcodec.CommitGroupHeadSize+recordcodec.DescriptorHeadSize)) / uint64(recordcodec.MutationSize),
	}, nil
}

func (s *Store) Status(ctx context.Context, id model.BatchID) (BatchStatus, error) {
	if id == 0 {
		return BatchStatus{}, base.ErrInvalidID
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return BatchStatus{}, err
	}
	if err := s.beginOperation(); err != nil {
		return BatchStatus{}, err
	}
	defer s.endOperation()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return BatchStatus{}, base.ErrClosed
	}
	if batch, ok := s.open[id]; ok {
		state, seq := batch.inner.State()
		return BatchStatus{BatchID: id, State: publicBatchState(state), CommitSeq: seq}, nil
	}
	if status, ok := s.statuses[id]; ok {
		return status.status, nil
	}
	raw := uint64(id)
	if s.recoveryAbortedValid && raw >= s.recoveryAbortedStart && raw < s.recoveryAbortedEnd {
		return BatchStatus{BatchID: id, State: BatchStateAborted}, nil
	}
	if raw < s.batches.IssuedHigh() {
		return BatchStatus{}, base.ErrStatusExpired
	}
	return BatchStatus{}, base.ErrNotFound
}

func (s *Store) Begin(ctx context.Context) (*Batch, error) {
	if err := s.beginOperation(); err != nil {
		return nil, err
	}
	defer s.endOperation()
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.Lock()
		if s.closed || s.closing {
			s.mu.Unlock()
			return nil, base.ErrClosed
		}
		if fault := s.operationFaultLocked(); fault != nil {
			s.mu.Unlock()
			return nil, errors.Join(base.ErrReadOnly, fault)
		}
		capacityBlocked := !s.statusCapacityAvailableLocked()
		if s.openCount < s.maxOpen && !capacityBlocked {
			s.openCount++
			s.mu.Unlock()
			s.batchSnapshotMu.Lock()
			raw, err := s.batches.Allocate(ctx)
			if err != nil {
				s.batchSnapshotMu.Unlock()
				s.mu.Lock()
				s.openCount--
				s.signalLocked()
				s.mu.Unlock()
				return nil, err
			}
			inner, err := transaction.New(model.BatchID(raw), s.limits, s.userAppender, s.ids)
			if err != nil {
				s.batchSnapshotMu.Unlock()
				s.mu.Lock()
				s.openCount--
				s.signalLocked()
				s.mu.Unlock()
				return nil, err
			}
			batch := &Batch{store: s, inner: inner}
			s.mu.Lock()
			s.open[batch.ID()] = batch
			s.mu.Unlock()
			s.batchSnapshotMu.Unlock()
			return batch, nil
		}
		notify := s.notify
		s.mu.Unlock()
		if capacityBlocked {
			if err := s.Checkpoint(ctx); err != nil {
				return nil, err
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-notify:
		}
	}
}

func (s *Store) Get(ctx context.Context, id model.ID) (Record, error) {
	if id == 0 {
		return Record{}, base.ErrInvalidID
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.beginOperation(); err != nil {
		return Record{}, err
	}
	defer s.endOperation()
	for {
		if err := ctx.Err(); err != nil {
			return Record{}, err
		}
		s.mu.Lock()
		closed, fault := s.closed, s.operationFaultLocked()
		s.mu.Unlock()
		if closed {
			return Record{}, base.ErrClosed
		}
		if fault != nil {
			return Record{}, errors.Join(base.ErrReadOnly, fault)
		}
		addr, exists, err := s.mapping.Lookup(id)
		if err != nil {
			s.setFault(err)
			return Record{}, err
		}
		if !exists {
			return Record{}, base.ErrNotFound
		}
		payload, err := s.log.Read(ctx, addr)
		if err != nil {
			if ctx.Err() == nil {
				s.setFault(err)
			}
			return Record{}, err
		}
		current, stillExists, err := s.mapping.Lookup(id)
		if err != nil {
			s.setFault(err)
			return Record{}, err
		}
		if !stillExists || current != addr {
			continue
		}
		put, err := recordcodec.DecodePut(payload, s.limits.MaxValueSize)
		if err != nil || put.RecordID != id {
			corrupt := errors.Join(base.ErrCorrupt, err)
			s.setFault(corrupt)
			return Record{}, corrupt
		}
		return Record{Value: put.Value, Addr: addr}, nil
	}
}

func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed || s.closing {
		s.mu.Unlock()
		return base.ErrClosed
	}
	s.closing = true
	s.signalLocked()
	s.mu.Unlock()
	s.mu.Lock()
	for s.activeOps != 0 {
		notify := s.notify
		s.mu.Unlock()
		<-notify
		s.mu.Lock()
	}
	s.closed = true
	open := make([]*Batch, 0, len(s.open))
	for _, batch := range s.open {
		open = append(open, batch)
	}
	s.signalLocked()
	s.mu.Unlock()
	s.stopCheckpointWorker()
	var result error
	for _, batch := range open {
		state, _ := batch.inner.State()
		if state == transaction.StateOpen || state == transaction.StateFailed {
			result = errors.Join(result, batch.inner.Abort(context.Background(), 2))
			batch.finish()
		}
	}
	result = errors.Join(result, s.commits.Close())
	result = errors.Join(result, s.log.Close())
	if s.mapStore != nil {
		result = errors.Join(result, s.mapStore.Close())
	}
	if s.dirLock != nil {
		result = errors.Join(result, s.dirLock.Close())
	}
	return result
}

// Checkpoint installs one atomic Mapping, replay-cut, allocator, open-batch,
// and exact sealed-segment statistics generation.
func (s *Store) Checkpoint(ctx context.Context) error {
	if err := s.beginOperation(); err != nil {
		return err
	}
	defer s.endOperation()
	return s.checkpoint(ctx, false)
}

func (s *Store) checkpoint(ctx context.Context, gcAdmission bool) error {
	return s.requestCheckpoint(ctx, 0, true, gcAdmission)
}

// executeCheckpoint is owned by the checkpoint worker. Callers enqueue work
// through checkpoint so pressure, periodic, GC, and explicit requests share
// one serialized execution path.
func (s *Store) executeCheckpoint(ctx context.Context, gcAdmission bool) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	started := time.Now()
	s.metrics.checkpointsStarted.Add(1)
	defer func() {
		duration := uint64(time.Since(started))
		s.metrics.checkpointDurationNanos.Add(duration)
		updateAtomicMax(&s.metrics.checkpointMaxDurationNanos, duration)
		if err == nil {
			s.metrics.checkpointsCompleted.Add(1)
		} else {
			s.metrics.checkpointsFailed.Add(1)
		}
	}()
	s.checkpointMu.Lock()
	defer s.checkpointMu.Unlock()

	work, err := s.prepareCheckpoint(ctx)
	if err != nil {
		return err
	}
	var reservation *spaceReservation
	if gcAdmission {
		entries, err := work.frozen.EntryUpperBound()
		if err != nil {
			return errors.Join(err, s.mapping.AbortCheckpoint(work.frozen))
		}
		reservation, err = s.reserveGCCheckpoint(ctx, s.catalog.Snapshot(), entries)
		if err != nil {
			return errors.Join(err, s.mapping.AbortCheckpoint(work.frozen))
		}
		defer reservation.complete(false)
	}
	err = s.finishCheckpoint(ctx, work)
	if err == nil && reservation != nil {
		reservation.complete(true)
	}
	return err
}

type checkpointWork struct {
	frozen              *mapping.FrozenCheckpoint
	cut                 coordinator.CheckpointCut
	open                []model.BatchID
	statusCut           uint64
	reservedIDHigh      uint64
	reservedBatchIDHigh uint64
	issuedBatchIDHigh   uint64
}

// prepareCheckpoint captures a durable commit cut, recovery metadata, and the
// corresponding frozen Mapping generation while commit admission is fenced.
// Reads, record appends, and unrelated Batch construction remain concurrent.
func (s *Store) prepareCheckpoint(ctx context.Context) (checkpointWork, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return checkpointWork{}, base.ErrClosed
	}
	if fault := s.operationFaultLocked(); fault != nil {
		s.mu.Unlock()
		return checkpointWork{}, errors.Join(base.ErrReadOnly, fault)
	}
	if s.catalog == nil || s.mapStore == nil || s.maxStats == 0 {
		s.mu.Unlock()
		return checkpointWork{}, base.ErrInvalidConfig
	}
	s.mu.Unlock()
	// Drain potentially slow Begin/Abort metadata work before stopping Commit
	// admission. Once held, this mutex keeps the open-Batch/allocator snapshot
	// stable through the short durable cut and Mapping freeze.
	s.batchSnapshotMu.Lock()
	defer s.batchSnapshotMu.Unlock()
	fence, err := s.commits.AcquireCheckpointFence(ctx)
	if err != nil {
		return checkpointWork{}, err
	}
	defer fence.Release()
	cut := fence.Cut
	s.mu.Lock()
	open, statusCut, err := s.openBatchIDsAtCut()
	if err != nil {
		s.mu.Unlock()
		s.setFault(err)
		return checkpointWork{}, err
	}
	sort.Slice(open, func(i, j int) bool { return open[i] < open[j] })
	reservedIDHigh, reservedBatchIDHigh := s.ids.DurableHigh(), s.batches.DurableHigh()
	issuedBatchIDHigh := s.batches.IssuedHigh()
	s.mu.Unlock()
	frozen, err := s.mapping.Freeze(cut.CoveredCommitSeq)
	if err != nil {
		return checkpointWork{}, err
	}
	return checkpointWork{
		frozen: frozen, cut: cut, open: open, statusCut: statusCut,
		reservedIDHigh: reservedIDHigh, reservedBatchIDHigh: reservedBatchIDHigh,
		issuedBatchIDHigh: issuedBatchIDHigh,
	}, nil
}

func (s *Store) finishCheckpoint(ctx context.Context, work checkpointWork) error {
	frozen := work.frozen
	abort := func(cause error) error {
		return errors.Join(cause, s.mapping.AbortCheckpoint(frozen))
	}
	candidate, err := s.mapping.BuildCheckpoint(frozen)
	if err != nil {
		if errors.Is(err, mapstore.ErrPoisoned) || errors.Is(err, mapping.ErrCorrupt) {
			s.setFault(err)
		}
		return abort(err)
	}
	manifest := s.catalog.Snapshot()
	if manifest.MappingRoot != candidate.BaseRoot() || manifest.CoveredCommitSeq != candidate.BaseCoveredCommitSeq() {
		err := errors.Join(base.ErrCorrupt, errors.New("checkpoint Mapping base differs from Manifest"))
		s.setFault(err)
		return abort(err)
	}
	mappingEntries, err := candidate.EntryCount(manifest.MappingEntryCount)
	if err != nil {
		s.setFault(err)
		return abort(err)
	}
	stats, err := checkpointSegmentStats(candidate.LiveStats(), manifest, work.cut.ReplayStart, s.maxStats)
	if err != nil {
		if errors.Is(err, base.ErrCorrupt) {
			s.setFault(err)
		}
		return abort(err)
	}
	installed, err := s.catalog.InstallCheckpoint(manifest, storecatalog.Checkpoint{
		MappingRoot: candidate.Root(), MappingEntryCount: mappingEntries, CoveredCommitSeq: candidate.CoveredCommitSeq(), ReplayStart: work.cut.ReplayStart,
		ReservedIDHigh: work.reservedIDHigh, ReservedBatchIDHigh: work.reservedBatchIDHigh, IssuedBatchIDHighAtCut: work.issuedBatchIDHigh,
		OpenBatchIDsAtCut: work.open, StatsCoveredCommitSeq: candidate.CoveredCommitSeq(), SegmentStats: stats,
	})
	if err != nil {
		if !errors.Is(err, storecatalog.ErrConflict) {
			s.setFault(err)
		}
		return abort(err)
	}
	if err := s.mapping.InstallCheckpoint(candidate); err != nil {
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	now := time.Now()
	if s.gcNow != nil {
		now = s.gcNow()
	}
	s.gcStability.sample(installed, now)
	s.completeCheckpointPressure(frozen.PressureGeneration())
	s.mu.Lock()
	if work.statusCut > s.terminalBase {
		s.terminalBase = work.statusCut
	}
	s.recoveryAbortedValid = false
	s.signalLocked()
	s.mu.Unlock()
	return nil
}

func checkpointSegmentStats(live map[recordlog.SegmentID]mapping.SegmentLiveStats, manifest storecatalog.Manifest, replayStart recordlog.LogPos, maxEntries uint64) ([]storecatalog.SegmentStats, error) {
	if !replayStart.Valid() {
		return nil, base.ErrCorrupt
	}
	sealed := make(map[recordlog.SegmentID]recordlog.SegmentSummary, len(manifest.SealedDataSegments))
	for _, summary := range manifest.SealedDataSegments {
		sealed[summary.SegmentID] = summary
	}
	result := make([]storecatalog.SegmentStats, 0, len(live))
	for segmentID, stat := range live {
		if stat.LiveBytes == 0 || stat.LiveRecords == 0 {
			return nil, base.ErrCorrupt
		}
		if segmentID == manifest.ActiveDataSegmentID {
			if replayStart.SegmentID != segmentID || replayStart.Offset < recordlog.SegmentHeaderSize ||
				stat.LiveBytes > uint64(replayStart.Offset-recordlog.SegmentHeaderSize) ||
				stat.LiveRecords > stat.LiveBytes/uint64(recordlog.RecordHeaderSize) {
				return nil, base.ErrCorrupt
			}
		} else {
			summary, ok := sealed[segmentID]
			if !ok || summary.ValidEnd < recordlog.SegmentHeaderSize ||
				stat.LiveBytes > uint64(summary.ValidEnd-recordlog.SegmentHeaderSize) || stat.LiveRecords > summary.RecordCount {
				return nil, base.ErrCorrupt
			}
		}
		if uint64(len(result)) == maxEntries {
			return nil, base.ErrOverflow
		}
		result = append(result, storecatalog.SegmentStats{SegmentID: segmentID, LiveBytes: stat.LiveBytes, LiveRecords: stat.LiveRecords})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].SegmentID < result[j].SegmentID })
	return result, nil
}

func containsDataSegment(manifest storecatalog.Manifest, id recordlog.SegmentID) bool {
	if id == manifest.ActiveDataSegmentID {
		return true
	}
	index := sort.Search(len(manifest.SealedDataSegments), func(index int) bool {
		return manifest.SealedDataSegments[index].SegmentID >= id
	})
	return index < len(manifest.SealedDataSegments) && manifest.SealedDataSegments[index].SegmentID == id
}

// openBatchIDsAtCut runs after the Coordinator barrier. Every Commit admitted
// before that barrier has already reached a terminal transaction state even
// when its caller has not yet consumed the response and removed the Batch from
// Store.open. Only genuinely non-terminal, non-committing batches belong in
// the recovery snapshot.
func (s *Store) openBatchIDsAtCut() ([]model.BatchID, uint64, error) {
	open := make([]model.BatchID, 0, len(s.open))
	for id, batch := range s.open {
		state, _ := batch.inner.State()
		switch state {
		case transaction.StateOpen, transaction.StateFailed:
			open = append(open, id)
		case transaction.StateCommitted, transaction.StateAborted, transaction.StateCommitUnknown:
			// Client-side finish may lag behind the durable terminal state.
		case transaction.StateCommitting:
			return nil, 0, errors.Join(base.ErrCorrupt, errors.New("committing batch remained after checkpoint barrier"))
		default:
			return nil, 0, errors.Join(base.ErrCorrupt, errors.New("unknown batch state at checkpoint"))
		}
	}
	return open, s.terminalTotal, nil
}

func (s *Store) beginOperation() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.closing {
		return base.ErrClosed
	}
	s.activeOps++
	return nil
}

func (s *Store) endOperation() {
	s.mu.Lock()
	if s.activeOps == 0 {
		s.fault = errors.Join(base.ErrReadOnly, base.ErrCorrupt)
	} else {
		s.activeOps--
	}
	s.signalLocked()
	s.mu.Unlock()
}

func (s *Store) setFault(err error) {
	s.mu.Lock()
	if s.fault == nil {
		s.fault = err
	}
	s.mu.Unlock()
}

// operationFaultLocked returns the single fail-closed view used by public data
// operations. The caller holds s.mu; Coordinator never acquires s.mu, so
// consulting its terminal state here does not introduce a lock cycle.
func (s *Store) operationFaultLocked() error {
	if s.fault != nil {
		return s.fault
	}
	return s.commits.Fault()
}

func (s *Store) signalLocked() {
	close(s.notify)
	s.notify = make(chan struct{})
}

func (s *Store) releaseSlot() {
	s.mu.Lock()
	if s.openCount > 0 {
		s.openCount--
	}
	s.signalLocked()
	s.mu.Unlock()
}

type Batch struct {
	store *Store
	inner *transaction.Batch
	done  sync.Once
}

func (b *Batch) ID() model.BatchID { return b.inner.ID() }

func (b *Batch) Allocate(ctx context.Context) (model.ID, error) {
	return withBatch(b, func() (model.ID, error) { return b.inner.Allocate(ctx) })
}

func (b *Batch) Create(ctx context.Context, value []byte) (model.ID, error) {
	return withBatchMutation(b, func() (model.ID, error) { return b.inner.Create(ctx, value) })
}

func (b *Batch) Put(ctx context.Context, id model.ID, value []byte) error {
	_, err := withBatchMutation(b, func() (struct{}, error) { return struct{}{}, b.inner.Put(ctx, id, value) })
	return err
}

func (b *Batch) CompareAndPut(ctx context.Context, id model.ID, expected recordlog.VAddr, value []byte) error {
	_, err := withBatchMutation(b, func() (struct{}, error) { return struct{}{}, b.inner.CompareAndPut(ctx, id, expected, value) })
	return err
}

func (b *Batch) Delete(id model.ID) error {
	_, err := withBatch(b, func() (struct{}, error) { return struct{}{}, b.inner.Delete(id) })
	return err
}

func (b *Batch) CompareAndDelete(id model.ID, expected recordlog.VAddr) error {
	_, err := withBatch(b, func() (struct{}, error) { return struct{}{}, b.inner.CompareAndDelete(id, expected) })
	return err
}

func (b *Batch) ExpectAddress(id model.ID, addr recordlog.VAddr) error {
	_, err := withBatch(b, func() (struct{}, error) { return struct{}{}, b.inner.ExpectAddress(id, addr) })
	return err
}

func (b *Batch) ExpectAbsent(id model.ID) error {
	_, err := withBatch(b, func() (struct{}, error) { return struct{}{}, b.inner.ExpectAbsent(id) })
	return err
}

func (b *Batch) Commit(ctx context.Context) (coordinator.Result, error) {
	if b == nil || b.store == nil || b.inner == nil {
		return coordinator.Result{}, base.ErrBatchClosed
	}
	if err := b.store.beginOperation(); err != nil {
		return coordinator.Result{}, err
	}
	defer b.store.endOperation()
	for {
		receipt, err := b.store.submitCommit(ctx, b.inner)
		if errors.Is(err, mapping.ErrBudget) {
			// Reservation failed before Prepare or durable append. Wait for the
			// shared worker to checkpoint this Delta generation, then retry.
			if err := b.store.awaitCheckpointPressure(ctx, receipt.DeltaPressureGeneration(), false); err != nil {
				return coordinator.Result{}, err
			}
			continue
		}
		if err != nil {
			if terminal(b.inner) {
				b.finish()
			}
			return coordinator.Result{}, err
		}
		if receipt.DeltaPressure() {
			b.store.requestBackgroundCheckpoint(receipt.DeltaPressureGeneration())
		}
		result, err := receipt.Wait()
		if err == nil || terminal(b.inner) {
			b.finish()
		}
		return result, err
	}
}

func (s *Store) submitCommit(ctx context.Context, batch *transaction.Batch) (coordinator.Receipt, error) {
	s.mu.Lock()
	if s.closed || s.closing {
		s.mu.Unlock()
		return coordinator.Receipt{}, base.ErrClosed
	}
	if fault := s.operationFaultLocked(); fault != nil {
		s.mu.Unlock()
		return coordinator.Receipt{}, errors.Join(base.ErrReadOnly, fault)
	}
	s.mu.Unlock()
	return s.commits.Submit(ctx, batch)
}

func (b *Batch) Abort(ctx context.Context) error {
	if b == nil || b.store == nil || b.inner == nil {
		return base.ErrBatchClosed
	}
	if err := b.store.beginOperation(); err != nil {
		return err
	}
	defer b.store.endOperation()
	b.store.batchSnapshotMu.Lock()
	defer b.store.batchSnapshotMu.Unlock()
	b.store.mu.Lock()
	closed, fault := b.store.closed || b.store.closing, b.store.operationFaultLocked()
	b.store.mu.Unlock()
	if closed {
		return base.ErrClosed
	}
	if fault != nil {
		return errors.Join(base.ErrReadOnly, fault)
	}
	err := b.inner.Abort(ctx, 1)
	if terminal(b.inner) {
		b.finish()
	}
	return err
}

func withBatch[T any](b *Batch, run func() (T, error)) (T, error) {
	var zero T
	if b == nil || b.store == nil || b.inner == nil {
		return zero, base.ErrBatchClosed
	}
	if err := b.store.beginOperation(); err != nil {
		return zero, err
	}
	defer b.store.endOperation()
	b.store.mu.Lock()
	closed, fault := b.store.closed, b.store.operationFaultLocked()
	b.store.mu.Unlock()
	if closed {
		return zero, base.ErrClosed
	}
	if fault != nil {
		return zero, errors.Join(base.ErrReadOnly, fault)
	}
	return run()
}

func withBatchMutation[T any](b *Batch, run func() (T, error)) (T, error) {
	return withBatch(b, func() (T, error) {
		// Put-like operations append a Record before publishing the final
		// mutation into the Batch. Retirement briefly takes the write side to
		// drain this interval before snapshotting source references.
		b.store.mutationFence.RLock()
		defer b.store.mutationFence.RUnlock()
		return run()
	})
}

func terminal(batch *transaction.Batch) bool {
	state, _ := batch.State()
	return state == transaction.StateCommitted || state == transaction.StateAborted || state == transaction.StateCommitUnknown
}

func (b *Batch) finish() {
	b.done.Do(func() {
		state, seq := b.inner.State()
		b.store.mu.Lock()
		if terminalState := publicBatchState(state); terminalState == BatchStateCommitted || terminalState == BatchStateAborted || terminalState == BatchStateCommitUnknown {
			switch terminalState {
			case BatchStateCommitted:
				b.store.metrics.committed.Add(1)
			case BatchStateAborted:
				b.store.metrics.aborted.Add(1)
			case BatchStateCommitUnknown:
				b.store.metrics.unknown.Add(1)
			}
			if b.store.terminalTotal == math.MaxUint64 {
				b.store.fault = errors.Join(base.ErrReadOnly, base.ErrStatusCapacity)
			} else {
				b.store.terminalTotal++
				b.store.addStatusLocked(BatchStatus{BatchID: b.ID(), State: terminalState, CommitSeq: seq})
			}
		}
		delete(b.store.open, b.ID())
		if b.store.openCount > 0 {
			b.store.openCount--
		}
		b.store.signalLocked()
		b.store.mu.Unlock()
	})
}

func publicBatchState(state transaction.State) BatchState {
	switch state {
	case transaction.StateOpen, transaction.StateFailed:
		return BatchStateOpen
	case transaction.StateCommitting:
		return BatchStateCommitting
	case transaction.StateCommitted:
		return BatchStateCommitted
	case transaction.StateAborted:
		return BatchStateAborted
	case transaction.StateCommitUnknown:
		return BatchStateCommitUnknown
	default:
		return 0
	}
}

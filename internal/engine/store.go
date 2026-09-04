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

// storeState owns the user Batch registry, retained status, and fault state.
// Lifecycle admission and shutdown are deliberately owned by storeLifecycle.
type storeState struct {
	mu sync.Mutex

	fault  error
	notify chan struct{}

	limits          transaction.Limits
	maxOpen         int
	open            map[model.BatchID]*Batch
	statuses        map[model.BatchID]statusEntry
	statusOrder     []statusOrderEntry
	statusOrderHead int
	statusSerial    uint64
	statusRetention uint64
	terminalTotal   uint64
	terminalBase    uint64
	openCount       int
	batchEpoch      atomic.Uint64

	recoveryAbortedStart uint64
	recoveryAbortedEnd   uint64
	recoveryAbortedValid bool
}

// storeCore contains the long-lived storage components assembled by Open.
// Their internal synchronization remains owned by the respective component.
type storeCore struct {
	root          string
	log           Log
	compactionLog maintenanceLog
	mapping       *mapping.Persistent
	mapStore      *mapstore.Store
	catalog       *storecatalog.Manager
	publisher     *PublishCoordinator
	ids           *idalloc.Allocator
	batches       *idalloc.Allocator
	commits       *coordinator.Coordinator
	userAppender  transaction.Appender
	space         *spaceGate
	dirLock       *filelock.Lock
	identity      [16]byte
}

// maintenanceRuntime owns replaceable maintenance policy, fault injection,
// scheduling, and stability state. It is separate from the durable components
// in storeCore so background policy cannot be mistaken for persisted state.
type maintenanceRuntime struct {
	stateHook     maintstate.FaultHook
	mapStoreHook  mapstore.FaultHook
	mappingGCHook mapgcstate.FaultHook

	maxStats               uint64
	mappingCacheBytes      uint64
	maxRelocationBytes     uint64
	maxRelocationMutations uint64
	gcMinFreeBytes         uint64
	gcBytesPerSecond       atomic.Uint64
	gcNow                  func() time.Time
	gcWait                 func(context.Context, time.Duration) error
	gcStability            gcStability
	scheduler              *MaintenanceScheduler
	config                 MaintenanceConfig
	lastMappingGCUnixNano  atomic.Int64
	mappingUsage           atomic.Pointer[mappingUsage]
}

type mappingUsage struct {
	generation, physicalBytes, reachableBytes uint64
	root                                      model.MapAddr
}

type Store struct {
	lifecycle   *storeLifecycle
	state       storeState
	checkpoints checkpointRuntime
	core        storeCore
	maintenance maintenanceRuntime
	metrics     runtimeMetrics
}

// Identity returns the persistent identity of this store. It is stable across
// reopen and is used by the public package to bind opaque observation tokens
// to the store that issued them.
func (s *Store) Identity() [16]byte {
	if s == nil {
		return [16]byte{}
	}
	return s.core.identity
}

// SetGCBytesPerSecond changes the copy-rate budget sampled by the next Data
// compaction. An already running compaction retains the rate it sampled when
// its pacer was created. Zero is rejected rather than overloaded as pause or
// unlimited; callers pause maintenance by not starting another compaction.
func (s *Store) SetGCBytesPerSecond(rate uint64) error {
	if rate == 0 {
		return base.ErrInvalidConfig
	}
	_, end, err := s.beginOperation(context.Background())
	if err != nil {
		return err
	}
	defer end()
	s.maintenance.gcBytesPerSecond.Store(rate)
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
	store := &Store{
		lifecycle: newStoreLifecycle(),
		state: storeState{
			limits: config.Batch, maxOpen: config.MaxOpenBatches, open: make(map[model.BatchID]*Batch),
			statuses: make(map[model.BatchID]statusEntry), statusRetention: config.StatusRetention, notify: make(chan struct{}),
		},
		core: storeCore{
			log: log, mapping: current, ids: ids, batches: batches, commits: commits, userAppender: log,
		},
		maintenance: maintenanceRuntime{
			maxRelocationBytes:     config.Batch.MaxBatchBytes,
			maxRelocationMutations: (config.Commit.MaxGroupPayload - uint64(recordcodec.CommitGroupHeadSize+recordcodec.DescriptorHeadSize)) / uint64(recordcodec.MutationSize),
		},
	}
	store.maintenance.scheduler = newMaintenanceScheduler(store.lifecycle.Context(), store)
	return store, nil
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
	_, end, err := s.beginOperation(ctx)
	if err != nil {
		return BatchStatus{}, err
	}
	defer end()
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	if batch, ok := s.state.open[id]; ok {
		state, seq := batch.inner.State()
		return BatchStatus{BatchID: id, State: publicBatchState(state), CommitSeq: seq}, nil
	}
	if status, ok := s.state.statuses[id]; ok {
		return status.status, nil
	}
	raw := uint64(id)
	if s.state.recoveryAbortedValid && raw >= s.state.recoveryAbortedStart && raw < s.state.recoveryAbortedEnd {
		return BatchStatus{BatchID: id, State: BatchStateAborted}, nil
	}
	if raw < s.core.batches.IssuedHigh() {
		return BatchStatus{}, base.ErrStatusExpired
	}
	return BatchStatus{}, base.ErrNotFound
}

func (s *Store) Begin(ctx context.Context) (*Batch, error) {
	ctx, end, err := s.beginOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer end()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.state.mu.Lock()
		if fault := s.operationFaultLocked(); fault != nil {
			s.state.mu.Unlock()
			return nil, errors.Join(base.ErrReadOnly, fault)
		}
		capacityBlocked := !s.statusCapacityAvailableLocked()
		if s.state.openCount < s.state.maxOpen && !capacityBlocked {
			s.state.openCount++
			s.state.mu.Unlock()
			raw, err := s.core.batches.Allocate(ctx)
			if err != nil {
				s.state.mu.Lock()
				s.state.openCount--
				s.signalLocked()
				s.state.mu.Unlock()
				return nil, err
			}
			inner, err := transaction.New(model.BatchID(raw), s.state.limits, s.core.userAppender, s.core.ids)
			if err != nil {
				s.state.mu.Lock()
				s.state.openCount--
				s.signalLocked()
				s.state.mu.Unlock()
				return nil, err
			}
			batch := &Batch{store: s, inner: inner}
			s.state.mu.Lock()
			s.state.open[batch.ID()] = batch
			s.state.batchEpoch.Add(1)
			s.state.mu.Unlock()
			return batch, nil
		}
		notify := s.state.notify
		s.state.mu.Unlock()
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
	ctx, end, err := s.beginOperation(ctx)
	if err != nil {
		return Record{}, err
	}
	defer end()
	for {
		if err := ctx.Err(); err != nil {
			return Record{}, err
		}
		s.state.mu.Lock()
		fault := s.operationFaultLocked()
		s.state.mu.Unlock()
		if fault != nil {
			return Record{}, errors.Join(base.ErrReadOnly, fault)
		}
		addr, exists, err := s.core.mapping.Lookup(id)
		if err != nil {
			s.setFault(err)
			return Record{}, err
		}
		if !exists {
			return Record{}, base.ErrNotFound
		}
		payload, err := s.core.log.Read(ctx, addr)
		if err != nil {
			if ctx.Err() == nil {
				s.setFault(err)
			}
			return Record{}, err
		}
		current, stillExists, err := s.core.mapping.Lookup(id)
		if err != nil {
			s.setFault(err)
			return Record{}, err
		}
		if !stillExists || current != addr {
			continue
		}
		put, err := recordcodec.DecodePut(payload, s.state.limits.MaxValueSize)
		if err != nil || put.RecordID != id {
			corrupt := errors.Join(base.ErrCorrupt, err)
			s.setFault(corrupt)
			return Record{}, corrupt
		}
		return Record{Value: put.Value, Addr: addr}, nil
	}
}

func (s *Store) Close() error { return s.CloseContext(context.Background()) }

// CloseContext starts shutdown once and waits for the Store-owned goroutines
// and resources to finish. A caller timeout stops only the wait; shutdown
// continues and can be observed through Done or a later CloseContext call.
func (s *Store) CloseContext(ctx context.Context) error {
	if s == nil || s.lifecycle == nil {
		return base.ErrClosed
	}
	s.lifecycle.startClose(s.shutdown)
	return s.lifecycle.wait(ctx)
}

// Done closes after all Store-owned goroutines have exited and resources have
// been released. Callers may select on it to impose their own shutdown bound.
func (s *Store) Done() <-chan struct{} {
	if s == nil || s.lifecycle == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return s.lifecycle.Done()
}

func (s *Store) shutdown() error {
	// Cancel and drain background workers first. Foreground operations waiting
	// on them observe lifecycle cancellation and can release their admission.
	s.stopCheckpointWorker()
	<-s.lifecycle.Drained()
	s.state.mu.Lock()
	open := make([]*Batch, 0, len(s.state.open))
	for _, batch := range s.state.open {
		open = append(open, batch)
	}
	s.signalLocked()
	s.state.mu.Unlock()
	var result error
	for _, batch := range open {
		state, _ := batch.inner.State()
		if state == transaction.StateOpen || state == transaction.StateFailed {
			result = errors.Join(result, batch.inner.Abort(context.Background(), 2))
			batch.finish()
		}
	}
	result = errors.Join(result, s.core.commits.Close())
	result = errors.Join(result, s.core.log.Close())
	if s.core.mapStore != nil {
		result = errors.Join(result, s.core.mapStore.Close())
	}
	if s.core.dirLock != nil {
		result = errors.Join(result, s.core.dirLock.Close())
	}
	return result
}

// Checkpoint installs one atomic Mapping, replay-cut, allocator, open-batch,
// and exact sealed-segment statistics generation. Optimistic publication
// conflicts release maintenance resources and retry with a capped delay until
// publication succeeds or ctx is canceled.
func (s *Store) Checkpoint(ctx context.Context) error {
	ctx, end, err := s.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer end()
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
		} else if !checkpointConflict(err) {
			s.metrics.checkpointsFailed.Add(1)
		}
	}()
	// Serialize only the short capture/freeze boundary. The expensive COW
	// build, stats computation, and durable publication below run without the
	// Store checkpoint mutex. A concurrent maintenance publication may make a
	// candidate stale; abort that frozen plan and let the scheduler retry with
	// backoff instead of monopolizing mappingWriter with repeated COW builds.
	captureWaitStarted := time.Now()
	s.checkpoints.captureMu.Lock()
	captureWait := uint64(time.Since(captureWaitStarted))
	s.metrics.checkpointCaptureWaitNanos.Add(captureWait)
	updateAtomicMax(&s.metrics.checkpointMaxCaptureWaitNanos, captureWait)
	captureStarted := time.Now()
	var work checkpointWork
	for captureAttempts := 0; ; captureAttempts++ {
		work, err = s.prepareCheckpoint(ctx)
		if errors.Is(err, base.ErrConflict) || errors.Is(err, mapping.ErrStalePlan) {
			s.metrics.checkpointCaptureConflicts.Add(1)
		}
		if !errors.Is(err, base.ErrConflict) || captureAttempts >= 8 {
			break
		}
		if err := ctx.Err(); err != nil {
			captureDuration := uint64(time.Since(captureStarted))
			s.metrics.checkpointCaptureNanos.Add(captureDuration)
			updateAtomicMax(&s.metrics.checkpointMaxCaptureNanos, captureDuration)
			s.checkpoints.captureMu.Unlock()
			return err
		}
	}
	captureDuration := uint64(time.Since(captureStarted))
	s.metrics.checkpointCaptureNanos.Add(captureDuration)
	updateAtomicMax(&s.metrics.checkpointMaxCaptureNanos, captureDuration)
	s.checkpoints.captureMu.Unlock()
	if err != nil {
		if errors.Is(err, base.ErrConflict) || errors.Is(err, mapping.ErrStalePlan) {
			return retryCheckpointConflict(err)
		}
		return err
	}
	var reservation *spaceReservation
	if gcAdmission {
		entries, entryErr := work.frozen.EntryUpperBound()
		if entryErr != nil {
			return errors.Join(entryErr, s.core.mapping.AbortCheckpoint(work.frozen))
		}
		reservation, err = s.reserveGCCheckpoint(ctx, s.catalogSnapshot(), entries)
		if err != nil {
			return errors.Join(err, s.core.mapping.AbortCheckpoint(work.frozen))
		}
	}
	err = s.finishCheckpoint(ctx, work)
	if reservation != nil {
		reservation.complete(err == nil)
	}
	if errors.Is(err, storecatalog.ErrConflict) {
		s.metrics.checkpointPublishConflicts.Add(1)
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
	s.state.mu.Lock()
	if fault := s.operationFaultLocked(); fault != nil {
		s.state.mu.Unlock()
		return checkpointWork{}, errors.Join(base.ErrReadOnly, fault)
	}
	if s.core.catalog == nil || s.core.mapStore == nil || s.maintenance.maxStats == 0 {
		s.state.mu.Unlock()
		return checkpointWork{}, base.ErrInvalidConfig
	}
	s.state.mu.Unlock()
	// Drain potentially slow Begin/Abort metadata work before stopping Commit
	// admission. Once held, this mutex keeps the open-Batch/allocator snapshot
	// stable through the short durable cut and Mapping freeze.
	fence, err := s.core.commits.AcquireCheckpointFence(ctx)
	if err != nil {
		return checkpointWork{}, err
	}
	defer fence.Release()
	cut := fence.Cut
	epoch := s.state.batchEpoch.Load()
	s.state.mu.Lock()
	open, statusCut, err := s.openBatchIDsAtCut()
	if err != nil {
		s.state.mu.Unlock()
		s.setFault(err)
		return checkpointWork{}, err
	}
	sort.Slice(open, func(i, j int) bool { return open[i] < open[j] })
	reservedIDHigh, reservedBatchIDHigh := s.core.ids.DurableHigh(), s.core.batches.DurableHigh()
	issuedBatchIDHigh := s.core.batches.IssuedHigh()
	s.state.mu.Unlock()
	if epoch != s.state.batchEpoch.Load() {
		return checkpointWork{}, base.ErrConflict
	}
	frozen, err := s.core.mapping.Freeze(cut.CoveredCommitSeq)
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
	buildStarted := time.Now()
	buildRecorded := false
	defer func() {
		if !buildRecorded {
			duration := uint64(time.Since(buildStarted))
			s.metrics.checkpointBuildNanos.Add(duration)
			updateAtomicMax(&s.metrics.checkpointMaxBuildNanos, duration)
		}
	}()
	abort := func(cause error) error {
		abortErr := s.core.mapping.AbortCheckpoint(frozen)
		if abortErr == nil && (errors.Is(cause, base.ErrConflict) || errors.Is(cause, mapping.ErrStalePlan) || errors.Is(cause, storecatalog.ErrConflict)) {
			return retryCheckpointConflict(cause)
		}
		return errors.Join(cause, abortErr)
	}
	candidate, err := s.core.mapping.BuildCheckpoint(frozen)
	if err != nil {
		if errors.Is(err, mapstore.ErrPoisoned) || errors.Is(err, mapping.ErrCorrupt) {
			s.setFault(err)
		}
		return abort(err)
	}
	manifest := s.catalogSnapshot()
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
	var nextUsage *mappingUsage
	if usage := s.maintenance.mappingUsage.Load(); usage != nil && usage.root == manifest.MappingRoot {
		reachable, usageErr := candidate.ReachableBytes(usage.reachableBytes)
		if usageErr != nil {
			s.setFault(usageErr)
			return abort(usageErr)
		}
		physical, usageErr := s.core.mapStore.PhysicalBytes()
		if usageErr != nil {
			return abort(usageErr)
		}
		nextUsage = &mappingUsage{root: candidate.Root(), physicalBytes: physical, reachableBytes: reachable}
	}
	stats, err := checkpointSegmentStats(candidate.LiveStats(), manifest, work.cut.ReplayStart, s.maintenance.maxStats)
	if err != nil {
		if errors.Is(err, base.ErrCorrupt) {
			s.setFault(err)
		}
		return abort(err)
	}
	buildDuration := uint64(time.Since(buildStarted))
	s.metrics.checkpointBuildNanos.Add(buildDuration)
	updateAtomicMax(&s.metrics.checkpointMaxBuildNanos, buildDuration)
	buildRecorded = true
	publishStarted := time.Now()
	defer func() {
		duration := uint64(time.Since(publishStarted))
		s.metrics.checkpointPublishNanos.Add(duration)
		updateAtomicMax(&s.metrics.checkpointMaxPublishNanos, duration)
	}()
	installed, err := s.core.publisher.InstallCheckpoint(manifest, storecatalog.Checkpoint{
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
	if err := s.core.mapping.InstallCheckpoint(candidate); err != nil {
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	if nextUsage != nil {
		nextUsage.generation = installed.Generation
		s.maintenance.mappingUsage.Store(nextUsage)
		s.metrics.mappingSurveyGeneration.Store(installed.Generation)
		s.metrics.mappingSurveyPhysicalBytes.Store(nextUsage.physicalBytes)
		s.metrics.mappingSurveyReachableBytes.Store(nextUsage.reachableBytes)
	}
	now := time.Now()
	if s.maintenance.gcNow != nil {
		now = s.maintenance.gcNow()
	}
	s.maintenance.gcStability.sample(installed, now)
	s.completeCheckpointPressure(frozen.PressureGeneration())
	s.state.mu.Lock()
	if work.statusCut > s.state.terminalBase {
		s.state.terminalBase = work.statusCut
	}
	s.state.recoveryAbortedValid = false
	s.signalLocked()
	s.state.mu.Unlock()
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
	open := make([]model.BatchID, 0, len(s.state.open))
	for id, batch := range s.state.open {
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
	return open, s.state.terminalTotal, nil
}

func (s *Store) beginOperation(ctx context.Context) (context.Context, func(), error) {
	if s == nil || s.lifecycle == nil {
		return nil, nil, base.ErrClosed
	}
	return s.lifecycle.begin(ctx)
}

func (s *Store) setFault(err error) {
	s.state.mu.Lock()
	if s.state.fault == nil {
		s.state.fault = err
	}
	s.state.mu.Unlock()
}

// operationFaultLocked returns the single fail-closed view used by public data
// operations. The caller holds state.mu; Coordinator never acquires state.mu,
// so consulting its terminal state here does not introduce a lock cycle.
func (s *Store) operationFaultLocked() error {
	if s.state.fault != nil {
		return s.state.fault
	}
	return s.core.commits.Fault()
}

func (s *Store) signalLocked() {
	close(s.state.notify)
	s.state.notify = make(chan struct{})
}

func (s *Store) releaseSlot() {
	s.state.mu.Lock()
	if s.state.openCount > 0 {
		s.state.openCount--
	}
	s.signalLocked()
	s.state.mu.Unlock()
}

type Batch struct {
	store *Store
	inner *transaction.Batch
	done  sync.Once
}

func (b *Batch) ID() model.BatchID { return b.inner.ID() }

func (b *Batch) Allocate(ctx context.Context) (model.ID, error) {
	return withBatchContext(b, ctx, func(ctx context.Context) (model.ID, error) { return b.inner.Allocate(ctx) })
}

func (b *Batch) Create(ctx context.Context, value []byte) (model.ID, error) {
	return withBatchContext(b, ctx, func(ctx context.Context) (model.ID, error) { return b.inner.Create(ctx, value) })
}

func (b *Batch) Put(ctx context.Context, id model.ID, value []byte) error {
	_, err := withBatchContext(b, ctx, func(ctx context.Context) (struct{}, error) { return struct{}{}, b.inner.Put(ctx, id, value) })
	return err
}

func (b *Batch) CompareAndPut(ctx context.Context, id model.ID, expected recordlog.VAddr, value []byte) error {
	_, err := withBatchContext(b, ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, b.inner.CompareAndPut(ctx, id, expected, value)
	})
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
	ctx, end, err := b.store.beginOperation(ctx)
	if err != nil {
		return coordinator.Result{}, err
	}
	defer end()
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
	s.state.mu.Lock()
	if fault := s.operationFaultLocked(); fault != nil {
		s.state.mu.Unlock()
		return coordinator.Receipt{}, errors.Join(base.ErrReadOnly, fault)
	}
	s.state.mu.Unlock()
	return s.core.commits.Submit(ctx, batch)
}

func (b *Batch) Abort(ctx context.Context) error {
	if b == nil || b.store == nil || b.inner == nil {
		return base.ErrBatchClosed
	}
	ctx, end, err := b.store.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer end()
	b.store.state.mu.Lock()
	fault := b.store.operationFaultLocked()
	b.store.state.mu.Unlock()
	if fault != nil {
		return errors.Join(base.ErrReadOnly, fault)
	}
	err = b.inner.Abort(ctx, 1)
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
	_, end, err := b.store.beginOperation(context.Background())
	if err != nil {
		return zero, err
	}
	defer end()
	b.store.state.mu.Lock()
	fault := b.store.operationFaultLocked()
	b.store.state.mu.Unlock()
	if fault != nil {
		return zero, errors.Join(base.ErrReadOnly, fault)
	}
	return run()
}

func withBatchContext[T any](b *Batch, ctx context.Context, run func(context.Context) (T, error)) (T, error) {
	var zero T
	if b == nil || b.store == nil || b.inner == nil {
		return zero, base.ErrBatchClosed
	}
	ctx, end, err := b.store.beginOperation(ctx)
	if err != nil {
		return zero, err
	}
	defer end()
	b.store.state.mu.Lock()
	fault := b.store.operationFaultLocked()
	b.store.state.mu.Unlock()
	if fault != nil {
		return zero, errors.Join(base.ErrReadOnly, fault)
	}
	return run(ctx)
}

func terminal(batch *transaction.Batch) bool {
	state, _ := batch.State()
	return state == transaction.StateCommitted || state == transaction.StateAborted || state == transaction.StateCommitUnknown
}

func (b *Batch) finish() {
	b.done.Do(func() {
		state, seq := b.inner.State()
		b.store.state.mu.Lock()
		if terminalState := publicBatchState(state); terminalState == BatchStateCommitted || terminalState == BatchStateAborted || terminalState == BatchStateCommitUnknown {
			switch terminalState {
			case BatchStateCommitted:
				b.store.metrics.committed.Add(1)
			case BatchStateAborted:
				b.store.metrics.aborted.Add(1)
			case BatchStateCommitUnknown:
				b.store.metrics.unknown.Add(1)
			}
			if b.store.state.terminalTotal == math.MaxUint64 {
				b.store.state.fault = errors.Join(base.ErrReadOnly, base.ErrStatusCapacity)
			} else {
				b.store.state.terminalTotal++
				b.store.addStatusLocked(BatchStatus{BatchID: b.ID(), State: terminalState, CommitSeq: seq})
			}
		}
		delete(b.store.state.open, b.ID())
		b.store.state.batchEpoch.Add(1)
		if b.store.state.openCount > 0 {
			b.store.state.openCount--
		}
		b.store.signalLocked()
		b.store.state.mu.Unlock()
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

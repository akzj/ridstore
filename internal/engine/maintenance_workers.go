package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/recordlog"
)

// NewMaintenanceWorker is the only production factory for scheduler-owned
// workers. Store APIs submit data; they never construct executable closures.
func (s *Store) NewMaintenanceWorker(request maintenanceRequest) (maintenanceWorker, error) {
	switch request.kind {
	case maintenanceCheckpointRequest:
		return &checkpointMaintenanceWorker{store: s, request: request}, nil
	case maintenanceSegmentRelocateRequest, maintenanceSegmentPrepareRequest,
		maintenanceSegmentCompactRequest, maintenanceSegmentNextRequest:
		return &segmentMaintenanceWorker{store: s, request: request}, nil
	case maintenanceMappingGCRequest:
		return &mappingGCMaintenanceWorker{store: s, request: request}, nil
	case maintenanceMappingSurveyRequest:
		return &mappingSurveyMaintenanceWorker{store: s}, nil
	default:
		return nil, base.ErrInvalidConfig
	}
}

type checkpointMaintenanceWorker struct {
	store            *Store
	request          maintenanceRequest
	conflictAttempts uint8
}

type checkpointRetryableConflict struct{ cause error }

func (e *checkpointRetryableConflict) Error() string { return e.cause.Error() }
func (e *checkpointRetryableConflict) Unwrap() error { return e.cause }

func retryCheckpointConflict(cause error) error {
	if cause == nil {
		return nil
	}
	return &checkpointRetryableConflict{cause: cause}
}

func (*checkpointMaintenanceWorker) Resources(maintenancePhase) maintenanceResource {
	return maintenanceMappingWriter
}

func (w *checkpointMaintenanceWorker) Run(ctx context.Context, _ maintenancePhase, _ maintenanceResult) maintenanceTransition {
	err := w.store.runCheckpointCycle(ctx, w.request.periodic, w.request.force, w.request.gcAdmission, w.request.generation)
	if checkpointConflict(err) {
		delay := checkpointConflictRetryDelay(w.conflictAttempts)
		if w.conflictAttempts < 7 {
			w.conflictAttempts++
		}
		return maintenanceTransition{next: maintenancePhaseStart, retryAfter: delay}
	}
	return maintenanceTransition{done: true, err: err}
}

func checkpointConflict(err error) bool {
	var conflict *checkpointRetryableConflict
	return errors.As(err, &conflict)
}

func checkpointConflictRetryDelay(attempt uint8) time.Duration {
	return time.Millisecond << min(attempt, uint8(6))
}

type segmentMaintenanceWorker struct {
	store         *Store
	request       maintenanceRequest
	result        maintenanceResult
	inputs        []recordlog.SegmentSummary
	copyWork      *segmentCompactionWork
	started       time.Time
	proofAttempts int
}

func (w *segmentMaintenanceWorker) Resources(phase maintenancePhase) maintenanceResource {
	switch phase {
	case maintenancePhaseStart, maintenancePhaseCopy, maintenancePhasePublish:
		return maintenanceHeavyIO | maintenanceRecoveryProtocol
	case maintenancePhaseProve:
		return maintenanceRecoveryProtocol
	case maintenancePhaseRetire:
		return maintenanceHeavyIO | maintenanceRecoveryProtocol
	default:
		return maintenanceRecoveryProtocol
	}
}

func (w *segmentMaintenanceWorker) Run(ctx context.Context, phase maintenancePhase, _ maintenanceResult) maintenanceTransition {
	var transition maintenanceTransition
	switch w.request.kind {
	case maintenanceSegmentRelocateRequest:
		result, err := w.store.relocateSegment(ctx, w.request.source, w.store.maintenance.gcBytesPerSecond.Load())
		mergeRelocationResult(&w.result.relocation, result)
		if generation, needed := checkpointDependency(err); needed {
			transition = maintenanceTransition{next: maintenancePhaseStart, retain: maintenanceRecoveryProtocol,
				dependency: &maintenanceRequest{kind: maintenanceCheckpointRequest, generation: generation, force: true}}
			break
		}
		transition = maintenanceTransition{done: true, result: w.result, err: err}
	case maintenanceSegmentPrepareRequest:
		transition = w.runPrepare(ctx, phase)
	case maintenanceSegmentCompactRequest:
		transition = w.runCompact(ctx, phase)
	case maintenanceSegmentNextRequest:
		transition = w.runNext(ctx, phase)
	default:
		transition = maintenanceTransition{done: true, err: base.ErrInvalidConfig}
	}
	if w.request.automatic && transition.err != nil && !expectedAutomaticMaintenanceError(transition.err) {
		w.store.metrics.maintenanceAutomaticFailed.Add(1)
	}
	return transition
}

func expectedAutomaticMaintenanceError(err error) bool {
	return errors.Is(err, base.ErrClosed) || errors.Is(err, context.Canceled) || errors.Is(err, base.ErrInsufficientSpace) || errors.Is(err, base.ErrConflict) || errors.Is(err, mapstore.ErrRecoveryRequired)
}

func (w *segmentMaintenanceWorker) runPrepare(ctx context.Context, phase maintenancePhase) maintenanceTransition {
	switch phase {
	case maintenancePhaseStart:
		relocated, err := w.store.relocateSegment(ctx, w.request.source, w.store.maintenance.gcBytesPerSecond.Load())
		mergeRelocationResult(&w.result.relocation, relocated)
		if generation, needed := checkpointDependency(err); needed {
			return maintenanceTransition{next: maintenancePhaseStart, retain: maintenanceRecoveryProtocol,
				dependency: &maintenanceRequest{kind: maintenanceCheckpointRequest, generation: generation, force: true}}
		}
		if err != nil {
			return maintenanceTransition{done: true, result: w.result, err: err}
		}
		return maintenanceTransition{next: maintenancePhaseProve, retain: maintenanceRecoveryProtocol,
			dependency: &maintenanceRequest{kind: maintenanceCheckpointRequest, force: true, gcAdmission: true}}
	case maintenancePhaseProve:
		proof, err := w.store.proveSegmentRetirement(ctx, w.request.source, w.result.relocation.LastCommitSeq)
		if errors.Is(err, errCheckpointStatsStale) && w.proofAttempts < 31 {
			w.proofAttempts++
			return maintenanceTransition{next: maintenancePhaseProve, retain: maintenanceRecoveryProtocol,
				dependency: &maintenanceRequest{kind: maintenanceCheckpointRequest, force: true, gcAdmission: true}}
		}
		w.result.proof = proof
		return maintenanceTransition{done: true, result: w.result, err: err}
	default:
		return maintenanceTransition{done: true, err: base.ErrInvalidConfig}
	}
}

// Complete compaction is being migrated into explicit phases. The copy helper
// returns before Checkpoint; proof and retirement are resumed by this worker.
func (w *segmentMaintenanceWorker) runCompact(ctx context.Context, phase maintenancePhase) maintenanceTransition {
	switch phase {
	case maintenancePhaseStart:
		manifest := w.store.catalogSnapshot()
		index := sort.Search(len(manifest.SealedDataSegments), func(i int) bool { return manifest.SealedDataSegments[i].SegmentID >= w.request.source })
		if index == len(manifest.SealedDataSegments) || manifest.SealedDataSegments[index].SegmentID != w.request.source || recordlog.IsCompactionSegment(w.request.source) {
			return maintenanceTransition{done: true, err: recordlog.ErrSegmentMissing}
		}
		w.inputs = []recordlog.SegmentSummary{manifest.SealedDataSegments[index]}
		w.started = time.Now()
		w.store.metrics.gcStarted.Add(1)
		work, err := w.store.compactSegmentsCopy(ctx, w.inputs, w.store.maintenance.gcBytesPerSecond.Load())
		if work != nil {
			w.copyWork = work
			w.result.compaction = work.result
		}
		if err != nil {
			w.store.recordCompactionMetrics(w.started, w.result.compaction, err)
			return maintenanceTransition{done: true, result: w.result, err: err}
		}
		return maintenanceTransition{next: maintenancePhasePublish, retain: maintenanceRecoveryProtocol}
	case maintenancePhasePublish:
		err := w.store.publishCompactionRelocations(ctx, w.copyWork)
		if generation, needed := checkpointDependency(err); needed {
			return maintenanceTransition{next: maintenancePhasePublish, retain: maintenanceRecoveryProtocol,
				dependency: &maintenanceRequest{kind: maintenanceCheckpointRequest, generation: generation, force: true}}
		}
		if err != nil {
			w.copyWork.space.complete(false)
			err = w.store.compactionFailure(err)
			w.store.recordCompactionMetrics(w.started, w.result.compaction, err)
			return maintenanceTransition{done: true, result: w.result, err: err}
		}
		w.result.compaction = w.copyWork.result
		return maintenanceTransition{next: maintenancePhaseProve, retain: maintenanceRecoveryProtocol,
			dependency: &maintenanceRequest{kind: maintenanceCheckpointRequest, force: true, gcAdmission: true}}
	case maintenancePhaseProve:
		proofs, stale, err := w.store.proveCompactionRetirements(w.inputs, w.result.compaction.Relocation.LastCommitSeq)
		if stale && w.proofAttempts < 31 {
			w.proofAttempts++
			return maintenanceTransition{next: maintenancePhaseProve, retain: maintenanceRecoveryProtocol,
				dependency: &maintenanceRequest{kind: maintenanceCheckpointRequest, force: true, gcAdmission: true}}
		}
		if err != nil {
			w.store.recordCompactionMetrics(w.started, w.result.compaction, err)
			return maintenanceTransition{done: true, result: w.result, err: err}
		}
		w.result.compaction.Proofs = proofs
		if len(proofs) != 0 {
			w.result.compaction.Proof = proofs[0]
		}
		return maintenanceTransition{next: maintenancePhaseRetire, retain: maintenanceRecoveryProtocol}
	case maintenancePhaseRetire:
		err := w.store.finishCompactionRetirement(ctx, w.copyWork, w.result.compaction.Proofs)
		if w.copyWork != nil {
			w.result.compaction = w.copyWork.result
		}
		w.store.recordCompactionMetrics(w.started, w.result.compaction, err)
		return maintenanceTransition{done: true, result: w.result, err: err}
	default:
		return maintenanceTransition{done: true, err: base.ErrInvalidConfig}
	}
}

func (w *segmentMaintenanceWorker) runNext(ctx context.Context, phase maintenancePhase) maintenanceTransition {
	if phase != maintenancePhaseStart {
		return w.runCompact(ctx, phase)
	}
	published := w.store.PublishedState()
	if published == nil {
		return maintenanceTransition{done: true, err: base.ErrInvalidConfig}
	}
	excluded, err := w.store.openBatchCompactionOutputReferences(published.Manifest.SealedDataSegments)
	if err != nil {
		return maintenanceTransition{done: true, err: err}
	}
	now := w.store.maintenanceNow()
	candidate, found, err := selectCompactionCandidate(published.Manifest, w.request.policy, excluded, func(id recordlog.SegmentID) (gcStabilityView, bool) {
		return w.store.maintenance.gcStability.view(id, now, w.request.policy)
	})
	if err != nil {
		return maintenanceTransition{done: true, err: err}
	}
	if !found {
		// Refresh candidate statistics once, then return to Start. The worker
		// remembers the refresh to avoid an unbounded no-candidate loop.
		if w.proofAttempts == 0 {
			w.proofAttempts = 1
			return maintenanceTransition{next: maintenancePhaseStart, retain: maintenanceRecoveryProtocol,
				dependency: &maintenanceRequest{kind: maintenanceCheckpointRequest, force: true}}
		}
		w.store.metrics.gcNoCandidate.Add(1)
		return maintenanceTransition{done: true, result: w.result}
	}
	w.result.next.Candidate = candidate
	w.result.found = true
	w.inputs = candidate.Sources
	w.started = time.Now()
	w.store.metrics.gcStarted.Add(1)
	work, err := w.store.compactSegmentsCopy(ctx, w.inputs, w.store.maintenance.gcBytesPerSecond.Load())
	if work != nil {
		w.copyWork = work
		w.result.compaction = work.result
		w.result.next.Compaction = work.result
	}
	if err != nil {
		w.store.recordCompactionMetrics(w.started, w.result.compaction, err)
		return maintenanceTransition{done: true, result: w.result, err: fmt.Errorf("compact segment %d: %w", candidate.Source.SegmentID, err)}
	}
	return maintenanceTransition{next: maintenancePhasePublish, retain: maintenanceRecoveryProtocol}
}

func mergeRelocationResult(total *SegmentRelocationResult, next SegmentRelocationResult) {
	total.ScannedRecords += next.ScannedRecords
	total.PutRecords += next.PutRecords
	total.LiveCandidates += next.LiveCandidates
	total.CopiedRecords += next.CopiedRecords
	total.CopiedValueBytes += next.CopiedValueBytes
	total.CopiedPhysicalBytes += next.CopiedPhysicalBytes
	total.Applied += next.Applied
	total.Skipped += next.Skipped
	if total.FirstCommitSeq == 0 {
		total.FirstCommitSeq = next.FirstCommitSeq
	}
	if next.LastCommitSeq > total.LastCommitSeq {
		total.LastCommitSeq = next.LastCommitSeq
	}
}

type mappingGCMaintenanceWorker struct {
	store   *Store
	request maintenanceRequest
	work    *mappingGCWork
}

func (w *mappingGCMaintenanceWorker) Resources(phase maintenancePhase) maintenanceResource {
	if phase == maintenancePhaseStart && w.request.automatic {
		return maintenanceHeavyIO
	}
	if phase == maintenancePhaseStart || phase == maintenancePhaseCleanup {
		return maintenanceRecoveryProtocol
	}
	if phase == maintenancePhasePublish {
		return maintenanceMappingWriter | maintenanceRecoveryProtocol
	}
	return maintenanceHeavyIO | maintenanceRecoveryProtocol
}
func (w *mappingGCMaintenanceWorker) Run(ctx context.Context, phase maintenancePhase, _ maintenanceResult) maintenanceTransition {
	if phase == maintenancePhaseStart {
		if w.request.automatic {
			config := w.store.maintenance.config
			last := w.store.maintenance.lastMappingGCUnixNano.Load()
			if last != 0 && w.store.maintenanceNow().Sub(time.Unix(0, last)) < config.MappingMinInterval {
				return maintenanceTransition{done: true}
			}
			usage, err := w.store.runMappingSurvey(ctx)
			if err != nil {
				if !expectedAutomaticMaintenanceError(err) {
					w.store.metrics.maintenanceAutomaticFailed.Add(1)
				}
				return maintenanceTransition{done: true, err: err}
			}
			garbage := usage.physicalBytes - usage.reachableBytes
			ratio := uint32(0)
			if usage.physicalBytes != 0 {
				ratio = uint32(garbage * uint64(compactionRatioScale) / usage.physicalBytes)
			}
			if garbage < config.MappingMinReclaimableBytes || ratio < config.MappingMinReclaimableRatioBasis {
				return maintenanceTransition{done: true}
			}
		}
		retain := maintenanceRecoveryProtocol
		if w.request.automatic {
			retain = 0
		}
		return maintenanceTransition{next: maintenancePhaseCopy, retain: retain,
			dependency: &maintenanceRequest{kind: maintenanceCheckpointRequest, force: true}}
	}
	switch phase {
	case maintenancePhaseCopy:
		work, err := w.store.prepareMappingGC(ctx)
		w.work = work
		if err != nil {
			if w.request.automatic && !expectedAutomaticMaintenanceError(err) {
				w.store.metrics.maintenanceAutomaticFailed.Add(1)
			}
			return maintenanceTransition{done: true, err: err}
		}
		return maintenanceTransition{next: maintenancePhasePublish, retain: maintenanceRecoveryProtocol}
	case maintenancePhasePublish:
		if err := w.store.publishMappingGC(ctx, w.work); err != nil {
			if w.request.automatic && !expectedAutomaticMaintenanceError(err) {
				w.store.metrics.maintenanceAutomaticFailed.Add(1)
			}
			return maintenanceTransition{done: true, err: err}
		}
		return maintenanceTransition{next: maintenancePhaseCleanup, retain: maintenanceRecoveryProtocol}
	case maintenancePhaseCleanup:
		err := w.store.cleanupMappingGC(w.work)
		if err != nil && w.request.automatic && !expectedAutomaticMaintenanceError(err) {
			w.store.metrics.maintenanceAutomaticFailed.Add(1)
		}
		if err == nil && w.request.automatic {
			w.store.maintenance.lastMappingGCUnixNano.Store(w.store.maintenanceNow().UnixNano())
		}
		return maintenanceTransition{done: true, err: err}
	default:
		return maintenanceTransition{done: true, err: base.ErrInvalidConfig}
	}
}

type mappingSurveyMaintenanceWorker struct{ store *Store }

func (*mappingSurveyMaintenanceWorker) Resources(maintenancePhase) maintenanceResource {
	return maintenanceHeavyIO
}
func (w *mappingSurveyMaintenanceWorker) Run(ctx context.Context, _ maintenancePhase, _ maintenanceResult) maintenanceTransition {
	usage, err := w.store.runMappingSurvey(ctx)
	return maintenanceTransition{done: true, result: maintenanceResult{usage: usage}, err: err}
}

func (s *Store) runMappingSurvey(ctx context.Context) (*mappingUsage, error) {
	published := s.PublishedState()
	if published == nil {
		return nil, base.ErrInvalidConfig
	}
	view, err := s.core.mapping.CheckpointView()
	if err != nil {
		return nil, err
	}
	if view.Root() != published.MappingRoot || view.Covered() != published.CoveredCommit {
		return nil, base.ErrConflict
	}
	reachable, err := view.ReachableBytes(ctx)
	if err != nil {
		return nil, err
	}
	snapshot := s.core.publisher.SnapshotMapStore()
	if snapshot.Generation != published.Generation || snapshot.Root != published.MappingRoot {
		return nil, base.ErrConflict
	}
	report, err := mapstore.VerifyFiles(ctx, s.core.root, snapshot)
	if err != nil {
		return nil, err
	}
	if current := s.PublishedState(); current == nil || current.Generation != published.Generation || current.MappingRoot != published.MappingRoot {
		return nil, base.ErrConflict
	}
	if reachable > report.PhysicalBytes {
		return nil, base.ErrCorrupt
	}
	usage := &mappingUsage{generation: published.Generation, root: published.MappingRoot, physicalBytes: report.PhysicalBytes, reachableBytes: reachable}
	s.maintenance.mappingUsage.Store(usage)
	s.metrics.mappingSurveyPhysicalBytes.Store(report.PhysicalBytes)
	s.metrics.mappingSurveyReachableBytes.Store(reachable)
	s.metrics.mappingSurveyGeneration.Store(published.Generation)
	return usage, nil
}

package engine

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/compactionstate"
	"github.com/akzj/ridstore/internal/maintstate"
	"github.com/akzj/ridstore/internal/mapgcstate"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/radix"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

// CompactMapping rewrites the current logical Mapping into a fresh physical
// generation. The immutable checkpoint Root is rebuilt without blocking data
// operations or later checkpoints. Commit admission is fenced only by the
// initial checkpoint cut; final publication serializes with checkpoints and
// returns ErrConflict if one advanced the Root during the rebuild.
func (s *Store) CompactMapping(ctx context.Context) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, end, err := s.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer end()
	started := time.Now()
	s.metrics.mappingGCStarted.Add(1)
	defer func() {
		duration := uint64(time.Since(started))
		s.metrics.mappingGCDurationNanos.Add(duration)
		updateAtomicMax(&s.metrics.mappingGCMaxDurationNanos, duration)
		if err == nil {
			s.metrics.mappingGCCompleted.Add(1)
		} else if errors.Is(err, base.ErrConflict) {
			s.metrics.mappingGCConflicts.Add(1)
		} else {
			s.metrics.mappingGCFailed.Add(1)
		}
	}()
	// Mapping rewrite is a low-priority COW phase. It holds recoveryProtocol so
	// no other marker-producing GC can overlap, but deliberately does not hold
	// mappingWriter while rebuilding: Checkpoint may advance and make this plan
	// stale, which the publication validation handles as a normal conflict.
	if s.maintenance.scheduler == nil {
		return base.ErrInvalidConfig
	}
	_, err = s.maintenance.scheduler.Submit(ctx, maintenanceRequest{kind: maintenanceMappingGCRequest})
	return err
}

type mappingGCWork struct {
	manifest   storecatalog.Manifest
	view       mapping.CheckpointView
	generation mapstore.Generation
	newTree    *radix.Tree
	staging    string
	space      *spaceReservation
	state      mapgcstate.State
	installed  storecatalog.Manifest
	drained    <-chan struct{}
	oldStore   *mapstore.Store
}

func (s *Store) prepareMappingGC(ctx context.Context) (work *mappingGCWork, err error) {
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	checks := []struct {
		name  string
		check func(string) (bool, error)
	}{
		{"data maintenance", maintstate.RecoveryArtifacts}, {"data compaction", compactionstate.RecoveryArtifacts},
		{"mapping rotation", mapstore.RecoveryArtifacts}, {"mapping gc", mapgcstate.RecoveryArtifacts},
	}
	for _, check := range checks {
		artifacts, checkErr := check.check(s.core.root)
		if checkErr != nil {
			return nil, checkErr
		}
		if artifacts {
			return nil, errors.Join(base.ErrRecoveryRequired, errors.New(check.name+" is active"))
		}
	}
	s.checkpoints.captureMu.Lock()
	published := s.PublishedState()
	if published == nil {
		s.checkpoints.captureMu.Unlock()
		return nil, base.ErrInvalidConfig
	}
	manifest := published.Manifest.Clone()
	view, err := s.core.mapping.CheckpointView()
	if err != nil {
		s.checkpoints.captureMu.Unlock()
		return nil, err
	}
	if view.Root() != manifest.MappingRoot || view.Covered() != manifest.CoveredCommitSeq {
		s.checkpoints.captureMu.Unlock()
		return nil, base.ErrCorrupt
	}
	s.checkpoints.captureMu.Unlock()
	space, err := s.reserveMappingGC(ctx, manifest, manifest.MappingEntryCount)
	if err != nil {
		return nil, err
	}
	work = &mappingGCWork{manifest: manifest, view: view, staging: mapgcstate.StagingRoot(s.core.root), space: space}
	fail := func(cause error) (*mappingGCWork, error) { space.complete(false); return work, cause }
	if err = os.Mkdir(work.staging, 0o700); err != nil {
		return fail(err)
	}
	if err = syncEngineDirectory(s.core.root); err != nil {
		return fail(s.mappingGCPrepublishFailure(err, discardMappingGCStaging(s.core.root)))
	}
	writer, err := mapstore.CreateGenerationWriter(work.staging, mapstore.StoreID(manifest.StoreUUID), uint32(manifest.HardLimits.SegmentSize), manifest.NextMapSegmentID, s.maintenance.mapStoreHook)
	if err != nil {
		return fail(s.mappingGCPrepublishFailure(err, discardMappingGCStaging(s.core.root)))
	}
	cleanupWriter := func(cause error) (*mappingGCWork, error) {
		cleanup := errors.Join(writer.Close(), discardMappingGCStaging(s.core.root))
		if errors.Is(cause, mapping.ErrStalePlan) {
			return fail(s.mappingGCConflict(cause, cleanup))
		}
		return fail(s.mappingGCPrepublishFailure(cause, cleanup))
	}
	rebuildStarted := time.Now()
	builder, err := radix.NewRebuildBuilder(writer, manifest.CoveredCommitSeq, s.maintenance.mappingCacheBytes)
	if err != nil {
		return cleanupWriter(err)
	}
	var oldCount uint64
	if err = view.WalkRefs(ctx, func(id model.ID, ref recordlog.RecordRef) error {
		if oldCount == ^uint64(0) {
			return base.ErrOverflow
		}
		oldCount++
		return builder.Add(id, ref)
	}); err != nil {
		return cleanupWriter(err)
	}
	if oldCount != manifest.MappingEntryCount {
		return cleanupWriter(base.ErrCorrupt)
	}
	work.newTree, err = builder.Finish()
	if err != nil {
		return cleanupWriter(err)
	}
	work.generation, err = writer.Finish(work.newTree.Root(), manifest.CoveredCommitSeq)
	if err != nil {
		return cleanupWriter(err)
	}
	s.metrics.mappingGCRebuildNanos.Add(uint64(time.Since(rebuildStarted)))
	verifyStarted := time.Now()
	var newCount uint64
	if err = work.newTree.WalkRefs(ctx, func(id model.ID, ref recordlog.RecordRef) error {
		oldRef, exists, lookupErr := view.LookupRef(id)
		if lookupErr != nil {
			return lookupErr
		}
		if !exists || oldRef != ref {
			return base.ErrCorrupt
		}
		newCount++
		return nil
	}); err != nil || newCount != oldCount {
		if err == nil {
			err = base.ErrCorrupt
		}
		return cleanupWriter(err)
	}
	s.metrics.mappingGCVerifyNanos.Add(uint64(time.Since(verifyStarted)))
	if err = writer.Close(); err != nil {
		return fail(s.mappingGCPrepublishFailure(err, discardMappingGCStaging(s.core.root)))
	}
	return work, nil
}

func (s *Store) publishMappingGC(work *mappingGCWork) (err error) {
	if work == nil {
		return base.ErrInvalidConfig
	}
	fail := func(cause error) error { work.space.complete(false); return cause }
	s.checkpoints.captureMu.Lock()
	publishStarted := time.Now()
	defer func() {
		duration := uint64(time.Since(publishStarted))
		s.metrics.mappingGCPublishNanos.Add(duration)
		updateAtomicMax(&s.metrics.mappingGCMaxPublishNanos, duration)
		s.checkpoints.captureMu.Unlock()
	}()
	currentView, err := s.core.mapping.CheckpointView()
	if err != nil || currentView.Root() != work.manifest.MappingRoot || currentView.Covered() != work.manifest.CoveredCommitSeq {
		if err == nil {
			err = base.ErrCorrupt
		}
		return fail(s.mappingGCPrepublishFailure(err, discardMappingGCStaging(s.core.root)))
	}
	oldSet := mappingGCFileSet(work.manifest.SealedMapSegments, work.manifest.ActiveMapSegmentID, work.manifest.NextMapSegmentID, work.manifest.MappingRoot)
	prepared := false
	rollback := func(cause error) error {
		cleanup := mapstore.RollbackGeneration(s.core.root, work.staging, work.generation, s.maintenance.mapStoreHook)
		if cleanup == nil {
			cleanup = mapgcstate.Remove(s.core.root, s.maintenance.mappingGCHook)
		}
		return s.mappingGCPrepublishFailure(cause, cleanup)
	}
	installed, err := s.core.publisher.PrepareAndInstallMappingRewrite(func(latest storecatalog.Manifest) error {
		if latest.CoveredCommitSeq != work.manifest.CoveredCommitSeq || latest.MappingEntryCount != work.manifest.MappingEntryCount || !manifestMatchesMappingSet(latest, oldSet) {
			return s.mappingGCConflict(mapping.ErrStalePlan, discardMappingGCStaging(s.core.root))
		}
		work.state = mapgcstate.State{StoreID: [16]byte(latest.StoreUUID), BaseGeneration: latest.Generation,
			SegmentSize: uint32(latest.HardLimits.SegmentSize), Covered: latest.CoveredCommitSeq, Old: oldSet,
			New: mapgcstate.FileSet{Sealed: work.generation.SealedSegments, Active: work.generation.ActiveSegment, Next: work.generation.NextSegment, Root: work.generation.Root}}
		if installErr := mapgcstate.Install(s.core.root, work.state, s.maintenance.mappingGCHook); installErr != nil {
			cleanup := discardMappingGCStaging(s.core.root)
			if cleanup == nil {
				cleanup = mapgcstate.Remove(s.core.root, s.maintenance.mappingGCHook)
			}
			return s.mappingGCPrepublishFailure(installErr, cleanup)
		}
		if promoteErr := mapstore.PromoteGeneration(s.core.root, work.staging, work.generation, s.maintenance.mapStoreHook); promoteErr != nil {
			return rollback(promoteErr)
		}
		prepared = true
		return nil
	}, storecatalog.MappingRewrite{SealedSegments: mappingGCSummaries(work.generation.SealedSegments), ActiveSegment: work.generation.ActiveSegment,
		NextSegment: work.generation.NextSegment, Root: work.generation.Root, Covered: work.generation.Covered})
	if err != nil {
		if !prepared {
			return fail(err)
		}
		s.setFault(err)
		return fail(errors.Join(base.ErrReadOnly, err))
	}
	newStore, err := mapstore.OpenWithFaultHook(s.core.root, s.core.publisher, s.maintenance.mapStoreHook)
	if err != nil {
		s.setFault(err)
		return fail(errors.Join(base.ErrReadOnly, err))
	}
	newRoot, err := radix.Open(newStore, installed.MappingRoot, installed.CoveredCommitSeq, s.maintenance.mappingCacheBytes)
	if err != nil {
		_ = newStore.Close()
		s.setFault(err)
		return fail(errors.Join(base.ErrReadOnly, err))
	}
	reachableBytes, err := newRoot.ReachableBytes(context.Background())
	if err != nil {
		_ = newStore.Close()
		s.setFault(err)
		return fail(errors.Join(base.ErrReadOnly, err))
	}
	physicalBytes, err := newStore.PhysicalBytes()
	if err != nil || reachableBytes > physicalBytes {
		_ = newStore.Close()
		if err == nil {
			err = base.ErrCorrupt
		}
		s.setFault(err)
		return fail(errors.Join(base.ErrReadOnly, err))
	}
	work.drained, err = s.core.mapping.ReplaceCheckpointRoot(work.view, newRoot, newStore)
	if err != nil {
		_ = newStore.Close()
		s.setFault(err)
		return fail(errors.Join(base.ErrReadOnly, err))
	}
	work.oldStore, work.installed = s.core.mapStore, installed
	s.core.mapStore = newStore
	s.maintenance.mappingUsage.Store(&mappingUsage{generation: installed.Generation, root: installed.MappingRoot, physicalBytes: physicalBytes, reachableBytes: reachableBytes})
	s.metrics.mappingSurveyGeneration.Store(installed.Generation)
	s.metrics.mappingSurveyPhysicalBytes.Store(physicalBytes)
	s.metrics.mappingSurveyReachableBytes.Store(reachableBytes)
	return nil
}

func (s *Store) cleanupMappingGC(work *mappingGCWork) error {
	if work == nil || work.drained == nil || work.oldStore == nil {
		return base.ErrInvalidConfig
	}
	fail := func(err error) error {
		work.space.complete(false)
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	<-work.drained
	if err := work.oldStore.Close(); err != nil {
		return fail(err)
	}
	if err := mapstore.RetireGeneration(s.core.root, generationFromState(work.state.Old, work.state.Covered), work.installed.Generation, s.maintenance.mapStoreHook); err != nil {
		return fail(err)
	}
	if err := mapstore.RemoveGenerationStaging(s.core.root, work.staging); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fail(err)
	}
	if err := mapgcstate.Remove(s.core.root, s.maintenance.mappingGCHook); err != nil {
		return fail(err)
	}
	work.space.complete(true)
	return nil
}

func (s *Store) mappingGCPrepublishFailure(cause, cleanup error) error {
	if cleanup == nil && (errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded)) {
		return cause
	}
	result := errors.Join(cause, cleanup)
	if cleanup != nil {
		result = errors.Join(base.ErrRecoveryRequired, result)
	}
	s.setFault(result)
	return result
}

func (s *Store) mappingGCConflict(cause, cleanup error) error {
	if cleanup == nil {
		return errors.Join(base.ErrConflict, cause)
	}
	result := errors.Join(base.ErrRecoveryRequired, base.ErrConflict, cause, cleanup)
	s.setFault(result)
	return result
}

func mappingGCFileSet(sealed []storecatalog.MapSegmentSummary, active, next model.MapSegmentID, root model.MapAddr) mapgcstate.FileSet {
	refs := make([]mapstore.SegmentRef, len(sealed))
	for index, summary := range sealed {
		refs[index] = mapstore.SegmentRef{SegmentID: summary.SegmentID, ValidEnd: summary.ValidEnd}
	}
	return mapgcstate.FileSet{Sealed: refs, Active: active, Next: next, Root: root}
}

func mappingGCSummaries(refs []mapstore.SegmentRef) []storecatalog.MapSegmentSummary {
	summaries := make([]storecatalog.MapSegmentSummary, len(refs))
	for index, ref := range refs {
		summaries[index] = storecatalog.MapSegmentSummary{SegmentID: ref.SegmentID, ValidEnd: ref.ValidEnd}
	}
	return summaries
}

func discardMappingGCStaging(root string) error {
	staging := mapgcstate.StagingRoot(root)
	if err := os.RemoveAll(staging); err != nil {
		return err
	}
	return syncEngineDirectory(root)
}

func syncEngineDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

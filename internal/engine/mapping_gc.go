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
	if err := s.beginOperation(); err != nil {
		return err
	}
	defer s.endOperation()
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
	if err := s.checkpoint(ctx, false); err != nil {
		return err
	}
	// Mapping rewrite is a low-priority COW phase. Serialize it with
	// Checkpoint at the dispatcher boundary, but never use this gate to block
	// foreground operations while the tree is rebuilt.
	if s.maintenance.scheduler == nil {
		s.maintenance.scheduler = &MaintenanceScheduler{}
	}
	if err := s.maintenance.scheduler.acquireMappingRewrite(ctx); err != nil {
		return err
	}
	defer s.maintenance.scheduler.releaseMappingRewrite()
	return s.compactCheckpointMapping(ctx)
}

func (s *Store) compactCheckpointMapping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if artifacts, err := maintstate.RecoveryArtifacts(s.core.root); err != nil {
		return err
	} else if artifacts {
		return errors.Join(base.ErrRecoveryRequired, errors.New("data maintenance is active"))
	}
	if artifacts, err := compactionstate.RecoveryArtifacts(s.core.root); err != nil {
		return err
	} else if artifacts {
		return errors.Join(base.ErrRecoveryRequired, errors.New("data compaction is active"))
	}
	if artifacts, err := mapstore.RecoveryArtifacts(s.core.root); err != nil {
		return err
	} else if artifacts {
		return errors.Join(base.ErrRecoveryRequired, errors.New("mapping rotation is active"))
	}
	if artifacts, err := mapgcstate.RecoveryArtifacts(s.core.root); err != nil {
		return err
	} else if artifacts {
		return errors.Join(base.ErrRecoveryRequired, errors.New("mapping gc is active"))
	}

	// Capture a self-consistent immutable Root, then release the checkpoint
	// capture lock for the complete rebuild. A concurrent checkpoint may advance
	// this Root; the publication phase detects that as a normal optimistic conflict.
	s.checkpoints.captureMu.Lock()
	published := s.PublishedState()
	if published == nil {
		s.checkpoints.captureMu.Unlock()
		return base.ErrInvalidConfig
	}
	manifest := published.Manifest.Clone()
	view, err := s.core.mapping.CheckpointView()
	if err != nil {
		s.checkpoints.captureMu.Unlock()
		return err
	}
	if view.Root() != manifest.MappingRoot || view.Covered() != manifest.CoveredCommitSeq {
		s.checkpoints.captureMu.Unlock()
		return base.ErrCorrupt
	}
	s.checkpoints.captureMu.Unlock()
	space, err := s.reserveMappingGC(ctx, manifest, manifest.MappingEntryCount)
	if err != nil {
		return err
	}
	spaceCommitted := false
	defer func() { space.complete(spaceCommitted) }()
	staging := mapgcstate.StagingRoot(s.core.root)
	if err := os.Mkdir(staging, 0o700); err != nil {
		return err
	}
	if err := syncEngineDirectory(s.core.root); err != nil {
		return s.mappingGCPrepublishFailure(err, discardMappingGCStaging(s.core.root))
	}
	writer, err := mapstore.CreateGenerationWriter(staging, mapstore.StoreID(manifest.StoreUUID), uint32(manifest.HardLimits.SegmentSize), manifest.NextMapSegmentID, s.maintenance.mapStoreHook)
	if err != nil {
		return s.mappingGCPrepublishFailure(err, discardMappingGCStaging(s.core.root))
	}
	cleanupWriter := func(cause error) error {
		cleanup := errors.Join(writer.Close(), discardMappingGCStaging(s.core.root))
		if errors.Is(cause, mapping.ErrStalePlan) {
			return s.mappingGCConflict(cause, cleanup)
		}
		return s.mappingGCPrepublishFailure(cause, cleanup)
	}
	rebuildStarted := time.Now()
	builder, err := radix.NewRebuildBuilder(writer, manifest.CoveredCommitSeq, s.maintenance.mappingCacheBytes)
	if err != nil {
		return cleanupWriter(err)
	}
	var oldCount uint64
	if err := view.WalkRefs(ctx, func(id model.ID, ref recordlog.RecordRef) error {
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
	newTree, err := builder.Finish()
	if err != nil {
		return cleanupWriter(err)
	}
	generation, err := writer.Finish(newTree.Root(), manifest.CoveredCommitSeq)
	if err != nil {
		return cleanupWriter(err)
	}
	s.metrics.mappingGCRebuildNanos.Add(uint64(time.Since(rebuildStarted)))
	verifyStarted := time.Now()
	var newCount uint64
	if err := newTree.WalkRefs(ctx, func(id model.ID, ref recordlog.RecordRef) error {
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
	if err := writer.Close(); err != nil {
		return s.mappingGCPrepublishFailure(err, discardMappingGCStaging(s.core.root))
	}

	// Publication and recovery-marker cleanup remain serialized with
	// checkpoints. The long rebuild and verification above do not.
	s.checkpoints.captureMu.Lock()
	publishStarted := time.Now()
	unlockPublication := func() {
		duration := uint64(time.Since(publishStarted))
		s.metrics.mappingGCPublishNanos.Add(duration)
		updateAtomicMax(&s.metrics.mappingGCMaxPublishNanos, duration)
		s.checkpoints.captureMu.Unlock()
	}
	latest := s.catalogSnapshot()
	currentView, currentErr := s.core.mapping.CheckpointView()
	if currentErr != nil || currentView.Root() != latest.MappingRoot || currentView.Covered() != latest.CoveredCommitSeq {
		unlockPublication()
		if currentErr == nil {
			currentErr = base.ErrCorrupt
		}
		return s.mappingGCPrepublishFailure(currentErr, discardMappingGCStaging(s.core.root))
	}
	oldSet := mappingGCFileSet(manifest.SealedMapSegments, manifest.ActiveMapSegmentID, manifest.NextMapSegmentID, manifest.MappingRoot)
	// Data-segment rotations advance the global Catalog generation without
	// invalidating this Mapping-only plan. Validate the independent Mapping
	// dimensions here; InstallMappingRewrite performs the same append-only
	// Data rebase check before durable publication.
	if latest.CoveredCommitSeq != manifest.CoveredCommitSeq || latest.MappingEntryCount != manifest.MappingEntryCount ||
		!manifestMatchesMappingSet(latest, oldSet) {
		unlockPublication()
		return s.mappingGCConflict(mapping.ErrStalePlan, discardMappingGCStaging(s.core.root))
	}
	defer unlockPublication()
	state := mapgcstate.State{
		StoreID: [16]byte(latest.StoreUUID), BaseGeneration: latest.Generation,
		SegmentSize: uint32(latest.HardLimits.SegmentSize), Covered: latest.CoveredCommitSeq,
		Old: oldSet,
		New: mapgcstate.FileSet{Sealed: generation.SealedSegments, Active: generation.ActiveSegment, Next: generation.NextSegment, Root: generation.Root},
	}
	if err := mapgcstate.Install(s.core.root, state, s.maintenance.mappingGCHook); err != nil {
		cleanupErr := discardMappingGCStaging(s.core.root)
		if cleanupErr == nil {
			cleanupErr = mapgcstate.Remove(s.core.root, s.maintenance.mappingGCHook)
		}
		return s.mappingGCPrepublishFailure(err, cleanupErr)
	}
	rollback := func(cause error) error {
		cleanupErr := mapstore.RollbackGeneration(s.core.root, staging, generation, s.maintenance.mapStoreHook)
		if cleanupErr == nil {
			cleanupErr = mapgcstate.Remove(s.core.root, s.maintenance.mappingGCHook)
		}
		return s.mappingGCPrepublishFailure(cause, cleanupErr)
	}
	if err := mapstore.PromoteGeneration(s.core.root, staging, generation, s.maintenance.mapStoreHook); err != nil {
		return rollback(err)
	}
	installed, err := s.core.publisher.InstallMappingRewrite(latest, storecatalog.MappingRewrite{
		SealedSegments: mappingGCSummaries(generation.SealedSegments), ActiveSegment: generation.ActiveSegment,
		NextSegment: generation.NextSegment, Root: generation.Root, Covered: generation.Covered,
	})
	if err != nil {
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}

	newStore, err := mapstore.OpenWithFaultHook(s.core.root, s.core.publisher, s.maintenance.mapStoreHook)
	if err != nil {
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	newRoot, err := radix.Open(newStore, installed.MappingRoot, installed.CoveredCommitSeq, s.maintenance.mappingCacheBytes)
	if err != nil {
		_ = newStore.Close()
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	drained, err := s.core.mapping.ReplaceCheckpointRoot(view, newRoot, newStore)
	if err != nil {
		_ = newStore.Close()
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	oldStore := s.core.mapStore
	s.core.mapStore = newStore
	<-drained
	if err := oldStore.Close(); err != nil {
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	if err := mapstore.RetireGeneration(s.core.root, generationFromState(state.Old, state.Covered), installed.Generation, s.maintenance.mapStoreHook); err != nil {
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	if err := mapstore.RemoveGenerationStaging(s.core.root, staging); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	if err := mapgcstate.Remove(s.core.root, s.maintenance.mappingGCHook); err != nil {
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	spaceCommitted = true
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

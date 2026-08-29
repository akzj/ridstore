package engine

import (
	"context"
	"errors"
	"os"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/maintstate"
	"github.com/akzj/ridstore/internal/mapgcstate"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/radix"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

// CompactMapping rewrites the current logical Mapping into a fresh physical
// generation. The immutable checkpoint Root is rebuilt without blocking data
// operations; ops.Lock is held only to capture the cut and to publish/switch.
func (s *Store) CompactMapping(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	s.checkpointMu.Lock()
	defer s.checkpointMu.Unlock()
	s.ops.Lock()
	work, err := s.prepareCheckpointLocked(ctx)
	s.ops.Unlock()
	if err != nil {
		return err
	}
	if err := s.finishCheckpoint(ctx, work); err != nil {
		return err
	}
	return s.compactCheckpointMapping(ctx)
}

func (s *Store) compactCheckpointMapping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if artifacts, err := maintstate.RecoveryArtifacts(s.root); err != nil {
		return err
	} else if artifacts {
		return errors.Join(base.ErrRecoveryRequired, errors.New("data maintenance is active"))
	}
	if artifacts, err := mapstore.RecoveryArtifacts(s.root); err != nil {
		return err
	} else if artifacts {
		return errors.Join(base.ErrRecoveryRequired, errors.New("mapping rotation is active"))
	}
	if artifacts, err := mapgcstate.RecoveryArtifacts(s.root); err != nil {
		return err
	} else if artifacts {
		return errors.Join(base.ErrRecoveryRequired, errors.New("mapping gc is active"))
	}

	manifest := s.catalog.Snapshot()
	view, err := s.mapping.CheckpointView()
	if err != nil {
		return err
	}
	if view.Root() != manifest.MappingRoot || view.Covered() != manifest.CoveredCommitSeq {
		return base.ErrCorrupt
	}
	space, err := s.reserveMappingGC(ctx, manifest, manifest.MappingEntryCount)
	if err != nil {
		return err
	}
	spaceCommitted := false
	defer func() { space.complete(spaceCommitted) }()
	staging := mapgcstate.StagingRoot(s.root)
	if err := os.Mkdir(staging, 0o700); err != nil {
		return err
	}
	if err := syncEngineDirectory(s.root); err != nil {
		return s.mappingGCPrepublishFailure(err, discardMappingGCStaging(s.root))
	}
	writer, err := mapstore.CreateGenerationWriter(staging, mapstore.StoreID(manifest.StoreUUID), uint32(manifest.HardLimits.SegmentSize), manifest.NextMapSegmentID, s.mapStoreHook)
	if err != nil {
		return s.mappingGCPrepublishFailure(err, discardMappingGCStaging(s.root))
	}
	cleanupWriter := func(cause error) error {
		return s.mappingGCPrepublishFailure(cause, errors.Join(writer.Close(), discardMappingGCStaging(s.root)))
	}
	builder, err := radix.NewRebuildBuilder(writer, manifest.CoveredCommitSeq, s.mappingCacheBytes)
	if err != nil {
		return cleanupWriter(err)
	}
	var oldCount uint64
	if err := view.Walk(ctx, func(id model.ID, addr recordlog.VAddr) error {
		if oldCount == ^uint64(0) {
			return base.ErrOverflow
		}
		oldCount++
		return builder.Add(id, addr)
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
	var newCount uint64
	if err := newTree.Walk(ctx, func(id model.ID, addr recordlog.VAddr) error {
		oldAddr, exists, lookupErr := view.Lookup(id)
		if lookupErr != nil {
			return lookupErr
		}
		if !exists || oldAddr != addr {
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
	if err := writer.Close(); err != nil {
		return s.mappingGCPrepublishFailure(err, discardMappingGCStaging(s.root))
	}

	// Stop new submissions and drain every commit admitted before the lock.
	// Newer commits remain in the active Mapping Delta and are preserved when
	// the physically equivalent checkpoint Root is switched below.
	s.ops.Lock()
	opsLocked := true
	defer func() {
		if opsLocked {
			s.ops.Unlock()
		}
	}()
	if _, err := s.commits.CheckpointCut(ctx); err != nil {
		return s.mappingGCPrepublishFailure(err, discardMappingGCStaging(s.root))
	}
	latest := s.catalog.Snapshot()
	oldSet := mappingGCFileSet(manifest.SealedMapSegments, manifest.ActiveMapSegmentID, manifest.NextMapSegmentID, manifest.MappingRoot)
	if latest.CoveredCommitSeq != manifest.CoveredCommitSeq || latest.MappingEntryCount != manifest.MappingEntryCount ||
		!manifestMatchesMappingSet(latest, oldSet) {
		return s.mappingGCPrepublishFailure(base.ErrCorrupt, discardMappingGCStaging(s.root))
	}
	state := mapgcstate.State{
		StoreID: [16]byte(latest.StoreUUID), BaseGeneration: latest.Generation,
		SegmentSize: uint32(latest.HardLimits.SegmentSize), Covered: latest.CoveredCommitSeq,
		Old: oldSet,
		New: mapgcstate.FileSet{Sealed: generation.SealedSegments, Active: generation.ActiveSegment, Next: generation.NextSegment, Root: generation.Root},
	}
	if err := mapgcstate.Install(s.root, state, s.mappingGCHook); err != nil {
		cleanupErr := discardMappingGCStaging(s.root)
		if cleanupErr == nil {
			cleanupErr = mapgcstate.Remove(s.root, s.mappingGCHook)
		}
		return s.mappingGCPrepublishFailure(err, cleanupErr)
	}
	rollback := func(cause error) error {
		cleanupErr := mapstore.RollbackGeneration(s.root, staging, generation, s.mapStoreHook)
		if cleanupErr == nil {
			cleanupErr = mapgcstate.Remove(s.root, s.mappingGCHook)
		}
		return s.mappingGCPrepublishFailure(cause, cleanupErr)
	}
	if err := mapstore.PromoteGeneration(s.root, staging, generation, s.mapStoreHook); err != nil {
		return rollback(err)
	}
	installed, err := s.catalog.InstallMappingRewrite(latest.Generation, storecatalog.MappingRewrite{
		SealedSegments: mappingGCSummaries(generation.SealedSegments), ActiveSegment: generation.ActiveSegment,
		NextSegment: generation.NextSegment, Root: generation.Root, Covered: generation.Covered,
	})
	if err != nil {
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}

	newStore, err := mapstore.OpenWithFaultHook(s.root, s.catalog, s.mapStoreHook)
	if err != nil {
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	newRoot, err := radix.Open(newStore, installed.MappingRoot, installed.CoveredCommitSeq, s.mappingCacheBytes)
	if err != nil {
		_ = newStore.Close()
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	if err := s.mapping.ReplaceCheckpointRoot(view, newRoot, newStore); err != nil {
		_ = newStore.Close()
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	oldStore := s.mapStore
	s.mapStore = newStore
	if err := oldStore.Close(); err != nil {
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	s.ops.Unlock()
	opsLocked = false
	if err := mapstore.RetireGeneration(s.root, generationFromState(state.Old, state.Covered), installed.Generation, s.mapStoreHook); err != nil {
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	if err := mapstore.RemoveGenerationStaging(s.root, staging); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	if err := mapgcstate.Remove(s.root, s.mappingGCHook); err != nil {
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

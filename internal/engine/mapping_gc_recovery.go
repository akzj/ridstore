package engine

import (
	"context"
	"errors"
	"math"
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

func recoverMappingGC(ctx context.Context, root string, catalog *storecatalog.Manager, stateHook mapgcstate.FaultHook, mapHook mapstore.FaultHook) error {
	state, found, err := mapgcstate.LoadWithFaultHook(root, stateHook)
	if errors.Is(err, mapgcstate.ErrCorrupt) {
		return errors.Join(base.ErrCorrupt, err)
	}
	if err != nil {
		return err
	}
	if !found {
		if artifacts, err := mapgcstate.RecoveryArtifacts(root); err != nil {
			return err
		} else if artifacts {
			// Promotion is forbidden before the durable marker. Therefore a
			// fixed staging directory without a marker can only be an
			// unpublished build abandoned before publication.
			if err := discardMappingGCStaging(root); err != nil {
				return errors.Join(base.ErrRecoveryRequired, err)
			}
		}
		return nil
	}
	if artifacts, err := maintstate.RecoveryArtifacts(root); err != nil {
		return err
	} else if artifacts {
		return errors.Join(base.ErrCorrupt, errors.New("data maintenance overlaps mapping gc"))
	}
	if artifacts, err := mapstore.RecoveryArtifacts(root); err != nil {
		return err
	} else if artifacts {
		return errors.Join(base.ErrCorrupt, errors.New("mapping rotation overlaps mapping gc"))
	}
	manifest := catalog.Snapshot()
	if state.StoreID != [16]byte(manifest.StoreUUID) || uint64(state.SegmentSize) != manifest.HardLimits.SegmentSize || state.Covered != manifest.CoveredCommitSeq {
		return errors.Join(base.ErrCorrupt, errors.New("mapping gc identity mismatch"))
	}
	oldMatches := manifest.Generation >= state.BaseGeneration && manifestMatchesMappingSet(manifest, state.Old)
	newMatches := state.BaseGeneration != math.MaxUint64 && manifest.Generation > state.BaseGeneration && manifestMatchesMappingSet(manifest, state.New)
	staging := mapgcstate.StagingRoot(root)
	if oldMatches {
		if err := mapstore.RollbackGeneration(root, staging, generationFromState(state.New, state.Covered), mapHook); err != nil {
			return classifyMappingGCRecovery(err)
		}
		return mapgcstate.Remove(root, stateHook)
	}
	if !newMatches {
		return errors.Join(base.ErrCorrupt, errors.New("mapping gc catalog does not match old or new generation"))
	}
	snapshot := snapshotFromState(manifest.Generation, state.StoreID, state.SegmentSize, state.Covered, state.New)
	reader, _, err := mapstore.OpenVerifiedGeneration(ctx, root, snapshot)
	if err != nil {
		return classifyMappingGCRecovery(err)
	}
	// The root is pinned by radix.OpenReadOnly, so verification needs room for
	// one maximally dense node even though the recovery walk itself is bounded.
	tree, err := radix.OpenReadOnly(reader, state.New.Root, state.Covered, uint64(mapstore.DenseNodeSize)+64)
	if err == nil {
		err = tree.Walk(ctx, func(model.ID, recordlog.VAddr) error { return nil })
	}
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	} else {
		err = errors.Join(err, closeErr)
	}
	if err != nil {
		return classifyMappingGCRecovery(err)
	}
	if err := mapstore.RetireGeneration(root, generationFromState(state.Old, state.Covered), manifest.Generation, mapHook); err != nil {
		return classifyMappingGCRecovery(err)
	}
	if err := mapstore.RemoveGenerationStaging(root, staging); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return mapgcstate.Remove(root, stateHook)
}

func classifyMappingGCRecovery(err error) error {
	if errors.Is(err, mapstore.ErrCorrupt) || errors.Is(err, radix.ErrCorrupt) || errors.Is(err, mapgcstate.ErrCorrupt) {
		return errors.Join(base.ErrCorrupt, err)
	}
	if errors.Is(err, mapstore.ErrRecoveryRequired) {
		return errors.Join(base.ErrRecoveryRequired, err)
	}
	return err
}

func manifestMatchesMappingSet(manifest storecatalog.Manifest, set mapgcstate.FileSet) bool {
	if manifest.ActiveMapSegmentID != set.Active || manifest.NextMapSegmentID != set.Next || manifest.MappingRoot != set.Root || len(manifest.SealedMapSegments) != len(set.Sealed) {
		return false
	}
	for index, ref := range set.Sealed {
		if manifest.SealedMapSegments[index].SegmentID != ref.SegmentID || manifest.SealedMapSegments[index].ValidEnd != ref.ValidEnd {
			return false
		}
	}
	return true
}

func generationFromState(set mapgcstate.FileSet, covered model.CommitSeq) mapstore.Generation {
	return mapstore.Generation{
		SealedSegments: append([]mapstore.SegmentRef(nil), set.Sealed...), ActiveSegment: set.Active,
		NextSegment: set.Next, Root: set.Root, Covered: covered,
	}
}

func snapshotFromState(generation uint64, storeID [16]byte, segmentSize uint32, covered model.CommitSeq, set mapgcstate.FileSet) mapstore.CatalogSnapshot {
	return mapstore.CatalogSnapshot{
		Generation: generation, StoreID: mapstore.StoreID(storeID), SegmentSize: segmentSize,
		SealedSegments: append([]mapstore.SegmentRef(nil), set.Sealed...), ActiveSegment: set.Active,
		NextSegment: set.Next, Root: set.Root, Covered: covered,
	}
}

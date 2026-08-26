package engine

import (
	"errors"
	"math"
	"sort"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/maintstate"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

// recoverMaintenance resolves the single irreversible v2 maintenance marker
// before RecordLog opens files. Catalog membership selects the only safe
// direction: present rolls the unstarted retire back; absent finishes physical
// cleanup.
func recoverMaintenance(root string, catalog *storecatalog.Manager, maintenanceHook maintstate.FaultHook, recordLogHook recordlog.FaultHook) error {
	state, found, err := maintstate.LoadWithFaultHook(root, maintenanceHook)
	if errors.Is(err, maintstate.ErrCorrupt) {
		return errors.Join(base.ErrCorrupt, err)
	}
	if err != nil || !found {
		return err
	}
	manifest := catalog.Snapshot()
	if state.Operation != maintstate.DataRetire || state.StoreUUID != manifest.StoreUUID || state.LogID != manifest.RecordLogID {
		return errors.Join(base.ErrCorrupt, errors.New("maintenance identity mismatch"))
	}
	index := sort.Search(len(manifest.SealedDataSegments), func(i int) bool {
		return manifest.SealedDataSegments[i].SegmentID >= state.Source.SegmentID
	})
	present := index < len(manifest.SealedDataSegments) && manifest.SealedDataSegments[index].SegmentID == state.Source.SegmentID
	if present {
		if manifest.Generation != state.BaseGeneration || manifest.SealedDataSegments[index] != state.Source ||
			manifest.CoveredCommitSeq != state.CoveredCommitSeq || manifest.ReplayStart != state.ReplayStart {
			return errors.Join(base.ErrCorrupt, errors.New("maintenance rollback evidence mismatch"))
		}
		return maintstate.RemoveWithFaultHook(root, maintenanceHook)
	}
	if state.BaseGeneration == math.MaxUint64 || manifest.Generation != state.BaseGeneration+1 {
		return errors.Join(base.ErrCorrupt, errors.New("maintenance completion generation mismatch"))
	}
	if err := recordlog.CleanupRetiredSegmentWithFaultHook(root, state.Source.SegmentID, manifest.Generation, recordLogHook); err != nil {
		return err
	}
	return maintstate.RemoveWithFaultHook(root, maintenanceHook)
}

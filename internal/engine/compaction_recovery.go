package engine

import (
	"context"
	"errors"
	"sort"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/compactionstate"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

func recoverCompactionBeforeOpen(root string, catalog *storecatalog.Manager) (*compactionstate.State, error) {
	state, found, err := compactionstate.Load(root)
	if errors.Is(err, compactionstate.ErrCorrupt) {
		return nil, errors.Join(base.ErrCorrupt, err)
	}
	if err != nil || !found {
		return nil, err
	}
	manifest := catalog.Snapshot()
	if state.StoreUUID != manifest.StoreUUID || state.LogID != manifest.RecordLogID || manifest.Generation < state.BaseGeneration {
		return nil, errors.Join(base.ErrCorrupt, errors.New("compaction identity mismatch"))
	}
	inputsPresent := 0
	for _, input := range state.Inputs {
		if containsSealedSegment(manifest, input) {
			inputsPresent++
		}
	}
	outputs := make([]recordlog.SegmentSummary, 0, len(state.OutputIDs))
	for _, id := range state.OutputIDs {
		index := sort.Search(len(manifest.SealedDataSegments), func(i int) bool { return manifest.SealedDataSegments[i].SegmentID >= id })
		if index < len(manifest.SealedDataSegments) && manifest.SealedDataSegments[index].SegmentID == id {
			outputs = append(outputs, manifest.SealedDataSegments[index])
		}
	}
	if inputsPresent != 0 && inputsPresent != len(state.Inputs) {
		return nil, errors.Join(base.ErrCorrupt, errors.New("partially retired compaction inputs"))
	}
	if inputsPresent == 0 {
		for _, input := range state.Inputs {
			if err := recordlog.CleanupRetiredSegment(root, input.SegmentID, manifest.Generation); err != nil {
				return nil, err
			}
		}
		if err := compactionstate.Remove(root); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if len(outputs) == 0 {
		if state.Phase == compactionstate.PhaseReserved {
			if err := recordlog.CleanupUnpublishedCompactionFiles(root, state.OutputIDs); err != nil {
				return nil, err
			}
			if err := compactionstate.Remove(root); err != nil {
				return nil, err
			}
			return nil, nil
		}
		if len(state.Outputs) != 0 {
			return nil, errors.Join(base.ErrCorrupt, errors.New("published compaction outputs missing"))
		}
		return &state, nil
	}
	if state.Phase >= compactionstate.PhaseOutputsPublished && !equalSegmentSummaries(outputs, state.Outputs) {
		return nil, errors.Join(base.ErrCorrupt, errors.New("partial compaction output publication"))
	}
	state.Outputs = outputs
	state.Phase = compactionstate.PhaseOutputsPublished
	return &state, nil
}

func equalSegmentSummaries(left, right []recordlog.SegmentSummary) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (s *Store) resumeCompaction(ctx context.Context, state compactionstate.State) error {
	var relocated SegmentRelocationResult
	// Open Batches do not survive restart. Payload equality reconstructs the
	// exact source/output pairing when interrupted compaction copied multiple
	// unpublished versions of the same RecordID.
	if err := s.publishCompactionOutputs(ctx, state.Inputs, state.Outputs, nil, true, &relocated); err != nil {
		return err
	}
	proofs, err := s.checkpointAndProveRetirements(ctx, state.Inputs, relocated.LastCommitSeq)
	if err != nil {
		return err
	}
	installed, err := s.installCompactionRetirement(state.Inputs, proofs)
	if err != nil {
		return err
	}
	state.Phase = compactionstate.PhaseInputsRetired
	if err := compactionstate.Update(s.root, state); err != nil {
		return err
	}
	if err := s.maintenance.FinalizeCompactionRetirement(ctx, state.Inputs, installed.Generation); err != nil {
		return err
	}
	return compactionstate.Remove(s.root)
}

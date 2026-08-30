package engine

import (
	"context"
	"errors"
	"sort"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/compactionstate"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
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
	outputByID := make(map[model.ID]recordlog.RecordRef)
	for _, output := range state.Outputs {
		if err := s.maintenance.ScanSegment(ctx, output.SegmentID, func(scanned recordlog.AppendResult, payload []byte) error {
			typ, err := recordcodec.TypeOf(payload)
			if err != nil {
				return errors.Join(base.ErrCorrupt, err)
			}
			if typ != recordcodec.RecordTypePut {
				return errors.Join(base.ErrCorrupt, errors.New("non-put record in compaction output"))
			}
			put, err := recordcodec.DecodePut(payload, s.limits.MaxValueSize)
			if err != nil {
				return errors.Join(base.ErrCorrupt, err)
			}
			if _, duplicate := outputByID[put.RecordID]; duplicate {
				return errors.Join(base.ErrCorrupt, errors.New("duplicate compaction output record"))
			}
			ref, err := scanned.Ref()
			if err != nil {
				return err
			}
			outputByID[put.RecordID] = ref
			return nil
		}); err != nil {
			return err
		}
	}
	var pending []copiedRecord
	for _, input := range state.Inputs {
		if err := s.maintenance.ScanSegment(ctx, input.SegmentID, func(scanned recordlog.AppendResult, payload []byte) error {
			typ, err := recordcodec.TypeOf(payload)
			if err != nil {
				return errors.Join(base.ErrCorrupt, err)
			}
			if typ != recordcodec.RecordTypePut {
				return nil
			}
			put, err := recordcodec.DecodePut(payload, s.limits.MaxValueSize)
			if err != nil {
				return errors.Join(base.ErrCorrupt, err)
			}
			current, exists, err := s.mapping.LookupRef(put.RecordID)
			if err != nil {
				return err
			}
			if !exists || current.Addr != scanned.Addr {
				return nil
			}
			ref, ok := outputByID[put.RecordID]
			if !ok {
				return errors.Join(base.ErrCorrupt, errors.New("live input absent from compaction output"))
			}
			pending = append(pending, copiedRecord{id: put.RecordID, oldAddr: scanned.Addr, newRef: ref, valueBytes: uint64(len(put.Value))})
			return nil
		}); err != nil {
			return err
		}
	}
	var relocated SegmentRelocationResult
	if err := s.publishCopiedRecords(ctx, pending, &relocated, s.gcBytesPerSecond.Load()); err != nil {
		return err
	}
	proofs, err := s.checkpointAndProveRetirements(ctx, state.Inputs, relocated.LastCommitSeq)
	if err != nil {
		return err
	}
	_ = proofs
	installed, err := s.installCompactionRetirement(state.Inputs)
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

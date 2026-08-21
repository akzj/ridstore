package recovery

import (
	"crypto/sha256"
	"fmt"
	"math"

	"github.com/akzj/ridstore/internal/allocator"
	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/mapping/api"
	"github.com/akzj/ridstore/internal/mapping/memory"
	"github.com/akzj/ridstore/internal/segment"
)

type BatchState uint8

const (
	BatchCommitted BatchState = iota + 1
	BatchAborted
)

type BatchStatus struct {
	State     BatchState
	CommitSeq base.CommitSeq
}

type Result struct {
	Mapping                      api.Mapping
	NextFrameSeq                 base.FrameSeq
	NextCommitSeq                base.CommitSeq
	ReservedIDHighExclusive      uint64
	ReservedBatchIDHighExclusive uint64
	Statuses                     map[base.BatchID]BatchStatus
}

type putRecord struct {
	RecordID     base.ID
	BatchID      base.BatchID
	FrameSeq     base.FrameSeq
	Bytes        uint64
	PhysicalSize uint64
	ValueDigest  [sha256.Size]byte
}

func RecoverPhase1(manifest storeformat.Manifest, active *segment.ActiveData) (Result, error) {
	if active == nil || manifest.MappingRoot != 0 || len(manifest.SealedDataSegments) != 0 || manifest.ReplayStart.SegmentID() != active.SegmentID() {
		return Result{}, fmt.Errorf("phase 1 recovery topology: %w", base.ErrUnsupported)
	}
	mapping, err := memory.New(api.Snapshot{CoveredCommitSeq: manifest.CoveredCommitSeq})
	if err != nil {
		return Result{}, err
	}
	return RecoverInto(manifest, nil, active, mapping)
}

func Recover(manifest storeformat.Manifest, sealed []*segment.SealedData, active *segment.ActiveData) (Result, error) {
	mapping, err := memory.New(api.Snapshot{CoveredCommitSeq: manifest.CoveredCommitSeq})
	if err != nil {
		return Result{}, err
	}
	return RecoverInto(manifest, sealed, active, mapping)
}

func RecoverInto(manifest storeformat.Manifest, sealed []*segment.SealedData, active *segment.ActiveData, mapping api.Mapping) (Result, error) {
	if active == nil || mapping == nil || mapping.CoveredCommitSeq() != manifest.CoveredCommitSeq || len(sealed) != len(manifest.SealedDataSegments) {
		return Result{}, fmt.Errorf("mapping recovery configuration: %w", base.ErrInvalidConfig)
	}
	for i := range sealed {
		if sealed[i] == nil || uint32(sealed[i].SegmentID()) != manifest.SealedDataSegments[i].FileID {
			return Result{}, fmt.Errorf("sealed recovery order: %w", base.ErrCorrupt)
		}
	}
	replaySegment := manifest.ReplayStart.SegmentID()
	if replaySegment > active.SegmentID() {
		return Result{}, fmt.Errorf("replay start after active segment: %w", base.ErrCorrupt)
	}
	result := Result{
		Mapping: mapping, NextFrameSeq: manifest.NextFrameSeq, NextCommitSeq: manifest.NextCommitSeq,
		ReservedIDHighExclusive:      manifest.ReservedIDHighExclusive,
		ReservedBatchIDHighExclusive: manifest.ReservedBatchIDHighExclusive,
		Statuses:                     make(map[base.BatchID]BatchStatus),
	}
	puts := make(map[base.VAddr]putRecord)
	parts := make(map[base.BatchID][]storeformat.Frame)
	lastCommit := manifest.CoveredCommitSeq
	replayOffset := uint64(manifest.ReplayStart.Offset())
	visit := func(addr base.VAddr, frame storeformat.Frame) error {
		if frame.FrameSeq >= result.NextFrameSeq {
			if frame.FrameSeq == base.FrameSeq(math.MaxUint64) {
				return fmt.Errorf("frame sequence exhausted on disk: %w", base.ErrCorrupt)
			}
			result.NextFrameSeq = frame.FrameSeq + 1
		}
		if frame.Type == storeformat.FrameTypePutRecord {
			physicalSize, err := base.Align8(storeformat.FrameHeaderSize + uint64(len(frame.Payload)))
			if err != nil {
				return fmt.Errorf("put physical size: %w", base.ErrCorrupt)
			}
			puts[addr] = putRecord{
				RecordID: frame.RecordID, BatchID: frame.BatchID, FrameSeq: frame.FrameSeq,
				Bytes: uint64(len(frame.Payload)), PhysicalSize: physicalSize, ValueDigest: sha256.Sum256(frame.Payload),
			}
		}
		if addr.SegmentID() < replaySegment || (addr.SegmentID() == replaySegment && uint64(addr.Offset()) < replayOffset) {
			return nil
		}
		switch frame.Type {
		case storeformat.FrameTypeCommitPart, storeformat.FrameTypeRelocationPart:
			parts[frame.BatchID] = append(parts[frame.BatchID], frame)
		case storeformat.FrameTypeCommitSeal:
			if _, exists := result.Statuses[frame.BatchID]; exists {
				return fmt.Errorf("duplicate terminal batch state: %w", base.ErrCorrupt)
			}
			decoded, err := storeformat.ValidateDescriptorFrames(storeformat.DescriptorCommit, parts[frame.BatchID], frame, uint32(manifest.HardLimits.MaxBatchMutations))
			if err != nil {
				return err
			}
			delete(parts, frame.BatchID)
			if decoded.Seal.CommitSeq <= lastCommit || decoded.Seal.CommitSeq == base.CommitSeq(math.MaxUint64) {
				return fmt.Errorf("commit sequence regression or exhaustion: %w", base.ErrCorrupt)
			}
			changes := make([]api.Change, len(decoded.Entries))
			var logicalBytes uint64
			for i, entry := range decoded.Entries {
				changes[i].RecordID = entry.RecordID
				if entry.Operation == storeformat.MutationPut {
					record, ok := puts[entry.NewVAddr]
					if !ok || record.RecordID != entry.RecordID || record.BatchID != frame.BatchID || record.FrameSeq >= frame.FrameSeq {
						return fmt.Errorf("commit points to invalid PutRecord: %w", base.ErrCorrupt)
					}
					changes[i].NewAddr = entry.NewVAddr
					logicalBytes, err = base.AddUint64(logicalBytes, record.Bytes)
					if err != nil {
						return fmt.Errorf("commit logical bytes: %w", base.ErrCorrupt)
					}
				}
			}
			if logicalBytes != decoded.Seal.LogicalPayloadBytes {
				return fmt.Errorf("commit logical payload bytes mismatch: %w", base.ErrCorrupt)
			}
			applied, err := mapping.Apply(decoded.Seal.CommitSeq, api.ApplyUserCommit, changes)
			if err != nil || applied.Applied != uint32(len(changes)) || applied.Skipped != 0 {
				return fmt.Errorf("replay mapping apply: %w", base.ErrCorrupt)
			}
			lastCommit = decoded.Seal.CommitSeq
			if decoded.Seal.CommitSeq >= result.NextCommitSeq {
				result.NextCommitSeq = decoded.Seal.CommitSeq + 1
			}
			result.Statuses[frame.BatchID] = BatchStatus{State: BatchCommitted, CommitSeq: decoded.Seal.CommitSeq}
		case storeformat.FrameTypeRelocationSeal:
			if _, exists := result.Statuses[frame.BatchID]; exists {
				return fmt.Errorf("duplicate terminal batch state: %w", base.ErrCorrupt)
			}
			decoded, err := storeformat.ValidateDescriptorFrames(storeformat.DescriptorRelocation, parts[frame.BatchID], frame, uint32(manifest.HardLimits.MaxBatchMutations))
			if err != nil {
				return err
			}
			delete(parts, frame.BatchID)
			if decoded.Seal.CommitSeq <= lastCommit || decoded.Seal.CommitSeq == base.CommitSeq(math.MaxUint64) {
				return fmt.Errorf("relocation sequence regression or exhaustion: %w", base.ErrCorrupt)
			}
			changes := make([]api.Change, len(decoded.Entries))
			var logicalBytes uint64
			for i, entry := range decoded.Entries {
				oldRecord, oldOK := puts[entry.ExpectedOldAddr]
				newRecord, newOK := puts[entry.NewVAddr]
				if !oldOK || !newOK || oldRecord.RecordID != entry.RecordID || newRecord.RecordID != entry.RecordID ||
					oldRecord.BatchID == 0 || oldRecord.BatchID != newRecord.BatchID || frame.BatchID == oldRecord.BatchID ||
					oldRecord.Bytes != newRecord.Bytes || oldRecord.PhysicalSize != newRecord.PhysicalSize || oldRecord.ValueDigest != newRecord.ValueDigest ||
					oldRecord.FrameSeq >= frame.FrameSeq || newRecord.FrameSeq >= frame.FrameSeq {
					return fmt.Errorf("relocation points to mismatched PutRecord: %w", base.ErrCorrupt)
				}
				logicalBytes, err = base.AddUint64(logicalBytes, newRecord.Bytes)
				if err != nil {
					return fmt.Errorf("relocation logical bytes: %w", base.ErrCorrupt)
				}
				changes[i] = api.Change{RecordID: entry.RecordID, ExpectedOldAddr: entry.ExpectedOldAddr, NewAddr: entry.NewVAddr}
			}
			if logicalBytes != decoded.Seal.LogicalPayloadBytes {
				return fmt.Errorf("relocation logical payload bytes mismatch: %w", base.ErrCorrupt)
			}
			applied, err := mapping.Apply(decoded.Seal.CommitSeq, api.ApplyRelocation, changes)
			if err != nil || applied.Applied+applied.Skipped != uint32(len(changes)) {
				return fmt.Errorf("replay relocation apply: %w", base.ErrCorrupt)
			}
			lastCommit = decoded.Seal.CommitSeq
			if decoded.Seal.CommitSeq >= result.NextCommitSeq {
				result.NextCommitSeq = decoded.Seal.CommitSeq + 1
			}
			result.Statuses[frame.BatchID] = BatchStatus{State: BatchCommitted, CommitSeq: decoded.Seal.CommitSeq}
		case storeformat.FrameTypeBatchAbort:
			if _, exists := result.Statuses[frame.BatchID]; exists {
				return fmt.Errorf("abort and commit for same batch: %w", base.ErrCorrupt)
			}
			result.Statuses[frame.BatchID] = BatchStatus{State: BatchAborted}
		case storeformat.FrameTypeIDReserve, storeformat.FrameTypeBatchIDReserve:
			payload, err := storeformat.DecodeReservePayload(frame.Payload)
			if err != nil {
				return err
			}
			if frame.Type == storeformat.FrameTypeIDReserve {
				result.ReservedIDHighExclusive, err = allocator.AdvanceRecovered(allocator.RecordID, manifest.HardLimits.IDReserveSize, result.ReservedIDHighExclusive, payload)
			} else {
				result.ReservedBatchIDHighExclusive, err = allocator.AdvanceRecovered(allocator.BatchID, manifest.HardLimits.BatchIDReserveSize, result.ReservedBatchIDHighExclusive, payload)
			}
			if err != nil {
				return err
			}
		case storeformat.FrameTypeSegmentSeal:
			if len(parts) != 0 {
				return fmt.Errorf("incomplete descriptor at sealed boundary: %w", base.ErrCorrupt)
			}
		}
		return nil
	}
	for _, item := range sealed {
		if err := item.Scan(visit); err != nil {
			return Result{}, err
		}
	}
	if err := active.Scan(visit); err != nil {
		return Result{}, err
	}
	return result, nil
}

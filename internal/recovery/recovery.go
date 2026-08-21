package recovery

import (
	"crypto/sha256"
	"errors"
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
	StatusOrder                  []base.BatchID
	TerminalStatusCount          uint64
}

type DataScanner interface {
	SegmentID() base.DataSegmentID
	Scan(func(base.VAddr, storeformat.Frame) error) error
	ReadFrame(base.VAddr) (storeformat.Frame, error)
}

type putRecord struct {
	RecordID     base.ID
	BatchID      base.BatchID
	FrameSeq     base.FrameSeq
	Bytes        uint64
	PhysicalSize uint64
	ValueDigest  [sha256.Size]byte
}

func RecoverPhase1(manifest storeformat.Manifest, active *segment.ActiveData, statusLimit uint64) (Result, error) {
	if active == nil || manifest.MappingRoot != 0 || len(manifest.SealedDataSegments) != 0 || manifest.ReplayStart.SegmentID() != active.SegmentID() {
		return Result{}, fmt.Errorf("phase 1 recovery topology: %w", base.ErrUnsupported)
	}
	mapping, err := memory.New(api.Snapshot{CoveredCommitSeq: manifest.CoveredCommitSeq})
	if err != nil {
		return Result{}, err
	}
	return RecoverInto(manifest, nil, active, mapping, statusLimit)
}

func Recover(manifest storeformat.Manifest, sealed []*segment.SealedData, active *segment.ActiveData, statusLimit uint64) (Result, error) {
	mapping, err := memory.New(api.Snapshot{CoveredCommitSeq: manifest.CoveredCommitSeq})
	if err != nil {
		return Result{}, err
	}
	return RecoverInto(manifest, sealed, active, mapping, statusLimit)
}

func RecoverInto(manifest storeformat.Manifest, sealed []*segment.SealedData, active *segment.ActiveData, mapping api.Mapping, statusLimit uint64) (Result, error) {
	if active == nil {
		return Result{}, fmt.Errorf("mapping recovery active scanner: %w", base.ErrInvalidConfig)
	}
	scanners := make([]DataScanner, len(sealed))
	for i := range sealed {
		if sealed[i] == nil {
			return Result{}, fmt.Errorf("mapping recovery sealed scanner: %w", base.ErrInvalidConfig)
		}
		scanners[i] = sealed[i]
	}
	return RecoverIntoScanners(manifest, scanners, active, mapping, statusLimit)
}

// RecoverIntoScanners replays validated read-only scanners into Mapping. It is
// shared by normal recovery and the offline verifier; it never mutates files.
func RecoverIntoScanners(manifest storeformat.Manifest, sealed []DataScanner, active DataScanner, mapping api.Mapping, statusLimit uint64) (Result, error) {
	if active == nil || mapping == nil || statusLimit == 0 || mapping.CoveredCommitSeq() != manifest.CoveredCommitSeq || len(sealed) != len(manifest.SealedDataSegments) {
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
	var (
		partBatchID base.BatchID
		parts       []storeformat.Frame
	)
	recordTerminal := func(id base.BatchID, status BatchStatus) error {
		if _, exists := result.Statuses[id]; exists {
			return fmt.Errorf("duplicate terminal batch state: %w", base.ErrCorrupt)
		}
		if result.TerminalStatusCount >= statusLimit {
			return fmt.Errorf("replay contains more than %d terminal batch states: %w", statusLimit, base.ErrStatusCapacity)
		}
		result.TerminalStatusCount++
		result.Statuses[id] = status
		result.StatusOrder = append(result.StatusOrder, id)
		return nil
	}
	readers := make(map[base.DataSegmentID]DataScanner, len(sealed)+1)
	for _, scanner := range sealed {
		readers[scanner.SegmentID()] = scanner
	}
	readers[active.SegmentID()] = active
	readPut := func(addr base.VAddr) (putRecord, error) {
		reader := readers[addr.SegmentID()]
		if reader == nil {
			return putRecord{}, fmt.Errorf("referenced PutRecord segment %d is absent: %w", addr.SegmentID(), base.ErrCorrupt)
		}
		frame, err := reader.ReadFrame(addr)
		if err != nil {
			return putRecord{}, fmt.Errorf("read referenced PutRecord %x: %w", addr, errors.Join(err, base.ErrCorrupt))
		}
		if frame.Type != storeformat.FrameTypePutRecord {
			return putRecord{}, fmt.Errorf("referenced frame %x is not PutRecord: %w", addr, base.ErrCorrupt)
		}
		physicalSize, err := base.Align8(storeformat.FrameHeaderSize + uint64(len(frame.Payload)))
		if err != nil {
			return putRecord{}, fmt.Errorf("put physical size: %w", base.ErrCorrupt)
		}
		return putRecord{
			RecordID: frame.RecordID, BatchID: frame.BatchID, FrameSeq: frame.FrameSeq,
			Bytes: uint64(len(frame.Payload)), PhysicalSize: physicalSize, ValueDigest: sha256.Sum256(frame.Payload),
		}, nil
	}
	lastCommit := manifest.CoveredCommitSeq
	replayOffset := uint64(manifest.ReplayStart.Offset())
	visit := func(addr base.VAddr, frame storeformat.Frame) error {
		if addr.SegmentID() < replaySegment || (addr.SegmentID() == replaySegment && uint64(addr.Offset()) < replayOffset) {
			return nil
		}
		if frame.FrameSeq >= result.NextFrameSeq {
			if frame.FrameSeq == base.FrameSeq(math.MaxUint64) {
				return fmt.Errorf("frame sequence exhausted on disk: %w", base.ErrCorrupt)
			}
			result.NextFrameSeq = frame.FrameSeq + 1
		}
		if len(parts) != 0 {
			continuesDescriptor := (frame.Type == storeformat.FrameTypeCommitPart || frame.Type == storeformat.FrameTypeRelocationPart ||
				frame.Type == storeformat.FrameTypeCommitSeal || frame.Type == storeformat.FrameTypeRelocationSeal) && frame.BatchID == partBatchID
			if !continuesDescriptor {
				return fmt.Errorf("non-contiguous descriptor frames: %w", base.ErrCorrupt)
			}
		}
		switch frame.Type {
		case storeformat.FrameTypeCommitPart, storeformat.FrameTypeRelocationPart:
			if len(parts) != 0 && frame.BatchID != partBatchID {
				return fmt.Errorf("interleaved descriptor batches: %w", base.ErrCorrupt)
			}
			if uint64(len(parts)) >= manifest.HardLimits.MaxBatchMutations {
				return fmt.Errorf("descriptor part count exceeds mutation limit: %w", base.ErrCorrupt)
			}
			partBatchID = frame.BatchID
			parts = append(parts, frame)
		case storeformat.FrameTypeCommitSeal:
			decoded, err := storeformat.ValidateDescriptorFrames(storeformat.DescriptorCommit, parts, frame, uint32(manifest.HardLimits.MaxBatchMutations))
			if err != nil {
				return err
			}
			parts, partBatchID = parts[:0], 0
			if decoded.Seal.CommitSeq <= lastCommit || decoded.Seal.CommitSeq == base.CommitSeq(math.MaxUint64) {
				return fmt.Errorf("commit sequence regression or exhaustion: %w", base.ErrCorrupt)
			}
			changes := make([]api.Change, len(decoded.Entries))
			var logicalBytes uint64
			for i, entry := range decoded.Entries {
				changes[i].RecordID = entry.RecordID
				if entry.Operation == storeformat.MutationPut {
					record, readErr := readPut(entry.NewVAddr)
					if readErr != nil {
						return readErr
					}
					if record.RecordID != entry.RecordID || record.BatchID != frame.BatchID || record.FrameSeq >= frame.FrameSeq {
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
			if err := recordTerminal(frame.BatchID, BatchStatus{State: BatchCommitted, CommitSeq: decoded.Seal.CommitSeq}); err != nil {
				return err
			}
		case storeformat.FrameTypeRelocationSeal:
			decoded, err := storeformat.ValidateDescriptorFrames(storeformat.DescriptorRelocation, parts, frame, uint32(manifest.HardLimits.MaxBatchMutations))
			if err != nil {
				return err
			}
			parts, partBatchID = parts[:0], 0
			if decoded.Seal.CommitSeq <= lastCommit || decoded.Seal.CommitSeq == base.CommitSeq(math.MaxUint64) {
				return fmt.Errorf("relocation sequence regression or exhaustion: %w", base.ErrCorrupt)
			}
			changes := make([]api.Change, len(decoded.Entries))
			var logicalBytes uint64
			for i, entry := range decoded.Entries {
				oldRecord, readErr := readPut(entry.ExpectedOldAddr)
				if readErr != nil {
					return readErr
				}
				newRecord, readErr := readPut(entry.NewVAddr)
				if readErr != nil {
					return readErr
				}
				if oldRecord.RecordID != entry.RecordID || newRecord.RecordID != entry.RecordID ||
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
			if err := recordTerminal(frame.BatchID, BatchStatus{State: BatchCommitted, CommitSeq: decoded.Seal.CommitSeq}); err != nil {
				return err
			}
		case storeformat.FrameTypeBatchAbort:
			if err := recordTerminal(frame.BatchID, BatchStatus{State: BatchAborted}); err != nil {
				return err
			}
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
		if item.SegmentID() < replaySegment {
			continue
		}
		if err := item.Scan(visit); err != nil {
			return Result{}, err
		}
	}
	if err := active.Scan(visit); err != nil {
		return Result{}, err
	}
	return result, nil
}

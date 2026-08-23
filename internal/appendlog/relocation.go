package appendlog

import (
	"fmt"
	"math"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/segment"
)

type RelocationEntry struct {
	RecordID        base.ID
	ExpectedOldAddr base.VAddr
	NewAddr         base.VAddr
}

type RelocationPrepared struct {
	BatchID             base.BatchID
	Entries             []RelocationEntry
	LogicalPayloadBytes uint64
}

const (
	PointRelocationPartWritten failpoint.Point = "appendlog.relocation-part-written"
	PointRelocationSealWritten failpoint.Point = "appendlog.relocation-seal-written"
	PointRelocationSynced      failpoint.Point = "appendlog.relocation-synced"
)

// AppendRelocation appends one relocation descriptor and durably orders it in
// the same FrameSeq/CommitSeq stream as user commits.
func (l *Log) AppendRelocation(prepared RelocationPrepared, commitSeq base.CommitSeq) (CommitAppendResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ready(); err != nil {
		return CommitAppendResult{}, err
	}
	plan, err := l.buildRelocationPlan(prepared, commitSeq, l.nextFrameSeq)
	if err != nil {
		return CommitAppendResult{}, err
	}
	if err := l.ensureCapacityLocked(plan.bytes); err != nil {
		return CommitAppendResult{}, err
	}
	// Rotation consumes a FrameSeq, so rebuild against the new sequence origin.
	plan, err = l.buildRelocationPlan(prepared, commitSeq, l.nextFrameSeq)
	if err != nil {
		return CommitAppendResult{}, err
	}
	if plan.bytes+segmentSealReserve > l.active.Remaining() {
		return CommitAppendResult{}, segment.ErrFull
	}
	result := plan.result
	if l.hook == nil {
		written, appendErr := l.appendFrameBatchLocked(plan.frames)
		result.SealStarted = written > plan.bytes-descriptorSealFrameSize
		if appendErr != nil {
			return result, appendErr
		}
	} else {
		for i, frame := range plan.frames {
			_, written, appendErr := l.appendLocked(frame)
			if appendErr != nil {
				if i == len(plan.frames)-1 && written != 0 {
					result.SealStarted = true
				}
				return result, appendErr
			}
			if i == len(plan.frames)-1 {
				result.SealStarted = true
				if err := failpoint.Hit(l.hook, PointRelocationSealWritten); err != nil {
					l.faulted = true
					return result, err
				}
			} else if err := failpoint.Hit(l.hook, PointRelocationPartWritten); err != nil {
				l.faulted = true
				return result, err
			}
		}
	}
	if err := l.active.Sync(); err != nil {
		l.faulted = true
		return result, err
	}
	if err := failpoint.Hit(l.hook, PointRelocationSynced); err != nil {
		l.faulted = true
		return result, err
	}
	return result, nil
}

func (l *Log) buildRelocationPlan(prepared RelocationPrepared, commitSeq base.CommitSeq, next base.FrameSeq) (commitPlan, error) {
	if prepared.BatchID == 0 || commitSeq == 0 || len(prepared.Entries) == 0 || uint64(len(prepared.Entries)) > math.MaxUint32 {
		return commitPlan{}, fmt.Errorf("relocation identity or count: %w", base.ErrInvalidConfig)
	}
	partPayloads, err := l.relocationParts(prepared.Entries)
	if err != nil {
		return commitPlan{}, err
	}
	partCount := len(partPayloads)
	firstPartSeq := next
	lastPartSeq := next + base.FrameSeq(partCount) - 1
	if lastPartSeq < firstPartSeq || lastPartSeq == base.FrameSeq(math.MaxUint64) {
		return commitPlan{}, base.ErrGenerationExhausted
	}
	sealSeq := lastPartSeq + 1
	if sealSeq == 0 || sealSeq == base.FrameSeq(math.MaxUint64) {
		return commitPlan{}, base.ErrGenerationExhausted
	}
	sealPayload, err := storeformat.EncodeDescriptorSealPayload(storeformat.DescriptorSeal{
		CommitSeq: commitSeq, PartCount: uint32(partCount), MutationCount: uint32(len(prepared.Entries)),
		LogicalPayloadBytes: prepared.LogicalPayloadBytes, FirstPartFrameSeq: firstPartSeq, LastPartFrameSeq: lastPartSeq,
	}, partPayloads)
	if err != nil {
		return commitPlan{}, err
	}
	frames := make([]storeformat.Frame, 0, partCount+1)
	for i, payload := range partPayloads {
		frames = append(frames, storeformat.Frame{Type: storeformat.FrameTypeRelocationPart, FrameSeq: next + base.FrameSeq(i), BatchID: prepared.BatchID, Payload: payload})
	}
	frames = append(frames, storeformat.Frame{Type: storeformat.FrameTypeRelocationSeal, FrameSeq: sealSeq, BatchID: prepared.BatchID, Payload: sealPayload[:]})
	var totalBytes uint64
	for _, frame := range frames {
		size, err := storeformat.EncodedFrameSize(frame, l.maxFramePayload)
		if err != nil {
			return commitPlan{}, err
		}
		totalBytes, err = base.AddUint64(totalBytes, size)
		if err != nil {
			return commitPlan{}, err
		}
	}
	return commitPlan{frames: frames, bytes: totalBytes, result: CommitAppendResult{SealFrameSeq: sealSeq}}, nil
}

func (l *Log) relocationParts(entries []RelocationEntry) ([][]byte, error) {
	encoded := make([]storeformat.MutationEntry, len(entries))
	var previous base.ID
	for i, entry := range entries {
		if entry.RecordID == 0 || (previous != 0 && entry.RecordID <= previous) || entry.ExpectedOldAddr == 0 || entry.NewAddr == 0 {
			return nil, fmt.Errorf("relocation entry: %w", base.ErrInvalidConfig)
		}
		encoded[i] = storeformat.MutationEntry{
			RecordID: entry.RecordID, Operation: storeformat.MutationRelocate,
			NewVAddr: entry.NewAddr, ExpectedOldAddr: entry.ExpectedOldAddr,
		}
		previous = entry.RecordID
	}
	perPart := int(l.maxPartPayload / storeformat.MutationEntrySize)
	parts := make([][]byte, 0, (len(encoded)+perPart-1)/perPart)
	for start := 0; start < len(encoded); start += perPart {
		end := start + perPart
		if end > len(encoded) {
			end = len(encoded)
		}
		payload, err := storeformat.EncodeMutationEntries(storeformat.DescriptorRelocation, encoded[start:end])
		if err != nil {
			return nil, err
		}
		parts = append(parts, payload)
	}
	return parts, nil
}

package appendlog

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/batch"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/segment"
)

type Log struct {
	mu              sync.Mutex
	active          *segment.ActiveData
	nextFrameSeq    base.FrameSeq
	maxFramePayload uint64
	maxPartPayload  uint64
	faulted         bool
	hook            failpoint.Hook
	rotator         Rotator
}

type Rotator interface {
	Rotate(*segment.ActiveData, base.FrameSeq) (*segment.ActiveData, error)
}

const (
	PointPutWritten        failpoint.Point = "appendlog.put-written"
	PointAbortPrepared     failpoint.Point = "appendlog.abort-prepared"
	PointAbortWritten      failpoint.Point = "appendlog.abort-written"
	PointReservePrepared   failpoint.Point = "appendlog.reserve-prepared"
	PointReserveWritten    failpoint.Point = "appendlog.reserve-written"
	PointReserveSynced     failpoint.Point = "appendlog.reserve-synced"
	PointCommitPartWritten failpoint.Point = "appendlog.commit-part-written"
	PointCommitSealWritten failpoint.Point = "appendlog.commit-seal-written"
	PointCommitSynced      failpoint.Point = "appendlog.commit-synced"
)

type CommitAppendResult struct {
	SealFrameSeq base.FrameSeq
	SealStarted  bool
}

type Barrier struct {
	SegmentID    base.DataSegmentID
	End          uint64
	LastFrameSeq base.FrameSeq
	NextFrameSeq base.FrameSeq
}

type putRequest struct {
	Context  context.Context
	BatchID  base.BatchID
	RecordID base.ID
	Value    []byte
}

type putAppendResult struct {
	Addr    base.VAddr
	Seq     base.FrameSeq
	Written uint64
	Err     error
}

type commitPlan struct {
	frames []storeformat.Frame
	bytes  uint64
	result CommitAppendResult
}

func New(active *segment.ActiveData, nextFrameSeq base.FrameSeq, maxFramePayload, maxPartPayload uint64) (*Log, error) {
	return NewWithHook(active, nextFrameSeq, maxFramePayload, maxPartPayload, nil)
}

func NewWithHook(active *segment.ActiveData, nextFrameSeq base.FrameSeq, maxFramePayload, maxPartPayload uint64, hook failpoint.Hook) (*Log, error) {
	return NewWithRotator(active, nextFrameSeq, maxFramePayload, maxPartPayload, hook, nil)
}

func NewWithRotator(active *segment.ActiveData, nextFrameSeq base.FrameSeq, maxFramePayload, maxPartPayload uint64, hook failpoint.Hook, rotator Rotator) (*Log, error) {
	if active == nil || nextFrameSeq == 0 || maxFramePayload < storeformat.DescriptorSealSize || maxPartPayload < storeformat.MutationEntrySize || maxPartPayload > maxFramePayload {
		return nil, fmt.Errorf("append log configuration: %w", base.ErrInvalidConfig)
	}
	maxPartPayload -= maxPartPayload % storeformat.MutationEntrySize
	return &Log{active: active, nextFrameSeq: nextFrameSeq, maxFramePayload: maxFramePayload, maxPartPayload: maxPartPayload, hook: hook, rotator: rotator}, nil
}

func (l *Log) AppendPut(ctx context.Context, batchID base.BatchID, id base.ID, value []byte) (base.VAddr, base.FrameSeq, uint64, error) {
	results := l.appendPutGroup([]putRequest{{Context: ctx, BatchID: batchID, RecordID: id, Value: value}})
	if len(results) != 1 {
		return 0, 0, 0, base.ErrCorrupt
	}
	result := results[0]
	return result.Addr, result.Seq, result.Written, result.Err
}

// appendPutGroup appends adjacent Put requests using one write per maximal
// segment-local group. Canceled or invalid requests consume no FrameSeq. A
// rotation may split the input into more than one physical write.
func (l *Log) appendPutGroup(requests []putRequest) []putAppendResult {
	results := make([]putAppendResult, len(requests))
	if len(requests) == 0 {
		return results
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for start := 0; start < len(requests); {
		for start < len(requests) {
			ctx := requests[start].Context
			if ctx == nil {
				ctx = context.Background()
			}
			if err := ctx.Err(); err != nil {
				results[start].Err = err
				start++
				continue
			}
			break
		}
		if start == len(requests) {
			break
		}
		if err := l.ready(); err != nil {
			for i := start; i < len(results); i++ {
				results[i].Err = err
			}
			break
		}

		// Preflight the first request so rotation happens before assigning its
		// sequence. Rotation itself consumes the SegmentSeal FrameSeq.
		first := requests[start]
		probeSize, err := storeformat.EncodedFrameSize(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: l.nextFrameSeq, BatchID: first.BatchID, RecordID: first.RecordID, Payload: first.Value}, l.maxFramePayload)
		if err != nil {
			results[start].Err = err
			start++
			continue
		}
		if err := l.ensureCapacityLocked(probeSize); err != nil {
			results[start].Err = err
			start++
			if l.faulted {
				for i := start; i < len(results); i++ {
					results[i].Err = segment.ErrPoisoned
				}
				break
			}
			continue
		}

		activeRemaining := l.active.Remaining()
		if activeRemaining < segmentSealReserve {
			l.faulted = true
			err := fmt.Errorf("active data segment lost seal reserve: %w", base.ErrCorrupt)
			for i := start; i < len(results); i++ {
				results[i].Err = err
			}
			break
		}
		remaining := activeRemaining - segmentSealReserve
		frames := make([]storeformat.Frame, 0, len(requests)-start)
		frameSizes := make([]uint64, 0, len(requests)-start)
		indexes := make([]int, 0, len(requests)-start)
		var bytes uint64
		next := start
		for ; next < len(requests); next++ {
			i := next
			ctx := requests[i].Context
			if ctx == nil {
				ctx = context.Background()
			}
			if err := ctx.Err(); err != nil {
				results[i].Err = err
				continue
			}
			if l.nextFrameSeq+base.FrameSeq(len(frames)) < l.nextFrameSeq || l.nextFrameSeq+base.FrameSeq(len(frames)) == base.FrameSeq(math.MaxUint64) {
				results[i].Err = base.ErrGenerationExhausted
				continue
			}
			frame := storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: l.nextFrameSeq + base.FrameSeq(len(frames)), BatchID: requests[i].BatchID, RecordID: requests[i].RecordID, Payload: requests[i].Value}
			size, err := storeformat.EncodedFrameSize(frame, l.maxFramePayload)
			if err != nil {
				results[i].Err = err
				continue
			}
			if size > remaining || bytes > remaining-size {
				break
			}
			frames = append(frames, frame)
			frameSizes = append(frameSizes, size)
			indexes = append(indexes, i)
			bytes += size
		}
		if len(frames) == 0 {
			if next == start {
				l.faulted = true
				err := fmt.Errorf("preflighted put does not fit active data segment: %w", base.ErrCorrupt)
				for i := start; i < len(results); i++ {
					results[i].Err = err
				}
				break
			}
			// Every request before next was canceled or invalid. next is either
			// the first request that needs rotation, or len(requests).
			start = next
			continue
		}
		appended, written, err := l.active.AppendBatch(frames)
		if err != nil {
			l.faulted = true
			if errors.Is(err, segment.ErrFull) {
				err = fmt.Errorf("planned put group no longer fits active data segment: %w", base.ErrCorrupt)
			}
			remainingWritten := written
			for i, index := range indexes {
				frameWritten := frameSizes[i]
				if frameWritten > remainingWritten {
					frameWritten = remainingWritten
				}
				results[index].Written = frameWritten
				results[index].Err = err
				remainingWritten -= frameWritten
			}
			restErr := err
			if l.faulted {
				restErr = segment.ErrPoisoned
			}
			for i := indexes[len(indexes)-1] + 1; i < len(results); i++ {
				results[i].Err = restErr
			}
			break
		}
		l.nextFrameSeq += base.FrameSeq(len(frames))
		for i, index := range indexes {
			results[index] = putAppendResult{Addr: appended[i].Addr, Seq: frames[i].FrameSeq, Written: appended[i].Size}
		}
		for i, index := range indexes {
			if err := failpoint.Hit(l.hook, PointPutWritten); err != nil {
				l.faulted = true
				results[index] = putAppendResult{Written: appended[i].Size, Err: err}
				for restIndex, rest := range indexes[i+1:] {
					results[rest] = putAppendResult{Written: appended[i+1+restIndex].Size, Err: segment.ErrPoisoned}
				}
				for rest := indexes[len(indexes)-1] + 1; rest < len(results); rest++ {
					results[rest].Err = segment.ErrPoisoned
				}
				return results
			}
		}
		start = next
	}
	return results
}

func (l *Log) AppendAbort(ctx context.Context, batchID base.BatchID, payload storeformat.BatchAbortPayload) error {
	encoded, err := storeformat.EncodeBatchAbortPayload(payload)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ready(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	frame := storeformat.Frame{Type: storeformat.FrameTypeBatchAbort, FrameSeq: l.nextFrameSeq, BatchID: batchID, Payload: encoded[:]}
	frameSize, err := storeformat.EncodedFrameSize(frame, l.maxFramePayload)
	if err != nil {
		return err
	}
	if err := l.ensureCapacityLocked(frameSize); err != nil {
		return err
	}
	if err := failpoint.Hit(l.hook, PointAbortPrepared); err != nil {
		return err
	}
	frame.FrameSeq = l.nextFrameSeq
	_, _, err = l.appendLocked(frame)
	if err != nil {
		return err
	}
	if err := failpoint.Hit(l.hook, PointAbortWritten); err != nil {
		l.faulted = true
		return err
	}
	return nil
}

func (l *Log) AppendReserve(ctx context.Context, typ storeformat.FrameType, payload storeformat.ReservePayload) error {
	if typ != storeformat.FrameTypeIDReserve && typ != storeformat.FrameTypeBatchIDReserve {
		return fmt.Errorf("reserve frame type: %w", base.ErrInvalidConfig)
	}
	encoded, err := storeformat.EncodeReservePayload(payload)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ready(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	frame := storeformat.Frame{Type: typ, FrameSeq: l.nextFrameSeq, Payload: encoded[:]}
	frameSize, err := storeformat.EncodedFrameSize(frame, l.maxFramePayload)
	if err != nil {
		return err
	}
	if err := l.ensureCapacityLocked(frameSize); err != nil {
		return err
	}
	if err := failpoint.Hit(l.hook, PointReservePrepared); err != nil {
		return err
	}
	frame.FrameSeq = l.nextFrameSeq
	if _, _, err := l.appendLocked(frame); err != nil {
		return err
	}
	if err := failpoint.Hit(l.hook, PointReserveWritten); err != nil {
		l.faulted = true
		return err
	}
	if err := l.active.Sync(); err != nil {
		l.faulted = true
		return err
	}
	if err := failpoint.Hit(l.hook, PointReserveSynced); err != nil {
		l.faulted = true
		return err
	}
	return nil
}

// AppendCommitGroup appends every descriptor in CommitSeq order and performs
// exactly one sync. Capacity and descriptor encoding are preflighted for the
// entire group, so ErrFull never leaves a partial group on disk.
func (l *Log) AppendCommitGroup(prepared []batch.Prepared, commitSeqs []base.CommitSeq) ([]CommitAppendResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ready(); err != nil {
		return nil, err
	}
	if len(prepared) == 0 || len(prepared) != len(commitSeqs) {
		return nil, fmt.Errorf("commit group size: %w", base.ErrInvalidConfig)
	}
	plans, totalBytes, err := l.buildCommitGroup(prepared, commitSeqs)
	if err != nil {
		return nil, err
	}
	plannedAt := l.nextFrameSeq
	if err := l.ensureCapacityLocked(totalBytes); err != nil {
		return make([]CommitAppendResult, len(prepared)), err
	}
	if l.nextFrameSeq != plannedAt {
		// Rotation consumes one FrameSeq for SegmentSeal, so only that case
		// requires rebuilding descriptors against the new sequence origin.
		plans, _, err = l.buildCommitGroup(prepared, commitSeqs)
		if err != nil {
			return nil, err
		}
	}
	results := make([]CommitAppendResult, len(plans))
	for planIndex, plan := range plans {
		results[planIndex].SealFrameSeq = plan.result.SealFrameSeq
	}
	if l.hook == nil {
		frames := flattenPlanFrames(plans)
		written, appendErr := l.appendFrameBatchLocked(frames)
		var planOffset uint64
		for planIndex, plan := range plans {
			results[planIndex].SealStarted = written > planOffset+plan.bytes-descriptorSealFrameSize
			planOffset += plan.bytes
		}
		if appendErr != nil {
			return results, appendErr
		}
	} else {
		if err := l.appendCommitPlansWithHooksLocked(plans, results); err != nil {
			return results, err
		}
	}
	if err := l.active.Sync(); err != nil {
		l.faulted = true
		return results, err
	}
	if err := failpoint.Hit(l.hook, PointCommitSynced); err != nil {
		l.faulted = true
		return results, err
	}
	return results, nil
}

// appendCommitPlansWithHooksLocked retains per-Frame fault boundaries. Hooks
// are dependency-injected test instrumentation; production uses one batch
// append above so Descriptor Frames share one write syscall.
func (l *Log) appendCommitPlansWithHooksLocked(plans []commitPlan, results []CommitAppendResult) error {
	for planIndex, plan := range plans {
		for frameIndex, frame := range plan.frames {
			_, written, err := l.appendLocked(frame)
			if err != nil {
				if frameIndex == len(plan.frames)-1 && written != 0 {
					results[planIndex].SealStarted = true
				}
				return err
			}
			if frameIndex == len(plan.frames)-1 {
				results[planIndex].SealStarted = true
				if err := failpoint.Hit(l.hook, PointCommitSealWritten); err != nil {
					l.faulted = true
					return err
				}
			} else if err := failpoint.Hit(l.hook, PointCommitPartWritten); err != nil {
				l.faulted = true
				return err
			}
		}
	}
	return nil
}

const (
	segmentSealReserve      = uint64(storeformat.FrameHeaderSize + 64)
	descriptorSealFrameSize = uint64(storeformat.FrameHeaderSize + storeformat.DescriptorSealSize)
)

func flattenPlanFrames(plans []commitPlan) []storeformat.Frame {
	count := 0
	for _, plan := range plans {
		count += len(plan.frames)
	}
	frames := make([]storeformat.Frame, 0, count)
	for _, plan := range plans {
		frames = append(frames, plan.frames...)
	}
	return frames
}

func (l *Log) appendFrameBatchLocked(frames []storeformat.Frame) (uint64, error) {
	_, written, err := l.active.AppendBatch(frames)
	if err == nil {
		l.nextFrameSeq += base.FrameSeq(len(frames))
		return written, nil
	}
	l.faulted = true
	complete := 0
	remaining := written
	for _, frame := range frames {
		size, sizeErr := storeformat.EncodedFrameSize(frame, l.maxFramePayload)
		if sizeErr != nil || remaining < size {
			break
		}
		remaining -= size
		complete++
	}
	l.nextFrameSeq += base.FrameSeq(complete)
	if errors.Is(err, segment.ErrFull) {
		err = fmt.Errorf("preflighted descriptor group does not fit active data segment: %w", base.ErrCorrupt)
	}
	return written, err
}

func (l *Log) buildCommitGroup(prepared []batch.Prepared, commitSeqs []base.CommitSeq) ([]commitPlan, uint64, error) {
	plans := make([]commitPlan, len(prepared))
	next := l.nextFrameSeq
	var totalBytes uint64
	for i := range prepared {
		if prepared[i].BatchID == 0 || commitSeqs[i] == 0 || (i != 0 && commitSeqs[i] <= commitSeqs[i-1]) {
			return nil, 0, fmt.Errorf("commit append identity or order: %w", base.ErrInvalidConfig)
		}
		plan, err := l.buildCommitPlan(prepared[i], commitSeqs[i], next)
		if err != nil {
			return nil, 0, err
		}
		plans[i] = plan
		next = plan.result.SealFrameSeq + 1
		totalBytes, err = base.AddUint64(totalBytes, plan.bytes)
		if err != nil {
			return nil, 0, err
		}
	}
	return plans, totalBytes, nil
}

func (l *Log) buildCommitPlan(prepared batch.Prepared, commitSeq base.CommitSeq, next base.FrameSeq) (commitPlan, error) {
	partPayloads, entries, err := l.commitParts(prepared)
	if err != nil {
		return commitPlan{}, err
	}
	partCount := len(partPayloads)
	if uint64(partCount) > math.MaxUint32 || uint64(len(entries)) > math.MaxUint32 {
		return commitPlan{}, fmt.Errorf("commit descriptor count: %w", base.ErrInvalidConfig)
	}
	firstPartSeq, lastPartSeq := base.FrameSeq(0), base.FrameSeq(0)
	if partCount != 0 {
		firstPartSeq = next
		lastPartSeq = next + base.FrameSeq(partCount) - 1
		if lastPartSeq < firstPartSeq || lastPartSeq == base.FrameSeq(math.MaxUint64) {
			return commitPlan{}, base.ErrGenerationExhausted
		}
	}
	sealSeq := next + base.FrameSeq(partCount)
	if sealSeq < next || sealSeq == 0 || sealSeq == base.FrameSeq(math.MaxUint64) {
		return commitPlan{}, base.ErrGenerationExhausted
	}
	sealPayload, err := storeformat.EncodeDescriptorSealPayload(storeformat.DescriptorSeal{
		CommitSeq: commitSeq, PartCount: uint32(partCount), MutationCount: uint32(len(entries)),
		LogicalPayloadBytes: prepared.LogicalPayloadBytes, FirstPartFrameSeq: firstPartSeq, LastPartFrameSeq: lastPartSeq,
	}, partPayloads)
	if err != nil {
		return commitPlan{}, err
	}
	frames := make([]storeformat.Frame, 0, partCount+1)
	for i, payload := range partPayloads {
		frames = append(frames, storeformat.Frame{Type: storeformat.FrameTypeCommitPart, FrameSeq: next + base.FrameSeq(i), BatchID: prepared.BatchID, Payload: payload})
	}
	frames = append(frames, storeformat.Frame{Type: storeformat.FrameTypeCommitSeal, FrameSeq: sealSeq, BatchID: prepared.BatchID, Payload: sealPayload[:]})
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

func (l *Log) NextFrameSeq() base.FrameSeq {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextFrameSeq
}

func (l *Log) Barrier() (Barrier, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ready(); err != nil {
		return Barrier{}, err
	}
	if err := l.active.Sync(); err != nil {
		l.faulted = true
		return Barrier{}, err
	}
	last := base.FrameSeq(0)
	if l.nextFrameSeq > 1 {
		last = l.nextFrameSeq - 1
	}
	return Barrier{SegmentID: l.active.SegmentID(), End: l.active.End(), LastFrameSeq: last, NextFrameSeq: l.nextFrameSeq}, nil
}

func (l *Log) Faulted() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.faulted
}

func (l *Log) appendLocked(frame storeformat.Frame) (base.VAddr, uint64, error) {
	addr, written, err := l.active.Append(frame)
	if err != nil {
		if errors.Is(err, segment.ErrFull) && l.rotator != nil {
			l.faulted = true
			err = fmt.Errorf("preflighted frame no longer fits active data segment: %w", base.ErrCorrupt)
		} else if !errors.Is(err, segment.ErrFull) {
			l.faulted = true
		}
		return 0, written, err
	}
	l.nextFrameSeq++
	return addr, written, nil
}

func (l *Log) ensureCapacityLocked(required uint64) error {
	if required > ^uint64(0)-segmentSealReserve {
		return l.capacityErrorLocked(required)
	}
	if required+segmentSealReserve <= l.active.Remaining() {
		return nil
	}
	if l.rotator == nil {
		return segment.ErrFull
	}
	if l.active.End() == storeformat.SegmentHeaderSize {
		return l.capacityErrorLocked(required)
	}
	active, err := l.rotator.Rotate(l.active, l.nextFrameSeq)
	if err != nil {
		l.faulted = true
		return err
	}
	l.active = active
	if l.nextFrameSeq == base.FrameSeq(math.MaxUint64) {
		l.faulted = true
		return base.ErrGenerationExhausted
	}
	l.nextFrameSeq++
	if required+segmentSealReserve > l.active.Remaining() {
		return l.capacityErrorLocked(required)
	}
	return nil
}

func (l *Log) capacityErrorLocked(required uint64) error {
	if l.rotator == nil {
		return segment.ErrFull
	}
	l.faulted = true
	return fmt.Errorf("append requirement %d cannot fit an empty data segment: %w", required, base.ErrCorrupt)
}

func (l *Log) commitParts(prepared batch.Prepared) ([][]byte, []storeformat.MutationEntry, error) {
	entries := make([]storeformat.MutationEntry, len(prepared.Mutations))
	var previous base.ID
	for i, mutation := range prepared.Mutations {
		if mutation.RecordID == 0 || (previous != 0 && mutation.RecordID <= previous) {
			return nil, nil, fmt.Errorf("batch mutation order: %w", base.ErrInvalidConfig)
		}
		entry := storeformat.MutationEntry{RecordID: mutation.RecordID}
		switch mutation.Operation {
		case batch.Put:
			entry.Operation, entry.NewVAddr = storeformat.MutationPut, mutation.Addr
		case batch.Delete:
			entry.Operation = storeformat.MutationDelete
		default:
			return nil, nil, fmt.Errorf("batch mutation operation: %w", base.ErrInvalidConfig)
		}
		entries[i] = entry
		previous = mutation.RecordID
	}
	if len(entries) == 0 {
		return nil, entries, nil
	}
	perPart := int(l.maxPartPayload / storeformat.MutationEntrySize)
	parts := make([][]byte, 0, (len(entries)+perPart-1)/perPart)
	for start := 0; start < len(entries); start += perPart {
		end := start + perPart
		if end > len(entries) {
			end = len(entries)
		}
		payload, err := storeformat.EncodeMutationEntries(storeformat.DescriptorCommit, entries[start:end])
		if err != nil {
			return nil, nil, err
		}
		parts = append(parts, payload)
	}
	return parts, entries, nil
}

func (l *Log) ready() error {
	if l.faulted {
		return segment.ErrPoisoned
	}
	if l.nextFrameSeq == base.FrameSeq(math.MaxUint64) {
		return base.ErrGenerationExhausted
	}
	return nil
}

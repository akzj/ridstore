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
}

const (
	PointPutWritten        failpoint.Point = "appendlog.put-written"
	PointAbortWritten      failpoint.Point = "appendlog.abort-written"
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

func New(active *segment.ActiveData, nextFrameSeq base.FrameSeq, maxFramePayload, maxPartPayload uint64) (*Log, error) {
	return NewWithHook(active, nextFrameSeq, maxFramePayload, maxPartPayload, nil)
}

func NewWithHook(active *segment.ActiveData, nextFrameSeq base.FrameSeq, maxFramePayload, maxPartPayload uint64, hook failpoint.Hook) (*Log, error) {
	if active == nil || nextFrameSeq == 0 || maxFramePayload < storeformat.DescriptorSealSize || maxPartPayload < storeformat.MutationEntrySize || maxPartPayload > maxFramePayload {
		return nil, fmt.Errorf("append log configuration: %w", base.ErrInvalidConfig)
	}
	maxPartPayload -= maxPartPayload % storeformat.MutationEntrySize
	return &Log{active: active, nextFrameSeq: nextFrameSeq, maxFramePayload: maxFramePayload, maxPartPayload: maxPartPayload, hook: hook}, nil
}

func (l *Log) AppendPut(ctx context.Context, batchID base.BatchID, id base.ID, value []byte) (base.VAddr, base.FrameSeq, uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, 0, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ready(); err != nil {
		return 0, 0, 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, 0, 0, err
	}
	seq := l.nextFrameSeq
	addr, written, err := l.active.Append(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: seq, BatchID: batchID, RecordID: id, Payload: value})
	if err != nil {
		if !errors.Is(err, segment.ErrFull) {
			l.faulted = true
		}
		return 0, 0, written, err
	}
	l.nextFrameSeq++
	if err := failpoint.Hit(l.hook, PointPutWritten); err != nil {
		l.faulted = true
		return 0, 0, written, err
	}
	return addr, seq, written, nil
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
	_, _, err = l.appendLocked(storeformat.Frame{Type: storeformat.FrameTypeBatchAbort, FrameSeq: l.nextFrameSeq, BatchID: batchID, Payload: encoded[:]})
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
	if _, _, err := l.appendLocked(storeformat.Frame{Type: typ, FrameSeq: l.nextFrameSeq, Payload: encoded[:]}); err != nil {
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

func (l *Log) AppendCommit(prepared batch.Prepared, commitSeq base.CommitSeq) (CommitAppendResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ready(); err != nil {
		return CommitAppendResult{}, err
	}
	if prepared.BatchID == 0 || commitSeq == 0 {
		return CommitAppendResult{}, fmt.Errorf("commit append identity: %w", base.ErrInvalidConfig)
	}
	partPayloads, entries, err := l.commitParts(prepared)
	if err != nil {
		return CommitAppendResult{}, err
	}
	partCount := len(partPayloads)
	if uint64(partCount) > math.MaxUint32 || uint64(len(entries)) > math.MaxUint32 {
		return CommitAppendResult{}, fmt.Errorf("commit descriptor count: %w", base.ErrInvalidConfig)
	}
	firstPartSeq, lastPartSeq := base.FrameSeq(0), base.FrameSeq(0)
	if partCount != 0 {
		firstPartSeq = l.nextFrameSeq
		lastPartSeq = l.nextFrameSeq + base.FrameSeq(partCount) - 1
		if lastPartSeq < firstPartSeq || lastPartSeq == base.FrameSeq(math.MaxUint64) {
			return CommitAppendResult{}, base.ErrGenerationExhausted
		}
	}
	sealSeq := l.nextFrameSeq + base.FrameSeq(partCount)
	if sealSeq < l.nextFrameSeq || sealSeq == 0 || sealSeq == base.FrameSeq(math.MaxUint64) {
		return CommitAppendResult{}, base.ErrGenerationExhausted
	}
	sealPayload, err := storeformat.EncodeDescriptorSealPayload(storeformat.DescriptorSeal{
		CommitSeq: commitSeq, PartCount: uint32(partCount), MutationCount: uint32(len(entries)),
		LogicalPayloadBytes: prepared.LogicalPayloadBytes, FirstPartFrameSeq: firstPartSeq, LastPartFrameSeq: lastPartSeq,
	}, partPayloads)
	if err != nil {
		return CommitAppendResult{}, err
	}
	frames := make([]storeformat.Frame, 0, partCount+1)
	for i, payload := range partPayloads {
		frames = append(frames, storeformat.Frame{Type: storeformat.FrameTypeCommitPart, FrameSeq: l.nextFrameSeq + base.FrameSeq(i), BatchID: prepared.BatchID, Payload: payload})
	}
	frames = append(frames, storeformat.Frame{Type: storeformat.FrameTypeCommitSeal, FrameSeq: sealSeq, BatchID: prepared.BatchID, Payload: sealPayload[:]})
	var totalBytes uint64
	for _, frame := range frames {
		encoded, err := storeformat.EncodeFrame(frame, l.maxFramePayload)
		if err != nil {
			return CommitAppendResult{}, err
		}
		totalBytes, err = base.AddUint64(totalBytes, uint64(len(encoded)))
		if err != nil {
			return CommitAppendResult{}, err
		}
	}
	if totalBytes > l.active.Remaining() {
		return CommitAppendResult{}, segment.ErrFull
	}
	result := CommitAppendResult{SealFrameSeq: sealSeq}
	for i, frame := range frames {
		_, written, err := l.appendLocked(frame)
		if err != nil {
			if i == len(frames)-1 && written != 0 {
				result.SealStarted = true
			}
			return result, err
		}
		if i == len(frames)-1 {
			result.SealStarted = true
			if err := failpoint.Hit(l.hook, PointCommitSealWritten); err != nil {
				l.faulted = true
				return result, err
			}
		} else if err := failpoint.Hit(l.hook, PointCommitPartWritten); err != nil {
			l.faulted = true
			return result, err
		}
	}
	if err := l.active.Sync(); err != nil {
		l.faulted = true
		return result, err
	}
	if err := failpoint.Hit(l.hook, PointCommitSynced); err != nil {
		l.faulted = true
		return result, err
	}
	return result, nil
}

func (l *Log) NextFrameSeq() base.FrameSeq {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextFrameSeq
}

func (l *Log) Faulted() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.faulted
}

func (l *Log) appendLocked(frame storeformat.Frame) (base.VAddr, uint64, error) {
	addr, written, err := l.active.Append(frame)
	if err != nil {
		if !errors.Is(err, segment.ErrFull) {
			l.faulted = true
		}
		return 0, written, err
	}
	l.nextFrameSeq++
	return addr, written, nil
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

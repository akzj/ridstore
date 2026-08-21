package format

import (
	"encoding/binary"
	"fmt"

	"github.com/akzj/ridstore/internal/base"
)

type AbortReason uint32

const (
	AbortReasonCaller AbortReason = iota + 1
	AbortReasonConflict
	AbortReasonDefinitePreSealFailure
	AbortReasonCloseCleanup
	AbortReasonBatchFailed
)

type BatchAbortPayload struct {
	Reason               AbortReason
	FinalMutationCount   uint32
	AppendedPayloadBytes uint64
	LastBatchFrameSeq    base.FrameSeq
}

type ReservePayload struct {
	PreviousHighExclusive uint64
	NewHighExclusive      uint64
	Generation            uint64
}

type SegmentSealPayload struct {
	SegmentID     base.DataSegmentID
	ValidDataEnd  uint64
	FirstFrameSeq base.FrameSeq
	LastFrameSeq  base.FrameSeq
	FrameCount    uint64
	MinCommitSeq  base.CommitSeq
	MaxCommitSeq  base.CommitSeq
}

func PutRecordValue(frame Frame) ([]byte, base.Revision, error) {
	if frame.Type != FrameTypePutRecord || frame.BatchID == 0 || frame.RecordID == 0 {
		return nil, 0, fmt.Errorf("put record frame: %w", base.ErrInvalidConfig)
	}
	return frame.Payload, base.Revision(frame.BatchID), nil
}

func EncodeBatchAbortPayload(p BatchAbortPayload) ([32]byte, error) {
	var dst [32]byte
	if !p.Reason.valid() {
		return dst, fmt.Errorf("abort reason %d: %w", p.Reason, base.ErrInvalidConfig)
	}
	binary.LittleEndian.PutUint32(dst[0:4], uint32(p.Reason))
	binary.LittleEndian.PutUint32(dst[4:8], p.FinalMutationCount)
	binary.LittleEndian.PutUint64(dst[8:16], p.AppendedPayloadBytes)
	binary.LittleEndian.PutUint64(dst[16:24], uint64(p.LastBatchFrameSeq))
	return dst, nil
}

func DecodeBatchAbortPayload(src []byte) (BatchAbortPayload, error) {
	var p BatchAbortPayload
	if len(src) != 32 || !allZero(src[24:32]) {
		return p, corruptf("batch abort payload size or reserved bytes")
	}
	p = BatchAbortPayload{
		Reason:               AbortReason(binary.LittleEndian.Uint32(src[0:4])),
		FinalMutationCount:   binary.LittleEndian.Uint32(src[4:8]),
		AppendedPayloadBytes: binary.LittleEndian.Uint64(src[8:16]),
		LastBatchFrameSeq:    base.FrameSeq(binary.LittleEndian.Uint64(src[16:24])),
	}
	if !p.Reason.valid() {
		return BatchAbortPayload{}, corruptf("batch abort reason")
	}
	return p, nil
}

func EncodeReservePayload(p ReservePayload) ([24]byte, error) {
	var dst [24]byte
	if p.PreviousHighExclusive == 0 || p.NewHighExclusive <= p.PreviousHighExclusive || p.Generation == 0 {
		return dst, fmt.Errorf("reserve payload: %w", base.ErrInvalidConfig)
	}
	binary.LittleEndian.PutUint64(dst[0:8], p.PreviousHighExclusive)
	binary.LittleEndian.PutUint64(dst[8:16], p.NewHighExclusive)
	binary.LittleEndian.PutUint64(dst[16:24], p.Generation)
	return dst, nil
}

func DecodeReservePayload(src []byte) (ReservePayload, error) {
	var p ReservePayload
	if len(src) != 24 {
		return p, corruptf("reserve payload size")
	}
	p = ReservePayload{
		PreviousHighExclusive: binary.LittleEndian.Uint64(src[0:8]),
		NewHighExclusive:      binary.LittleEndian.Uint64(src[8:16]),
		Generation:            binary.LittleEndian.Uint64(src[16:24]),
	}
	if p.PreviousHighExclusive == 0 || p.NewHighExclusive <= p.PreviousHighExclusive || p.Generation == 0 {
		return ReservePayload{}, corruptf("reserve payload fields")
	}
	return p, nil
}

func EncodeSegmentSealPayload(p SegmentSealPayload) ([64]byte, error) {
	var dst [64]byte
	if err := validateSegmentSealPayload(p); err != nil {
		return dst, err
	}
	binary.LittleEndian.PutUint32(dst[0:4], uint32(p.SegmentID))
	binary.LittleEndian.PutUint64(dst[8:16], p.ValidDataEnd)
	binary.LittleEndian.PutUint64(dst[16:24], uint64(p.FirstFrameSeq))
	binary.LittleEndian.PutUint64(dst[24:32], uint64(p.LastFrameSeq))
	binary.LittleEndian.PutUint64(dst[32:40], p.FrameCount)
	binary.LittleEndian.PutUint64(dst[40:48], uint64(p.MinCommitSeq))
	binary.LittleEndian.PutUint64(dst[48:56], uint64(p.MaxCommitSeq))
	return dst, nil
}

func DecodeSegmentSealPayload(src []byte) (SegmentSealPayload, error) {
	var p SegmentSealPayload
	if len(src) != 64 || binary.LittleEndian.Uint32(src[4:8]) != 0 || !allZero(src[56:64]) {
		return p, corruptf("segment seal size, flags, or reserved bytes")
	}
	p = SegmentSealPayload{
		SegmentID:     base.DataSegmentID(binary.LittleEndian.Uint32(src[0:4])),
		ValidDataEnd:  binary.LittleEndian.Uint64(src[8:16]),
		FirstFrameSeq: base.FrameSeq(binary.LittleEndian.Uint64(src[16:24])),
		LastFrameSeq:  base.FrameSeq(binary.LittleEndian.Uint64(src[24:32])),
		FrameCount:    binary.LittleEndian.Uint64(src[32:40]),
		MinCommitSeq:  base.CommitSeq(binary.LittleEndian.Uint64(src[40:48])),
		MaxCommitSeq:  base.CommitSeq(binary.LittleEndian.Uint64(src[48:56])),
	}
	if err := validateSegmentSealPayload(p); err != nil {
		return SegmentSealPayload{}, corruptf("segment seal fields: %v", err)
	}
	return p, nil
}

func DecodeSegmentSealFrame(frame Frame) (SegmentSealPayload, error) {
	if frame.Type != FrameTypeSegmentSeal || frame.BatchID != 0 || frame.RecordID != 0 {
		return SegmentSealPayload{}, corruptf("segment seal frame identity")
	}
	p, err := DecodeSegmentSealPayload(frame.Payload)
	if err != nil {
		return SegmentSealPayload{}, err
	}
	if p.LastFrameSeq != frame.FrameSeq {
		return SegmentSealPayload{}, corruptf("segment seal last frame sequence")
	}
	return p, nil
}

func validateSegmentSealPayload(p SegmentSealPayload) error {
	return validateDataFooter(DataSegmentFooter{
		SegmentID: p.SegmentID, ValidDataEnd: p.ValidDataEnd,
		FirstFrameSeq: p.FirstFrameSeq, LastFrameSeq: p.LastFrameSeq,
		FrameCount: p.FrameCount, MinCommitSeq: p.MinCommitSeq, MaxCommitSeq: p.MaxCommitSeq,
	})
}

func (r AbortReason) valid() bool {
	return r >= AbortReasonCaller && r <= AbortReasonBatchFailed
}

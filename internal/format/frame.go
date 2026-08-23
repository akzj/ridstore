package format

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/akzj/ridstore/internal/base"
)

const (
	FrameHeaderSize   = 64
	MutationEntrySize = 32
)

var frameMagic = [4]byte{'R', 'D', 'F', '1'}

type FrameType uint8

const (
	FrameTypePutRecord FrameType = iota + 1
	FrameTypeCommitPart
	FrameTypeCommitSeal
	FrameTypeBatchAbort
	FrameTypeIDReserve
	FrameTypeSegmentSeal
	FrameTypeRelocationPart
	FrameTypeRelocationSeal
	FrameTypeBatchIDReserve
)

type FrameLimits struct {
	MaxPayloadSize       uint64
	RemainingSegmentSize uint64
}

type FrameHeader struct {
	Type        FrameType
	TotalSize   uint64
	FrameSeq    base.FrameSeq
	BatchID     base.BatchID
	RecordID    base.ID
	PayloadSize uint64
	PayloadCRC  uint32
}

// Frame.Payload aliases the input buffer returned to DecodeFrame.
type Frame struct {
	Type     FrameType
	FrameSeq base.FrameSeq
	BatchID  base.BatchID
	RecordID base.ID
	Payload  []byte
}

func EncodeFrame(frame Frame, maxPayloadSize uint64) ([]byte, error) {
	totalSize, err := EncodedFrameSize(frame, maxPayloadSize)
	if err != nil {
		return nil, err
	}
	totalInt, err := base.Uint64ToInt(totalSize)
	if err != nil {
		return nil, fmt.Errorf("frame size: %w", err)
	}
	dst := make([]byte, totalInt)
	if _, err := EncodeFrameTo(dst, frame, maxPayloadSize); err != nil {
		return nil, err
	}
	return dst, nil
}

func EncodedFrameSize(frame Frame, maxPayloadSize uint64) (uint64, error) {
	payloadSize := uint64(len(frame.Payload))
	totalSize, err := validateFrameFields(frame.Type, frame.FrameSeq, frame.BatchID, frame.RecordID, payloadSize, maxPayloadSize)
	if err != nil {
		return 0, err
	}
	if err := validateFramePayloadForEncode(frame); err != nil {
		return 0, err
	}
	return totalSize, nil
}

// EncodeFrameTo encodes one complete Frame into caller-owned storage. dst may
// be reused; the complete encoded range, including padding, is overwritten.
func EncodeFrameTo(dst []byte, frame Frame, maxPayloadSize uint64) (int, error) {
	totalSize, err := EncodedFrameSize(frame, maxPayloadSize)
	if err != nil {
		return 0, err
	}
	totalInt, err := base.Uint64ToInt(totalSize)
	if err != nil {
		return 0, fmt.Errorf("frame size: %w", err)
	}
	if len(dst) < totalInt {
		return 0, fmt.Errorf("frame destination size: %w", base.ErrInvalidConfig)
	}
	dst = dst[:totalInt]
	clear(dst)
	payloadSize := uint64(len(frame.Payload))
	copy(dst[0:4], frameMagic[:])
	binary.LittleEndian.PutUint16(dst[4:6], FormatMajorVersion)
	dst[6] = byte(frame.Type)
	binary.LittleEndian.PutUint16(dst[8:10], FrameHeaderSize)
	binary.LittleEndian.PutUint64(dst[12:20], totalSize)
	binary.LittleEndian.PutUint64(dst[20:28], uint64(frame.FrameSeq))
	binary.LittleEndian.PutUint64(dst[28:36], uint64(frame.BatchID))
	binary.LittleEndian.PutUint64(dst[36:44], uint64(frame.RecordID))
	binary.LittleEndian.PutUint64(dst[44:52], payloadSize)
	if payloadSize != 0 {
		binary.LittleEndian.PutUint32(dst[56:60], crc32.Checksum(frame.Payload, castagnoliTable))
	}
	binary.LittleEndian.PutUint32(dst[52:56], crc32.Checksum(dst[:FrameHeaderSize], castagnoliTable))
	copy(dst[FrameHeaderSize:FrameHeaderSize+len(frame.Payload)], frame.Payload)
	return totalInt, nil
}

func DecodeFrameHeader(src []byte, limits FrameLimits) (FrameHeader, error) {
	var h FrameHeader
	if len(src) < FrameHeaderSize {
		return h, corruptf("frame header truncated: %d", len(src))
	}
	if !equal4(src[0:4], frameMagic) {
		return h, corruptf("frame magic")
	}
	if binary.LittleEndian.Uint16(src[8:10]) != FrameHeaderSize {
		return h, corruptf("frame header size")
	}
	if !validChecksum(src[:FrameHeaderSize], 52) {
		return h, corruptf("frame header checksum")
	}
	major := binary.LittleEndian.Uint16(src[4:6])
	if major != FormatMajorVersion {
		return h, fmt.Errorf("frame major version %d: %w", major, base.ErrUnsupported)
	}
	typ := FrameType(src[6])
	if !typ.valid() {
		return h, fmt.Errorf("frame type %d: %w", typ, base.ErrUnsupported)
	}
	if src[7] != 0 || binary.LittleEndian.Uint16(src[10:12]) != 0 || binary.LittleEndian.Uint32(src[60:64]) != 0 {
		return h, corruptf("frame flags or reserved bytes")
	}
	h = FrameHeader{
		Type:        typ,
		TotalSize:   binary.LittleEndian.Uint64(src[12:20]),
		FrameSeq:    base.FrameSeq(binary.LittleEndian.Uint64(src[20:28])),
		BatchID:     base.BatchID(binary.LittleEndian.Uint64(src[28:36])),
		RecordID:    base.ID(binary.LittleEndian.Uint64(src[36:44])),
		PayloadSize: binary.LittleEndian.Uint64(src[44:52]),
		PayloadCRC:  binary.LittleEndian.Uint32(src[56:60]),
	}
	wantTotal, err := validateFrameFields(h.Type, h.FrameSeq, h.BatchID, h.RecordID, h.PayloadSize, limits.MaxPayloadSize)
	if err != nil {
		return FrameHeader{}, corruptf("frame fields: %v", err)
	}
	if h.TotalSize != wantTotal || h.TotalSize > limits.RemainingSegmentSize {
		return FrameHeader{}, corruptf("frame total size or segment boundary")
	}
	if h.PayloadSize == 0 && h.PayloadCRC != 0 {
		return FrameHeader{}, corruptf("empty frame payload checksum")
	}
	return h, nil
}

func DecodeFrame(src []byte, limits FrameLimits) (Frame, int, error) {
	h, err := DecodeFrameHeader(src, limits)
	if err != nil {
		return Frame{}, 0, err
	}
	totalSize, err := base.Uint64ToInt(h.TotalSize)
	if err != nil || totalSize > len(src) {
		return Frame{}, 0, corruptf("frame truncated")
	}
	payloadSize, err := base.Uint64ToInt(h.PayloadSize)
	if err != nil {
		return Frame{}, 0, corruptf("frame payload size")
	}
	payload := src[FrameHeaderSize : FrameHeaderSize+payloadSize]
	if h.PayloadSize != 0 && crc32.Checksum(payload, castagnoliTable) != h.PayloadCRC {
		return Frame{}, 0, corruptf("frame payload checksum")
	}
	if !allZero(src[FrameHeaderSize+payloadSize : totalSize]) {
		return Frame{}, 0, corruptf("frame padding")
	}
	frame := Frame{
		Type: h.Type, FrameSeq: h.FrameSeq, BatchID: h.BatchID,
		RecordID: h.RecordID, Payload: payload,
	}
	if err := validateDecodedFramePayload(frame); err != nil {
		return Frame{}, 0, err
	}
	return frame, totalSize, nil
}

func validateFrameFields(typ FrameType, frameSeq base.FrameSeq, batchID base.BatchID, recordID base.ID, payloadSize, maxPayloadSize uint64) (uint64, error) {
	if !typ.valid() || frameSeq == 0 || maxPayloadSize == 0 || payloadSize > maxPayloadSize {
		return 0, fmt.Errorf("invalid frame type, sequence, or payload limit: %w", base.ErrInvalidConfig)
	}
	if err := validateFrameIdentity(typ, batchID, recordID); err != nil {
		return 0, err
	}
	if err := validatePayloadSize(typ, payloadSize); err != nil {
		return 0, err
	}
	unpadded, err := base.AddUint64(FrameHeaderSize, payloadSize)
	if err != nil {
		return 0, err
	}
	totalSize, err := base.Align8(unpadded)
	if err != nil || totalSize > math.MaxUint32 {
		return 0, fmt.Errorf("frame total size: %w", base.ErrInvalidConfig)
	}
	return totalSize, nil
}

func validateFrameIdentity(typ FrameType, batchID base.BatchID, recordID base.ID) error {
	switch typ {
	case FrameTypePutRecord:
		if batchID == 0 || recordID == 0 {
			return fmt.Errorf("put record identity: %w", base.ErrInvalidConfig)
		}
	case FrameTypeCommitPart, FrameTypeCommitSeal, FrameTypeBatchAbort,
		FrameTypeRelocationPart, FrameTypeRelocationSeal:
		if batchID == 0 || recordID != 0 {
			return fmt.Errorf("batch frame identity: %w", base.ErrInvalidConfig)
		}
	case FrameTypeIDReserve, FrameTypeSegmentSeal, FrameTypeBatchIDReserve:
		if batchID != 0 || recordID != 0 {
			return fmt.Errorf("system frame identity: %w", base.ErrInvalidConfig)
		}
	}
	return nil
}

func validatePayloadSize(typ FrameType, payloadSize uint64) error {
	valid := false
	switch typ {
	case FrameTypePutRecord:
		valid = true
	case FrameTypeCommitPart, FrameTypeRelocationPart:
		valid = payloadSize != 0 && payloadSize%MutationEntrySize == 0
	case FrameTypeCommitSeal, FrameTypeRelocationSeal, FrameTypeSegmentSeal:
		valid = payloadSize == 64
	case FrameTypeBatchAbort:
		valid = payloadSize == 32
	case FrameTypeIDReserve, FrameTypeBatchIDReserve:
		valid = payloadSize == 24
	}
	if !valid {
		return fmt.Errorf("frame payload shape: %w", base.ErrInvalidConfig)
	}
	return nil
}

func (t FrameType) valid() bool {
	return t >= FrameTypePutRecord && t <= FrameTypeBatchIDReserve
}

func equal4(src []byte, magic [4]byte) bool {
	return len(src) == len(magic) && src[0] == magic[0] && src[1] == magic[1] && src[2] == magic[2] && src[3] == magic[3]
}

func validateFramePayloadForEncode(frame Frame) error {
	var err error
	switch frame.Type {
	case FrameTypeBatchAbort:
		_, err = DecodeBatchAbortPayload(frame.Payload)
	case FrameTypeIDReserve, FrameTypeBatchIDReserve:
		_, err = DecodeReservePayload(frame.Payload)
	case FrameTypeSegmentSeal:
		_, err = DecodeSegmentSealFrame(frame)
	}
	if err != nil {
		return fmt.Errorf("frame payload semantics: %w", base.ErrInvalidConfig)
	}
	return nil
}

func validateDecodedFramePayload(frame Frame) error {
	switch frame.Type {
	case FrameTypeBatchAbort:
		_, err := DecodeBatchAbortPayload(frame.Payload)
		return err
	case FrameTypeIDReserve, FrameTypeBatchIDReserve:
		_, err := DecodeReservePayload(frame.Payload)
		return err
	case FrameTypeSegmentSeal:
		_, err := DecodeSegmentSealFrame(frame)
		return err
	default:
		return nil
	}
}

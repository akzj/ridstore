package recordlog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
)

const FormatVersion = uint16(2)

var (
	segmentHeaderMagic = [8]byte{'R', 'I', 'D', 'A', 'P', 'V', '2', 'H'}
	recordMagic        = [4]byte{'R', '2', 'R', 'C'}
	segmentFooterMagic = [8]byte{'R', 'I', 'D', 'A', 'P', 'V', '2', 'F'}
	crcTable           = crc32.MakeTable(crc32.Castagnoli)
)

type SegmentHeader struct {
	LogID           LogID
	SegmentID       SegmentID
	PreviousSegment SegmentID
	SegmentSize     uint32
}

type RecordHeader struct {
	PhysicalSize uint32
	PayloadSize  uint32
	Addr         VAddr
	PayloadCRC   uint32
}

type SegmentFooter struct {
	SegmentID   SegmentID
	DataEnd     uint32
	FirstAddr   VAddr
	LastAddr    VAddr
	RecordCount uint64
}

func PhysicalRecordSize(payloadSize uint64) (uint32, error) {
	if payloadSize > math.MaxUint32 || payloadSize > math.MaxUint64-uint64(RecordHeaderSize) {
		return 0, ErrPayloadTooBig
	}
	total := uint64(RecordHeaderSize) + payloadSize
	total = (total + uint64(RecordAlignment-1)) &^ uint64(RecordAlignment-1)
	if total > math.MaxUint32 {
		return 0, ErrPayloadTooBig
	}
	return uint32(total), nil
}

func EncodeSegmentHeader(h SegmentHeader) ([SegmentHeaderSize]byte, error) {
	var dst [SegmentHeaderSize]byte
	if h.LogID == (LogID{}) || h.SegmentID == 0 || h.SegmentSize <= SegmentHeaderSize+RecordHeaderSize+SegmentFooterSize || h.SegmentSize&uint32(RecordAlignment-1) != 0 {
		return dst, ErrInvalidConfig
	}
	copy(dst[0:8], segmentHeaderMagic[:])
	binary.LittleEndian.PutUint16(dst[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(dst[10:12], uint16(SegmentHeaderSize))
	binary.LittleEndian.PutUint32(dst[12:16], uint32(h.SegmentID))
	binary.LittleEndian.PutUint32(dst[16:20], h.SegmentSize)
	binary.LittleEndian.PutUint32(dst[20:24], uint32(h.PreviousSegment))
	copy(dst[24:40], h.LogID[:])
	binary.LittleEndian.PutUint32(dst[40:44], SegmentHeaderSize)
	binary.LittleEndian.PutUint32(dst[60:64], crc32.Checksum(dst[:60], crcTable))
	return dst, nil
}

func DecodeSegmentHeader(src []byte) (SegmentHeader, error) {
	if len(src) != int(SegmentHeaderSize) || string(src[:8]) != string(segmentHeaderMagic[:]) {
		return SegmentHeader{}, fmt.Errorf("segment header magic or size: %w", ErrCorrupt)
	}
	if binary.LittleEndian.Uint16(src[8:10]) != FormatVersion {
		return SegmentHeader{}, ErrUnsupported
	}
	if binary.LittleEndian.Uint16(src[10:12]) != uint16(SegmentHeaderSize) || binary.LittleEndian.Uint32(src[60:64]) != crc32.Checksum(src[:60], crcTable) {
		return SegmentHeader{}, fmt.Errorf("segment header fields: %w", ErrCorrupt)
	}
	h := SegmentHeader{
		SegmentID:       SegmentID(binary.LittleEndian.Uint32(src[12:16])),
		SegmentSize:     binary.LittleEndian.Uint32(src[16:20]),
		PreviousSegment: SegmentID(binary.LittleEndian.Uint32(src[20:24])),
	}
	copy(h.LogID[:], src[24:40])
	if h.LogID == (LogID{}) || h.SegmentID == 0 || h.SegmentSize <= SegmentHeaderSize+RecordHeaderSize+SegmentFooterSize || h.SegmentSize&uint32(RecordAlignment-1) != 0 ||
		binary.LittleEndian.Uint32(src[40:44]) != SegmentHeaderSize || !allZero(src[44:60]) {
		return SegmentHeader{}, fmt.Errorf("segment header values: %w", ErrCorrupt)
	}
	return h, nil
}

func EncodeRecord(addr VAddr, payload []byte) ([]byte, error) {
	total, err := PhysicalRecordSize(uint64(len(payload)))
	if err != nil {
		return nil, err
	}
	dst := make([]byte, total)
	if err := EncodeRecordTo(dst, addr, payload); err != nil {
		return nil, err
	}
	return dst, nil
}

func EncodeRecordTo(dst []byte, addr VAddr, payload []byte) error {
	total, err := PhysicalRecordSize(uint64(len(payload)))
	if err != nil {
		return err
	}
	if len(dst) != int(total) {
		return ErrInvalidConfig
	}
	if !addr.Valid() || !addr.MatchesPhysicalSize(total) {
		return ErrInvalidVAddr
	}
	clear(dst)
	copy(dst[0:4], recordMagic[:])
	binary.LittleEndian.PutUint16(dst[4:6], FormatVersion)
	binary.LittleEndian.PutUint16(dst[6:8], uint16(RecordHeaderSize))
	binary.LittleEndian.PutUint32(dst[8:12], total)
	binary.LittleEndian.PutUint32(dst[12:16], uint32(len(payload)))
	binary.LittleEndian.PutUint64(dst[16:24], uint64(addr))
	binary.LittleEndian.PutUint32(dst[24:28], crc32.Checksum(payload, crcTable))
	binary.LittleEndian.PutUint32(dst[28:32], crc32.Checksum(dst[:28], crcTable))
	copy(dst[RecordHeaderSize:], payload)
	return nil
}

func DecodeRecordHeader(src []byte) (RecordHeader, error) {
	if len(src) != int(RecordHeaderSize) || string(src[:4]) != string(recordMagic[:]) {
		return RecordHeader{}, fmt.Errorf("record header magic or size: %w", ErrCorrupt)
	}
	if binary.LittleEndian.Uint16(src[4:6]) != FormatVersion {
		return RecordHeader{}, ErrUnsupported
	}
	if binary.LittleEndian.Uint16(src[6:8]) != uint16(RecordHeaderSize) || binary.LittleEndian.Uint32(src[28:32]) != crc32.Checksum(src[:28], crcTable) {
		return RecordHeader{}, fmt.Errorf("record header fields: %w", ErrCorrupt)
	}
	h := RecordHeader{
		PhysicalSize: binary.LittleEndian.Uint32(src[8:12]),
		PayloadSize:  binary.LittleEndian.Uint32(src[12:16]),
		Addr:         VAddr(binary.LittleEndian.Uint64(src[16:24])),
		PayloadCRC:   binary.LittleEndian.Uint32(src[24:28]),
	}
	want, err := PhysicalRecordSize(uint64(h.PayloadSize))
	if err != nil || h.PhysicalSize != want || !h.Addr.Valid() || !h.Addr.MatchesPhysicalSize(h.PhysicalSize) {
		return RecordHeader{}, fmt.Errorf("record header values: %w", ErrCorrupt)
	}
	return h, nil
}

// DecodeRecord returns a payload slice that aliases src.
func DecodeRecord(src []byte) (RecordHeader, []byte, error) {
	if len(src) < int(RecordHeaderSize) {
		return RecordHeader{}, nil, fmt.Errorf("record truncated: %w", ErrCorrupt)
	}
	h, err := DecodeRecordHeader(src[:RecordHeaderSize])
	if err != nil {
		return RecordHeader{}, nil, err
	}
	if len(src) != int(h.PhysicalSize) || h.PayloadSize > h.PhysicalSize-RecordHeaderSize {
		return RecordHeader{}, nil, fmt.Errorf("record size: %w", ErrCorrupt)
	}
	payloadEnd := RecordHeaderSize + h.PayloadSize
	payload := src[RecordHeaderSize:payloadEnd]
	if crc32.Checksum(payload, crcTable) != h.PayloadCRC || !allZero(src[payloadEnd:]) {
		return RecordHeader{}, nil, fmt.Errorf("record payload or padding: %w", ErrCorrupt)
	}
	return h, payload, nil
}

func EncodeSegmentFooter(f SegmentFooter) ([SegmentFooterSize]byte, error) {
	var dst [SegmentFooterSize]byte
	if err := validateSegmentFooter(f); err != nil {
		return dst, err
	}
	copy(dst[0:8], segmentFooterMagic[:])
	binary.LittleEndian.PutUint16(dst[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(dst[10:12], uint16(SegmentFooterSize))
	binary.LittleEndian.PutUint32(dst[12:16], uint32(f.SegmentID))
	binary.LittleEndian.PutUint32(dst[16:20], f.DataEnd)
	binary.LittleEndian.PutUint64(dst[24:32], uint64(f.FirstAddr))
	binary.LittleEndian.PutUint64(dst[32:40], uint64(f.LastAddr))
	binary.LittleEndian.PutUint64(dst[40:48], f.RecordCount)
	binary.LittleEndian.PutUint32(dst[60:64], crc32.Checksum(dst[:60], crcTable))
	return dst, nil
}

func DecodeSegmentFooter(src []byte) (SegmentFooter, error) {
	if len(src) != int(SegmentFooterSize) || string(src[:8]) != string(segmentFooterMagic[:]) {
		return SegmentFooter{}, fmt.Errorf("segment footer magic or size: %w", ErrCorrupt)
	}
	if binary.LittleEndian.Uint16(src[8:10]) != FormatVersion {
		return SegmentFooter{}, ErrUnsupported
	}
	if binary.LittleEndian.Uint16(src[10:12]) != uint16(SegmentFooterSize) || binary.LittleEndian.Uint32(src[60:64]) != crc32.Checksum(src[:60], crcTable) || !allZero(src[20:24]) || !allZero(src[48:60]) {
		return SegmentFooter{}, fmt.Errorf("segment footer fields: %w", ErrCorrupt)
	}
	f := SegmentFooter{
		SegmentID:   SegmentID(binary.LittleEndian.Uint32(src[12:16])),
		DataEnd:     binary.LittleEndian.Uint32(src[16:20]),
		FirstAddr:   VAddr(binary.LittleEndian.Uint64(src[24:32])),
		LastAddr:    VAddr(binary.LittleEndian.Uint64(src[32:40])),
		RecordCount: binary.LittleEndian.Uint64(src[40:48]),
	}
	if err := validateSegmentFooter(f); err != nil {
		return SegmentFooter{}, fmt.Errorf("segment footer values: %w", ErrCorrupt)
	}
	return f, nil
}

func validateSegmentFooter(f SegmentFooter) error {
	if f.SegmentID == 0 || f.DataEnd < SegmentHeaderSize || f.DataEnd&uint32(RecordAlignment-1) != 0 {
		return ErrInvalidConfig
	}
	if f.RecordCount == 0 {
		if f.FirstAddr != 0 || f.LastAddr != 0 || f.DataEnd != SegmentHeaderSize {
			return ErrInvalidConfig
		}
		return nil
	}
	if !f.FirstAddr.Valid() || !f.LastAddr.Valid() || f.FirstAddr.SegmentID() != f.SegmentID || f.LastAddr.SegmentID() != f.SegmentID || f.FirstAddr > f.LastAddr || f.LastAddr.Offset() >= f.DataEnd {
		return ErrInvalidConfig
	}
	return nil
}

func allZero(src []byte) bool {
	for _, value := range src {
		if value != 0 {
			return false
		}
	}
	return true
}

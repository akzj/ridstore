package v2

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
)

const (
	formatVersion     = uint16(1)
	segmentHeaderSize = uint64(64)
	recordHeaderSize  = uint64(32)
	segmentFooterSize = uint64(64)
	recordAlignment   = uint64(8)
)

var (
	segmentHeaderMagic = [8]byte{'R', 'I', 'D', 'A', 'P', 'V', '2', 'H'}
	recordMagic        = [4]byte{'R', '2', 'R', 'C'}
	segmentFooterMagic = [8]byte{'R', 'I', 'D', 'A', 'P', 'V', '2', 'F'}
	crcTable           = crc32.MakeTable(crc32.Castagnoli)
)

type logID [16]byte

type segmentHeader struct {
	LogID           logID
	SegmentID       uint32
	PreviousSegment uint32
	SegmentSize     uint64
}

type recordHeader struct {
	PhysicalSize uint32
	PayloadSize  uint32
	Addr         VAddr
	PayloadCRC   uint32
}

type segmentFooter struct {
	SegmentID   uint32
	DataEnd     uint64
	FirstAddr   VAddr
	LastAddr    VAddr
	RecordCount uint64
}

func alignRecordSize(n uint64) (uint64, error) {
	if n > math.MaxUint64-(recordAlignment-1) {
		return 0, ErrPayloadTooBig
	}
	return (n + recordAlignment - 1) &^ (recordAlignment - 1), nil
}

func encodedRecordSize(payloadSize uint64) (uint64, error) {
	if payloadSize > math.MaxUint32 || payloadSize > math.MaxUint64-recordHeaderSize {
		return 0, ErrPayloadTooBig
	}
	total, err := alignRecordSize(recordHeaderSize + payloadSize)
	if err != nil || total > math.MaxUint32 {
		return 0, ErrPayloadTooBig
	}
	return total, nil
}

func encodeSegmentHeader(h segmentHeader) ([segmentHeaderSize]byte, error) {
	var dst [segmentHeaderSize]byte
	if h.LogID == (logID{}) || h.SegmentID == 0 || h.SegmentSize > maxSegmentSize || h.SegmentSize <= segmentHeaderSize+recordHeaderSize+segmentFooterSize {
		return dst, ErrInvalidConfig
	}
	copy(dst[0:8], segmentHeaderMagic[:])
	binary.LittleEndian.PutUint16(dst[8:10], formatVersion)
	binary.LittleEndian.PutUint16(dst[10:12], uint16(segmentHeaderSize))
	binary.LittleEndian.PutUint32(dst[12:16], h.SegmentID)
	binary.LittleEndian.PutUint64(dst[16:24], h.SegmentSize)
	copy(dst[24:40], h.LogID[:])
	binary.LittleEndian.PutUint32(dst[40:44], h.PreviousSegment)
	binary.LittleEndian.PutUint64(dst[48:56], segmentHeaderSize)
	binary.LittleEndian.PutUint32(dst[60:64], crc32.Checksum(dst[:60], crcTable))
	return dst, nil
}

func decodeSegmentHeader(src []byte) (segmentHeader, error) {
	if len(src) != int(segmentHeaderSize) || string(src[:8]) != string(segmentHeaderMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != formatVersion || binary.LittleEndian.Uint16(src[10:12]) != uint16(segmentHeaderSize) ||
		binary.LittleEndian.Uint32(src[60:64]) != crc32.Checksum(src[:60], crcTable) {
		return segmentHeader{}, fmt.Errorf("segment header: %w", ErrCorrupt)
	}
	h := segmentHeader{
		SegmentID:       binary.LittleEndian.Uint32(src[12:16]),
		SegmentSize:     binary.LittleEndian.Uint64(src[16:24]),
		PreviousSegment: binary.LittleEndian.Uint32(src[40:44]),
	}
	copy(h.LogID[:], src[24:40])
	if h.LogID == (logID{}) || h.SegmentID == 0 || h.SegmentSize > maxSegmentSize || h.SegmentSize <= segmentHeaderSize+recordHeaderSize+segmentFooterSize ||
		binary.LittleEndian.Uint64(src[48:56]) != segmentHeaderSize {
		return segmentHeader{}, fmt.Errorf("segment header fields: %w", ErrCorrupt)
	}
	return h, nil
}

func encodeRecord(addr VAddr, payload []byte) ([]byte, error) {
	if !addr.Valid() {
		return nil, ErrInvalidVAddr
	}
	total, err := encodedRecordSize(uint64(len(payload)))
	if err != nil {
		return nil, err
	}
	dst := make([]byte, total)
	copy(dst[0:4], recordMagic[:])
	binary.LittleEndian.PutUint16(dst[4:6], formatVersion)
	binary.LittleEndian.PutUint16(dst[6:8], uint16(recordHeaderSize))
	binary.LittleEndian.PutUint32(dst[8:12], uint32(total))
	binary.LittleEndian.PutUint32(dst[12:16], uint32(len(payload)))
	binary.LittleEndian.PutUint64(dst[16:24], uint64(addr))
	binary.LittleEndian.PutUint32(dst[24:28], crc32.Checksum(payload, crcTable))
	binary.LittleEndian.PutUint32(dst[28:32], crc32.Checksum(dst[:28], crcTable))
	copy(dst[recordHeaderSize:], payload)
	return dst, nil
}

func decodeRecordHeader(src []byte) (recordHeader, error) {
	if len(src) != int(recordHeaderSize) || string(src[:4]) != string(recordMagic[:]) ||
		binary.LittleEndian.Uint16(src[4:6]) != formatVersion || binary.LittleEndian.Uint16(src[6:8]) != uint16(recordHeaderSize) ||
		binary.LittleEndian.Uint32(src[28:32]) != crc32.Checksum(src[:28], crcTable) {
		return recordHeader{}, fmt.Errorf("record header: %w", ErrCorrupt)
	}
	h := recordHeader{
		PhysicalSize: binary.LittleEndian.Uint32(src[8:12]),
		PayloadSize:  binary.LittleEndian.Uint32(src[12:16]),
		Addr:         VAddr(binary.LittleEndian.Uint64(src[16:24])),
		PayloadCRC:   binary.LittleEndian.Uint32(src[24:28]),
	}
	want, err := encodedRecordSize(uint64(h.PayloadSize))
	if err != nil || uint64(h.PhysicalSize) != want || !h.Addr.Valid() {
		return recordHeader{}, fmt.Errorf("record header fields: %w", ErrCorrupt)
	}
	return h, nil
}

func decodeRecord(src []byte) (recordHeader, []byte, error) {
	if len(src) < int(recordHeaderSize) {
		return recordHeader{}, nil, fmt.Errorf("short record: %w", ErrCorrupt)
	}
	h, err := decodeRecordHeader(src[:recordHeaderSize])
	if err != nil {
		return recordHeader{}, nil, err
	}
	if len(src) != int(h.PhysicalSize) || uint64(h.PayloadSize) > uint64(len(src))-recordHeaderSize {
		return recordHeader{}, nil, fmt.Errorf("record size: %w", ErrCorrupt)
	}
	payload := src[recordHeaderSize : recordHeaderSize+uint64(h.PayloadSize)]
	if crc32.Checksum(payload, crcTable) != h.PayloadCRC {
		return recordHeader{}, nil, fmt.Errorf("record payload: %w", ErrCorrupt)
	}
	for _, b := range src[recordHeaderSize+uint64(h.PayloadSize):] {
		if b != 0 {
			return recordHeader{}, nil, fmt.Errorf("record padding: %w", ErrCorrupt)
		}
	}
	return h, payload, nil
}

func encodeSegmentFooter(f segmentFooter) ([segmentFooterSize]byte, error) {
	var dst [segmentFooterSize]byte
	if f.SegmentID == 0 || f.DataEnd < segmentHeaderSize || (f.RecordCount == 0 && (f.FirstAddr != 0 || f.LastAddr != 0)) ||
		(f.RecordCount != 0 && (!f.FirstAddr.Valid() || !f.LastAddr.Valid() || f.FirstAddr > f.LastAddr)) {
		return dst, ErrInvalidConfig
	}
	copy(dst[:8], segmentFooterMagic[:])
	binary.LittleEndian.PutUint16(dst[8:10], formatVersion)
	binary.LittleEndian.PutUint16(dst[10:12], uint16(segmentFooterSize))
	binary.LittleEndian.PutUint32(dst[12:16], f.SegmentID)
	binary.LittleEndian.PutUint64(dst[16:24], f.DataEnd)
	binary.LittleEndian.PutUint64(dst[24:32], uint64(f.FirstAddr))
	binary.LittleEndian.PutUint64(dst[32:40], uint64(f.LastAddr))
	binary.LittleEndian.PutUint64(dst[40:48], f.RecordCount)
	binary.LittleEndian.PutUint32(dst[60:64], crc32.Checksum(dst[:60], crcTable))
	return dst, nil
}

func decodeSegmentFooter(src []byte) (segmentFooter, error) {
	if len(src) != int(segmentFooterSize) || string(src[:8]) != string(segmentFooterMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != formatVersion || binary.LittleEndian.Uint16(src[10:12]) != uint16(segmentFooterSize) ||
		binary.LittleEndian.Uint32(src[60:64]) != crc32.Checksum(src[:60], crcTable) {
		return segmentFooter{}, fmt.Errorf("segment footer: %w", ErrCorrupt)
	}
	f := segmentFooter{
		SegmentID:   binary.LittleEndian.Uint32(src[12:16]),
		DataEnd:     binary.LittleEndian.Uint64(src[16:24]),
		FirstAddr:   VAddr(binary.LittleEndian.Uint64(src[24:32])),
		LastAddr:    VAddr(binary.LittleEndian.Uint64(src[32:40])),
		RecordCount: binary.LittleEndian.Uint64(src[40:48]),
	}
	if f.SegmentID == 0 || f.DataEnd < segmentHeaderSize || (f.RecordCount == 0 && (f.FirstAddr != 0 || f.LastAddr != 0)) ||
		(f.RecordCount != 0 && (!f.FirstAddr.Valid() || !f.LastAddr.Valid() || f.FirstAddr > f.LastAddr)) {
		return segmentFooter{}, fmt.Errorf("segment footer fields: %w", ErrCorrupt)
	}
	return f, nil
}

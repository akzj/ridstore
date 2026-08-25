package mapstore

import (
	"encoding/binary"
	"hash/crc32"
	"math"

	"github.com/akzj/ridstore/internal/model"
)

const (
	SegmentHeaderSize = uint32(64)
	SegmentFooterSize = uint32(64)
	Alignment         = uint32(8)
)

var (
	segmentHeaderMagic = [8]byte{'R', 'I', 'D', 'M', 'A', 'P', '2', 'H'}
	segmentFooterMagic = [8]byte{'R', 'I', 'D', 'M', 'A', 'P', '2', 'F'}
)

type StoreID [16]byte

type SegmentHeader struct {
	StoreID         StoreID
	SegmentID       model.MapSegmentID
	PreviousSegment model.MapSegmentID
	SegmentSize     uint32
}

type SegmentFooter struct {
	SegmentID model.MapSegmentID
	ValidEnd  uint32
	FirstSeq  uint64
	LastSeq   uint64
	NodeCount uint64
}

type SegmentSummary struct {
	SegmentID model.MapSegmentID
	ValidEnd  uint32
	FirstSeq  uint64
	LastSeq   uint64
	NodeCount uint64
}

func EncodeSegmentHeader(header SegmentHeader) ([SegmentHeaderSize]byte, error) {
	var dst [SegmentHeaderSize]byte
	if !validSegmentHeader(header) {
		return dst, ErrInvalid
	}
	copy(dst[0:8], segmentHeaderMagic[:])
	binary.LittleEndian.PutUint16(dst[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(dst[10:12], uint16(SegmentHeaderSize))
	binary.LittleEndian.PutUint32(dst[12:16], uint32(header.SegmentID))
	binary.LittleEndian.PutUint32(dst[16:20], header.SegmentSize)
	binary.LittleEndian.PutUint32(dst[20:24], uint32(header.PreviousSegment))
	copy(dst[24:40], header.StoreID[:])
	binary.LittleEndian.PutUint32(dst[40:44], SegmentHeaderSize)
	binary.LittleEndian.PutUint32(dst[60:64], crc32.Checksum(dst[:60], crcTable))
	return dst, nil
}

func DecodeSegmentHeader(src []byte) (SegmentHeader, error) {
	if len(src) != int(SegmentHeaderSize) || string(src[:8]) != string(segmentHeaderMagic[:]) {
		return SegmentHeader{}, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(src[8:10]) != FormatVersion {
		return SegmentHeader{}, ErrUnsupported
	}
	if binary.LittleEndian.Uint16(src[10:12]) != uint16(SegmentHeaderSize) || binary.LittleEndian.Uint32(src[60:64]) != crc32.Checksum(src[:60], crcTable) || !zeroBytes(src[44:60]) {
		return SegmentHeader{}, ErrCorrupt
	}
	header := SegmentHeader{
		SegmentID:       model.MapSegmentID(binary.LittleEndian.Uint32(src[12:16])),
		SegmentSize:     binary.LittleEndian.Uint32(src[16:20]),
		PreviousSegment: model.MapSegmentID(binary.LittleEndian.Uint32(src[20:24])),
	}
	copy(header.StoreID[:], src[24:40])
	if binary.LittleEndian.Uint32(src[40:44]) != SegmentHeaderSize || !validSegmentHeader(header) {
		return SegmentHeader{}, ErrCorrupt
	}
	return header, nil
}

func EncodeSegmentFooter(footer SegmentFooter) ([SegmentFooterSize]byte, error) {
	var dst [SegmentFooterSize]byte
	if !validSegmentFooter(footer) {
		return dst, ErrInvalid
	}
	copy(dst[0:8], segmentFooterMagic[:])
	binary.LittleEndian.PutUint16(dst[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(dst[10:12], uint16(SegmentFooterSize))
	binary.LittleEndian.PutUint32(dst[12:16], uint32(footer.SegmentID))
	binary.LittleEndian.PutUint32(dst[16:20], footer.ValidEnd)
	binary.LittleEndian.PutUint64(dst[24:32], footer.FirstSeq)
	binary.LittleEndian.PutUint64(dst[32:40], footer.LastSeq)
	binary.LittleEndian.PutUint64(dst[40:48], footer.NodeCount)
	binary.LittleEndian.PutUint32(dst[60:64], crc32.Checksum(dst[:60], crcTable))
	return dst, nil
}

func DecodeSegmentFooter(src []byte) (SegmentFooter, error) {
	if len(src) != int(SegmentFooterSize) || string(src[:8]) != string(segmentFooterMagic[:]) {
		return SegmentFooter{}, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(src[8:10]) != FormatVersion {
		return SegmentFooter{}, ErrUnsupported
	}
	if binary.LittleEndian.Uint16(src[10:12]) != uint16(SegmentFooterSize) || binary.LittleEndian.Uint32(src[60:64]) != crc32.Checksum(src[:60], crcTable) || !zeroBytes(src[20:24]) || !zeroBytes(src[48:60]) {
		return SegmentFooter{}, ErrCorrupt
	}
	footer := SegmentFooter{
		SegmentID: model.MapSegmentID(binary.LittleEndian.Uint32(src[12:16])),
		ValidEnd:  binary.LittleEndian.Uint32(src[16:20]),
		FirstSeq:  binary.LittleEndian.Uint64(src[24:32]),
		LastSeq:   binary.LittleEndian.Uint64(src[32:40]),
		NodeCount: binary.LittleEndian.Uint64(src[40:48]),
	}
	if !validSegmentFooter(footer) {
		return SegmentFooter{}, ErrCorrupt
	}
	return footer, nil
}

func (s SegmentSummary) footer() SegmentFooter {
	return SegmentFooter{SegmentID: s.SegmentID, ValidEnd: s.ValidEnd, FirstSeq: s.FirstSeq, LastSeq: s.LastSeq, NodeCount: s.NodeCount}
}

func (s SegmentSummary) valid(segmentSize uint32) bool {
	return validSegmentFooter(s.footer()) && s.ValidEnd <= segmentSize-SegmentFooterSize
}

func validSegmentHeader(header SegmentHeader) bool {
	if header.StoreID == (StoreID{}) || header.SegmentID == 0 || header.SegmentID == model.MapSegmentID(math.MaxUint32) || header.SegmentSize < SegmentHeaderSize+DenseNodeSize+SegmentFooterSize || header.SegmentSize&uint32(Alignment-1) != 0 {
		return false
	}
	if header.SegmentID == 1 {
		return header.PreviousSegment == 0
	}
	return header.PreviousSegment == header.SegmentID-1
}

func validSegmentFooter(footer SegmentFooter) bool {
	if footer.SegmentID == 0 || footer.ValidEnd < SegmentHeaderSize || footer.ValidEnd&uint32(Alignment-1) != 0 {
		return false
	}
	if footer.NodeCount == 0 {
		return footer.ValidEnd == SegmentHeaderSize && footer.FirstSeq == 0 && footer.LastSeq == 0
	}
	return footer.FirstSeq != 0 && footer.LastSeq >= footer.FirstSeq && footer.LastSeq-footer.FirstSeq+1 == footer.NodeCount
}

func zeroBytes(src []byte) bool {
	for _, value := range src {
		if value != 0 {
			return false
		}
	}
	return true
}

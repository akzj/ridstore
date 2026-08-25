package recordlog

import (
	"fmt"
	"math"
)

const (
	SegmentHeaderSize = uint32(64)
	RecordHeaderSize  = uint32(32)
	SegmentFooterSize = uint32(64)
	RecordAlignment   = uint32(8)

	vaddrOffsetBits = 32
	vaddrSizeMask   = uint32(7)
	reservedSizeTag = uint32(7)
)

type SegmentID uint32
type LogID [16]byte

// VAddr identifies the first byte of one physical Record. The lower three bits
// are a read-size hint; Offset returns the aligned byte offset without the tag.
type VAddr uint64

func NewVAddr(segmentID SegmentID, offset, physicalSize uint32) (VAddr, error) {
	tag, err := sizeTag(physicalSize)
	if err != nil || segmentID == 0 || offset < SegmentHeaderSize || offset&vaddrSizeMask != 0 {
		return 0, ErrInvalidVAddr
	}
	return VAddr(uint64(segmentID)<<vaddrOffsetBits | uint64(offset) | uint64(tag)), nil
}

func ParseVAddr(raw uint64) (VAddr, error) {
	v := VAddr(raw)
	if !v.Valid() {
		return 0, ErrInvalidVAddr
	}
	return v, nil
}

func (v VAddr) SegmentID() SegmentID { return SegmentID(uint64(v) >> vaddrOffsetBits) }
func (v VAddr) Offset() uint32       { return uint32(v) &^ vaddrSizeMask }
func (v VAddr) SizeTag() uint8       { return uint8(uint32(v) & vaddrSizeMask) }

func (v VAddr) Valid() bool {
	return v.SegmentID() != 0 && v.Offset() >= SegmentHeaderSize && uint32(v.SizeTag()) != reservedSizeTag
}

func (v VAddr) ReadHint() (uint32, error) {
	if !v.Valid() {
		return 0, ErrInvalidVAddr
	}
	return uint32(64) << v.SizeTag(), nil
}

func (v VAddr) MatchesPhysicalSize(physicalSize uint32) bool {
	tag, err := sizeTag(physicalSize)
	return err == nil && uint8(tag) == v.SizeTag()
}

func (v VAddr) End(physicalSize uint32) (LogPos, error) {
	if !v.Valid() || !v.MatchesPhysicalSize(physicalSize) || physicalSize > math.MaxUint32-v.Offset() {
		return LogPos{}, ErrInvalidVAddr
	}
	return NewLogPos(v.SegmentID(), v.Offset()+physicalSize)
}

func (v VAddr) String() string {
	return fmt.Sprintf("%d:%d", v.SegmentID(), v.Offset())
}

func sizeTag(physicalSize uint32) (uint32, error) {
	if physicalSize < RecordHeaderSize || physicalSize&uint32(RecordAlignment-1) != 0 {
		return 0, ErrInvalidVAddr
	}
	for tag, upper := uint32(0), uint32(64); tag < reservedSizeTag; tag, upper = tag+1, upper<<1 {
		if physicalSize <= upper || tag == reservedSizeTag-1 {
			return tag, nil
		}
	}
	return 0, ErrInvalidVAddr
}

// LogPos is an untagged scan boundary. It may point to the first byte after a
// Record and must never be interpreted as a VAddr.
type LogPos struct {
	SegmentID SegmentID
	Offset    uint32
}

func NewLogPos(segmentID SegmentID, offset uint32) (LogPos, error) {
	if segmentID == 0 || offset < SegmentHeaderSize || offset&uint32(RecordAlignment-1) != 0 {
		return LogPos{}, ErrInvalidLogPos
	}
	return LogPos{SegmentID: segmentID, Offset: offset}, nil
}

func ParseLogPos(raw uint64) (LogPos, error) {
	return NewLogPos(SegmentID(raw>>32), uint32(raw))
}

func (p LogPos) Valid() bool {
	_, err := NewLogPos(p.SegmentID, p.Offset)
	return err == nil
}

func (p LogPos) Uint64() uint64 {
	return uint64(p.SegmentID)<<32 | uint64(p.Offset)
}

func (p LogPos) Compare(other LogPos) int {
	if p.SegmentID < other.SegmentID || (p.SegmentID == other.SegmentID && p.Offset < other.Offset) {
		return -1
	}
	if p == other {
		return 0
	}
	return 1
}

type AppendResult struct {
	Addr VAddr
	End  LogPos
}

// RecordMetadata is the structural information available for both buffered
// and persisted Records. It intentionally excludes on-disk checksum fields.
type RecordMetadata struct {
	PhysicalSize uint32
	PayloadSize  uint32
	Addr         VAddr
}

func NewAppendResult(addr VAddr, physicalSize uint32) (AppendResult, error) {
	end, err := addr.End(physicalSize)
	if err != nil {
		return AppendResult{}, err
	}
	return AppendResult{Addr: addr, End: end}, nil
}

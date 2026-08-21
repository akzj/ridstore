package base

import "fmt"

const (
	AddressOffsetAlignment = uint32(8)
	FirstContentOffset     = uint32(4096)
)

type ID uint64
type BatchID uint64
type CommitSeq uint64
type FrameSeq uint64
type NodeSeq uint64
type Revision uint64
type DataSegmentID uint32
type MapSegmentID uint32
type StoreUUID [16]byte

// VAddr points to a Data Frame header. Zero means NotFound.
type VAddr uint64

// MapAddr points to a Mapping Node header. Zero means no node.
type MapAddr uint64

// LogPos is a Data Log scan boundary. It is deliberately not a VAddr.
type LogPos uint64

func NewVAddr(segmentID DataSegmentID, offset uint32) (VAddr, error) {
	raw, err := encodeAddress(uint32(segmentID), offset)
	if err != nil {
		return 0, fmt.Errorf("vaddr: %w", err)
	}
	return VAddr(raw), nil
}

func NewMapAddr(segmentID MapSegmentID, offset uint32) (MapAddr, error) {
	raw, err := encodeAddress(uint32(segmentID), offset)
	if err != nil {
		return 0, fmt.Errorf("map addr: %w", err)
	}
	return MapAddr(raw), nil
}

func NewLogPos(segmentID DataSegmentID, offset uint32) (LogPos, error) {
	raw, err := encodeAddress(uint32(segmentID), offset)
	if err != nil {
		return 0, fmt.Errorf("log pos: %w", err)
	}
	return LogPos(raw), nil
}

func ParseVAddr(raw uint64) (VAddr, error) {
	if err := validateAddress(raw); err != nil {
		return 0, fmt.Errorf("vaddr: %w", err)
	}
	return VAddr(raw), nil
}

func ParseMapAddr(raw uint64) (MapAddr, error) {
	if err := validateAddress(raw); err != nil {
		return 0, fmt.Errorf("map addr: %w", err)
	}
	return MapAddr(raw), nil
}

func ParseLogPos(raw uint64) (LogPos, error) {
	if err := validateAddress(raw); err != nil {
		return 0, fmt.Errorf("log pos: %w", err)
	}
	return LogPos(raw), nil
}

func (a VAddr) SegmentID() DataSegmentID  { return DataSegmentID(uint64(a) >> 32) }
func (a VAddr) Offset() uint32            { return uint32(a) }
func (a MapAddr) SegmentID() MapSegmentID { return MapSegmentID(uint64(a) >> 32) }
func (a MapAddr) Offset() uint32          { return uint32(a) }
func (p LogPos) SegmentID() DataSegmentID { return DataSegmentID(uint64(p) >> 32) }
func (p LogPos) Offset() uint32           { return uint32(p) }

func encodeAddress(fileID, offset uint32) (uint64, error) {
	if fileID == 0 || offset < FirstContentOffset || offset%AddressOffsetAlignment != 0 {
		return 0, ErrInvalidAddress
	}
	return uint64(fileID)<<32 | uint64(offset), nil
}

func validateAddress(raw uint64) error {
	if raw == 0 {
		return ErrInvalidAddress
	}
	_, err := encodeAddress(uint32(raw>>32), uint32(raw))
	return err
}

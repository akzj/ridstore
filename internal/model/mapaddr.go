package model

import "errors"

var ErrInvalidMapAddr = errors.New("model: invalid mapping address")

// MapAddr identifies a node in the persistent mapping store. Zero means no node.
type MapAddr uint64

func NewMapAddr(segmentID MapSegmentID, offset uint32) (MapAddr, error) {
	if segmentID == 0 || offset < 64 || offset&7 != 0 {
		return 0, ErrInvalidMapAddr
	}
	return MapAddr(uint64(segmentID)<<32 | uint64(offset)), nil
}

func ParseMapAddr(raw uint64) (MapAddr, error) {
	addr := MapAddr(raw)
	if !addr.Valid() {
		return 0, ErrInvalidMapAddr
	}
	return addr, nil
}

func (a MapAddr) SegmentID() MapSegmentID { return MapSegmentID(uint64(a) >> 32) }
func (a MapAddr) Offset() uint32          { return uint32(a) }
func (a MapAddr) Valid() bool {
	return a.SegmentID() != 0 && a.Offset() >= 64 && a.Offset()&7 == 0
}

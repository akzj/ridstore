package mapstore

import (
	"math"
	"sort"

	"github.com/akzj/ridstore/internal/model"
)

type CatalogSnapshot struct {
	Generation     uint64
	StoreID        StoreID
	SegmentSize    uint32
	ActiveSegment  model.MapSegmentID
	NextSegment    model.MapSegmentID
	SealedSegments []SegmentRef
	Root           model.MapAddr
	Covered        model.CommitSeq
}

func (s CatalogSnapshot) Clone() CatalogSnapshot {
	s.SealedSegments = append([]SegmentRef(nil), s.SealedSegments...)
	return s
}

func (s CatalogSnapshot) validate() error {
	if s.Generation == 0 || s.StoreID == (StoreID{}) || s.ActiveSegment == 0 || s.ActiveSegment == model.MapSegmentID(math.MaxUint32) || s.NextSegment != s.ActiveSegment+1 || s.SegmentSize > math.MaxUint32 {
		return ErrInvalid
	}
	if !validSegmentHeader(s.headerFor(s.ActiveSegment)) {
		return ErrInvalid
	}
	var previous model.MapSegmentID
	for _, summary := range s.SealedSegments {
		if summary.SegmentID <= previous || summary.SegmentID >= s.ActiveSegment || summary.ValidEnd < SegmentHeaderSize || summary.ValidEnd > s.SegmentSize-SegmentFooterSize || summary.ValidEnd&uint32(Alignment-1) != 0 {
			return ErrInvalid
		}
		previous = summary.SegmentID
	}
	if s.Root == 0 {
		return nil
	}
	if !s.Root.Valid() {
		return ErrInvalid
	}
	if s.Root.SegmentID() == s.ActiveSegment {
		return nil
	}
	index := sort.Search(len(s.SealedSegments), func(i int) bool { return s.SealedSegments[i].SegmentID >= s.Root.SegmentID() })
	if index == len(s.SealedSegments) || s.SealedSegments[index].SegmentID != s.Root.SegmentID() || s.Root.Offset() >= s.SealedSegments[index].ValidEnd {
		return ErrInvalid
	}
	return nil
}

// SegmentRef is the catalog's authoritative membership and boundary. Sequence
// metadata remains in the sealed footer and is verified while opening.
type SegmentRef struct {
	SegmentID model.MapSegmentID
	ValidEnd  uint32
}

func (s CatalogSnapshot) headerFor(id model.MapSegmentID) SegmentHeader {
	previous := model.MapSegmentID(0)
	if id > 1 {
		previous = id - 1
	}
	return SegmentHeader{StoreID: s.StoreID, SegmentID: id, PreviousSegment: previous, SegmentSize: s.SegmentSize}
}

type CatalogPort interface {
	SnapshotMapStore() CatalogSnapshot
	InstallMapStoreRotation(expectGeneration uint64, sealed SegmentRef, newActive, next model.MapSegmentID) (CatalogSnapshot, error)
}

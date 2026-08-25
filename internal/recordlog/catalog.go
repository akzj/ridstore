package recordlog

import (
	"fmt"
	"math"
	"sort"
)

type CatalogSnapshot struct {
	Generation      uint64
	LogID           LogID
	SegmentSize     uint32
	MaxPayloadBytes uint32
	ActiveSegmentID SegmentID
	NextSegmentID   SegmentID
	SealedSegments  []SegmentSummary
}

func (s CatalogSnapshot) Clone() CatalogSnapshot {
	s.SealedSegments = append([]SegmentSummary(nil), s.SealedSegments...)
	return s
}

func (s CatalogSnapshot) validate() error {
	if s.Generation == 0 || s.LogID == (LogID{}) || s.ActiveSegmentID == 0 || s.NextSegmentID != s.ActiveSegmentID+1 || s.ActiveSegmentID == SegmentID(math.MaxUint32) || s.SegmentSize > math.MaxUint32 || s.MaxPayloadBytes == 0 {
		return ErrInvalidConfig
	}
	if _, err := EncodeSegmentHeader(s.headerFor(s.ActiveSegmentID)); err != nil {
		return err
	}
	maximum, err := PhysicalRecordSize(uint64(s.MaxPayloadBytes))
	if err != nil || maximum > s.SegmentSize-SegmentHeaderSize-SegmentFooterSize {
		return ErrInvalidConfig
	}
	var previous SegmentID
	for _, summary := range s.SealedSegments {
		if summary.SegmentID <= previous || summary.SegmentID >= s.ActiveSegmentID || summary.validate(s.SegmentSize) != nil {
			return ErrInvalidConfig
		}
		previous = summary.SegmentID
	}
	return nil
}

func (s CatalogSnapshot) headerFor(id SegmentID) SegmentHeader {
	previous := SegmentID(0)
	if id > 1 {
		previous = id - 1
	}
	return SegmentHeader{LogID: s.LogID, SegmentID: id, PreviousSegment: previous, SegmentSize: s.SegmentSize}
}

func (s CatalogSnapshot) sealedSummary(id SegmentID) (SegmentSummary, bool) {
	index := sort.Search(len(s.SealedSegments), func(i int) bool { return s.SealedSegments[i].SegmentID >= id })
	if index < len(s.SealedSegments) && s.SealedSegments[index].SegmentID == id {
		return s.SealedSegments[index], true
	}
	return SegmentSummary{}, false
}

type CatalogPort interface {
	SnapshotRecordLog() CatalogSnapshot
	InstallRecordLogRotation(expectGeneration uint64, sealed SegmentSummary, newActive, next SegmentID) (CatalogSnapshot, error)
	RemoveRecordLogSegment(expectGeneration uint64, sealed SegmentSummary) (CatalogSnapshot, error)
}

func validateRotationResult(before, after CatalogSnapshot, sealed SegmentSummary, newActive, next SegmentID) error {
	if after.Generation <= before.Generation || after.LogID != before.LogID || after.SegmentSize != before.SegmentSize || after.MaxPayloadBytes != before.MaxPayloadBytes || after.ActiveSegmentID != newActive || after.NextSegmentID != next {
		return fmt.Errorf("catalog rotation result: %w", ErrCorrupt)
	}
	installed, ok := after.sealedSummary(sealed.SegmentID)
	if !ok || installed != sealed {
		return fmt.Errorf("catalog sealed summary: %w", ErrCorrupt)
	}
	return after.validate()
}

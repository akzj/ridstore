package storecatalog

import (
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

const (
	FormatMajor = uint16(2)
	FormatMinor = uint16(0)
)

type StoreUUID [16]byte

type HardLimits struct {
	SegmentSize         uint64
	MaxValueSize        uint64
	MaxBatchBytes       uint64
	MaxBatchMutations   uint64
	MaxBatchConditions  uint64
	MaxOpenBatches      uint64
	MaxRecordLogPayload uint64
	IDReserveSize       uint64
	BatchIDReserveSize  uint64
}

type DataSegmentSummary = recordlog.SegmentSummary

type MapSegmentSummary struct {
	SegmentID model.MapSegmentID
	ValidEnd  uint32
}

type SegmentStats struct {
	SegmentID   recordlog.SegmentID
	LiveBytes   uint64
	LiveRecords uint64
}

type Manifest struct {
	Generation uint64
	StoreUUID  StoreUUID
	HardLimits HardLimits

	RecordLogID         recordlog.LogID
	ActiveDataSegmentID recordlog.SegmentID
	NextDataSegmentID   recordlog.SegmentID
	SealedDataSegments  []DataSegmentSummary

	ActiveMapSegmentID model.MapSegmentID
	NextMapSegmentID   model.MapSegmentID
	SealedMapSegments  []MapSegmentSummary
	MappingRoot        model.MapAddr

	CoveredCommitSeq       model.CommitSeq
	ReplayStart            recordlog.LogPos
	ReservedIDHigh         uint64
	ReservedBatchIDHigh    uint64
	IssuedBatchIDHighAtCut uint64
	OpenBatchIDsAtCut      []model.BatchID

	StatsCoveredCommitSeq model.CommitSeq
	SegmentStats          []SegmentStats
	MaintenanceGeneration uint64
}

func (m Manifest) Clone() Manifest {
	m.SealedDataSegments = append([]DataSegmentSummary(nil), m.SealedDataSegments...)
	m.SealedMapSegments = append([]MapSegmentSummary(nil), m.SealedMapSegments...)
	m.OpenBatchIDsAtCut = append([]model.BatchID(nil), m.OpenBatchIDsAtCut...)
	m.SegmentStats = append([]SegmentStats(nil), m.SegmentStats...)
	return m
}

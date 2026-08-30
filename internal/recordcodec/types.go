package recordcodec

import (
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

const (
	FormatVersion       = uint16(3)
	CommonHeaderSize    = uint32(16)
	PutHeaderSize       = uint32(32)
	CommitGroupHeadSize = uint32(32)
	DescriptorHeadSize  = uint32(40)
	MutationSize        = uint32(32)
	FixedRecordSize     = uint32(32)
)

type RecordType uint8

const (
	RecordTypePut RecordType = iota + 1
	RecordTypeCommitGroup
	RecordTypeAbort
	RecordTypeIDReserve
	RecordTypeBatchIDReserve
	RecordTypeCheckpoint
)

type DescriptorKind uint8

const (
	DescriptorUserCommit DescriptorKind = iota + 1
	DescriptorRelocation
)

type Operation uint8

const (
	OperationPut Operation = iota + 1
	OperationDelete
	OperationRelocate
)

type PutRecord struct {
	OriginBatchID model.BatchID
	RecordID      model.ID
	Value         []byte
}

type PutMetadata struct {
	OriginBatchID model.BatchID
	RecordID      model.ID
	ValueBytes    uint64
}

type Mutation struct {
	RecordID        model.ID
	NewAddr         recordlog.VAddr
	ExpectedOldAddr recordlog.VAddr
	Operation       Operation
	PhysicalSize    uint32
}

type Descriptor struct {
	Kind                DescriptorKind
	BatchID             model.BatchID
	CommitSeq           model.CommitSeq
	LogicalPayloadBytes uint64
	Mutations           []Mutation
}

type CommitGroup struct {
	Descriptors []Descriptor
}

type AbortRecord struct {
	BatchID model.BatchID
	Reason  uint32
}

type ReserveRecord struct {
	HighExclusive uint64
}

type CheckpointMarker struct {
	CoveredCommitSeq model.CommitSeq
}

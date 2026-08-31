package recordcodec

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

func testAddr(t *testing.T, segment recordlog.SegmentID, offset uint32) recordlog.VAddr {
	t.Helper()
	addr, err := recordlog.NewVAddr(segment, offset, 64)
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

func TestPutRoundTrip(t *testing.T) {
	t.Parallel()
	want := PutRecord{OriginBatchID: 11, RecordID: 22, Value: []byte("value")}
	encoded, err := EncodePut(want, 1024)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodePut(encoded, 1024)
	if err != nil || got.OriginBatchID != want.OriginBatchID || got.RecordID != want.RecordID || string(got.Value) != string(want.Value) {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if typ, err := TypeOf(encoded); err != nil || typ != RecordTypePut {
		t.Fatalf("type=%d err=%v", typ, err)
	}
	metadata, err := DecodePutMetadata(encoded[:PutHeaderSize], uint32(len(encoded)), 1024)
	if err != nil || metadata.OriginBatchID != want.OriginBatchID || metadata.RecordID != want.RecordID || metadata.ValueBytes != uint64(len(want.Value)) {
		t.Fatalf("metadata=%+v err=%v", metadata, err)
	}
	got.Value[0] = 'V'
	if encoded[PutHeaderSize] != 'V' {
		t.Fatal("decoded value must alias source")
	}
	if _, err := EncodePut(PutRecord{OriginBatchID: 1, RecordID: 2, Value: make([]byte, 9)}, 8); !errors.Is(err, ErrInvalid) {
		t.Fatalf("value limit err=%v", err)
	}
}

func TestTypeOfRejectsUnknownOrMalformedHeader(t *testing.T) {
	t.Parallel()
	encoded, _ := EncodeAbort(AbortRecord{BatchID: 1})
	unknown := append([]byte(nil), encoded...)
	unknown[6] = 99
	if _, err := TypeOf(unknown); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("unknown err=%v", err)
	}
	malformed := append([]byte(nil), encoded...)
	malformed[7] = 1
	if _, err := TypeOf(malformed); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("malformed err=%v", err)
	}
}

func TestCommitGroupRoundTrip(t *testing.T) {
	t.Parallel()
	a1 := testAddr(t, 1, 64)
	a2 := testAddr(t, 2, 64)
	a3 := testAddr(t, 3, 64)
	want := CommitGroup{Descriptors: []Descriptor{
		{
			Kind: DescriptorUserCommit, BatchID: 10, CommitSeq: 20, LogicalPayloadBytes: 7,
			Mutations: []Mutation{
				{RecordID: 1, NewAddr: a1, PhysicalSize: 64, Operation: OperationPut},
				{RecordID: 3, Operation: OperationDelete},
			},
		},
		{
			Kind: DescriptorRelocation, BatchID: 11, CommitSeq: 21, LogicalPayloadBytes: 9,
			Mutations: []Mutation{{RecordID: 2, NewAddr: a3, ExpectedOldAddr: a2, PhysicalSize: 64, Operation: OperationRelocate}},
		},
	}}
	encoded, err := EncodeCommitGroup(want, 4096)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeCommitGroup(encoded, 4096, 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Descriptors) != 2 || len(got.Descriptors[0].Mutations) != 2 || len(got.Descriptors[1].Mutations) != 1 {
		t.Fatalf("unexpected shape: %+v", got)
	}
	if got.Descriptors[0].CommitSeq != 20 || got.Descriptors[1].Mutations[0].ExpectedOldAddr != a2 {
		t.Fatalf("unexpected values: %+v", got)
	}
}

func TestEmptyUserCommitIsValid(t *testing.T) {
	t.Parallel()
	group := CommitGroup{Descriptors: []Descriptor{{Kind: DescriptorUserCommit, BatchID: 1, CommitSeq: 1}}}
	encoded, err := EncodeCommitGroup(group, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCommitGroup(encoded, 1024, 1, 0); err != nil {
		t.Fatal(err)
	}
}

func TestCommitGroupRejectsInvalidSemantics(t *testing.T) {
	t.Parallel()
	addr := testAddr(t, 1, 64)
	tests := []CommitGroup{
		{},
		{Descriptors: []Descriptor{{Kind: DescriptorUserCommit, BatchID: 0, CommitSeq: 1}}},
		{Descriptors: []Descriptor{{Kind: DescriptorRelocation, BatchID: 1, CommitSeq: 1}}},
		{Descriptors: []Descriptor{{Kind: DescriptorUserCommit, BatchID: 1, CommitSeq: 1, Mutations: []Mutation{{RecordID: 2, NewAddr: addr, Operation: OperationPut}, {RecordID: 1, NewAddr: addr, Operation: OperationPut}}}}},
		{Descriptors: []Descriptor{{Kind: DescriptorUserCommit, BatchID: 1, CommitSeq: 1}, {Kind: DescriptorUserCommit, BatchID: 1, CommitSeq: 2}}},
		{Descriptors: []Descriptor{{Kind: DescriptorUserCommit, BatchID: 1, CommitSeq: 1}, {Kind: DescriptorUserCommit, BatchID: 2, CommitSeq: 3}}},
	}
	for i, group := range tests {
		if _, err := EncodeCommitGroup(group, 4096); err == nil {
			t.Fatalf("case %d accepted", i)
		}
	}
}

func TestCommitGroupDecoderRejectsCorruptionWithoutLargeAllocation(t *testing.T) {
	t.Parallel()
	group := CommitGroup{Descriptors: []Descriptor{{Kind: DescriptorUserCommit, BatchID: 1, CommitSeq: 1}}}
	seed, _ := EncodeCommitGroup(group, 1024)

	hugeCount := append([]byte(nil), seed...)
	binary.LittleEndian.PutUint32(hugeCount[16:20], math.MaxUint32)
	if _, err := DecodeCommitGroup(hugeCount, 1024, math.MaxUint32, math.MaxUint32); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("huge count err=%v", err)
	}

	wrongFirst := append([]byte(nil), seed...)
	binary.LittleEndian.PutUint64(wrongFirst[24:32], 2)
	if _, err := DecodeCommitGroup(wrongFirst, 1024, 1, 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("wrong first sequence err=%v", err)
	}

	trailing := append(append([]byte(nil), seed...), 0)
	binary.LittleEndian.PutUint32(trailing[12:16], uint32(len(trailing)))
	if _, err := DecodeCommitGroup(trailing, 1024, 1, 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("trailing byte err=%v", err)
	}
}

func TestFixedRecordsRoundTrip(t *testing.T) {
	t.Parallel()
	abortBytes, err := EncodeAbort(AbortRecord{BatchID: 9, Reason: 4})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeAbort(abortBytes); err != nil || got != (AbortRecord{BatchID: 9, Reason: 4}) {
		t.Fatalf("abort=%+v err=%v", got, err)
	}
	for _, typ := range []RecordType{RecordTypeIDReserve, RecordTypeBatchIDReserve} {
		encoded, err := EncodeReserve(typ, ReserveRecord{HighExclusive: 100})
		if err != nil {
			t.Fatal(err)
		}
		if got, err := DecodeReserve(encoded, typ); err != nil || got.HighExclusive != 100 {
			t.Fatalf("type=%d reserve=%+v err=%v", typ, got, err)
		}
	}
	checkpoint := EncodeCheckpoint(CheckpointMarker{CoveredCommitSeq: model.CommitSeq(77)})
	if got, err := DecodeCheckpoint(checkpoint); err != nil || got.CoveredCommitSeq != 77 {
		t.Fatalf("checkpoint=%+v err=%v", got, err)
	}
}

func TestSizeArithmetic(t *testing.T) {
	t.Parallel()
	if got, err := PutPayloadSize(10); err != nil || got != 42 {
		t.Fatalf("put size=%d err=%v", got, err)
	}
	if got, err := DescriptorSize(3); err != nil || got != 136 {
		t.Fatalf("descriptor size=%d err=%v", got, err)
	}
	if _, err := DescriptorSize(math.MaxUint64); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("overflow err=%v", err)
	}
}

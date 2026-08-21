package format

import (
	"errors"
	"testing"

	"github.com/akzj/ridstore/internal/base"
)

func TestCommitDescriptorGoldenAndValidation(t *testing.T) {
	t.Parallel()

	addr1, _ := base.NewVAddr(2, 4096)
	entries1 := []MutationEntry{{RecordID: 1, Operation: MutationPut, NewVAddr: addr1}}
	entries2 := []MutationEntry{{RecordID: 2, Operation: MutationDelete}}
	part1, err := EncodeMutationEntries(DescriptorCommit, entries1)
	if err != nil {
		t.Fatal(err)
	}
	part2, err := EncodeMutationEntries(DescriptorCommit, entries2)
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenSHA256(t, part1, "8a57f09e283ec72445627ca581eaf45f1d1b4d3b7ac72a766de8ce87620768e3")

	sealMeta := DescriptorSeal{
		CommitSeq: 5, PartCount: 2, MutationCount: 2, LogicalPayloadBytes: 17,
		FirstPartFrameSeq: 10, LastPartFrameSeq: 11,
	}
	sealPayload, err := EncodeDescriptorSealPayload(sealMeta, [][]byte{part1, part2})
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenSHA256(t, sealPayload[:], "79282e46cdef3cc2a0606016fc53de50ebafeed0d4b8f05701b079e67b5c6414")

	parts := []Frame{
		{Type: FrameTypeCommitPart, FrameSeq: 10, BatchID: 7, Payload: part1},
		{Type: FrameTypeCommitPart, FrameSeq: 11, BatchID: 7, Payload: part2},
	}
	decoded, err := ValidateDescriptorFrames(DescriptorCommit, parts, Frame{
		Type: FrameTypeCommitSeal, FrameSeq: 12, BatchID: 7, Payload: sealPayload[:],
	}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.BatchID != 7 || decoded.Seal.CommitSeq != 5 || len(decoded.Entries) != 2 || decoded.Entries[1].Operation != MutationDelete {
		t.Fatalf("unexpected descriptor: %+v", decoded)
	}
}

func TestRelocationAndEmptyDescriptors(t *testing.T) {
	t.Parallel()

	oldAddr, _ := base.NewVAddr(1, 4096)
	newAddr, _ := base.NewVAddr(2, 8192)
	payload, err := EncodeMutationEntries(DescriptorRelocation, []MutationEntry{{
		RecordID: 9, Operation: MutationRelocate, NewVAddr: newAddr, ExpectedOldAddr: oldAddr,
	}})
	if err != nil {
		t.Fatal(err)
	}
	seal, err := EncodeDescriptorSealPayload(DescriptorSeal{
		CommitSeq: 3, PartCount: 1, MutationCount: 1,
		FirstPartFrameSeq: 20, LastPartFrameSeq: 20,
	}, [][]byte{payload})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateDescriptorFrames(DescriptorRelocation,
		[]Frame{{Type: FrameTypeRelocationPart, FrameSeq: 20, BatchID: 8, Payload: payload}},
		Frame{Type: FrameTypeRelocationSeal, FrameSeq: 21, BatchID: 8, Payload: seal[:]}, 1); err != nil {
		t.Fatal(err)
	}

	empty, err := EncodeDescriptorSealPayload(DescriptorSeal{CommitSeq: 4}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateDescriptorFrames(DescriptorCommit, nil,
		Frame{Type: FrameTypeCommitSeal, FrameSeq: 30, BatchID: 9, Payload: empty[:]}, 1); err != nil {
		t.Fatal(err)
	}
}

func TestDescriptorRejectsInvalidEntries(t *testing.T) {
	t.Parallel()

	addr, _ := base.NewVAddr(1, 4096)
	tests := []struct {
		kind    DescriptorKind
		entries []MutationEntry
	}{
		{DescriptorCommit, []MutationEntry{{RecordID: 0, Operation: MutationDelete}}},
		{DescriptorCommit, []MutationEntry{{RecordID: 1, Operation: MutationPut}}},
		{DescriptorCommit, []MutationEntry{{RecordID: 1, Operation: MutationRelocate, NewVAddr: addr, ExpectedOldAddr: addr}}},
		{DescriptorRelocation, []MutationEntry{{RecordID: 1, Operation: MutationRelocate, NewVAddr: addr}}},
		{DescriptorCommit, []MutationEntry{{RecordID: 2, Operation: MutationDelete}, {RecordID: 1, Operation: MutationDelete}}},
	}
	for i, tt := range tests {
		if _, err := EncodeMutationEntries(tt.kind, tt.entries); !errors.Is(err, base.ErrInvalidConfig) {
			t.Fatalf("case %d error=%v", i, err)
		}
	}
}

func TestDescriptorRejectsCorruptionAndCrossFrameMismatch(t *testing.T) {
	t.Parallel()

	addr, _ := base.NewVAddr(1, 4096)
	payload, _ := EncodeMutationEntries(DescriptorCommit, []MutationEntry{{RecordID: 1, Operation: MutationPut, NewVAddr: addr}})
	seal, _ := EncodeDescriptorSealPayload(DescriptorSeal{
		CommitSeq: 1, PartCount: 1, MutationCount: 1,
		FirstPartFrameSeq: 4, LastPartFrameSeq: 4,
	}, [][]byte{payload})
	part := Frame{Type: FrameTypeCommitPart, FrameSeq: 4, BatchID: 2, Payload: payload}
	sealFrame := Frame{Type: FrameTypeCommitSeal, FrameSeq: 5, BatchID: 2, Payload: seal[:]}

	badCRC := seal
	badCRC[40] ^= 1
	if _, err := ValidateDescriptorFrames(DescriptorCommit, []Frame{part}, Frame{
		Type: FrameTypeCommitSeal, FrameSeq: 5, BatchID: 2, Payload: badCRC[:],
	}, 1); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("CRC error=%v", err)
	}

	wrongBatch := part
	wrongBatch.BatchID = 3
	if _, err := ValidateDescriptorFrames(DescriptorCommit, []Frame{wrongBatch}, sealFrame, 1); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("batch error=%v", err)
	}

	nonAdjacent := sealFrame
	nonAdjacent.FrameSeq = 6
	if _, err := ValidateDescriptorFrames(DescriptorCommit, []Frame{part}, nonAdjacent, 1); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("adjacency error=%v", err)
	}

	wrongRangePayload, err := EncodeDescriptorSealPayload(DescriptorSeal{
		CommitSeq: 1, PartCount: 1, MutationCount: 1,
		FirstPartFrameSeq: 5, LastPartFrameSeq: 5,
	}, [][]byte{payload})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateDescriptorFrames(DescriptorCommit, []Frame{part}, Frame{
		Type: FrameTypeCommitSeal, FrameSeq: 5, BatchID: 2, Payload: wrongRangePayload[:],
	}, 1); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("part range error=%v", err)
	}
}

func FuzzDecodeMutationEntries(f *testing.F) {
	addr, _ := base.NewVAddr(1, 4096)
	seed, _ := EncodeMutationEntries(DescriptorCommit, []MutationEntry{{RecordID: 1, Operation: MutationPut, NewVAddr: addr}})
	f.Add(seed)
	f.Add([]byte("short"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeMutationEntries(DescriptorCommit, data, 1024)
		_, _ = DecodeMutationEntries(DescriptorRelocation, data, 1024)
	})
}

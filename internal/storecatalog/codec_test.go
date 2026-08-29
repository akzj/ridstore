package storecatalog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

func testManifest() Manifest {
	first, _ := recordlog.NewVAddr(1, recordlog.SegmentHeaderSize, 64)
	root, _ := model.NewMapAddr(2, 64)
	replay, _ := recordlog.NewLogPos(2, recordlog.SegmentHeaderSize)
	maxPut, _ := recordcodec.PutPayloadSize(64 << 20)
	return Manifest{
		Generation: 1,
		StoreUUID:  StoreUUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		HardLimits: HardLimits{
			SegmentSize: 256 << 20, MaxValueSize: 64 << 20, MaxBatchBytes: 256 << 20,
			MaxBatchMutations: 1_000_000, MaxBatchConditions: 1_000_000, MaxOpenBatches: 1024,
			MaxRecordLogPayload: uint64(maxPut), IDReserveSize: 1 << 20, BatchIDReserveSize: 1 << 16,
		},
		RecordLogID:            recordlog.LogID{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		ActiveDataSegmentID:    2,
		NextDataSegmentID:      3,
		SealedDataSegments:     []DataSegmentSummary{{SegmentID: 1, ValidEnd: 128, RecordCount: 1, FirstAddr: first, LastAddr: first}},
		ActiveMapSegmentID:     2,
		NextMapSegmentID:       3,
		SealedMapSegments:      []MapSegmentSummary{{SegmentID: 1, ValidEnd: 128}},
		MappingRoot:            root,
		MappingEntryCount:      1,
		ReplayStart:            replay,
		ReservedIDHigh:         100,
		ReservedBatchIDHigh:    100,
		IssuedBatchIDHighAtCut: 50,
		OpenBatchIDsAtCut:      []model.BatchID{2, 7},
		SegmentStats:           []SegmentStats{{SegmentID: 1, LiveBytes: 64, LiveRecords: 1}},
	}
}

func TestManifestRoundTrip(t *testing.T) {
	t.Parallel()
	want := testManifest()
	encoded, err := Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != want.Generation || got.StoreUUID != want.StoreUUID || got.RecordLogID != want.RecordLogID || got.MappingRoot != want.MappingRoot || got.MappingEntryCount != want.MappingEntryCount || got.ReplayStart != want.ReplayStart || len(got.SealedDataSegments) != 1 || len(got.OpenBatchIDsAtCut) != 2 || len(got.SegmentStats) != 1 {
		t.Fatalf("unexpected manifest: %+v", got)
	}
	got.OpenBatchIDsAtCut[0] = 99
	if want.OpenBatchIDsAtCut[0] == 99 {
		t.Fatal("decoded manifest must own slices")
	}
}

func TestInspectHeaderAcceptsChecksummedFutureVersion(t *testing.T) {
	encoded, err := Encode(testManifest())
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint16(encoded[8:10], FormatMajor+1)
	binary.LittleEndian.PutUint32(encoded[52:56], crc32.Checksum(encoded[:52], crc32.MakeTable(crc32.Castagnoli)))
	header, err := InspectHeader(encoded)
	if err != nil || header.FormatMajor != FormatMajor+1 || header.Generation != testManifest().Generation || header.StoreUUID != testManifest().StoreUUID {
		t.Fatalf("header=%+v error=%v", header, err)
	}
	if _, err := Decode(encoded); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("decode error=%v", err)
	}
}

func TestDecodeRejectsOlderMinorWithoutMappingEntryCount(t *testing.T) {
	encoded, err := Encode(testManifest())
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint16(encoded[10:12], FormatMinor-1)
	binary.LittleEndian.PutUint32(encoded[52:56], crc32.Checksum(encoded[:52], crc32.MakeTable(crc32.Castagnoli)))
	if _, err := Decode(encoded); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("decode error=%v", err)
	}
}

func TestManifestAllowsCanonicalEmptyMappingRoot(t *testing.T) {
	manifest := testManifest()
	manifest.MappingRoot = 0
	manifest.MappingEntryCount = 0
	manifest.CoveredCommitSeq = 7
	manifest.StatsCoveredCommitSeq = 7
	encoded, err := Encode(manifest)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.MappingRoot != 0 || decoded.CoveredCommitSeq != 7 {
		t.Fatalf("decoded=%+v", decoded)
	}
}

func TestManifestDetectsCorruption(t *testing.T) {
	t.Parallel()
	seed, _ := Encode(testManifest())
	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{"header", func(value []byte) { value[16] ^= 1 }},
		{"payload", func(value []byte) { value[containerHeaderSize+10] ^= 1 }},
		{"unsupported", func(value []byte) { binary.LittleEndian.PutUint16(value[8:10], FormatMajor+1) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := append([]byte(nil), seed...)
			tt.mutate(value)
			_, err := Decode(value)
			if tt.name == "unsupported" {
				if !errors.Is(err, ErrUnsupported) {
					t.Fatalf("err=%v", err)
				}
			} else if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestManifestValidationRejectsUnsafeLimitsAndReferences(t *testing.T) {
	t.Parallel()
	tests := []func(*Manifest){
		func(m *Manifest) { m.HardLimits.SegmentSize = 64 },
		func(m *Manifest) { m.HardLimits.MaxRecordLogPayload = 1024 },
		func(m *Manifest) { m.ReplayStart.SegmentID = 99 },
		func(m *Manifest) { m.MappingRoot = model.MapAddr(uint64(99)<<32 | 64) },
		func(m *Manifest) { m.MappingRoot = model.MapAddr(1) },
		func(m *Manifest) { m.MappingEntryCount = 0 },
		func(m *Manifest) { m.SegmentStats[0].LiveRecords = 2 },
		func(m *Manifest) { m.OpenBatchIDsAtCut = []model.BatchID{7, 2} },
	}
	for i, mutate := range tests {
		manifest := testManifest()
		mutate(&manifest)
		if err := Validate(manifest); !errors.Is(err, ErrInvalid) {
			t.Fatalf("case %d err=%v", i, err)
		}
	}
}

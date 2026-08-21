package format

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"reflect"
	"testing"

	"github.com/akzj/ridstore/internal/base"
)

func testManifest() Manifest {
	replay, _ := base.NewLogPos(1, 4096)
	return Manifest{
		Generation: 1, StoreUUID: testStoreUUID,
		HardLimits: HardLimits{
			SegmentSize: 256 << 20, MaxValueSize: 64 << 20, MaxBatchBytes: 256 << 20,
			MaxBatchMutations: 1_000_000, MaxBatchConditions: 1_000_000,
			MaxOpenBatches: 1024, IDReserveSize: 1 << 20, BatchIDReserveSize: 1 << 16,
		},
		NextDataSegmentID: 2, NextMapSegmentID: 2,
		ActiveDataSegmentID: 1, ActiveMapSegmentID: 1,
		ReplayStart: replay, ReservedIDHighExclusive: 1,
		ReservedBatchIDHighExclusive: 1, IssuedBatchIDHighExclusiveAtCut: 1,
		NextFrameSeq: 1, NextCommitSeq: 1,
	}
}

func TestManifestGoldenAndRoundTrip(t *testing.T) {
	t.Parallel()
	m := testManifest()
	encoded, err := EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenSHA256(t, encoded, "2d5af4a8a65730d54a3346810a1870d75428ead4cf4fc213fd2516444c04269b")
	decoded, err := DecodeManifest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, m) {
		t.Fatalf("decoded manifest differs:\n got=%+v\nwant=%+v", decoded, m)
	}
}

func TestManifestWithFilesStatsAndOpenBatches(t *testing.T) {
	t.Parallel()
	m := testManifest()
	m.Generation = 9
	m.NextDataSegmentID, m.ActiveDataSegmentID = 4, 3
	m.NextMapSegmentID, m.ActiveMapSegmentID = 4, 3
	m.SealedDataSegments = []FileSummary{{FileID: 1, ValidEnd: 8192, FirstSeq: 1, LastSeq: 8}, {FileID: 2, ValidEnd: 12288, FirstSeq: 9, LastSeq: 12}}
	m.SealedMappingSegments = []FileSummary{{FileID: 1, ValidEnd: 8192, FirstSeq: 1, LastSeq: 2}, {FileID: 2, ValidEnd: 12288, FirstSeq: 3, LastSeq: 4}}
	root, _ := base.NewMapAddr(2, 4096)
	replay, _ := base.NewLogPos(3, 8192)
	m.MappingRoot, m.CoveredCommitSeq, m.StatsCoveredCommitSeq = root, 7, 7
	m.CutFrameSeq, m.NextFrameSeq, m.NextCommitSeq, m.ReplayStart = 12, 13, 8, replay
	m.ReservedBatchIDHighExclusive, m.IssuedBatchIDHighExclusiveAtCut = 100, 50
	m.OpenBatchIDsAtCut = []base.BatchID{11, 49}
	m.SegmentStats = []SegmentStatsEntry{{SegmentID: 1, ExactLiveBytes: 128, ExactLiveRecords: 1}, {SegmentID: 3, ExactLiveBytes: 256, ExactLiveRecords: 2}}
	encoded, err := EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeManifest(encoded)
	if err != nil || !reflect.DeepEqual(decoded, m) {
		t.Fatalf("decoded=%+v error=%v", decoded, err)
	}
}

func TestContainerRejectsCorruptionAndUnknownRequired(t *testing.T) {
	t.Parallel()
	c := Container{Magic: ManifestMagic, Generation: 1, StoreUUID: testStoreUUID, TLVs: []TLV{{Type: 99, Required: true, Value: []byte{1}}}}
	encoded, err := EncodeContainer(c)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeContainer(encoded, ManifestMagic, 1024)
	if err != nil || len(decoded.TLVs) != 1 {
		t.Fatalf("container decode=%+v error=%v", decoded, err)
	}
	if _, err := DecodeManifest(encoded); !errors.Is(err, base.ErrUnsupported) {
		t.Fatalf("unknown required error=%v", err)
	}

	bad := append([]byte(nil), encoded...)
	bad[len(bad)-1] = 1
	if _, err := DecodeContainer(bad, ManifestMagic, 1024); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("padding/checksum error=%v", err)
	}
}

func TestInspectContainerHeaderReportsUnknownVersion(t *testing.T) {
	t.Parallel()
	encoded, err := EncodeContainer(Container{Magic: ManifestMagic, Generation: 7, StoreUUID: testStoreUUID, TLVs: []TLV{{Type: 1, Value: []byte{1}}}})
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint16(encoded[8:10], FormatMajorVersion+1)
	binary.LittleEndian.PutUint32(encoded[52:56], 0)
	binary.LittleEndian.PutUint32(encoded[52:56], crc32.Checksum(encoded[:ContainerHeaderSize], castagnoliTable))
	header, err := InspectContainerHeader(encoded[:ContainerHeaderSize], ManifestMagic, uint64(len(encoded)), 1024)
	if err != nil || header.MajorVersion != FormatMajorVersion+1 || header.MinorVersion != FormatMinorVersion || header.Generation != 7 || header.StoreUUID != testStoreUUID {
		t.Fatalf("header=%+v error=%v", header, err)
	}
	if _, err := DecodeContainer(encoded, ManifestMagic, 1024); !errors.Is(err, base.ErrUnsupported) {
		t.Fatalf("decode error=%v", err)
	}
}

func TestManifestRejectsCrossFieldViolations(t *testing.T) {
	t.Parallel()
	tests := []func(*Manifest){
		func(m *Manifest) { m.StatsCoveredCommitSeq = 1 },
		func(m *Manifest) { m.NextDataSegmentID = 1 },
		func(m *Manifest) { m.ReservedBatchIDHighExclusive = 0 },
		func(m *Manifest) { m.OpenBatchIDsAtCut = []base.BatchID{1}; m.IssuedBatchIDHighExclusiveAtCut = 1 },
		func(m *Manifest) {
			m.SegmentStats = []SegmentStatsEntry{{SegmentID: 9, ExactLiveBytes: 1, ExactLiveRecords: 1}}
		},
		func(m *Manifest) { m.ReplayStart, _ = base.NewLogPos(9, 4096) },
		func(m *Manifest) { m.MappingRoot, _ = base.NewMapAddr(9, 4096) },
		func(m *Manifest) {
			m.NextDataSegmentID = 3
			m.SealedDataSegments = []FileSummary{{FileID: 2, ValidEnd: m.HardLimits.SegmentSize, FirstSeq: 1, LastSeq: 1}}
		},
	}
	for i, mutate := range tests {
		m := testManifest()
		mutate(&m)
		if _, err := EncodeManifest(m); !errors.Is(err, base.ErrInvalidConfig) {
			t.Fatalf("case %d error=%v", i, err)
		}
	}
}

func TestManifestAllowsFourGiBSegmentAndListedAddressBounds(t *testing.T) {
	t.Parallel()
	m := testManifest()
	m.HardLimits.SegmentSize = uint64(1) << 32
	m.NextDataSegmentID, m.ActiveDataSegmentID = 3, 2
	m.NextMapSegmentID, m.ActiveMapSegmentID = 3, 2
	m.SealedDataSegments = []FileSummary{{FileID: 1, ValidEnd: 8192, FirstSeq: 1, LastSeq: 2}}
	m.SealedMappingSegments = []FileSummary{{FileID: 1, ValidEnd: 8192, FirstSeq: 1, LastSeq: 1}}
	m.ReplayStart, _ = base.NewLogPos(1, 8192)
	m.MappingRoot, _ = base.NewMapAddr(1, 4096)
	if _, err := EncodeManifest(m); err != nil {
		t.Fatal(err)
	}
	m.MappingRoot, _ = base.NewMapAddr(1, 8192)
	if _, err := EncodeManifest(m); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("root at valid end error=%v", err)
	}
}

func FuzzDecodeManifest(f *testing.F) {
	seed, _ := EncodeManifest(testManifest())
	f.Add(seed)
	f.Add([]byte("short"))
	f.Fuzz(func(t *testing.T, data []byte) { _, _ = DecodeManifest(data) })
}

func TestManifestTLVScalarIsLittleEndian(t *testing.T) {
	t.Parallel()
	b := scalar64(0x0807060504030201)
	if binary.LittleEndian.Uint64(b) != 0x0807060504030201 || !equalBytes(b, []byte{1, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatalf("scalar bytes=%x", b)
	}
}

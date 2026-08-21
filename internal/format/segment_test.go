package format

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/akzj/ridstore/internal/base"
)

var testStoreUUID = StoreUUID{
	0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
	0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
}

func TestSegmentHeaderGoldenAndRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		header     SegmentHeader
		wantSHA256 string
	}{
		{
			name: "data",
			header: SegmentHeader{
				Kind: SegmentKindData, StoreUUID: testStoreUUID, FileID: 7,
				CreatedUnixNano: 0x0102030405060708, FirstSeq: 11,
			},
			wantSHA256: "3fa80447c317933216777e8eaec8a48d4817a4c7a4066636cc281e3307f2f30a",
		},
		{
			name: "mapping",
			header: SegmentHeader{
				Kind: SegmentKindMapping, StoreUUID: testStoreUUID, FileID: 9,
				CreatedUnixNano: 0x1112131415161718, FirstSeq: 13,
			},
			wantSHA256: "3e732c0c58ae3025bfacc5bafc593525d2c6738e658007b07130aecba34c0c3d",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			encoded, err := EncodeSegmentHeader(tt.header)
			if err != nil {
				t.Fatal(err)
			}
			assertGoldenSHA256(t, encoded[:], tt.wantSHA256)
			decoded, err := DecodeSegmentHeader(encoded[:])
			if err != nil {
				t.Fatal(err)
			}
			if decoded != tt.header {
				t.Fatalf("decoded = %+v, want %+v", decoded, tt.header)
			}
		})
	}
}

func TestSegmentHeaderLittleEndianLayout(t *testing.T) {
	t.Parallel()

	encoded, err := EncodeSegmentHeader(SegmentHeader{
		Kind: SegmentKindData, StoreUUID: testStoreUUID,
		FileID: 0x78563412, CreatedUnixNano: 0x0807060504030201,
		FirstSeq: 0x1817161514131211,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := encoded[32:36], []byte{0x12, 0x34, 0x56, 0x78}; !equalBytes(got, want) {
		t.Fatalf("FileID bytes = %x, want %x", got, want)
	}
	if got, want := encoded[40:48], []byte{1, 2, 3, 4, 5, 6, 7, 8}; !equalBytes(got, want) {
		t.Fatalf("CreatedUnixNano bytes = %x, want %x", got, want)
	}
	if got, want := encoded[48:56], []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18}; !equalBytes(got, want) {
		t.Fatalf("FirstSeq bytes = %x, want %x", got, want)
	}
}

func TestSegmentFooterGoldenAndRoundTrip(t *testing.T) {
	t.Parallel()

	data := DataSegmentFooter{
		SegmentID: 7, ValidDataEnd: 0x2000,
		FirstFrameSeq: 11, LastFrameSeq: 19, FrameCount: 8,
		MinCommitSeq: 3, MaxCommitSeq: 5,
	}
	dataBytes, err := EncodeDataSegmentFooter(data)
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenSHA256(t, dataBytes[:], "9e99a7f9b65effb951aa3ab0b4b6ad125b4dde29a18addeef5b2c4dea82cc10c")
	decodedData, err := DecodeDataSegmentFooter(dataBytes[:])
	if err != nil {
		t.Fatal(err)
	}
	if decodedData != data {
		t.Fatalf("decoded data footer = %+v, want %+v", decodedData, data)
	}

	mapping := MappingSegmentFooter{
		SegmentID: 9, ValidNodeEnd: 0x3000,
		FirstNodeSeq: 13, LastNodeSeq: 21, NodeCount: 7,
	}
	mappingBytes, err := EncodeMappingSegmentFooter(mapping)
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenSHA256(t, mappingBytes[:], "3eb1949da73bb198e2b7bd3be0e654c5df0808eefdd535cb42b4f066c1287fa5")
	decodedMapping, err := DecodeMappingSegmentFooter(mappingBytes[:])
	if err != nil {
		t.Fatal(err)
	}
	if decodedMapping != mapping {
		t.Fatalf("decoded mapping footer = %+v, want %+v", decodedMapping, mapping)
	}
}

func TestSegmentCodecRejectsCorruption(t *testing.T) {
	t.Parallel()

	header, err := EncodeSegmentHeader(SegmentHeader{
		Kind: SegmentKindData, StoreUUID: testStoreUUID, FileID: 1, FirstSeq: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("truncated", func(t *testing.T) {
		if _, err := DecodeSegmentHeader(header[:100]); !errors.Is(err, base.ErrCorrupt) {
			t.Fatalf("error = %v, want ErrCorrupt", err)
		}
	})
	t.Run("checksum", func(t *testing.T) {
		bad := header
		bad[20] ^= 0xff
		if _, err := DecodeSegmentHeader(bad[:]); !errors.Is(err, base.ErrCorrupt) {
			t.Fatalf("error = %v, want ErrCorrupt", err)
		}
	})
	t.Run("reserved", func(t *testing.T) {
		bad := header
		bad[100] = 1
		rewriteChecksum(bad[:], 64)
		if _, err := DecodeSegmentHeader(bad[:]); !errors.Is(err, base.ErrCorrupt) {
			t.Fatalf("error = %v, want ErrCorrupt", err)
		}
	})
	t.Run("unknown major", func(t *testing.T) {
		bad := header
		binary.LittleEndian.PutUint16(bad[8:10], 2)
		rewriteChecksum(bad[:], 64)
		if _, err := DecodeSegmentHeader(bad[:]); !errors.Is(err, base.ErrUnsupported) {
			t.Fatalf("error = %v, want ErrUnsupported", err)
		}
	})
}

func TestFooterCodecRejectsInvalidFields(t *testing.T) {
	t.Parallel()

	if _, err := EncodeDataSegmentFooter(DataSegmentFooter{}); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("data footer error = %v, want ErrInvalidConfig", err)
	}
	if _, err := EncodeMappingSegmentFooter(MappingSegmentFooter{}); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("mapping footer error = %v, want ErrInvalidConfig", err)
	}
	if _, err := EncodeDataSegmentFooter(DataSegmentFooter{
		SegmentID: 1, ValidDataEnd: uint64(1) << 32,
		FirstFrameSeq: 1, LastFrameSeq: 1, FrameCount: 1,
	}); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("oversized data footer error = %v, want ErrInvalidConfig", err)
	}

	valid, err := EncodeDataSegmentFooter(DataSegmentFooter{
		SegmentID: 1, ValidDataEnd: 8192,
		FirstFrameSeq: 1, LastFrameSeq: 1, FrameCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	valid[100] = 1
	rewriteChecksum(valid[:], 64)
	if _, err := DecodeDataSegmentFooter(valid[:]); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("reserved error = %v, want ErrCorrupt", err)
	}
}

func FuzzDecodeSegmentStructures(f *testing.F) {
	header, _ := EncodeSegmentHeader(SegmentHeader{
		Kind: SegmentKindData, StoreUUID: testStoreUUID, FileID: 1, FirstSeq: 1,
	})
	dataFooter, _ := EncodeDataSegmentFooter(DataSegmentFooter{
		SegmentID: 1, ValidDataEnd: 8192,
		FirstFrameSeq: 1, LastFrameSeq: 1, FrameCount: 1,
	})
	mappingFooter, _ := EncodeMappingSegmentFooter(MappingSegmentFooter{
		SegmentID: 1, ValidNodeEnd: 8192,
		FirstNodeSeq: 1, LastNodeSeq: 1, NodeCount: 1,
	})
	f.Add(byte(0), header[:])
	f.Add(byte(1), dataFooter[:])
	f.Add(byte(2), mappingFooter[:])
	f.Add(byte(0), []byte("short"))

	f.Fuzz(func(t *testing.T, kind byte, data []byte) {
		switch kind % 3 {
		case 0:
			_, _ = DecodeSegmentHeader(data)
		case 1:
			_, _ = DecodeDataSegmentFooter(data)
		case 2:
			_, _ = DecodeMappingSegmentFooter(data)
		}
	})
}

func assertGoldenSHA256(t *testing.T, data []byte, want string) {
	t.Helper()
	got := fmt.Sprintf("%x", sha256.Sum256(data))
	if got != want {
		t.Fatalf("golden SHA-256 = %s, want %s", got, want)
	}
}

func rewriteChecksum(data []byte, offset int) {
	for i := 0; i < 4; i++ {
		data[offset+i] = 0
	}
	binary.LittleEndian.PutUint32(data[offset:offset+4], checksum(data))
}

func checksum(data []byte) uint32 {
	return crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

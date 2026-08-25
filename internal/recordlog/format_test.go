package recordlog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func testLogID() LogID {
	return LogID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
}

func TestSegmentHeaderRoundTrip(t *testing.T) {
	t.Parallel()
	want := SegmentHeader{LogID: testLogID(), SegmentID: 17, PreviousSegment: 16, SegmentSize: 256 << 20}
	encoded, err := EncodeSegmentHeader(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeSegmentHeader(encoded[:])
	if err != nil || got != want {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	encoded[44] = 1
	binary.LittleEndian.PutUint32(encoded[60:64], crc32.Checksum(encoded[:60], crcTable))
	if _, err := DecodeSegmentHeader(encoded[:]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("reserved byte err=%v", err)
	}
}

func TestRecordRoundTripAndOwnership(t *testing.T) {
	t.Parallel()
	payload := []byte("record payload")
	physical, err := PhysicalRecordSize(uint64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	addr, err := NewVAddr(3, 4096, physical)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeRecord(addr, payload)
	if err != nil {
		t.Fatal(err)
	}
	header, got, err := DecodeRecord(encoded)
	if err != nil || header.Addr != addr || string(got) != string(payload) || header.PhysicalSize != physical {
		t.Fatalf("header=%+v payload=%q err=%v", header, got, err)
	}
	got[0] = 'R'
	if encoded[RecordHeaderSize] != 'R' {
		t.Fatal("decoded payload must alias source")
	}
}

func TestRecordDetectsCorruption(t *testing.T) {
	t.Parallel()
	payload := []byte("payload")
	physical, _ := PhysicalRecordSize(uint64(len(payload)))
	addr, _ := NewVAddr(1, SegmentHeaderSize, physical)
	seed, _ := EncodeRecord(addr, payload)
	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{"header", func(value []byte) { value[8] ^= 1 }},
		{"payload", func(value []byte) { value[RecordHeaderSize] ^= 1 }},
		{"padding", func(value []byte) { value[len(value)-1] = 1 }},
		{"address-tag", func(value []byte) {
			binary.LittleEndian.PutUint64(value[16:24], binary.LittleEndian.Uint64(value[16:24])|2)
			binary.LittleEndian.PutUint32(value[28:32], crc32.Checksum(value[:28], crcTable))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := append([]byte(nil), seed...)
			tt.mutate(value)
			if _, _, err := DecodeRecord(value); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestSegmentFooterRoundTrip(t *testing.T) {
	t.Parallel()
	first, _ := NewVAddr(5, 64, 64)
	last, _ := NewVAddr(5, 128, 128)
	want := SegmentFooter{SegmentID: 5, DataEnd: 256, FirstAddr: first, LastAddr: last, RecordCount: 2}
	encoded, err := EncodeSegmentFooter(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeSegmentFooter(encoded[:])
	if err != nil || got != want {
		t.Fatalf("got=%+v err=%v", got, err)
	}

	empty := SegmentFooter{SegmentID: 6, DataEnd: SegmentHeaderSize}
	encoded, err = EncodeSegmentFooter(empty)
	if err != nil {
		t.Fatal(err)
	}
	if got, err = DecodeSegmentFooter(encoded[:]); err != nil || got != empty {
		t.Fatalf("empty got=%+v err=%v", got, err)
	}
}

func TestFormatRejectsUnsupportedVersion(t *testing.T) {
	t.Parallel()
	header, _ := EncodeSegmentHeader(SegmentHeader{LogID: testLogID(), SegmentID: 1, SegmentSize: 1 << 20})
	binary.LittleEndian.PutUint16(header[8:10], FormatVersion+1)
	if _, err := DecodeSegmentHeader(header[:]); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("err=%v", err)
	}
}

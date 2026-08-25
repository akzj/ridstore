package v2

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func TestRecordFormatRoundTrip(t *testing.T) {
	addr, err := makeVAddr(7, segmentHeaderSize, 40)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeRecord(addr, []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded)%int(recordAlignment) != 0 {
		t.Fatalf("record is not aligned: %d", len(encoded))
	}
	header, payload, err := decodeRecord(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if header.Addr != addr || string(payload) != "payload" {
		t.Fatalf("round trip = %+v %q", header, payload)
	}

	encoded[recordHeaderSize] ^= 0xff
	if _, _, err := decodeRecord(encoded); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt payload error = %v", err)
	}
}

func TestSegmentMetadataRoundTrip(t *testing.T) {
	id := logID{1, 2, 3}
	headerBytes, err := encodeSegmentHeader(segmentHeader{LogID: id, SegmentID: 2, PreviousSegment: 1, SegmentSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	header, err := decodeSegmentHeader(headerBytes[:])
	if err != nil {
		t.Fatal(err)
	}
	if header.LogID != id || header.SegmentID != 2 || header.PreviousSegment != 1 || header.SegmentSize != 1<<20 {
		t.Fatalf("header = %+v", header)
	}
	oldVersion := headerBytes
	binary.LittleEndian.PutUint16(oldVersion[8:10], 1)
	binary.LittleEndian.PutUint32(oldVersion[60:64], crc32.Checksum(oldVersion[:60], crcTable))
	if _, err := decodeSegmentHeader(oldVersion[:]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("old format version error = %v", err)
	}

	first, _ := makeVAddr(2, segmentHeaderSize, 64)
	last, _ := makeVAddr(2, segmentHeaderSize+64, 64)
	footerBytes, err := encodeSegmentFooter(segmentFooter{SegmentID: 2, DataEnd: 1024, FirstAddr: first, LastAddr: last, RecordCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	footer, err := decodeSegmentFooter(footerBytes[:])
	if err != nil {
		t.Fatal(err)
	}
	if footer.SegmentID != 2 || footer.DataEnd != 1024 || footer.FirstAddr != first || footer.LastAddr != last || footer.RecordCount != 2 {
		t.Fatalf("footer = %+v", footer)
	}
}

func TestVAddrSizeClasses(t *testing.T) {
	tests := []struct {
		physicalSize uint64
		class        uint32
		hint         uint64
	}{
		{physicalSize: 32, class: 0, hint: 64},
		{physicalSize: 64, class: 0, hint: 64},
		{physicalSize: 72, class: 1, hint: 128},
		{physicalSize: 128, class: 1, hint: 128},
		{physicalSize: 136, class: 2, hint: 256},
		{physicalSize: 256, class: 2, hint: 256},
		{physicalSize: 264, class: 3, hint: 512},
		{physicalSize: 512, class: 3, hint: 512},
		{physicalSize: 520, class: 4, hint: 1024},
		{physicalSize: 1024, class: 4, hint: 1024},
		{physicalSize: 1032, class: 5, hint: 2048},
		{physicalSize: 2048, class: 5, hint: 2048},
		{physicalSize: 2056, class: 6, hint: 4096},
		{physicalSize: 4096, class: 6, hint: 4096},
		{physicalSize: 4104, class: 6, hint: 4096},
	}
	for _, tc := range tests {
		addr, err := makeVAddr(9, segmentHeaderSize, tc.physicalSize)
		if err != nil {
			t.Fatalf("size %d: %v", tc.physicalSize, err)
		}
		hint, err := addr.readHint()
		if err != nil || addr.Offset() != uint32(segmentHeaderSize) || addr.sizeClass() != tc.class || hint != tc.hint {
			t.Fatalf("size %d: addr=%v offset=%d class=%d hint=%d err=%v", tc.physicalSize, addr, addr.Offset(), addr.sizeClass(), hint, err)
		}
	}
	if _, err := makeVAddr(1, segmentHeaderSize+1, 64); !errors.Is(err, ErrInvalidVAddr) {
		t.Fatalf("unaligned offset error = %v", err)
	}
	reserved := VAddr(uint64(1)<<vaddrOffsetBits | segmentHeaderSize | uint64(vaddrReservedSize))
	if reserved.Valid() {
		t.Fatalf("reserved size class accepted: %v", reserved)
	}
}

func TestRecordRejectsMismatchedVAddrSizeClass(t *testing.T) {
	addr, err := makeVAddr(1, segmentHeaderSize, 128)
	if err != nil {
		t.Fatal(err)
	}
	record := make([]byte, 64)
	copy(record[0:4], recordMagic[:])
	binary.LittleEndian.PutUint16(record[4:6], formatVersion)
	binary.LittleEndian.PutUint16(record[6:8], uint16(recordHeaderSize))
	binary.LittleEndian.PutUint32(record[8:12], uint32(len(record)))
	binary.LittleEndian.PutUint32(record[12:16], 32)
	binary.LittleEndian.PutUint64(record[16:24], uint64(addr))
	binary.LittleEndian.PutUint32(record[28:32], crc32.Checksum(record[:28], crcTable))
	if _, err := decodeRecordHeader(record[:recordHeaderSize]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("mismatched size class error = %v", err)
	}
}

package v2

import (
	"errors"
	"testing"
)

func TestRecordFormatRoundTrip(t *testing.T) {
	addr, err := makeVAddr(7, segmentHeaderSize)
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

	first, _ := makeVAddr(2, segmentHeaderSize)
	last, _ := makeVAddr(2, segmentHeaderSize+64)
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

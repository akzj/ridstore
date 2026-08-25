package mapstore

import (
	"errors"
	"testing"

	"github.com/akzj/ridstore/internal/model"
)

func testStoreID() StoreID {
	return StoreID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
}

func TestSegmentHeaderRoundTrip(t *testing.T) {
	want := SegmentHeader{StoreID: testStoreID(), SegmentID: 2, PreviousSegment: 1, SegmentSize: 8192}
	encoded, err := EncodeSegmentHeader(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeSegmentHeader(encoded[:])
	if err != nil || got != want {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	corrupt := encoded
	corrupt[24] ^= 1
	if _, err := DecodeSegmentHeader(corrupt[:]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err=%v", err)
	}
}

func TestSegmentFooterRoundTrip(t *testing.T) {
	want := SegmentFooter{SegmentID: 2, ValidEnd: 4096, FirstSeq: 7, LastSeq: 9, NodeCount: 3}
	encoded, err := EncodeSegmentFooter(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeSegmentFooter(encoded[:])
	if err != nil || got != want {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if _, err := EncodeSegmentFooter(SegmentFooter{SegmentID: model.MapSegmentID(1), ValidEnd: SegmentHeaderSize, NodeCount: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("err=%v", err)
	}
}

func TestEmptySegmentFooter(t *testing.T) {
	want := SegmentFooter{SegmentID: 1, ValidEnd: SegmentHeaderSize}
	encoded, err := EncodeSegmentFooter(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeSegmentFooter(encoded[:])
	if err != nil || got != want {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

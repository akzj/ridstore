package v2

import (
	"encoding/hex"
	"testing"
)

func TestFormatGoldenVectors(t *testing.T) {
	var id logID
	for i := range id {
		id[i] = byte(i)
	}
	header, err := encodeSegmentHeader(segmentHeader{LogID: id, SegmentID: 7, PreviousSegment: 6, SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := makeVAddr(7, segmentHeaderSize, 40)
	if err != nil {
		t.Fatal(err)
	}
	taggedAddr, err := makeVAddr(7, segmentHeaderSize, 72)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := uint64(taggedAddr), uint64(0x0000000700000041); got != want {
		t.Fatalf("tagged VAddr = %#016x, want %#016x", got, want)
	}
	record, err := encodeRecord(addr, []byte("abc"))
	if err != nil {
		t.Fatal(err)
	}
	footer, err := encodeSegmentFooter(segmentFooter{SegmentID: 7, DataEnd: segmentHeaderSize + uint64(len(record)), FirstAddr: addr, LastAddr: addr, RecordCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "header", header[:], "524944415056324802004000070000000010000000000000000102030405060708090a0b0c0d0e0f0600000000000000400000000000000000000000314febbe")
	assertGolden(t, "record", record, "523252430200200028000000030000004000000007000000b73f4b366820f6ed6162630000000000")
	assertGolden(t, "footer", footer[:], "5249444150563246020040000700000068000000000000004000000007000000400000000700000001000000000000000000000000000000000000003cadf297")
}

func assertGolden(t *testing.T, name string, got []byte, wantHex string) {
	t.Helper()
	want, err := hex.DecodeString(wantHex)
	if err != nil {
		t.Fatalf("%s golden: %v", name, err)
	}
	if string(got) != string(want) {
		t.Fatalf("%s golden mismatch\ngot  %s\nwant %s", name, hex.EncodeToString(got), wantHex)
	}
}

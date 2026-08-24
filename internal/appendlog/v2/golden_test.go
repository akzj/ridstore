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
	addr, err := makeVAddr(7, segmentHeaderSize)
	if err != nil {
		t.Fatal(err)
	}
	record, err := encodeRecord(addr, []byte("abc"))
	if err != nil {
		t.Fatal(err)
	}
	footer, err := encodeSegmentFooter(segmentFooter{SegmentID: 7, DataEnd: segmentHeaderSize + uint64(len(record)), FirstAddr: addr, LastAddr: addr, RecordCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "header", header[:], "524944415056324801004000070000000010000000000000000102030405060708090a0b0c0d0e0f06000000000000004000000000000000000000003e24b457")
	assertGolden(t, "record", record, "523252430100200028000000030000004000000007000000b73f4b360b11ca266162630000000000")
	assertGolden(t, "footer", footer[:], "52494441505632460100400007000000680000000000000040000000070000004000000007000000010000000000000000000000000000000000000033c6ad7e")
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

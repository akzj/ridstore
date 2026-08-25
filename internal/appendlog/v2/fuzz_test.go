package v2

import "testing"

func FuzzDecodeRecord(f *testing.F) {
	addr, _ := makeVAddr(1, segmentHeaderSize, 40)
	valid, _ := encodeRecord(addr, []byte("seed"))
	f.Add(valid)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = decodeRecord(data)
	})
}

func FuzzDecodeSegmentHeader(f *testing.F) {
	valid, _ := encodeSegmentHeader(segmentHeader{LogID: logID{1}, SegmentID: 1, SegmentSize: 4096})
	f.Add(valid[:])
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeSegmentHeader(data)
	})
}

func FuzzDecodeSegmentFooter(f *testing.F) {
	valid, _ := encodeSegmentFooter(segmentFooter{SegmentID: 1, DataEnd: segmentHeaderSize})
	f.Add(valid[:])
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeSegmentFooter(data)
	})
}

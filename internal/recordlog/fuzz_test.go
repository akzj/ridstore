package recordlog

import "testing"

func FuzzDecodeRecord(f *testing.F) {
	physical, _ := PhysicalRecordSize(3)
	addr, _ := NewVAddr(1, SegmentHeaderSize, physical)
	seed, _ := EncodeRecord(addr, []byte("abc"))
	f.Add(seed)
	f.Fuzz(func(t *testing.T, value []byte) {
		_, _, _ = DecodeRecord(value)
	})
}

func FuzzDecodeSegmentHeader(f *testing.F) {
	seed, _ := EncodeSegmentHeader(SegmentHeader{LogID: testLogID(), SegmentID: 1, SegmentSize: 1 << 20})
	f.Add(seed[:])
	f.Fuzz(func(t *testing.T, value []byte) {
		_, _ = DecodeSegmentHeader(value)
	})
}

func FuzzDecodeSegmentFooter(f *testing.F) {
	seed, _ := EncodeSegmentFooter(SegmentFooter{SegmentID: 1, DataEnd: SegmentHeaderSize})
	f.Add(seed[:])
	f.Fuzz(func(t *testing.T, value []byte) {
		_, _ = DecodeSegmentFooter(value)
	})
}
